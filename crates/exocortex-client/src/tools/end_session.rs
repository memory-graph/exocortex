// crates/exocortex-client/src/tools/end_session.rs
//! MCP tool the harness calls at the end of a productive turn (§13.5,
//! agent-instructions PRD P8). Wraps 1-5 memory drafts into an
//! IngestBatch (§18.6) and submits over gRPC. Entities are NOT accepted
//! from the harness — the backend extracts them (R-T18).
//! Session-wrapup sends no ExternalSnapshotInfo (§18.3).
//!
//! r4: the tool self-preflights — the same local validation
//! `preflight_wrapup` runs executes BEFORE any wire dispatch, so an
//! invalid batch costs zero round-trips and comes back with
//! deterministic correction hints.
//! §4.5: edges may target existing memories by `to_memory_id`.
//! §4.8: the session id is client-owned; an explicit one (deliberate
//! multi-agent sharing) overrides the process-minted default.
//! D10b: the backend's advisory `similar_to` near-duplicate hints ride
//! the ack verbatim.

use rmcp::model::CallToolRequestParam;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, IngestBatch, MemoryDraft as WireMemoryDraft,
    ProducerIdentity, RegisterSourceRequest,
};

/// `exocortex.end_session` arguments (§13.5).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct EndSessionArgs {
    /// Session identifier. §4.8: OPTIONAL — the client stamps its
    /// process-minted conversation id when omitted. Pass an explicit id
    /// only when deliberately sharing a conversation group with another
    /// agent.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Project identifier.
    pub project_id: String,
    /// 1..=5 memory drafts. Anything else is rejected client-side before the wire.
    pub memories: Vec<MemoryDraftInput>,
    /// Optional edges between the memories in this batch, linked by draft_key.
    #[serde(default)]
    pub edges: Vec<EdgeHintInput>,
}

/// One memory draft (§13.5). ONE shape with the registry's preflight
/// input (D2/CR-9): the MCP tool schema and the HTTP schema cannot
/// drift because they are the same type.
pub type MemoryDraftInput = crate::preflight::PreflightMemoryDraft;

/// One edge hint between drafts (§13.5) or to an existing memory (§4.5).
pub type EdgeHintInput = crate::preflight::PreflightEdgeHint;

/// Ack shape returned to the harness.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct EndSessionAck {
    /// Accepted row count.
    pub accepted: u32,
    /// Rejected row count.
    pub rejected: u32,
    /// Backend LSN of the commit (0 when nothing landed).
    pub assigned_lsn: u64,
    /// Rejection summaries.
    pub rejections: Vec<RejectionSummary>,
    /// r4: `true` when the LOCAL pass rejected the batch — nothing was
    /// sent; fixing the named rows and resubmitting costs no wire calls.
    pub local_validation_failed: bool,
    /// §3.2 r3: checks only the backend can run, named honestly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unverified: Vec<crate::preflight::UnverifiedCheck>,
    /// D10b: advisory near-duplicate hints from the backend (or, offline,
    /// from the local pass). The producer decides whether to supersede.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub similar_to: Vec<SimilarToSummary>,
}

/// One rejection row.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RejectionSummary {
    /// Producer-local key of the offending row.
    pub draft_key: String,
    /// RejectCode name.
    pub code: String,
    /// The backend's own explanation (W4: which kind, which from/to type,
    /// which bound — verbatim from the wire RejectRow).
    pub detail: String,
    /// P3: deterministic remediation text (the wire guidance table).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub correction: String,
}

/// D10b: one advisory near-duplicate hint (§4.10).
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct SimilarToSummary {
    /// The draft in this batch.
    pub draft_key: String,
    /// 32-hex id of the near neighbor.
    pub existing_memory_id: String,
    /// Its title.
    pub existing_title: String,
    /// "replaces" | "contradicts" | "duplicate" — advisory, never blocking.
    pub suggestion: String,
}

/// The tool: client-side validation (1..=5 + the full local pass), batch
/// construction with `source_uri = session://<id>`, `producer_id =
/// session-wrapup`, no snapshot, HMAC signing, and Submit.
pub struct EndSessionTool {
    /// gRPC client to the backend IngestService.
    pub client: IngestServiceClient<tonic::transport::Channel>,
    /// Owning org.
    pub org_id: String,
    /// The effective fingerprint.
    pub fingerprint: [u8; 32],
    /// Producer authentication key.
    pub hmac_key: [u8; 32],
    /// Node identity.
    pub node_id: String,
    /// Agent identity.
    pub agent_id: String,
    /// Bearer token for the backend (R-Sec7 / audit CL4): attached as
    /// `authorization` metadata on every call.
    pub auth_token: Option<String>,
    /// The effective ontology (self-preflight's rulebook, W2-shared).
    pub ontology: Arc<exocortex_kernel::Ontology>,
    /// The local cache (cross-batch triple lookups; `None` ⇒ those land
    /// in `unverified`).
    pub cache: Option<Arc<exocortex_cache::LocalCache>>,
    /// Caller visibility (cache lookups are caller-scoped, R-MT4).
    pub vc: exocortex_ops::VisibilityContext,
}

fn parse_visibility(s: &str) -> Result<i32, String> {
    match s.to_lowercase().as_str() {
        "private" => Ok(0),
        "project" => Ok(1),
        "team" => Ok(2),
        "org" => Ok(3),
        other => Err(format!("unknown visibility `{other}`")),
    }
}

impl EndSessionTool {
    /// Handle a call. `call_tool` dispatch from the MCP server feeds the
    /// parsed `EndSessionArgs` here.
    pub async fn handle(&self, args: EndSessionArgs) -> Result<EndSessionAck, rmcp::Error> {
        let session_id = args.session_id.clone().unwrap_or_default();
        if session_id.is_empty() {
            return Err(rmcp::Error::invalid_params(
                "session_id: the MCP layer must stamp the client-minted conversation id (§4.8)",
                None,
            ));
        }
        if args.edges.len() > exocortex_wire::limits::MAX_EDGES_PER_BATCH {
            return Err(rmcp::Error::invalid_params(
                "edges: at most 64 relationships per request",
                None,
            ));
        }
        for memory in &args.memories {
            if let Err(detail) =
                exocortex_wire::limits::validate_memory_fields(&memory.content, &memory.tags)
            {
                return Err(rmcp::Error::invalid_params(detail, None));
            }
        }
        // r4 self-preflight: the SAME local pass preflight_wrapup runs,
        // before any wire work. Invalid batches never leave the process.
        let cache = self.cache.clone();
        let org = self.org_id.clone();
        let vc = self.vc.clone();
        let pre =
            crate::preflight::validate_batch(&self.ontology, &args.memories, &args.edges, |id| {
                cache.as_ref().and_then(|c| {
                    let mut out = [0u8; 16];
                    let b = id.as_bytes();
                    if b.len() != 32 {
                        return None;
                    }
                    for i in 0..16 {
                        out[i] =
                            u8::from_str_radix(std::str::from_utf8(&b[i * 2..i * 2 + 2]).ok()?, 16)
                                .ok()?;
                    }
                    c.get_memory(&org, &exocortex_kernel::MemoryId(out), &vc)
                        .map(|m| m.memory_type)
                })
            });
        if !pre.rejections.is_empty() {
            return Ok(EndSessionAck {
                accepted: 0,
                rejected: pre.would_reject,
                assigned_lsn: 0,
                rejections: pre
                    .rejections
                    .iter()
                    .map(|r| RejectionSummary {
                        draft_key: r.draft_key.clone(),
                        code: r.code.clone(),
                        detail: r.detail.clone(),
                        correction: r.correction.clone(),
                    })
                    .collect(),
                local_validation_failed: true,
                unverified: pre.unverified,
                similar_to: vec![],
            });
        }
        let unverified = pre.unverified;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let ts = prost_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        };
        // W5 (audit): an edge is never more visible than the narrower of
        // its endpoints — derived here so org-visible memories no longer
        // get project-invisible edges.
        let draft_vis: std::collections::HashMap<String, i32> = args
            .memories
            .iter()
            .map(|m| {
                let v = match m.visibility.to_lowercase().as_str() {
                    "private" => 0,
                    "project" => 1,
                    "team" => 2,
                    _ => 3,
                };
                (m.draft_key.clone(), v)
            })
            .collect();
        let mut memories = Vec::with_capacity(args.memories.len());
        for m in args.memories {
            let vis = parse_visibility(&m.visibility)
                .map_err(|_| rmcp::Error::invalid_params("unknown visibility", None))?;
            memories.push(WireMemoryDraft {
                draft_key: m.draft_key,
                id: uuid::Uuid::now_v7().simple().to_string(),
                memory_type: m.memory_type,
                title: m.title,
                content: m.content,
                tags: m.tags,
                visibility: vis,
                valid_from: Some(ts),
                valid_until: None,
                external_key: None, // never for session-wrapup (§18.3)
            });
        }
        let relationships: Vec<exocortex_wire::ingest::v1::RelationshipDraft> = args
            .edges
            .into_iter()
            .map(|e| exocortex_wire::ingest::v1::RelationshipDraft {
                from_draft_key: e.from_draft_key.clone(),
                to_draft_key: e.to_draft_key.clone(),
                kind: e.kind,
                strength: e.strength,
                confidence: 0.8,
                context: String::new(),
                visibility: if e.to_draft_key.is_empty() {
                    // Cross-batch edge: the target's visibility is unknown
                    // client-side; the server derives the narrower endpoint.
                    draft_vis
                        .get(&e.from_draft_key)
                        .copied()
                        .unwrap_or(1)
                        .min(3)
                } else {
                    draft_vis
                        .get(&e.from_draft_key)
                        .copied()
                        .unwrap_or(1)
                        .min(draft_vis.get(&e.to_draft_key).copied().unwrap_or(1))
                },
                to_memory_id: e.to_memory_id,
            })
            .collect();

        let mut batch = IngestBatch {
            org_id: self.org_id.clone(),
            source_uri: format!("session://{session_id}"),
            producer_id: "session-wrapup".into(),
            // IN7 (audit): the id is CONTENT-bound, so a retried wrapup
            // after a lost response hits the server's idempotency registry
            // instead of committing a duplicate.
            batch_id: crate::drain::content_batch_id(&session_id, &memories, &relationships),
            mapping_version: "session-wrapup:1.0.0".into(),
            ontology_fingerprint: self.fingerprint.to_vec(),
            ceiling: 3,              // registered ceiling (§18.2)
            checksum: String::new(), // set by prepare_batch below
            observed_at: Some(ts),
            recorded_at: Some(ts),
            snapshot: None, // no ExternalSnapshotInfo (§18.3)
            memories,
            relationships,
            producer: Some(ProducerIdentity {
                node_id: self.node_id.clone(),
                agent_id: self.agent_id.clone(),
                adapter_id: String::new(),
                hmac_signature: vec![],
                // §4.4: producer telemetry rides the signed identity.
                client_metadata: Some(exocortex_wire::ingest::v1::ClientMetadata {
                    playbook_version: crate::playbook::PLAYBOOK_VERSION.into(),
                    client_version: env!("CARGO_PKG_VERSION").into(),
                    harness_hint: String::new(),
                }),
            }),
        };
        // Canonical checksum + HMAC from the single wire implementation
        // (PRD R3/R6 — no local copies).
        exocortex_wire::signing::prepare_batch(&self.hmac_key, &mut batch);

        // RegisterSource before the first Submit (R-I3); idempotent on the
        // server side. The registration is signed with the same producer
        // HMAC as the batch (audit WS1) — the server rejects unsigned
        // registry mutations. D8: declares the closed producer kind.
        let mut client = self.client.clone();
        let mut registration = RegisterSourceRequest {
            org_id: self.org_id.clone(),
            source_uri: batch.source_uri.clone(),
            producer_id: "session-wrapup".into(),
            ceiling: 3,
            source_flavor: "session".into(),
            producer_kind: exocortex_wire::ingest::v1::ProducerKind::CodingAgent.into(),
            producer: Some(ProducerIdentity {
                node_id: self.node_id.clone(),
                agent_id: self.agent_id.clone(),
                adapter_id: String::new(),
                hmac_signature: vec![],
                client_metadata: batch
                    .producer
                    .as_ref()
                    .and_then(|p| p.client_metadata.clone()),
            }),
        };
        exocortex_wire::signing::sign_registration(&self.hmac_key, &mut registration);
        let mut reg_req = tonic::Request::new(registration);
        if let Some(token) = &self.auth_token {
            if let Ok(v) = format!("Bearer {token}").parse() {
                reg_req.metadata_mut().insert("authorization", v);
            }
        }
        if let Err(e) = client.register_source(reg_req).await {
            tracing::warn!(%e, "register_source failed; submit will surface the cause");
        }

        let mut submit_req = tonic::Request::new(batch);
        if let Some(token) = &self.auth_token {
            if let Ok(v) = format!("Bearer {token}").parse() {
                submit_req.metadata_mut().insert("authorization", v);
            }
        }
        let ack = client
            .submit(submit_req)
            .await
            .map_err(|e| rmcp::Error::internal_error(format!("ingest: {e}"), None))?
            .into_inner();

        Ok(EndSessionAck {
            accepted: ack.accepted,
            rejected: ack.rejected,
            assigned_lsn: ack.assigned_lsn,
            rejections: ack
                .rejections
                .iter()
                .map(|r| {
                    let code = exocortex_wire::ingest::v1::RejectCode::try_from(r.code)
                        .unwrap_or(exocortex_wire::ingest::v1::RejectCode::Unknown);
                    RejectionSummary {
                        draft_key: r.draft_key.clone(),
                        code: format!("{code:?}"),
                        detail: r.detail.clone(),
                        // P3: the ack's own explanation plus the table's fix.
                        correction: exocortex_wire::corrections::guidance(code)
                            .correction
                            .into(),
                    }
                })
                .collect(),
            local_validation_failed: false,
            unverified,
            similar_to: ack
                .similar_to
                .iter()
                .map(|h| SimilarToSummary {
                    draft_key: h.draft_key.clone(),
                    existing_memory_id: h.existing_memory_id.clone(),
                    existing_title: h.existing_title.clone(),
                    suggestion: h.suggestion.clone(),
                })
                .collect(),
        })
    }
}

/// Parse `EndSessionArgs` from a raw tool call (used by the MCP dispatch).
pub fn parse_args(request: CallToolRequestParam) -> Result<EndSessionArgs, rmcp::Error> {
    let tcc = ToolCallContextArgs { request };
    serde_json::from_value(serde_json::Value::Object(tcc.arguments()))
        .map_err(|_| rmcp::Error::invalid_params("bad end_session args", None))
}

struct ToolCallContextArgs {
    request: CallToolRequestParam,
}

impl ToolCallContextArgs {
    fn arguments(&self) -> serde_json::Map<String, serde_json::Value> {
        self.request.arguments.clone().unwrap_or_default()
    }
}
