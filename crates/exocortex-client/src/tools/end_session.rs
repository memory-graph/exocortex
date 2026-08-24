// crates/exocortex-client/src/tools/end_session.rs
//! MCP tool the harness calls at the end of a coding session (§13.5).
//! Wraps 1-5 memory drafts into an IngestBatch (§18.6) and submits over
//! gRPC. Entities are NOT accepted from the harness — the backend extracts
//! them (R-T18). Session-wrapup sends no ExternalSnapshotInfo (§18.3).

use rmcp::model::CallToolRequestParam;
use serde::{Deserialize, Serialize};

use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, IngestBatch, MemoryDraft as WireMemoryDraft,
    ProducerIdentity, RegisterSourceRequest,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// `exocortex.end_session` arguments (§13.5).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct EndSessionArgs {
    /// Session identifier.
    pub session_id: String,
    /// Project identifier.
    pub project_id: String,
    /// 1..=5 memory drafts. Anything else is rejected client-side before the wire.
    pub memories: Vec<MemoryDraftInput>,
    /// Optional edges between the memories in this batch, linked by draft_key.
    #[serde(default)]
    pub edges: Vec<EdgeHintInput>,
}

/// One memory draft (§13.5).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct MemoryDraftInput {
    /// Links edges within this batch.
    pub draft_key: String,
    /// MUST match a registered MemoryType label.
    pub memory_type: String,
    /// 1..=200 chars (R-T5).
    pub title: String,
    /// Free-text content.
    pub content: String,
    /// "private"|"project"|"team"|"org" (R-T6).
    pub visibility: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// One edge hint between drafts (§13.5).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct EdgeHintInput {
    /// Source draft key.
    pub from_draft_key: String,
    /// Target draft key.
    pub to_draft_key: String,
    /// MUST match a registered kind display_name.
    pub kind: String,
    #[serde(default)]
    pub strength: f32,
}

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
}

/// One rejection row.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RejectionSummary {
    /// Producer-local key of the offending row.
    pub draft_key: String,
    /// RejectCode name.
    pub code: String,
}

/// The tool: client-side validation (1..=5), batch construction with
/// `source_uri = session://<id>`, `producer_id = session-wrapup`, no
/// snapshot, HMAC signing, and Submit.
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

/// Canonical checksum over sorted row projections (§13.6 step 3).
pub fn compute_checksum(m: &[WireMemoryDraft]) -> String {
    // BLAKE3 over canonical (sorted) serialization — same input => same
    // checksum; edge order cannot change it (§13.6 step 3).
    let mut canonical: Vec<String> = m
        .iter()
        .map(|d| {
            serde_json::json!({
                "content": d.content,
                "draft_key": d.draft_key,
                "memory_type": d.memory_type,
                "title": d.title,
                "visibility": d.visibility,
            })
            .to_string()
        })
        .collect();
    canonical.sort();
    blake3::hash(canonical.join("\n").as_bytes())
        .to_hex()
        .to_string()
}

fn sign_batch(key: &[u8; 32], b: &mut IngestBatch) {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).unwrap();
    mac.update(&prost::Message::encode_to_vec(b));
    if let Some(p) = b.producer.as_mut() {
        p.hmac_signature = mac.finalize().into_bytes().to_vec();
    }
}

impl EndSessionTool {
    /// Handle a call. `call_tool` dispatch from the MCP server feeds the
    /// parsed `EndSessionArgs` here.
    pub async fn handle(&self, args: EndSessionArgs) -> Result<EndSessionAck, rmcp::Error> {
        if args.memories.is_empty() || args.memories.len() > 5 {
            return Err(rmcp::Error::invalid_params(
                "memories: expected 1..=5",
                None,
            ));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let ts = prost_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        };
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
        let relationships = args
            .edges
            .into_iter()
            .map(|e| exocortex_wire::ingest::v1::RelationshipDraft {
                from_draft_key: e.from_draft_key,
                to_draft_key: e.to_draft_key,
                kind: e.kind,
                strength: e.strength,
                confidence: 0.8,
                context: String::new(),
                visibility: 1, // Project; ≤ the registered ceiling
            })
            .collect();

        let mut batch = IngestBatch {
            org_id: self.org_id.clone(),
            source_uri: format!("session://{}", args.session_id),
            producer_id: "session-wrapup".into(),
            batch_id: uuid::Uuid::now_v7().simple().to_string(),
            mapping_version: "session-wrapup:1.0.0".into(),
            ontology_fingerprint: self.fingerprint.to_vec(),
            ceiling: 3, // registered ceiling (§18.2)
            checksum: compute_checksum(&memories),
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
            }),
        };
        sign_batch(&self.hmac_key, &mut batch);

        // RegisterSource before the first Submit (R-I3); idempotent on the
        // server side.
        let mut client = self.client.clone();
        let _ = client
            .register_source(RegisterSourceRequest {
                org_id: self.org_id.clone(),
                source_uri: batch.source_uri.clone(),
                producer_id: "session-wrapup".into(),
                ceiling: 3,
                source_flavor: "session".into(),
            })
            .await;

        let ack = client
            .submit(batch)
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
                .map(|r| RejectionSummary {
                    draft_key: r.draft_key.clone(),
                    code: format!(
                        "{:?}",
                        exocortex_wire::ingest::v1::RejectCode::try_from(r.code)
                            .unwrap_or(exocortex_wire::ingest::v1::RejectCode::Unknown)
                    ),
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
