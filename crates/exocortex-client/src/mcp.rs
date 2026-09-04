//! The stdio MCP surface (§13.5). `search_memories` (Function
//! `SearchMemories`, 500µs/3ms budgets) reads the ArcSwap cache;
//! `end_session` (§13.6) submits a wrapup batch to the backend over gRPC
//! when `--backend` is configured, or buffers it into the local WAL offline
//! (§5.2, M3 path) and answers `{ local_lsns, sync_pending: true }`. The
//! remaining Functions are registered as stubs that return a structured
//! not-implemented error rather than panicking — a panicking tool would
//! tear down the stdio server.

use rmcp::handler::server::tool::ToolCallContext;
use rmcp::{tool, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use exocortex_cache::LocalCache;
use exocortex_ops::VisibilityContext;

use crate::eof_drain::InFlightCalls;
use crate::tools::end_session::{EdgeHintInput, EndSessionArgs, EndSessionTool, MemoryDraftInput};
use crate::wal;

/// Shared server state: the local cache plus the caller's fixed visibility
/// context (v1: one user per client process, §17).
#[derive(Clone)]
pub struct ExocortexMcp {
    org: smol_str::SmolStr,
    cache: Arc<LocalCache>,
    vc: VisibilityContext,
    /// The gRPC-backed end_session tool; `None` when no `--backend` is
    /// configured (offline mode falls through to the WAL path).
    end_session: Option<Arc<EndSessionTool>>,
    /// Offline write buffer (§5.2): present → `end_session` buffers locally
    /// and answers `sync_pending: true`.
    wal: Option<Arc<wal::Wal>>,
    /// Effective ontology (draft→memory resolution on the offline path,
    /// and the preflight rulebook on every path).
    ontology: Arc<exocortex_kernel::Ontology>,
    /// §4.8: the client-minted conversation id. The client process spans
    /// the whole harness conversation (stdio servers launch per session),
    /// so this id IS the session grouping key unless the caller passes an
    /// explicit one (deliberate multi-agent sharing).
    process_session_id: String,
    /// D27: in-flight tool calls; stdin EOF drains these before the
    /// process exits (see `eof_drain`).
    in_flight: Arc<InFlightCalls>,
}

impl ExocortexMcp {
    /// Build the server over a cache.
    pub fn new(
        org: smol_str::SmolStr,
        cache: Arc<LocalCache>,
        vc: VisibilityContext,
        ontology: Arc<exocortex_kernel::Ontology>,
    ) -> Self {
        Self {
            org,
            cache,
            vc,
            end_session: None,
            wal: None,
            ontology,
            process_session_id: uuid::Uuid::now_v7().simple().to_string(),
            in_flight: InFlightCalls::new(),
        }
    }

    /// Attach the gRPC-backed `end_session` tool (built from `--backend`).
    pub fn with_end_session(mut self, tool: Arc<EndSessionTool>) -> Self {
        self.end_session = Some(tool);
        self
    }

    /// Attach the offline WAL (M3 offline write path). The ontology is
    /// always present (constructor), so this only adds the buffer.
    pub fn with_offline_wal(mut self, wal: Arc<wal::Wal>) -> Self {
        self.wal = Some(wal);
        self
    }

    /// §4.8: the process-minted conversation id (test surface).
    pub fn process_session_id(&self) -> &str {
        &self.process_session_id
    }

    /// D27: the in-flight call tracker feeding the EOF-draining stdio
    /// reader (see `eof_drain`).
    pub fn in_flight_calls(&self) -> Arc<InFlightCalls> {
        Arc::clone(&self.in_flight)
    }
}

/// Input for `exocortex.search_memories`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SearchMemoriesInput {
    /// Free-text query matched against titles and tags.
    pub query: String,
    /// Maximum results (server caps at 500, §6.3).
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    20
}

impl ExocortexMcp {
    /// IN10 (audit): the registry OpContext — the same typed handler the
    /// HTTP bind dispatches serves this MCP surface (CR-9). The storage is
    /// the no-backend shim: interactive reads are cache-served.
    fn registry_ctx(&self) -> std::sync::Arc<exocortex_ops::OpContext> {
        std::sync::Arc::new(exocortex_ops::OpContext {
            visibility_ctx: self.vc.clone(),
            audit_admin: true,
            storage: std::sync::Arc::new(crate::no_backend::NoBackendStorage),
            cache: self.cache.clone(),
            deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
            ontology: Some(self.ontology.clone()),
            // D21-b: no ingest surface in standalone mode — `preflight_batch`
            // fails loudly rather than approximating Submit's verdicts.
            ingest_preflight: None,
            // D4: no embedding runtime in standalone mode —
            // `reindex_embeddings` fails loudly (backend node only).
            embedding_reindex: None,
        })
    }

    /// `exocortex.search_memories` (§7.12; p50 500µs / p99 3ms).
    #[tool(
        name = "exocortex.search_memories",
        description = "Search the exocortex graph: ranked memories matching a free-text query over titles and tags."
    )]
    pub async fn search_memories(
        &self,
        #[tool(param)] query: String,
        #[tool(param)] limit: Option<u32>,
    ) -> Result<String, String> {
        // IN10 (audit): dispatch through the ONE registry handler — the
        // same implementation and output shape HTTP serves (CR-9).
        let entry = exocortex_ops::entries()
            .into_iter()
            .find(|e| e.mcp_tool_name == "exocortex.search_memories")
            .expect("search_memories registered");
        let out = (entry.handler)(
            entry,
            &self.registry_ctx(),
            serde_json::to_value(exocortex_ops::operations::SearchInput {
                query,
                limit: limit.unwrap_or(20),
            })
            .map_err(|e| e.to_string())?,
        )
        .await
        .map_err(|e| e.to_string())?;
        // R-M7: the client surface adds the version stamp AROUND the
        // registry shape (inner object stays byte-identical).
        let version = self.cache.version(&self.org);
        let mut v = serde_json::to_value(&out).map_err(|e| e.to_string())?;
        if let serde_json::Value::Object(map) = &mut v {
            map.insert(
                "snapshot_version".into(),
                serde_json::json!({
                    "local_lsn": version.map(|x| x.local_lsn).unwrap_or(0),
                    "backend_lsn": version.map(|x| x.backend_lsn).unwrap_or(0),
                }),
            );
        }
        serde_json::to_string(&v).map_err(|e| e.to_string())
    }

    /// `exocortex.get_memory` (registry op, client-side over the cache).
    #[tool(
        name = "exocortex.get_memory",
        description = "Fetch one memory by hex id from the local org graph."
    )]
    pub async fn get_memory(&self, #[tool(param)] id: String) -> Result<String, String> {
        // IN10 (audit): registry dispatch — the hit AND the miss carry the
        // registry's `{memory: ...}` shape (the hand-rolled path returned a
        // flat object on hit and `{memory: null}` on miss).
        let entry = exocortex_ops::entries()
            .into_iter()
            .find(|e| e.mcp_tool_name == "exocortex.get_memory")
            .expect("get_memory registered");
        let out = (entry.handler)(
            entry,
            &self.registry_ctx(),
            serde_json::to_value(exocortex_ops::operations::GetMemoryInput { id })
                .map_err(|e| e.to_string())?,
        )
        .await
        .map_err(|e| e.to_string())?;
        serde_json::to_string(&out).map_err(|e| e.to_string())
    }

    /// `exocortex.find_related` (registry op, client-side over the cache).
    #[tool(
        name = "exocortex.find_related",
        description = "Bounded k-hop neighborhood of a memory (hex id anchor, depth <= 4)."
    )]
    pub async fn find_related(
        &self,
        #[tool(param)] anchor: String,
        #[tool(param)] k: Option<u8>,
    ) -> Result<String, String> {
        // IN10 (audit): registry dispatch — the registry's
        // `{memories: [...]}` shape, never a bare array.
        let entry = exocortex_ops::entries()
            .into_iter()
            .find(|e| e.mcp_tool_name == "exocortex.find_related")
            .expect("find_related registered");
        let out = (entry.handler)(
            entry,
            &self.registry_ctx(),
            serde_json::to_value(exocortex_ops::operations::FindRelatedInput {
                anchor,
                k: k.unwrap_or(2),
            })
            .map_err(|e| e.to_string())?,
        )
        .await
        .map_err(|e| e.to_string())?;
        serde_json::to_string(&out).map_err(|e| e.to_string())
    }

    /// `exocortex.end_session` (§13.6): wrapup batch submit. Online: gRPC
    /// Submit to the backend IngestService. Offline: WAL append with
    /// `{ local_lsns, sync_pending: true }` (§5.2). §4.8: an omitted
    /// session id is stamped with the client-minted conversation id.
    #[tool(
        name = "exocortex.end_session",
        description = "Submit a session wrapup: 1-5 memory drafts plus optional edges (by draft_key within the batch, or to_memory_id for an existing memory). session_id is optional — the client stamps its conversation id."
    )]
    pub async fn end_session(
        &self,
        #[tool(param)] session_id: Option<String>,
        #[tool(param)] project_id: String,
        #[tool(param)] team_id: Option<String>,
        #[tool(param)] memories: Vec<MemoryDraftInput>,
        #[tool(param)] edges: Vec<EdgeHintInput>,
    ) -> Result<String, String> {
        let args = EndSessionArgs {
            // §4.8: explicit id (deliberate sharing) wins; otherwise the
            // process-minted conversation id groups every batch of this
            // client process into one backend group.
            session_id: Some(session_id.unwrap_or_else(|| self.process_session_id.clone())),
            project_id,
            team_id,
            memories,
            edges,
        };
        if let Some(tool) = &self.end_session {
            let ack = tool.handle(args).await.map_err(|e| e.to_string())?;
            return serde_json::to_string(&ack).map_err(|e| e.to_string());
        }
        if let Some(wal) = &self.wal {
            return self
                .end_session_offline(wal, &self.ontology, args)
                .map_err(|e| e.to_string());
        }
        Err(json_error("not-connected", "end_session requires --backend (gRPC submit) or the offline WAL; neither is configured"))
    }

    /// Offline path (§5.2): resolve drafts against the effective ontology,
    /// assign ids, buffer into the local WAL, and answer with the local LSNs.
    fn end_session_offline(
        &self,
        wal: &wal::Wal,
        ontology: &exocortex_kernel::Ontology,
        args: EndSessionArgs,
    ) -> Result<String, String> {
        use exocortex_kernel::{MemoryDraft, MemoryId};
        // §4.8: the MCP layer has already stamped the default id; an
        // explicit one rides through untouched.
        let session_id = args.session_id.clone().unwrap_or_default();
        if session_id.is_empty() {
            return Err(json_error("invalid-params", "session_id required"));
        }
        if args.memories.is_empty() || args.memories.len() > 5 {
            return Err(json_error("invalid-params", "memories: expected 1..=5"));
        }
        let now = chrono::Utc::now();
        // W1/IN7/CL1: capture the rebuild inputs BEFORE the loop consumes
        // the drafts. One canonical derivation (drain::content_batch_id):
        // the offline WAL stamp and a later online submission of the same
        // content hit one server idempotency entry.
        let batch_id = crate::drain::content_batch_id(&session_id, &args.memories, &args.edges);
        let draft_keys: Vec<String> = args.memories.iter().map(|m| m.draft_key.clone()).collect();
        let tags: Vec<Vec<String>> = args.memories.iter().map(|m| m.tags.clone()).collect();
        let mut ids: Vec<(String, MemoryId)> = Vec::with_capacity(args.memories.len());
        let mut drafts: Vec<MemoryDraft> = Vec::with_capacity(args.memories.len());
        for m in args.memories {
            let memory_type = ontology.memory_type_id(&m.memory_type).ok_or_else(|| {
                json_error(
                    "unknown-memory-type",
                    format!("unknown memory type `{}`", m.memory_type),
                )
            })?;
            let visibility = match m.visibility.to_lowercase().as_str() {
                "private" => exocortex_kernel::Visibility::Private,
                "project" => exocortex_kernel::Visibility::Project,
                "team" => exocortex_kernel::Visibility::Team,
                "org" => exocortex_kernel::Visibility::Org,
                other => {
                    return Err(json_error(
                        "invalid-params",
                        format!("unknown visibility `{other}`"),
                    ))
                }
            };
            // W2 (audit): the offline path runs the SAME kernel validator
            // as ingest — title/content bounds, no-widening — so a batch
            // cannot succeed or fail based on which transport carried it.
            let context = exocortex_kernel::MemoryContext {
                timestamp: now,
                project_id: Some(args.project_id.clone().into()),
                project_path: None,
                team_id: args
                    .team_id
                    .as_deref()
                    .filter(|id| !id.is_empty())
                    .map(Into::into),
                tenant_id: Some(self.vc.org_id.clone()),
                session_id: Some(session_id.clone().into()),
                user_id: Some(self.vc.user_id.clone()),
                created_by: None,
                files_involved: Default::default(),
                languages: Default::default(),
                frameworks: Default::default(),
                technologies: Default::default(),
                git_commit: None,
                git_branch: None,
                working_directory: None,
                entities: Default::default(),
                additional_metadata: serde_json::Value::Null,
            };
            let probe = MemoryDraft {
                memory_type,
                title: m.title.clone().into(),
                content: m.content.clone(),
                summary: None,
                visibility,
                context: context.clone(),
                edge_hints: Default::default(),
                external_key: None,
            };
            if let Err(e) = exocortex_kernel::validator::validate_draft(
                ontology,
                &probe,
                exocortex_kernel::validator::SourceCeiling {
                    source: "offline-wal",
                    ceiling: exocortex_kernel::Visibility::Org,
                },
            ) {
                return Err(json_error("validation", e.to_string()));
            }
            let id = MemoryId::new_v7();
            let draft = MemoryDraft {
                memory_type,
                title: m.title.into(),
                content: m.content,
                summary: None,
                visibility,
                context,
                edge_hints: Default::default(),
                external_key: None,
            };
            ids.push((m.draft_key, id));
            drafts.push(draft);
        }
        // Edge hints become typed hints against the assigned ids, attached to
        // the draft named by from_draft_key. §4.5: a to_memory_id edge
        // targets an EXISTING memory — the hex id is used directly and
        // existence/triple is checked at commit (drain) time.
        for e in &args.edges {
            let Some(src) = ids.iter().position(|(k, _)| *k == e.from_draft_key) else {
                return Err(json_error(
                    "invalid-params",
                    format!("edge references unknown draft_key `{}`", e.from_draft_key),
                ));
            };
            let to = if !e.to_memory_id.is_empty() {
                let Some(to) = MemoryId::parse_hex(&e.to_memory_id) else {
                    return Err(json_error(
                        "invalid-params",
                        format!(
                            "to_memory_id `{}` is not a 32-hex memory id",
                            e.to_memory_id
                        ),
                    ));
                };
                to
            } else {
                let Some((_, to)) = ids.iter().find(|(k, _)| *k == e.to_draft_key) else {
                    return Err(json_error(
                        "invalid-params",
                        format!("edge references unknown draft_key `{}`", e.to_draft_key),
                    ));
                };
                *to
            };
            let Some(kind) = ontology.kind_id(&e.kind) else {
                return Err(json_error(
                    "unknown-kind",
                    format!("unknown relationship kind `{}`", e.kind),
                ));
            };
            drafts[src].edge_hints.push(exocortex_kernel::EdgeHint {
                kind,
                to,
                strength: if e.strength == 0.0 {
                    None
                } else {
                    Some(e.strength)
                },
                confidence: None,
            });
        }
        let memory_ids: Vec<MemoryId> = ids.into_iter().map(|(_, id)| id).collect();
        let local_lsn = wal
            .append_batch_full_idempotent(
                &session_id,
                drafts,
                memory_ids,
                batch_id,
                draft_keys,
                tags,
            )
            .map_err(|e| json_error("wal-error", e.to_string()))?;
        // SR-PRD F2: read-your-writes — publish the materialized batch
        // into the served snapshot (ONE copy-on-write swap that also
        // stamps the R-M7 local LSN). Read back through the SAME
        // materializer boot seeding uses; cross-batch edge targets
        // resolve against the served graph. A missing just-appended row may
        // still degrade to an LSN-only advance; a corrupt row is a precise,
        // fail-closed error and remains untouched for recovery (R6-B15).
        match wal.entry(local_lsn) {
            Ok(Some(entry)) => {
                let rows = crate::materialize::materialize_entry(
                    &self.ontology,
                    &self.org,
                    &entry,
                    &|id| {
                        self.cache
                            .get_memory(&self.org, id, &self.vc)
                            .map(|m| (m.memory_type, m.visibility))
                    },
                );
                if !rows.dropped_edges.is_empty() {
                    tracing::warn!(?rows.dropped_edges, "offline edges not served");
                }
                self.cache
                    .apply_local(&self.org, &rows.memories, &rows.edges, local_lsn);
            }
            Ok(None) => {
                tracing::warn!("wal entry {local_lsn} unreadable after append; advancing LSN only");
                self.cache.advance_local_lsn(&self.vc.org_id, local_lsn);
            }
            Err(error) => return Err(json_error("wal-corrupt", error.to_string())),
        }
        #[derive(Serialize)]
        struct OfflineAck {
            local_lsns: Vec<u64>,
            sync_pending: bool,
        }
        serde_json::to_string(&OfflineAck {
            local_lsns: vec![local_lsn],
            sync_pending: true,
        })
        .map_err(|e| e.to_string())
    }

    /// `exocortex.preflight_wrapup` (D2, §3.2): validate a proposed batch
    /// locally — the SAME kernel rulebook end_session's self-preflight
    /// and `IngestService::Submit` run (W2) — without writing. Returns
    /// rejections with deterministic correction hints plus the
    /// `unverified` list of server-only checks.
    #[tool(
        name = "exocortex.preflight_wrapup",
        description = "Validate a proposed wrapup batch without writing: the same rules end_session enforces, answered locally with correction hints and an unverified list of server-only checks."
    )]
    pub async fn preflight_wrapup(
        &self,
        #[tool(param)] _project_id: String,
        #[tool(param)] memories: Vec<MemoryDraftInput>,
        #[tool(param)] edges: Vec<EdgeHintInput>,
    ) -> Result<String, String> {
        let cache = self.cache.clone();
        let org = self.org.to_string();
        let vc = self.vc.clone();
        let result = crate::preflight::validate_batch(&self.ontology, &memories, &edges, |id| {
            let id = exocortex_kernel::MemoryId::parse_hex(id)?;
            cache.get_memory(&org, &id, &vc).map(|m| m.memory_type)
        });
        serde_json::to_string(&result).map_err(|e| e.to_string())
    }

    /// `exocortex.playbook_version` (D3, §3.3): the compiled playbook
    /// version plus content hashes — one version string governs the
    /// playbook and the instruction block.
    #[tool(
        name = "exocortex.playbook_version",
        description = "Report the compiled Agent Playbook version and the content hashes of the playbook and the CLAUDE.md/AGENTS.md instruction block."
    )]
    pub async fn playbook_version(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct VersionReport {
            version: String,
            playbook_hash: String,
            block_hash: String,
        }
        serde_json::to_string(&VersionReport {
            version: crate::playbook::PLAYBOOK_VERSION.into(),
            playbook_hash: crate::playbook::playbook_hash(),
            block_hash: crate::playbook::block_hash(),
        })
        .map_err(|e| e.to_string())
    }
}

/// Self-contained tool schemas. schemars emits referenced types as
/// `{"$ref": "#/definitions/X"}` beside a root `definitions` map; MCP
/// harnesses legitimately drop that map while passing the `$ref`
/// through unresolved, which leaves the calling model guessing draft
/// fields (observed live in the Crush dogfood: `missing field
/// memory_type`, then `missing field content`). Inline every
/// `#/definitions/X` ref so the served schema needs nothing but itself.
fn self_contained(tool: rmcp::model::Tool) -> rmcp::model::Tool {
    let mut schema = tool.input_schema.as_ref().clone();
    let defs = match schema.remove("definitions") {
        Some(serde_json::Value::Object(defs)) => defs,
        _ => return tool,
    };
    let mut root = serde_json::Value::Object(schema);
    inline_schema_refs(&mut root, &defs, &mut Vec::new());
    match root {
        serde_json::Value::Object(map) => rmcp::model::Tool {
            name: tool.name,
            description: tool.description,
            input_schema: Arc::new(map),
        },
        _ => tool,
    }
}

/// Replace each `#/definitions/X` ref object with the definition itself,
/// depth-first. A name already being expanded (a cyclic schema) or a ref
/// pointing elsewhere is left verbatim rather than recursing forever.
fn inline_schema_refs(
    value: &mut serde_json::Value,
    defs: &serde_json::Map<String, serde_json::Value>,
    expanding: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(reference)) = map.get("$ref") {
                if let Some(name) = reference.strip_prefix("#/definitions/") {
                    if !expanding.iter().any(|n| n == name) {
                        if let Some(definition) = defs.get(name) {
                            let mut definition = definition.clone();
                            expanding.push(name.to_string());
                            inline_schema_refs(&mut definition, defs, expanding);
                            expanding.pop();
                            *value = definition;
                            return;
                        }
                    }
                    return;
                }
            }
            for child in map.values_mut() {
                inline_schema_refs(child, defs, expanding);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                inline_schema_refs(item, defs, expanding);
            }
        }
        _ => {}
    }
}

/// Structured error payload (never a bare string): `{ error, message }`.
fn json_error(error: &str, message: impl std::fmt::Display) -> String {
    serde_json::json!({ "error": error, "message": message.to_string() }).to_string()
}

impl ServerHandler for ExocortexMcp {
    /// Identify the server and advertise its tool surface during the rmcp
    /// bootstrap. rmcp 0.1.x answers the first initialize request directly
    /// from `get_info`; overriding `initialize` alone is therefore inert.
    fn get_info(&self) -> rmcp::model::ServerInfo {
        rmcp::model::ServerInfo {
            capabilities: rmcp::model::ServerCapabilities {
                tools: Some(rmcp::model::ToolsCapability { list_changed: None }),
                ..Default::default()
            },
            server_info: rmcp::model::Implementation {
                name: "exocortex-mcp-client".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            // D7 (§3.7): producer-neutral, both directions, ~40 words — the
            // protocol-defined server description is the one instruction
            // surface we control without user action.
            instructions: Some("Exocortex typed memory graph. Read with exocortex.search_memories / exocortex.find_related. To write, submit with exocortex.end_session (1-5 typed memories, ≤200-char titles, edges by draft_key or memory id) — it validates locally and explains any rejection. exocortex.preflight_wrapup checks a batch without writing.".into()),
            ..Default::default()
        }
    }

    /// List the registered Functions (§7.12).
    async fn list_tools(
        &self,
        _pagination: rmcp::model::PaginatedRequestParam,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::Error> {
        // H13/M7: the tool catalogue is registry-driven. Every registry
        // function the client executes locally (the interactive-read set)
        // plus the session-capture tool are listed; admin/write ops are
        // backend HTTP-surface only by design.
        let mut tools = vec![
            Self::search_memories_tool_attr(),
            Self::get_memory_tool_attr(),
            Self::find_related_tool_attr(),
            Self::end_session_tool_attr(),
            Self::preflight_wrapup_tool_attr(),
            Self::playbook_version_tool_attr(),
        ];
        for entry in exocortex_ops::entries() {
            let dispatchable = matches!(
                entry.mcp_tool_name,
                "exocortex.search_memories" | "exocortex.get_memory" | "exocortex.find_related"
            );
            if dispatchable && !tools.iter().any(|t| t.name == entry.mcp_tool_name) {
                if let Ok(t) = serde_json::from_value::<rmcp::model::Tool>(serde_json::json!({
                    "name": entry.mcp_tool_name,
                    "description": entry.name,
                })) {
                    tools.push(t);
                }
            }
        }
        Ok(rmcp::model::ListToolsResult {
            next_cursor: None,
            tools: tools.into_iter().map(self_contained).collect(),
        })
    }

    /// Dispatch to the registered Functions. D27: the guard marks the
    /// call in flight so stdin EOF drains it before the process exits.
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParam,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::CallToolResult, rmcp::Error> {
        let _in_flight = self.in_flight.guard();
        let tcc = ToolCallContext::new(self, request, context);
        match tcc.name() {
            "exocortex.search_memories" => Self::search_memories_tool_call(tcc).await,
            "exocortex.get_memory" => Self::get_memory_tool_call(tcc).await,
            "exocortex.find_related" => Self::find_related_tool_call(tcc).await,
            "exocortex.end_session" => Self::end_session_tool_call(tcc).await,
            "exocortex.preflight_wrapup" => Self::preflight_wrapup_tool_call(tcc).await,
            "exocortex.playbook_version" => Self::playbook_version_tool_call(tcc).await,
            _other => Err(rmcp::Error::invalid_params(
                "method not found (backend-only operations are served over HTTP)",
                None,
            )),
        }
    }
}

#[cfg(test)]
mod schema_inline_tests {
    use super::inline_schema_refs;
    use serde_json::json;

    fn object_map(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().expect("object").clone()
    }

    #[test]
    fn refs_resolve_to_their_definitions_depth_first() {
        let defs = object_map(json!({
            "Draft": {
                "type": "object",
                "properties": {
                    "memory_type": { "type": "string" },
                    "inner": { "$ref": "#/definitions/Inner" }
                }
            },
            "Inner": { "type": "string" }
        }));
        let mut schema = json!({
            "type": "object",
            "properties": {
                "memories": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/Draft" }
                }
            }
        });
        inline_schema_refs(&mut schema, &defs, &mut Vec::new());
        assert_eq!(
            schema["properties"]["memories"]["items"]["properties"]["memory_type"],
            json!({ "type": "string" }),
            "one hop resolves: {schema}"
        );
        assert_eq!(
            schema["properties"]["memories"]["items"]["properties"]["inner"],
            json!({ "type": "string" }),
            "nested refs resolve depth-first: {schema}"
        );
        assert!(
            !schema.to_string().contains("$ref"),
            "no refs remain: {schema}"
        );
    }

    #[test]
    fn cyclic_and_foreign_refs_are_left_verbatim() {
        let defs = object_map(json!({
            "A": { "type": "object", "properties": { "b": { "$ref": "#/definitions/A" } } }
        }));
        let mut cyclic = json!({ "$ref": "#/definitions/A" });
        inline_schema_refs(&mut cyclic, &defs, &mut Vec::new());
        // The definition's self-reference stays a ref instead of recursing.
        assert_eq!(
            cyclic["properties"]["b"],
            json!({ "$ref": "#/definitions/A" }),
            "cycle stops expanding: {cyclic}"
        );
        let mut foreign = json!({ "$ref": "https://example.com/other" });
        inline_schema_refs(&mut foreign, &defs, &mut Vec::new());
        assert_eq!(foreign, json!({ "$ref": "https://example.com/other" }));
    }
}
