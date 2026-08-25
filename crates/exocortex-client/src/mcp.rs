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
    /// Effective ontology (draft→memory resolution on the offline path).
    ontology: Option<Arc<exocortex_kernel::Ontology>>,
}

impl ExocortexMcp {
    /// Build the server over a cache.
    pub fn new(org: smol_str::SmolStr, cache: Arc<LocalCache>, vc: VisibilityContext) -> Self {
        Self {
            org,
            cache,
            vc,
            end_session: None,
            wal: None,
            ontology: None,
        }
    }

    /// Attach the gRPC-backed `end_session` tool (built from `--backend`).
    pub fn with_end_session(mut self, tool: Arc<EndSessionTool>) -> Self {
        self.end_session = Some(tool);
        self
    }

    /// Attach the offline WAL + ontology (M3 offline write path).
    pub fn with_offline_wal(
        mut self,
        wal: Arc<wal::Wal>,
        ontology: Arc<exocortex_kernel::Ontology>,
    ) -> Self {
        self.wal = Some(wal);
        self.ontology = Some(ontology);
        self
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

/// Output for `exocortex.search_memories`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SearchMemoriesOutput {
    /// Ranked memories.
    pub memories: Vec<ScoredMemory>,
    /// Read stamp (R-M7): local + backend LSN frontiers.
    pub snapshot_version: SnapshotVersion,
}

/// One scored memory.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ScoredMemory {
    /// The memory id (hex).
    pub id: String,
    /// Title.
    pub title: String,
    /// Memory type name in the effective ontology.
    pub memory_type: String,
    /// §14.1 relevance score.
    pub score: f32,
}

/// Snapshot version stamp (R-M7).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SnapshotVersion {
    /// Local WAL frontier.
    pub local_lsn: u64,
    /// Backend commits observed.
    pub backend_lsn: u64,
}

impl ExocortexMcp {
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
        let limit = limit.unwrap_or(20).min(500);
        let hits = self.cache.search(&self.org, &query, limit, &self.vc);
        let memories = hits
            .into_iter()
            .map(|(m, score)| ScoredMemory {
                id: {
                    use std::fmt::Write as _;
                    let mut hex = String::with_capacity(32);
                    for b in m.id.0 {
                        let _ = write!(hex, "{b:02x}");
                    }
                    hex
                },
                title: m.title.to_string(),
                memory_type: format!("type:{}", m.memory_type),
                score,
            })
            .collect();
        let version = self
            .cache
            .version(&self.org)
            .unwrap_or(exocortex_cache::CacheVersion {
                local_lsn: 0,
                backend_lsn: 0,
                published_at: std::time::Instant::now(),
            });
        let out = SearchMemoriesOutput {
            memories,
            snapshot_version: SnapshotVersion {
                local_lsn: version.local_lsn,
                backend_lsn: version.backend_lsn,
            },
        };
        serde_json::to_string(&out).map_err(|e| e.to_string())
    }

    /// `exocortex.traverse_relationships` — wired at M4/M7.
    #[tool(
        name = "exocortex.traverse_relationships",
        description = "Bounded k-hop typed traversal (arrives with the reasoning milestone)."
    )]
    pub async fn traverse_relationships_stub(&self) -> Result<String, String> {
        Err("not implemented until M4/M7".to_string())
    }

    /// `exocortex.get_chain` — wired at M4/M7.
    #[tool(
        name = "exocortex.get_chain",
        description = "Provenance chain for a memory (arrives with the reasoning milestone)."
    )]
    pub async fn get_chain_stub(&self) -> Result<String, String> {
        Err("not implemented until M4/M7".to_string())
    }

    /// `exocortex.explain_edge` — wired at M4.
    #[tool(
        name = "exocortex.explain_edge",
        description = "Structured proof for a derived edge (arrives with the reasoning milestone)."
    )]
    pub async fn explain_edge_stub(&self) -> Result<String, String> {
        Err("not implemented until M4".to_string())
    }

    /// `exocortex.end_session` (§13.6): wrapup batch submit. Online: gRPC
    /// Submit to the backend IngestService. Offline: WAL append with
    /// `{ local_lsns, sync_pending: true }` (§5.2).
    #[tool(
        name = "exocortex.end_session",
        description = "Submit a session wrapup: 1-5 memory drafts plus optional edge hints between them. Requires session_id and project_id."
    )]
    pub async fn end_session(
        &self,
        #[tool(param)] session_id: String,
        #[tool(param)] project_id: String,
        #[tool(param)] memories: Vec<MemoryDraftInput>,
        #[tool(param)] edges: Vec<EdgeHintInput>,
    ) -> Result<String, String> {
        let args = EndSessionArgs {
            session_id,
            project_id,
            memories,
            edges,
        };
        if let Some(tool) = &self.end_session {
            let ack = tool.handle(args).await.map_err(|e| e.to_string())?;
            return serde_json::to_string(&ack).map_err(|e| e.to_string());
        }
        if let (Some(wal), Some(ontology)) = (&self.wal, &self.ontology) {
            return self
                .end_session_offline(wal, ontology, args)
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
        if args.memories.is_empty() || args.memories.len() > 5 {
            return Err(json_error("invalid-params", "memories: expected 1..=5"));
        }
        let now = chrono::Utc::now();
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
            let id = MemoryId::new_v7();
            let draft = MemoryDraft {
                memory_type,
                title: m.title.into(),
                content: m.content,
                summary: None,
                visibility,
                context: exocortex_kernel::MemoryContext {
                    timestamp: now,
                    project_id: Some(args.project_id.clone().into()),
                    project_path: None,
                    team_id: None,
                    tenant_id: None,
                    session_id: Some(args.session_id.clone().into()),
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
                },
                edge_hints: Default::default(),
                external_key: None,
            };
            ids.push((m.draft_key, id));
            drafts.push(draft);
        }
        // Edge hints become typed hints against the assigned ids, attached to
        // the draft named by from_draft_key.
        for e in &args.edges {
            let Some(src) = ids.iter().position(|(k, _)| *k == e.from_draft_key) else {
                return Err(json_error(
                    "invalid-params",
                    format!("edge references unknown draft_key `{}`", e.from_draft_key),
                ));
            };
            let Some((_, to)) = ids.iter().find(|(k, _)| *k == e.to_draft_key) else {
                return Err(json_error(
                    "invalid-params",
                    format!("edge references unknown draft_key `{}`", e.to_draft_key),
                ));
            };
            let Some(kind) = ontology.kind_id(&e.kind) else {
                return Err(json_error(
                    "unknown-kind",
                    format!("unknown relationship kind `{}`", e.kind),
                ));
            };
            drafts[src].edge_hints.push(exocortex_kernel::EdgeHint {
                kind,
                to: *to,
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
            .append_batch(&args.session_id, drafts, memory_ids)
            .map_err(|e| json_error("wal-error", e.to_string()))?;
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
}

/// Structured error payload (never a bare string): `{ error, message }`.
fn json_error(error: &str, message: impl std::fmt::Display) -> String {
    serde_json::json!({ "error": error, "message": message.to_string() }).to_string()
}

impl ServerHandler for ExocortexMcp {
    /// Initialize: identify the server to the harness.
    async fn initialize(
        &self,
        _request: rmcp::model::InitializeRequestParam,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::InitializeResult, rmcp::Error> {
        Ok(rmcp::model::InitializeResult {
            capabilities: rmcp::model::ServerCapabilities {
                tools: Some(rmcp::model::ToolsCapability { list_changed: None }),
                ..Default::default()
            },
            server_info: rmcp::model::Implementation {
                name: "exocortex-mcp-client".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            instructions: Some("Exocortex local memory graph. Call exocortex.search_memories to query the org graph.".into()),
            ..Default::default()
        })
    }

    /// List the registered Functions (§7.12).
    async fn list_tools(
        &self,
        _pagination: rmcp::model::PaginatedRequestParam,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::Error> {
        Ok(rmcp::model::ListToolsResult {
            next_cursor: None,
            tools: vec![
                Self::search_memories_tool_attr(),
                Self::traverse_relationships_stub_tool_attr(),
                Self::get_chain_stub_tool_attr(),
                Self::explain_edge_stub_tool_attr(),
                Self::end_session_tool_attr(),
            ],
        })
    }

    /// Dispatch to the registered Functions.
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParam,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::CallToolResult, rmcp::Error> {
        let tcc = ToolCallContext::new(self, request, context);
        match tcc.name() {
            "exocortex.search_memories" => Self::search_memories_tool_call(tcc).await,
            "exocortex.traverse_relationships" => {
                Self::traverse_relationships_stub_tool_call(tcc).await
            }
            "exocortex.get_chain" => Self::get_chain_stub_tool_call(tcc).await,
            "exocortex.explain_edge" => Self::explain_edge_stub_tool_call(tcc).await,
            "exocortex.end_session" => Self::end_session_tool_call(tcc).await,
            _other => Err(rmcp::Error::invalid_params("method not found", None)),
        }
    }
}
