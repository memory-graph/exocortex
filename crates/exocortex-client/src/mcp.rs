//! The stdio MCP surface (§13.5 arrives at M6). M3 exposes `search_memories`
//! (Function `SearchMemories`, 500µs/3ms budgets); the remaining Functions are
//! registered as stubs that return a structured not-implemented error rather
//! than panicking — a panicking tool would tear down the stdio server.

use rmcp::handler::server::tool::ToolCallContext;
use rmcp::{tool, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use exocortex_cache::LocalCache;
use exocortex_storage::VisibilityContext;

/// Shared server state: the local cache plus the caller's fixed visibility
/// context (v1: one user per client process, §17).
#[derive(Clone)]
pub struct ExocortexMcp {
    org: smol_str::SmolStr,
    cache: Arc<LocalCache>,
    vc: VisibilityContext,
}

impl ExocortexMcp {
    /// Build the server over a cache.
    pub fn new(org: smol_str::SmolStr, cache: Arc<LocalCache>, vc: VisibilityContext) -> Self {
        Self { org, cache, vc }
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
            _other => Err(rmcp::Error::invalid_params("method not found", None)),
        }
    }
}
