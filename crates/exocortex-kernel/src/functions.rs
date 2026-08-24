// functions.rs — typed reads (§7.12). Handler bodies live in exocortex-ops.
use crate::{MemoryId, RelKindId, RelationshipId};
use serde::{Deserialize, Serialize};

/// Typed read surface. Functions are the only way the graph is read on the
/// interactive path; each carries a latency budget enforced by perf CI
/// (§15, R-Lat1).
pub trait Function: Send + Sync + 'static {
    /// Typed input shape.
    type Input: Serialize + for<'de> Deserialize<'de>;
    /// Typed output shape.
    type Output: Serialize + for<'de> Deserialize<'de>;
    /// Stable, human-readable name.
    const NAME: &'static str;
    /// p50 latency budget in microseconds (perf CI, §15, R-Lat1).
    const P50_BUDGET_US: u32;
    /// p99 latency budget in microseconds.
    const P99_BUDGET_US: u32;
}

/// Text + entity + type filters → ranked memory ids. 500µs / 3ms budgets.
pub struct SearchMemories;
/// k-hop typed traversal with visibility filter. 2ms / 10ms budgets.
pub struct TraverseRelationships;
/// Provenance chain for a memory (§10). 1ms / 5ms budgets.
pub struct GetChain;
/// Human-readable proof for a Derived edge (Steel; §10). 1ms / 5ms budgets.
pub struct ExplainEdge;

impl Function for SearchMemories {
    type Input = SearchMemoriesInput;
    type Output = SearchMemoriesOutput;
    const NAME: &'static str = "search_memories";
    const P50_BUDGET_US: u32 = 500;
    const P99_BUDGET_US: u32 = 3_000;
}

impl Function for TraverseRelationships {
    type Input = TraverseRelationshipsInput;
    type Output = TraverseRelationshipsOutput;
    const NAME: &'static str = "traverse_relationships";
    const P50_BUDGET_US: u32 = 2_000;
    const P99_BUDGET_US: u32 = 10_000;
}

impl Function for GetChain {
    type Input = GetChainInput;
    type Output = GetChainOutput;
    const NAME: &'static str = "get_chain";
    const P50_BUDGET_US: u32 = 1_000;
    const P99_BUDGET_US: u32 = 5_000;
}

impl Function for ExplainEdge {
    type Input = ExplainEdgeInput;
    type Output = ExplainEdgeOutput;
    const NAME: &'static str = "explain_edge";
    const P50_BUDGET_US: u32 = 1_000;
    const P99_BUDGET_US: u32 = 5_000;
}

/// Input for the `search_memories` Function.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchMemoriesInput {
    /// Free-text query matched against title/content/tags.
    pub query: String,
    /// Restrict to these memory type ids (empty = all).
    pub memory_types: Vec<u8>,
    /// Restrict to memories about these entities.
    pub entities: Vec<crate::EntityId>,
    /// Maximum number of results (hard-capped server-side).
    pub limit: u32,
}

/// Output for the `search_memories` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchMemoriesOutput {
    /// Ranked matching memories.
    pub memories: Vec<crate::Memory>,
}

/// Input for the `traverse_relationships` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraverseRelationshipsInput {
    /// Traversal start.
    pub from: MemoryId,
    /// Restrict to these kinds (empty = all).
    pub kinds: Vec<RelKindId>,
    /// Hop bound (hard-capped at 4, CR-6).
    pub max_depth: u8,
    /// Node budget (hard-capped at 2048, CR-6).
    pub max_nodes: u32,
}

/// Output for the `traverse_relationships` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraverseRelationshipsOutput {
    /// Memories reached by the bounded traversal.
    pub memories: Vec<crate::Memory>,
}

/// Input for the `get_chain` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetChainInput {
    /// Memory whose provenance chain is requested.
    pub memory: MemoryId,
    /// Depth bound.
    pub max_depth: u8,
}

/// Output for the `get_chain` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetChainOutput {
    /// Chain of memory ids from origin to the requested memory.
    pub chain: Vec<MemoryId>,
}

/// Input for the `explain_edge` Function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExplainEdgeInput {
    /// The derived edge to explain.
    pub edge: RelationshipId,
}

/// Output for the `explain_edge` Function: a structured Steel-rendered
/// explanation tree naming every input fact (§10).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExplainEdgeOutput {
    /// Structured explanation tree (sexp string rendered by Steel).
    pub tree: String,
}
