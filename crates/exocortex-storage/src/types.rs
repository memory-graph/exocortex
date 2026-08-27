// crates/exocortex-storage/src/types.rs
use chrono::{DateTime, Utc};
use exocortex_kernel::{EntityId, MemoryId, RelKindId, RelationshipId, Visibility};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

/// The result of a committed mutation: a monotonic per-graph LSN plus commit
/// metadata (R-S3, CR-15).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CommitRecord {
    /// Monotonic per-graph log sequence number.
    pub lsn: u64,
    /// When the commit landed.
    pub committed_at: DateTime<Utc>,
    /// Backend-assigned node id, when available.
    pub node_id: Option<u64>,
    /// Backend-assigned edge id, when available.
    pub edge_id: Option<u64>,
}

/// Bounded traversal descriptor. Every field carries a hard cap enforced
/// server-side (CR-6: no unbounded traversal).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraversalSpec {
    /// Edge direction to follow.
    pub direction: Direction,
    /// Restrict to these kinds; empty = all kinds.
    pub kinds: SmallVec<[RelKindId; 8]>,
    /// Hard-capped at 4 by the validator.
    pub max_depth: u8,
    /// Hard-capped at 2048.
    pub max_nodes: u32,
    /// Identity + visibility scope for the traversal.
    pub visibility_ctx: VisibilityContext,
    /// Bi-temporal snapshot read, if any.
    pub as_of: Option<DateTime<Utc>>,
}

impl Default for VisibilityContext {
    /// The narrowest possible context: anonymous user, empty org, `Private`
    /// ceiling. Exists so `MemoryFilter: Default` (§6.3) compiles; production
    /// callers always construct a real context.
    fn default() -> Self {
        Self {
            user_id: SmolStr::new_inline(""),
            org_id: SmolStr::new_inline(""),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: Visibility::Private,
        }
    }
}

/// Traversal direction.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Direction {
    /// Follow outgoing edges only.
    Out,
    /// Follow incoming edges only.
    In,
    /// Follow both.
    Both,
}

/// The per-request identity + visibility scope. Every read is filtered by this
/// at the storage boundary; there is no "unfiltered read" surface (CR-22).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisibilityContext {
    /// Calling user.
    pub user_id: SmolStr,
    /// Owning org.
    pub org_id: SmolStr,
    /// Projects the caller belongs to.
    pub project_ids: SmallVec<[SmolStr; 4]>,
    /// Teams the caller belongs to.
    pub team_ids: SmallVec<[SmolStr; 4]>,
    /// Effective ceiling.
    pub max_visibility: Visibility,
}

/// One authoritative §17.2 memory visibility decision, shared by storage and
/// cache. Project/team rows require an explicit row scope and membership;
/// missing scope never widens access.
pub fn memory_visible(memory: &exocortex_kernel::Memory, vc: &VisibilityContext) -> bool {
    if memory
        .context
        .tenant_id
        .as_deref()
        .is_some_and(|tenant| tenant != vc.org_id.as_str())
    {
        return false;
    }
    let effective = match memory.visibility {
        Visibility::Public => Visibility::Org,
        other => other,
    };
    if effective > vc.max_visibility {
        return false;
    }
    match effective {
        Visibility::Private => memory.context.user_id.as_deref() == Some(vc.user_id.as_str()),
        Visibility::Project => memory
            .context
            .project_id
            .as_ref()
            .is_some_and(|project| vc.project_ids.contains(project)),
        Visibility::Team => memory
            .context
            .team_id
            .as_ref()
            .is_some_and(|team| vc.team_ids.contains(team)),
        Visibility::Org | Visibility::Public => true,
    }
}

/// One authoritative relationship visibility decision. An edge is visible
/// only when its own label is within the caller's ceiling and both endpoint
/// memories are visible to that caller; endpoint contexts resolve the label's
/// project, team, private-author, and tenant subjects (§17.2).
pub fn relationship_visible(
    relationship: &exocortex_kernel::Relationship,
    from: &exocortex_kernel::Memory,
    to: &exocortex_kernel::Memory,
    vc: &VisibilityContext,
) -> bool {
    let effective = match relationship.visibility {
        Visibility::Public => Visibility::Org,
        other => other,
    };
    if effective > vc.max_visibility || !memory_visible(from, vc) || !memory_visible(to, vc) {
        return false;
    }
    let endpoints = [from, to];
    match effective {
        Visibility::Private => endpoints.iter().any(|memory| {
            memory.visibility == Visibility::Private
                && memory.context.user_id.as_deref() == Some(vc.user_id.as_str())
        }),
        Visibility::Project => endpoints.iter().any(|memory| {
            memory.visibility == Visibility::Project
                && memory
                    .context
                    .project_id
                    .as_ref()
                    .is_some_and(|project| vc.project_ids.contains(project))
        }),
        Visibility::Team => endpoints.iter().any(|memory| {
            memory.visibility == Visibility::Team
                && memory
                    .context
                    .team_id
                    .as_ref()
                    .is_some_and(|team| vc.team_ids.contains(team))
        }),
        Visibility::Org | Visibility::Public => true,
    }
}

/// Server-side memory filter for point and entity queries.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryFilter {
    /// Restrict to these memory types; empty = all types.
    pub memory_types: SmallVec<[u8; 8]>,
    /// Restrict to memories about any of these entities.
    pub entity_any_of: SmallVec<[EntityId; 8]>,
    /// Restrict to this project.
    pub project_id: Option<SmolStr>,
    /// Restrict to this session.
    pub session_id: Option<SmolStr>,
    /// Bi-temporal read, if any.
    pub valid_at: Option<DateTime<Utc>>,
    /// Hard-capped at 500.
    pub limit: u32,
    /// Identity + visibility scope.
    pub visibility_ctx: VisibilityContext,
}

/// Summary counts for a point-in-time graph reconstruction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphSnapshot {
    /// The as-of instant.
    pub as_of: DateTime<Utc>,
    /// Backend LSN at snapshot time.
    pub backend_lsn: u64,
    /// Memories valid at `as_of`.
    pub memory_count: u64,
    /// Relationships valid at `as_of`.
    pub relationship_count: u64,
}

/// A registered, parameterized Cypher template invocation (§6.4). The
/// `template_id` must resolve in the compile-time catalogue.
#[derive(Clone, Debug)]
pub struct CypherQuery {
    /// Must match a registered template.
    pub template_id: &'static str,
    /// Parameter values; every `required_params` entry must be present.
    pub params: serde_json::Value,
    /// Whether the caller intends a read-only execution.
    pub read_only: bool,
    /// Deadline for server-side enforcement.
    pub deadline: DateTime<Utc>,
}

/// Rows returned by `Storage::query_cypher`.
#[derive(Clone, Debug)]
pub struct ResultSet {
    /// One JSON value per row.
    pub rows: Vec<serde_json::Value>,
    /// Rows scanned server-side.
    pub scanned_rows: u64,
}

/// Embedding vector alias (backend default: bge-small, 384 dims).
pub type Embedding = SmallVec<[f32; 384]>;

/// Chubby-style lease key. Every owner-only operation names its lease with one
/// of these; a lease holder never runs work outside the key it holds.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LeaseKey {
    /// Dreams cycle owner for one region.
    Dreams {
        /// Owning org.
        org: SmolStr,
        /// Region key string (`{project}:{memory_type}`).
        region: SmolStr,
    },
    /// Retroactive backfill owner.
    Backfill {
        /// Owning org.
        org: SmolStr,
    },
    /// Cleanup cycle owner.
    Cleanup {
        /// Owning org.
        org: SmolStr,
    },
    /// Consolidation owner for one region.
    Consolidation {
        /// Owning org.
        org: SmolStr,
        /// Region key string (`{project}:{memory_type}`).
        region: SmolStr,
    },
}

/// A held owner lease with fencing epoch (§9.2, R-C3).
#[derive(Clone, Debug)]
pub struct OwnerLease {
    /// The lease key held.
    pub key: LeaseKey,
    /// Node holding the lease.
    pub owner_node_id: SmolStr,
    /// Monotonic per key.
    pub epoch: u64,
    /// When the lease was acquired.
    pub acquired_at: DateTime<Utc>,
    /// When the lease expires absent renewal.
    pub expires_at: DateTime<Utc>,
    /// Chubby-style grace window (§9.2).
    pub grace_period: chrono::Duration,
    /// Opaque to callers; echoed on writes as the fencing token.
    pub fencing_token: SmolStr,
}

/// A consolidation region: a stable slice of the org graph (§17.3).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RegionKey {
    /// Owning org.
    pub org: SmolStr,
    /// Project slice.
    pub project: SmolStr,
    /// Memory-type slice.
    pub memory_type: u8,
}

/// A change-feed event backing SSE clients and cache invalidation (§9.1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Invalidation {
    /// A memory row was upserted.
    MemoryUpserted {
        /// The memory's id.
        id: MemoryId,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// A memory was (soft-)deleted.
    MemoryDeleted {
        /// The memory's id.
        id: MemoryId,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// A relationship row was upserted.
    RelationshipUpserted {
        /// The relationship's id.
        id: RelationshipId,
        /// Source memory.
        from: MemoryId,
        /// Target memory.
        to: MemoryId,
        /// Kind of the relationship.
        kind: RelKindId,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// A relationship was (soft-)deleted.
    RelationshipDeleted {
        /// The relationship's id.
        id: RelationshipId,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// Identifier-free LSN advancement substituted for a row that is outside
    /// an authenticated change-feed subscriber's visibility context.
    VisibilityAdvance {
        /// Backend LSN of the hidden commit.
        lsn: u64,
    },
}

/// What a storage backend supports; drives capability-gated code paths.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct StorageCapabilities {
    /// Bi-temporal reads supported.
    pub bi_temporal: bool,
    /// Bulk streaming supported.
    pub streaming: bool,
    /// Owner leases supported.
    pub leases: bool,
    /// Change feed supported.
    pub change_feed: bool,
    /// Maximum traversal depth the backend enforces.
    pub max_traversal_depth: u8,
}

/// Identifies the backend implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageBackendId {
    /// The FalkorDB adapter.
    FalkorDB,
    /// The in-memory test double.
    InMemory,
}

impl Invalidation {
    /// The backend LSN of the commit that produced this invalidation
    /// (CS6: every variant carries one).
    pub fn lsn_of(&self) -> u64 {
        match self {
            Invalidation::MemoryUpserted { lsn, .. }
            | Invalidation::MemoryDeleted { lsn, .. }
            | Invalidation::RelationshipUpserted { lsn, .. }
            | Invalidation::RelationshipDeleted { lsn, .. }
            | Invalidation::VisibilityAdvance { lsn } => *lsn,
        }
    }
}
