use chrono::{DateTime, Utc};
use exocortex_kernel::{
    EntityId, Memory, MemoryId, RelKindId, Relationship, RelationshipId, Visibility,
};
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

/// Durable idempotency identity for one ingestion batch (R6-B09).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IngestBatchKey {
    /// Owning organization.
    pub org_id: SmolStr,
    /// Authenticated producer.
    pub producer_id: SmolStr,
    /// Producer-assigned stable batch identifier.
    pub batch_id: SmolStr,
}

/// Settled result retained with an ingestion dedup claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettledIngestBatch {
    /// Number of accepted rows reported by the original commit.
    pub accepted: u32,
    /// Number of rejected rows reported by the original commit.
    pub rejected: u32,
    /// Last LSN assigned to the atomic batch.
    pub assigned_lsn: u64,
}

/// One region's durable post-ingest work delta.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngestRegionDelta {
    /// Exact Dreams region affected by the committed batch.
    pub region: RegionKey,
    /// Newly committed session memories in this region.
    pub memories: u32,
    /// Newly committed relationship rows in this region.
    pub relationships: u32,
}

/// Durable post-commit work emitted atomically with ingest settlement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostIngestEffect {
    /// Stable retry identity derived from the ingest batch identity.
    pub effect_id: SmolStr,
    /// Session-memory identities reasoning must evaluate.
    #[serde(default)]
    pub session_memory_ids: Vec<MemoryId>,
    /// Exact regional write deltas delivered to Dreams.
    #[serde(default)]
    pub region_deltas: Vec<IngestRegionDelta>,
}

/// Result of the atomic dedup-claim plus graph commit boundary.
#[derive(Clone, Debug)]
pub enum IngestCommitOutcome {
    /// This caller claimed the key and committed every supplied row.
    Committed {
        /// Per-row commit records in input order.
        records: Vec<CommitRecord>,
        /// Durable settled replay result.
        settled: SettledIngestBatch,
    },
    /// The same key was already settled; no supplied row was evaluated or
    /// written and the original result is returned.
    Duplicate(SettledIngestBatch),
}

/// Exact semantic identities committed by one fenced owner batch.
///
/// The identity maps let an owner journal the backend LSN of every row it
/// actually wrote, including storage-materialized inverse relationships.
#[derive(Clone, Debug, Default)]
pub struct FencedBatchCommit {
    /// Ordinary per-row commit metadata.
    pub records: Vec<CommitRecord>,
    /// Backend LSN assigned to each memory written by the batch.
    pub memory_lsns: std::collections::BTreeMap<MemoryId, std::collections::BTreeSet<u64>>,
    /// Backend LSN assigned to each relationship written by the batch.
    pub relationship_lsns:
        std::collections::BTreeMap<RelationshipId, std::collections::BTreeSet<u64>>,
}

/// Exact semantic preimage and cycle-created rows for one fenced rollback.
/// Restored rows receive fresh backend LSNs; every other field is restored
/// verbatim. Created ids are physically removed rather than soft-closed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FencedRestore {
    /// Pre-existing memories changed by the failed owner cycle.
    pub memories: Vec<Memory>,
    /// Pre-existing relationships changed by the failed owner cycle.
    pub relationships: Vec<Relationship>,
    /// Memory rows absent before, but created by, this cycle.
    pub created_memories: Vec<Memory>,
    /// Relationship rows absent before, but created by, this cycle.
    pub created_relationships: Vec<Relationship>,
    /// Exact cycle-written memory version that may be compensated.
    #[serde(with = "memory_lsn_map")]
    pub owned_memory_lsns: std::collections::BTreeMap<MemoryId, std::collections::BTreeSet<u64>>,
    /// Exact cycle-written relationship version that may be compensated.
    #[serde(with = "relationship_lsn_map")]
    pub owned_relationship_lsns:
        std::collections::BTreeMap<RelationshipId, std::collections::BTreeSet<u64>>,
}

mod memory_lsn_map {
    use super::*;

    pub fn serialize<S>(
        value: &std::collections::BTreeMap<MemoryId, std::collections::BTreeSet<u64>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<std::collections::BTreeMap<MemoryId, std::collections::BTreeSet<u64>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rows = Vec::<(MemoryId, std::collections::BTreeSet<u64>)>::deserialize(deserializer)?;
        Ok(rows.into_iter().collect())
    }
}

mod relationship_lsn_map {
    use super::*;

    pub fn serialize<S>(
        value: &std::collections::BTreeMap<RelationshipId, std::collections::BTreeSet<u64>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<std::collections::BTreeMap<RelationshipId, std::collections::BTreeSet<u64>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rows =
            Vec::<(RelationshipId, std::collections::BTreeSet<u64>)>::deserialize(deserializer)?;
        Ok(rows.into_iter().collect())
    }
}

impl FencedRestore {
    /// Merge another prepared/committed cycle fragment into this journal.
    pub fn merge(&mut self, other: &Self) {
        for memory in &other.memories {
            if !self
                .memories
                .iter()
                .any(|existing| existing.id == memory.id)
            {
                self.memories.push(memory.clone());
            }
        }
        for relationship in &other.relationships {
            if !self
                .relationships
                .iter()
                .any(|existing| existing.id == relationship.id)
            {
                self.relationships.push(relationship.clone());
            }
        }
        for memory in &other.created_memories {
            if !self
                .created_memories
                .iter()
                .any(|existing| existing.id == memory.id)
            {
                self.created_memories.push(memory.clone());
            }
        }
        for relationship in &other.created_relationships {
            if !self
                .created_relationships
                .iter()
                .any(|existing| existing.id == relationship.id)
            {
                self.created_relationships.push(relationship.clone());
            }
        }
        for (id, lsns) in &other.owned_memory_lsns {
            self.owned_memory_lsns.entry(*id).or_default().extend(lsns);
        }
        for (id, lsns) in &other.owned_relationship_lsns {
            self.owned_relationship_lsns
                .entry(*id)
                .or_default()
                .extend(lsns);
        }
    }
}

/// Durable state of a fenced owner cycle journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CycleJournalState {
    /// The cycle may require successor recovery.
    Active,
    /// Recovery or the normal cycle completed; retained as an idempotent tombstone.
    Completed,
    /// The whole cycle, including durable discoveries, settled successfully.
    Succeeded,
}

/// Durable rollback material for one fenced owner cycle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CycleJournalRecord {
    /// Stable cycle identity supplied by the owner.
    pub cycle_id: SmolStr,
    /// Lease scope whose successor is allowed to recover this cycle.
    pub lease_key: LeaseKey,
    /// Epoch that originally created the journal.
    pub lease_epoch: u64,
    /// Exact preimages, created identities, and cycle-owned backend LSNs.
    pub restore: FencedRestore,
    /// Recovery state.
    pub state: CycleJournalState,
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

/// Immutable server-issued discovery proposal. Acceptance must match every
/// field and the exact caller scope that received the proposal (R6-B05).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryProposal {
    /// Opaque server-issued discovery identifier.
    pub discovery_id: SmolStr,
    /// Exact graph region in which the proposal was discovered.
    pub region: RegionKey,
    /// Proposed source endpoint.
    pub from: MemoryId,
    /// Proposed destination endpoint.
    pub to: MemoryId,
    /// Proposed relationship kind.
    pub kind: RelKindId,
    /// Maximum visibility the accepted edge may carry.
    pub proposed_visibility: Visibility,
    /// Exact caller and authorization scope to which this proposal was issued.
    pub caller_scope: VisibilityContext,
    /// Server timestamp at issuance.
    pub issued_at: DateTime<Utc>,
}

/// Durable, unasserted candidate emitted by a Dreams discovery cycle. This is
/// presentation state only: it cannot become an edge until a caller receives
/// an immutable [`DiscoveryProposal`] and explicitly accepts it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRecord {
    /// Opaque discovery identity.
    pub discovery_id: SmolStr,
    /// Region in which the candidate was found.
    pub region: RegionKey,
    /// Candidate source endpoint.
    pub from: MemoryId,
    /// Candidate destination endpoint.
    pub to: MemoryId,
    /// Stable finder name.
    pub discovery_type: SmolStr,
    /// Quality stamped once by the finder.
    pub quality: f32,
    /// Relationship kinds on the supporting two-hop path.
    pub via_types: [u32; 2],
    /// Dreams cycle that emitted the candidate.
    pub discovery_cycle_id: SmolStr,
    /// Server timestamp at discovery.
    pub discovered_at: DateTime<Utc>,
}

/// Audit payload coupled to a protected mutation by storage (R6-B18).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Stable action name.
    pub action: SmolStr,
    /// Authenticated actor identity.
    pub actor: SmolStr,
    /// Owning organization.
    pub org_id: SmolStr,
    /// Digest of the canonical action input.
    pub input_digest: [u8; 32],
    /// Identifiers produced by the action.
    pub output_ids: SmallVec<[SmolStr; 8]>,
    /// Ontology fingerprint at execution.
    pub fingerprint: [u8; 32],
    /// Owner lease epoch, when applicable.
    pub lease_epoch: Option<u64>,
    /// Time the action was recorded.
    pub recorded_at: DateTime<Utc>,
}

/// Atomic discovery acceptance request.
#[derive(Clone, Debug)]
pub struct DiscoveryAcceptance {
    /// Exact proposal identifier.
    pub discovery_id: SmolStr,
    /// Exact issuing region.
    pub region: RegionKey,
    /// Current authenticated caller scope.
    pub caller_scope: VisibilityContext,
    /// Asserted edge built from the immutable proposal.
    pub relationship: exocortex_kernel::Relationship,
    /// Required immutable audit event.
    pub audit: AuditEvent,
}

/// One authoritative §17.2 memory visibility decision, shared by storage and
/// cache. Project/team rows require an explicit row scope and membership;
/// missing scope never widens access.
pub fn memory_visible(memory: &exocortex_kernel::Memory, vc: &VisibilityContext) -> bool {
    if memory.context.tenant_id.as_deref() != Some(vc.org_id.as_str()) {
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
    /// A durable, unasserted Dreams discovery became available. This is
    /// presentation state only; it never implies an asserted relationship.
    DiscoveryAvailable {
        /// Complete durable discovery record, visibility-filtered above the
        /// storage seam before external delivery.
        record: DiscoveryRecord,
        /// Backend LSN allocated by the persistence commit.
        lsn: u64,
    },
    /// Identifier-free LSN advancement substituted for a row that is outside
    /// an authenticated change-feed subscriber's visibility context.
    VisibilityAdvance {
        /// Backend LSN of the hidden commit.
        lsn: u64,
    },
    /// A hydrated memory upsert carried by the client-facing SSE feed.
    MemorySnapshotUpserted {
        /// Complete row, already visibility-filtered by the server.
        memory: Box<exocortex_kernel::Memory>,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// A hydrated relationship upsert carried by the client-facing SSE feed.
    RelationshipSnapshotUpserted {
        /// Complete row, already visibility-filtered by the server.
        relationship: Box<exocortex_kernel::Relationship>,
        /// Backend LSN of the commit.
        lsn: u64,
    },
    /// A serialized, caller-filtered full graph image for SSE reseeding.
    GraphReseed {
        /// JSON payload interpreted by the client sync layer.
        snapshot_json: Vec<u8>,
        /// Backend frontier represented by the image.
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
            | Invalidation::DiscoveryAvailable { lsn, .. }
            | Invalidation::VisibilityAdvance { lsn }
            | Invalidation::MemorySnapshotUpserted { lsn, .. }
            | Invalidation::RelationshipSnapshotUpserted { lsn, .. }
            | Invalidation::GraphReseed { lsn, .. } => *lsn,
        }
    }
}

/// Encode one Redis change-feed payload. `DiscoveryAvailable` deliberately
/// rides an old `MemoryUpserted` envelope with an additive field: pre-Round-6
/// consumers ignore the unknown field, refresh an existing endpoint, and still
/// advance their LSN; current consumers recover the complete discovery event.
pub(crate) fn encode_feed_invalidation(
    invalidation: &Invalidation,
) -> Result<String, serde_json::Error> {
    if let Invalidation::DiscoveryAvailable { record, lsn } = invalidation {
        let mut compatible = serde_json::to_value(Invalidation::MemoryUpserted {
            id: record.from,
            lsn: *lsn,
        })?;
        let fields = compatible
            .get_mut("MemoryUpserted")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| serde::ser::Error::custom("invalid compatibility envelope"))?;
        fields.insert("discovery_available".into(), serde_json::to_value(record)?);
        serde_json::to_string(&compatible)
    } else {
        serde_json::to_string(invalidation)
    }
}

/// Decode a Redis change-feed payload, including the additive compatibility
/// envelope emitted for discoveries during rolling upgrades.
pub(crate) fn decode_feed_invalidation(payload: &str) -> Result<Invalidation, serde_json::Error> {
    use serde::de::Error as _;

    let value: serde_json::Value = serde_json::from_str(payload)?;
    if let Some(fields) = value
        .get("MemoryUpserted")
        .and_then(serde_json::Value::as_object)
    {
        if let Some(discovery) = fields.get("discovery_available") {
            let record = serde_json::from_value(discovery.clone())?;
            let lsn = fields
                .get("lsn")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| serde_json::Error::custom("discovery envelope has invalid LSN"))?;
            return Ok(Invalidation::DiscoveryAvailable { record, lsn });
        }
    }
    serde_json::from_value(value)
}

#[cfg(test)]
mod feed_compatibility_tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    enum LegacyInvalidation {
        MemoryUpserted {
            id: MemoryId,
            lsn: u64,
        },
        MemoryDeleted {
            id: MemoryId,
            lsn: u64,
        },
        RelationshipUpserted {
            id: RelationshipId,
            from: MemoryId,
            to: MemoryId,
            kind: RelKindId,
            lsn: u64,
        },
        RelationshipDeleted {
            id: RelationshipId,
            lsn: u64,
        },
    }

    #[test]
    fn discovery_feed_payload_preserves_legacy_progress_and_current_semantics() {
        let record = DiscoveryRecord {
            discovery_id: "rolling-discovery".into(),
            region: RegionKey {
                org: "org".into(),
                project: "project".into(),
                memory_type: 3,
            },
            from: MemoryId([1; 16]),
            to: MemoryId([2; 16]),
            discovery_type: "transitive".into(),
            quality: 0.75,
            via_types: [1, 2],
            discovery_cycle_id: "cycle".into(),
            discovered_at: Utc::now(),
        };
        let current = Invalidation::DiscoveryAvailable {
            record: record.clone(),
            lsn: 42,
        };
        let payload = encode_feed_invalidation(&current).unwrap();

        match serde_json::from_str::<LegacyInvalidation>(&payload).unwrap() {
            LegacyInvalidation::MemoryUpserted { id, lsn } => {
                assert_eq!(id, record.from);
                assert_eq!(lsn, 42);
            }
            other => panic!("unexpected legacy compatibility event: {other:?}"),
        }
        match decode_feed_invalidation(&payload).unwrap() {
            Invalidation::DiscoveryAvailable {
                record: decoded,
                lsn,
            } => {
                assert_eq!(decoded, record);
                assert_eq!(lsn, 42);
            }
            other => panic!("unexpected current event: {other:?}"),
        }
    }
}
