// trait_.rs — the storage seam. Full signature in §6.1; support types in §6.3.
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::stream::BoxStream;

use exocortex_kernel::{EntityId, Memory, MemoryId, Relationship, RelationshipId};

use crate::types::{
    AuditEvent, CommitRecord, CypherQuery, DiscoveryAcceptance, DiscoveryProposal, Embedding,
    FencedRestore, GraphSnapshot, IngestBatchKey, IngestCommitOutcome, Invalidation, LeaseKey,
    MemoryFilter, OwnerLease, RegionKey, ResultSet, StorageBackendId, StorageCapabilities,
    TraversalSpec,
};

/// Errors surfaced by every `Storage` implementation.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Backend/driver failure, with detail.
    #[error("backend error: {0}")]
    Backend(String),
    /// The caller may not read this row (R-MT4: distinct from NotFound —
    /// the row exists but is outside the caller's visibility).
    #[error("permission denied")]
    PermissionDenied,
    /// The runtime ontology fingerprint does not match the one pinned in
    /// storage (R-D5). Startup must fail fast.
    #[error("ontology fingerprint mismatch: storage={storage:?} runtime={runtime:?}")]
    FingerprintMismatch {
        /// Fingerprint persisted in the backend.
        storage: [u8; 32],
        /// Fingerprint of the running process.
        runtime: [u8; 32],
    },
    /// Persisted storage metadata exists but cannot be decoded safely.
    #[error("corrupt storage metadata `{key}`: {detail}")]
    CorruptMetadata {
        /// Stable metadata key.
        key: &'static str,
        /// Precise incompatibility/corruption detail.
        detail: String,
    },
    /// A write made under a lease that is no longer current (R-C3 fencing
    /// token): the lease expired or was re-acquired by another owner. The
    /// write is rejected before any row commits.
    #[error("fenced write rejected: lease epoch {lease_epoch} is stale")]
    FencedWriteRejected {
        /// Epoch of the stale lease presented with the write.
        lease_epoch: u64,
    },
    /// No unconsumed server-issued proposal exists for the supplied id.
    #[error("discovery proposal not found")]
    ProposalNotFound,
    /// Proposal fields or caller scope do not exactly match the persisted
    /// server-issued record.
    #[error("discovery proposal scope or fields do not match")]
    ProposalMismatch,
}

/// The one deliberate seam (§6.0): every subsystem above storage depends on
/// this trait, never on FalkorDB. Cypher stays inside implementations (CR-10).
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    // ---- Memory + relationship writes ----

    /// Upsert one memory row; returns the commit record with a monotonic
    /// backend LSN (R-S3).
    async fn upsert_memory(&self, m: &Memory) -> crate::Result<CommitRecord>;
    /// Atomically upsert a batch of memories and relationships; on any
    /// per-row failure the whole batch fails (R-T17).
    async fn upsert_batch(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> crate::Result<Vec<CommitRecord>>;
    /// Soft-delete a memory: close `valid_until`, never remove the node.
    async fn delete_memory(&self, id: &MemoryId) -> crate::Result<CommitRecord>;
    /// Upsert one relationship row (DELETE-then-CREATE, R-S2).
    async fn upsert_relationship(&self, r: &Relationship) -> crate::Result<CommitRecord>;
    /// Soft-delete a relationship: close `valid_until`.
    async fn delete_relationship(&self, id: &RelationshipId) -> crate::Result<CommitRecord>;
    /// Atomically claim one ingestion idempotency key, commit every supplied
    /// row, and settle the replay result. A crash/failure before commit leaves
    /// no claim; a retry may claim. Once settled, retries return the original
    /// result without evaluating or writing rows (R6-B09/R6-B24).
    async fn commit_ingest_batch(
        &self,
        key: &IngestBatchKey,
        memories: &[Memory],
        relationships: &[Relationship],
        accepted: u32,
    ) -> crate::Result<IngestCommitOutcome> {
        let _ = (key, memories, relationships, accepted);
        Err(StorageError::Backend("atomic ingest unsupported".into()))
    }
    /// Atomically upsert a protected memory mutation and its required audit
    /// event. Neither may commit without the other (R6-B18).
    async fn upsert_memory_audited(
        &self,
        memory: &Memory,
        audit: &AuditEvent,
    ) -> crate::Result<CommitRecord> {
        let _ = (memory, audit);
        Err(StorageError::Backend(
            "audited mutations unsupported".into(),
        ))
    }
    /// Persist one immutable server-issued discovery proposal. Reissuing the
    /// same id is idempotent only when every scoped field is identical.
    async fn create_discovery_proposal(&self, proposal: &DiscoveryProposal) -> crate::Result<()> {
        let _ = proposal;
        Err(StorageError::Backend(
            "discovery proposals unsupported".into(),
        ))
    }
    /// Load an unconsumed proposal by id for validation and edge construction.
    async fn get_discovery_proposal(
        &self,
        discovery_id: &str,
    ) -> crate::Result<Option<DiscoveryProposal>> {
        let _ = discovery_id;
        Err(StorageError::Backend(
            "discovery proposals unsupported".into(),
        ))
    }
    /// Atomically consume a matching proposal, write its asserted edge, and
    /// append the required audit event. Any failure leaves all three unchanged.
    async fn accept_discovery(
        &self,
        acceptance: &DiscoveryAcceptance,
    ) -> crate::Result<CommitRecord> {
        let _ = acceptance;
        Err(StorageError::Backend(
            "discovery acceptance unsupported".into(),
        ))
    }
    /// Read durable audit rows for one organization after an LSN.
    async fn audit_range(
        &self,
        org_id: &str,
        since_lsn: u64,
        limit: u32,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let _ = (org_id, since_lsn, limit);
        Err(StorageError::Backend("audit storage unsupported".into()))
    }

    // ---- Reads (interactive path) ----

    /// Point read at the historical `Visibility::Org` ceiling (kept for
    /// internal paths that re-check visibility above the seam).
    async fn get_memory(&self, id: &MemoryId) -> crate::Result<Option<Memory>>;
    /// Caller-visibility point read (R-MT4): filters at the caller's
    /// ceiling AND resolves `Private` against `vc.user_id`; a row the
    /// caller may not see yields `Err(StorageError::PermissionDenied)`
    /// (distinguishable from a missing row's `Ok(None)`).
    async fn get_memory_for(
        &self,
        id: &MemoryId,
        vc: &crate::VisibilityContext,
    ) -> crate::Result<Option<Memory>>;
    /// Point read of several memories; missing ids are omitted.
    async fn get_memories(&self, ids: &[MemoryId]) -> crate::Result<Vec<Memory>>;
    /// Indexed point read of one current relationship row.
    async fn get_relationship(&self, id: &RelationshipId) -> crate::Result<Option<Relationship>> {
        use futures::StreamExt;
        let mut rows = self.stream_all_relationships().await;
        while let Some(row) = rows.next().await {
            let row = row?;
            if row.id == *id {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
    /// Bounded relationship rows touching any node in `frontier`. Production
    /// backends implement this with endpoint indexes; the default preserves
    /// compatibility for specialized test stores.
    async fn relationships_touching(
        &self,
        frontier: &[MemoryId],
        limit: u32,
    ) -> crate::Result<Vec<Relationship>> {
        use futures::StreamExt;
        let ids: std::collections::HashSet<_> = frontier.iter().copied().collect();
        let mut rows = self.stream_all_relationships().await;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await {
            let row = row?;
            if ids.contains(&row.from) || ids.contains(&row.to) {
                out.push(row);
                if out.len() >= limit as usize {
                    break;
                }
            }
        }
        Ok(out)
    }
    /// Bounded attribute-posting expansion for reasoning rules R7/R9.
    async fn memories_sharing_attributes(
        &self,
        tags: &[smol_str::SmolStr],
        entities: &[EntityId],
        limit: u32,
    ) -> crate::Result<Vec<Memory>> {
        use futures::StreamExt;
        let mut rows = self.stream_all_memories().await;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await {
            let row = row?;
            if row.tags.iter().any(|tag| tags.contains(tag))
                || row
                    .context
                    .entities
                    .iter()
                    .any(|entity| entities.contains(entity))
            {
                out.push(row);
                if out.len() >= limit as usize {
                    break;
                }
            }
        }
        Ok(out)
    }
    /// Bounded k-hop traversal (CR-6 hard caps).
    async fn traverse(&self, from: &MemoryId, spec: &TraversalSpec) -> crate::Result<Vec<Memory>>;
    /// Memories about an entity, filtered and bounded.
    async fn find_by_entity(
        &self,
        entity: &EntityId,
        filter: &MemoryFilter,
    ) -> crate::Result<Vec<Memory>>;

    // ---- Bi-temporal ----

    /// Count summary of the graph state valid at `t` (CR-4).
    async fn get_state_at(&self, t: DateTime<Utc>) -> crate::Result<GraphSnapshot>;
    /// Bi-temporal point read: the memory row valid at `at`.
    async fn valid_at(&self, id: &MemoryId, at: DateTime<Utc>) -> crate::Result<Option<Memory>>;

    // ---- Bulk / streaming (Dreams, backfill) ----

    /// Execute a registered Cypher template (§6.4); unregistered templates
    /// are refused. Cypher never leaks to callers (CR-10).
    async fn query_cypher(&self, q: &CypherQuery) -> crate::Result<ResultSet>;
    /// Stream every memory row (current versions).
    async fn stream_all_memories(&self) -> BoxStream<'_, crate::Result<Memory>>;
    /// Stream every relationship row (current versions).
    async fn stream_all_relationships(&self) -> BoxStream<'_, crate::Result<Relationship>>;

    // ---- Offline similarity (Dreams / ingest enrichment ONLY) ----
    // Never called on the interactive path. See principle 6 (§0.4).

    /// Offline k-NN over stored embeddings (Dreams-only, R-Mcr4).
    async fn find_similar_offline(
        &self,
        query: &Embedding,
        k: usize,
        filter: &MemoryFilter,
    ) -> crate::Result<Vec<(MemoryId, f32)>>;

    // ---- Leases + fencing (called from cluster code; §9.2) ----

    /// Acquire a Chubby-style owner lease: `SET NX EX` plus a monotonic
    /// fencing epoch (R-C1, R-C3).
    async fn acquire_lease(
        &self,
        key: &LeaseKey,
        ttl: std::time::Duration,
    ) -> crate::Result<OwnerLease>;
    /// Renew a held lease; errors if the lease was lost.
    async fn renew_lease(&self, lease: &OwnerLease) -> crate::Result<OwnerLease>;
    /// Release a held lease; safe to call after expiry.
    async fn release_lease(&self, lease: OwnerLease) -> crate::Result<()>;

    // ---- Fenced writes (R-C3: owner-only writes carry the fencing token) ----

    /// [`Storage::upsert_batch`](Self::upsert_batch), but the batch only
    /// commits while `lease` is still the current holder of its lease key.
    /// A stale lease (epoch bumped by re-election, or expiry) rejects with
    /// [`crate::StorageError::FencedWriteRejected`] before any row lands —
    /// the storage-side fencing guarantee for consolidation/Dreams writes.
    async fn upsert_batch_fenced(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        lease: &OwnerLease,
    ) -> crate::Result<Vec<CommitRecord>>;
    /// [`Storage::delete_memory`](Self::delete_memory) under the same
    /// fencing check (a rollback must not delete a newer owner's rows).
    async fn delete_memory_fenced(
        &self,
        id: &MemoryId,
        lease: &OwnerLease,
    ) -> crate::Result<CommitRecord>;
    /// Atomically restore the semantic preimage of an owner cycle and
    /// physically remove only rows that cycle created. The fence check and
    /// complete restore share one storage linearization point (R6-B10).
    async fn restore_fenced(
        &self,
        restore: &FencedRestore,
        lease: &OwnerLease,
    ) -> crate::Result<Vec<CommitRecord>>;

    // ---- Change feed (backs SSE clients; §9.1, §9.6) ----

    /// Subscribe to invalidations for a region (or `"*"` wildcards).
    async fn subscribe_invalidations(
        &self,
        region: &RegionKey,
    ) -> crate::Result<BoxStream<'_, crate::Result<Invalidation>>>;

    // ---- Metadata ----

    /// Liveness probe for R-O4 readiness: a cheap round trip that fails
    /// when the backend is unreachable. Default `Ok(())` for doubles with
    /// no network hop.
    async fn ping(&self) -> crate::Result<()> {
        Ok(())
    }
    /// Backend capability set.
    fn capabilities(&self) -> StorageCapabilities;
    /// Backend identity: `"falkordb" | "in-memory"`.
    fn backend_id(&self) -> StorageBackendId;
    /// The pinned `OntologyFingerprint` (R-T21).
    fn ontology_fingerprint(&self) -> [u8; 32];
}

/// The Chubby-style grace window applied to leases (§9.2).
pub const LEASE_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(15);

/// `owner_grace_period_seconds` default from §16.
pub fn grace_duration() -> Duration {
    Duration::from_std(LEASE_GRACE_PERIOD).expect("15s fits in chrono::Duration")
}
