//! IN10 (audit): a `Storage` that answers "no backend". The MCP read
//! tools dispatch through the ONE registry implementation
//! (`exocortex_ops::entries()` → `entry.handler`), exactly like the HTTP
//! bind — so the registry's `get_memory` fallthrough needs a storage to
//! ask. This one says "nothing there": the client's interactive reads are
//! cache-served (R-C8 fills only from a real backend, wired with the SSE
//! sync work).

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;

use exocortex_kernel::{EntityId, Memory, MemoryId, Relationship, RelationshipId};
use exocortex_storage::{
    types::{
        CommitRecord, CypherQuery, Embedding, FencedRestore, GraphSnapshot, Invalidation, LeaseKey,
        MemoryFilter, OwnerLease, RegionKey, ResultSet, StorageBackendId, StorageCapabilities,
        TraversalSpec,
    },
    Storage, StorageError, VisibilityContext,
};

type R<T> = std::result::Result<T, StorageError>;

fn no_backend<T>() -> R<T> {
    Err(StorageError::Backend("no backend configured".into()))
}

/// A `Storage` that always answers "not here". See the module docs.
#[derive(Default)]
pub struct NoBackendStorage;

#[async_trait::async_trait]
impl Storage for NoBackendStorage {
    async fn upsert_memory(&self, _m: &Memory) -> R<CommitRecord> {
        no_backend()
    }
    async fn upsert_batch(&self, _ms: &[Memory], _rs: &[Relationship]) -> R<Vec<CommitRecord>> {
        no_backend()
    }
    async fn delete_memory(&self, _id: &MemoryId) -> R<CommitRecord> {
        no_backend()
    }
    async fn upsert_relationship(&self, _r: &Relationship) -> R<CommitRecord> {
        no_backend()
    }
    async fn delete_relationship(&self, _id: &RelationshipId) -> R<CommitRecord> {
        no_backend()
    }
    async fn get_memory(&self, _id: &MemoryId) -> R<Option<Memory>> {
        Ok(None)
    }
    async fn get_memory_for(&self, _id: &MemoryId, _vc: &VisibilityContext) -> R<Option<Memory>> {
        Ok(None)
    }
    async fn get_memories(&self, _ids: &[MemoryId]) -> R<Vec<Memory>> {
        Ok(vec![])
    }
    async fn traverse(&self, _from: &MemoryId, _spec: &TraversalSpec) -> R<Vec<Memory>> {
        Ok(vec![])
    }
    async fn find_by_entity(&self, _e: &EntityId, _f: &MemoryFilter) -> R<Vec<Memory>> {
        Ok(vec![])
    }
    async fn get_state_at(&self, _t: DateTime<Utc>) -> R<GraphSnapshot> {
        no_backend()
    }
    async fn valid_at(&self, _id: &MemoryId, _at: DateTime<Utc>) -> R<Option<Memory>> {
        Ok(None)
    }
    async fn query_cypher(&self, _q: &CypherQuery) -> R<ResultSet> {
        no_backend()
    }
    async fn stream_all_memories(&self) -> BoxStream<'_, R<Memory>> {
        Box::pin(futures::stream::iter(std::iter::empty()))
    }
    async fn stream_all_relationships(&self) -> BoxStream<'_, R<Relationship>> {
        Box::pin(futures::stream::iter(std::iter::empty()))
    }
    async fn find_similar_offline(
        &self,
        _q: &Embedding,
        _k: usize,
        _f: &MemoryFilter,
    ) -> R<Vec<(MemoryId, f32)>> {
        Ok(vec![])
    }
    async fn acquire_lease(&self, _key: &LeaseKey, _ttl: std::time::Duration) -> R<OwnerLease> {
        no_backend()
    }
    async fn renew_lease(&self, _lease: &OwnerLease) -> R<OwnerLease> {
        no_backend()
    }
    async fn release_lease(&self, _lease: OwnerLease) -> R<()> {
        no_backend()
    }
    async fn upsert_batch_fenced(
        &self,
        _ms: &[Memory],
        _rs: &[Relationship],
        _lease: &OwnerLease,
    ) -> R<Vec<CommitRecord>> {
        no_backend()
    }
    async fn delete_memory_fenced(&self, _id: &MemoryId, _lease: &OwnerLease) -> R<CommitRecord> {
        no_backend()
    }
    async fn restore_fenced(
        &self,
        _restore: &FencedRestore,
        _lease: &OwnerLease,
    ) -> R<Vec<CommitRecord>> {
        no_backend()
    }
    async fn subscribe_invalidations(&self, _r: &RegionKey) -> R<BoxStream<'_, R<Invalidation>>> {
        no_backend()
    }
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            bi_temporal: false,
            streaming: false,
            leases: false,
            change_feed: false,
            max_traversal_depth: 0,
        }
    }
    fn backend_id(&self) -> StorageBackendId {
        StorageBackendId::InMemory
    }
    fn ontology_fingerprint(&self) -> [u8; 32] {
        [0u8; 32]
    }
}
