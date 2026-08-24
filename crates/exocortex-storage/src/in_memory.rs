// crates/exocortex-storage/src/in_memory.rs
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;

use crate::types::*;
use crate::{Storage, StorageError};
use exocortex_kernel::{EntityId, Memory, MemoryId, Relationship, RelationshipId, Visibility};

/// The deterministic test double (§6.6): HashMaps and Vecs, no Cypher, no
/// I/O. Ships as a v1 deliverable — every unit test above the storage seam
/// depends on it.
pub struct InMemoryStorage {
    memories: Mutex<HashMap<MemoryId, Vec<Memory>>>, // history stack per id
    rels: Mutex<HashMap<RelationshipId, Vec<Relationship>>>,
    lsn: AtomicU64,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
}

impl InMemoryStorage {
    /// Build a double over an assembled ontology.
    pub fn new(ontology: std::sync::Arc<exocortex_kernel::Ontology>) -> Self {
        Self {
            memories: Default::default(),
            rels: Default::default(),
            lsn: AtomicU64::new(0),
            ontology,
        }
    }
    fn next_lsn(&self) -> u64 {
        self.lsn.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Current (last) version of a memory row, if any.
    pub fn memory_history(&self, id: &MemoryId) -> Vec<Memory> {
        self.memories
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Current (last) version of a relationship row, if any.
    pub fn relationship_history(&self, id: &RelationshipId) -> Vec<Relationship> {
        self.rels
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Highest LSN emitted so far.
    pub fn last_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.memories.lock().unwrap();
        let mut m = m.clone();
        m.lsn = exocortex_kernel::LSN::new_backend(lsn);
        store.entry(m.id).or_default().push(m);
        Ok(CommitRecord {
            lsn,
            committed_at: Utc::now(),
            node_id: None,
            edge_id: None,
        })
    }
    async fn upsert_batch(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let mut out = Vec::new();
        for m in ms {
            out.push(self.upsert_memory(m).await?);
        }
        for r in rs {
            out.push(self.upsert_relationship(r).await?);
        }
        Ok(out)
    }
    async fn delete_memory(&self, id: &MemoryId) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.memories.lock().unwrap();
        if let Some(h) = store.get_mut(id) {
            if let Some(last) = h.last_mut() {
                last.valid_until = Some(Utc::now());
                last.lsn = exocortex_kernel::LSN::new_backend(lsn);
            }
        }
        Ok(CommitRecord {
            lsn,
            committed_at: Utc::now(),
            node_id: None,
            edge_id: None,
        })
    }
    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.rels.lock().unwrap();
        let mut r = r.clone();
        r.lsn = exocortex_kernel::LSN::new_backend(lsn);
        store.entry(r.id).or_default().push(r);
        Ok(CommitRecord {
            lsn,
            committed_at: Utc::now(),
            node_id: None,
            edge_id: None,
        })
    }
    async fn delete_relationship(&self, id: &RelationshipId) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.rels.lock().unwrap();
        if let Some(h) = store.get_mut(id) {
            if let Some(last) = h.last_mut() {
                last.valid_until = Some(Utc::now());
                last.lsn = exocortex_kernel::LSN::new_backend(lsn);
            }
        }
        Ok(CommitRecord {
            lsn,
            committed_at: Utc::now(),
            node_id: None,
            edge_id: None,
        })
    }
    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        Ok(self
            .memories
            .lock()
            .unwrap()
            .get(id)
            .and_then(|h| h.last().cloned()))
    }
    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> {
        let store = self.memories.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| store.get(id).and_then(|h| h.last().cloned()))
            .collect())
    }
    async fn traverse(
        &self,
        _from: &MemoryId,
        _spec: &TraversalSpec,
    ) -> Result<Vec<Memory>, StorageError> {
        Ok(vec![]) // Cache-backed traversal lives in §8.
    }
    async fn find_by_entity(
        &self,
        _e: &EntityId,
        _f: &MemoryFilter,
    ) -> Result<Vec<Memory>, StorageError> {
        Ok(vec![])
    }
    async fn get_state_at(&self, t: DateTime<Utc>) -> Result<GraphSnapshot, StorageError> {
        let store = self.memories.lock().unwrap();
        let rels = self.rels.lock().unwrap();
        let valid =
            |vf: DateTime<Utc>, vu: &Option<DateTime<Utc>>| vf <= t && vu.map_or(true, |v| v > t);
        Ok(GraphSnapshot {
            as_of: t,
            backend_lsn: self.lsn.load(Ordering::SeqCst),
            memory_count: store
                .values()
                .filter(|h| h.iter().any(|m| valid(m.valid_from, &m.valid_until)))
                .count() as u64,
            relationship_count: rels
                .values()
                .filter(|h| h.iter().any(|r| valid(r.valid_from, &r.valid_until)))
                .count() as u64,
        })
    }
    async fn valid_at(
        &self,
        id: &MemoryId,
        at: DateTime<Utc>,
    ) -> Result<Option<Memory>, StorageError> {
        let store = self.memories.lock().unwrap();
        Ok(store.get(id).and_then(|h| {
            h.iter()
                .rev()
                .find(|m| m.valid_from <= at && m.valid_until.is_none_or(|v| v > at))
                .cloned()
        }))
    }
    async fn query_cypher(&self, _q: &CypherQuery) -> Result<ResultSet, StorageError> {
        Err(StorageError::Backend(
            "InMemoryStorage does not implement Cypher".into(),
        ))
    }
    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        let all: Vec<_> = self
            .memories
            .lock()
            .unwrap()
            .values()
            .flat_map(|h| h.iter().cloned().map(Ok))
            .collect();
        Box::pin(futures::stream::iter(all))
    }
    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        let all: Vec<_> = self
            .rels
            .lock()
            .unwrap()
            .values()
            .flat_map(|h| h.iter().cloned().map(Ok))
            .collect();
        Box::pin(futures::stream::iter(all))
    }
    async fn find_similar_offline(
        &self,
        _q: &Embedding,
        _k: usize,
        _f: &MemoryFilter,
    ) -> Result<Vec<(MemoryId, f32)>, StorageError> {
        Ok(vec![])
    }
    async fn acquire_lease(
        &self,
        key: &LeaseKey,
        ttl: std::time::Duration,
    ) -> Result<OwnerLease, StorageError> {
        let now = Utc::now();
        Ok(OwnerLease {
            key: key.clone(),
            owner_node_id: "in-memory".into(),
            epoch: 1,
            acquired_at: now,
            expires_at: now + chrono::Duration::from_std(ttl).unwrap(),
            grace_period: chrono::Duration::from_std(ttl).unwrap(),
            fencing_token: "in-memory:1".into(),
        })
    }
    async fn renew_lease(&self, l: &OwnerLease) -> Result<OwnerLease, StorageError> {
        Ok(l.clone())
    }
    async fn release_lease(&self, _l: OwnerLease) -> Result<(), StorageError> {
        Ok(())
    }
    async fn subscribe_invalidations(
        &self,
        _r: &RegionKey,
    ) -> Result<BoxStream<'_, Result<Invalidation, StorageError>>, StorageError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            bi_temporal: true,
            streaming: true,
            leases: false,
            change_feed: false,
            max_traversal_depth: 4,
        }
    }
    fn backend_id(&self) -> StorageBackendId {
        StorageBackendId::InMemory
    }
    fn ontology_fingerprint(&self) -> [u8; 32] {
        self.ontology.fingerprint.0
    }
}

/// Helper for tests above the seam: a visibility context that can see
/// everything up to `Visibility::Org`.
pub fn org_visibility_ctx(org: &str, user: &str) -> VisibilityContext {
    VisibilityContext {
        user_id: user.into(),
        org_id: org.into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    }
}
