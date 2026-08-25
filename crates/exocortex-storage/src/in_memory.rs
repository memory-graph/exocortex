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
/// depends on it. Clones share the same underlying state.
pub struct InMemoryStorage {
    inner: std::sync::Arc<InMemoryInner>,
    lsn: std::sync::Arc<AtomicU64>,
}

struct InMemoryInner {
    memories: Mutex<HashMap<MemoryId, Vec<Memory>>>, // history stack per id
    rels: Mutex<HashMap<RelationshipId, Vec<Relationship>>>,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
    /// Chubby-style lease table (§9.2): current holder token per key plus
    /// the monotonic epoch counter — same semantics as the Redis path, so
    /// fencing is exercisable without a live backend.
    leases: Mutex<HashMap<LeaseKey, InMemoryLease>>,
    /// Monotonic fencing-epoch counter per lease key (never resets).
    lease_epochs: Mutex<HashMap<LeaseKey, u64>>,
}

/// Current lease holder entry for the in-memory lease table.
struct InMemoryLease {
    token: String,
    expires_at: DateTime<Utc>,
}

impl Clone for InMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            lsn: self.lsn.clone(),
        }
    }
}

impl InMemoryStorage {
    /// Build a double over an assembled ontology.
    pub fn new(ontology: std::sync::Arc<exocortex_kernel::Ontology>) -> Self {
        Self {
            inner: std::sync::Arc::new(InMemoryInner {
                memories: Default::default(),
                rels: Default::default(),
                ontology,
                leases: Default::default(),
                lease_epochs: Default::default(),
            }),
            lsn: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }
    /// A clone handle sharing the same underlying state (tests and caches).
    pub fn clone_dyn(&self) -> Self {
        self.clone()
    }
    fn next_lsn(&self) -> u64 {
        self.lsn.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Current (last) version of a memory row, if any.
    pub fn memory_history(&self, id: &MemoryId) -> Vec<Memory> {
        self.inner
            .memories
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Current (last) version of a relationship row, if any.
    pub fn relationship_history(&self, id: &RelationshipId) -> Vec<Relationship> {
        self.inner
            .rels
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

    /// R-C3 fencing check: the lease table must still hold this lease's
    /// token and it must be unexpired. Mirrors the Redis-side check in
    /// `FalkorStorage`.
    fn check_lease_current(&self, lease: &OwnerLease) -> Result<(), StorageError> {
        let leases = self.inner.leases.lock().unwrap();
        match leases.get(&lease.key) {
            Some(held)
                if held.token == lease.fencing_token.as_str() && held.expires_at > Utc::now() =>
            {
                Ok(())
            }
            _ => Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            }),
        }
    }

    /// Row write without inverse materialization (the companion path's
    /// terminal, so R-T4 never recurses).
    async fn upsert_relationship_row(
        &self,
        r: &Relationship,
    ) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.inner.rels.lock().unwrap();
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
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.inner.memories.lock().unwrap();
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
        let mut store = self.inner.memories.lock().unwrap();
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
        let rec = self.upsert_relationship_row(r).await?;
        // R-T4: write `k'(b,a)` in the same operation. Skipped when the
        // companion is already current, so repeated writes are idempotent.
        if let Some(inv) = exocortex_kernel::materialize_inverse(&self.inner.ontology, r) {
            let already_current = self
                .inner
                .rels
                .lock()
                .unwrap()
                .get(&inv.id)
                .and_then(|h| h.last())
                .is_some_and(|cur| cur.valid_until.is_none());
            if !already_current {
                self.upsert_relationship_row(&inv).await?;
            }
        }
        Ok(rec)
    }
    async fn delete_relationship(&self, id: &RelationshipId) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.inner.rels.lock().unwrap();
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
            .inner
            .memories
            .lock()
            .unwrap()
            .get(id)
            .and_then(|h| h.last().cloned()))
    }
    async fn get_memory_for(
        &self,
        id: &MemoryId,
        vc: &crate::VisibilityContext,
    ) -> Result<Option<Memory>, StorageError> {
        let Some(m) = self
            .inner
            .memories
            .lock()
            .unwrap()
            .get(id)
            .and_then(|h| h.last().cloned())
        else {
            return Ok(None);
        };
        if m.visibility as u8 > vc.max_visibility as u8
            || (m.visibility == Visibility::Private
                && m.context.user_id.as_deref() != Some(vc.user_id.as_str()))
        {
            return Err(StorageError::PermissionDenied);
        }
        Ok(Some(m))
    }
    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> {
        let store = self.inner.memories.lock().unwrap();
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
        let store = self.inner.memories.lock().unwrap();
        let rels = self.inner.rels.lock().unwrap();
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
        let store = self.inner.memories.lock().unwrap();
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
            .inner
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
            .inner
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
        let mut leases = self.inner.leases.lock().unwrap();
        if let Some(held) = leases.get(key) {
            if held.expires_at > now {
                return Err(StorageError::Backend("lease held by another node".into()));
            }
        }
        // Monotonic epoch bump (R-C3): every acquisition mints a fresh
        // fencing token, so a prior holder's writes can never pass the check.
        let mut epochs = self.inner.lease_epochs.lock().unwrap();
        let epoch = epochs
            .entry(key.clone())
            .and_modify(|e| *e += 1)
            .or_insert(1);
        let epoch = *epoch;
        drop(epochs);
        let token = format!("in-memory:{epoch}");
        let expires_at = now + chrono::Duration::from_std(ttl).unwrap();
        leases.insert(
            key.clone(),
            InMemoryLease {
                token: token.clone(),
                expires_at,
            },
        );
        drop(leases);
        Ok(OwnerLease {
            key: key.clone(),
            owner_node_id: "in-memory".into(),
            epoch,
            acquired_at: now,
            expires_at,
            grace_period: crate::trait_::grace_duration(),
            fencing_token: token.into(),
        })
    }
    async fn renew_lease(&self, l: &OwnerLease) -> Result<OwnerLease, StorageError> {
        let mut leases = self.inner.leases.lock().unwrap();
        match leases.get_mut(&l.key) {
            Some(held)
                if held.token == l.fencing_token.as_str() && held.expires_at > Utc::now() =>
            {
                let ttl = l.expires_at - l.acquired_at;
                held.expires_at = Utc::now() + ttl;
                Ok(OwnerLease {
                    expires_at: held.expires_at,
                    ..l.clone()
                })
            }
            _ => Err(StorageError::Backend("lease lost (token mismatch)".into())),
        }
    }
    async fn release_lease(&self, l: OwnerLease) -> Result<(), StorageError> {
        let mut leases = self.inner.leases.lock().unwrap();
        if leases
            .get(&l.key)
            .map(|h| h.token == l.fencing_token.as_str())
            .unwrap_or(false)
        {
            leases.remove(&l.key);
        }
        Ok(())
    }
    async fn upsert_batch_fenced(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        lease: &OwnerLease,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        self.check_lease_current(lease)?;
        self.upsert_batch(ms, rs).await
    }
    async fn delete_memory_fenced(
        &self,
        id: &MemoryId,
        lease: &OwnerLease,
    ) -> Result<CommitRecord, StorageError> {
        self.check_lease_current(lease)?;
        self.delete_memory(id).await
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
            leases: true,
            change_feed: false,
            max_traversal_depth: 4,
        }
    }
    fn backend_id(&self) -> StorageBackendId {
        StorageBackendId::InMemory
    }
    fn ontology_fingerprint(&self) -> [u8; 32] {
        self.inner.ontology.fingerprint.0
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
