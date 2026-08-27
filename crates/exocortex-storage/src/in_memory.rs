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
    /// Change-feed fan-out (§9.1): every committed write publishes an
    /// invalidation so the double exercises the full cluster/SSE path.
    feed: tokio::sync::broadcast::Sender<Invalidation>,
}

struct InMemoryInner {
    /// Serializes lease turnover with every mutation. A fenced write holds
    /// this gate from token validation through the staged state swap.
    mutation_gate: Mutex<()>,
    memories: Mutex<HashMap<MemoryId, Vec<Memory>>>, // history stack per id
    rels: Mutex<HashMap<RelationshipId, Vec<Relationship>>>,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
    /// Chubby-style lease table (§9.2): current holder token per key plus
    /// the monotonic epoch counter — same semantics as the Redis path, so
    /// fencing is exercisable without a live backend.
    leases: Mutex<HashMap<LeaseKey, InMemoryLease>>,
    /// Monotonic fencing-epoch counter per lease key (never resets).
    lease_epochs: Mutex<HashMap<LeaseKey, u64>>,
    #[cfg(test)]
    fence_checkpoint: Mutex<Option<std::sync::Arc<FenceCheckpoint>>>,
}

#[cfg(test)]
struct FenceCheckpoint {
    reached: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
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
            feed: self.feed.clone(),
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
                mutation_gate: Default::default(),
                #[cfg(test)]
                fence_checkpoint: Default::default(),
            }),
            lsn: std::sync::Arc::new(AtomicU64::new(0)),
            feed: tokio::sync::broadcast::channel(4096).0,
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

    #[cfg(test)]
    fn set_fence_checkpoint(&self, checkpoint: std::sync::Arc<FenceCheckpoint>) {
        *self.inner.fence_checkpoint.lock().unwrap() = Some(checkpoint);
    }

    #[cfg(test)]
    fn pause_at_fence_checkpoint(&self) {
        let checkpoint = self.inner.fence_checkpoint.lock().unwrap().take();
        if let Some(checkpoint) = checkpoint {
            checkpoint.reached.wait();
            checkpoint.release.wait();
        }
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
    fn upsert_relationship_row_locked(
        &self,
        r: &Relationship,
    ) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.inner.rels.lock().unwrap();
        let mut r = r.clone();
        r.lsn = exocortex_kernel::LSN::new_backend(lsn);
        // ST7/ST8 (audit): overwrite in place — one current row per id, the
        // same semantics the FalkorDB adapter's MERGE gives. The double's
        // old per-id version stack was a model the production backend does
        // not have.
        store.insert(r.id, vec![r.clone()]);
        drop(store);
        let _ = self.feed.send(Invalidation::RelationshipUpserted {
            id: r.id,
            from: r.from,
            to: r.to,
            kind: r.kind,
            lsn,
        });
        Ok(CommitRecord {
            lsn,
            committed_at: Utc::now(),
            node_id: None,
            edge_id: None,
        })
    }

    /// Stage and atomically swap one complete batch. The caller holds
    /// `mutation_gate`, which also excludes lease turnover for fenced calls.
    fn upsert_batch_locked(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> Result<(Vec<CommitRecord>, Vec<Invalidation>), StorageError> {
        let mut staged_m: HashMap<MemoryId, Vec<Memory>> =
            self.inner.memories.lock().unwrap().clone();
        let mut staged_r: HashMap<RelationshipId, Vec<Relationship>> =
            self.inner.rels.lock().unwrap().clone();

        // A missing endpoint is a per-row failure. Reject before allocating
        // an LSN or swapping any staged row, matching Falkor's query guard.
        if let Some(r) = rs.iter().find(|r| {
            !staged_m.contains_key(&r.from) && !ms.iter().any(|m| m.id == r.from)
                || !staged_m.contains_key(&r.to) && !ms.iter().any(|m| m.id == r.to)
        }) {
            return Err(StorageError::Backend(format!(
                "relationship {:02x?} endpoint missing",
                r.id.0
            )));
        }

        let mut records = Vec::with_capacity(ms.len() + rs.len());
        let mut invalidations = Vec::new();
        let mut next = self.lsn.load(Ordering::SeqCst);
        for m in ms {
            next += 1;
            let mut m = m.clone();
            m.lsn = exocortex_kernel::LSN::new_backend(next);
            staged_m.insert(m.id, vec![m.clone()]);
            invalidations.push(Invalidation::MemoryUpserted {
                id: m.id,
                lsn: next,
            });
            records.push(CommitRecord {
                lsn: next,
                committed_at: Utc::now(),
                node_id: None,
                edge_id: None,
            });
        }
        for r in rs {
            next += 1;
            let mut r = r.clone();
            r.lsn = exocortex_kernel::LSN::new_backend(next);
            staged_r.insert(r.id, vec![r.clone()]);
            invalidations.push(Invalidation::RelationshipUpserted {
                id: r.id,
                from: r.from,
                to: r.to,
                kind: r.kind,
                lsn: next,
            });
            records.push(CommitRecord {
                lsn: next,
                committed_at: Utc::now(),
                node_id: None,
                edge_id: None,
            });
            if let Some(mut inv) = exocortex_kernel::materialize_inverse(&self.inner.ontology, &r) {
                next += 1;
                inv.lsn = exocortex_kernel::LSN::new_backend(next);
                staged_r.insert(inv.id, vec![inv.clone()]);
                invalidations.push(Invalidation::RelationshipUpserted {
                    id: inv.id,
                    from: inv.from,
                    to: inv.to,
                    kind: inv.kind,
                    lsn: next,
                });
                records.push(CommitRecord {
                    lsn: next,
                    committed_at: Utc::now(),
                    node_id: None,
                    edge_id: None,
                });
            }
        }
        self.lsn.store(next, Ordering::SeqCst);
        *self.inner.memories.lock().unwrap() = staged_m;
        *self.inner.rels.lock().unwrap() = staged_r;
        Ok((records, invalidations))
    }

    fn delete_memory_locked(&self, id: &MemoryId) -> CommitRecord {
        let lsn = self.next_lsn();
        let now = Utc::now();
        let mut store = self.inner.memories.lock().unwrap();
        if let Some(h) = store.get_mut(id) {
            if let Some(last) = h.last_mut() {
                last.valid_until = Some(now);
                last.lsn = exocortex_kernel::LSN::new_backend(lsn);
            }
        }
        CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let _gate = self.inner.mutation_gate.lock().unwrap();
        let lsn = self.next_lsn();
        let mut store = self.inner.memories.lock().unwrap();
        let mut m = m.clone();
        m.lsn = exocortex_kernel::LSN::new_backend(lsn);
        let id = m.id;
        // ST7/ST8 (audit): overwrite in place — one current row per id,
        // matching the FalkorDB adapter's MERGE and the trait's "current
        // versions" streaming contract.
        store.insert(id, vec![m]);
        drop(store);
        let _ = self.feed.send(Invalidation::MemoryUpserted { id, lsn });
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
        let (records, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.upsert_batch_locked(ms, rs)?
        };
        for inv in invalidations {
            let _ = self.feed.send(inv);
        }
        Ok(records)
    }
    async fn delete_memory(&self, id: &MemoryId) -> Result<CommitRecord, StorageError> {
        let record = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.delete_memory_locked(id)
        };
        let _ = self.feed.send(Invalidation::MemoryDeleted {
            id: *id,
            lsn: record.lsn,
        });
        Ok(record)
    }
    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError> {
        let _gate = self.inner.mutation_gate.lock().unwrap();
        let rec = self.upsert_relationship_row_locked(r)?;
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
                self.upsert_relationship_row_locked(&inv)?;
            }
        }
        Ok(rec)
    }
    async fn delete_relationship(&self, id: &RelationshipId) -> Result<CommitRecord, StorageError> {
        let _gate = self.inner.mutation_gate.lock().unwrap();
        let lsn = self.next_lsn();
        let mut store = self.inner.rels.lock().unwrap();
        // ST11 (audit): publish the invalidation like every other write —
        // relationship deletions must reach the change feed.
        let mut closed = false;
        if let Some(h) = store.get_mut(id) {
            if let Some(last) = h.last_mut() {
                if last.valid_until.is_none() {
                    last.valid_until = Some(Utc::now());
                    last.lsn = exocortex_kernel::LSN::new_backend(lsn);
                    closed = true;
                }
            }
        }
        drop(store);
        if !closed {
            // ST9 parity: a no-op delete is an error, never a silent success.
            return Err(StorageError::Backend(format!(
                "delete_relationship: {:02x?} not found or already closed",
                id.0
            )));
        }
        let _ = self
            .feed
            .send(Invalidation::RelationshipDeleted { id: *id, lsn });
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
        if !crate::memory_visible(&m, vc) {
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
        // ST12 (audit): evaluate validity against the CURRENT version only
        // (the historical `any()` counted an id if ANY version was valid, so
        // a deleted memory stayed counted forever). Visibility follows
        // R-T11: Public reads as Org, so every row is in scope at the
        // snapshot's Org-level view (parity with the Falkor count).
        Ok(GraphSnapshot {
            as_of: t,
            backend_lsn: self.lsn.load(Ordering::SeqCst),
            memory_count: store
                .values()
                .filter_map(|h| h.last())
                .filter(|m| valid(m.valid_from, &m.valid_until))
                .count() as u64,
            relationship_count: rels
                .values()
                .filter_map(|h| h.last())
                .filter(|r| valid(r.valid_from, &r.valid_until))
                .count() as u64,
        })
    }
    async fn valid_at(
        &self,
        id: &MemoryId,
        at: DateTime<Utc>,
    ) -> Result<Option<Memory>, StorageError> {
        let store = self.inner.memories.lock().unwrap();
        // ST7 (audit): the double overwrites in place, so exactly one row
        // per id exists — the same semantics the FalkorDB adapter serves.
        Ok(store
            .get(id)
            .and_then(|h| h.last())
            .filter(|m| m.valid_from <= at && m.valid_until.is_none_or(|v| v > at))
            .cloned())
    }
    async fn query_cypher(&self, _q: &CypherQuery) -> Result<ResultSet, StorageError> {
        Err(StorageError::Backend(
            "InMemoryStorage does not implement Cypher".into(),
        ))
    }
    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        // ST8 (audit): current versions only — one row per id, matching the
        // FalkorDB pagers and the trait contract.
        let all: Vec<_> = self
            .inner
            .memories
            .lock()
            .unwrap()
            .values()
            .filter_map(|h| h.last().cloned().map(Ok))
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
            .filter_map(|h| h.last().cloned().map(Ok))
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
        let _gate = self.inner.mutation_gate.lock().unwrap();
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
        let _gate = self.inner.mutation_gate.lock().unwrap();
        let mut leases = self.inner.leases.lock().unwrap();
        let now = Utc::now();
        match leases.get_mut(&l.key) {
            Some(held) if held.token == l.fencing_token.as_str() && held.expires_at > now => {
                let ttl = l.expires_at - l.acquired_at;
                held.expires_at = now + ttl;
                Ok(OwnerLease {
                    acquired_at: now,
                    expires_at: held.expires_at,
                    ..l.clone()
                })
            }
            _ => Err(StorageError::Backend("lease lost (token mismatch)".into())),
        }
    }
    async fn release_lease(&self, l: OwnerLease) -> Result<(), StorageError> {
        let _gate = self.inner.mutation_gate.lock().unwrap();
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
        let (records, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.check_lease_current(lease)?;
            #[cfg(test)]
            self.pause_at_fence_checkpoint();
            self.upsert_batch_locked(ms, rs)?
        };
        for inv in invalidations {
            let _ = self.feed.send(inv);
        }
        Ok(records)
    }
    async fn delete_memory_fenced(
        &self,
        id: &MemoryId,
        lease: &OwnerLease,
    ) -> Result<CommitRecord, StorageError> {
        let record = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.check_lease_current(lease)?;
            self.delete_memory_locked(id)
        };
        let _ = self.feed.send(Invalidation::MemoryDeleted {
            id: *id,
            lsn: record.lsn,
        });
        Ok(record)
    }
    async fn subscribe_invalidations(
        &self,
        _r: &RegionKey,
    ) -> Result<BoxStream<'_, Result<Invalidation, StorageError>>, StorageError> {
        // Wildcard regions (§9.1): the double fans every invalidation to
        // every subscriber regardless of the requested region key.
        let rx = self.feed.subscribe();
        use futures::StreamExt as _;
        Ok(Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx)
                .filter_map(|item| async move { item.ok().map(Ok) }),
        ))
    }
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            bi_temporal: true,
            streaming: true,
            leases: true,
            change_feed: true,
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

#[cfg(test)]
mod atomic_fence_tests {
    use super::*;
    use exocortex_kernel::{MemoryContext, Provenance, LSN};

    fn memory() -> Memory {
        Memory {
            id: MemoryId::new_v7(),
            memory_type: 0,
            title: "atomic fence".into(),
            content: "probe".into(),
            summary: None,
            tags: Default::default(),
            visibility: Visibility::Org,
            provenance: Provenance::Asserted {
                author: "test".into(),
                producer_kind: None,
            },
            context: MemoryContext {
                timestamp: Utc::now(),
                project_id: None,
                project_path: None,
                team_id: None,
                tenant_id: None,
                session_id: None,
                user_id: None,
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
            importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
            confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
            effectiveness: None,
            usage_count: 0,
            valid_from: Utc::now(),
            valid_until: None,
            recorded_at: Utc::now(),
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_local(0),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lease_turnover_waits_for_fenced_commit_linearization_point() {
        let ontology = std::sync::Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = InMemoryStorage::new(ontology);
        let key = LeaseKey::Cleanup { org: "org".into() };
        let old = storage
            .acquire_lease(&key, std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let reached = std::sync::Arc::new(std::sync::Barrier::new(2));
        let release = std::sync::Arc::new(std::sync::Barrier::new(2));
        storage.set_fence_checkpoint(std::sync::Arc::new(FenceCheckpoint {
            reached: reached.clone(),
            release: release.clone(),
        }));

        let row = memory();
        let old_writer = {
            let storage = storage.clone();
            let row = row.clone();
            let old = old.clone();
            tokio::spawn(async move { storage.upsert_batch_fenced(&[row], &[], &old).await })
        };
        tokio::task::spawn_blocking(move || reached.wait())
            .await
            .unwrap();

        // Expiry becomes observable while the old writer is paused after
        // validation. A replacement acquisition must still wait for the
        // mutation gate; otherwise it can become owner before the old row
        // commits.
        storage
            .inner
            .leases
            .lock()
            .unwrap()
            .get_mut(&key)
            .unwrap()
            .expires_at = Utc::now() - chrono::Duration::seconds(1);
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let replacement = storage.clone();
        let replacement_key = key.clone();
        tokio::spawn(async move {
            let result = replacement
                .acquire_lease(&replacement_key, std::time::Duration::from_secs(60))
                .await;
            let _ = tx.send(result);
        });
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "lease turnover crossed the fenced commit boundary"
        );

        tokio::task::spawn_blocking(move || release.wait())
            .await
            .unwrap();
        old_writer.await.unwrap().unwrap();
        let new = rx.await.unwrap().unwrap();
        assert!(new.epoch > old.epoch);
        assert!(storage.get_memory(&row.id).await.unwrap().is_some());
    }
}
