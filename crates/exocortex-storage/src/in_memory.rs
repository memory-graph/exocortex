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
    rels_by_node: Mutex<HashMap<MemoryId, std::collections::HashSet<RelationshipId>>>,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
    /// Chubby-style lease table (§9.2): current holder token per key plus
    /// the monotonic epoch counter — same semantics as the Redis path, so
    /// fencing is exercisable without a live backend.
    leases: Mutex<HashMap<LeaseKey, InMemoryLease>>,
    /// Monotonic fencing-epoch counter per lease key (never resets).
    lease_epochs: Mutex<HashMap<LeaseKey, u64>>,
    proposals: Mutex<HashMap<smol_str::SmolStr, StoredProposal>>,
    discoveries: Mutex<HashMap<smol_str::SmolStr, DiscoveryRecord>>,
    audits: Mutex<Vec<serde_json::Value>>,
    stream_memory_calls: AtomicU64,
    stream_relationship_calls: AtomicU64,
    frontier_relationship_calls: AtomicU64,
    attribute_memory_calls: AtomicU64,
    settled_ingest: Mutex<HashMap<IngestBatchKey, SettledIngestBatch>>,
    cycle_journals: Mutex<HashMap<LeaseKey, CycleJournalRecord>>,
    point_reads: AtomicU64,
    batch_reads: AtomicU64,
    #[cfg(test)]
    fence_checkpoint: Mutex<Option<std::sync::Arc<FenceCheckpoint>>>,
    #[cfg(test)]
    atomic_fault: Mutex<Option<AtomicFault>>,
    #[cfg(feature = "testing")]
    stream_memory_fault_after: Mutex<Option<usize>>,
    #[cfg(feature = "testing")]
    stream_relationship_fault_after: Mutex<Option<usize>>,
}

#[derive(Clone)]
struct StoredProposal {
    proposal: DiscoveryProposal,
    consumed: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum AtomicFault {
    Mutation,
    Audit,
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
                rels_by_node: Default::default(),
                ontology,
                leases: Default::default(),
                lease_epochs: Default::default(),
                proposals: Default::default(),
                discoveries: Default::default(),
                audits: Default::default(),
                stream_memory_calls: AtomicU64::new(0),
                stream_relationship_calls: AtomicU64::new(0),
                frontier_relationship_calls: AtomicU64::new(0),
                attribute_memory_calls: AtomicU64::new(0),
                settled_ingest: Default::default(),
                cycle_journals: Default::default(),
                point_reads: Default::default(),
                batch_reads: Default::default(),
                mutation_gate: Default::default(),
                #[cfg(test)]
                fence_checkpoint: Default::default(),
                #[cfg(test)]
                atomic_fault: Default::default(),
                #[cfg(feature = "testing")]
                stream_memory_fault_after: Default::default(),
                #[cfg(feature = "testing")]
                stream_relationship_fault_after: Default::default(),
            }),
            lsn: std::sync::Arc::new(AtomicU64::new(0)),
            feed: tokio::sync::broadcast::channel(4096).0,
        }
    }
    /// A clone handle sharing the same underlying state (tests and caches).
    pub fn clone_dyn(&self) -> Self {
        self.clone()
    }
    /// Fail the next bulk stream after `after` valid rows (Dreams/cache fault tests).
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn fail_next_stream_after(&self, memories: Option<usize>, relationships: Option<usize>) {
        *self.inner.stream_memory_fault_after.lock().unwrap() = memories;
        *self.inner.stream_relationship_fault_after.lock().unwrap() = relationships;
    }
    /// Reset and return backend-read counters for scaling/conformance tests.
    /// The first element is point reads; the second is batched reads.
    pub fn take_read_counts(&self) -> (u64, u64) {
        (
            self.inner.point_reads.swap(0, Ordering::SeqCst),
            self.inner.batch_reads.swap(0, Ordering::SeqCst),
        )
    }
    fn next_lsn(&self) -> u64 {
        self.lsn.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn index_relationship(&self, id: RelationshipId, from: MemoryId, to: MemoryId) {
        let mut index = self.inner.rels_by_node.lock().unwrap();
        index.entry(from).or_default().insert(id);
        index.entry(to).or_default().insert(id);
    }

    fn index_invalidation(&self, invalidation: &Invalidation) {
        if let Invalidation::RelationshipUpserted { id, from, to, .. } = invalidation {
            self.index_relationship(*id, *from, *to);
        }
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

    /// Query counters for deterministic bounded-scan tests:
    /// `(memory streams, relationship streams, frontier relationship reads,
    /// attribute-posting reads)`.
    #[doc(hidden)]
    pub fn reasoning_query_counts(&self) -> (u64, u64, u64, u64) {
        (
            self.inner.stream_memory_calls.load(Ordering::Relaxed),
            self.inner.stream_relationship_calls.load(Ordering::Relaxed),
            self.inner
                .frontier_relationship_calls
                .load(Ordering::Relaxed),
            self.inner.attribute_memory_calls.load(Ordering::Relaxed),
        )
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
        // R-T16a: retain every assertion while exposing the newest row as
        // current through the ordinary read APIs.
        store.entry(r.id).or_default().push(r.clone());
        drop(store);
        self.index_relationship(r.id, r.from, r.to);
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
        let mut seen_relationships: std::collections::HashSet<RelationshipId> =
            rs.iter().map(|relationship| relationship.id).collect();
        for m in ms {
            next += 1;
            let mut m = m.clone();
            m.lsn = exocortex_kernel::LSN::new_backend(next);
            staged_m.entry(m.id).or_default().push(m.clone());
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
            staged_r.entry(r.id).or_default().push(r.clone());
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
                if !seen_relationships.insert(inv.id) {
                    continue;
                }
                next += 1;
                inv.lsn = exocortex_kernel::LSN::new_backend(next);
                staged_r.entry(inv.id).or_default().push(inv.clone());
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
            if let Some(last) = h.last().cloned() {
                let mut closed = last;
                closed.valid_until = Some(now);
                closed.recorded_at = now;
                closed.lsn = exocortex_kernel::LSN::new_backend(lsn);
                h.push(closed);
            }
        }
        CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        }
    }

    fn audit_value(audit: &AuditEvent, lsn: u64) -> serde_json::Value {
        fn digest_hex(bytes: &[u8; 32]) -> String {
            use std::fmt::Write as _;
            let mut out = String::with_capacity(64);
            for byte in bytes {
                let _ = write!(out, "{byte:02x}");
            }
            out
        }
        serde_json::json!({
            "action": audit.action,
            "actor": audit.actor,
            "org_id": audit.org_id,
            "input_digest": digest_hex(&audit.input_digest),
            "output_ids": audit.output_ids,
            "fingerprint": digest_hex(&audit.fingerprint),
            "lease_epoch": audit.lease_epoch.map(|epoch| epoch.to_string()).unwrap_or_default(),
            "recorded_at": audit.recorded_at.to_rfc3339(),
            "lsn": lsn,
        })
    }

    #[cfg(test)]
    fn take_atomic_fault(&self, expected: AtomicFault) -> bool {
        let mut fault = self.inner.atomic_fault.lock().unwrap();
        if fault.as_ref().is_some_and(|actual| {
            std::mem::discriminant(actual) == std::mem::discriminant(&expected)
        }) {
            fault.take();
            true
        } else {
            false
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
        // R-T16a: assertion history is append-only; current reads select last.
        store.entry(id).or_default().push(m);
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
            self.index_invalidation(&inv);
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
            if let Some(last) = h.last().cloned() {
                if last.valid_until.is_none() {
                    let now = Utc::now();
                    let mut closed_row = last;
                    closed_row.valid_until = Some(now);
                    closed_row.recorded_at = now;
                    closed_row.lsn = exocortex_kernel::LSN::new_backend(lsn);
                    h.push(closed_row);
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
    async fn commit_ingest_batch(
        &self,
        key: &IngestBatchKey,
        memories: &[Memory],
        relationships: &[Relationship],
        accepted: u32,
    ) -> Result<IngestCommitOutcome, StorageError> {
        let (records, invalidations, settled) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            if let Some(settled) = self.inner.settled_ingest.lock().unwrap().get(key).copied() {
                return Ok(IngestCommitOutcome::Duplicate(settled));
            }
            let (records, invalidations) = self.upsert_batch_locked(memories, relationships)?;
            let settled = SettledIngestBatch {
                accepted,
                rejected: 0,
                assigned_lsn: records.last().map_or(0, |record| record.lsn),
            };
            self.inner
                .settled_ingest
                .lock()
                .unwrap()
                .insert(key.clone(), settled);
            (records, invalidations, settled)
        };
        for invalidation in invalidations {
            self.index_invalidation(&invalidation);
            let _ = self.feed.send(invalidation);
        }
        Ok(IngestCommitOutcome::Committed { records, settled })
    }
    async fn upsert_memory_audited(
        &self,
        memory: &Memory,
        audit: &AuditEvent,
    ) -> Result<CommitRecord, StorageError> {
        let (record, invalidation) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            let mut memories = self.inner.memories.lock().unwrap().clone();
            let mut audits = self.inner.audits.lock().unwrap().clone();
            let lsn = self.lsn.load(Ordering::SeqCst) + 1;
            let now = Utc::now();
            let mut row = memory.clone();
            row.lsn = exocortex_kernel::LSN::new_backend(lsn);
            memories.entry(row.id).or_default().push(row);
            #[cfg(test)]
            if self.take_atomic_fault(AtomicFault::Mutation) {
                return Err(StorageError::Backend("injected mutation failure".into()));
            }
            audits.push(Self::audit_value(audit, lsn));
            #[cfg(test)]
            if self.take_atomic_fault(AtomicFault::Audit) {
                return Err(StorageError::Backend("injected audit failure".into()));
            }
            *self.inner.memories.lock().unwrap() = memories;
            *self.inner.audits.lock().unwrap() = audits;
            self.lsn.store(lsn, Ordering::SeqCst);
            (
                CommitRecord {
                    lsn,
                    committed_at: now,
                    node_id: None,
                    edge_id: None,
                },
                Invalidation::MemoryUpserted { id: memory.id, lsn },
            )
        };
        let _ = self.feed.send(invalidation);
        Ok(record)
    }

    async fn create_discovery_proposal(
        &self,
        proposal: &DiscoveryProposal,
    ) -> Result<(), StorageError> {
        if proposal.region.org != proposal.caller_scope.org_id
            || proposal.proposed_visibility > proposal.caller_scope.max_visibility
            || (!proposal.region.project.is_empty()
                && proposal.region.project != "*"
                && !proposal
                    .caller_scope
                    .project_ids
                    .contains(&proposal.region.project))
        {
            return Err(StorageError::ProposalMismatch);
        }
        let _gate = self.inner.mutation_gate.lock().unwrap();
        let mut proposals = self.inner.proposals.lock().unwrap();
        match proposals.get(&proposal.discovery_id) {
            Some(stored) if stored.proposal == *proposal && !stored.consumed => Ok(()),
            Some(stored) if stored.proposal == *proposal => Err(StorageError::ProposalNotFound),
            Some(_) => Err(StorageError::ProposalMismatch),
            None => {
                let mut discoveries = self.inner.discoveries.lock().unwrap();
                let discovery = discoveries
                    .get(&proposal.discovery_id)
                    .ok_or(StorageError::ProposalNotFound)?;
                if discovery.region != proposal.region
                    || discovery.from != proposal.from
                    || discovery.to != proposal.to
                {
                    return Err(StorageError::ProposalMismatch);
                }
                proposals.insert(
                    proposal.discovery_id.clone(),
                    StoredProposal {
                        proposal: proposal.clone(),
                        consumed: false,
                    },
                );
                discoveries.remove(&proposal.discovery_id);
                Ok(())
            }
        }
    }

    async fn store_discovery(&self, discovery: &DiscoveryRecord) -> Result<(), StorageError> {
        let lsn = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            if self
                .inner
                .proposals
                .lock()
                .unwrap()
                .contains_key(&discovery.discovery_id)
            {
                return Err(StorageError::ProposalMismatch);
            }
            let mut discoveries = self.inner.discoveries.lock().unwrap();
            match discoveries.get(&discovery.discovery_id) {
                Some(stored) if stored == discovery => return Ok(()),
                Some(_) => return Err(StorageError::ProposalMismatch),
                None => {
                    let lsn = self.lsn.load(Ordering::SeqCst) + 1;
                    discoveries.insert(discovery.discovery_id.clone(), discovery.clone());
                    self.lsn.store(lsn, Ordering::SeqCst);
                    lsn
                }
            }
        };
        let _ = self.feed.send(Invalidation::DiscoveryAvailable {
            record: discovery.clone(),
            lsn,
        });
        Ok(())
    }

    async fn get_discovery(
        &self,
        discovery_id: &str,
    ) -> Result<Option<DiscoveryRecord>, StorageError> {
        Ok(self
            .inner
            .discoveries
            .lock()
            .unwrap()
            .get(discovery_id)
            .cloned())
    }

    async fn list_discoveries(
        &self,
        org_id: &str,
        limit: u32,
    ) -> Result<Vec<DiscoveryRecord>, StorageError> {
        let mut rows: Vec<_> = self
            .inner
            .discoveries
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.region.org == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|row| {
            (
                std::cmp::Reverse(row.discovered_at),
                row.discovery_id.clone(),
            )
        });
        rows.truncate(limit.min(100) as usize);
        Ok(rows)
    }

    async fn get_discovery_proposal(
        &self,
        discovery_id: &str,
    ) -> Result<Option<DiscoveryProposal>, StorageError> {
        Ok(self
            .inner
            .proposals
            .lock()
            .unwrap()
            .get(discovery_id)
            .filter(|stored| !stored.consumed)
            .map(|stored| stored.proposal.clone()))
    }

    async fn accept_discovery(
        &self,
        acceptance: &DiscoveryAcceptance,
    ) -> Result<CommitRecord, StorageError> {
        let (record, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            let mut proposals = self.inner.proposals.lock().unwrap().clone();
            let mut relationships = self.inner.rels.lock().unwrap().clone();
            let mut audits = self.inner.audits.lock().unwrap().clone();
            let stored = proposals
                .get_mut(&acceptance.discovery_id)
                .ok_or(StorageError::ProposalNotFound)?;
            if stored.consumed {
                return Err(StorageError::ProposalNotFound);
            }
            let proposal = &stored.proposal;
            let relationship = &acceptance.relationship;
            if proposal.region != acceptance.region
                || proposal.caller_scope != acceptance.caller_scope
                || proposal.from != relationship.from
                || proposal.to != relationship.to
                || proposal.kind != relationship.kind
                || proposal.proposed_visibility != relationship.visibility
                || proposal.region.org != acceptance.audit.org_id
                || proposal.caller_scope.user_id != acceptance.audit.actor
                || relationship.visibility > acceptance.caller_scope.max_visibility
            {
                return Err(StorageError::ProposalMismatch);
            }
            stored.consumed = true;
            let lsn = self.lsn.load(Ordering::SeqCst) + 1;
            let now = Utc::now();
            let mut row = relationship.clone();
            row.lsn = exocortex_kernel::LSN::new_backend(lsn);
            relationships.entry(row.id).or_default().push(row.clone());
            let mut next_lsn = lsn;
            let mut invalidations = vec![Invalidation::RelationshipUpserted {
                id: row.id,
                from: row.from,
                to: row.to,
                kind: row.kind,
                lsn,
            }];
            if let Some(mut inverse) =
                exocortex_kernel::materialize_inverse(&self.inner.ontology, relationship)
            {
                next_lsn += 1;
                inverse.lsn = exocortex_kernel::LSN::new_backend(next_lsn);
                relationships
                    .entry(inverse.id)
                    .or_default()
                    .push(inverse.clone());
                invalidations.push(Invalidation::RelationshipUpserted {
                    id: inverse.id,
                    from: inverse.from,
                    to: inverse.to,
                    kind: inverse.kind,
                    lsn: next_lsn,
                });
            }
            #[cfg(test)]
            if self.take_atomic_fault(AtomicFault::Mutation) {
                return Err(StorageError::Backend("injected mutation failure".into()));
            }
            audits.push(Self::audit_value(&acceptance.audit, lsn));
            #[cfg(test)]
            if self.take_atomic_fault(AtomicFault::Audit) {
                return Err(StorageError::Backend("injected audit failure".into()));
            }
            *self.inner.proposals.lock().unwrap() = proposals;
            *self.inner.rels.lock().unwrap() = relationships;
            *self.inner.audits.lock().unwrap() = audits;
            self.lsn.store(next_lsn, Ordering::SeqCst);
            (
                CommitRecord {
                    lsn,
                    committed_at: now,
                    node_id: None,
                    edge_id: None,
                },
                invalidations,
            )
        };
        for invalidation in invalidations {
            self.index_invalidation(&invalidation);
            let _ = self.feed.send(invalidation);
        }
        Ok(record)
    }

    async fn audit_range(
        &self,
        org_id: &str,
        since_lsn: u64,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        Ok(self
            .inner
            .audits
            .lock()
            .unwrap()
            .iter()
            .filter(|row| {
                row["org_id"] == org_id && row["lsn"].as_u64().is_some_and(|lsn| lsn > since_lsn)
            })
            .take(limit as usize)
            .cloned()
            .collect())
    }
    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        self.inner.point_reads.fetch_add(1, Ordering::SeqCst);
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
        self.inner.batch_reads.fetch_add(1, Ordering::SeqCst);
        let store = self.inner.memories.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| store.get(id).and_then(|h| h.last().cloned()))
            .collect())
    }
    async fn get_visible_memories(
        &self,
        ids: &[MemoryId],
        vc: &crate::VisibilityContext,
    ) -> Result<Vec<Memory>, StorageError> {
        self.inner.batch_reads.fetch_add(1, Ordering::SeqCst);
        let store = self.inner.memories.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| store.get(id).and_then(|history| history.last()))
            .filter(|memory| crate::memory_visible(memory, vc))
            .cloned()
            .collect())
    }
    async fn get_relationship(
        &self,
        id: &RelationshipId,
    ) -> Result<Option<Relationship>, StorageError> {
        Ok(self
            .inner
            .rels
            .lock()
            .unwrap()
            .get(id)
            .and_then(|history| history.last().cloned()))
    }
    async fn relationships_touching(
        &self,
        frontier: &[MemoryId],
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        self.inner
            .frontier_relationship_calls
            .fetch_add(1, Ordering::Relaxed);
        let index = self.inner.rels_by_node.lock().unwrap();
        let ids: std::collections::HashSet<_> = frontier
            .iter()
            .filter_map(|node| index.get(node))
            .flatten()
            .copied()
            .collect();
        drop(index);
        let relationships = self.inner.rels.lock().unwrap();
        Ok(ids
            .into_iter()
            .filter_map(|id| relationships.get(&id).and_then(|history| history.last()))
            .filter(|row| row.valid_until.is_none() && row.invalidated_by.is_none())
            .take(limit as usize)
            .cloned()
            .collect())
    }
    async fn relationships_in_region(
        &self,
        region: &RegionKey,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let memories = self.inner.memories.lock().unwrap();
        let relationships = self.inner.rels.lock().unwrap();
        let in_region = |id: &MemoryId| {
            memories
                .get(id)
                .and_then(|history| history.last())
                .is_some_and(|memory| {
                    memory.memory_type == region.memory_type
                        && (region.org == "*"
                            || memory.context.tenant_id.as_deref() == Some(region.org.as_str()))
                        && (region.project == "*"
                            || memory.context.project_id.as_deref()
                                == Some(region.project.as_str()))
                })
        };
        let mut rows: Vec<_> = relationships
            .values()
            .filter_map(|history| history.last())
            .filter(|relationship| {
                relationship.valid_until.is_none()
                    && relationship.invalidated_by.is_none()
                    && in_region(&relationship.from)
                    && in_region(&relationship.to)
            })
            .cloned()
            .collect();
        rows.sort_by_key(|relationship| {
            (
                relationship.from,
                relationship.to,
                relationship.kind,
                relationship.id,
            )
        });
        if rows.len() > limit as usize {
            return Err(StorageError::Backend(format!(
                "region relationship budget exceeded: more than {limit} rows"
            )));
        }
        Ok(rows)
    }
    async fn memories_sharing_attributes(
        &self,
        tags: &[smol_str::SmolStr],
        entities: &[EntityId],
        limit: u32,
    ) -> Result<Vec<Memory>, StorageError> {
        self.inner
            .attribute_memory_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(self
            .inner
            .memories
            .lock()
            .unwrap()
            .values()
            .filter_map(|history| history.last())
            .filter(|memory| {
                memory.tags.iter().any(|tag| tags.contains(tag))
                    || memory
                        .context
                        .entities
                        .iter()
                        .any(|entity| entities.contains(entity))
            })
            .take(limit as usize)
            .cloned()
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
        entity: &EntityId,
        filter: &MemoryFilter,
    ) -> Result<Vec<Memory>, StorageError> {
        if filter.limit > 500 {
            return Err(StorageError::Backend("limit > 500".into()));
        }
        let mut rows: Vec<Memory> = self
            .inner
            .memories
            .lock()
            .unwrap()
            .values()
            .filter_map(|history| history.last())
            .filter(|memory| memory.context.entities.contains(entity))
            .filter(|memory| {
                filter.memory_types.is_empty() || filter.memory_types.contains(&memory.memory_type)
            })
            .filter(|memory| {
                filter
                    .project_id
                    .as_ref()
                    .is_none_or(|project| memory.context.project_id.as_ref() == Some(project))
            })
            .filter(|memory| {
                filter.valid_at.is_none_or(|at| {
                    memory.valid_from <= at && memory.valid_until.is_none_or(|until| until > at)
                })
            })
            .filter(|memory| crate::memory_visible(memory, &filter.visibility_ctx))
            .cloned()
            .collect();
        rows.sort_by_key(|memory| std::cmp::Reverse(memory.recorded_at));
        rows.truncate(filter.limit as usize);
        Ok(rows)
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
                .filter(|h| {
                    h.iter()
                        .rev()
                        .find(|m| m.recorded_at <= t && m.valid_from <= t)
                        .is_some_and(|m| valid(m.valid_from, &m.valid_until))
                })
                .count() as u64,
            relationship_count: rels
                .values()
                .filter(|h| {
                    h.iter()
                        .rev()
                        .find(|r| r.recorded_at <= t && r.valid_from <= t)
                        .is_some_and(|r| valid(r.valid_from, &r.valid_until))
                })
                .count() as u64,
        })
    }
    async fn valid_at(
        &self,
        id: &MemoryId,
        at: DateTime<Utc>,
    ) -> Result<Option<Memory>, StorageError> {
        let store = self.inner.memories.lock().unwrap();
        Ok(store
            .get(id)
            .and_then(|h| {
                h.iter()
                    .rev()
                    .find(|m| m.recorded_at <= at && m.valid_from <= at)
                    .filter(|m| m.valid_until.is_none_or(|v| v > at))
            })
            .cloned())
    }
    async fn query_cypher(&self, _q: &CypherQuery) -> Result<ResultSet, StorageError> {
        Err(StorageError::Backend(
            "InMemoryStorage does not implement Cypher".into(),
        ))
    }
    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        self.inner
            .stream_memory_calls
            .fetch_add(1, Ordering::Relaxed);
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
        #[cfg(feature = "testing")]
        let all = {
            let mut all = all;
            if let Some(after) = self.inner.stream_memory_fault_after.lock().unwrap().take() {
                all.truncate(after);
                all.push(Err(StorageError::Backend(
                    "injected memory stream failure".into(),
                )));
            }
            all
        };
        Box::pin(futures::stream::iter(all))
    }
    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        self.inner
            .stream_relationship_calls
            .fetch_add(1, Ordering::Relaxed);
        let all: Vec<_> = self
            .inner
            .rels
            .lock()
            .unwrap()
            .values()
            .filter_map(|h| h.last().cloned().map(Ok))
            .collect();
        #[cfg(feature = "testing")]
        let all = {
            let mut all = all;
            if let Some(after) = self
                .inner
                .stream_relationship_fault_after
                .lock()
                .unwrap()
                .take()
            {
                all.truncate(after);
                all.push(Err(StorageError::Backend(
                    "injected relationship stream failure".into(),
                )));
            }
            all
        };
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
    ) -> Result<FencedBatchCommit, StorageError> {
        let (records, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.check_lease_current(lease)?;
            #[cfg(test)]
            self.pause_at_fence_checkpoint();
            self.upsert_batch_locked(ms, rs)?
        };
        let mut committed = FencedBatchCommit {
            records,
            ..FencedBatchCommit::default()
        };
        for inv in invalidations {
            match &inv {
                Invalidation::MemoryUpserted { id, lsn } => {
                    committed.memory_lsns.entry(*id).or_default().insert(*lsn);
                }
                Invalidation::RelationshipUpserted { id, lsn, .. } => {
                    committed
                        .relationship_lsns
                        .entry(*id)
                        .or_default()
                        .insert(*lsn);
                }
                _ => {}
            }
            self.index_invalidation(&inv);
            let _ = self.feed.send(inv);
        }
        Ok(committed)
    }
    async fn upsert_batch_fenced_journaled(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        prepared_restore: &FencedRestore,
        cycle_id: &str,
        lease: &OwnerLease,
    ) -> Result<FencedBatchCommit, StorageError> {
        let (mut committed, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.check_lease_current(lease)?;
            let mut journals = self.inner.cycle_journals.lock().unwrap();
            if let Some(active) = journals.get(&lease.key) {
                if active.state == CycleJournalState::Active && active.cycle_id != cycle_id {
                    return Err(StorageError::Backend(format!(
                        "active cycle {} requires recovery before {cycle_id}",
                        active.cycle_id
                    )));
                }
            }
            let (records, invalidations) = self.upsert_batch_locked(ms, rs)?;
            let mut committed = FencedBatchCommit {
                records,
                ..FencedBatchCommit::default()
            };
            for invalidation in &invalidations {
                match invalidation {
                    Invalidation::MemoryUpserted { id, lsn } => {
                        committed.memory_lsns.entry(*id).or_default().insert(*lsn);
                    }
                    Invalidation::RelationshipUpserted { id, lsn, .. } => {
                        committed
                            .relationship_lsns
                            .entry(*id)
                            .or_default()
                            .insert(*lsn);
                    }
                    _ => {}
                }
            }
            let journal = journals
                .entry(lease.key.clone())
                .or_insert_with(|| CycleJournalRecord {
                    cycle_id: cycle_id.into(),
                    lease_key: lease.key.clone(),
                    lease_epoch: lease.epoch,
                    restore: FencedRestore::default(),
                    state: CycleJournalState::Active,
                });
            journal.cycle_id = cycle_id.into();
            journal.lease_epoch = lease.epoch;
            journal.state = CycleJournalState::Active;
            journal.restore.merge(prepared_restore);
            for (id, lsns) in &committed.memory_lsns {
                journal
                    .restore
                    .owned_memory_lsns
                    .entry(*id)
                    .or_default()
                    .extend(lsns);
            }
            for (id, lsns) in &committed.relationship_lsns {
                journal
                    .restore
                    .owned_relationship_lsns
                    .entry(*id)
                    .or_default()
                    .extend(lsns);
            }
            (committed, invalidations)
        };
        for invalidation in invalidations {
            self.index_invalidation(&invalidation);
            let _ = self.feed.send(invalidation);
        }
        Ok(std::mem::take(&mut committed))
    }
    async fn get_active_cycle_journal(
        &self,
        key: &LeaseKey,
    ) -> Result<Option<CycleJournalRecord>, StorageError> {
        Ok(self
            .inner
            .cycle_journals
            .lock()
            .unwrap()
            .get(key)
            .filter(|journal| journal.state == CycleJournalState::Active)
            .cloned())
    }
    async fn complete_cycle_journal_fenced(
        &self,
        cycle_id: &str,
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let _gate = self.inner.mutation_gate.lock().unwrap();
        self.check_lease_current(lease)?;
        let mut journals = self.inner.cycle_journals.lock().unwrap();
        let journal = journals
            .get_mut(&lease.key)
            .ok_or_else(|| StorageError::Backend("cycle journal not found".into()))?;
        if journal.cycle_id != cycle_id {
            return Err(StorageError::Backend(
                "cycle journal identity mismatch".into(),
            ));
        }
        journal.state = CycleJournalState::Completed;
        Ok(())
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
    async fn restore_fenced(
        &self,
        restore: &FencedRestore,
        lease: &OwnerLease,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let (records, invalidations) = {
            let _gate = self.inner.mutation_gate.lock().unwrap();
            self.check_lease_current(lease)?;
            let mut memories = self.inner.memories.lock().unwrap().clone();
            let mut relationships = self.inner.rels.lock().unwrap().clone();
            let mut next = self.lsn.load(Ordering::SeqCst);
            let mut records = Vec::new();
            let mut invalidations = Vec::new();
            let mut record = || {
                next += 1;
                let committed_at = Utc::now();
                records.push(CommitRecord {
                    lsn: next,
                    committed_at,
                    node_id: None,
                    edge_id: None,
                });
                next
            };

            for (id, owned_lsns) in &restore.owned_memory_lsns {
                let Some(history) = memories.get_mut(id) else {
                    continue;
                };
                let before = history.len();
                history.retain(|version| !owned_lsns.contains(&version.lsn.value));
                if history.len() == before {
                    continue;
                }
                let lsn = record();
                if history.is_empty() {
                    memories.remove(id);
                    invalidations.push(Invalidation::MemoryDeleted { id: *id, lsn });
                } else {
                    invalidations.push(Invalidation::MemoryUpserted { id: *id, lsn });
                }
            }
            for (id, owned_lsns) in &restore.owned_relationship_lsns {
                let Some(history) = relationships.get_mut(id) else {
                    continue;
                };
                let before = history.len();
                history.retain(|version| !owned_lsns.contains(&version.lsn.value));
                if history.len() == before {
                    continue;
                }
                let lsn = record();
                if let Some(current) = history.last() {
                    invalidations.push(Invalidation::RelationshipUpserted {
                        id: *id,
                        from: current.from,
                        to: current.to,
                        kind: current.kind,
                        lsn,
                    });
                } else {
                    relationships.remove(id);
                    invalidations.push(Invalidation::RelationshipDeleted { id: *id, lsn });
                }
            }
            self.lsn.store(next, Ordering::SeqCst);
            *self.inner.memories.lock().unwrap() = memories;
            *self.inner.rels.lock().unwrap() = relationships;
            (records, invalidations)
        };
        for invalidation in invalidations {
            self.index_invalidation(&invalidation);
            let _ = self.feed.send(invalidation);
        }
        Ok(records)
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
    use exocortex_kernel::{MemoryContext, Provenance, RelationshipProperties, LSN};

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

    #[tokio::test]
    async fn discovery_acceptance_rolls_back_mutation_and_audit_faults() {
        for fault in [AtomicFault::Mutation, AtomicFault::Audit] {
            let ontology = std::sync::Arc::new(
                exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                    .unwrap(),
            );
            let storage = InMemoryStorage::new(ontology);
            let from = memory();
            let to = memory();
            storage.upsert_memory(&from).await.unwrap();
            storage.upsert_memory(&to).await.unwrap();
            let scope = org_visibility_ctx("org", "alice");
            let discovery_id: smol_str::SmolStr = "proposal-atomic".into();
            let region = RegionKey {
                org: "org".into(),
                project: "*".into(),
                memory_type: from.memory_type,
            };
            let kind = exocortex_kernel::kinds::SOLVES;
            let issued_at = Utc::now();
            storage
                .store_discovery(&DiscoveryRecord {
                    discovery_id: discovery_id.clone(),
                    region: region.clone(),
                    from: from.id,
                    to: to.id,
                    discovery_type: "transitive".into(),
                    quality: 0.6,
                    via_types: [1, 2],
                    discovery_cycle_id: "atomic-cycle".into(),
                    discovered_at: issued_at,
                })
                .await
                .unwrap();
            storage
                .create_discovery_proposal(&DiscoveryProposal {
                    discovery_id: discovery_id.clone(),
                    region: region.clone(),
                    from: from.id,
                    to: to.id,
                    kind,
                    proposed_visibility: Visibility::Org,
                    caller_scope: scope.clone(),
                    issued_at,
                })
                .await
                .unwrap();
            let relationship = Relationship {
                id: RelationshipId::derive(from.id, kind, to.id, Some(&discovery_id)),
                kind,
                from: from.id,
                to: to.id,
                visibility: Visibility::Org,
                provenance: Provenance::Asserted {
                    author: "alice".into(),
                    producer_kind: None,
                },
                properties: RelationshipProperties {
                    strength: 0.5,
                    confidence: 0.8,
                    context: Some("discovery:proposal-atomic".into()),
                    evidence_count: 1,
                    success_rate: None,
                    validation_count: 0,
                    counter_evidence_count: 0,
                    last_validated: Utc::now(),
                },
                description: None,
                bidirectional: false,
                valid_from: Utc::now(),
                valid_until: None,
                recorded_at: Utc::now(),
                invalidated_by: None,
                lsn: LSN::new_local(0),
            };
            let acceptance = DiscoveryAcceptance {
                discovery_id: discovery_id.clone(),
                region,
                caller_scope: scope,
                relationship: relationship.clone(),
                audit: AuditEvent {
                    action: "accept_discovery".into(),
                    actor: "alice".into(),
                    org_id: "org".into(),
                    input_digest: [7; 32],
                    output_ids: ["edge".into()].into_iter().collect(),
                    fingerprint: storage.ontology_fingerprint(),
                    lease_epoch: None,
                    recorded_at: Utc::now(),
                },
            };
            *storage.inner.atomic_fault.lock().unwrap() = Some(fault);
            assert!(storage.accept_discovery(&acceptance).await.is_err());
            assert!(storage
                .get_discovery_proposal(&discovery_id)
                .await
                .unwrap()
                .is_some());
            assert!(storage.relationship_history(&relationship.id).is_empty());
            assert!(storage.audit_range("org", 0, 10).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn visibility_promotion_rolls_back_mutation_and_audit_faults() {
        for fault in [AtomicFault::Mutation, AtomicFault::Audit] {
            let ontology = std::sync::Arc::new(
                exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                    .unwrap(),
            );
            let storage = InMemoryStorage::new(ontology);
            let mut original = memory();
            original.visibility = Visibility::Private;
            storage.upsert_memory(&original).await.unwrap();
            let mut promoted = original.clone();
            promoted.visibility = Visibility::Org;
            let audit = AuditEvent {
                action: "promote_visibility".into(),
                actor: "alice".into(),
                org_id: "org".into(),
                input_digest: [3; 32],
                output_ids: ["memory".into()].into_iter().collect(),
                fingerprint: storage.ontology_fingerprint(),
                lease_epoch: None,
                recorded_at: Utc::now(),
            };
            *storage.inner.atomic_fault.lock().unwrap() = Some(fault);
            assert!(storage
                .upsert_memory_audited(&promoted, &audit)
                .await
                .is_err());
            assert_eq!(
                storage
                    .get_memory(&original.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .visibility,
                Visibility::Private
            );
            assert!(storage.audit_range("org", 0, 10).await.unwrap().is_empty());
        }
    }
}
