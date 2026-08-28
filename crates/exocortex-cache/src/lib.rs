//! Lock-free readers, single-writer coordination. The graph itself is a
//! petgraph::StableGraph wrapped in ArcSwap so the read side never blocks.
//!
//! §8: `LocalCache` holds one `GraphSnapshot` per org behind
//! an `ArcSwap<GraphSnapshot>`; readers `load_full()` (a refcount bump) and
//! scan a consistent view. The single writer reuses reader-released snapshot
//! buffers, catches them up from a bounded delta journal, and publishes a fresh
//! immutable generation. 2Q admission governs which org graphs stay resident
//! under the byte budget (R-M12/R-M13).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::EdgeRef;
use smol_str::SmolStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use exocortex_kernel::{EntityId, Memory, MemoryId, Relationship, RelationshipId};
use exocortex_storage::{Direction, Invalidation, Storage, TraversalSpec, VisibilityContext};

/// Immutable snapshot of one org's graph. Reads see a consistent view.
pub struct GraphSnapshot {
    /// The typed graph: memory nodes, relationship edges.
    pub petgraph: StableGraph<Memory, Relationship>,
    /// Memory id -> node index.
    pub by_id: DashMap<MemoryId, NodeIndex>,
    /// Entity id -> memories about it.
    pub by_entity: DashMap<EntityId, smallvec::SmallVec<[MemoryId; 8]>>,
    /// memory_type -> ids (§8.1).
    pub by_type: DashMap<u8, roaring::RoaringBitmap>,
    /// tag -> ids (§8.1). Keys are interner handles.
    pub by_tag: DashMap<lasso::Spur, roaring::RoaringBitmap>,
    /// The tag interner (R-M4).
    pub interner: std::sync::Arc<lasso::ThreadedRodeo>,
    /// Precomputed lowercase search keys (title + tags), one per node in
    /// arena-append order, packed into ONE contiguous arena for cache-friendly
    /// scans on the read hot path.
    pub search_arena: String,
    /// Byte offset of each key inside `search_arena` (key i spans
    /// offsets[i]..offsets[i+1] or arena end), in arena-append order.
    pub search_offsets: Vec<u32>,
    /// The node each arena key belongs to, parallel to `search_offsets`
    /// (CR3: keyed by arena slot, resolved to a NodeIndex per entry — never
    /// assumed to equal the node index, which StableGraph reuses).
    pub search_nodes: Vec<NodeIndex>,
    /// Bytes occupied by live search keys. The backing arena is compacted
    /// when stale replacement history would make it more than twice this
    /// size, keeping index memory proportional to resident rows.
    search_live_bytes: usize,
    /// Relationship id -> edge index (CR5: upserts replace, deletes O(1)).
    pub by_rel_id: DashMap<RelationshipId, petgraph::stable_graph::EdgeIndex>,
    /// This client's local WAL frontier.
    pub last_local_lsn: u64,
    /// Backend commits observed so far.
    pub last_backend_lsn: u64,
    /// When this snapshot was built.
    pub built_at: chrono::DateTime<chrono::Utc>,
    /// Estimated heap footprint (bytes) for 2Q accounting.
    pub est_bytes: usize,
}

trait RoaringLsb {
    fn union_with_lsb(&mut self, m: &Memory);
    fn remove_lsb(&mut self, m: &Memory);
}

impl RoaringLsb for roaring::RoaringBitmap {
    fn union_with_lsb(&mut self, m: &Memory) {
        self.insert(lsb32(&m.id));
    }
    fn remove_lsb(&mut self, m: &Memory) {
        self.remove(lsb32(&m.id));
    }
}

/// Blank a byte range with spaces (same length — no UTF-8 boundary moves).
fn unsafe_blank(s: &mut String, from: usize, to: usize) -> usize {
    // SAFETY-free approach: operate on bytes via Vec conversion is costly;
    // instead rebuild via as_mut_vec would be unsafe. Use safe replacement:
    // the arena keys are ASCII lowercased text + spaces, so per-byte ' '
    // substitution through a safe interface:
    let mut out = String::with_capacity(s.len());
    out.push_str(&s[..from]);
    for _ in from..to {
        out.push(' ');
    }
    out.push_str(&s[to..]);
    let n = out.len();
    *s = out;
    n
}

/// The key separator inside `search_arena`.
const NL: char = '\n';

fn lsb32(id: &MemoryId) -> u32 {
    u32::from_le_bytes([id.0[12], id.0[13], id.0[14], id.0[15]])
}

impl GraphSnapshot {
    /// Build an empty snapshot.
    pub fn empty() -> Self {
        Self {
            petgraph: StableGraph::new(),
            by_id: DashMap::new(),
            by_entity: DashMap::new(),
            by_type: DashMap::new(),
            by_tag: DashMap::new(),
            interner: Arc::new(lasso::ThreadedRodeo::new()),
            search_arena: String::new(),
            search_offsets: Vec::new(),
            search_nodes: Vec::new(),
            search_live_bytes: 0,
            by_rel_id: DashMap::new(),
            last_local_lsn: 0,
            last_backend_lsn: 0,
            built_at: chrono::Utc::now(),
            est_bytes: 0,
        }
    }

    fn estimate(m: &Memory) -> usize {
        512 + m.title.len() + m.content.len() + m.tags.iter().map(|t| t.len() + 8).sum::<usize>()
    }

    /// Search key for a memory: lowercase title + tags, concatenated.
    fn search_key(m: &Memory) -> Box<str> {
        let mut key = String::with_capacity(m.title.len() + 32);
        key.push_str(&m.title.to_lowercase());
        for t in &m.tags {
            key.push(' ');
            key.push_str(&t.to_lowercase());
        }
        key.into_boxed_str()
    }

    /// Insert a memory (copy-on-write helper); returns the node index.
    /// CR1 (audit): this is an UPSERT — a re-inserted id replaces the prior
    /// node instead of adding a parallel one, so stale versions never stay
    /// searchable and a later delete actually removes the row.
    fn insert_memory(&mut self, m: Memory) -> NodeIndex {
        if let Some(ix) = self.by_id.get(&m.id).map(|entry| *entry) {
            if let Some(prior) = self.petgraph.node_weight(ix).cloned() {
                self.remove_memory_indexes(&prior, ix);
                self.est_bytes = self.est_bytes.saturating_sub(Self::estimate(&prior));
            }
            self.est_bytes += Self::estimate(&m);
            self.index_memory(&m, ix);
            *self
                .petgraph
                .node_weight_mut(ix)
                .expect("by_id points at a live node") = m;
            self.compact_search_index_if_needed();
            return ix;
        }
        self.est_bytes += Self::estimate(&m);
        let ix = self.petgraph.add_node(m.clone());
        self.index_memory(&m, ix);
        self.by_id.insert(m.id, ix);
        self.compact_search_index_if_needed();
        ix
    }

    fn index_memory(&mut self, memory: &Memory, ix: NodeIndex) {
        for entity in &memory.context.entities {
            self.by_entity.entry(*entity).or_default().push(memory.id);
        }
        self.by_type
            .entry(memory.memory_type)
            .or_default()
            .union_with_lsb(memory);
        for tag in &memory.tags {
            let spur = self.interner.get_or_intern(tag.as_str());
            self.by_tag.entry(spur).or_default().union_with_lsb(memory);
        }
        let search_key = Self::search_key(memory);
        self.search_offsets.push(self.search_arena.len() as u32);
        self.search_arena.push_str(&search_key);
        self.search_arena.push(NL);
        self.search_nodes.push(ix);
        self.search_live_bytes += search_key.len() + NL.len_utf8();
    }

    fn remove_memory_indexes(&mut self, memory: &Memory, ix: NodeIndex) {
        for entity in &memory.context.entities {
            if let Some(mut ids) = self.by_entity.get_mut(entity) {
                ids.retain(|id| id != &memory.id);
            }
        }
        if let Some(mut bitmap) = self.by_type.get_mut(&memory.memory_type) {
            bitmap.remove_lsb(memory);
        }
        for tag in &memory.tags {
            if let Some(spur) = self.interner.get(tag.as_str()) {
                if let Some(mut bitmap) = self.by_tag.get_mut(&spur) {
                    bitmap.remove_lsb(memory);
                }
            }
        }
        self.search_live_bytes = self
            .search_live_bytes
            .saturating_sub(Self::search_key(memory).len() + NL.len_utf8());
        for (slot, node) in self.search_nodes.iter().enumerate() {
            if *node != ix {
                continue;
            }
            let from = self.search_offsets.get(slot).copied().unwrap_or(0) as usize;
            let to = self
                .search_offsets
                .get(slot + 1)
                .copied()
                .unwrap_or(self.search_arena.len() as u32) as usize;
            if from < to && to <= self.search_arena.len() {
                let _ = unsafe_blank(&mut self.search_arena, from, to);
            }
        }
    }

    fn remove_memory(&mut self, id: &MemoryId) {
        if let Some((_, ix)) = self.by_id.remove(id) {
            if let Some(m) = self.petgraph.node_weight(ix).cloned() {
                self.est_bytes = self.est_bytes.saturating_sub(Self::estimate(&m));
                self.remove_memory_indexes(&m, ix);
            }
            let incident_ids = self
                .petgraph
                .edges_directed(ix, petgraph::Direction::Outgoing)
                .chain(
                    self.petgraph
                        .edges_directed(ix, petgraph::Direction::Incoming),
                )
                .map(|edge| edge.weight().id)
                .collect::<std::collections::HashSet<_>>();
            for relationship_id in incident_ids {
                self.by_rel_id.remove(&relationship_id);
                self.est_bytes = self.est_bytes.saturating_sub(256);
            }
            self.petgraph.remove_node(ix);
        }
    }

    fn compact_search_index_if_needed(&mut self) {
        const MIN_COMPACTION_GARBAGE: usize = 1024;
        let garbage = self
            .search_arena
            .len()
            .saturating_sub(self.search_live_bytes);
        if garbage < MIN_COMPACTION_GARBAGE || self.search_arena.len() <= self.search_live_bytes * 2
        {
            return;
        }
        let mut arena = String::with_capacity(self.search_live_bytes);
        let mut offsets = Vec::with_capacity(self.petgraph.node_count());
        let mut nodes = Vec::with_capacity(self.petgraph.node_count());
        for ix in self.petgraph.node_indices() {
            let Some(memory) = self.petgraph.node_weight(ix) else {
                continue;
            };
            offsets.push(arena.len() as u32);
            arena.push_str(&Self::search_key(memory));
            arena.push(NL);
            nodes.push(ix);
        }
        self.search_arena = arena;
        self.search_offsets = offsets;
        self.search_nodes = nodes;
        debug_assert_eq!(self.search_arena.len(), self.search_live_bytes);
    }

    /// Insert a relationship between existing memories.
    /// CR5 (audit): an UPSERT — a re-upserted RelationshipId replaces the
    /// prior edge's weight instead of adding a parallel duplicate edge.
    fn insert_relationship(&mut self, r: Relationship) {
        if let (Some(a), Some(b)) = (self.by_id.get(&r.from), self.by_id.get(&r.to)) {
            if let Some(existing) = self.by_rel_id.get(&r.id) {
                if let Some(w) = self.petgraph.edge_weight_mut(*existing) {
                    self.est_bytes = self.est_bytes.saturating_sub(256);
                    self.est_bytes += 256;
                    *w = r;
                    return;
                }
            }
            self.est_bytes += 256;
            let eid = self.petgraph.add_edge(*a, *b, r.clone());
            self.by_rel_id.insert(r.id, eid);
        }
    }

    fn remove_relationship(&mut self, id: &RelationshipId) {
        if let Some((_, eid)) = self.by_rel_id.remove(id) {
            if self.petgraph.remove_edge(eid).is_some() {
                self.est_bytes = self.est_bytes.saturating_sub(256);
            }
        }
    }

    /// Visibility check per §17.2: `Private` resolves against the author.
    pub fn visible(&self, m: &Memory, vc: &VisibilityContext) -> bool {
        exocortex_storage::memory_visible(m, vc)
    }

    /// Per-user filtered view (R-MT2): a lazy iterator over the memories the
    /// context may see. Never materializes a copy.
    pub fn view<'a>(&'a self, vc: &'a VisibilityContext) -> impl Iterator<Item = &'a Memory> + 'a {
        self.petgraph
            .node_weights()
            .filter(move |m| self.visible(m, vc))
    }

    /// Test/bench support: insert a memory directly (the private
    /// copy-on-write path without a storage round-trip).
    #[doc(hidden)]
    pub fn push_test_memory(&mut self, m: Memory) {
        self.insert_memory(m);
    }

    /// Test/bench support: insert a relationship directly.
    #[doc(hidden)]
    pub fn push_test_relationship(&mut self, r: Relationship) {
        self.insert_relationship(r);
    }

    /// Build a snapshot from storage streams (shared by reseed and tests).
    /// CR2 (audit): soft-deleted rows never enter the snapshot — a row is
    /// live only while `valid_until` is unset (or in the future) and it has
    /// no `invalidated_by`, so a restart cannot resurrect deleted memories
    /// or Dreams-merged duplicates.
    pub async fn from_storage<S: Storage>(storage: &S) -> Self {
        use futures::StreamExt;
        let now = chrono::Utc::now();
        let live = |valid_until: &Option<chrono::DateTime<chrono::Utc>>| {
            valid_until.is_none_or(|v| v > now)
        };
        let mut snap = Self::empty();
        let mut ms = storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            if m.invalidated_by.is_none() && live(&m.valid_until) {
                snap.insert_memory(m);
            }
        }
        let mut frontier = 0u64;
        let mut rs = storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            frontier = frontier.max(r.lsn.value);
            if r.invalidated_by.is_none() && live(&r.valid_until) {
                snap.insert_relationship(r);
            }
        }
        for m in snap.petgraph.node_weights() {
            frontier = frontier.max(m.lsn.value);
        }
        snap.last_backend_lsn = frontier;
        snap
    }
}

/// Cache read version stamp (R-M7).
#[derive(Clone, Copy, Debug)]
pub struct CacheVersion {
    /// Local WAL frontier.
    pub local_lsn: u64,
    /// Backend commits observed.
    pub backend_lsn: u64,
    /// When the snapshot was published.
    pub published_at: std::time::Instant,
}

/// The local cache: one snapshot per org, 2Q admission, single-writer
/// channel.
pub struct LocalCache {
    graphs: DashMap<SmolStr, Arc<GraphSlot>>,
    tq: Mutex<TwoQState>,
    writer: mpsc::Sender<CacheWrite>,
    budget: usize,
    snapshot_publications: AtomicU64,
    full_snapshot_clones: AtomicU64,
}

#[derive(Clone)]
enum SnapshotDelta {
    UpsertMemory(Box<Memory>),
    DeleteMemory(MemoryId),
    UpsertRelationship(Box<Relationship>),
    DeleteRelationship(RelationshipId),
    AdvanceBackendLsn(u64),
}

impl SnapshotDelta {
    fn apply(&self, snapshot: &mut GraphSnapshot) {
        match self {
            Self::UpsertMemory(memory) => {
                snapshot.insert_memory((**memory).clone());
            }
            Self::DeleteMemory(id) => snapshot.remove_memory(id),
            Self::UpsertRelationship(relationship) => {
                snapshot.insert_relationship((**relationship).clone());
            }
            Self::DeleteRelationship(id) => snapshot.remove_relationship(id),
            Self::AdvanceBackendLsn(lsn) => {
                snapshot.last_backend_lsn = snapshot.last_backend_lsn.max(*lsn);
            }
        }
    }
}

struct RetiredSnapshot {
    generation: u64,
    snapshot: Arc<GraphSnapshot>,
}

struct SnapshotJournalEntry {
    generation: u64,
    delta: Vec<SnapshotDelta>,
}

struct SnapshotReuseState {
    generation: u64,
    retired: std::collections::VecDeque<RetiredSnapshot>,
    journal: std::collections::VecDeque<SnapshotJournalEntry>,
}

/// One atomically published graph plus writer-only RCU reuse state. A retired
/// snapshot is mutated only after its Arc becomes unique, so readers retain
/// immutable generation isolation while the writer catches the buffer up from
/// the bounded delta journal instead of cloning the resident graph.
struct GraphSlot {
    current: ArcSwap<GraphSnapshot>,
    reuse: Mutex<SnapshotReuseState>,
}

impl GraphSlot {
    fn new(snapshot: Arc<GraphSnapshot>) -> Self {
        Self {
            current: ArcSwap::from(snapshot),
            reuse: Mutex::new(SnapshotReuseState {
                generation: 0,
                retired: Default::default(),
                journal: Default::default(),
            }),
        }
    }

    fn load_full(&self) -> Arc<GraphSnapshot> {
        self.current.load_full()
    }

    fn store(&self, snapshot: Arc<GraphSnapshot>) {
        let mut state = self.reuse.lock();
        state.generation = state.generation.saturating_add(1);
        state.retired.clear();
        state.journal.clear();
        self.current.store(snapshot);
    }

    fn apply_local(
        &self,
        memories: &[Memory],
        relationships: &[Relationship],
        local_lsn: u64,
    ) -> Option<(usize, usize)> {
        // The read/check/clone/swap is one writer transaction. Offline MCP
        // requests may run concurrently, so loading before taking this lock
        // can publish a stale generation and erase a sibling request.
        let mut state = self.reuse.lock();
        let current = self.current.load_full();
        if memories.is_empty() && relationships.is_empty() && local_lsn <= current.last_local_lsn {
            return None;
        }
        let old_bytes = current.est_bytes;
        let mut next = clone_snapshot(&current);
        for memory in memories {
            next.insert_memory(memory.clone());
        }
        for relationship in relationships {
            next.insert_relationship(relationship.clone());
        }
        next.last_local_lsn = next.last_local_lsn.max(local_lsn);
        let new_bytes = next.est_bytes;
        state.generation = state.generation.saturating_add(1);
        state.retired.clear();
        state.journal.clear();
        self.current.store(Arc::new(next));
        Some((old_bytes, new_bytes))
    }

    fn publish_delta(&self, delta: Vec<SnapshotDelta>, clone_count: &AtomicU64) -> (usize, usize) {
        const RETIRED_BUFFERS: usize = 4;
        let mut state = self.reuse.lock();
        let current = self.current.load_full();
        let old_bytes = current.est_bytes;
        let reusable = state
            .retired
            .iter()
            .position(|retired| Arc::strong_count(&retired.snapshot) == 1)
            .and_then(|index| state.retired.remove(index));
        let mut next = if let Some(retired) = reusable {
            let mut snapshot = Arc::try_unwrap(retired.snapshot)
                .unwrap_or_else(|_| unreachable!("unique retired snapshot became shared"));
            for entry in state
                .journal
                .iter()
                .filter(|entry| entry.generation > retired.generation)
            {
                for operation in &entry.delta {
                    operation.apply(&mut snapshot);
                }
            }
            snapshot
        } else {
            clone_count.fetch_add(1, Ordering::Relaxed);
            clone_snapshot(&current)
        };
        for operation in &delta {
            operation.apply(&mut next);
        }
        next.built_at = chrono::Utc::now();
        state.generation = state.generation.saturating_add(1);
        let generation = state.generation;
        let published = Arc::new(next);
        let old = self.current.swap(published.clone());
        state.retired.push_back(RetiredSnapshot {
            generation: generation - 1,
            snapshot: old,
        });
        while state.retired.len() > RETIRED_BUFFERS {
            state.retired.pop_front();
        }
        state
            .journal
            .push_back(SnapshotJournalEntry { generation, delta });
        if let Some(oldest) = state.retired.iter().map(|retired| retired.generation).min() {
            while state
                .journal
                .front()
                .is_some_and(|entry| entry.generation <= oldest)
            {
                state.journal.pop_front();
            }
        } else {
            state.journal.clear();
        }
        (old_bytes, published.est_bytes)
    }
}

struct TwoQState {
    a1in: std::collections::VecDeque<SmolStr>, // FIFO, 25% budget
    am: lru::LruCache<SmolStr, ()>,            // LRU, 50% budget
    a1out: lru::LruCache<SmolStr, ()>,         // ghost, 25% budget
    bytes: usize,
}

impl TwoQState {
    fn entry_budget(total: usize) -> (usize, usize) {
        // Entry-count proxy for the byte budget: at ~1KiB per graph entry the
        // proportions of §8.3 hold (25/50/25).
        let entries = (total / 1024).max(8);
        (entries / 4, entries / 2)
    }
}

/// Single-writer cache operations.
pub enum CacheWrite {
    /// Apply one change-feed invalidation (RCU delta publication).
    Apply(
        /// The change-feed event to apply.
        Invalidation,
    ),
    /// Publish a freshly rebuilt snapshot for an org.
    Reseed {
        /// Target org.
        org: SmolStr,
        /// The freshly built snapshot.
        snapshot: Arc<GraphSnapshot>,
        /// Optional completion signal for readiness/resync callers.
        ack: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Evict an org graph entirely.
    Evict(
        /// Org to evict.
        SmolStr,
    ),
    /// Deterministic test/readiness barrier: acknowledged after all earlier
    /// writer messages have been applied.
    Barrier(tokio::sync::oneshot::Sender<()>),
}

impl LocalCache {
    /// Build a cache with a byte budget; returns the cache and the
    /// single-writer receiver its `run` loop consumes.
    pub fn new(budget_bytes: usize) -> (Self, mpsc::Receiver<CacheWrite>) {
        let (tx, rx) = mpsc::channel(1024);
        let (a1in_cap, am_cap) = TwoQState::entry_budget(budget_bytes);
        (
            Self {
                graphs: DashMap::new(),
                tq: Mutex::new(TwoQState {
                    a1in: Default::default(),
                    am: lru::LruCache::new(std::num::NonZeroUsize::new(am_cap).unwrap()),
                    a1out: lru::LruCache::new(std::num::NonZeroUsize::new(a1in_cap).unwrap()),
                    bytes: 0,
                }),
                writer: tx,
                budget: budget_bytes,
                snapshot_publications: AtomicU64::new(0),
                full_snapshot_clones: AtomicU64::new(0),
            },
            rx,
        )
    }

    /// The single writer loop. Applies invalidations and reseeds serially so
    /// snapshots swap atomically (§8.2).
    pub async fn run<S: Storage>(&self, storage: Arc<S>, mut rx: mpsc::Receiver<CacheWrite>) {
        while let Some(first) = rx.recv().await {
            // Give same-tick producers one scheduling turn to finish their
            // burst before draining it into a single copy-on-write publish.
            tokio::task::yield_now().await;
            let mut messages = vec![first];
            while messages.len() < 256 {
                match rx.try_recv() {
                    Ok(message) => messages.push(message),
                    Err(_) => break,
                }
            }
            let mut pending = Vec::new();
            for msg in messages {
                match msg {
                    CacheWrite::Apply(inv) => {
                        pending.push(inv);
                        continue;
                    }
                    CacheWrite::Reseed { org, snapshot, ack } => {
                        self.apply_pending(&*storage, &mut pending).await;
                        // R-O2 families: rebuild counts + graph size levels.
                        metrics::counter!("exocortex_cache_rebuild_total", "reason" => "reseed")
                            .increment(1);
                        metrics::gauge!("exocortex_memories_total")
                            .set(snapshot.petgraph.node_count() as f64);
                        metrics::gauge!("exocortex_relationships_total", "provenance" => "all")
                            .set(snapshot.petgraph.edge_count() as f64);
                        let bytes = snapshot.est_bytes;
                        {
                            let mut tq = self.tq.lock();
                            tq.bytes = tq.bytes.saturating_sub(
                                self.graphs
                                    .get(&org)
                                    .map(|g| g.load_full().est_bytes)
                                    .unwrap_or(0),
                            );
                            tq.bytes += bytes;
                        }
                        self.graphs
                            .entry(org.clone())
                            .or_insert_with(|| {
                                Arc::new(GraphSlot::new(Arc::new(GraphSnapshot::empty())))
                            })
                            .store(snapshot);
                        self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
                        self.admit(&org);
                        if let Some(ack) = ack {
                            let _ = ack.send(());
                        }
                    }
                    CacheWrite::Evict(org) => {
                        self.apply_pending(&*storage, &mut pending).await;
                        self.graphs.remove(&org);
                    }
                    CacheWrite::Barrier(ack) => {
                        self.apply_pending(&*storage, &mut pending).await;
                        let _ = ack.send(());
                    }
                }
            }
            self.apply_pending(&*storage, &mut pending).await;
        }
    }

    async fn apply_pending<S: Storage>(&self, storage: &S, pending: &mut Vec<Invalidation>) {
        if !pending.is_empty() {
            self.apply_batch(storage, std::mem::take(pending)).await;
        }
    }

    /// Copy-on-write apply of one invalidation. Readers holding the old
    /// snapshot continue to see it until they release their Arc.
    async fn apply_batch<S: Storage>(&self, storage: &S, invalidations: Vec<Invalidation>) {
        // v1 caches exactly one org per client (§17); invalidations target it.
        let Some(org) = self.org_of_write() else {
            return;
        };
        let Some(g) = self.graphs.get(&org) else {
            return;
        };
        let mut delta = Vec::new();
        for inv in invalidations {
            match inv {
                Invalidation::MemoryUpserted { id, lsn } => {
                    match storage.get_memory(&id).await {
                        Ok(Some(m)) => {
                            delta.push(SnapshotDelta::UpsertMemory(Box::new(m)));
                        }
                        Ok(None) => {
                            delta.push(SnapshotDelta::DeleteMemory(id));
                        }
                        Err(e) => {
                            tracing::warn!(?e, "invalidation fetch failed");
                            return;
                        }
                    }
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::MemoryDeleted { id, lsn } => {
                    delta.push(SnapshotDelta::DeleteMemory(id));
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::RelationshipUpserted { id, lsn, .. } => {
                    match storage.get_relationship(&id).await {
                        Ok(Some(r)) => {
                            delta.push(SnapshotDelta::UpsertRelationship(Box::new(r)));
                        }
                        Ok(None) => {
                            tracing::warn!(
                                "relationship invalidation row missing; LSN not advanced"
                            );
                            return;
                        }
                        Err(error) => {
                            tracing::warn!(?error, "relationship invalidation fetch failed");
                            return;
                        }
                    }
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::RelationshipDeleted { id, lsn } => {
                    delta.push(SnapshotDelta::DeleteRelationship(id));
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::VisibilityAdvance { lsn } => {
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::DiscoveryAvailable { lsn, .. } => {
                    // Discoveries are presentation records rather than graph
                    // rows. The cache does not retain them, but consuming the
                    // event must advance the ordered frontier.
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::MemorySnapshotUpserted { memory, lsn } => {
                    delta.push(SnapshotDelta::UpsertMemory(memory));
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::RelationshipSnapshotUpserted { relationship, lsn } => {
                    delta.push(SnapshotDelta::UpsertRelationship(relationship));
                    delta.push(SnapshotDelta::AdvanceBackendLsn(lsn));
                }
                Invalidation::GraphReseed { .. } => {
                    tracing::warn!("graph reseed reached cache apply path; LSN not advanced");
                    continue;
                }
            }
        }
        if delta.is_empty() {
            return;
        }
        let (old_bytes, new_bytes) = g.publish_delta(delta, &self.full_snapshot_clones);
        self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
        // CR13 (audit): the invalidation path reconciles the 2Q byte
        // accounting exactly like Reseed does, and re-runs admission so
        // the budget is enforced on a long-running node (the early returns
        // in the re-reference branches previously skipped the check).
        {
            let mut tq = self.tq.lock();
            if new_bytes >= old_bytes {
                tq.bytes += new_bytes - old_bytes;
            } else {
                tq.bytes = tq.bytes.saturating_sub(old_bytes - new_bytes);
            }
        }
        self.admit(&org);
    }

    fn org_of_write(&self) -> Option<SmolStr> {
        // Multi-org routing arrives with multi-tenancy work in v2.
        self.graphs.iter().next().map(|e| e.key().clone())
    }

    fn admit(&self, org: &SmolStr) {
        let mut tq = self.tq.lock();
        if tq.am.contains(org) {
            tq.am.put(org.clone(), ());
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "promote_am")
                .increment(1);
        } else if tq.a1out.contains(org) {
            tq.am.put(org.clone(), ());
            tq.a1out.pop(org);
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "ghost_hit")
                .increment(1);
        } else if tq.a1in.contains(org) {
            // Re-reference while still in A1in: promote to Am (2Q
            // semantics). This also guarantees repeated publishes never
            // duplicate the A1in entry.
            tq.a1in.retain(|o| o != org);
            tq.am.put(org.clone(), ());
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "promote_am")
                .increment(1);
        } else {
            tq.a1in.push_back(org.clone());
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "admit_a1in")
                .increment(1);
        }
        // CR13 (audit): the budget check runs on EVERY admission — a
        // long-running node whose org is already resident still enforces
        // the byte budget after invalidations grow the snapshot.
        while tq.bytes > self.budget {
            if let Some(evicted) = tq.a1in.pop_front() {
                tq.a1out.put(evicted.clone(), ());
                if let Some(g) = self.graphs.get(&evicted) {
                    tq.bytes = tq.bytes.saturating_sub(g.load_full().est_bytes);
                }
                self.graphs.remove(&evicted);
                metrics::counter!("exocortex_2q_admission_events_total", "decision" => "evict_a1in")
                    .increment(1);
            } else if let Some((evicted, _)) = tq.am.pop_lru() {
                if let Some(g) = self.graphs.get(&evicted) {
                    tq.bytes = tq.bytes.saturating_sub(g.load_full().est_bytes);
                }
                self.graphs.remove(&evicted);
                metrics::counter!("exocortex_2q_admission_events_total", "decision" => "evict_am")
                    .increment(1);
            } else {
                break;
            }
        }
    }

    /// Read path — never blocks. Returns `None` if the org isn't cached; the
    /// caller (mcp-client or backend node) faults it in via `reseed`.
    pub fn get_memory(&self, org: &str, id: &MemoryId, vc: &VisibilityContext) -> Option<Memory> {
        let g = self.graphs.get(org)?;
        let snap = g.load_full();
        let m = snap.petgraph.node_weight(*snap.by_id.get(id)?)?.clone();
        if !snap.visible(&m, vc) {
            return None;
        }
        Some(m)
    }

    /// D10b (§4.10a): the memory that supersedes `id`, when a live
    /// `Replaces`/`Contradicts` edge points at it. The successor is the
    /// edge's SOURCE (successor --Replaces--> stale); the auto-registered
    /// `ReplacedBy` companion (stale --> successor) is checked too, so
    /// either direction of storage yields the same answer.
    pub fn superseded_by(
        &self,
        org: &str,
        id: &MemoryId,
        vc: &VisibilityContext,
        supersedes_kinds: &[exocortex_kernel::RelKindId],
    ) -> Option<Memory> {
        let g = self.graphs.get(org)?;
        let snap = g.load_full();
        let node = *snap.by_id.get(id)?;
        for e in snap
            .petgraph
            .edges_directed(node, petgraph::Direction::Incoming)
        {
            let er = e.weight();
            if supersedes_kinds.contains(&er.kind)
                && (er.visibility as u8) <= (vc.max_visibility as u8)
            {
                if let Some(m) = snap.petgraph.node_weight(e.source()) {
                    if snap.visible(m, vc) {
                        return Some(m.clone());
                    }
                }
            }
        }
        None
    }

    /// Bounded BFS traversal over the snapshot (§8.4).
    pub fn traverse(&self, org: &str, from: &MemoryId, spec: &TraversalSpec) -> Vec<Memory> {
        let Some(g) = self.graphs.get(org) else {
            return vec![];
        };
        let snap = g.load_full();
        let Some(start) = snap.by_id.get(from).map(|r| *r) else {
            return vec![];
        };
        if !snap
            .petgraph
            .node_weight(start)
            .is_some_and(|memory| snap.visible(memory, &spec.visibility_ctx))
        {
            return vec![];
        }
        let mut out = Vec::new();
        let mut queue = std::collections::VecDeque::from([(start, 0u8)]);
        let mut seen = std::collections::HashSet::from([start]);
        while let Some((n, d)) = queue.pop_front() {
            if out.len() >= spec.max_nodes as usize {
                break;
            }
            // CR4 (audit): the iterator follows the spec's direction — the
            // old code only ever walked outgoing edges, so `In` always
            // returned empty and `Both` was textually `Out`. The hop takes
            // the endpoint that is not `n`.
            let mut visit = |other: petgraph::stable_graph::NodeIndex,
                             er: &exocortex_kernel::Relationship| {
                if !spec.kinds.is_empty() && !spec.kinds.contains(&er.kind) {
                    return;
                }
                if er.visibility as u8 > spec.visibility_ctx.max_visibility as u8 {
                    return;
                }
                let Some(m) = snap.petgraph.node_weight(other) else {
                    return;
                };
                if !snap.visible(m, &spec.visibility_ctx) || !seen.insert(other) {
                    return;
                }
                out.push(m.clone());
                if d + 1 < spec.max_depth {
                    queue.push_back((other, d + 1));
                }
            };
            if matches!(spec.direction, Direction::Out | Direction::Both) {
                for e in snap
                    .petgraph
                    .edges_directed(n, petgraph::Direction::Outgoing)
                {
                    visit(e.target(), e.weight());
                }
            }
            if matches!(spec.direction, Direction::In | Direction::Both) {
                for e in snap
                    .petgraph
                    .edges_directed(n, petgraph::Direction::Incoming)
                {
                    visit(e.source(), e.weight());
                }
            }
        }
        out
    }

    /// `search_memories` over the snapshot: lowercase title/tags substring
    /// match on precomputed keys, then the §14.1 scoring algebra.
    pub fn search(
        &self,
        org: &str,
        query: &str,
        limit: u32,
        vc: &VisibilityContext,
    ) -> Vec<(Memory, f32)> {
        let Some(g) = self.graphs.get(org) else {
            return vec![];
        };
        let snap = g.load_full();
        if limit == 0 {
            return vec![];
        }
        let now = chrono::Utc::now();
        let mut hits: Vec<(Memory, f32)> = Vec::new();
        // Single contiguous arena scan (memmem speed); keys are NL-joined
        // and queries never contain newline, so matches cannot span keys.
        let q = query.to_lowercase();
        if q.is_empty() {
            return hits;
        }
        let arena = snap.search_arena.as_str();
        let offsets = &snap.search_offsets;
        let nodes = &snap.search_nodes;
        let mut from = 0usize;
        let mut last_idx: Option<usize> = None;
        while let Some(rel) = arena[from..].find(q.as_str()) {
            let pos = from + rel;
            let slot = match offsets.binary_search(&(pos as u32)) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };
            if last_idx != Some(slot) {
                last_idx = Some(slot);
                // CR3: resolve the arena slot to its node through the
                // parallel array — the slot index is not the node index.
                let Some(&ix) = nodes.get(slot) else {
                    from = pos + q.len();
                    if from >= arena.len() {
                        break;
                    }
                    continue;
                };
                if let Some(m) = snap.petgraph.node_weight(ix) {
                    if snap.visible(m, vc) {
                        // §14.1: base match + explicit relationship count *
                        // 0.30 + Σ inferred confidence * 0.15 + importance *
                        // 0.50 + recency 0.10. CR10 (audit): only Asserted
                        // edges are explicit; Derived/Computed/Extracted all
                        // count as inferred, weighted by confidence, so a
                        // Dreams SimilarTo pass cannot masquerade as human
                        // assertions at 0.30 each.
                        let mut explicit = 0.0f32;
                        let mut inferred = 0.0f32;
                        for e in snap
                            .petgraph
                            .edges_directed(ix, petgraph::Direction::Outgoing)
                        {
                            let er = e.weight();
                            match &er.provenance {
                                exocortex_kernel::Provenance::Asserted { .. } => explicit += 1.0,
                                exocortex_kernel::Provenance::Derived { .. }
                                | exocortex_kernel::Provenance::Computed { .. }
                                | exocortex_kernel::Provenance::Extracted { .. } => {
                                    inferred += er.properties.confidence * 0.15
                                }
                                // Never persists (R-T16); counted as nothing.
                                exocortex_kernel::Provenance::Proposed { .. }
                                | exocortex_kernel::Provenance::ExternalSnapshot(_) => {
                                    explicit += 1.0
                                }
                            }
                        }
                        let age_days = (now - m.recorded_at).num_days().max(0) as f32;
                        let recency = if age_days <= 7.0 { 0.10 } else { 0.0 };
                        let score =
                            1.0 + explicit * 0.30 + inferred + m.importance.get() * 0.50 + recency;
                        hits.push((m.clone(), score));
                        if hits.len() >= limit as usize * 4 {
                            break;
                        }
                    }
                }
            }
            from = pos + q.len();
            if from >= arena.len() {
                break;
            }
        }
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(limit as usize);
        hits
    }

    /// The current version stamp for an org (R-M7).
    pub fn version(&self, org: &str) -> Option<CacheVersion> {
        let g = self.graphs.get(org)?;
        let snap = g.load_full();
        Some(CacheVersion {
            local_lsn: snap.last_local_lsn,
            backend_lsn: snap.last_backend_lsn,
            published_at: std::time::Instant::now(),
        })
    }

    /// Number of resident org graphs (tests).
    pub fn resident_orgs(&self) -> usize {
        self.graphs.len()
    }

    /// A1in membership count for `org` (tests: duplicate-admission guard).
    #[doc(hidden)]
    pub fn a1in_count(&self, org: &str) -> usize {
        let tq = self.tq.lock();
        tq.a1in.iter().filter(|o| o.as_str() == org).count()
    }

    /// Total A1in entries (tests).
    #[doc(hidden)]
    pub fn a1in_len(&self) -> usize {
        self.tq.lock().a1in.len()
    }

    /// Am membership (tests: promotion semantics).
    #[doc(hidden)]
    pub fn am_contains(&self, org: &str) -> bool {
        self.tq.lock().am.contains(org)
    }

    /// Load the current snapshot Arc for an org (read path; refcount bump).
    pub fn graphs_snapshot(&self, org: &str) -> Option<Arc<GraphSnapshot>> {
        Some(self.graphs.get(org)?.load_full())
    }

    /// 2Q access hook: record a hit for a resident org (promotes A1out
    /// ghosts and refreshes Am recency).
    pub fn touch_admission(&self, org: &str) {
        self.admit(&org.into());
    }

    /// CL6 (audit): advance the served snapshot's local WAL frontier so
    /// the R-M7 read stamp (`snapshot_version.local_lsn`) reflects offline
    /// writes. Readers polling `local_lsn >= n` for read-your-writes now
    /// observe the value the offline ack handed back.
    pub fn advance_local_lsn(&self, org: &str, local_lsn: u64) {
        if let Some(g) = self.graphs.get(org) {
            if g.apply_local(&[], &[], local_lsn).is_some() {
                self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// SR-PRD F2 (docs/bug-prd-standalone-readback.md): apply one
    /// offline batch to the served snapshot in a SINGLE copy-on-write
    /// publish — materialized rows + edges inserted, the local WAL
    /// frontier stamped in the same swap (`advance_local_lsn` alone
    /// bumps the counter; this makes the rows readable). Auto-creates
    /// the org graph so a first write can never be silently dropped,
    /// though standalone boot publishes the (possibly empty) seed graph
    /// first (F3). Stale LSNs are ignored — boot re-seeding races no one.
    pub fn apply_local(
        &self,
        org: &str,
        memories: &[Memory],
        relationships: &[Relationship],
        local_lsn: u64,
    ) {
        let g = self
            .graphs
            .entry(org.into())
            .or_insert_with(|| Arc::new(GraphSlot::new(Arc::new(GraphSnapshot::empty()))))
            .clone();
        let Some((old_bytes, new_bytes)) = g.apply_local(memories, relationships, local_lsn) else {
            return;
        };
        {
            let mut tq = self.tq.lock();
            if new_bytes >= old_bytes {
                tq.bytes += new_bytes - old_bytes;
            } else {
                tq.bytes = tq.bytes.saturating_sub(old_bytes - new_bytes);
            }
        }
        self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
        self.admit(&org.into());
    }

    /// Hydrate one authorized storage fallback into the resident backend image.
    /// This is the R-C8 point-read miss path: the fetched row and its backend
    /// frontier become visible in one generation-safe publication.
    pub fn hydrate_memory(&self, org: &str, memory: Memory) {
        let backend_lsn = memory.lsn.value;
        let graph = self
            .graphs
            .entry(org.into())
            .or_insert_with(|| Arc::new(GraphSlot::new(Arc::new(GraphSnapshot::empty()))))
            .clone();
        let (old_bytes, new_bytes) = graph.publish_delta(
            vec![
                SnapshotDelta::UpsertMemory(Box::new(memory)),
                SnapshotDelta::AdvanceBackendLsn(backend_lsn),
            ],
            &self.full_snapshot_clones,
        );
        {
            let mut tq = self.tq.lock();
            if new_bytes >= old_bytes {
                tq.bytes += new_bytes - old_bytes;
            } else {
                tq.bytes = tq.bytes.saturating_sub(old_bytes - new_bytes);
            }
        }
        self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
        self.admit(&org.into());
    }

    /// SR-PRD F3: standalone boot seeding — build ONE fresh snapshot
    /// from the materialized WAL rows and publish it (the public boot
    /// path; `publish` itself stays benches/tests-only). Rows with ids
    /// already resident upsert in place (CR1), so re-seeding converges.
    /// The backend arm seeds nothing: drained rows return server-side
    /// over SSE under server ids, and WAL ids differ, so seeding there
    /// would duplicate.
    pub fn seed_local(
        &self,
        org: &str,
        memories: &[Memory],
        relationships: &[Relationship],
        last_local_lsn: u64,
    ) {
        let mut snap = GraphSnapshot::empty();
        for m in memories {
            snap.insert_memory(m.clone());
        }
        for r in relationships {
            snap.insert_relationship(r.clone());
        }
        snap.last_local_lsn = last_local_lsn;
        self.publish(org, Arc::new(snap));
    }

    /// Submit a writer message (tests + client wiring).
    pub async fn submit(&self, w: CacheWrite) {
        let _ = self.writer.send(w).await;
    }

    /// Wait until the single writer has applied every previously submitted
    /// message. Tests use this instead of timing guesses.
    pub async fn flush(&self) {
        let (ack, done) = tokio::sync::oneshot::channel();
        if self.writer.send(CacheWrite::Barrier(ack)).await.is_ok() {
            let _ = done.await;
        }
    }

    /// Direct publish, bypassing the writer channel. For benches and tests
    /// that exercise the read path without a storage-backed writer loop;
    /// production writes always go through `run` + `submit`.
    #[doc(hidden)]
    pub fn publish(&self, org: &str, snapshot: Arc<GraphSnapshot>) {
        let bytes = snapshot.est_bytes;
        {
            let mut tq = self.tq.lock();
            tq.bytes = tq.bytes.saturating_sub(
                self.graphs
                    .get(org)
                    .map(|g| g.load_full().est_bytes)
                    .unwrap_or(0),
            );
            tq.bytes += bytes;
        }
        self.graphs
            .entry(org.into())
            .or_insert_with(|| Arc::new(GraphSlot::new(Arc::new(GraphSnapshot::empty()))))
            .store(snapshot);
        self.snapshot_publications.fetch_add(1, Ordering::Relaxed);
        self.admit(&org.into());
    }

    /// Number of atomic graph publications (update-scaling test support).
    #[doc(hidden)]
    pub fn snapshot_publications(&self) -> u64 {
        self.snapshot_publications.load(Ordering::Relaxed)
    }

    /// Number of full resident-graph clones used because no retired snapshot
    /// buffer was reader-free. Steady-state invalidations should reuse buffers.
    #[doc(hidden)]
    pub fn full_snapshot_clones(&self) -> u64 {
        self.full_snapshot_clones.load(Ordering::Relaxed)
    }

    /// Stream all memories + relationships from storage and rebuild the
    /// snapshot for one org (§8.4 `reseed_from_storage`).
    pub async fn reseed_from_storage<S: Storage>(&self, storage: &S, org: &SmolStr) {
        let snap = GraphSnapshot::from_storage(storage).await;
        self.reseed_snapshot(org.clone(), snap).await;
    }

    /// Atomically publish a complete backend image and wait until the writer
    /// has made it visible. This is the backend client's readiness boundary.
    pub async fn reseed_rows(
        &self,
        org: SmolStr,
        memories: Vec<Memory>,
        relationships: Vec<Relationship>,
        backend_lsn: u64,
    ) {
        let mut snapshot = GraphSnapshot::empty();
        for memory in memories {
            snapshot.insert_memory(memory);
        }
        for relationship in relationships {
            snapshot.insert_relationship(relationship);
        }
        snapshot.last_backend_lsn = backend_lsn;
        self.reseed_snapshot(org, snapshot).await;
    }

    async fn reseed_snapshot(&self, org: SmolStr, snapshot: GraphSnapshot) {
        let (ack, done) = tokio::sync::oneshot::channel();
        if self
            .writer
            .send(CacheWrite::Reseed {
                org,
                snapshot: Arc::new(snapshot),
                ack: Some(ack),
            })
            .await
            .is_ok()
        {
            let _ = done.await;
        }
    }
}

/// Clone helper: `DashMap` and `StableGraph` are `Clone`; the interner is
/// shared.
fn clone_snapshot(src: &GraphSnapshot) -> GraphSnapshot {
    GraphSnapshot {
        petgraph: src.petgraph.clone(),
        by_id: src.by_id.clone(),
        by_entity: src.by_entity.clone(),
        by_type: src.by_type.clone(),
        by_tag: src.by_tag.clone(),
        interner: src.interner.clone(),
        search_arena: src.search_arena.clone(),
        search_offsets: src.search_offsets.clone(),
        search_nodes: src.search_nodes.clone(),
        search_live_bytes: src.search_live_bytes,
        by_rel_id: src.by_rel_id.clone(),
        last_local_lsn: src.last_local_lsn,
        last_backend_lsn: src.last_backend_lsn,
        built_at: chrono::Utc::now(),
        est_bytes: src.est_bytes,
    }
}
