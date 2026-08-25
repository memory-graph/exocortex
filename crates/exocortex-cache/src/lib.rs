// crates/exocortex-cache/src/lib.rs
//! Lock-free readers, single-writer coordination. The graph itself is a
//! petgraph::StableGraph wrapped in ArcSwap so the read side never blocks.
//!
//! §8: `LocalCache` holds one `GraphSnapshot` per org behind
//! `Arc<ArcSwap<GraphSnapshot>>`; readers `load_full()` (a refcount bump) and
//! scan a consistent view; the single writer applies invalidations by
//! copy-on-write and publishes a fresh `Arc`. 2Q admission governs which org
//! graphs stay resident under the byte budget (R-M12/R-M13).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::EdgeRef;
use smol_str::SmolStr;
use std::sync::Arc;
use tokio::sync::mpsc;

use exocortex_kernel::{EntityId, Memory, MemoryId, Relationship, RelationshipId, Visibility};
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
    /// node-index order, packed into ONE contiguous arena for cache-friendly
    /// scans on the read hot path.
    pub search_arena: String,
    /// Byte offset of each key inside `search_arena` (key i spans
    /// offsets[i]..offsets[i+1] or arena end).
    pub search_offsets: Vec<u32>,
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
    fn insert_memory(&mut self, m: Memory) -> NodeIndex {
        self.est_bytes += Self::estimate(&m);
        for e in &m.context.entities {
            self.by_entity.entry(*e).or_default().push(m.id);
        }
        self.by_type
            .entry(m.memory_type)
            .or_default()
            .union_with_lsb(&m);
        for tag in &m.tags {
            let spur = self.interner.get_or_intern(tag.as_str());
            self.by_tag.entry(spur).or_default().union_with_lsb(&m);
        }
        self.search_offsets.push(self.search_arena.len() as u32);
        self.search_arena.push_str(&Self::search_key(&m));
        self.search_arena.push(NL);
        let ix = self.petgraph.add_node(m.clone());
        self.by_id.insert(m.id, ix);
        ix
    }

    fn remove_memory(&mut self, id: &MemoryId) {
        if let Some((_, ix)) = self.by_id.remove(id) {
            if let Some(m) = self.petgraph.node_weight(ix).cloned() {
                self.est_bytes = self.est_bytes.saturating_sub(Self::estimate(&m));
                if let Some(mut bitmap) = self.by_type.get_mut(&m.memory_type) {
                    bitmap.remove_lsb(&m);
                }
                for tag in &m.tags {
                    if let Some(spur) = self.interner.get(tag.as_str()) {
                        if let Some(mut bitmap) = self.by_tag.get_mut(&spur) {
                            bitmap.remove_lsb(&m);
                        }
                    }
                }
            }
            // Blank the removed key in place (arena stays append-only).
            let from = self.search_offsets.get(ix.index()).copied().unwrap_or(0) as usize;
            let to = self
                .search_offsets
                .get(ix.index() + 1)
                .copied()
                .unwrap_or(self.search_arena.len() as u32) as usize;
            if from < to && to <= self.search_arena.len() {
                let bytes = unsafe_blank(&mut self.search_arena, from, to);
                let _ = bytes;
            }
            self.petgraph.remove_node(ix);
        }
    }

    /// Insert a relationship between existing memories.
    fn insert_relationship(&mut self, r: Relationship) {
        if let (Some(a), Some(b)) = (self.by_id.get(&r.from), self.by_id.get(&r.to)) {
            self.est_bytes += 256;
            self.petgraph.add_edge(*a, *b, r);
        }
    }

    fn remove_relationship(&mut self, id: &RelationshipId) {
        let mut hit = None;
        for ix in self.petgraph.node_indices() {
            for e in self.petgraph.edges(ix) {
                if e.weight().id == *id {
                    hit = Some(e.id());
                    break;
                }
            }
            if hit.is_some() {
                break;
            }
        }
        if let Some(eid) = hit {
            self.petgraph.remove_edge(eid);
            self.est_bytes = self.est_bytes.saturating_sub(256);
        }
    }

    /// Visibility check per §17.2: `Private` resolves against the author.
    pub fn visible(&self, m: &Memory, vc: &VisibilityContext) -> bool {
        if m.visibility as u8 > vc.max_visibility as u8 {
            return false;
        }
        if m.visibility == Visibility::Private {
            return m.context.user_id.as_deref() == Some(vc.user_id.as_str());
        }
        true
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

    /// Build a snapshot from storage streams (shared by reseed and tests).
    pub async fn from_storage<S: Storage>(storage: &S) -> Self {
        use futures::StreamExt;
        let mut snap = Self::empty();
        let mut ms = storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            snap.insert_memory(m);
        }
        let mut frontier = 0u64;
        let mut rs = storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            frontier = frontier.max(r.lsn.value);
            snap.insert_relationship(r);
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
    graphs: DashMap<SmolStr, Arc<ArcSwap<GraphSnapshot>>>,
    tq: Mutex<TwoQState>,
    writer: mpsc::Sender<CacheWrite>,
    budget: usize,
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
    /// Apply one change-feed invalidation (copy-on-write).
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
    },
    /// Evict an org graph entirely.
    Evict(
        /// Org to evict.
        SmolStr,
    ),
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
            },
            rx,
        )
    }

    /// The single writer loop. Applies invalidations and reseeds serially so
    /// snapshots swap atomically (§8.2).
    pub async fn run<S: Storage>(&self, storage: Arc<S>, mut rx: mpsc::Receiver<CacheWrite>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                CacheWrite::Apply(inv) => self.apply(&*storage, inv).await,
                CacheWrite::Reseed { org, snapshot } => {
                    // R-O2 families: rebuild counts + graph size levels.
                    metrics::counter!("exocortex_cache_rebuild_total", "reason" => "reseed")
                        .increment(1);
                    metrics::gauge!("exocortex_memories_total", "graph" => org.to_string())
                        .set(snapshot.petgraph.node_count() as f64);
                    metrics::gauge!("exocortex_relationships_total", "graph" => org.to_string(), "provenance" => "all")
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
                            Arc::new(ArcSwap::from(Arc::new(GraphSnapshot::empty())))
                        })
                        .store(snapshot);
                    self.admit(&org);
                }
                CacheWrite::Evict(org) => {
                    self.graphs.remove(&org);
                }
            }
        }
    }

    /// Copy-on-write apply of one invalidation. Readers holding the old
    /// snapshot continue to see it until they release their Arc.
    async fn apply<S: Storage>(&self, storage: &S, inv: Invalidation) {
        // v1 caches exactly one org per client (§17); invalidations target it.
        let Some(org) = self.org_of_write() else {
            return;
        };
        let Some(g) = self.graphs.get(&org) else {
            return;
        };
        let mut next = clone_snapshot(&g.load_full());
        match inv {
            Invalidation::MemoryUpserted { id, lsn } => {
                match storage.get_memory(&id).await {
                    Ok(Some(m)) => {
                        next.insert_memory(m);
                    }
                    Ok(None) => {
                        next.remove_memory(&id);
                    }
                    Err(e) => {
                        tracing::warn!(?e, "invalidation fetch failed");
                        return;
                    }
                }
                next.last_backend_lsn = next.last_backend_lsn.max(lsn);
            }
            Invalidation::MemoryDeleted { id, lsn } => {
                next.remove_memory(&id);
                next.last_backend_lsn = next.last_backend_lsn.max(lsn);
            }
            Invalidation::RelationshipUpserted { id, lsn, .. } => {
                if let Ok(Some(r)) = fetch_relationship(storage, &id).await {
                    next.insert_relationship(r);
                }
                next.last_backend_lsn = next.last_backend_lsn.max(lsn);
            }
            Invalidation::RelationshipDeleted { id, lsn } => {
                next.remove_relationship(&id);
                next.last_backend_lsn = next.last_backend_lsn.max(lsn);
            }
        }
        g.store(Arc::new(next));
    }

    fn org_of_write(&self) -> Option<SmolStr> {
        // Multi-org routing arrives with multi-tenancy work in v2.
        self.graphs.iter().next().map(|e| e.key().clone())
    }

    fn admit(&self, org: &SmolStr) {
        let mut tq = self.tq.lock();
        if tq.am.contains(org) {
            tq.am.put(org.clone(), ());
            drop(tq);
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "promote_am")
                .increment(1);
            return;
        }
        if tq.a1out.contains(org) {
            tq.am.put(org.clone(), ());
            tq.a1out.pop(org);
            drop(tq);
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "ghost_hit")
                .increment(1);
            return;
        }
        if tq.a1in.contains(org) {
            // Re-reference while still in A1in: promote to Am (2Q
            // semantics). This also guarantees repeated publishes never
            // duplicate the A1in entry.
            tq.a1in.retain(|o| o != org);
            tq.am.put(org.clone(), ());
            drop(tq);
            metrics::counter!("exocortex_2q_admission_events_total", "decision" => "promote_am")
                .increment(1);
            return;
        }
        tq.a1in.push_back(org.clone());
        metrics::counter!("exocortex_2q_admission_events_total", "decision" => "admit_a1in")
            .increment(1);
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

    /// Bounded BFS traversal over the snapshot (§8.4).
    pub fn traverse(&self, org: &str, from: &MemoryId, spec: &TraversalSpec) -> Vec<Memory> {
        let Some(g) = self.graphs.get(org) else {
            return vec![];
        };
        let snap = g.load_full();
        let Some(start) = snap.by_id.get(from).map(|r| *r) else {
            return vec![];
        };
        let mut out = Vec::new();
        let mut queue = std::collections::VecDeque::from([(start, 0u8)]);
        let mut seen = std::collections::HashSet::from([start]);
        while let Some((n, d)) = queue.pop_front() {
            if out.len() >= spec.max_nodes as usize {
                break;
            }
            for e in snap.petgraph.edges(n) {
                let er = e.weight();
                if !spec.kinds.is_empty() && !spec.kinds.contains(&er.kind) {
                    continue;
                }
                if er.visibility as u8 > spec.visibility_ctx.max_visibility as u8 {
                    continue;
                }
                let dst = match spec.direction {
                    Direction::Out => e.target(),
                    Direction::In => e.source(),
                    Direction::Both => e.target(),
                };
                if !seen.insert(dst) {
                    continue;
                }
                if let Some(m) = snap.petgraph.node_weight(dst) {
                    if snap.visible(m, &spec.visibility_ctx) {
                        out.push(m.clone());
                    }
                }
                if d + 1 < spec.max_depth {
                    queue.push_back((dst, d + 1));
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
        let mut from = 0usize;
        let mut last_idx: Option<usize> = None;
        while let Some(rel) = arena[from..].find(q.as_str()) {
            let pos = from + rel;
            let idx = match offsets.binary_search(&(pos as u32)) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };
            if last_idx != Some(idx) {
                last_idx = Some(idx);
                let ix = NodeIndex::new(idx);
                if let Some(m) = snap.petgraph.node_weight(ix) {
                    if snap.visible(m, vc) {
                        // §14.1: base match + explicit relationship count *
                        // 0.30 + Σ inferred confidence * 0.15 + importance *
                        // 0.50 + recency 0.10.
                        let mut explicit = 0.0f32;
                        let mut inferred = 0.0f32;
                        for e in snap
                            .petgraph
                            .edges_directed(ix, petgraph::Direction::Outgoing)
                        {
                            let er = e.weight();
                            if matches!(er.provenance, exocortex_kernel::Provenance::Derived { .. })
                            {
                                inferred += er.properties.confidence * 0.15;
                            } else {
                                explicit += 1.0;
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

    /// Submit a writer message (tests + client wiring).
    pub async fn submit(&self, w: CacheWrite) {
        let _ = self.writer.send(w).await;
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
            .or_insert_with(|| Arc::new(ArcSwap::from(Arc::new(GraphSnapshot::empty()))))
            .store(snapshot);
        self.admit(&org.into());
    }

    /// Stream all memories + relationships from storage and rebuild the
    /// snapshot for one org (§8.4 `reseed_from_storage`).
    pub async fn reseed_from_storage<S: Storage>(&self, storage: &S, org: &SmolStr) {
        let snap = GraphSnapshot::from_storage(storage).await;
        let _ = self
            .writer
            .send(CacheWrite::Reseed {
                org: org.clone(),
                snapshot: Arc::new(snap),
            })
            .await;
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
        last_local_lsn: src.last_local_lsn,
        last_backend_lsn: src.last_backend_lsn,
        built_at: chrono::Utc::now(),
        est_bytes: src.est_bytes,
    }
}

/// Storage has no point edge-read in the v1 trait; scan current rows.
async fn fetch_relationship<S: Storage>(
    storage: &S,
    id: &RelationshipId,
) -> exocortex_storage::Result<Option<Relationship>> {
    use futures::StreamExt;
    let mut rs = storage.stream_all_relationships().await;
    while let Some(r) = rs.next().await {
        if let Ok(r) = r {
            if r.id == *id {
                return Ok(Some(r));
            }
        }
    }
    Ok(None)
}
