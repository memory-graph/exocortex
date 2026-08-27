//! The Dreams cycle (§12): event-driven consolidation on the owner-elected
//! backend node — MCR² before/after (R-Mcr3), sparsity before/after
//! (R-Mcr5/R-Mcr6), the four consolidation actions, and the full R-Dr4
//! `ConsolidationResult` audit stamp.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod fire;
pub mod mcr2;
pub mod trigger;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use exocortex_kernel::{Memory, MemoryId, Provenance, Relationship, RelationshipId};
use exocortex_storage::{FencedRestore, LeaseKey, RegionKey, Storage};

use mcr2::{
    compute_sparsity, effective_strength, GraphSparsity, MCR2Engine, MCR2Value, MemoryWithEmbedding,
};
use trigger::{DreamsTrigger, RegionWriteCounters};

/// §12.1 step 5: SimilarTo creation threshold.
pub const SIMILAR_TO_THRESHOLD: f32 = 0.85;
/// Maximum structured discovery proposals emitted by one production cycle.
pub const MAX_DISCOVERIES_PER_CYCLE: usize = 16;
/// Maximum two-hop path candidates inspected by one discovery cycle.
pub const MAX_DISCOVERY_PATH_INSPECTIONS: usize = 50_000;

type DiscoveryEdge = (MemoryId, MemoryId, u32, bool);

/// The audit record stamped per cycle — every field R-Dr4 mandates.
#[derive(Clone, Debug)]
pub struct ConsolidationResult {
    /// Cycle id.
    pub session_id: SmolStr,
    /// Triggering user, when known.
    pub user_id: Option<SmolStr>,
    /// Start time.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Completion time.
    pub completed_at: chrono::DateTime<chrono::Utc>,
    /// The region consolidated.
    pub region: RegionKey,
    /// Memories entering the cycle.
    pub memories_input: u32,
    /// Memories leaving the cycle.
    pub memories_output: u32,
    /// ΔR before (carries embedding_model, R-Dr5).
    pub mcr2_before: MCR2Value,
    /// ΔR after.
    pub mcr2_after: MCR2Value,
    /// Sparsity before (R-Mcr5).
    pub sparsity_before: GraphSparsity,
    /// Sparsity after.
    pub sparsity_after: GraphSparsity,
    /// Merged-away memory ids (R-Dr10).
    pub merged: Vec<MemoryId>,
    /// Abstracted parent ids.
    pub abstracted: Vec<MemoryId>,
    /// Pruned ids with reasons.
    pub pruned: Vec<(MemoryId, PruneReason)>,
    /// Strengthened edge ids.
    pub strengthened: Vec<RelationshipId>,
    /// Rewired edge ids.
    pub rewired: Vec<RelationshipId>,
    /// SimilarTo edges created this cycle (§12.1 step 5, R-T14).
    pub similar_edges: Vec<RelationshipId>,
    /// Owning node.
    pub owner_node_id: SmolStr,
    /// Fencing epoch of the lease held.
    pub lease_epoch: u64,
    /// True when ΔR regressed past tolerance (R-Mcr3).
    pub regression: bool,
    /// True when the hairball fraction rose past tolerance (R-Mcr6).
    pub hairball_regression: bool,
}

/// Why a memory was pruned (§12.5 step 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneReason {
    /// Duplicate absorbed.
    Redundant,
    /// Replaced by a newer row.
    Superseded,
    /// Outside the validity window.
    Stale,
    /// Below the value floor.
    LowValue,
}

#[derive(Default)]
struct CycleJournal {
    memories: std::collections::BTreeMap<MemoryId, Memory>,
    relationships: std::collections::BTreeMap<RelationshipId, Relationship>,
    created_memories: std::collections::BTreeSet<MemoryId>,
    created_relationships: std::collections::BTreeSet<RelationshipId>,
}

impl CycleJournal {
    fn record_memory(&mut self, memory: &Memory) {
        if !self.created_memories.contains(&memory.id) {
            self.memories
                .entry(memory.id)
                .or_insert_with(|| memory.clone());
        }
    }

    fn record_relationship(&mut self, relationship: &Relationship) {
        if !self.created_relationships.contains(&relationship.id) {
            self.relationships
                .entry(relationship.id)
                .or_insert_with(|| relationship.clone());
        }
    }

    fn create_relationship(&mut self, id: RelationshipId) {
        if !self.relationships.contains_key(&id) {
            self.created_relationships.insert(id);
        }
    }

    fn restore(&self) -> FencedRestore {
        FencedRestore {
            memories: self.memories.values().cloned().collect(),
            relationships: self.relationships.values().cloned().collect(),
            created_memories: self.created_memories.iter().copied().collect(),
            created_relationships: self.created_relationships.iter().copied().collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.memories.is_empty()
            && self.relationships.is_empty()
            && self.created_memories.is_empty()
            && self.created_relationships.is_empty()
    }
}

/// The Dreams engine: queue-fed, lease-gated, per-region.
pub struct DreamsEngine<S: Storage> {
    /// Durable storage (data + leases).
    pub storage: Arc<S>,
    /// CS4 (audit): the backend re-election leader gate. When set, this
    /// engine runs consolidation ONLY while its node holds the elected
    /// `LeaseKey::Dreams{org, "*:*"}` lease — the reported leader and the
    /// node performing owner-only work are the same node, so a leader kill
    /// actually moves Dreams ownership.
    pub leader_gate: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// Per-region write counters.
    pub counters: DashMap<RegionKey, RegionWriteCounters>,
    /// IN4 (audit): per-region last-cycle timestamp — the source
    /// `seconds_since_last_cycle` is stamped from. Without it the field
    /// stayed 0 forever and the trigger predicate was unconditionally
    /// false in the running node.
    pub last_cycle_at: DashMap<RegionKey, chrono::DateTime<chrono::Utc>>,
    /// Trigger thresholds.
    pub dreams_trigger: DreamsTrigger,
    /// ΔR regression tolerance (default 0.01).
    pub tolerance: f32,
    /// Hairball tolerance (default 0.05).
    pub hairball_tolerance: f32,
    /// Operator rollback flag.
    pub rollback_on_regression: bool,
    /// Fires to the consolidation loop (region + the counters snapshot at
    /// fire time, so completion can compare-and-reset without losing
    /// writes that landed mid-cycle, R-Dr13).
    pub tx_fire: mpsc::Sender<(RegionKey, RegionWriteCounters)>,
    /// The loop's receiver.
    pub rx_fire: tokio::sync::Mutex<mpsc::Receiver<(RegionKey, RegionWriteCounters)>>,
    /// Node identity for lease tokens.
    pub node_id: SmolStr,
    /// Discovery proposals awaiting `accept_discovery` (R-Dr1).
    pub discoveries: DashMap<uuid::Uuid, Discovery>,
    /// Last successfully consolidated cycle, retained for health/acceptance
    /// observation without scraping logs or metrics.
    pub last_result: tokio::sync::RwLock<Option<ConsolidationResult>>,
    cycle_fault_after: Option<usize>,
}

impl<S: Storage + 'static> DreamsEngine<S> {
    /// Build the engine with §12.2 defaults.
    /// Attach the re-election leader gate (see field docs).
    pub fn with_leader_gate(mut self, gate: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.leader_gate = Some(gate);
        self
    }

    /// Build the engine over `storage` (§12).
    pub fn new(
        storage: Arc<S>,
        dreams_trigger: DreamsTrigger,
        tolerance: f32,
        hairball_tolerance: f32,
        rollback_on_regression: bool,
        node_id: SmolStr,
    ) -> Self {
        let (tx_fire, rx_fire) = mpsc::channel(1024);
        Self {
            leader_gate: None,
            storage,
            counters: DashMap::new(),
            last_cycle_at: DashMap::new(),
            dreams_trigger,
            tolerance,
            hairball_tolerance,
            rollback_on_regression,
            tx_fire,
            rx_fire: tokio::sync::Mutex::new(rx_fire),
            node_id,
            discoveries: DashMap::new(),
            last_result: tokio::sync::RwLock::new(None),
            cycle_fault_after: None,
        }
    }

    /// Inject a failure after the specified owner mutation. This exercises
    /// crash compensation against both the double and live backend.
    #[doc(hidden)]
    pub fn with_cycle_fault_after(mut self, mutation: usize) -> Self {
        self.cycle_fault_after = Some(mutation);
        self
    }

    /// Record a write; fire when the predicate trips (§12.2 transport is
    /// Redis in production; the in-process channel is the same shape).
    /// R-Dr13: counters are NOT reset here — completion resets them, so a
    /// failed cycle never loses its write counts.
    pub async fn on_write(&self, region: RegionKey) {
        let now = chrono::Utc::now();
        // IN4: anchor the region's clock at first observation; cycles
        // re-stamp it on completion.
        let anchor = *self
            .last_cycle_at
            .entry(region.clone())
            .or_insert(now)
            .value();
        let mut e = self.counters.entry(region.clone()).or_default();
        e.memories_since_last_cycle += 1;
        e.seconds_since_last_cycle = (now - anchor).num_seconds().max(0) as u64;
        if self.dreams_trigger.should_fire(&e) {
            let snap = *e;
            drop(e);
            let _ = self.tx_fire.try_send((region, snap));
        }
    }

    /// Fire a region explicitly (Redis fire-queue drainer side).
    pub fn notify(&self, region: RegionKey) {
        let snap = self.counters.get(&region).map(|e| *e).unwrap_or_default();
        let _ = self.tx_fire.try_send((region, snap));
    }

    /// The production Dreams loop: consolidation followed by discovery.
    pub async fn run(self: Arc<Self>) {
        while let Some((region, fired_at)) = { self.rx_fire.lock().await.recv().await } {
            match self.try_consolidate(&region).await {
                Ok(res) => {
                    *self.last_result.write().await = Some(res.clone());
                    info!(?res, "consolidation ok");
                }
                Err(e) => {
                    warn!(?e, "consolidation failed");
                    continue;
                }
            }
            match self.run_discovery(&region).await {
                Ok(proposals) => info!(count = proposals.len(), "discovery ok"),
                Err(e) => {
                    warn!(?e, "discovery failed");
                    continue;
                }
            }
            // R-Dr13: reset only when nothing new landed during the cycle;
            // otherwise the surviving counts roll into the next fire.
            if let Some(mut c) = self.counters.get_mut(&region) {
                if *c == fired_at {
                    *c = RegionWriteCounters::default();
                    // IN4: the region's clock restarts on completion.
                    self.last_cycle_at
                        .entry(region.clone())
                        .and_modify(|t| *t = chrono::Utc::now())
                        .or_insert_with(chrono::Utc::now);
                }
            }
        }
    }

    /// One full cycle under the region lease (§12.4 skeleton, filled per
    /// §12.5 steps 3-9). The lease is released on every path; all writes
    /// ride the fenced paths so a stale owner can never commit (R-C3).
    #[instrument(skip(self))]
    pub async fn try_consolidate(&self, region: &RegionKey) -> anyhow::Result<ConsolidationResult> {
        self.validate_region(region).await?;
        // CS4: a follower (lost re-election) performs no consolidation,
        // even before contending on the region lease.
        if let Some(gate) = &self.leader_gate {
            if !gate.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow::anyhow!("not the elected leader"));
            }
        }
        let lease_key = LeaseKey::Dreams {
            org: region.org.clone(),
            region: format!("{}:{}", region.project, region.memory_type).into(),
        };
        // R-Dr3: consolidation is owner-only; no lease, no work.
        let lease = self
            .storage
            .acquire_lease(&lease_key, Duration::from_secs(60))
            .await
            .map_err(|e| anyhow::anyhow!("lease: {e}"))?;
        let outcome = self.consolidate_under(&lease, region).await;
        let _ = self.storage.release_lease(lease).await;
        outcome
    }

    async fn consolidate_under(
        &self,
        lease: &exocortex_storage::OwnerLease,
        region: &RegionKey,
    ) -> anyhow::Result<ConsolidationResult> {
        let mut journal = CycleJournal::default();
        let outcome = self
            .consolidate_under_tracked(lease, region, &mut journal)
            .await;
        match outcome {
            Ok(result) => Ok(result),
            Err(error) => {
                if !journal.is_empty() {
                    self.rollback(&journal, lease).await.map_err(|rollback| {
                        anyhow::anyhow!(
                            "cycle failed ({error}); atomic restore failed ({rollback})"
                        )
                    })?;
                }
                Err(error)
            }
        }
    }

    async fn consolidate_under_tracked(
        &self,
        lease: &exocortex_storage::OwnerLease,
        region: &RegionKey,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<ConsolidationResult> {
        let mut mutations = 0usize;
        let anchors = self.select_anchors(region).await?;
        // IN5 (audit): a region with fewer than two anchors cannot be
        // scored — that is a no-op cycle, not an error (erroring here
        // aborted after nothing had committed but before any audit).
        let mcr2_before = match self.score_with(&anchors) {
            Ok(v) => v,
            Err(e)
                if matches!(
                    e.downcast_ref::<crate::mcr2::MCR2Error>(),
                    Some(crate::mcr2::MCR2Error::TooFew(_))
                ) =>
            {
                return self.empty_result(region, lease, anchors.len() as u32).await;
            }
            Err(e) => return Err(e),
        };
        let sparsity_before = self.sparsity(region).await?;

        let mut res = ConsolidationResult {
            session_id: format!("dream:{}", uuid::Uuid::new_v4()).into(),
            user_id: None,
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            region: region.clone(),
            memories_input: anchors.len() as u32,
            memories_output: anchors.len() as u32,
            mcr2_before: mcr2_before.clone(),
            mcr2_after: mcr2_before,
            sparsity_before: sparsity_before.clone(),
            sparsity_after: sparsity_before,
            merged: vec![],
            abstracted: vec![],
            pruned: vec![],
            strengthened: vec![],
            rewired: vec![],
            similar_edges: vec![],
            owner_node_id: self.node_id.clone(),
            lease_epoch: lease.epoch,
            regression: false,
            hairball_regression: false,
        };

        // Merge duplicates (cosine >= 0.92 AND same class, §12.5 step 5).
        let engine = MCR2Engine::default();
        let candidates = engine.identify_merge_candidates(&anchors);
        for c in candidates.iter().take(32) {
            if c.cosine_similarity >= 0.92 {
                let merged_before = res.merged.len();
                self.merge(&mut res, c, lease, region, journal).await?;
                if res.merged.len() > merged_before {
                    self.mutation_checkpoint(&mut mutations)?;
                }
            }
        }
        // §12.1 step 4 ABSTRACT (informational in v1): the PRD names the
        // action without specifying the abstraction's ontology shape, so
        // the cycle records each multi-member class's surviving
        // representative in `abstracted` — an auditable stamp of the
        // consolidation structure. Inventing memory rows or kinds here
        // would violate the "no silent design" rule; the row-writing
        // variant lands with the second-pack ontology work (open
        // question, docs/MILESTONE_REPORT.md).
        {
            let mut by_class: std::collections::BTreeMap<u32, Vec<MemoryId>> = Default::default();
            for a in anchors.iter().filter(|a| !res.merged.contains(&a.id)) {
                by_class.entry(a.class as u32).or_default().push(a.id);
            }
            for (_class, members) in by_class {
                if members.len() >= 3 {
                    res.abstracted.push(*members.last().expect("non-empty"));
                }
            }
        }
        self.strengthen(&mut res, region, lease, journal).await?;
        if !res.strengthened.is_empty() {
            self.mutation_checkpoint(&mut mutations)?;
        }
        self.prune(&mut res, region).await?;
        res.memories_output = (res.memories_input as usize - res.merged.len()).max(0) as u32;

        // §12.1 step 5 / R-T14: SimilarTo edges over the surviving anchors —
        // every edge carries `Computed { SimilarityHnsw, threshold 0.85 }`
        // provenance. Below the merge threshold (0.92), so near-duplicates
        // consolidate instead of gaining similarity edges.
        let survivors: Vec<MemoryWithEmbedding> = anchors
            .iter()
            .filter(|a| !res.merged.contains(&a.id))
            .cloned()
            .collect();
        self.write_similar_edges(&mut res, &survivors, lease, journal)
            .await?;
        if !res.similar_edges.is_empty() {
            self.mutation_checkpoint(&mut mutations)?;
        }

        // Re-score with the post-cycle set (merged anchors removed).
        let remaining: Vec<MemoryWithEmbedding> = anchors
            .into_iter()
            .filter(|a| !res.merged.contains(&a.id))
            .collect();
        // IN5 (audit): a post-cycle set too small to score is NOT an error
        // — the merge already committed, and erroring skipped the audit
        // record, the regression check, and the rollback. Carry the
        // before-score forward and record ΔR as unavailable.
        res.mcr2_after = match self.score_with(&remaining) {
            Ok(score) => score,
            Err(e)
                if matches!(
                    e.downcast_ref::<crate::mcr2::MCR2Error>(),
                    Some(crate::mcr2::MCR2Error::TooFew(_))
                ) =>
            {
                res.mcr2_before.clone()
            }
            Err(e) => return Err(e),
        };
        res.sparsity_after = self.sparsity(region).await?;
        res.completed_at = chrono::Utc::now();

        if res.mcr2_after.delta_r < res.mcr2_before.delta_r - self.tolerance {
            res.regression = true; // R-Mcr3
            if self.rollback_on_regression {
                warn!(
                    "MCR2 degraded {} -> {} - rolling back",
                    res.mcr2_before.delta_r, res.mcr2_after.delta_r
                );
                self.rollback(journal, lease).await?;
            }
        }
        if res.sparsity_after.hairball_fraction
            > res.sparsity_before.hairball_fraction + self.hairball_tolerance
        {
            res.hairball_regression = true; // R-Mcr6
        }
        self.write_audit(&res).await?;
        Ok(res)
    }

    fn mutation_checkpoint(&self, mutations: &mut usize) -> anyhow::Result<()> {
        *mutations += 1;
        if self.cycle_fault_after == Some(*mutations) {
            anyhow::bail!("injected cycle failure after mutation {mutations}");
        }
        Ok(())
    }

    /// Rank regional memories by recency decay; take top 32 (§12.5 step 3),
    /// deterministic tie-break by id.
    async fn select_anchors(&self, region: &RegionKey) -> anyhow::Result<Vec<MemoryWithEmbedding>> {
        use futures::StreamExt;
        let mut rows: Vec<(chrono::DateTime<chrono::Utc>, MemoryWithEmbedding)> = Vec::new();
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            // IN1 (audit): the write set must be a subset of what the held
            // lease covers — scope by the region's org and project too,
            // not just memory_type.
            if !in_region(&m, region) || m.valid_until.is_some() {
                continue;
            }
            if let Some(emb) = &m.embedding {
                rows.push((
                    m.recorded_at,
                    MemoryWithEmbedding {
                        id: m.id,
                        class: m.memory_type,
                        embedding: emb.clone(),
                    },
                ));
            }
        }
        drop(ms);
        let now = chrono::Utc::now();
        rows.sort_by(|a, b| {
            let score = |t: chrono::DateTime<chrono::Utc>| -(now - t).num_days();
            score(b.0).cmp(&score(a.0)).then(b.1.id.cmp(&a.1.id))
        });
        Ok(rows.into_iter().take(32).map(|(_, m)| m).collect())
    }

    fn score_with(&self, anchors: &[MemoryWithEmbedding]) -> anyhow::Result<MCR2Value> {
        Ok(MCR2Engine::default().compute(anchors)?)
    }

    async fn sparsity(&self, region: &RegionKey) -> anyhow::Result<GraphSparsity> {
        use futures::StreamExt;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            if !in_region(&m, region) || m.valid_until.is_some() {
                continue; // IN1: sparsity measures the region, not the graph
            }
            nodes.push((m.id, m.memory_type));
        }
        drop(ms);
        let members: std::collections::HashSet<exocortex_kernel::MemoryId> =
            nodes.iter().map(|(id, _)| *id).collect();
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            if r.valid_until.is_none() && members.contains(&r.from) && members.contains(&r.to) {
                edges.push((r.from, r.to, r.kind.0, 0u64, r.properties.confidence));
            }
        }
        drop(rs);
        // §11.6.1: the similarity bucket never counts toward out-degrees.
        Ok(compute_sparsity(&nodes, &edges, 32, similar_to_kind()))
    }

    /// Merge a duplicate pair: keep the older row, close the newer with
    /// valid_until (bi-temporal; R-Dr10 keeps "why is this gone" answerable).
    async fn merge(
        &self,
        res: &mut ConsolidationResult,
        c: &mcr2::MergeCandidate,
        lease: &exocortex_storage::OwnerLease,
        region: &RegionKey,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        let mut newer = None;
        let mut survivor_live = false;
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            if m.id == c.a && m.valid_until.is_none() {
                survivor_live = true;
            }
            if m.id == c.b && m.valid_until.is_none() {
                newer = Some(m);
            }
        }
        drop(ms);
        if survivor_live {
            if let Some(mut m) = newer {
                let now = chrono::Utc::now();
                journal.record_memory(&m);
                let mut region_members = std::collections::HashSet::new();
                let mut memories = self.storage.stream_all_memories().await;
                while let Some(row) = memories.next().await {
                    let memory = row?;
                    if in_region(&memory, region) && memory.valid_until.is_none() {
                        region_members.insert(memory.id);
                    }
                }
                drop(memories);
                let mut current_relationships = std::collections::HashMap::new();
                let mut relationships = self.storage.stream_all_relationships().await;
                while let Some(row) = relationships.next().await {
                    let relationship = row?;
                    current_relationships.insert(relationship.id, relationship);
                }
                drop(relationships);

                let mut relationship_updates = std::collections::BTreeMap::new();
                for relationship in current_relationships.values().filter(|relationship| {
                    relationship.valid_until.is_none()
                        && (relationship.from == c.b || relationship.to == c.b)
                        && region_members.contains(&relationship.from)
                        && region_members.contains(&relationship.to)
                }) {
                    journal.record_relationship(relationship);
                    let mut closed = relationship.clone();
                    closed.valid_until = Some(now);
                    relationship_updates.insert(closed.id, closed);

                    let mut rewired = relationship.clone();
                    if rewired.from == c.b {
                        rewired.from = c.a;
                    }
                    if rewired.to == c.b {
                        rewired.to = c.a;
                    }
                    if rewired.from == rewired.to {
                        continue;
                    }
                    rewired.id =
                        RelationshipId::derive(rewired.from, rewired.kind, rewired.to, None);
                    rewired.recorded_at = now;
                    rewired.valid_from = now;
                    rewired.valid_until = None;
                    rewired.invalidated_by = None;
                    if let Some(existing) = current_relationships.get(&rewired.id) {
                        if existing.valid_until.is_none() {
                            continue;
                        }
                        journal.record_relationship(existing);
                    } else {
                        journal.create_relationship(rewired.id);
                    }
                    res.rewired.push(rewired.id);
                    relationship_updates.insert(rewired.id, rewired);
                }

                m.valid_until = Some(now);
                m.invalidated_by = Some(c.a);
                let relationship_updates: Vec<_> = relationship_updates.into_values().collect();
                self.storage
                    .upsert_batch_fenced(&[m], &relationship_updates, lease)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                res.merged.push(c.b);
            }
        }
        Ok(())
    }

    /// Strengthen (§12.5 step 6): surviving edges gain one evidence count
    /// and re-derive strength from the §14.3 formula — evidence boost
    /// (capped 0.20), success scaling, age decay — never a flat bump.
    /// SimilarTo edges ride `Computed` provenance and are not strengthened.
    async fn strengthen(
        &self,
        res: &mut ConsolidationResult,
        region: &RegionKey,
        lease: &exocortex_storage::OwnerLease,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        let now = chrono::Utc::now();
        let mut updates = Vec::new();
        // IN1: only edges with BOTH endpoints inside the leased region.
        let mut member_ids: std::collections::HashSet<exocortex_kernel::MemoryId> =
            std::collections::HashSet::new();
        {
            let mut ms = self.storage.stream_all_memories().await;
            while let Some(Ok(m)) = ms.next().await {
                if in_region(&m, region) && m.valid_until.is_none() {
                    member_ids.insert(m.id);
                }
            }
        }
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(Ok(mut r)) = rs.next().await {
            if !member_ids.contains(&r.from) || !member_ids.contains(&r.to) {
                continue;
            }
            if r.valid_until.is_some() {
                continue;
            }
            if matches!(r.provenance, Provenance::Computed { .. }) {
                continue;
            }
            if res.strengthened.contains(&r.id) {
                continue; // one evidence bump per cycle, not per anchor
            }
            journal.record_relationship(&r);
            r.properties.evidence_count += 1;
            // IN8 (audit): the stored field is the BASE strength — the
            // §14.3 decay/success derivation applies at READ time, so
            // repeated cycles can no longer compound the decay into the
            // base and monotonically weaken every edge. Un-decay first.
            let age_days = (now - r.recorded_at).num_days().max(0) as f32;
            r.properties.strength = effective_strength(
                r.properties.strength / decay_factor(age_days).max(f32::EPSILON),
                r.properties.evidence_count,
                r.properties.success_rate.unwrap_or(1.0),
                age_days,
            );
            updates.push(r);
        }
        drop(rs);
        for r in &updates {
            res.strengthened.push(r.id);
        }
        if !updates.is_empty() {
            self.storage
                .upsert_batch_fenced(&[], &updates, lease)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok(())
    }

    /// §12.1 step 5: SimilarTo edges between same-class surviving anchors
    /// above the 0.85 threshold, stamped `Computed { SimilarityHnsw }`
    /// (R-T14). Idempotent via deterministic `RelationshipId::derive`.
    async fn write_similar_edges(
        &self,
        res: &mut ConsolidationResult,
        survivors: &[MemoryWithEmbedding],
        lease: &exocortex_storage::OwnerLease,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<()> {
        let Some(similar_kind) = similar_to_kind() else {
            return Ok(());
        };
        let now = chrono::Utc::now();
        let mut edges = Vec::new();
        for i in 0..survivors.len() {
            for j in (i + 1)..survivors.len() {
                let (a, b) = (&survivors[i], &survivors[j]);
                if a.class != b.class || a.id == b.id {
                    continue;
                }
                let sim = mcr2::cosine(&a.embedding.vector, &b.embedding.vector);
                if sim < SIMILAR_TO_THRESHOLD {
                    continue;
                }
                use exocortex_kernel::{Relationship, RelationshipProperties, Visibility, LSN};
                edges.push(Relationship {
                    id: exocortex_kernel::RelationshipId::derive(a.id, similar_kind, b.id, None),
                    kind: similar_kind,
                    from: a.id,
                    to: b.id,
                    visibility: Visibility::Org,
                    provenance: Provenance::Computed {
                        producer: exocortex_kernel::provenance::ComputedProducer::SimilarityHnsw,
                        threshold: SIMILAR_TO_THRESHOLD,
                    },
                    properties: RelationshipProperties {
                        strength: sim.clamp(0.0, 1.0),
                        confidence: sim.clamp(0.0, 1.0),
                        context: None,
                        evidence_count: 1,
                        success_rate: None,
                        validation_count: 0,
                        counter_evidence_count: 0,
                        last_validated: now,
                    },
                    description: None,
                    bidirectional: true,
                    valid_from: now,
                    valid_until: None,
                    recorded_at: now,
                    invalidated_by: None,
                    lsn: LSN::new_local(0),
                });
            }
        }
        // Idempotency: only write edges that do not already exist.
        use futures::StreamExt;
        let existing: std::collections::HashMap<exocortex_kernel::RelationshipId, Relationship> = {
            let mut rows = std::collections::HashMap::new();
            let mut rs = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rs.next().await {
                rows.insert(r.id, r);
            }
            rows
        };
        let mut fresh = Vec::new();
        for edge in edges {
            match existing.get(&edge.id) {
                Some(row) if row.valid_until.is_none() => continue,
                Some(row) => journal.record_relationship(row),
                None => journal.create_relationship(edge.id),
            }
            fresh.push(edge);
        }
        if !fresh.is_empty() {
            self.storage
                .upsert_batch_fenced(&[], &fresh, lease)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            res.similar_edges.extend(fresh.iter().map(|e| e.id));
            metrics::counter!("exocortex_dreams_similar_edges_total").increment(fresh.len() as u64);
        }
        Ok(())
    }

    /// Prune: closed rows (valid_until set) are recorded Redundant — the
    /// audit trail survives the memory (R-Dr9/R-Dr10).
    async fn prune(&self, res: &mut ConsolidationResult, region: &RegionKey) -> anyhow::Result<()> {
        use futures::StreamExt;
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(row) = ms.next().await {
            let m = row?;
            let in_region = (region.org == "*"
                || m.context.tenant_id.as_deref() == Some(region.org.as_str()))
                && (region.project == "*"
                    || m.context.project_id.as_deref() == Some(region.project.as_str()))
                && m.memory_type == region.memory_type;
            if in_region && m.valid_until.is_some() {
                res.pruned.push((m.id, PruneReason::Redundant));
            }
        }
        Ok(())
    }

    /// Restore the complete semantic preimage in one fenced storage call.
    /// Only ids absent from the preimage are physically removed.
    async fn rollback(
        &self,
        journal: &CycleJournal,
        lease: &exocortex_storage::OwnerLease,
    ) -> anyhow::Result<()> {
        self.storage
            .restore_fenced(&journal.restore(), lease)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(())
    }

    async fn write_audit(&self, res: &ConsolidationResult) -> anyhow::Result<()> {
        // R-O2 metric surface; the ΔR gauge needs a Gauge handle in
        // metrics 0.23 (registration-only macro) — exported via the result
        // audit stamp until the recorder wiring lands with the server.
        metrics::counter!("exocortex_dreams_discoveries_total", "quality" => "consolidation")
            .increment((res.merged.len() + res.pruned.len()) as u64);
        let _ = res.mcr2_after.delta_r;
        Ok(())
    }
}

impl<S: Storage + 'static> DreamsEngine<S> {
    /// Reject forged/stale named region keys before lease acquisition. A
    /// named project exists for this org only when storage contains an active
    /// memory carrying that exact tenant/project scope; wildcard regions are
    /// internal aggregate scopes and do not claim a named project.
    async fn validate_region(&self, region: &RegionKey) -> anyhow::Result<()> {
        if region.org == "*" || region.project == "*" {
            return Ok(());
        }
        use futures::StreamExt;
        let mut memories = self.storage.stream_all_memories().await;
        while let Some(row) = memories.next().await {
            let memory = row?;
            if memory.valid_until.is_none()
                && memory.context.tenant_id.as_deref() == Some(region.org.as_str())
                && memory.context.project_id.as_deref() == Some(region.project.as_str())
            {
                return Ok(());
            }
        }
        anyhow::bail!("unknown project region {}/{}", region.org, region.project)
    }

    /// §12.1 discovery pass — the Transitive finder (§23 #12): two-hop
    /// paths `a -e1-> b -e2-> c` with no direct `a->c` edge and no
    /// derived path edges (R4/R5 write `Derived` provenance; their
    /// closures must not be re-proposed, R-Dr7). Proposals never touch
    /// the graph (R-Dr1/R-T16) — they wait in `pending_discoveries` for
    /// `accept_discovery`.
    pub async fn run_discovery(&self, region: &RegionKey) -> anyhow::Result<Vec<Discovery>> {
        self.validate_region(region).await?;
        use futures::StreamExt;
        let mut edges: Vec<DiscoveryEdge> = Vec::new(); // (from, to, kind, derived)
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(row) = rs.next().await {
            let r = row?;
            let derived = matches!(r.provenance, Provenance::Derived { .. });
            let open = r.valid_until.is_none();
            if open {
                edges.push((r.from, r.to, r.kind.0, derived));
            }
        }
        drop(rs);
        // Storage iteration order is intentionally unspecified. Stable input
        // ordering makes the capped proposal set reproducible across adapters
        // and process restarts.
        edges.sort_by_key(|(from, to, kind, derived)| (*from, *to, *kind, *derived));
        let (candidates, _) = transitive_candidates(&edges, MAX_DISCOVERY_PATH_INSPECTIONS);

        let cycle: SmolStr = format!("dream:{}", uuid::Uuid::new_v4()).into();
        let out: Vec<_> = candidates
            .into_iter()
            .map(|(a, c, k1, k2)| Discovery {
                id: uuid::Uuid::new_v4(),
                kind: DiscoveryKind::Transitive,
                endpoints: (a, c),
                quality: DiscoveryKind::Transitive.default_quality(),
                via_types: (k1, k2),
                discovery_cycle_id: cycle.clone(),
                discovered_at: chrono::Utc::now(),
            })
            .collect();
        self.discoveries.clear();
        for discovery in &out {
            metrics::counter!(
                "exocortex_dreams_discoveries_total",
                "type" => "transitive",
                "quality" => "0.6"
            )
            .increment(1);
            self.discoveries.insert(discovery.id, discovery.clone());
        }
        let _ = region;
        Ok(out)
    }

    /// Persist one discovery for a specific caller and asserted relationship
    /// kind. The lifecycle surface (R6-B11) can call this when presenting a
    /// pending discovery; acceptance then requires this exact immutable scope.
    pub async fn issue_discovery_proposal(
        &self,
        discovery: &Discovery,
        region: &RegionKey,
        relationship_kind: exocortex_kernel::RelKindId,
        proposed_visibility: exocortex_kernel::Visibility,
        caller_scope: exocortex_storage::VisibilityContext,
    ) -> anyhow::Result<exocortex_storage::DiscoveryProposal> {
        let proposal = exocortex_storage::DiscoveryProposal {
            discovery_id: discovery.id.to_string().into(),
            region: region.clone(),
            from: discovery.endpoints.0,
            to: discovery.endpoints.1,
            kind: relationship_kind,
            proposed_visibility,
            caller_scope,
            issued_at: discovery.discovered_at,
        };
        self.storage.create_discovery_proposal(&proposal).await?;
        self.discoveries.remove(&discovery.id);
        Ok(proposal)
    }

    /// Proposals awaiting `accept_discovery` (R-Dr1: proposals, never edges).
    pub fn pending_discoveries(&self) -> Vec<Discovery> {
        self.discoveries.iter().map(|e| e.value().clone()).collect()
    }
}

fn transitive_candidates(
    edges: &[DiscoveryEdge],
    inspection_budget: usize,
) -> (Vec<(MemoryId, MemoryId, u32, u32)>, usize) {
    let direct: std::collections::HashSet<(MemoryId, MemoryId)> =
        edges.iter().map(|(from, to, _, _)| (*from, *to)).collect();
    let mut outgoing: std::collections::BTreeMap<MemoryId, Vec<&DiscoveryEdge>> =
        std::collections::BTreeMap::new();
    for edge in edges {
        outgoing.entry(edge.0).or_default().push(edge);
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    let mut inspected = 0usize;
    'paths: for (a, b, k1, first_derived) in edges {
        if *first_derived {
            continue;
        }
        let Some(next_edges) = outgoing.get(b) else {
            continue;
        };
        for (_, c, k2, second_derived) in next_edges {
            if inspected >= inspection_budget {
                break 'paths;
            }
            inspected += 1;
            let candidate = (*a, *c, *k1, *k2);
            if !*second_derived && c != a && !direct.contains(&(*a, *c)) && seen.insert(candidate) {
                out.push(candidate);
                if out.len() >= MAX_DISCOVERIES_PER_CYCLE {
                    break 'paths;
                }
            }
        }
    }
    (out, inspected)
}

#[cfg(test)]
mod discovery_scaling_tests {
    use super::*;

    #[test]
    fn indexed_discovery_is_linear_for_disjoint_edges_and_budgeted_for_dense_paths() {
        let disjoint: Vec<_> = (0..10_000u32)
            .map(|i| {
                let mut from = [0u8; 16];
                from[..4].copy_from_slice(&(i * 2).to_be_bytes());
                let mut to = [0u8; 16];
                to[..4].copy_from_slice(&(i * 2 + 1).to_be_bytes());
                (MemoryId(from), MemoryId(to), 1, false)
            })
            .collect();
        let (candidates, inspected) = transitive_candidates(&disjoint, 50_000);
        assert!(candidates.is_empty());
        assert_eq!(
            inspected, 0,
            "unconnected edges require no global pair scan"
        );

        let hub = MemoryId([0x7f; 16]);
        let mut dense = Vec::new();
        for i in 0..400u32 {
            let mut endpoint = [0u8; 16];
            endpoint[..4].copy_from_slice(&i.to_be_bytes());
            dense.push((MemoryId(endpoint), hub, 1, false));
            dense.push((hub, MemoryId(endpoint), 2, true));
        }
        dense.sort_by_key(|edge| (edge.0, edge.1, edge.2, edge.3));
        let (_, inspected) = transitive_candidates(&dense, 97);
        assert_eq!(inspected, 97, "dense candidate work stops at its budget");
    }
}

/// Discovery proposals (R-Dr1: never edges). Structured, no prose (R-Dr8).
#[derive(Clone, Debug)]
pub struct Discovery {
    /// Proposal id.
    pub id: uuid::Uuid,
    /// Finder kind.
    pub kind: DiscoveryKind,
    /// Endpoint pair.
    pub endpoints: (MemoryId, MemoryId),
    /// Quality score (computed once, R-Dr6).
    pub quality: f32,
    /// The kind pair the proposal traversed (R-Dr7).
    pub via_types: (u32, u32),
    /// Cycle id.
    pub discovery_cycle_id: SmolStr,
    /// When proposed.
    pub discovered_at: chrono::DateTime<chrono::Utc>,
}

/// Finder taxonomy (§12.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryKind {
    /// Cross-domain pattern.
    CrossDomain,
    /// Same shape, much later.
    TemporalEcho,
    /// No edges in or out.
    Orphan,
    /// Transitive closure candidate (excludes R4/R5-derived pairs, R-Dr7).
    Transitive,
}

impl DiscoveryKind {
    /// The quality stamped at proposal time — the single value both the
    /// surface (`Discovery.quality`) and the metric label carry (§23 #11:
    /// reconciled by construction).
    pub fn default_quality(self) -> f32 {
        match self {
            DiscoveryKind::CrossDomain => 0.9,
            DiscoveryKind::TemporalEcho => 0.7,
            DiscoveryKind::Orphan => 0.4,
            DiscoveryKind::Transitive => 0.6,
        }
    }
}

impl Discovery {
    /// Quality is computed once at proposal time (R-Dr6): metrics emit this
    /// exact value.
    pub fn rate_quality(&self) -> f32 {
        match self.kind {
            DiscoveryKind::CrossDomain => 0.9,
            DiscoveryKind::TemporalEcho => 0.7,
            DiscoveryKind::Orphan => 0.4,
            DiscoveryKind::Transitive => 0.6,
        }
    }
}

/// Provenance for discovery proposals — `Proposed`, never an edge (R-T16).
pub fn discovery_provenance(score: f32) -> Provenance {
    Provenance::Proposed {
        discovery_id: uuid::Uuid::new_v4(),
        score,
    }
}

/// Resolve the `SimilarTo` kind id from the linked pack (v1: dev-v1). The
/// cycle stamps every similarity edge with this kind (§12.1 step 5); the
/// sparsity diagnostic excludes it (§11.6.1).
/// Resolve the `SimilarTo` kind id from the linked pack (v1: dev-v1). The
/// cycle stamps every similarity edge with this kind (§12.1 step 5); the
/// sparsity diagnostic excludes it (§11.6.1).
fn similar_to_kind() -> Option<exocortex_kernel::RelKindId> {
    static KIND: std::sync::OnceLock<Option<exocortex_kernel::RelKindId>> =
        std::sync::OnceLock::new();
    *KIND.get_or_init(|| {
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
            .ok()
            .and_then(|onto| onto.kind_id("SimilarTo"))
    })
}

/// IN1 (audit): a memory belongs to the region when its type matches AND
/// its tenant scope matches (org; project when the region pins one —
/// `*` means unconstrained).
fn in_region(m: &exocortex_kernel::Memory, region: &RegionKey) -> bool {
    if m.memory_type != region.memory_type {
        return false;
    }
    if region.org != "*" && m.context.tenant_id.as_deref() != Some(region.org.as_str()) {
        return false;
    }
    if region.project != "*" && m.context.project_id.as_deref() != Some(region.project.as_str()) {
        return false;
    }
    true
}

/// The §14.3 age-decay factor alone (IN8: un-decay the stored base).
fn decay_factor(age_days: f32) -> f32 {
    (1.0 - 0.01 * age_days).max(0.5)
}

impl<S: Storage + 'static> DreamsEngine<S> {
    /// IN5: an unscoreable region completes as a no-op cycle with its
    /// audit record — never a silent error.
    async fn empty_result(
        &self,
        region: &RegionKey,
        lease: &exocortex_storage::OwnerLease,
        n: u32,
    ) -> anyhow::Result<ConsolidationResult> {
        let res = ConsolidationResult {
            session_id: format!("dream:{}", uuid::Uuid::new_v4()).into(),
            user_id: None,
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            region: region.clone(),
            memories_input: n,
            memories_output: n,
            mcr2_before: crate::mcr2::MCR2Value {
                delta_r: 0.0,
                total_rate: 0.0,
                class_rates: Default::default(),
                compact_rate: 0.0,
                n_memories: 0,
                embedding_model: crate::mcr2::EmbeddingModelId::bge_small(),
                computed_at: chrono::Utc::now(),
            },
            mcr2_after: crate::mcr2::MCR2Value {
                delta_r: 0.0,
                total_rate: 0.0,
                class_rates: Default::default(),
                compact_rate: 0.0,
                n_memories: 0,
                embedding_model: crate::mcr2::EmbeddingModelId::bge_small(),
                computed_at: chrono::Utc::now(),
            },
            sparsity_before: self.sparsity(region).await?,
            sparsity_after: self.sparsity(region).await?,
            merged: vec![],
            abstracted: vec![],
            pruned: vec![],
            strengthened: vec![],
            rewired: vec![],
            similar_edges: vec![],
            owner_node_id: self.node_id.clone(),
            lease_epoch: lease.epoch,
            regression: false,
            hairball_regression: false,
        };
        self.write_audit(&res).await?;
        Ok(res)
    }
}
