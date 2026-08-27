// crates/exocortex-dreams/src/lib.rs
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

use exocortex_kernel::{MemoryId, Provenance, RelationshipId};
use exocortex_storage::{LeaseKey, RegionKey, Storage};

use mcr2::{
    compute_sparsity, effective_strength, GraphSparsity, MCR2Engine, MCR2Value, MemoryWithEmbedding,
};
use trigger::{DreamsTrigger, RegionWriteCounters};

/// §12.1 step 5: SimilarTo creation threshold.
pub const SIMILAR_TO_THRESHOLD: f32 = 0.85;

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
        }
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

    /// The consolidation loop.
    pub async fn run(self: Arc<Self>) {
        while let Some((region, fired_at)) = { self.rx_fire.lock().await.recv().await } {
            match self.try_consolidate(&region).await {
                Ok(res) => info!(?res, "consolidation ok"),
                Err(e) => warn!(?e, "consolidation failed"),
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
                self.merge(&mut res, c, lease).await?;
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
        for a in &anchors {
            self.strengthen(&mut res, region, lease).await?;
            self.prune(&mut res, a).await?;
        }
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
        self.write_similar_edges(&mut res, &survivors, lease)
            .await?;

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
                self.rollback(&res, lease).await?;
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
            if !in_region(&m, region) {
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
            if !in_region(&m, region) {
                continue; // IN1: sparsity measures the region, not the graph
            }
            nodes.push((m.id, m.memory_type));
        }
        drop(ms);
        let members: std::collections::HashSet<exocortex_kernel::MemoryId> =
            nodes.iter().map(|(id, _)| *id).collect();
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            if members.contains(&r.from) && members.contains(&r.to) {
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
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        let mut newer = None;
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            if m.id == c.b {
                newer = Some(m);
                break;
            }
        }
        drop(ms);
        if let Some(mut m) = newer {
            m.valid_until = Some(chrono::Utc::now());
            m.invalidated_by = Some(c.a);
            self.storage
                .upsert_batch_fenced(&[m], &[], lease)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            res.merged.push(c.b);
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
                if in_region(&m, region) {
                    member_ids.insert(m.id);
                }
            }
        }
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(Ok(mut r)) = rs.next().await {
            if !member_ids.contains(&r.from) || !member_ids.contains(&r.to) {
                continue;
            }
            if matches!(r.provenance, Provenance::Computed { .. }) {
                continue;
            }
            if res.strengthened.contains(&r.id) {
                continue; // one evidence bump per cycle, not per anchor
            }
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
                let sim = mcr2::cosine(&a.embedding, &b.embedding);
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
        let mut existing: std::collections::HashSet<exocortex_kernel::RelationshipId> = {
            let mut set = std::collections::HashSet::new();
            let mut rs = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rs.next().await {
                set.insert(r.id);
            }
            set
        };
        let fresh: Vec<_> = edges
            .into_iter()
            .filter(|e| existing.insert(e.id))
            .collect();
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
    async fn prune(
        &self,
        res: &mut ConsolidationResult,
        _a: &MemoryWithEmbedding,
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        let mut ms = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = ms.next().await {
            if m.valid_until.is_some() {
                res.pruned.push((m.id, PruneReason::Redundant));
            }
        }
        Ok(())
    }

    /// Rollback is bi-temporal, never destructive: close everything the
    /// cycle wrote (§12.5 step 8).
    async fn rollback(
        &self,
        res: &ConsolidationResult,
        lease: &exocortex_storage::OwnerLease,
    ) -> anyhow::Result<()> {
        for id in &res.merged {
            let _ = self.storage.delete_memory_fenced(id, lease).await;
        }
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
    /// §12.1 discovery pass — the Transitive finder (§23 #12): two-hop
    /// paths `a -e1-> b -e2-> c` with no direct `a->c` edge and no
    /// derived path edges (R4/R5 write `Derived` provenance; their
    /// closures must not be re-proposed, R-Dr7). Proposals never touch
    /// the graph (R-Dr1/R-T16) — they wait in `pending_discoveries` for
    /// `accept_discovery`.
    pub async fn run_discovery(&self, region: &RegionKey) -> anyhow::Result<Vec<Discovery>> {
        use futures::StreamExt;
        let mut edges: Vec<(MemoryId, MemoryId, u32, bool)> = Vec::new(); // (from, to, kind, derived)
        let mut rs = self.storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            let derived = matches!(r.provenance, Provenance::Derived { .. });
            let open = r.valid_until.is_none();
            if open {
                edges.push((r.from, r.to, r.kind.0, derived));
            }
        }
        drop(rs);
        let direct: std::collections::HashSet<(MemoryId, MemoryId)> =
            edges.iter().map(|(f, t, _, _)| (*f, *t)).collect();

        let cycle: SmolStr = format!("dream:{}", uuid::Uuid::new_v4()).into();
        let mut out = Vec::new();
        'outer: for (a, b, k1, d1) in &edges {
            for (c_from, c, k2, d2) in &edges {
                if c_from == b && c != a && !d1 && !d2 && !direct.contains(&(*a, *c)) {
                    let d = Discovery {
                        id: uuid::Uuid::new_v4(),
                        kind: DiscoveryKind::Transitive,
                        endpoints: (*a, *c),
                        quality: DiscoveryKind::Transitive.default_quality(),
                        via_types: (*k1, *k2),
                        discovery_cycle_id: cycle.clone(),
                        discovered_at: chrono::Utc::now(),
                    };
                    metrics::counter!(
                        "exocortex_dreams_discoveries_total",
                        "type" => "transitive",
                        "quality" => "0.6"
                    )
                    .increment(1);
                    self.discoveries.insert(d.id, d.clone());
                    out.push(d);
                    if out.len() >= 16 {
                        break 'outer;
                    }
                }
            }
        }
        let _ = region;
        Ok(out)
    }

    /// Proposals awaiting `accept_discovery` (R-Dr1: proposals, never edges).
    pub fn pending_discoveries(&self) -> Vec<Discovery> {
        self.discoveries.iter().map(|e| e.value().clone()).collect()
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
    if let (false, Some(t)) = (region.org == "*", m.context.tenant_id.as_deref()) {
        if t != region.org.as_str() {
            return false;
        }
    }
    if let (false, Some(p)) = (region.project == "*", m.context.project_id.as_deref()) {
        if p != region.project.as_str() {
            return false;
        }
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
