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
use exocortex_storage::{FencedBatchCommit, FencedRestore, LeaseKey, RegionKey, Storage};

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

const MAX_REGION_MEMORIES: usize = 50_000;
const MAX_REGION_RELATIONSHIPS: usize = 50_000;

type DiscoveryEdge = (MemoryId, MemoryId, u32, bool);

struct RegionWorkingSet {
    memories: std::collections::HashMap<MemoryId, Memory>,
    relationships: std::collections::HashMap<RelationshipId, Relationship>,
}

impl RegionWorkingSet {
    fn apply_relationship_writes(&mut self, writes: &[Relationship]) {
        let ontology = dreams_ontology();
        for relationship in writes {
            self.relationships
                .insert(relationship.id, relationship.clone());
            if let Some(inverse) = exocortex_kernel::materialize_inverse(ontology, relationship) {
                self.relationships.insert(inverse.id, inverse);
            }
        }
    }
}

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

struct CycleJournal {
    cycle_id: SmolStr,
    memories: std::collections::BTreeMap<MemoryId, Memory>,
    relationships: std::collections::BTreeMap<RelationshipId, Relationship>,
    created_memories: std::collections::BTreeMap<MemoryId, Memory>,
    created_relationships: std::collections::BTreeMap<RelationshipId, Relationship>,
    owned_memory_lsns: std::collections::BTreeMap<MemoryId, std::collections::BTreeSet<u64>>,
    owned_relationship_lsns:
        std::collections::BTreeMap<RelationshipId, std::collections::BTreeSet<u64>>,
}

impl CycleJournal {
    fn new(cycle_id: impl Into<SmolStr>) -> Self {
        Self {
            cycle_id: cycle_id.into(),
            memories: Default::default(),
            relationships: Default::default(),
            created_memories: Default::default(),
            created_relationships: Default::default(),
            owned_memory_lsns: Default::default(),
            owned_relationship_lsns: Default::default(),
        }
    }

    fn record_memory(&mut self, memory: &Memory) {
        if !self.created_memories.contains_key(&memory.id) {
            self.memories
                .entry(memory.id)
                .or_insert_with(|| memory.clone());
        }
    }

    fn record_relationship(&mut self, relationship: &Relationship) {
        if !self.created_relationships.contains_key(&relationship.id) {
            self.relationships
                .entry(relationship.id)
                .or_insert_with(|| relationship.clone());
        }
    }

    fn create_relationship(&mut self, relationship: &Relationship) {
        if !self.relationships.contains_key(&relationship.id) {
            self.created_relationships
                .entry(relationship.id)
                .or_insert_with(|| relationship.clone());
        }
    }

    fn prepare_relationship_writes(
        &mut self,
        writes: &[Relationship],
        current: &std::collections::HashMap<RelationshipId, Relationship>,
    ) {
        let ontology = dreams_ontology();
        let mut seen: std::collections::HashSet<RelationshipId> =
            writes.iter().map(|relationship| relationship.id).collect();
        for relationship in writes {
            self.prepare_relationship(relationship, current);
            if let Some(inverse) = exocortex_kernel::materialize_inverse(ontology, relationship) {
                if seen.insert(inverse.id) {
                    self.prepare_relationship(&inverse, current);
                }
            }
        }
    }

    fn prepare_relationship(
        &mut self,
        relationship: &Relationship,
        current: &std::collections::HashMap<RelationshipId, Relationship>,
    ) {
        match current.get(&relationship.id) {
            Some(preimage) => self.record_relationship(preimage),
            None => self.create_relationship(relationship),
        }
    }

    fn record_commit(&mut self, commit: &FencedBatchCommit) {
        for (id, lsns) in &commit.memory_lsns {
            self.owned_memory_lsns.entry(*id).or_default().extend(lsns);
        }
        for (id, lsns) in &commit.relationship_lsns {
            self.owned_relationship_lsns
                .entry(*id)
                .or_default()
                .extend(lsns);
        }
    }

    fn restore(&self) -> FencedRestore {
        FencedRestore {
            memories: self.memories.values().cloned().collect(),
            relationships: self.relationships.values().cloned().collect(),
            created_memories: self.created_memories.values().cloned().collect(),
            created_relationships: self.created_relationships.values().cloned().collect(),
            owned_memory_lsns: self.owned_memory_lsns.clone(),
            owned_relationship_lsns: self.owned_relationship_lsns.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.owned_memory_lsns.is_empty() && self.owned_relationship_lsns.is_empty()
    }
}

fn dreams_ontology() -> &'static exocortex_kernel::Ontology {
    static ONTOLOGY: std::sync::OnceLock<exocortex_kernel::Ontology> = std::sync::OnceLock::new();
    ONTOLOGY.get_or_init(|| {
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
            .expect("the compiled development ontology must be valid")
    })
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
    /// Regions already queued or executing. A region occupies at most one
    /// channel slot; writes that land meanwhile remain in `counters` and are
    /// considered again after the current cycle completes.
    pending_regions: DashMap<RegionKey, ()>,
    /// Optional shared Redis transport used by every node to record committed
    /// writes and by the elected owner to acknowledge completed cycles.
    distributed_fire: Option<Arc<tokio::sync::Mutex<fire::RedisFireQueue>>>,
    /// Last durably delivered local write event per region. Production uses
    /// Redis for the same immediate-retry idempotency contract.
    last_local_write_event: DashMap<RegionKey, SmolStr>,
    /// Distributed notification metadata retained until owner completion.
    distributed_notifications: DashMap<RegionKey, fire::FireMessage>,
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
    lease_ttl: Duration,
    #[cfg(feature = "testing")]
    cycle_fault_after: Option<usize>,
    #[cfg(feature = "testing")]
    cycle_crash_after: Option<usize>,
    #[cfg(feature = "testing")]
    cycle_pause_after: Option<(usize, Duration)>,
    #[cfg(feature = "testing")]
    rollback_pause: Option<Duration>,
    #[cfg(feature = "testing")]
    rollback_concurrent_memories: Vec<Memory>,
    #[cfg(feature = "testing")]
    renewal_failure_after: Option<usize>,
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
        let (tx_fire, rx_fire) = mpsc::channel(1000);
        Self {
            leader_gate: None,
            storage,
            counters: DashMap::new(),
            pending_regions: DashMap::new(),
            distributed_fire: None,
            last_local_write_event: DashMap::new(),
            distributed_notifications: DashMap::new(),
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
            lease_ttl: Duration::from_secs(60),
            #[cfg(feature = "testing")]
            cycle_fault_after: None,
            #[cfg(feature = "testing")]
            cycle_crash_after: None,
            #[cfg(feature = "testing")]
            cycle_pause_after: None,
            #[cfg(feature = "testing")]
            rollback_pause: None,
            #[cfg(feature = "testing")]
            rollback_concurrent_memories: Vec::new(),
            #[cfg(feature = "testing")]
            renewal_failure_after: None,
        }
    }

    /// Attach the production shared fire transport. Followers record shared
    /// counters through this queue but never execute consolidation locally.
    pub fn with_distributed_fire(
        mut self,
        queue: Arc<tokio::sync::Mutex<fire::RedisFireQueue>>,
    ) -> Self {
        self.distributed_fire = Some(queue);
        self
    }

    /// Inject a failure after the specified owner mutation. This exercises
    /// crash compensation against both the double and live backend.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_cycle_fault_after(mut self, mutation: usize) -> Self {
        self.cycle_fault_after = Some(mutation);
        self
    }

    /// Simulate process loss after a durable owner mutation, intentionally
    /// bypassing in-process compensation so a successor must recover it.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_cycle_crash_after(mut self, mutation: usize) -> Self {
        self.cycle_crash_after = Some(mutation);
        self
    }

    /// Acquire the region as a successor and recover only its active journal.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub async fn recover_active_cycle_for_test(&self, region: &RegionKey) -> anyhow::Result<()> {
        let lease_key = LeaseKey::Dreams {
            org: region.org.clone(),
            region: format!("{}:{}", region.project, region.memory_type).into(),
        };
        let lease = self
            .storage
            .acquire_lease(&lease_key, self.lease_ttl)
            .await
            .map_err(|error| anyhow::anyhow!("lease: {error}"))?;
        let recovery = self.recover_active_cycle(&lease_key, &lease).await;
        let release = self.storage.release_lease(lease).await;
        recovery?;
        release.map_err(|error| anyhow::anyhow!("release recovery lease: {error}"))
    }

    /// Use a short owner lease in deterministic renewal regressions.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Pause after an owner mutation so tests can cross the original lease
    /// expiry without relying on a slow backend.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_cycle_pause_after(mut self, mutation: usize, pause: Duration) -> Self {
        self.cycle_pause_after = Some((mutation, pause));
        self
    }

    /// Pause inside rollback so tests can prove the lease remains renewed for
    /// the complete compensation phase.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_rollback_pause(mut self, pause: Duration) -> Self {
        self.rollback_pause = Some(pause);
        self
    }

    /// Commit deterministic non-cycle writes immediately before compensation.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_rollback_concurrent_memories(mut self, memories: Vec<Memory>) -> Self {
        self.rollback_concurrent_memories = memories;
        self
    }

    /// Inject persistent lease-renewal failures beginning with `attempt`.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn with_renewal_failure_after(mut self, attempt: usize) -> Self {
        self.renewal_failure_after = Some(attempt);
        self
    }

    /// Record a write; fire when the predicate trips (§12.2 transport is
    /// Redis in production; the in-process channel is the same shape).
    /// R-Dr13: counters are NOT reset here — completion resets them, so a
    /// failed cycle never loses its write counts.
    pub async fn on_write(&self, region: RegionKey) -> anyhow::Result<()> {
        self.on_writes(region, 1, 0).await
    }

    /// Record committed memory/relationship deltas. With a distributed fire
    /// transport this is an atomic shared counter update on every node.
    pub async fn on_writes(
        &self,
        region: RegionKey,
        memories: u32,
        edges: u32,
    ) -> anyhow::Result<()> {
        if let Some(queue) = &self.distributed_fire {
            queue
                .lock()
                .await
                .record_write(
                    &region,
                    memories,
                    edges,
                    self.dreams_trigger,
                    self.node_id.as_str(),
                )
                .await?;
            return Ok(());
        }
        let now = chrono::Utc::now();
        // IN4: anchor the region's clock at first observation; cycles
        // re-stamp it on completion.
        let anchor = *self
            .last_cycle_at
            .entry(region.clone())
            .or_insert(now)
            .value();
        let mut e = self.counters.entry(region.clone()).or_default();
        e.memories_since_last_cycle = e.memories_since_last_cycle.saturating_add(memories);
        e.edges_since_last_cycle = e.edges_since_last_cycle.saturating_add(edges);
        e.seconds_since_last_cycle = (now - anchor).num_seconds().max(0) as u64;
        if self.is_leader() && self.dreams_trigger.should_fire(&e) {
            let snap = *e;
            drop(e);
            self.schedule_region(region, snap);
        }
        Ok(())
    }

    /// Deliver one stable post-ingest effect. The durable outbox drains in
    /// order, so retaining the last event per region is sufficient to make an
    /// ambiguous response retry idempotent without an unbounded processed set.
    pub async fn on_writes_once(
        &self,
        event_id: &str,
        region: RegionKey,
        memories: u32,
        edges: u32,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !event_id.is_empty(),
            "Dreams write event id must not be empty"
        );
        if let Some(queue) = &self.distributed_fire {
            queue
                .lock()
                .await
                .record_write_once(
                    &region,
                    memories,
                    edges,
                    self.dreams_trigger,
                    self.node_id.as_str(),
                    event_id,
                )
                .await?;
            return Ok(());
        }
        if self
            .last_local_write_event
            .get(&region)
            .is_some_and(|seen| seen.as_str() == event_id)
        {
            return Ok(());
        }
        self.on_writes(region.clone(), memories, edges).await?;
        self.last_local_write_event.insert(region, event_id.into());
        Ok(())
    }

    /// Fire a region explicitly (Redis fire-queue drainer side).
    pub fn notify(&self, region: RegionKey) {
        if !self.is_leader() {
            return;
        }
        let snap = self.counters.get(&region).map(|e| *e).unwrap_or_default();
        self.schedule_region(region, snap);
    }

    /// Deliver a shared notification to this elected owner. Its exact fired
    /// snapshot is acknowledged by [`Self::run`] only after the whole cycle.
    pub fn notify_distributed(&self, notification: fire::FireMessage) {
        if !self.is_leader() {
            return;
        }
        let fired_at = notification.fired_at.unwrap_or_default();
        let region = notification.region.clone();
        self.distributed_notifications
            .insert(region.clone(), notification);
        self.schedule_region(region, fired_at);
    }

    fn is_leader(&self) -> bool {
        self.leader_gate
            .as_ref()
            .is_none_or(|gate| gate.load(std::sync::atomic::Ordering::SeqCst))
    }

    fn schedule_region(&self, region: RegionKey, snapshot: RegionWriteCounters) {
        use dashmap::mapref::entry::Entry;
        match self.pending_regions.entry(region.clone()) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                entry.insert(());
                if self.tx_fire.try_send((region.clone(), snapshot)).is_err() {
                    self.pending_regions.remove(&region);
                    metrics::counter!("exocortex_dreams_queue_dropped_total").increment(1);
                }
            }
        }
    }

    fn complete_region(&self, region: &RegionKey, fired_at: RegionWriteCounters, success: bool) {
        if success {
            if let Some(mut current) = self.counters.get_mut(region) {
                current.memories_since_last_cycle = current
                    .memories_since_last_cycle
                    .saturating_sub(fired_at.memories_since_last_cycle);
                current.edges_since_last_cycle = current
                    .edges_since_last_cycle
                    .saturating_sub(fired_at.edges_since_last_cycle);
                current.seconds_since_last_cycle = 0;
            }
            self.last_cycle_at
                .insert(region.clone(), chrono::Utc::now());
        }
        self.pending_regions.remove(region);

        if success && self.is_leader() {
            if let Some(current) = self.counters.get(region).map(|entry| *entry) {
                if self.dreams_trigger.should_fire(&current) {
                    self.schedule_region(region.clone(), current);
                }
            }
        }
    }

    /// The production Dreams loop: consolidation and discovery under one
    /// region-owner lease.
    pub async fn run(self: Arc<Self>) {
        while let Some((region, fired_at)) = { self.rx_fire.lock().await.recv().await } {
            let fire_cycle_id = self
                .distributed_notifications
                .get(&region)
                .and_then(|notification| notification.fire_id.clone())
                .map(|fire_id| format!("dream-fire:{fire_id}"));
            let success = match self
                .try_consolidate_inner(&region, fire_cycle_id.as_deref())
                .await
            {
                Ok(Some(res)) => {
                    *self.last_result.write().await = Some(res.clone());
                    info!(?res, "Dreams cycle ok");
                    true
                }
                Ok(None) => {
                    info!(?region, "Dreams fire already settled; acknowledging replay");
                    true
                }
                Err(e) => {
                    warn!(?e, "Dreams cycle failed");
                    false
                }
            };
            self.complete_region(&region, fired_at, success);
            if let Some((_, notification)) = self.distributed_notifications.remove(&region) {
                if let Some(queue) = &self.distributed_fire {
                    let mut delay = Duration::from_millis(50);
                    loop {
                        match queue
                            .lock()
                            .await
                            .acknowledge(
                                &notification,
                                success,
                                self.dreams_trigger,
                                self.node_id.as_str(),
                            )
                            .await
                        {
                            Ok(_) => break,
                            Err(error) => {
                                warn!(?error, "Dreams distributed acknowledgement retrying");
                                tokio::time::sleep(delay).await;
                                delay = (delay * 2).min(Duration::from_secs(5));
                            }
                        }
                    }
                }
            }
        }
    }

    /// One full cycle under the region lease (§12.4 skeleton, filled per
    /// §12.5 steps 3-9). The lease is released on every path; all writes
    /// ride the fenced paths so a stale owner can never commit (R-C3).
    #[instrument(skip(self))]
    pub async fn try_consolidate(&self, region: &RegionKey) -> anyhow::Result<ConsolidationResult> {
        self.try_consolidate_inner(region, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fresh Dreams cycle unexpectedly already settled"))
    }

    /// Execute the exact distributed-fire idempotency path with a stable fire
    /// identity. Returns `None` when that fire already settled successfully.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub async fn try_consolidate_once_for_testing(
        &self,
        region: &RegionKey,
        fire_id: &str,
    ) -> anyhow::Result<Option<ConsolidationResult>> {
        self.try_consolidate_inner(region, Some(&format!("dream-fire:{fire_id}")))
            .await
    }

    async fn try_consolidate_inner(
        &self,
        region: &RegionKey,
        stable_cycle_id: Option<&str>,
    ) -> anyhow::Result<Option<ConsolidationResult>> {
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
            .acquire_lease(&lease_key, self.lease_ttl)
            .await
            .map_err(|e| anyhow::anyhow!("lease: {e}"))?;
        let cycle_id = stable_cycle_id
            .map(SmolStr::new)
            .unwrap_or_else(|| format!("dream:{}", uuid::Uuid::new_v4()).into());
        if self
            .storage
            .cycle_succeeded(&lease_key, cycle_id.as_str())
            .await
            .map_err(|error| anyhow::anyhow!("load Dreams cycle settlement: {error}"))?
        {
            self.storage
                .release_lease(lease)
                .await
                .map_err(|error| anyhow::anyhow!("release settled Dreams lease: {error}"))?;
            return Ok(None);
        }
        if let Err(error) = self.recover_active_cycle(&lease_key, &lease).await {
            let _ = self.storage.release_lease(lease).await;
            return Err(error);
        }
        let working_set = match self.load_region_working_set(region).await {
            Ok(working_set) => working_set,
            Err(error) => {
                let _ = self.storage.release_lease(lease).await;
                return Err(error);
            }
        };
        if let Err(error) = self.validate_loaded_region(region, &working_set) {
            let _ = self.storage.release_lease(lease).await;
            return Err(error);
        }
        // A cycle contains multiple scans and separately fenced mutations.
        // Keep the same token/epoch alive until compensation has finished;
        // otherwise an expiry after an early write makes rollback itself fail
        // its fence and strands a partial cycle.
        let mut renewal = self.spawn_lease_renewal(lease.clone());
        let mut journal = CycleJournal::new(cycle_id);
        let mut renewal_stopped = false;
        let outcome = {
            let consolidation =
                self.consolidate_under_tracked(&lease, region, &mut journal, working_set);
            tokio::pin!(consolidation);
            tokio::select! {
                biased;
                renewal_outcome = &mut renewal => {
                    renewal_stopped = true;
                    Err(Self::renewal_task_error(renewal_outcome))
                }
                outcome = &mut consolidation => outcome,
            }
        };
        #[cfg(feature = "testing")]
        let crashed = self.cycle_crash_after.is_some() && outcome.is_err();
        #[cfg(not(feature = "testing"))]
        let crashed = false;
        let outcome = if crashed {
            outcome
        } else if renewal_stopped {
            self.finish_cycle(outcome, &journal, &lease).await
        } else {
            let finishing = self.finish_cycle(outcome, &journal, &lease);
            tokio::pin!(finishing);
            tokio::select! {
                renewal_outcome = &mut renewal => {
                    renewal_stopped = true;
                    let renewal_error = Self::renewal_task_error(renewal_outcome);
                    match finishing.await {
                        Ok(_) => Err(renewal_error),
                        Err(cycle_error) => Err(anyhow::anyhow!(
                            "cycle failed ({cycle_error}); lease renewal also failed ({renewal_error})"
                        )),
                    }
                }
                outcome = &mut finishing => outcome,
            }
        };
        let outcome = match outcome {
            Ok(result) if !renewal_stopped && !crashed => {
                let discovery =
                    self.run_discovery_fenced(region, &lease, Some(journal.cycle_id.as_str()));
                tokio::pin!(discovery);
                tokio::select! {
                    biased;
                    renewal_outcome = &mut renewal => {
                        renewal_stopped = true;
                        Err(Self::renewal_task_error(renewal_outcome))
                    }
                    outcome = &mut discovery => outcome.map(|proposals| {
                        info!(count = proposals.len(), "discovery ok");
                        result
                    }),
                }
            }
            other => other,
        };
        if !renewal_stopped {
            renewal.abort();
            let _ = renewal.await;
        }
        let release = self.storage.release_lease(lease).await;
        match (outcome, release) {
            (Ok(result), Ok(())) => Ok(Some(result)),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(release)) => Err(anyhow::anyhow!("release Dreams lease: {release}")),
            (Err(error), Err(release)) => Err(anyhow::anyhow!(
                "cycle failed ({error}); lease release also failed ({release})"
            )),
        }
    }

    async fn recover_active_cycle(
        &self,
        lease_key: &LeaseKey,
        lease: &exocortex_storage::OwnerLease,
    ) -> anyhow::Result<()> {
        let Some(active) = self
            .storage
            .get_active_cycle_journal(lease_key)
            .await
            .map_err(|error| anyhow::anyhow!("load active Dreams journal: {error}"))?
        else {
            return Ok(());
        };
        self.storage
            .restore_fenced(&active.restore, lease)
            .await
            .map_err(|error| anyhow::anyhow!("recover active Dreams cycle: {error}"))?;
        self.storage
            .complete_cycle_journal_fenced(active.cycle_id.as_str(), lease)
            .await
            .map_err(|error| anyhow::anyhow!("complete recovered Dreams journal: {error}"))?;
        Ok(())
    }

    fn renewal_task_error(
        outcome: Result<anyhow::Result<()>, tokio::task::JoinError>,
    ) -> anyhow::Error {
        match outcome {
            Ok(Err(error)) => error,
            Ok(Ok(())) => anyhow::anyhow!("Dreams lease renewal stopped unexpectedly"),
            Err(error) => anyhow::anyhow!("Dreams lease renewal task failed: {error}"),
        }
    }

    fn spawn_lease_renewal(
        &self,
        lease: exocortex_storage::OwnerLease,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let storage = self.storage.clone();
        let ttl = (lease.expires_at - lease.acquired_at)
            .to_std()
            .unwrap_or(self.lease_ttl);
        let interval = (ttl / 3).max(Duration::from_millis(1));
        let retry = (ttl / 12).max(Duration::from_millis(1));
        let reserve = interval;
        #[cfg(feature = "testing")]
        let renewal_failure_after = self.renewal_failure_after;
        tokio::spawn(async move {
            let mut current = lease;
            let mut delay = interval;
            #[cfg(feature = "testing")]
            let mut attempt = 0usize;
            loop {
                tokio::time::sleep(delay).await;
                #[cfg(feature = "testing")]
                {
                    attempt += 1;
                }
                #[cfg(feature = "testing")]
                let renewed = if renewal_failure_after.is_some_and(|start| attempt >= start) {
                    Err(exocortex_storage::StorageError::Backend(
                        "injected lease renewal failure".into(),
                    ))
                } else {
                    storage.renew_lease(&current).await
                };
                #[cfg(not(feature = "testing"))]
                let renewed = storage.renew_lease(&current).await;
                match renewed {
                    Ok(renewed) => {
                        current = renewed;
                        delay = interval;
                    }
                    Err(error) => {
                        let remaining = (current.expires_at - chrono::Utc::now())
                            .to_std()
                            .unwrap_or_default();
                        if remaining <= reserve {
                            anyhow::bail!(
                                "Dreams owner lease renewal could not be confirmed with rollback reserve: {error}"
                            );
                        }
                        warn!(?error, ?remaining, "Dreams owner lease renewal retrying");
                        delay = retry.min(remaining.saturating_sub(reserve));
                    }
                }
            }
        })
    }

    async fn finish_cycle(
        &self,
        outcome: anyhow::Result<ConsolidationResult>,
        journal: &CycleJournal,
        lease: &exocortex_storage::OwnerLease,
    ) -> anyhow::Result<ConsolidationResult> {
        match outcome {
            Ok(result) => Ok(result),
            Err(error) => {
                if !journal.is_empty() {
                    self.rollback(journal, lease).await.map_err(|rollback| {
                        anyhow::anyhow!(
                            "cycle failed ({error}); atomic restore failed ({rollback})"
                        )
                    })?;
                    self.storage
                        .complete_cycle_journal_fenced(journal.cycle_id.as_str(), lease)
                        .await
                        .map_err(|complete| {
                            anyhow::anyhow!(
                                "cycle failed ({error}); rollback succeeded but journal completion failed ({complete})"
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
        mut working_set: RegionWorkingSet,
    ) -> anyhow::Result<ConsolidationResult> {
        let mut mutations = 0usize;
        let anchors = self.select_anchors(&working_set);
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
                return self
                    .empty_result(region, lease, anchors.len() as u32, &working_set)
                    .await;
            }
            Err(e) => return Err(e),
        };
        let sparsity_before = self.sparsity(&working_set);

        let mut res = ConsolidationResult {
            session_id: journal.cycle_id.clone(),
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
                self.merge(&mut res, c, lease, &mut working_set, journal)
                    .await?;
                if res.merged.len() > merged_before {
                    self.mutation_checkpoint(&mut mutations).await?;
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
        self.strengthen(&mut res, lease, &mut working_set, journal)
            .await?;
        if !res.strengthened.is_empty() {
            self.mutation_checkpoint(&mut mutations).await?;
        }
        self.prune(&mut res, &working_set);
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
        self.write_similar_edges(&mut res, &survivors, lease, &mut working_set, journal)
            .await?;
        if !res.similar_edges.is_empty() {
            self.mutation_checkpoint(&mut mutations).await?;
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
        res.sparsity_after = self.sparsity(&working_set);
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

    async fn mutation_checkpoint(&self, mutations: &mut usize) -> anyhow::Result<()> {
        *mutations += 1;
        #[cfg(feature = "testing")]
        if self
            .cycle_pause_after
            .is_some_and(|(mutation, _)| mutation == *mutations)
        {
            let pause = self
                .cycle_pause_after
                .expect("pause configuration checked")
                .1;
            tokio::time::sleep(pause).await;
        }
        #[cfg(feature = "testing")]
        if self.cycle_fault_after == Some(*mutations) || self.cycle_crash_after == Some(*mutations)
        {
            anyhow::bail!("injected cycle failure after mutation {mutations}");
        }
        Ok(())
    }

    async fn load_region_working_set(
        &self,
        region: &RegionKey,
    ) -> anyhow::Result<RegionWorkingSet> {
        let memories = self
            .storage
            .memories_in_region(region, MAX_REGION_MEMORIES as u32)
            .await?
            .into_iter()
            .map(|memory| (memory.id, memory))
            .collect();
        let relationships = self
            .storage
            .current_relationships_in_region(region, MAX_REGION_RELATIONSHIPS as u32)
            .await?
            .into_iter()
            .map(|relationship| (relationship.id, relationship))
            .collect();
        Ok(RegionWorkingSet {
            memories,
            relationships,
        })
    }

    /// Rank regional memories by recency decay; take top 32 (§12.5 step 3),
    /// deterministic tie-break by id.
    fn select_anchors(&self, working_set: &RegionWorkingSet) -> Vec<MemoryWithEmbedding> {
        let mut rows: Vec<(chrono::DateTime<chrono::Utc>, MemoryWithEmbedding)> = Vec::new();
        for memory in working_set.memories.values() {
            if memory.valid_until.is_some() {
                continue;
            }
            if let Some(embedding) = &memory.embedding {
                rows.push((
                    memory.recorded_at,
                    MemoryWithEmbedding {
                        id: memory.id,
                        class: memory.memory_type,
                        visibility: memory.visibility,
                        embedding: embedding.clone(),
                    },
                ));
            }
        }
        let now = chrono::Utc::now();
        rows.sort_by(|a, b| {
            let score = |t: chrono::DateTime<chrono::Utc>| -(now - t).num_days();
            score(b.0).cmp(&score(a.0)).then(b.1.id.cmp(&a.1.id))
        });
        rows.into_iter()
            .take(32)
            .map(|(_, memory)| memory)
            .collect()
    }

    fn score_with(&self, anchors: &[MemoryWithEmbedding]) -> anyhow::Result<MCR2Value> {
        Ok(MCR2Engine::default().compute(anchors)?)
    }

    fn sparsity(&self, working_set: &RegionWorkingSet) -> GraphSparsity {
        let nodes: Vec<_> = working_set
            .memories
            .values()
            .filter(|memory| memory.valid_until.is_none())
            .map(|memory| (memory.id, memory.memory_type))
            .collect();
        let members: std::collections::HashSet<exocortex_kernel::MemoryId> =
            nodes.iter().map(|(id, _)| *id).collect();
        let edges: Vec<_> = working_set
            .relationships
            .values()
            .filter(|relationship| {
                relationship.valid_until.is_none()
                    && members.contains(&relationship.from)
                    && members.contains(&relationship.to)
            })
            .map(|relationship| {
                (
                    relationship.from,
                    relationship.to,
                    relationship.kind.0,
                    0u64,
                    relationship.properties.confidence,
                )
            })
            .collect();
        // §11.6.1: the similarity bucket never counts toward out-degrees.
        compute_sparsity(&nodes, &edges, 32, similar_to_kind())
    }

    /// Merge a duplicate pair: keep the older row, close the newer with
    /// valid_until (bi-temporal; R-Dr10 keeps "why is this gone" answerable).
    async fn merge(
        &self,
        res: &mut ConsolidationResult,
        c: &mcr2::MergeCandidate,
        lease: &exocortex_storage::OwnerLease,
        working_set: &mut RegionWorkingSet,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<()> {
        let survivor_live = working_set
            .memories
            .get(&c.a)
            .is_some_and(|memory| memory.valid_until.is_none());
        let newer = working_set
            .memories
            .get(&c.b)
            .filter(|memory| memory.valid_until.is_none())
            .cloned();
        if survivor_live {
            if let Some(mut m) = newer {
                let now = chrono::Utc::now();
                journal.record_memory(&m);
                let mut relationship_updates = std::collections::BTreeMap::new();
                for relationship in working_set.relationships.values().filter(|relationship| {
                    relationship.valid_until.is_none()
                        && (relationship.from == c.b || relationship.to == c.b)
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
                    if let Some(existing) = working_set.relationships.get(&rewired.id) {
                        if existing.valid_until.is_none() {
                            continue;
                        }
                        journal.record_relationship(existing);
                    } else {
                        journal.create_relationship(&rewired);
                    }
                    res.rewired.push(rewired.id);
                    relationship_updates.insert(rewired.id, rewired);
                }

                m.valid_until = Some(now);
                m.invalidated_by = Some(c.a);
                let relationship_updates: Vec<_> = relationship_updates.into_values().collect();
                journal
                    .prepare_relationship_writes(&relationship_updates, &working_set.relationships);
                let commit = self
                    .storage
                    .upsert_batch_fenced_journaled(
                        &[m.clone()],
                        &relationship_updates,
                        &journal.restore(),
                        journal.cycle_id.as_str(),
                        lease,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                journal.record_commit(&commit);
                working_set.memories.insert(m.id, m);
                working_set.apply_relationship_writes(&relationship_updates);
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
        lease: &exocortex_storage::OwnerLease,
        working_set: &mut RegionWorkingSet,
        journal: &mut CycleJournal,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let mut updates = Vec::new();
        // IN1: only edges with BOTH endpoints inside the leased region.
        let member_ids: std::collections::HashSet<exocortex_kernel::MemoryId> = working_set
            .memories
            .values()
            .filter(|memory| memory.valid_until.is_none())
            .map(|memory| memory.id)
            .collect();
        for row in working_set.relationships.values() {
            let mut r = row.clone();
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
        updates.sort_by_key(|relationship| relationship.id);
        journal.prepare_relationship_writes(&updates, &working_set.relationships);
        for r in &updates {
            res.strengthened.push(r.id);
        }
        if !updates.is_empty() {
            let commit = self
                .storage
                .upsert_batch_fenced_journaled(
                    &[],
                    &updates,
                    &journal.restore(),
                    journal.cycle_id.as_str(),
                    lease,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            journal.record_commit(&commit);
            working_set.apply_relationship_writes(&updates);
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
        working_set: &mut RegionWorkingSet,
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
                use exocortex_kernel::{
                    relationship_visibility, Relationship, RelationshipProperties, LSN,
                };
                edges.push(Relationship {
                    id: exocortex_kernel::RelationshipId::derive(a.id, similar_kind, b.id, None),
                    kind: similar_kind,
                    from: a.id,
                    to: b.id,
                    visibility: relationship_visibility(a.visibility, b.visibility),
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
        let mut fresh = Vec::new();
        for edge in edges {
            match working_set.relationships.get(&edge.id) {
                Some(row) if row.valid_until.is_none() => continue,
                Some(row) => journal.record_relationship(row),
                None => journal.create_relationship(&edge),
            }
            fresh.push(edge);
        }
        if !fresh.is_empty() {
            journal.prepare_relationship_writes(&fresh, &working_set.relationships);
            let commit = self
                .storage
                .upsert_batch_fenced_journaled(
                    &[],
                    &fresh,
                    &journal.restore(),
                    journal.cycle_id.as_str(),
                    lease,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            journal.record_commit(&commit);
            res.similar_edges.extend(fresh.iter().map(|e| e.id));
            working_set.apply_relationship_writes(&fresh);
            metrics::counter!("exocortex_dreams_similar_edges_total").increment(fresh.len() as u64);
        }
        Ok(())
    }

    /// Prune: closed rows (valid_until set) are recorded Redundant — the
    /// audit trail survives the memory (R-Dr9/R-Dr10).
    fn prune(&self, res: &mut ConsolidationResult, working_set: &RegionWorkingSet) {
        for memory in working_set.memories.values() {
            if memory.valid_until.is_some() {
                res.pruned.push((memory.id, PruneReason::Redundant));
            }
        }
        res.pruned.sort_by_key(|(id, _)| *id);
    }

    /// Restore the complete semantic preimage in one fenced storage call.
    /// Only ids absent from the preimage are physically removed.
    async fn rollback(
        &self,
        journal: &CycleJournal,
        lease: &exocortex_storage::OwnerLease,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "testing")]
        if let Some(pause) = self.rollback_pause {
            tokio::time::sleep(pause).await;
        }
        #[cfg(feature = "testing")]
        if !self.rollback_concurrent_memories.is_empty() {
            self.storage
                .upsert_batch(&self.rollback_concurrent_memories, &[])
                .await
                .map_err(|error| anyhow::anyhow!("inject concurrent rollback write: {error}"))?;
        }
        self.storage
            .restore_fenced(&journal.restore(), lease)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(())
    }

    async fn write_audit(&self, res: &ConsolidationResult) -> anyhow::Result<()> {
        metrics::counter!("exocortex_dreams_discoveries_total", "quality" => "consolidation")
            .increment((res.merged.len() + res.pruned.len()) as u64);
        Ok(())
    }
}

impl<S: Storage + 'static> DreamsEngine<S> {
    fn validate_loaded_region(
        &self,
        region: &RegionKey,
        working_set: &RegionWorkingSet,
    ) -> anyhow::Result<()> {
        if region.org == "*" || region.project == "*" {
            return Ok(());
        }
        if working_set
            .memories
            .values()
            .any(|memory| memory.valid_until.is_none())
        {
            return Ok(());
        }
        anyhow::bail!("unknown project region {}/{}", region.org, region.project)
    }

    /// Reject forged/stale named region keys before lease acquisition. A
    /// named project exists for this org only when storage contains an active
    /// memory carrying that exact tenant/project scope; wildcard regions are
    /// internal aggregate scopes and do not claim a named project.
    async fn validate_region(&self, region: &RegionKey) -> anyhow::Result<()> {
        if region.org == "*" || region.project == "*" {
            return Ok(());
        }
        let memories = self
            .storage
            .memories_in_region(region, MAX_REGION_MEMORIES as u32)
            .await?;
        let working_set = RegionWorkingSet {
            memories: memories
                .into_iter()
                .map(|memory| (memory.id, memory))
                .collect(),
            relationships: Default::default(),
        };
        self.validate_loaded_region(region, &working_set)
    }

    /// §12.1 discovery pass — the Transitive finder (§23 #12): two-hop
    /// paths `a -e1-> b -e2-> c` with no direct `a->c` edge and no
    /// derived path edges (R4/R5 write `Derived` provenance; their
    /// closures must not be re-proposed, R-Dr7). Proposals never touch
    /// the graph (R-Dr1/R-T16) — they wait in `pending_discoveries` for
    /// `accept_discovery`.
    pub async fn run_discovery(&self, region: &RegionKey) -> anyhow::Result<Vec<Discovery>> {
        let lease_key = LeaseKey::Dreams {
            org: region.org.clone(),
            region: format!("{}:{}", region.project, region.memory_type).into(),
        };
        let lease = self
            .storage
            .acquire_lease(&lease_key, self.lease_ttl)
            .await
            .map_err(|error| anyhow::anyhow!("lease: {error}"))?;
        let mut renewal = self.spawn_lease_renewal(lease.clone());
        let outcome = {
            let discovery = async {
                self.validate_region(region).await?;
                self.run_discovery_fenced(region, &lease, None).await
            };
            tokio::pin!(discovery);
            tokio::select! {
                biased;
                renewal_outcome = &mut renewal => Err(Self::renewal_task_error(renewal_outcome)),
                outcome = &mut discovery => outcome,
            }
        };
        renewal.abort();
        let _ = renewal.await;
        let release = self.storage.release_lease(lease).await;
        match (outcome, release) {
            (Ok(discoveries), Ok(())) => Ok(discoveries),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(release)) => Err(anyhow::anyhow!("release Dreams lease: {release}")),
            (Err(error), Err(release)) => Err(anyhow::anyhow!(
                "discovery failed ({error}); lease release also failed ({release})"
            )),
        }
    }

    async fn run_discovery_fenced(
        &self,
        region: &RegionKey,
        lease: &exocortex_storage::OwnerLease,
        cycle_id: Option<&str>,
    ) -> anyhow::Result<Vec<Discovery>> {
        let relationships = self
            .storage
            .relationships_in_region(region, MAX_DISCOVERY_PATH_INSPECTIONS as u32)
            .await?;
        let edges: Vec<DiscoveryEdge> = relationships
            .into_iter()
            .map(|relationship| {
                (
                    relationship.from,
                    relationship.to,
                    relationship.kind.0,
                    matches!(relationship.provenance, Provenance::Derived { .. }),
                )
            })
            .collect();
        let (candidates, _) = transitive_candidates(&edges, MAX_DISCOVERY_PATH_INSPECTIONS);

        let cycle: SmolStr = cycle_id
            .map(SmolStr::new)
            .unwrap_or_else(|| format!("dream:{}", uuid::Uuid::new_v4()).into());
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
        let records = out
            .iter()
            .map(|discovery| exocortex_storage::DiscoveryRecord {
                discovery_id: discovery.id.to_string().into(),
                region: region.clone(),
                from: discovery.endpoints.0,
                to: discovery.endpoints.1,
                discovery_type: "transitive".into(),
                quality: discovery.quality,
                via_types: [discovery.via_types.0, discovery.via_types.1],
                discovery_cycle_id: discovery.discovery_cycle_id.clone(),
                discovered_at: discovery.discovered_at,
            })
            .collect::<Vec<_>>();
        if let Some(cycle_id) = cycle_id {
            self.storage
                .settle_dreams_cycle_fenced(cycle_id, &records, lease)
                .await?;
        } else {
            for record in &records {
                self.storage.store_discovery_fenced(record, lease).await?;
            }
        }
        self.discoveries.clear();
        for discovery in &out {
            emit_discovery_metric(discovery);
            self.discoveries.insert(discovery.id, discovery.clone());
        }
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
mod cycle_scheduling_tests {
    use super::*;
    use exocortex_storage::InMemoryStorage;

    fn region() -> RegionKey {
        RegionKey {
            org: "org".into(),
            project: "project".into(),
            memory_type: 3,
        }
    }

    fn engine(storage: &InMemoryStorage) -> DreamsEngine<InMemoryStorage> {
        DreamsEngine::new(
            Arc::new(storage.clone_dyn()),
            DreamsTrigger {
                memory_threshold: 1,
                edge_threshold: u32::MAX,
                age_floor_days: u32::MAX,
                min_interval_hours: 0,
            },
            0.01,
            0.05,
            false,
            "cycle-test".into(),
        )
    }

    fn storage() -> InMemoryStorage {
        InMemoryStorage::new(Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .expect("development ontology"),
        ))
    }

    #[tokio::test]
    async fn region_has_one_pending_cycle_and_retains_post_fire_writes() {
        let storage = storage();
        let engine = engine(&storage);
        let region = region();

        engine.on_write(region.clone()).await.unwrap();
        engine.on_write(region.clone()).await.unwrap();
        engine.notify(region.clone());
        assert_eq!(engine.pending_regions.len(), 1);
        let fired_at = {
            let mut receiver = engine.rx_fire.lock().await;
            let (_, fired_at) = receiver.try_recv().expect("one scheduled cycle");
            assert!(receiver.try_recv().is_err(), "the region must be coalesced");
            fired_at
        };
        assert_eq!(fired_at.memories_since_last_cycle, 1);

        engine.complete_region(&region, fired_at, true);
        assert_eq!(
            engine
                .counters
                .get(&region)
                .unwrap()
                .memories_since_last_cycle,
            1,
            "the write after fire remains pending"
        );
        assert_eq!(engine.pending_regions.len(), 1);
        let second = {
            let mut receiver = engine.rx_fire.lock().await;
            receiver
                .try_recv()
                .expect("retained write schedules next cycle")
        };
        assert_eq!(second.1.memories_since_last_cycle, 1);
        engine.complete_region(&region, second.1, true);
        assert_eq!(
            engine
                .counters
                .get(&region)
                .unwrap()
                .memories_since_last_cycle,
            0
        );
        assert!(engine.pending_regions.is_empty());
    }

    #[tokio::test]
    async fn stable_local_write_event_is_counted_once() {
        let storage = storage();
        let engine = engine(&storage);
        let region = region();
        engine
            .on_writes_once("batch:region", region.clone(), 2, 3)
            .await
            .unwrap();
        engine
            .on_writes_once("batch:region", region.clone(), 2, 3)
            .await
            .unwrap();
        let counters = *engine.counters.get(&region).unwrap();
        assert_eq!(counters.memories_since_last_cycle, 2);
        assert_eq!(counters.edges_since_last_cycle, 3);
    }

    #[tokio::test]
    async fn failed_cycle_keeps_counters_for_a_later_retry() {
        let storage = storage();
        let engine = engine(&storage);
        let region = region();
        engine.on_write(region.clone()).await.unwrap();
        let fired_at = {
            let mut receiver = engine.rx_fire.lock().await;
            receiver.try_recv().expect("scheduled cycle").1
        };

        engine.complete_region(&region, fired_at, false);
        assert_eq!(
            engine
                .counters
                .get(&region)
                .unwrap()
                .memories_since_last_cycle,
            1
        );
        assert!(engine.pending_regions.is_empty());
        engine.notify(region.clone());
        let retry = engine.rx_fire.lock().await.try_recv().expect("retry cycle");
        assert_eq!(retry.1.memories_since_last_cycle, 1);
    }

    #[tokio::test]
    async fn follower_rejects_cycle_before_any_region_scan() {
        let storage = storage();
        let leader = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let engine = engine(&storage).with_leader_gate(leader);

        let error = engine.try_consolidate(&region()).await.unwrap_err();
        assert!(error.to_string().contains("not the elected leader"));
        assert_eq!(
            storage.reasoning_query_counts(),
            (0, 0, 0, 0),
            "a follower must not bulk-scan storage"
        );
    }
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
        self.quality
    }
}

fn emit_discovery_metric(discovery: &Discovery) {
    match (discovery.kind, discovery.quality.to_bits()) {
        (DiscoveryKind::CrossDomain, bits) if bits == 0.9_f32.to_bits() => {
            metrics::counter!("exocortex_dreams_discoveries_total", "type" => "cross_domain", "quality" => "0.9").increment(1);
        }
        (DiscoveryKind::TemporalEcho, bits) if bits == 0.7_f32.to_bits() => {
            metrics::counter!("exocortex_dreams_discoveries_total", "type" => "temporal_echo", "quality" => "0.7").increment(1);
        }
        (DiscoveryKind::Orphan, bits) if bits == 0.4_f32.to_bits() => {
            metrics::counter!("exocortex_dreams_discoveries_total", "type" => "orphan", "quality" => "0.4").increment(1);
        }
        (DiscoveryKind::Transitive, bits) if bits == 0.6_f32.to_bits() => {
            metrics::counter!("exocortex_dreams_discoveries_total", "type" => "transitive", "quality" => "0.6").increment(1);
        }
        _ => metrics::counter!("exocortex_dreams_discoveries_total", "type" => "invalid", "quality" => "invalid").increment(1),
    }
}

#[cfg(test)]
mod discovery_metric_tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CaptureRecorder {
        labels: Mutex<Vec<(String, String)>>,
    }

    impl metrics::Recorder for CaptureRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            self.labels.lock().unwrap().extend(
                key.labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned())),
            );
            metrics::Counter::noop()
        }
        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn discovery_metric_reports_the_stamped_quality() {
        let recorder = CaptureRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let discovery = Discovery {
            id: uuid::Uuid::nil(),
            kind: DiscoveryKind::Transitive,
            endpoints: (MemoryId([0; 16]), MemoryId([1; 16])),
            quality: DiscoveryKind::Transitive.default_quality(),
            via_types: (1, 2),
            discovery_cycle_id: "cycle".into(),
            discovered_at: chrono::Utc::now(),
        };
        emit_discovery_metric(&discovery);
        let labels = recorder.labels.lock().unwrap();
        assert!(labels.contains(&("quality".into(), discovery.rate_quality().to_string())));
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
        working_set: &RegionWorkingSet,
    ) -> anyhow::Result<ConsolidationResult> {
        let sparsity = self.sparsity(working_set);
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
            sparsity_before: sparsity.clone(),
            sparsity_after: sparsity,
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
