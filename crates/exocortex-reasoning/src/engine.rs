//! The two-language runtime (§10.6): asynchronous reasoning over a queue,
//! Crepe fixpoints on k-hop bounded fact scopes, derived-edge writeback with
//! `Provenance::Derived`, and R6 (`reverse_solves`) as a Steel program.

use std::sync::Arc;

use exocortex_kernel::{Memory, MemoryId, Provenance, Relationship, RelationshipId};
use exocortex_storage::{Storage, StorageError};
use tokio::sync::mpsc;
use tracing::{instrument, warn};

use crate::rules::{self, Edge, EntityFact, TagFact};

/// Queued reasoning work.
pub enum ReasoningWork {
    /// Evaluate rules over the k-hop neighborhood of a seed.
    KHopOver {
        /// Seed memory.
        seed: MemoryId,
        /// Hop bound (2 interactive, 3 enrichment; §10.5 R-L4).
        k: u8,
    },
    /// Enrichment pass after a session-wrapup commit.
    SessionWrapup {
        /// The committed memories.
        memories: Vec<MemoryId>,
    },
    /// Durable session enrichment with completion acknowledgement.
    #[doc(hidden)]
    DurableSessionWrapup {
        /// The committed memories.
        memories: Vec<MemoryId>,
        /// Signals only after all reasoning reads and writes succeed.
        completion: tokio::sync::oneshot::Sender<Result<(), StorageError>>,
    },
}

/// The reasoning engine: storage-backed, queue-fed, single consumer.
pub struct ReasoningEngine<S: Storage> {
    storage: Arc<S>,
    tx_work: mpsc::Sender<ReasoningWork>,
    rx_work: tokio::sync::Mutex<mpsc::Receiver<ReasoningWork>>,
    k_hop: u8,
}

impl<S: Storage + 'static> ReasoningEngine<S> {
    /// Build the engine over a storage backend. `prime`s the pack rule ids.
    pub fn new(storage: Arc<S>, queue_depth: usize, k_hop: u8) -> Self {
        rules::prime(&storage_ontology(&storage));
        let (tx, rx) = mpsc::channel(queue_depth);
        Self {
            storage,
            tx_work: tx,
            rx_work: tokio::sync::Mutex::new(rx),
            k_hop,
        }
    }

    /// Enqueue work; overflow is observable, never silent (R-L7).
    pub async fn enqueue(&self, w: ReasoningWork) {
        if self.tx_work.try_send(w).is_err() {
            metrics::counter!("exocortex_reasoning_dropped_total").increment(1);
            warn!("reasoning queue full; dropping work");
        }
    }

    /// The consumer loop.
    pub async fn run(self: Arc<Self>) {
        loop {
            // The receiver remains engine-owned. Cancelling or panicking this
            // worker releases the guard, allowing its supervisor to restart
            // without losing queued durable work.
            let Some(w) = self.rx_work.lock().await.recv().await else {
                return;
            };
            match w {
                ReasoningWork::KHopOver { seed, k } => self.k_hop_reason(seed, k).await,
                ReasoningWork::SessionWrapup { memories } => {
                    if let Err(error) = self.process_session_wrapup(&memories).await {
                        warn!(?error, "session reasoning failed");
                    }
                }
                ReasoningWork::DurableSessionWrapup {
                    memories,
                    completion,
                } => {
                    let _ = completion.send(self.process_session_wrapup(&memories).await);
                }
            }
        }
    }

    /// Enqueue durable session work and wait for successful read/write
    /// completion. Queue saturation applies backpressure; worker failure is an
    /// error, so callers retain their durable outbox record for retry.
    pub async fn process_durable_session_wrapup(
        &self,
        memories: Vec<MemoryId>,
    ) -> Result<(), StorageError> {
        let (completion, completed) = tokio::sync::oneshot::channel();
        self.tx_work
            .send(ReasoningWork::DurableSessionWrapup {
                memories,
                completion,
            })
            .await
            .map_err(|_| StorageError::Backend("reasoning worker is unavailable".into()))?;
        completed.await.map_err(|_| {
            StorageError::Backend("reasoning worker stopped before completion".into())
        })?
    }

    /// Bounded k-hop reasoning (§10.7 step 4): gather the neighborhood,
    /// load facts into Crepe, run the fixpoint, write new relationships back
    /// with `Provenance::Derived { rule_id, evidence }`.
    ///
    /// §10.6: the neighborhood harvest is ONE relationship scan (O(E)),
    /// not one scan per hop — adjacency is built once, then BFS over it is
    /// bounded by `k` and the CR-6 node caps. Previously O(hops·E).
    #[instrument(skip(self))]
    pub async fn k_hop_reason(&self, seed: MemoryId, k: u8) {
        if let Err(error) = self.try_k_hop_reason(seed, k).await {
            warn!(?error, "bounded reasoning pass failed");
        }
    }

    async fn try_k_hop_reason(&self, seed: MemoryId, k: u8) -> Result<(), StorageError> {
        let k = k.clamp(1, self.k_hop.max(1));
        let mut edges: Vec<Edge> = Vec::new();
        let entities: Vec<EntityFact>;
        let tags: Vec<TagFact>;

        // Delta-driven BFS from the changed seed. Each hop asks storage for
        // only edges touching the current frontier; it never enumerates the
        // graph-wide relationship table.
        const MAX_NODES: usize = 512;
        const MAX_EDGES: usize = 4096;
        let mut neighborhood: std::collections::HashSet<MemoryId> =
            std::collections::HashSet::from([seed]);
        let mut seen_edges: std::collections::HashSet<RelationshipId> =
            std::collections::HashSet::new();
        let mut relationship_rows = Vec::new();
        let mut frontier = vec![seed];
        for _hop in 0..k {
            let mut next = Vec::new();
            let rows = match self
                .storage
                .relationships_touching(&frontier, MAX_EDGES as u32)
                .await
            {
                Ok(rows) => rows,
                Err(error) => return Err(error),
            };
            for row in rows {
                if !seen_edges.insert(row.id) {
                    continue;
                }
                for other in [row.from, row.to] {
                    if neighborhood.len() < MAX_NODES && neighborhood.insert(other) {
                        next.push(other);
                    }
                }
                relationship_rows.push(row);
            }
            if next.is_empty() || relationship_rows.len() >= MAX_EDGES {
                break;
            }
            frontier = next;
        }

        // Edge facts: every edge with BOTH endpoints inside the
        // neighborhood, directed as stored.
        for relationship in &relationship_rows {
            if neighborhood.contains(&relationship.from) && neighborhood.contains(&relationship.to)
            {
                edges.push(Edge(relationship.from, relationship.to, relationship.kind));
            }
        }

        // CR11 (audit): the attribute join is bounded on every axis — the
        // old harvest scanned EVERY memory with no posting-list filter, so
        // one common tag materialized ~N² pairs and OOMed before the first
        // write. Bounds:
        //   1. membership = the k-hop neighborhood PLUS a bounded
        //      expansion (memories sharing at least one attribute with a
        //      neighborhood member — R7/R9 exist precisely to bridge
        //      unconnected memories, so a strict-neighborhood filter
        //      would blind them);
        //   2. high-frequency attributes are dropped (posting lists above
        //      the cap carry no affinity signal);
        //   3. a hard cap on derived pairs per pass, with a drop counter.
        const MAX_POSTING_LIST: usize = 256;
        const MAX_DERIVED_PAIRS: usize = 10_000;
        let neighborhood_ids: Vec<_> = neighborhood.iter().copied().collect();
        let mut memory_rows = match self.storage.get_memories(&neighborhood_ids).await {
            Ok(rows) => rows,
            Err(error) => return Err(error),
        };
        let attribute_tags: std::collections::HashSet<_> = memory_rows
            .iter()
            .flat_map(|memory| memory.tags.iter().cloned())
            .collect();
        let attribute_entities: std::collections::HashSet<_> = memory_rows
            .iter()
            .flat_map(|memory| memory.context.entities.iter().copied())
            .collect();
        if !attribute_tags.is_empty() || !attribute_entities.is_empty() {
            let mut expansion = match self
                .storage
                .memories_sharing_attributes(
                    &attribute_tags.into_iter().collect::<Vec<_>>(),
                    &attribute_entities.into_iter().collect::<Vec<_>>(),
                    4096,
                )
                .await
            {
                Ok(rows) => rows,
                Err(error) => return Err(error),
            };
            let mut seen: std::collections::HashSet<_> =
                memory_rows.iter().map(|memory| memory.id).collect();
            expansion.retain(|memory| seen.insert(memory.id));
            memory_rows.extend(expansion);
        }
        let mut memories: Vec<rules::MemoryFact> = Vec::new();
        let mut raw_tags: Vec<(MemoryId, u32)> = Vec::new();
        let mut raw_entities: Vec<(MemoryId, exocortex_kernel::EntityId)> = Vec::new();
        for memory in &memory_rows {
            memories.push(rules::MemoryFact(memory.id, memory.memory_type));
            for tag in &memory.tags {
                raw_tags.push((memory.id, fxhash_tag(tag.as_str())));
            }
            for entity in &memory.context.entities {
                raw_entities.push((memory.id, *entity));
            }
        }

        // Posting-list filter: count per attribute, drop the frequent ones.
        {
            use std::collections::HashMap;
            let mut tag_counts: HashMap<u32, usize> = HashMap::new();
            for (_, t) in &raw_tags {
                *tag_counts.entry(*t).or_insert(0) += 1;
            }
            let dropped: usize = tag_counts
                .values()
                .filter(|c| **c > MAX_POSTING_LIST)
                .count();
            if dropped > 0 {
                metrics::counter!("exocortex_reasoning_high_frequency_attributes_dropped_total")
                    .increment(dropped as u64);
            }
            tags = raw_tags
                .into_iter()
                .filter(|(_, t)| tag_counts.get(t).is_some_and(|c| *c <= MAX_POSTING_LIST))
                .map(|(m, t)| TagFact(m, t))
                .collect();

            let mut ent_counts: HashMap<exocortex_kernel::EntityId, usize> = HashMap::new();
            for (_, e) in &raw_entities {
                *ent_counts.entry(*e).or_insert(0) += 1;
            }
            let dropped: usize = ent_counts
                .values()
                .filter(|c| **c > MAX_POSTING_LIST)
                .count();
            if dropped > 0 {
                metrics::counter!("exocortex_reasoning_high_frequency_attributes_dropped_total")
                    .increment(dropped as u64);
            }
            entities = raw_entities
                .into_iter()
                .filter(|(_, e)| ent_counts.get(e).is_some_and(|c| *c <= MAX_POSTING_LIST))
                .map(|(m, e)| EntityFact(m, e))
                .collect();
        }

        let mut derived = rules::evaluate(edges, entities, tags, memories);
        let before = derived.co_occurrence_affinity.len() + derived.similar_tags_affinity.len();
        if before > MAX_DERIVED_PAIRS {
            let scale = MAX_DERIVED_PAIRS as f64 / before as f64;
            let keep = |v: &mut Vec<(MemoryId, MemoryId)>| {
                let n = ((v.len() as f64) * scale).ceil() as usize;
                v.truncate(n.min(v.len()));
            };
            keep(&mut derived.co_occurrence_affinity);
            keep(&mut derived.similar_tags_affinity);
            metrics::counter!("exocortex_reasoning_derived_pairs_capped_total")
                .increment((before - MAX_DERIVED_PAIRS) as u64);
        }
        self.write_back(derived, &memory_rows, &relationship_rows)
            .await
    }

    async fn process_session_wrapup(&self, ms: &[MemoryId]) -> Result<(), StorageError> {
        for m in ms {
            self.try_k_hop_reason(*m, 3).await?;
        }
        Ok(())
    }

    /// Write derived relationships not already present (idempotent by
    /// deterministic `RelationshipId::derive`). CR12 (audit): each row's
    /// provenance evidence is the SUPPORTING EDGE SET for that derivation
    /// (the two hops behind a transitive edge), not the whole k-hop
    /// neighborhood; attribute derivations carry no edge evidence.
    async fn write_back(
        &self,
        mut derived: rules::Derived,
        memory_rows: &[Memory],
        relationship_rows: &[Relationship],
    ) -> Result<(), StorageError> {
        let ontology = self.storage_ontology();
        let mut new_rels: Vec<Relationship> = Vec::new();
        let now = chrono::Utc::now();
        let mut memory_visibility = std::collections::HashMap::new();
        for memory in memory_rows {
            memory_visibility.insert(memory.id, memory.visibility);
        }

        // Kind-aware adjacency for resolving transitive support edges.
        let mut adj: std::collections::HashMap<
            MemoryId,
            Vec<(MemoryId, exocortex_kernel::RelKindId, RelationshipId)>,
        > = std::collections::HashMap::new();
        let mut evidence_visibility = std::collections::HashMap::new();
        for relationship in relationship_rows {
            evidence_visibility.insert(relationship.id, relationship.visibility);
            adj.entry(relationship.from).or_default().push((
                relationship.to,
                relationship.kind,
                relationship.id,
            ));
        }
        // The two hops behind (from -> mid -> to) for the given kinds.
        let support = |from: MemoryId,
                       to: MemoryId,
                       k1: exocortex_kernel::RelKindId,
                       k2: exocortex_kernel::RelKindId|
         -> Vec<RelationshipId> {
            let empty = Vec::new();
            let outs = adj.get(&from).unwrap_or(&empty);
            for (mid, ek1, id1) in outs {
                if *ek1 != k1 {
                    continue;
                }
                if let Some(mids) = adj.get(mid) {
                    for (t, ek2, id2) in mids {
                        if *t == to && *ek2 == k2 {
                            return vec![*id1, *id2];
                        }
                    }
                }
            }
            vec![]
        };
        let kind_of = |name: &str| ontology.kind_id(name).expect("kind");

        let mut push = |from: MemoryId,
                        to: MemoryId,
                        rule_id: &str,
                        strength: f32,
                        shared_count: u32,
                        evidence: Vec<RelationshipId>| {
            let Some(from_visibility) = memory_visibility.get(&from).copied() else {
                return;
            };
            let Some(to_visibility) = memory_visibility.get(&to).copied() else {
                return;
            };
            let visibility = exocortex_kernel::narrowest_visibility(
                [from_visibility, to_visibility].into_iter().chain(
                    evidence
                        .iter()
                        .filter_map(|id| evidence_visibility.get(id).copied()),
                ),
            )
            .expect("two endpoint visibilities");
            let kind = derived_kind(&ontology, rule_id);
            // CR6 (audit): the rule id is part of the derived identity —
            // R7/R8/R9 all map to RelatedTo, and sharing one id let the
            // last rule in the batch silently overwrite the earlier
            // rules' provenance and confidence.
            let id = RelationshipId::derive(from, kind, to, Some(rule_id));
            new_rels.push(Relationship {
                id,
                kind,
                from,
                to,
                visibility,
                provenance: Provenance::Derived {
                    rule_id: rule_id.into(),
                    evidence,
                },
                properties: exocortex_kernel::RelationshipProperties {
                    strength,
                    // CR7 (audit): §14.2 confidence from the PER-PAIR
                    // shared count (R7/R9) — the old call fed the k-hop
                    // neighborhood edge count, which has no relationship
                    // to the formula.
                    confidence: derived_confidence(rule_id, shared_count),
                    context: None,
                    evidence_count: 1,
                    success_rate: None,
                    validation_count: 0,
                    counter_evidence_count: 0,
                    last_validated: now,
                },
                description: None,
                bidirectional: false,
                valid_from: now,
                valid_until: None,
                recorded_at: now,
                invalidated_by: None,
                lsn: exocortex_kernel::LSN::new_local(0),
            });
        };

        // Two-hop transitive rules: evidence = the two hops (CR12).
        let dep = kind_of("DependsOn");
        let req = kind_of("Requires");
        let builds = kind_of("BuildsOn");
        let blocks = kind_of("Blocks");
        let contradicts = kind_of("Contradicts");
        let confirms = kind_of("Confirms");
        for (a, c) in derived.transitive_depends_on {
            let ev = support(a, c, dep, dep);
            push(a, c, "R4", 0.5, 0, ev);
        }
        for (a, c) in derived.transitive_requires {
            let ev = support(a, c, req, req);
            push(a, c, "R5", 0.5, 0, ev);
        }
        // Attribute/bridge derivations carry no edge evidence (CR12): the
        // support is attribute provenance, which §7.9 keeps out of
        // evidence for non-edge derivations.
        let r7_counts = rules::pair_counts(std::mem::take(&mut derived.co_occurrence_affinity));
        for (a, b, shared) in r7_counts {
            push(a, b, "R7", 0.3, shared, vec![]);
        }
        for (a, b) in derived.problem_solution_bridge {
            push(a, b, "R8", 0.3, 0, vec![]);
        }
        let r9_counts = rules::pair_counts(std::mem::take(&mut derived.similar_tags_affinity));
        for (a, b, shared) in r9_counts {
            push(a, b, "R9", 0.3, shared, vec![]);
        }
        for (a, b) in derived.implied_solves {
            push(a, b, "D1", 0.8, 0, vec![]);
        }
        for (a, c) in derived.transitive_builds_on {
            let ev = support(a, c, builds, builds);
            push(a, c, "D2", 0.5, 0, ev);
        }
        for (a, c) in derived.indirect_blocker {
            let ev = support(a, c, blocks, req);
            push(a, c, "D3", 0.5, 0, ev);
        }
        // CR8 (audit): D4 (contradiction propagation) was evaluated on
        // every fixpoint and discarded — no writeback, no reader.
        for (a, c) in derived.contradiction_propagates {
            let ev = support(a, c, contradicts, confirms);
            push(a, c, "D4", 0.5, 0, ev);
        }
        // KP1 (audit): D5 (shared file-lineage target) now ships — the
        // pack declared it and the engine never did.
        for (a, b) in derived.shared_target {
            push(a, b, "D5", 0.4, 0, vec![]);
        }
        // CR9 (audit): D6's rule head re-emits its own input pair, whose
        // derived id is byte-identical to the edge the rule consumed — the
        // writeback could never fire. The cohort pairs (m1 <-> m2, same
        // session) are reconstructed here and written as RelatedTo.
        {
            let mut by_session: std::collections::HashMap<MemoryId, Vec<MemoryId>> =
                std::collections::HashMap::new();
            for (m, sess) in &derived.session_cohort {
                by_session.entry(*sess).or_default().push(*m);
            }
            let mut members: Vec<&MemoryId> = by_session.keys().collect();
            members.sort();
            for sess in members {
                let group = &by_session[sess];
                for (i, m1) in group.iter().enumerate() {
                    for m2 in group.iter().skip(i + 1) {
                        if m1 != m2 {
                            push(*m1, *m2, "D6", 0.6, 0, vec![]);
                        }
                    }
                }
            }
        }

        if new_rels.is_empty() {
            return Ok(());
        }
        // Idempotency uses indexed point reads over only the bounded derived
        // candidates, never a graph-wide relationship enumeration.
        let mut fresh = Vec::new();
        for relationship in new_rels {
            match self.storage.get_relationship(&relationship.id).await? {
                Some(_) => {}
                None => fresh.push(relationship),
            }
        }
        if !fresh.is_empty() {
            self.storage.upsert_batch(&[], &fresh).await?;
            metrics::counter!("exocortex_rules_executed_total", "engine" => "crepe")
                .increment(fresh.len() as u64);
        }
        Ok(())
    }

    /// Last derived type for a memory (R1/R2/R3) — the "re-derived as
    /// Solution within the same commit" surface (§3 M4).
    pub async fn inferred_type(&self, id: MemoryId) -> Option<u8> {
        // Evaluate R1-R3 over the neighborhood of `id`.
        let mut edges = Vec::new();
        let rels = self
            .storage
            .relationships_touching(&[id], 4096)
            .await
            .ok()?;
        for r in rels {
            if r.from == id || r.to == id {
                edges.push(Edge(r.from, r.to, r.kind));
            }
        }
        let derived = rules::evaluate(edges, vec![], vec![], vec![]);
        derived
            .type_from_solves
            .iter()
            .chain(derived.type_from_fixes.iter())
            .chain(derived.type_from_causes.iter())
            .find(|(m, _)| *m == id)
            .map(|(_, t)| *t)
    }

    fn storage_ontology(&self) -> exocortex_kernel::Ontology {
        storage_ontology(&self.storage)
    }
}

fn storage_ontology<S: Storage>(_: &Arc<S>) -> exocortex_kernel::Ontology {
    exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
        .expect("linked pack assembles")
}

/// Which kind a derived edge uses, by rule (RelatedTo for affinities, the
/// named kind for subsumption/transitivity rules).
fn derived_kind(onto: &exocortex_kernel::Ontology, rule_id: &str) -> exocortex_kernel::RelKindId {
    match rule_id {
        "R4" => onto.kind_id("DependsOn").expect("kind"),
        "R5" => onto.kind_id("Requires").expect("kind"),
        "D1" => exocortex_kernel::kinds::SOLVES,
        "D2" => onto.kind_id("BuildsOn").expect("kind"),
        "D3" => onto.kind_id("Blocks").expect("kind"),
        // CR8: contradiction propagation writes a Contradicts edge.
        "D4" => onto.kind_id("Contradicts").expect("kind"),
        // CR9: cohort pairs are memory<->memory affinity edges (RelatedTo),
        // not another InSession edge to the session node.
        "D6" => onto.kind_id("RelatedTo").expect("kind"),
        // R7/R8/R9: affinity edges are RelatedTo (R-T14 keeps SimilarTo
        // Computed-only, so affinity rides RelatedTo with Derived provenance).
        _ => onto.kind_id("RelatedTo").expect("kind"),
    }
}

/// Derived-edge confidence per §14.2. CR7: `n` is the PER-PAIR shared
/// count for R7/R9 (not the neighborhood edge count); R4/R5 are depth-2
/// transitives, so `1.0 / depth` = 0.5.
fn derived_confidence(rule_id: &str, n: u32) -> f32 {
    match rule_id {
        "R4" | "R5" => 0.5, // 1.0 / depth, depth 2
        "R7" | "R9" => (n as f32 / 5.0).min(1.0),
        "R8" | "D1" => 0.8,
        _ => 0.5,
    }
}

/// Stable tag hash for TagFact keys (deterministic; not a security surface).
fn fxhash_tag(tag: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in tag.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
