// crates/exocortex-reasoning/src/engine.rs
//! The two-language runtime (§10.6): asynchronous reasoning over a queue,
//! Crepe fixpoints on k-hop bounded fact scopes, derived-edge writeback with
//! `Provenance::Derived`, and R6 (`reverse_solves`) as a Steel program.

use std::sync::Arc;

use exocortex_kernel::{MemoryId, Provenance, Relationship, RelationshipId, Visibility};
use exocortex_storage::Storage;
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
            let w = { self.rx_work.lock().await.recv().await };
            let Some(w) = w else { break };
            match w {
                ReasoningWork::KHopOver { seed, k } => self.k_hop_reason(seed, k).await,
                ReasoningWork::SessionWrapup { memories } => self.session_reason(&memories).await,
            }
        }
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
        let k = k.clamp(1, self.k_hop.max(1));
        let mut edges: Vec<Edge> = Vec::new();
        let entities: Vec<EntityFact>;
        let tags: Vec<TagFact>;
        let mut evidence: Vec<RelationshipId> = Vec::new();

        // Single scan: build undirected adjacency for the BFS + the edge
        // rows (directed) for the rule program.
        let mut adjacency: std::collections::HashMap<MemoryId, Vec<(MemoryId, RelationshipId)>> =
            std::collections::HashMap::new();
        {
            use futures::StreamExt;
            let mut rels = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rels.next().await {
                adjacency.entry(r.from).or_default().push((r.to, r.id));
                adjacency.entry(r.to).or_default().push((r.from, r.id));
            }
        }

        // Bounded BFS from the seed (CR-6 hard caps).
        const MAX_NODES: usize = 512;
        let mut neighborhood: std::collections::HashSet<MemoryId> =
            std::collections::HashSet::from([seed]);
        let mut seen_edges: std::collections::HashSet<RelationshipId> =
            std::collections::HashSet::new();
        let mut frontier = vec![seed];
        for _hop in 0..k {
            let mut next = Vec::new();
            for node in &frontier {
                if let Some(neighbors) = adjacency.get(node) {
                    for (other, edge_id) in neighbors {
                        if neighborhood.insert(*other) && neighborhood.len() <= MAX_NODES {
                            next.push(*other);
                        }
                        if seen_edges.insert(*edge_id) {
                            evidence.push(*edge_id);
                        }
                    }
                }
            }
            if next.is_empty() || neighborhood.len() > MAX_NODES {
                break;
            }
            frontier = next;
        }

        // Edge facts: every edge with BOTH endpoints inside the
        // neighborhood, directed as stored.
        {
            use futures::StreamExt;
            let mut rels = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rels.next().await {
                if neighborhood.contains(&r.from) && neighborhood.contains(&r.to) {
                    edges.push(Edge(r.from, r.to, r.kind));
                }
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
        const MAX_ATTRIBUTE_MEMORIES: usize = 4096;
        const MAX_POSTING_LIST: usize = 256;
        const MAX_DERIVED_PAIRS: usize = 10_000;

        // Pass 1: the neighborhood's attribute sets (bounded by MAX_NODES).
        let mut nb_tag_set: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut nb_ent_set: std::collections::HashSet<exocortex_kernel::EntityId> =
            std::collections::HashSet::new();
        {
            use futures::StreamExt;
            let mut mems = self.storage.stream_all_memories().await;
            while let Some(Ok(m)) = mems.next().await {
                if !neighborhood.contains(&m.id) {
                    continue;
                }
                for t in &m.tags {
                    nb_tag_set.insert(fxhash_tag(t.as_str()));
                }
                for e in &m.context.entities {
                    nb_ent_set.insert(*e);
                }
            }
        }

        // Pass 2: harvest members (neighborhood + attribute-sharing
        // expansion), capped.
        let mut memories: Vec<rules::MemoryFact> = Vec::new();
        let mut raw_tags: Vec<(MemoryId, u32)> = Vec::new();
        let mut raw_entities: Vec<(MemoryId, exocortex_kernel::EntityId)> = Vec::new();
        {
            use futures::StreamExt;
            let mut mems = self.storage.stream_all_memories().await;
            while let Some(Ok(m)) = mems.next().await {
                if memories.len() >= MAX_ATTRIBUTE_MEMORIES {
                    metrics::counter!("exocortex_reasoning_attribute_harvest_capped_total")
                        .increment(1);
                    break;
                }
                let member = neighborhood.contains(&m.id) || {
                    m.tags
                        .iter()
                        .any(|t| nb_tag_set.contains(&fxhash_tag(t.as_str())))
                        || m.context.entities.iter().any(|e| nb_ent_set.contains(e))
                };
                if !member {
                    continue;
                }
                memories.push(rules::MemoryFact(m.id, m.memory_type));
                for t in &m.tags {
                    raw_tags.push((m.id, fxhash_tag(t.as_str())));
                }
                for e in &m.context.entities {
                    raw_entities.push((m.id, *e));
                }
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
        // CR12 (audit): evidence is per-rule (write_back resolves the
        // supporting edges for each derivation); the k-hop neighborhood
        // list is no longer stamped wholesale on every row.
        let _ = &evidence;
        self.write_back(derived).await;
    }

    async fn session_reason(&self, ms: &[MemoryId]) {
        for m in ms {
            self.k_hop_reason(*m, 3).await;
        }
    }

    /// Write derived relationships not already present (idempotent by
    /// deterministic `RelationshipId::derive`). CR12 (audit): each row's
    /// provenance evidence is the SUPPORTING EDGE SET for that derivation
    /// (the two hops behind a transitive edge), not the whole k-hop
    /// neighborhood; attribute derivations carry no edge evidence.
    async fn write_back(&self, mut derived: rules::Derived) {
        let ontology = self.storage_ontology();
        let mut new_rels: Vec<Relationship> = Vec::new();
        let now = chrono::Utc::now();

        // Kind-aware adjacency for resolving transitive support edges.
        let mut adj: std::collections::HashMap<
            MemoryId,
            Vec<(MemoryId, exocortex_kernel::RelKindId, RelationshipId)>,
        > = std::collections::HashMap::new();
        {
            use futures::StreamExt;
            let mut rels = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rels.next().await {
                adj.entry(r.from).or_default().push((r.to, r.kind, r.id));
            }
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
                visibility: Visibility::Org,
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
            return;
        }
        // Idempotency: skip rows whose derived id already exists.
        use futures::StreamExt;
        let existing: std::collections::HashSet<RelationshipId> = {
            let mut set = std::collections::HashSet::new();
            let mut rs = self.storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rs.next().await {
                set.insert(r.id);
            }
            set
        };
        let fresh: Vec<Relationship> = new_rels
            .into_iter()
            .filter(|r| !existing.contains(&r.id))
            .collect();
        if !fresh.is_empty() {
            if let Err(e) = self.storage.upsert_batch(&[], &fresh).await {
                warn!(?e, "derived-edge writeback failed");
            } else {
                metrics::counter!("exocortex_rules_executed_total", "engine" => "crepe")
                    .increment(fresh.len() as u64);
            }
        }
    }

    /// Last derived type for a memory (R1/R2/R3) — the "re-derived as
    /// Solution within the same commit" surface (§3 M4).
    pub async fn inferred_type(&self, id: MemoryId) -> Option<u8> {
        // Evaluate R1-R3 over the neighborhood of `id`.
        let mut edges = Vec::new();
        use futures::StreamExt;
        let mut rels = self.storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rels.next().await {
            if r.from == id || r.to == id {
                edges.push(Edge(r.from, r.to, r.kind));
            }
        }
        drop(rels);
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
