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
        let mut entities: Vec<EntityFact> = Vec::new();
        let mut tags: Vec<TagFact> = Vec::new();
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

        // Attribute facts (tags, entities) come from every memory: affinity
        // rules (R7/R9) compare attributes across memories, so scoping them
        // to the edge neighborhood would blind them. Edges stay k-hop
        // bounded; the memory scan is the enrichment-tier cost (§10.5).
        use futures::StreamExt;
        let mut mems = self.storage.stream_all_memories().await;
        while let Some(Ok(m)) = mems.next().await {
            for t in &m.tags {
                tags.push(TagFact(m.id, fxhash_tag(t.as_str())));
            }
            for e in &m.context.entities {
                entities.push(EntityFact(m.id, *e));
            }
        }
        drop(mems);

        let derived = rules::evaluate(edges, entities, tags);
        self.write_back(derived, &evidence).await;
    }

    async fn session_reason(&self, ms: &[MemoryId]) {
        for m in ms {
            self.k_hop_reason(*m, 3).await;
        }
    }

    /// Write derived relationships not already present (idempotent by
    /// deterministic `RelationshipId::derive`).
    async fn write_back(&self, derived: rules::Derived, evidence: &[RelationshipId]) {
        let ontology = self.storage_ontology();
        let mut new_rels: Vec<Relationship> = Vec::new();
        let now = chrono::Utc::now();

        let mut push = |from: MemoryId, to: MemoryId, rule_id: &str, strength: f32| {
            let kind = derived_kind(&ontology, rule_id);
            let id = RelationshipId::derive(from, kind, to, None);
            new_rels.push(Relationship {
                id,
                kind,
                from,
                to,
                visibility: Visibility::Org,
                provenance: Provenance::Derived {
                    rule_id: rule_id.into(),
                    evidence: evidence.to_vec(),
                },
                properties: exocortex_kernel::RelationshipProperties {
                    strength,
                    confidence: derived_confidence(rule_id, evidence.len()),
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

        for (a, b) in derived.transitive_depends_on {
            push(a, b, "R4", 0.5);
        }
        for (a, b) in derived.transitive_requires {
            push(a, b, "R5", 0.5);
        }
        for (a, b) in derived.co_occurrence_affinity {
            push(a, b, "R7", 0.3);
        }
        for (a, b) in derived.problem_solution_bridge {
            push(a, b, "R8", 0.3);
        }
        for (a, b) in derived.similar_tags_affinity {
            push(a, b, "R9", 0.3);
        }
        for (a, b) in derived.implied_solves {
            push(a, b, "D1", 0.8);
        }
        for (a, b) in derived.transitive_builds_on {
            push(a, b, "D2", 0.5);
        }
        for (a, b) in derived.indirect_blocker {
            push(a, b, "D3", 0.5);
        }
        for (a, b) in derived.session_cohort {
            push(a, b, "D6", 0.6);
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
        let derived = rules::evaluate(edges, vec![], vec![]);
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
        "D6" => exocortex_kernel::kinds::IN_SESSION,
        // R7/R8/R9: affinity edges are RelatedTo (R-T14 keeps SimilarTo
        // Computed-only, so affinity rides RelatedTo with Derived provenance).
        _ => onto.kind_id("RelatedTo").expect("kind"),
    }
}

/// Derived-edge confidence per §14.2.
fn derived_confidence(rule_id: &str, evidence: usize) -> f32 {
    match rule_id {
        "R4" | "R5" => 1.0 / 3.0, // depth 2 transitive
        "R7" => (evidence as f32 / 5.0).min(1.0),
        "R9" => (evidence as f32 / 5.0).min(1.0),
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
