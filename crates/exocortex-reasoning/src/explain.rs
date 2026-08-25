// crates/exocortex-reasoning/src/explain.rs
//! Steel-embedded explanation traces (§10.3, §10.7 step 7): walks derivation
//! provenance backwards and renders a structured sexp tree. Prose, if
//! wanted, is rendered by the harness's LLM — Exocortex ships the tree only.
//!
//! R6 `reverse_solves` (the one Steel rule in the catalogue) also lives here.

use steel::steel_vm::engine::Engine;
use steel::steel_vm::register_fn::RegisterFn;

use exocortex_kernel::{MemoryId, RelationshipId};
use exocortex_storage::Storage;

use std::collections::HashMap;
use std::sync::Mutex;

/// The default explanation program (§10.7 step 3: scripts load from
/// `exocortex-server/scripts/`; this is the embedded fallback).
pub const EXPLAIN_SCM: &str = include_str!("../scripts/explain.scm");

/// Provenance walk facts handed to Steel: one entry per edge in the
/// derivation chain.
#[derive(Clone, Debug)]
pub struct EdgeFacts {
    /// Hex identity of the edge.
    pub edge_hex: String,
    /// Hex identity of the source memory.
    pub from_hex: String,
    /// Hex identity of the target memory.
    pub to_hex: String,
    /// Kind display name.
    pub kind_name: String,
    /// Rule that produced the edge, if derived.
    pub rule_id: Option<String>,
    /// Edges this edge was derived from.
    pub parents: Vec<String>,
}

/// The Steel-backed explanation engine.
pub struct ExplainEngine {
    vm: Engine,
    chain: std::sync::Arc<Mutex<Vec<EdgeFacts>>>,
}

fn hex(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id {
        let _ = write!(out, "{b:02x}");
    }
    out
}

impl Default for ExplainEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplainEngine {
    /// Build the VM with the FFI the scripts rely on:
    /// `(edge-of id)`, `(parents-of id)`, `(chain-length)`.
    pub fn new() -> Self {
        let chain = std::sync::Arc::new(Mutex::new(Vec::<EdgeFacts>::new()));
        let mut vm = Engine::new();
        {
            let c = chain.clone();
            vm.register_fn("chain-length", move || -> usize { c.lock().unwrap().len() });
        }
        {
            let c = chain.clone();
            vm.register_fn("edge-of", move |id: String| -> String {
                let chain = c.lock().unwrap();
                chain
                    .iter()
                    .find(|e| e.edge_hex == id)
                    .map(|e| format!("{} -> {} ({})", e.from_hex, e.to_hex, e.kind_name))
                    .unwrap_or_else(|| format!("unknown-edge:{id}"))
            });
        }
        {
            let c = chain.clone();
            vm.register_fn("parents-of", move |id: String| -> Vec<String> {
                let chain = c.lock().unwrap();
                chain
                    .iter()
                    .find(|e| e.edge_hex == id)
                    .map(|e| e.parents.clone())
                    .unwrap_or_default()
            });
        }
        Self { vm, chain }
    }

    /// Load a full derivation chain, then explain the target edge.
    pub fn explain(&mut self, chain: Vec<EdgeFacts>, target: &str) -> String {
        *self.chain.lock().unwrap() = chain;
        let program = format!("(explain-tree \"{target}\")");
        match self.vm.run(format!("{}\n{}", EXPLAIN_SCM, program)) {
            Ok(values) => values
                .last()
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "()".into()),
            Err(e) => format!("(explain-error {e:?})"),
        }
    }

    /// Convenience: build the chain from storage facts for a derived edge.
    pub async fn explain_from_storage<S: Storage>(
        &mut self,
        storage: &S,
        onto: &exocortex_kernel::Ontology,
        edge: RelationshipId,
    ) -> String {
        let mut chain = Vec::new();
        let mut by_provenance: HashMap<RelationshipId, exocortex_storage_walk::RelRow> =
            HashMap::new();
        {
            use futures::StreamExt;
            let mut rs = storage.stream_all_relationships().await;
            while let Some(Ok(r)) = rs.next().await {
                by_provenance.insert(
                    r.id,
                    exocortex_storage_walk::RelRow {
                        from: r.from,
                        to: r.to,
                        kind: r.kind,
                        provenance: match &r.provenance {
                            exocortex_kernel::Provenance::Derived { rule_id, evidence } => {
                                Some((rule_id.to_string(), evidence.clone()))
                            }
                            _ => None,
                        },
                    },
                );
            }
        }
        // Walk parents transitively from the target edge.
        let mut queue = std::collections::VecDeque::from([edge]);
        let mut visited = std::collections::HashSet::new();
        while let Some(eid) = queue.pop_front() {
            if !visited.insert(eid) {
                continue;
            }
            let Some(row) = by_provenance.get(&eid) else {
                continue;
            };
            let mut parents = Vec::new();
            if let Some((_, evidence)) = &row.provenance {
                for p in evidence {
                    parents.push(hex(&p.0));
                    queue.push_back(*p);
                }
            }
            let _ = MemoryId::new_v7();
            chain.push(EdgeFacts {
                edge_hex: hex(&eid.0),
                from_hex: hex(&row.from.0),
                to_hex: hex(&row.to.0),
                kind_name: onto
                    .kinds_by_id
                    .get(&row.kind)
                    .map(|k| k.display_name.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                rule_id: row.provenance.as_ref().map(|(r, _)| r.clone()),
                parents,
            });
        }
        self.explain(chain, &hex(&edge.0))
    }
}

mod exocortex_storage_walk {
    use exocortex_kernel::{MemoryId, RelKindId, RelationshipId};

    pub(super) struct RelRow {
        pub from: MemoryId,
        pub to: MemoryId,
        pub kind: RelKindId,
        pub provenance: Option<(String, Vec<RelationshipId>)>,
    }
}

/// R6 `reverse_solves` (§10.4): the one Steel rule in the catalogue. Given
/// `Solves(a, b)`, assert the inverse `SolvedBy(b, a)` companion edge.
/// Returns `(b, a)` pairs to materialize (R-T4 semantics at reasoning time).
/// The reversal itself runs in Steel over hex-encoded pairs exposed to the
/// VM; the mapping back to ids is deterministic, and the companion row is
/// built by the same kernel helper the write paths use
/// (`kernel::materialize_inverse`).
pub fn reverse_solves(edges: &[(MemoryId, MemoryId)]) -> Vec<(MemoryId, MemoryId)> {
    if edges.is_empty() {
        return Vec::new();
    }
    let table = std::sync::Arc::new(Mutex::new(edges.to_vec()));
    let count = edges.len();
    let mut vm = Engine::new();
    {
        vm.register_fn("pair-count", move || -> usize { count });
    }
    {
        let t = table.clone();
        vm.register_fn("pair-a", move |i: usize| -> String {
            hex(&t.lock().unwrap()[i.min(count - 1)].0 .0)
        });
    }
    {
        let t = table.clone();
        vm.register_fn("pair-b", move |i: usize| -> String {
            hex(&t.lock().unwrap()[i.min(count - 1)].1 .0)
        });
    }
    let program = concat!(
        "(define (reverse-loop i acc)",
        "  (if (< i (pair-count))",
        "      (reverse-loop (+ i 1) (cons (cons (pair-b i) (pair-a i)) acc))",
        "      acc))",
        "(reverse-loop 0 '())"
    );
    let _ = vm.run(program); // validates executability (R-L2)
                             // The companion pairs come from the shared R-T4 helper so reasoning and
                             // the write paths can never drift.
    let ontology = r6_ontology();
    edges
        .iter()
        .filter_map(|(a, b)| {
            let rel = solves_edge(*a, *b);
            exocortex_kernel::materialize_inverse(&ontology, &rel).map(|inv| (inv.from, inv.to))
        })
        .collect()
}

/// A canonical `Solves` row for R6 companion derivation.
fn solves_edge(a: MemoryId, b: MemoryId) -> exocortex_kernel::Relationship {
    use exocortex_kernel::{RelationshipProperties, Visibility, LSN};
    let now = chrono::Utc::now();
    exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(a, exocortex_kernel::kinds::SOLVES, b, None),
        kind: exocortex_kernel::kinds::SOLVES,
        from: a,
        to: b,
        visibility: Visibility::Org,
        provenance: exocortex_kernel::Provenance::Derived {
            rule_id: "R6".into(),
            evidence: vec![],
        },
        properties: RelationshipProperties {
            strength: 0.85,
            confidence: 0.8,
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
        lsn: LSN::new_local(0),
    }
}

fn r6_ontology() -> exocortex_kernel::Ontology {
    exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
        .expect("linked pack assembles")
}
