//! OC-PRD S2 (docs/prd/ontology-compatibility-prd.md §6): additive
//! change is a non-event, release metadata cannot gate data, and each
//! boundary's rule comes from the kernel policy table — never a raw
//! comparison. Runs against two real ontologies assembled from the
//! same pack shape, one grown additively.

use exocortex_kernel::{
    admit_backup, admit_node_graph, admit_peer, admit_producer_batch, BackupOntology,
    NodeGraphDecision, Ontology, OntologySummary, PackVersion, PersistedPin, PinnedOntology,
};

exocortex_kernel::pack! {
    name: "compat-rolling",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! { Problem, Solution, Fix, Error, }

    entity_types! { File, Concept, }

    kinds! {
        Solves    => bucket: Solution, inverse: SolvedBy,  bi: false, default_strength: 0.85, kernel_const: SOLVES,
        Fixes     => bucket: Causal,   inverse: FixedBy,   bi: false, default_strength: 0.90, kernel_const: FIXES,
        Causes    => bucket: Causal,   inverse: CausedBy,  bi: false, default_strength: 0.85, kernel_const: CAUSES,
        InSession => bucket: Context,  inverse: HasMember, bi: false, default_strength: 0.80, kernel_const: IN_SESSION,
    }

    type_triples! {
        Solves    => (Solution | Fix, Problem | Error),
        Fixes     => (Fix, Error | Problem),
        Causes    => (_, Error | Problem),
        InSession => (_, _),
    }

    crepe_rules! {
        rolling_rule(a, b) <- edge(a, b, Solves), memory(a, _, _);
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn base_ontology() -> Ontology {
    Ontology::from_packs(vec![pack_def()]).expect("base ontology loads")
}

fn grown_ontology() -> Ontology {
    let mut grown = pack_def();
    grown.memory_type_names.push("FutureThing".into());
    grown.entity_type_names.push("FutureActor".into());
    Ontology::from_packs(vec![grown]).expect("grown ontology loads")
}

fn bumped_ontology() -> Ontology {
    let mut bumped = pack_def();
    bumped.version = PackVersion {
        major: 2,
        minor: 0,
        patch: 1,
    };
    bumped.kernel_min = PackVersion {
        major: 1,
        minor: 4,
        patch: 0,
    };
    Ontology::from_packs(vec![bumped]).expect("bumped ontology loads")
}

#[test]
fn fixture_enums_line_up_with_ontology_ids() {
    let onto = base_ontology();
    assert_eq!(
        onto.memory_type_id("Solution"),
        Some(MemoryType::Solution.id())
    );
    assert_eq!(
        onto.entity_type_by_name.get("Concept").copied(),
        Some(EntityType::Concept.id())
    );
}

/// S2 proper: appending a memory type leaves every pre-existing
/// name-to-id mapping intact and produces a superset verdict against
/// the prior ontology.
#[test]
fn appending_a_memory_type_is_a_non_event() {
    let base = base_ontology();
    let grown = grown_ontology();
    for (id, name) in base.memory_type_names.iter().enumerate() {
        assert_eq!(grown.memory_type_names.get(id), Some(name));
    }
    assert!(base.summary.is_subset_of(&grown.summary));
    assert!(base.summary.is_subset_of(&base.summary.clone()));
    // The fingerprints differ (growth is visible) but the verdict is
    // superset, not mismatch. The advance retains the prior
    // fingerprint in the recognized history (producer window).
    assert_ne!(base.fingerprint, grown.fingerprint);
    match admit_node_graph(
        PersistedPin::V2(Box::new(PinnedOntology::describing(&base))),
        &grown,
    )
    .expect("superset node boots")
    {
        NodeGraphDecision::Advance(next) => {
            let hex_prior = {
                use std::fmt::Write as _;
                let mut out = String::with_capacity(64);
                for b in base.fingerprint.0 {
                    let _ = write!(out, "{b:02x}");
                }
                out
            };
            assert_eq!(next.compatibility, {
                use std::fmt::Write as _;
                let mut out = String::with_capacity(64);
                for b in grown.fingerprint.0 {
                    let _ = write!(out, "{b:02x}");
                }
                out
            });
            assert_eq!(next.accepted, vec![hex_prior]);
        }
        other => panic!("expected Advance, got {other:?}"),
    }
}

/// S1 at the kernel level: a release-metadata-only edit leaves the
/// compatibility fingerprint byte-identical and moves the build
/// fingerprint.
#[test]
fn release_metadata_cannot_gate_data() {
    let base = base_ontology();
    let bumped = bumped_ontology();
    assert_eq!(base.fingerprint, bumped.fingerprint);
    assert_eq!(base.summary, bumped.summary);
    assert_ne!(base.build_fingerprint, bumped.build_fingerprint);
    // A node pinned by the base boots under the bumped build; the
    // compatibility value is untouched (at most the build attribution
    // in the record refreshes).
    match admit_node_graph(
        PersistedPin::V2(Box::new(PinnedOntology::describing(&base))),
        &bumped,
    )
    .expect("metadata-only change boots")
    {
        NodeGraphDecision::Satisfied => {}
        NodeGraphDecision::Advance(next) => {
            assert_eq!(next.compatibility, hex(&base.fingerprint.0));
            assert_eq!(next.summary, base.summary);
        }
        other => panic!("unexpected verdict {other:?}"),
    }
}

/// The full D2 table over the two ontologies: superset node accepts
/// subset producer (through its recognized window), subset node
/// rejects superset producer, peers require exact equality, backups
/// restore forward.
#[test]
fn boundary_rules_come_from_the_policy_table() {
    let base = base_ontology();
    let grown = grown_ontology();

    // Ingest row: the grown server recognizes the base fingerprint
    // (its pin advanced), so a base-stamped batch is accepted.
    admit_producer_batch(&base.fingerprint.0, &grown, &[base.fingerprint.0])
        .expect("superset server accepts subset producer");
    // The base server has never heard of the grown fingerprint.
    let err = admit_producer_batch(&grown.fingerprint.0, &base, &[]).unwrap_err();
    assert!(err.to_string().contains("re-negotiate"), "{err}");

    // Cluster/SSE rows: exact equality, both directions.
    admit_peer(&base.fingerprint.0, &base.fingerprint.0).expect("same fp admitted");
    assert!(admit_peer(&grown.fingerprint.0, &base.fingerprint.0).is_err());
    assert!(admit_peer(&base.fingerprint.0, &grown.fingerprint.0).is_err());

    // Backup row: base backups restore into the grown binary; the
    // reverse does not.
    admit_backup(
        BackupOntology::Summarized {
            summary: &base.summary,
        },
        &grown,
    )
    .expect("forward restore");
    assert!(admit_backup(
        BackupOntology::Summarized {
            summary: &grown.summary,
        },
        &base,
    )
    .is_err());
}

/// A stored summary must hash to its recorded fingerprint; tampering
/// is corruption, not a mismatch (fail closed, R6-B17 semantics).
#[test]
fn tampered_pin_record_is_corruption() {
    let base = base_ontology();
    let mut record = PinnedOntology::describing(&base);
    record.summary.memory_types.push("Injected".into());
    let err = admit_node_graph(PersistedPin::V2(Box::new(record)), &base).unwrap_err();
    assert!(err.to_string().contains("corrupt"), "{err}");
}

/// The summary encoding is deterministic and order-independent at the
/// pack boundary: two independently assembled ontologies of the same
/// pack set agree.
#[test]
fn summaries_are_deterministic() {
    let a = base_ontology();
    let b = base_ontology();
    assert_eq!(a.summary, b.summary);
    let direct = OntologySummary::of_canonical_packs(&a.packs);
    assert_eq!(direct, a.summary);
    assert_eq!(
        direct.compatibility_fingerprint(),
        exocortex_kernel::OntologyFingerprint::compute(std::slice::from_ref(&pack_def()))
    );
}
