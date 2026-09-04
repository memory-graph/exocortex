//! M1 acceptance: the dev-v1 pack loads through the real registration path
//! with 13/12/49 present (D8 appended the computed-only Summarizes kind),
//! kernel constants bound, and a deterministic fingerprint.

use exocortex_kernel::{kinds, pack::load_registered_packs, OntologyFingerprint};
use exocortex_pack_dev_v1::{pack_def, EntityType, MemoryType, KIND_TABLE};

#[test]
fn loads_correctly() {
    let onto = load_registered_packs().expect("dev-v1 pack loads");

    // 13 memory types, 12 entity types (§7.18). D8 deliberately adds NO
    // type: type ids run across packs with a shared offset.
    assert_eq!(onto.memory_type_names.len(), 13);
    assert_eq!(onto.entity_type_names.len(), 12);
    assert_eq!(MemoryType::ALL.len(), 13);
    assert_eq!(EntityType::ALL.len(), 12);

    // 49 authored kinds (D8 appended Summarizes; + auto-registered
    // inverse companions, R-T4).
    let authored = KIND_TABLE.iter().filter(|r| !r.companion).count();
    assert_eq!(
        authored, 49,
        "dev-v1 must register exactly 49 authored kinds"
    );
    assert!(onto.kinds_by_id.len() > 49, "inverse companions registered");

    // Bucket sizes: Solution 5, Causal 7, Context 9, Learning 6, Similarity 4,
    // Workflow 6, Quality 5, Integration 6.
    let bucket_counts = |b: exocortex_kernel::RelBucket| {
        KIND_TABLE
            .iter()
            .filter(|r| !r.companion && r.bucket == b)
            .count()
    };
    use exocortex_kernel::RelBucket::*;
    assert_eq!(bucket_counts(Solution), 5);
    assert_eq!(bucket_counts(Causal), 7);
    assert_eq!(bucket_counts(Context), 10); // D8: Summarizes joined Context
    assert_eq!(bucket_counts(Learning), 6);
    assert_eq!(bucket_counts(Similarity), 4);
    assert_eq!(bucket_counts(Workflow), 6);
    assert_eq!(bucket_counts(Quality), 5);
    assert_eq!(bucket_counts(Integration), 6);

    // All four kernel constants bound (R-Pk2).
    for k in [
        kinds::SOLVES,
        kinds::FIXES,
        kinds::CAUSES,
        kinds::IN_SESSION,
    ] {
        assert!(
            onto.kinds_by_id.contains_key(&k),
            "kernel constant {k:?} bound"
        );
    }
    assert_eq!(onto.kind_id("Solves"), Some(kinds::SOLVES));
    assert_eq!(onto.kind_id("Fixes"), Some(kinds::FIXES));
    assert_eq!(onto.kind_id("Causes"), Some(kinds::CAUSES));
    assert_eq!(onto.kind_id("InSession"), Some(kinds::IN_SESSION));

    // Every authored kind has a type triple entry (companions never do).
    for row in KIND_TABLE.iter().filter(|r| !r.companion) {
        let id = onto.kind_id(row.name).expect(row.name);
        assert!(
            onto.triples_by_kind.contains_key(&id),
            "kind {} must have at least one type triple",
            row.name
        );
    }

    // Pack rules D1-D6 harvested for fingerprinting.
    assert_eq!(onto.packs[0].rule_ids.len(), 6);

    // Fingerprint deterministic across two loads.
    let again = load_registered_packs().expect("second load");
    assert_eq!(onto.fingerprint, again.fingerprint);
    // And matches a fresh compute over the same def.
    let d = pack_def();
    assert_eq!(onto.fingerprint, OntologyFingerprint::compute(&[d]));
}

#[test]
fn inverse_materialization_pairs_are_symmetric() {
    let onto = load_registered_packs().unwrap();
    for k in onto.kinds_by_id.values() {
        if let Some(inv) = k.inverse {
            let partner = &onto.kinds_by_id[&inv];
            assert_eq!(
                partner.inverse,
                Some(k.id),
                "{} <-> {} must be symmetric (R-T4)",
                k.display_name,
                partner.display_name
            );
        }
    }
}

#[test]
fn golden_fingerprint_is_pinned() {
    let onto = load_registered_packs().unwrap();
    let mut compat = String::with_capacity(64);
    for b in onto.fingerprint.0 {
        use std::fmt::Write as _;
        let _ = write!(compat, "{b:02x}");
    }
    let mut build = String::with_capacity(64);
    for b in onto.build_fingerprint.0 {
        use std::fmt::Write as _;
        let _ = write!(build, "{b:02x}");
    }
    // OC-PRD 0f: the golden names both levels — line 1 gates
    // (compatibility), line 2 reports (build, the v1-scheme value).
    let mut lines = include_str!("dev_v1_fingerprint.txt").lines();
    let compat_golden = lines.next().unwrap().trim();
    let build_golden = lines.next().unwrap().trim();
    assert_eq!(
        compat, compat_golden,
        "ontology drift: regenerate the golden file deliberately"
    );
    assert_eq!(
        build, build_golden,
        "build fingerprint drift: the v1-scheme value must stay byte-stable"
    );
}

/// W6 (audit): the computed-only marker is a pack declaration consumed
/// through the ontology — SimilarTo and D8's Summarizes set it, nothing
/// else does.
#[test]
fn computed_only_marker_rides_the_ontology() {
    let onto = exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap();
    for name in ["SimilarTo", "Summarizes"] {
        let kind = onto
            .kinds_by_id
            .values()
            .find(|k| k.display_name == name)
            .expect("{name} registered");
        assert!(kind.computed_only, "{name} is computed-only (R-T14)");
    }
    let others: Vec<_> = onto
        .kinds_by_id
        .values()
        .filter(|k| {
            k.computed_only && k.display_name != "SimilarTo" && k.display_name != "Summarizes"
        })
        .map(|k| k.display_name.to_string())
        .collect();
    assert!(
        others.is_empty(),
        "no other dev-v1 kind is computed-only: {others:?}"
    );
}
