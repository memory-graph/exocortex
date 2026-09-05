//! LP1 acceptance (master plan): the third pack composes beside dev-v1
//! and ln-v1 into one effective ontology without renumbering a single
//! earlier id, keeps the kernel constants bound by dev-v1 alone,
//! enforces its own triple table, and marks its computed-only kind.

use exocortex_kernel::{pack::load_registered_packs, validator, OntologyFingerprint};
use exocortex_pack_dev_v1::pack_def as dev_pack;
use exocortex_pack_mortgage_v1::pack_def as mortgage_pack;
use exocortex_pack_study_v1::pack_def as learn_pack;

fn composed() -> exocortex_kernel::Ontology {
    load_registered_packs().expect("all three linked packs assemble")
}

#[test]
fn all_three_packs_register_into_one_ontology() {
    let onto = composed();
    assert_eq!(onto.packs.len(), 3, "dev-v1, study-v1, ln-v1 all linked");
    // Assert by membership, not position: the sort order is the
    // kernel's, and the no-renumbering property below is what matters.
    let names: std::collections::BTreeSet<String> =
        onto.packs.iter().map(|p| p.name.to_string()).collect();
    for expected in [
        "exocortex-pack-dev-v1",
        "exocortex-pack-study-v1",
        "exocortex-pack-mortgage-v1",
    ] {
        assert!(names.contains(expected), "{expected} registered");
    }
    // 13 + 7 + 7 memory types, 12 + 3 + 5 entity types, no renumbering.
    assert_eq!(onto.memory_type_names.len(), 27);
    assert_eq!(onto.entity_type_names.len(), 20);
    let before_learn =
        exocortex_kernel::Ontology::from_packs(vec![dev_pack(), mortgage_pack()]).unwrap();
    for (id, name) in before_learn.memory_type_names.iter().enumerate() {
        assert_eq!(
            onto.memory_type_names.get(id),
            Some(name),
            "pre-existing memory type id {id} must not renumber"
        );
    }
    for (id, name) in before_learn.entity_type_names.iter().enumerate() {
        assert_eq!(
            onto.entity_type_names.get(id),
            Some(name),
            "pre-existing entity type id {id} must not renumber"
        );
    }
    // Learn types sit at the offset after the prior composed set.
    assert_eq!(onto.memory_type_id("LearningGoal"), Some(20));
    assert_eq!(onto.memory_type_id("Topic"), Some(21));
    assert_eq!(onto.entity_type_by_name.get("Subject").copied(), Some(17));
}

#[test]
fn composed_fingerprint_differs_and_is_stable() {
    let onto = composed();
    let without_learn =
        exocortex_kernel::Ontology::from_packs(vec![dev_pack(), mortgage_pack()]).unwrap();
    assert_ne!(
        onto.fingerprint, without_learn.fingerprint,
        "the third pack changes the effective fingerprint"
    );
    // Deterministic across assemblies and equal to a direct compute
    // over the same set (order-independent: R-T21).
    let again = composed();
    assert_eq!(onto.fingerprint, again.fingerprint);
    assert_eq!(
        onto.fingerprint,
        OntologyFingerprint::compute(&[dev_pack(), learn_pack(), mortgage_pack()])
    );
    // OC-PRD: the composed summary is a superset of the prior set —
    // additive composition, a non-event for existing graphs.
    assert!(without_learn.summary.is_subset_of(&onto.summary));
}

#[test]
fn kernel_constants_stay_bound_by_dev_v1_alone() {
    let onto = composed();
    let dev_only = exocortex_kernel::Ontology::from_packs(vec![dev_pack()]).unwrap();
    for required in [
        exocortex_kernel::kinds::SOLVES,
        exocortex_kernel::kinds::FIXES,
        exocortex_kernel::kinds::CAUSES,
        exocortex_kernel::kinds::IN_SESSION,
    ] {
        assert!(
            onto.kinds_by_id.contains_key(&required),
            "kernel constant {required:?} stays bound (R-Pk2)"
        );
        assert_eq!(
            onto.kinds_by_id[&required].display_name, dev_only.kinds_by_id[&required].display_name,
            "kernel rules R1-R9 keep their dev-v1 bindings"
        );
    }
}

#[test]
fn pack_rules_harvest_and_triples_validate() {
    let onto = composed();
    // L1-L3 harvested for fingerprinting.
    let learn = onto
        .packs
        .iter()
        .find(|p| &*p.name == "exocortex-pack-study-v1")
        .unwrap();
    assert_eq!(learn.rule_ids.len(), 3);

    // Pack kinds resolve by name and validate their triples.
    let answers = onto.kind_id("Answers").expect("Answers registered");
    let covers = onto.kind_id("Covers").expect("Covers registered");
    let insight = onto.memory_type_id("Insight").unwrap();
    let question = onto.memory_type_id("Question").unwrap();
    let topic = onto.memory_type_id("Topic").unwrap();
    let resource = onto.memory_type_id("Resource").unwrap();
    let goal = onto.memory_type_id("LearningGoal").unwrap();

    // Right endpoints validate; wrong endpoints are rejected.
    validator::validate_triple(&onto, insight, answers, question)
        .expect("Insight -Answers-> Question is legal");
    assert!(
        validator::validate_triple(&onto, insight, answers, topic).is_err(),
        "the triple table enforces learning endpoints"
    );
    validator::validate_triple(&onto, resource, covers, topic)
        .expect("Resource -Covers-> Topic is legal");
    assert!(
        validator::validate_triple(&onto, resource, covers, goal).is_err(),
        "a Resource cannot Covers a LearningGoal"
    );
}

#[test]
fn computed_only_marker_rides_the_pack() {
    let onto = composed();
    let clustered = onto
        .kinds_by_id
        .values()
        .find(|k| k.display_name == "ClusteredWith")
        .expect("ClusteredWith registered");
    assert!(
        clustered.computed_only,
        "ClusteredWith is Dreams-exclusive; the boundary reads the pack marker (R-T14)"
    );
}
