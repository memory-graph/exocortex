//! PX1 acceptance (palantir-expansion PRD §3.1; supersedes D2/§23.26):
//! the second pack composes beside dev-v1 into one effective ontology
//! without renumbering a single dev-v1 id, exposes its kinds to pack
//! rules without rebinding kernel constants, and produces its own
//! byte-stable fingerprint.

use exocortex_kernel::{pack::load_registered_packs, validator, OntologyFingerprint};
use exocortex_pack_dev_v1::pack_def as dev_pack;
use exocortex_pack_mortgage_v1::pack_def as mortgage_pack;

fn composed() -> exocortex_kernel::Ontology {
    load_registered_packs().expect("both linked packs assemble")
}

#[test]
fn both_packs_register_into_one_ontology() {
    let onto = composed();
    assert_eq!(onto.packs.len(), 2, "dev-v1 and mortgage-v1 both linked");
    // Name-sorted: dev-v1 occupies the first pack slot.
    assert_eq!(&*onto.packs[0].name, "exocortex-pack-dev-v1");
    assert_eq!(&*onto.packs[1].name, "exocortex-pack-mortgage-v1");
    // 13 + 7 memory types, 12 + 5 entity types, no renumbering.
    assert_eq!(onto.memory_type_names.len(), 20);
    assert_eq!(onto.entity_type_names.len(), 17);
    let dev_only = exocortex_kernel::Ontology::from_packs(vec![dev_pack()]).unwrap();
    for (id, name) in dev_only.memory_type_names.iter().enumerate() {
        assert_eq!(
            onto.memory_type_names.get(id),
            Some(name),
            "dev-v1 memory type id {id} must not renumber"
        );
    }
    for (id, name) in dev_only.entity_type_names.iter().enumerate() {
        assert_eq!(
            onto.entity_type_names.get(id),
            Some(name),
            "dev-v1 entity type id {id} must not renumber"
        );
    }
    // Mortgage types sit at the offset after dev-v1's.
    assert_eq!(onto.memory_type_id("Applicant"), Some(13));
    assert_eq!(onto.memory_type_id("RuleDefinition"), Some(16));
    assert_eq!(onto.entity_type_by_name.get("Lender").copied(), Some(12));
}

#[test]
fn composed_fingerprint_differs_from_dev_only_and_is_stable() {
    let onto = composed();
    let dev_only = exocortex_kernel::Ontology::from_packs(vec![dev_pack()]).unwrap();
    assert_ne!(
        onto.fingerprint, dev_only.fingerprint,
        "§23.26: a second pack changes the effective fingerprint"
    );
    // Deterministic across assemblies and equal to a direct compute
    // over the same set (order-independent: R-T21).
    let again = composed();
    assert_eq!(onto.fingerprint, again.fingerprint);
    assert_eq!(
        onto.fingerprint,
        OntologyFingerprint::compute(&[dev_pack(), mortgage_pack()])
    );
    // OC-PRD: the composed summary is a superset of dev-v1's —
    // additive composition, which is what makes rolling in this pack a
    // non-event for dev-v1 graphs.
    assert!(dev_only.summary.is_subset_of(&onto.summary));
}

#[test]
fn kernel_constants_stay_bound_by_dev_v1_without_mortgage_rebinding() {
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
    // M1-M3 harvested for fingerprinting.
    let mortgage = onto
        .packs
        .iter()
        .find(|p| &*p.name == "exocortex-pack-mortgage-v1")
        .unwrap();
    assert_eq!(mortgage.rule_ids.len(), 3);

    // Pack kinds resolve by name and validate their triples.
    let governs = onto.kind_id("Governs").expect("Governs registered");
    let under_rule = onto.kind_id("UnderRule").expect("UnderRule registered");
    let supports = onto
        .kind_id("SupportsIncome")
        .expect("SupportsIncome registered");
    let finding = onto.memory_type_id("RuleFinding").unwrap();
    let rule = onto.memory_type_id("RuleDefinition").unwrap();
    let txn = onto.memory_type_id("Transaction").unwrap();
    let income = onto.memory_type_id("IncomeSource").unwrap();

    // Right endpoints validate; wrong endpoints are rejected (a
    // RuleFinding may not UnderRule a Transaction).
    validator::validate_triple(&onto, finding, under_rule, rule)
        .expect("RuleFinding -UnderRule-> RuleDefinition is legal");
    assert!(
        validator::validate_triple(&onto, finding, under_rule, txn).is_err(),
        "the triple table enforces mortgage endpoints"
    );
    validator::validate_triple(&onto, txn, supports, income)
        .expect("Transaction -SupportsIncome-> IncomeSource is legal");
    assert!(
        validator::validate_triple(&onto, rule, supports, income).is_err(),
        "a RuleDefinition cannot SupportIncome"
    );
    let _ = governs;
}

#[test]
fn computed_only_marker_rides_the_pack() {
    let onto = composed();
    let merge = onto
        .kind_id("MergeDuplicateApplicant")
        .expect("computed-only kind registered");
    assert!(
        onto.kinds_by_id[&merge].computed_only,
        "R-T14: MergeDuplicateApplicant is Dreams-exclusive"
    );
    // And dev-v1's marker is untouched.
    let similar = onto.kind_id("SimilarTo").unwrap();
    assert!(onto.kinds_by_id[&similar].computed_only);
}
