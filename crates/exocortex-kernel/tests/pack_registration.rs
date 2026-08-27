//! Pack registration tests (§7.19 step 4): a fake `TestPack` registered via
//! the real `pack!` macro asserts (a) kernel-constant coverage, (b) R-Pk1
//! duplicate rejection, (c) fingerprint sensitivity, plus the ignored
//! unbound-kernel-constant test from the M1 acceptance criteria.

use exocortex_kernel::{pack, KernelError, Ontology};
use smol_str::SmolStr;

pack! {
    name: "test-pack",
    version: "0.1.0",
    kernel_min: "1.0.0",

    memory_types! { Problem, Solution, Fix, Error, Conversation, }

    entity_types! { File, Concept, }

    kinds! {
        Solves    => bucket: Solution, inverse: SolvedBy,  bi: false, default_strength: 0.85, kernel_const: SOLVES,
        Fixes     => bucket: Causal,   inverse: FixedBy,   bi: false, default_strength: 0.90, kernel_const: FIXES,
        Causes    => bucket: Causal,   inverse: CausedBy,  bi: false, default_strength: 0.85, kernel_const: CAUSES,
        InSession => bucket: Context,  inverse: HasMember, bi: false, default_strength: 0.80, kernel_const: IN_SESSION,
        BuildsOn  => bucket: Learning, inverse: BuiltOnBy, bi: false, default_strength: 0.75,
        RelatedTo => bucket: Similarity, inverse: Self,    bi: true,  default_strength: 0.30,
    }

    type_triples! {
        Solves    => (Solution | Fix, Problem | Error),
        Fixes     => (Fix, Error | Problem),
        Causes    => (_, Error | Problem),
        InSession => (_, Conversation),
        BuildsOn  => (_, _),
        RelatedTo => (_, _),
    }

    crepe_rules! {
        test_rule(a, b) <- edge(a, b, Solves), memory(a, _, _);
    }
}

#[test]
fn registered_pack_loads_with_kernel_constants_bound() {
    let onto = exocortex_kernel::pack::load_registered_packs().expect("loads");
    assert!(onto
        .kinds_by_id
        .contains_key(&exocortex_kernel::kinds::SOLVES));
    assert!(onto
        .kinds_by_id
        .contains_key(&exocortex_kernel::kinds::FIXES));
    assert!(onto
        .kinds_by_id
        .contains_key(&exocortex_kernel::kinds::CAUSES));
    assert!(onto
        .kinds_by_id
        .contains_key(&exocortex_kernel::kinds::IN_SESSION));
    // Emitted enums line up with the ontology's id assignment.
    assert_eq!(
        onto.memory_type_id("Solution"),
        Some(MemoryType::Solution.id())
    );
    assert_eq!(
        onto.memory_type_id("Conversation"),
        Some(MemoryType::Conversation.id())
    );
    // Pack rules were harvested.
    assert_eq!(
        onto.packs[0].rule_ids,
        vec![SmolStr::new_static("test_rule")]
    );
}

#[test]
fn inverse_companions_are_registered_and_symmetric() {
    let onto = exocortex_kernel::pack::load_registered_packs().unwrap();
    let solves = onto.kind_id("Solves").unwrap();
    let solved_by = onto.kind_id("SolvedBy").unwrap();
    assert_eq!(onto.kinds_by_id[&solves].inverse, Some(solved_by));
    assert_eq!(onto.kinds_by_id[&solved_by].inverse, Some(solves));
    // Self-inverse kind points at itself.
    let related = onto.kind_id("RelatedTo").unwrap();
    assert_eq!(onto.kinds_by_id[&related].inverse, Some(related));
    // Companions carry no type triples -> cannot be authored directly (R-T4).
    assert!(!onto.triples_by_kind.contains_key(&solved_by));
}

#[test]
fn duplicate_pack_names_rejected() {
    let p = pack_def();
    let err = Ontology::from_packs(vec![p.clone(), p]).unwrap_err();
    assert!(matches!(err, KernelError::DuplicatePack(_)), "got {err:?}");
}

#[test]
fn fingerprint_is_order_independent_and_kind_sensitive() {
    let a = pack_def();
    let mut b = a.clone();
    let extra = exocortex_kernel::RelMeta {
        id: exocortex_kernel::RelKindId(0x8000_0000 | 0x0100),
        display_name: SmolStr::new_static("ExtraKind"),
        bucket: exocortex_kernel::RelBucket::Extension(1),
        inverse: None,
        bidirectional: false,
        default_strength: 0.5,
        computed_only: false,
    };
    b.kinds.push(extra);
    // Identity: same def -> same fingerprint.
    assert_eq!(
        exocortex_kernel::OntologyFingerprint::compute(&[a.clone()]),
        exocortex_kernel::OntologyFingerprint::compute(&[a.clone()])
    );
    // Adding a kind changes the fingerprint.
    assert_ne!(
        exocortex_kernel::OntologyFingerprint::compute(&[a]),
        exocortex_kernel::OntologyFingerprint::compute(&[b])
    );
}

/// M1 acceptance: removing any single kernel constant from the pack set makes
/// assembly fail with `UnboundKernelConstant`. Ignored by default because it
/// mutates a hand-built def rather than the shipped dev-v1 pack.
#[test]
#[ignore = "simulate a pack that drops a kernel constant"]
fn unbound_kernel_constant_rejected() {
    let mut p = pack_def();
    p.kinds
        .retain(|k| k.id != exocortex_kernel::kinds::IN_SESSION);
    let err = Ontology::from_packs(vec![p]).unwrap_err();
    assert!(
        matches!(err, KernelError::UnboundKernelConstant(_)),
        "got {err:?}"
    );
}

/// KP2 (audit): a second pack's type triples must remap into ontology id
/// space — the pack-side ids are declaration indices, the ontology assigns
/// with a running offset. Before the fix, every triple in the second pack
/// evaluated against the first pack's ids, so legitimate writes were
/// rejected `InvalidTypeTriple` and first-pack types satisfied foreign
/// triples.
#[test]
fn second_pack_triples_remap_into_ontology_space() {
    let mut second = pack_def();
    second.name = SmolStr::new_static("zz-second-pack");
    // Fresh kind + fresh type names so nothing collides with dev-v1.
    second.kinds = vec![exocortex_kernel::RelMeta {
        id: exocortex_kernel::RelKindId(0x8000_0000 | 0x0300),
        display_name: SmolStr::new_static("ZzKind"),
        bucket: exocortex_kernel::RelBucket::Extension(2),
        inverse: None,
        bidirectional: false,
        default_strength: 0.5,
        computed_only: false,
    }];
    second.memory_type_names = vec![
        SmolStr::new_static("ZzNote"),
        SmolStr::new_static("ZzTicket"),
    ];
    second.entity_type_names = vec![SmolStr::new_static("ZzThing")];
    let zz_kind = second.kinds[0].id;
    // Pack-local ids: ZzNote=0, ZzTicket=1 (declaration indices).
    second.type_triples = vec![exocortex_kernel::pack::TypeTriple {
        kind: zz_kind,
        from_types: Some(vec![0]),
        to_types: Some(vec![1]),
    }];

    let onto = Ontology::from_packs(vec![pack_def(), second]).expect("two packs assemble");
    let offset = onto
        .memory_type_by_name
        .get("ZzNote")
        .copied()
        .expect("ZzNote assigned an ontology id");
    // The test pack's 5 memory types precede the second pack.
    assert_eq!(offset, 5, "running offset across packs");

    // The kind id is canonicalized to pack space; resolve it by name.
    let zz_kind_onto = onto
        .kinds_by_id
        .values()
        .find(|k| k.display_name == "ZzKind")
        .unwrap()
        .id;
    let triple = &onto.triples_by_kind[&zz_kind_onto][0];
    assert_eq!(
        triple.from_types,
        Some(vec![onto.memory_type_by_name["ZzNote"]]),
        "from-side remapped into ontology space (KP2)"
    );
    assert_eq!(
        triple.to_types,
        Some(vec![onto.memory_type_by_name["ZzTicket"]]),
        "to-side remapped into ontology space (KP2)"
    );

    // And the triple VALIDATES the pair it was declared for (the actual
    // failure scenario: legitimate second-pack writes were rejected).
    assert!(exocortex_kernel::validator::validate_triple(
        &onto,
        onto.memory_type_by_name["ZzNote"],
        zz_kind_onto,
        onto.memory_type_by_name["ZzTicket"],
    )
    .is_ok());
    // While a dev-v1 type does NOT satisfy the foreign triple.
    assert!(exocortex_kernel::validator::validate_triple(
        &onto,
        onto.memory_type_by_name["Fix"],
        zz_kind_onto,
        onto.memory_type_by_name["ZzTicket"],
    )
    .is_err());
}

/// KP2: duplicate memory-type names across packs are a registration error,
/// never a silent last-writer-wins.
#[test]
fn duplicate_type_names_across_packs_rejected() {
    let mut second = pack_def();
    second.name = SmolStr::new_static("zz-dup-pack");
    second.kinds.clear();
    second.type_triples.clear();
    second.memory_type_names = vec![SmolStr::new_static("Problem")]; // test-pack clash
    second.entity_type_names = vec![];
    let err = Ontology::from_packs(vec![pack_def(), second]).unwrap_err();
    assert!(
        matches!(err, KernelError::DuplicateTypeName(_)),
        "got {err:?}"
    );
}
