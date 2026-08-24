//! Validator unit tests (M1 task 3 + 6): R-T5 field bounds, R-T11a
//! no-widening, unknown-kind rejection, R-T17 type-triple enforcement.

use chrono::Utc;
use exocortex_kernel::{
    draft::{EdgeHint, MemoryDraft},
    kinds,
    memory::MemoryContext,
    pack::PackDef,
    pack::PackVersion,
    pack::TypeTriple,
    validator::{validate_draft, SourceCeiling},
    KernelError, Ontology, RelBucket, RelKindId, RelMeta, Visibility,
};
use smallvec::{smallvec, SmallVec};
use smol_str::SmolStr;

const SOLUTION: u8 = 0;
const FIX: u8 = 1;
const PROBLEM: u8 = 2;
const ERROR: u8 = 3;

fn meta(id: RelKindId, name: &'static str) -> RelMeta {
    RelMeta {
        id,
        display_name: SmolStr::new_static(name),
        bucket: RelBucket::Solution,
        inverse: None,
        bidirectional: false,
        default_strength: 0.8,
    }
}

fn test_ontology() -> Ontology {
    let pack = PackDef {
        name: SmolStr::new_static("validator-test-pack"),
        version: PackVersion {
            major: 1,
            minor: 0,
            patch: 0,
        },
        kernel_min: PackVersion {
            major: 1,
            minor: 0,
            patch: 0,
        },
        memory_type_names: ["Solution", "Fix", "Problem", "Error"]
            .iter()
            .map(|n| SmolStr::new_static(n))
            .collect(),
        entity_type_names: vec![],
        kinds: vec![
            meta(kinds::SOLVES, "Solves"),
            meta(kinds::FIXES, "Fixes"),
            meta(kinds::CAUSES, "Causes"),
            meta(kinds::IN_SESSION, "InSession"),
        ],
        type_triples: vec![
            TypeTriple {
                kind: kinds::SOLVES,
                from_types: Some(vec![SOLUTION, FIX]),
                to_types: Some(vec![PROBLEM, ERROR]),
            },
            TypeTriple {
                kind: kinds::FIXES,
                from_types: Some(vec![FIX]),
                to_types: Some(vec![PROBLEM, ERROR]),
            },
            TypeTriple {
                kind: kinds::CAUSES,
                from_types: None,
                to_types: Some(vec![PROBLEM, ERROR]),
            },
            TypeTriple {
                kind: kinds::IN_SESSION,
                from_types: None,
                to_types: None,
            },
        ],
        rule_ids: vec![],
    };
    Ontology::from_packs(vec![pack]).expect("test ontology assembles")
}

fn draft(title: &str) -> MemoryDraft {
    MemoryDraft {
        memory_type: SOLUTION,
        title: SmolStr::new(title),
        content: "body".into(),
        summary: None,
        visibility: Visibility::Org,
        context: MemoryContext {
            timestamp: Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: None,
            user_id: None,
            created_by: None,
            files_involved: SmallVec::new(),
            languages: SmallVec::new(),
            frameworks: SmallVec::new(),
            technologies: SmallVec::new(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: SmallVec::new(),
            additional_metadata: serde_json::Value::Null,
        },
        edge_hints: SmallVec::new(),
        external_key: None,
    }
}

fn ceiling(v: Visibility) -> SourceCeiling {
    SourceCeiling {
        source: "test-source",
        ceiling: v,
    }
}

#[test]
fn valid_solves_solution_problem_is_accepted() {
    let onto = test_ontology();
    let mut d = draft("valid");
    d.edge_hints = smallvec![EdgeHint {
        kind: kinds::SOLVES,
        to: exocortex_kernel::MemoryId::new_v7(),
        strength: None,
        confidence: None,
    }];
    assert!(validate_draft(&onto, &d, ceiling(Visibility::Org)).is_ok());
}

#[test]
fn empty_title_rejected() {
    let onto = test_ontology();
    assert!(matches!(
        validate_draft(&onto, &draft(""), ceiling(Visibility::Org)),
        Err(KernelError::TitleBounds)
    ));
}

#[test]
fn title_over_200_chars_rejected() {
    let onto = test_ontology();
    let long = "x".repeat(201);
    assert!(matches!(
        validate_draft(&onto, &draft(&long), ceiling(Visibility::Org)),
        Err(KernelError::TitleBounds)
    ));
}

#[test]
fn title_at_exactly_200_chars_accepted() {
    let onto = test_ontology();
    let ok = "x".repeat(200);
    assert!(validate_draft(&onto, &draft(&ok), ceiling(Visibility::Org)).is_ok());
}

#[test]
fn empty_content_rejected() {
    let onto = test_ontology();
    let mut d = draft("t");
    d.content = String::new();
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Org)),
        Err(KernelError::EmptyContent)
    ));
}

#[test]
fn summary_over_500_chars_rejected() {
    let onto = test_ontology();
    let mut d = draft("t");
    d.summary = Some(SmolStr::from("s".repeat(501)));
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Org)),
        Err(KernelError::SummaryBounds)
    ));
}

#[test]
fn metadata_over_8kib_rejected() {
    let onto = test_ontology();
    let mut d = draft("t");
    d.context.additional_metadata = serde_json::Value::String("m".repeat(9 * 1024));
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Org)),
        Err(KernelError::MetadataTooLarge)
    ));
}

#[test]
fn visibility_widening_rejected() {
    let onto = test_ontology();
    let d = draft("t"); // visibility: Org
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Project)),
        Err(KernelError::VisibilityWidening { .. })
    ));
}

#[test]
fn unknown_kind_rejected() {
    let onto = test_ontology();
    let mut d = draft("t");
    d.edge_hints = smallvec![EdgeHint {
        kind: RelKindId(0x7FFF_FFFF), // not registered anywhere
        to: exocortex_kernel::MemoryId::new_v7(),
        strength: None,
        confidence: None,
    }];
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Org)),
        Err(KernelError::UnknownKind(_))
    ));
}

#[test]
fn invalid_triple_rejected() {
    let onto = test_ontology();
    // PROBLEM --Solves--> ... : the from-type is not Solution|Fix.
    let mut d = draft("t");
    d.memory_type = PROBLEM;
    d.edge_hints = smallvec![EdgeHint {
        kind: kinds::SOLVES,
        to: exocortex_kernel::MemoryId::new_v7(),
        strength: None,
        confidence: None,
    }];
    assert!(matches!(
        validate_draft(&onto, &d, ceiling(Visibility::Org)),
        Err(KernelError::InvalidTypeTriple { .. })
    ));
}

#[test]
fn wildcard_from_side_accepted() {
    let onto = test_ontology();
    // CAUSES has from_types: None (any) — a Problem source is fine.
    let mut d = draft("t");
    d.memory_type = PROBLEM;
    d.edge_hints = smallvec![EdgeHint {
        kind: kinds::CAUSES,
        to: exocortex_kernel::MemoryId::new_v7(),
        strength: None,
        confidence: None,
    }];
    assert!(validate_draft(&onto, &d, ceiling(Visibility::Org)).is_ok());
}

#[test]
fn score_out_of_range_rejected() {
    assert!(matches!(
        exocortex_kernel::memory::F01::new(1.01),
        Err(KernelError::ScoreOutOfRange(_))
    ));
    assert!(exocortex_kernel::memory::F01::new(1.0).is_ok());
    assert!(exocortex_kernel::memory::F01::new(0.0).is_ok());
    assert!(matches!(
        exocortex_kernel::memory::F01::new(-0.01),
        Err(KernelError::ScoreOutOfRange(_))
    ));
}
