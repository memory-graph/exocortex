//! D61 permanent pin: the widened `Answers` triple admits dev-surface
//! answers (Fix, Solution) to Questions through the ONE real validator
//! ingest consults, the study Insight case still passes, and a
//! non-answer to-side stays rejected. Before the widening, Fix and
//! Solution were `InvalidTypeTriple` here — coding-chat questions could
//! never be closed by the memories that answer them.

use exocortex_kernel::validator::validate_triple;

fn composed() -> exocortex_kernel::Ontology {
    exocortex_kernel::Ontology::from_packs(vec![
        exocortex_pack_dev_v1::pack_def(),
        exocortex_pack_mortgage_v1::pack_def(),
        exocortex_pack_study_v1::pack_def(),
    ])
    .unwrap()
}

#[test]
fn fix_solution_and_insight_all_answer_questions() {
    let onto = composed();
    let kind = onto.kind_id("Answers").unwrap();
    let question = onto.memory_type_id("Question").unwrap();
    for from_type in ["Fix", "Solution", "Insight"] {
        let from = onto.memory_type_id(from_type).unwrap();
        validate_triple(&onto, from, kind, question)
            .unwrap_or_else(|e| panic!("{from_type} -> Answers -> Question rejected: {e}"));
    }
}

#[test]
fn answers_still_only_land_on_questions() {
    let onto = composed();
    let kind = onto.kind_id("Answers").unwrap();
    let fix = onto.memory_type_id("Fix").unwrap();
    let task = onto.memory_type_id("Task").unwrap();
    assert!(
        validate_triple(&onto, fix, kind, task).is_err(),
        "the to-side must stay pinned to Question"
    );
}
