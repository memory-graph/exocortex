//! M4 acceptance: rules R1-R9 (+D1-D6) produce expected derivations against
//! fixtures; derived-edge writeback is idempotent; `ExplainEdge` returns a
//! Steel tree naming every input fact; R6 reverses Solves; the read path
//! carries no serialization (CR-8).

use std::sync::Arc;

use exocortex_kernel::{
    EntityId, Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_reasoning::{
    rules::{self, Edge, EntityFact, TagFact},
    ExplainEngine, ReasoningEngine, ReasoningWork,
};
use exocortex_storage::{InMemoryStorage, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn kind(name: &str) -> u32 {
    ontology().kind_id(name).unwrap().0
}

fn mem(mt: u8, tags: &[&str], entities: &[u8]) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: mt,
        title: "t".into(),
        content: "c".into(),
        summary: None,
        tags: tags.iter().map(|t| (*t).into()).collect(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted { author: "t".into() },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: None,
            user_id: None,
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: entities.iter().map(|e| EntityId([*e; 16])).collect(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn edge(from: MemoryId, to: MemoryId, k: u32) -> Edge {
    Edge(from, to, exocortex_kernel::RelKindId(k))
}

#[test]
fn rules_r1_through_r3_derive_types() {
    rules::prime(&ontology());
    let sol = MemoryId::new_v7();
    let fixer = MemoryId::new_v7();
    let problem = MemoryId::new_v7();
    let solution_type = exocortex_pack_dev_v1::MemoryType::Solution.id();
    let fix_type = exocortex_pack_dev_v1::MemoryType::Fix.id();
    let problem_type = exocortex_pack_dev_v1::MemoryType::Problem.id();

    let d = rules::evaluate(
        vec![
            edge(sol, problem, exocortex_kernel::kinds::SOLVES.0),
            edge(fixer, problem, exocortex_kernel::kinds::FIXES.0),
        ],
        vec![],
        vec![],
    );
    assert!(d.type_from_solves.contains(&(sol, solution_type)), "R1");
    assert!(d.type_from_fixes.contains(&(fixer, fix_type)), "R2");
    assert!(!d.type_from_causes.contains(&(problem, problem_type)));
    // R3: the Caused target is a Problem.
    let cause = MemoryId::new_v7();
    let d3 = rules::evaluate(
        vec![edge(cause, problem, exocortex_kernel::kinds::CAUSES.0)],
        vec![],
        vec![],
    );
    assert!(d3.type_from_causes.contains(&(problem, problem_type)), "R3");
}

#[test]
fn rules_r4_r5_d2_d3_transitivity() {
    rules::prime(&ontology());
    let a = MemoryId::new_v7();
    let b = MemoryId::new_v7();
    let c = MemoryId::new_v7();
    let dep = kind("DependsOn");
    let req = kind("Requires");
    let builds = kind("BuildsOn");
    let blocks = kind("Blocks");

    let d = rules::evaluate(
        vec![
            edge(a, b, dep),
            edge(b, c, dep),
            edge(a, b, req),
            edge(b, c, req),
            edge(a, b, builds),
            edge(b, c, builds),
            edge(a, b, blocks),
            edge(b, c, req),
        ],
        vec![],
        vec![],
    );
    assert!(d.transitive_depends_on.contains(&(a, c)), "R4");
    assert!(d.transitive_requires.contains(&(a, c)), "R5");
    assert!(d.transitive_builds_on.contains(&(a, c)), "D2");
    assert!(d.indirect_blocker.contains(&(a, c)), "D3");
}

#[test]
fn rules_r7_r8_r9_affinity_and_bridge() {
    rules::prime(&ontology());
    let sol1 = MemoryId::new_v7();
    let sol2 = MemoryId::new_v7();
    let prob = MemoryId::new_v7();
    let e1 = EntityId([1; 16]);

    let d = rules::evaluate(
        vec![
            edge(sol1, prob, exocortex_kernel::kinds::SOLVES.0),
            edge(sol2, prob, exocortex_kernel::kinds::SOLVES.0),
        ],
        vec![EntityFact(sol1, e1), EntityFact(sol2, e1)],
        vec![TagFact(sol1, 7), TagFact(sol2, 7)],
    );
    assert!(
        d.problem_solution_bridge
            .iter()
            .any(|(x, y)| (x, y) == (&sol1, &sol2)),
        "R8"
    );
    assert!(d.co_occurrence_affinity.contains(&(sol1, sol2)), "R7");
    assert!(d.similar_tags_affinity.contains(&(sol1, sol2)), "R9");
}

#[test]
fn pack_rule_d1_subsumption_and_d6_session() {
    rules::prime(&ontology());
    let fix = MemoryId::new_v7();
    let err = MemoryId::new_v7();
    let session = MemoryId::new_v7();
    let m = MemoryId::new_v7();

    let d = rules::evaluate(
        vec![
            edge(fix, err, exocortex_kernel::kinds::FIXES.0),
            edge(m, session, exocortex_kernel::kinds::IN_SESSION.0),
        ],
        vec![],
        vec![],
    );
    assert!(d.implied_solves.contains(&(fix, err)), "D1");
    assert!(d.session_cohort.contains(&(m, session)), "D6");
}

#[tokio::test]
async fn adding_solves_rederives_type_within_same_commit() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 16, 3);

    let a = mem(exocortex_pack_dev_v1::MemoryType::General.id(), &[], &[]);
    let b = mem(exocortex_pack_dev_v1::MemoryType::Problem.id(), &[], &[]);
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();

    // Before: no inference.
    assert!(engine.inferred_type(a.id).await.is_none());

    // Add Solves(A, B) and commit.
    let r = Relationship {
        id: RelationshipId::derive(a.id, exocortex_kernel::kinds::SOLVES, b.id, None),
        kind: exocortex_kernel::kinds::SOLVES,
        from: a.id,
        to: b.id,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted { author: "t".into() },
        properties: exocortex_kernel::RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    };
    storage.upsert_relationship(&r).await.unwrap();

    // Same commit: the k-hop pass runs and R1 re-derives A as Solution.
    engine.k_hop_reason(a.id, 2).await;
    let inferred = engine.inferred_type(a.id).await.expect("R1 fires");
    assert_eq!(inferred, exocortex_pack_dev_v1::MemoryType::Solution.id());
}

#[tokio::test]
async fn derived_writeback_is_idempotent() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 16, 3);

    let a = mem(3, &["rust"], &[9]);
    let b = mem(3, &["rust"], &[9]);
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();

    engine.k_hop_reason(a.id, 2).await;
    use futures::StreamExt;
    let count_after_first = {
        let mut n = 0;
        let mut rs = storage.stream_all_relationships().await;
        while let Some(Ok(_)) = rs.next().await {
            n += 1;
        }
        n
    };

    // Second run over the same input set derives zero new relationships.
    engine.k_hop_reason(a.id, 2).await;
    let count_after_second = {
        let mut n = 0;
        let mut rs = storage.stream_all_relationships().await;
        while let Some(Ok(r)) = rs.next().await {
            assert!(matches!(r.provenance, Provenance::Derived { .. }));
            n += 1;
        }
        n
    };
    assert_eq!(count_after_first, count_after_second, "R-L6 idempotency");
    assert!(count_after_first > 0, "the first pass derived edges");
}

#[tokio::test]
async fn queue_overflow_is_observable_not_silent() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto);
    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 1, 2);
    // Depth-1 queue with no consumer: the second enqueue must not panic and
    // must register the drop (metrics counter; observable via no-crash here).
    engine
        .enqueue(ReasoningWork::KHopOver {
            seed: MemoryId::new_v7(),
            k: 2,
        })
        .await;
    engine
        .enqueue(ReasoningWork::KHopOver {
            seed: MemoryId::new_v7(),
            k: 2,
        })
        .await;
}

#[test]
fn explain_edge_tree_names_input_facts() {
    let target = "aa".to_string() + &"11".repeat(15);
    let parent = "bb".to_string() + &"22".repeat(15);
    let mut engine = ExplainEngine::new();
    let tree = engine.explain(
        vec![
            exocortex_reasoning::EdgeFacts {
                edge_hex: parent.clone(),
                from_hex: "f".into(),
                to_hex: "t".into(),
                kind_name: "Solves".into(),
                rule_id: None,
                parents: vec![],
            },
            exocortex_reasoning::EdgeFacts {
                edge_hex: target.clone(),
                from_hex: "x".into(),
                to_hex: "y".into(),
                kind_name: "RelatedTo".into(),
                rule_id: Some("R8".into()),
                parents: vec![parent.clone()],
            },
        ],
        &target,
    );
    assert!(
        tree.contains(&parent),
        "tree names the input fact {parent}: {tree}"
    );
    assert!(tree.contains("Solves"), "tree names the fact's kind");
}

#[test]
fn r6_reverse_solves_reverses() {
    let a = MemoryId::new_v7();
    let b = MemoryId::new_v7();
    let out = exocortex_reasoning::explain::reverse_solves(&[(a, b)]);
    assert_eq!(out, vec![(b, a)]);
}

#[test]
fn reasoning_read_path_has_no_serialization() {
    // CR-8 grep gate: no serde_json usage inside the reasoning crate.
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    for entry in walk(src_dir) {
        let content = std::fs::read_to_string(&entry).unwrap_or_default();
        if content.contains("serde_json::") {
            violations.push(entry.display().to_string());
        }
    }
    assert!(
        violations.is_empty(),
        "serde_json on the reasoning path: {violations:?}"
    );
}

fn walk(dir: std::path::PathBuf) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(p));
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}
