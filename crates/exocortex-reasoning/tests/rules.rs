//! M4 acceptance: rules R1-R9 (+D1-D6) produce expected derivations against
//! fixtures; derived-edge writeback is idempotent; `ExplainEdge` returns a
//! Steel tree naming every input fact; R6 reverses Solves; the read path
//! carries no serialization (CR-8).

use std::sync::atomic::{AtomicU64, Ordering};
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
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
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

#[tokio::test]
async fn small_reasoning_update_never_scans_the_full_store() {
    let storage = InMemoryStorage::new(ontology());
    let seed = mem(exocortex_pack_dev_v1::MemoryType::General.id(), &[], &[]);
    storage.upsert_memory(&seed).await.unwrap();
    for _ in 0..2_000 {
        storage
            .upsert_memory(&mem(
                exocortex_pack_dev_v1::MemoryType::General.id(),
                &[],
                &[],
            ))
            .await
            .unwrap();
    }
    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 8, 3);
    engine.k_hop_reason(seed.id, 3).await;

    let (memory_streams, relationship_streams, frontier_reads, attribute_reads) =
        storage.reasoning_query_counts();
    assert_eq!(memory_streams, 0, "memory inputs use bounded point reads");
    assert_eq!(relationship_streams, 0, "edge inputs use frontier indexes");
    assert_eq!(
        frontier_reads, 1,
        "an isolated change stops after one frontier"
    );
    assert_eq!(
        attribute_reads, 0,
        "empty attributes need no expansion query"
    );
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
        vec![],
    );
    assert!(
        d.problem_solution_bridge
            .iter()
            .any(|(x, y)| (x, y) == (&sol1, &sol2)),
        "R8"
    );
    // CR7: the fold runs where confidence is computed; two memories
    // sharing exactly one entity and one tag fold to count 1 each.
    let r7 = rules::pair_counts(d.co_occurrence_affinity.clone());
    assert!(r7.contains(&(sol1, sol2, 1)), "R7 folded: {r7:?}");
    let r9 = rules::pair_counts(d.similar_tags_affinity.clone());
    assert!(r9.contains(&(sol1, sol2, 1)), "R9 folded: {r9:?}");
}

#[test]
fn pack_rule_d1_subsumption_and_d6_session() {
    rules::prime(&ontology());
    let fix = MemoryId::new_v7();
    let err = MemoryId::new_v7();
    let session = MemoryId::new_v7();
    let m = MemoryId::new_v7();

    let onto = ontology();
    let fix_type = onto.memory_type_id("Fix").unwrap();
    let d = rules::evaluate(
        vec![
            edge(fix, err, exocortex_kernel::kinds::FIXES.0),
            edge(m, session, exocortex_kernel::kinds::IN_SESSION.0),
        ],
        vec![],
        vec![],
        vec![rules::MemoryFact(fix, fix_type)],
    );
    assert!(d.implied_solves.contains(&(fix, err)), "D1");
    assert!(d.session_cohort.contains(&(m, session)), "D6");

    // KP1: the D1 guard is real — a non-Fix source memory does not derive
    // Solves from a Fixes edge (matches the pack's rule text).
    let not_fix = MemoryId::new_v7();
    let d2 = rules::evaluate(
        vec![edge(not_fix, err, exocortex_kernel::kinds::FIXES.0)],
        vec![],
        vec![],
        vec![rules::MemoryFact(
            not_fix,
            onto.memory_type_id("Problem").unwrap(),
        )],
    );
    assert!(
        !d2.implied_solves.contains(&(not_fix, err)),
        "D1 guard: only a Fix derives Solves"
    );
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
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
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
    #[derive(Default)]
    struct CounterValue(AtomicU64);

    impl metrics::CounterFn for CounterValue {
        fn increment(&self, value: u64) {
            self.0.fetch_add(value, Ordering::SeqCst);
        }

        fn absolute(&self, value: u64) {
            self.0.fetch_max(value, Ordering::SeqCst);
        }
    }

    struct DropRecorder {
        dropped: Arc<CounterValue>,
    }

    impl metrics::Recorder for DropRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            if key.name() == "exocortex_reasoning_dropped_total" {
                metrics::Counter::from_arc(self.dropped.clone())
            } else {
                metrics::Counter::noop()
            }
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    let dropped = Arc::new(CounterValue::default());
    let recorder = DropRecorder {
        dropped: dropped.clone(),
    };
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let onto = ontology();
    let storage = InMemoryStorage::new(onto);
    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 1, 2);
    // Depth-1 queue with no consumer: the second enqueue must register one
    // observable drop rather than silently losing work.
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
    assert_eq!(dropped.0.load(Ordering::SeqCst), 1);
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

/// KP1 (audit): the pack's declared rule set and the engine's implemented
/// rule set are the SAME set — the pack text is no longer a decoration the
/// engine can silently drift from.
#[test]
fn pack_rule_ids_match_engine_outputs() {
    let pack = exocortex_pack_dev_v1::pack_def();
    let declared: std::collections::BTreeSet<&str> =
        pack.rule_ids.iter().map(|s| s.as_str()).collect();
    // The engine's writeback consumes one Derived field per pack rule —
    // D5 (shared_target) included since the KP1 fix.
    let implemented: std::collections::BTreeSet<&str> = [
        "implied_solves",
        "transitive_builds_on",
        "indirect_blocker",
        "contradiction_propagates",
        "shared_target",
        "session_cohort",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        declared, implemented,
        "pack declares and engine implements different rule sets (KP1)"
    );
}

/// CR7 (audit): §14.2 confidence per-pair. Two memories sharing 2 tags
/// and no relationships get a derived R9 edge at 2/5 = 0.4 — the old code
/// fed the neighborhood edge count (0 edges → 0.0).
#[tokio::test]
async fn derived_confidence_uses_per_pair_shared_count() {
    let onto = ontology();
    rules::prime(&onto);
    let storage = InMemoryStorage::new(onto.clone());
    let a = MemoryId::new_v7();
    let b = MemoryId::new_v7();

    // Two memories sharing exactly 2 tags, no edges.
    let mem = |id: MemoryId| {
        let mut m = base_memory_for_tags();
        m.id = id;
        m
    };
    let mut ma = mem(a);
    ma.tags = ["shared1".into(), "shared2".into()].into_iter().collect();
    let mut mb = mem(b);
    mb.tags = ["shared1".into(), "shared2".into()].into_iter().collect();
    storage.upsert_memory(&ma).await.unwrap();
    storage.upsert_memory(&mb).await.unwrap();

    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 16, 2);
    engine.k_hop_reason(a, 2).await;

    use exocortex_storage::Storage;
    use futures::StreamExt;
    let mut rs = storage.stream_all_relationships().await;
    let mut found = None;
    while let Some(Ok(r)) = rs.next().await {
        let pair = (r.from, r.to);
        if pair == (a, b) || pair == (b, a) {
            if let exocortex_kernel::Provenance::Derived { rule_id, .. } = &r.provenance {
                if rule_id == "R9" {
                    found = Some(r.properties.confidence);
                }
            }
        }
    }
    let conf = found.expect("R9 edge derived");
    assert!(
        (conf - 0.4).abs() < 1e-4,
        "CR7: 2 shared tags / 5.0 = 0.4, got {conf}"
    );
}

fn base_memory_for_tags() -> exocortex_kernel::Memory {
    use exocortex_kernel::*;
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: "t".into(),
        content: "c".into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
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
            entities: Default::default(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: memory::F01::new(0.5).unwrap(),
        confidence: memory::F01::new(0.8).unwrap(),
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

/// CR11 (audit): 2,000 memories sharing one common tag inside a small
/// neighborhood must not materialize a quadratic derived-edge blowup —
/// the posting-list filter drops the high-frequency tag and the pass
/// completes bounded.
#[tokio::test]
async fn common_tag_does_not_blow_up_derived_pairs() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    // Seed + 300 memories all tagged "common" (above the 256 posting cap).
    let seed = MemoryId::new_v7();
    let mut ids = vec![seed];
    for i in 0..300 {
        let m = {
            let mut m = base_memory_for_tags();
            m.id = MemoryId::new_v7();
            m.title = format!("m{i}").into();
            m.tags = ["common".into()].into_iter().collect();
            m
        };
        ids.push(m.id);
        storage.upsert_memory(&m).await.unwrap();
    }

    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 16, 2);
    engine.k_hop_reason(seed, 2).await;

    use exocortex_storage::Storage;
    use futures::StreamExt;
    let mut rs = storage.stream_all_relationships().await;
    let mut r9 = 0usize;
    while let Some(Ok(r)) = rs.next().await {
        if let exocortex_kernel::Provenance::Derived { rule_id, .. } = &r.provenance {
            if rule_id == "R9" {
                r9 += 1;
            }
        }
    }
    assert_eq!(
        r9, 0,
        "CR11: high-frequency tag yields no R9 edges, got {r9}"
    );
    let _ = ids;
}

/// CR12 (audit): a transitive derivation's evidence is exactly its two
/// supporting hops — not the whole k-hop neighborhood's edge list.
#[tokio::test]
async fn transitive_evidence_is_the_two_hops() {
    let onto = ontology();
    rules::prime(&onto);
    let storage = InMemoryStorage::new(onto.clone());
    let dep = onto.kind_id("DependsOn").unwrap();
    let mk = |i: u8| {
        let mut m = base_memory_for_tags();
        m.id = MemoryId([i; 16]);
        m.title = format!("n{i}").into();
        m
    };
    let (mut a, mut b, c) = (mk(1), mk(2), mk(3));
    a.visibility = exocortex_kernel::Visibility::Project;
    b.visibility = exocortex_kernel::Visibility::Team;
    for m in [&a, &b, &c] {
        storage.upsert_memory(m).await.unwrap();
    }
    let rel = |from: MemoryId, to: MemoryId, visibility| {
        use exocortex_kernel::*;
        Relationship {
            id: RelationshipId::derive(from, dep, to, None),
            kind: dep,
            from,
            to,
            visibility,
            provenance: Provenance::Asserted {
                author: "t".into(),
                producer_kind: None,
            },
            properties: RelationshipProperties {
                strength: 0.5,
                confidence: 0.5,
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
            lsn: LSN::new_backend(1),
        }
    };
    storage
        .upsert_relationship(&rel(a.id, b.id, Visibility::Private))
        .await
        .unwrap();
    storage
        .upsert_relationship(&rel(b.id, c.id, Visibility::Team))
        .await
        .unwrap();

    let engine = ReasoningEngine::new(Arc::new(storage.clone_dyn()), 16, 2);
    engine.k_hop_reason(a.id, 2).await;

    use exocortex_storage::Storage;
    use futures::StreamExt;
    let mut rs = storage.stream_all_relationships().await;
    let mut found = None;
    while let Some(Ok(r)) = rs.next().await {
        if let exocortex_kernel::Provenance::Derived { rule_id, evidence } = &r.provenance {
            if rule_id == "R4" {
                found = Some((evidence.clone(), r.visibility));
            }
        }
    }
    let (ev, visibility) = found.expect("R4 transitive edge derived");
    assert_eq!(ev.len(), 2, "CR12: exactly the two hops, got {ev:?}");
    let hop1 = RelationshipId::derive(a.id, dep, b.id, None);
    let hop2 = RelationshipId::derive(b.id, dep, c.id, None);
    assert!(
        ev.contains(&hop1) && ev.contains(&hop2),
        "the two hops: {ev:?}"
    );
    assert_eq!(
        visibility,
        Visibility::Private,
        "derived edge is no wider than either endpoint or supporting hop"
    );
}
