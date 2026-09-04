//! D8 (§12.1 step 4): the ABSTRACT row-writing variant — a
//! multi-member class gets an `Abstraction` row with computed-only
//! `Summarizes` membership, deterministic identity, and rollback-able
//! commit. Fail-without-it: on the pre-D8 tree no such row or kind
//! exists and `abstracted` carried member representatives.

use std::sync::Arc;

use exocortex_dreams::trigger::DreamsTrigger;
use exocortex_dreams::DreamsEngine;
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_storage::{InMemoryStorage, RegionKey, Storage};
use futures::StreamExt;

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

/// Type 3 (Solution); 4-dim vectors kept mutually below the 0.92 merge
/// bar so the class survives consolidation intact (five members).
fn member(i: usize, vector: [f32; 4]) -> Memory {
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: format!("member {i}").into(),
        content: format!("content {i}"),
        summary: None,
        tags: Default::default(),
        visibility: if i == 0 {
            Visibility::Private
        } else {
            Visibility::Org
        },
        provenance: Provenance::Asserted {
            author: "dreams".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some("p".into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("o".into()),
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
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: Some(exocortex_kernel::Embedding {
            model: exocortex_kernel::EmbeddingModel {
                name: "bge-small".into(),
                version: "v1".into(),
            },
            vector: vector.to_vec(),
        }),
        lsn: LSN::new_local(0),
    }
}

fn dataset() -> Vec<Memory> {
    let c = 0.906_f32; // cos(25deg): pairwise cos^2 = 0.821, below the merge bar
    let s = 0.423_f32;
    vec![
        member(0, [c, s, 0.0, 0.0]),
        member(1, [c, 0.0, s, 0.0]),
        member(2, [c, 0.0, 0.0, s]),
        member(3, [0.0, 1.0, 0.0, 0.0]),
        member(4, [0.0, 0.0, 0.0, 1.0]),
    ]
}

fn region() -> RegionKey {
    RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    }
}

async fn all_memories(storage: &InMemoryStorage) -> Vec<Memory> {
    let mut stream = storage.stream_all_memories().await;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    rows
}

async fn all_relationships(storage: &InMemoryStorage) -> Vec<exocortex_kernel::Relationship> {
    let mut stream = storage.stream_all_relationships().await;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    rows
}

#[tokio::test]
async fn abstraction_rows_carry_the_class_with_computed_membership() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    for row in dataset() {
        storage.upsert_memory(&row).await.unwrap();
    }
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-abstract".into(),
    );
    let res = engine.try_consolidate(&region()).await.expect("cycle");
    assert!(
        res.merged.is_empty(),
        "the dataset stays under the merge bar"
    );

    let abstraction_type = onto.memory_type_id("General").expect("carrier type");
    let summarizes = onto.kind_id("Summarizes").expect("D8 kind");

    let memories = all_memories(&storage).await;
    let abstractions: Vec<_> = memories
        .iter()
        .filter(|row| row.memory_type == abstraction_type)
        .collect();
    assert_eq!(abstractions.len(), 1, "one abstraction for the one class");
    let abstraction = abstractions[0];
    assert!(
        abstraction.title.as_str().contains("5"),
        "{}",
        abstraction.title
    );
    assert!(matches!(
        &abstraction.provenance,
        Provenance::Computed {
            producer: exocortex_kernel::provenance::ComputedProducer::Abstraction,
            ..
        }
    ));
    // Narrowest member visibility (one member is Private).
    assert_eq!(abstraction.visibility, Visibility::Private);
    // The centroid embedding carries the members' common model.
    let embedding = abstraction.embedding.as_ref().expect("centroid");
    assert_eq!(embedding.model.name.as_str(), "bge-small");

    // Computed-only membership: one Summarizes edge per member.
    let edges = all_relationships(&storage).await;
    let membership: Vec<_> = edges
        .iter()
        .filter(|edge| edge.kind == summarizes)
        .collect();
    assert_eq!(membership.len(), 5, "{edges:?}");
    for edge in &membership {
        assert_eq!(edge.from, abstraction.id);
        assert!(edge.bidirectional);
        assert!(matches!(
            &edge.provenance,
            Provenance::Computed {
                producer: exocortex_kernel::provenance::ComputedProducer::Abstraction,
                ..
            }
        ));
        assert_eq!(edge.visibility, Visibility::Private);
    }

    // The result stamp carries the abstraction ROW id (its documented
    // meaning), not a member representative.
    assert_eq!(res.abstracted, vec![abstraction.id]);
}

#[tokio::test]
async fn abstraction_identity_is_idempotent_across_cycles() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto);
    for row in dataset() {
        storage.upsert_memory(&row).await.unwrap();
    }
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-abstract".into(),
    );
    let first = engine.try_consolidate(&region()).await.expect("cycle 1");
    let second = engine.try_consolidate(&region()).await.expect("cycle 2");

    // Same member set ⇒ same derived id, one row, five edges.
    assert_eq!(first.abstracted, second.abstracted);
    let memories = all_memories(&storage).await;
    let general = onto_kind_carrier();
    let abstractions = memories
        .iter()
        .filter(|row| {
            row.memory_type == general
                && matches!(
                    &row.provenance,
                    Provenance::Computed {
                        producer: exocortex_kernel::provenance::ComputedProducer::Abstraction,
                        ..
                    }
                )
        })
        .count();
    assert_eq!(abstractions, 1, "deterministic identity, no duplicates");
    let edges = all_relationships(&storage).await;
    let membership = edges
        .iter()
        .filter(|edge| edge.kind == onto_kind_summarizes())
        .count();
    assert_eq!(membership, 5, "no duplicate membership rows");
}

fn onto_kind_summarizes() -> exocortex_kernel::RelKindId {
    ontology().kind_id("Summarizes").unwrap()
}

fn onto_kind_carrier() -> u8 {
    ontology().memory_type_id("General").unwrap()
}
