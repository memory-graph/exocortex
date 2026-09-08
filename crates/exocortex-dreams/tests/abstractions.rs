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
    // Round 12 scope partition: the Private member (i == 0) no longer
    // joins the Org members' abstraction — one abstraction row cannot
    // faithfully carry two owners' read restrictions, and the old
    // narrowest-label row exposed the private member's title to the
    // first member's author.
    assert!(
        abstraction.title.as_str().contains("4"),
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
    // The abstraction carries the Org scope of its four Org members —
    // never the private member's label.
    assert_eq!(abstraction.visibility, Visibility::Org);
    assert!(
        !abstraction.content.contains("member 0"),
        "the private member's title never rides the Org abstraction"
    );
    // The centroid embedding carries the members' common model.
    let embedding = abstraction.embedding.as_ref().expect("centroid");
    assert_eq!(embedding.model.name.as_str(), "bge-small");

    // Computed-only membership: one Summarizes edge per member.
    let edges = all_relationships(&storage).await;
    let membership: Vec<_> = edges
        .iter()
        .filter(|edge| edge.kind == summarizes)
        .collect();
    assert_eq!(membership.len(), 4, "{edges:?}");
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
        assert_eq!(edge.visibility, Visibility::Org);
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
    // R12: idempotent means NOT REWRITTEN — the committed row keeps its
    // original timestamps (the old working-set guard could never fire in
    // a typed region, so every cycle re-created the row with fresh
    // stamps and a journaled create over a preimage-holding row made
    // rollback delete the prior cycle's committed abstraction).
    let row_after_first = storage
        .get_memory(&first.abstracted[0])
        .await
        .unwrap()
        .expect("row after cycle 1");
    let row_after_second = storage
        .get_memory(&second.abstracted[0])
        .await
        .unwrap()
        .expect("row after cycle 2");
    assert_eq!(
        row_after_first.valid_from, row_after_second.valid_from,
        "cycle 2 must not rewrite the committed abstraction row"
    );
    assert_eq!(
        row_after_first.recorded_at, row_after_second.recorded_at,
        "cycle 2 must not restamp the committed abstraction row"
    );
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
    assert_eq!(membership, 4, "no duplicate membership rows");
}

fn onto_kind_summarizes() -> exocortex_kernel::RelKindId {
    ontology().kind_id("Summarizes").unwrap()
}

fn onto_kind_carrier() -> u8 {
    ontology().memory_type_id("General").unwrap()
}

/// R9-1: Dreams never re-consolidates its own computed layer — an
/// org-wide ("*") cycle over a graph containing abstraction rows must
/// not anchor them: no abstraction is merged or closed, and the member
/// set stays the only anchor population.
#[tokio::test]
async fn org_wide_cycles_do_not_reconsolidate_abstractions() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto);
    for row in dataset() {
        storage.upsert_memory(&row).await.unwrap();
    }
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        exocortex_dreams::trigger::DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-abstract".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let first = engine.try_consolidate(&region).await.expect("region cycle");
    assert_eq!(first.abstracted.len(), 1);
    let abstraction = first.abstracted[0];

    // The org-wide cycle sees every project and type — including the
    // General-carried abstraction rows.
    let org_wide = engine
        .try_consolidate(&RegionKey {
            org: "o".into(),
            project: "*".into(),
            memory_type: 3,
        })
        .await
        .expect("org-wide cycle");
    assert!(
        !org_wide.merged.contains(&abstraction),
        "computed rows are never merge targets"
    );
    // The abstraction row itself stays open and un-re-anchored.
    let row = storage
        .get_memory(&abstraction)
        .await
        .unwrap()
        .expect("abstraction row present");
    assert!(row.valid_until.is_none(), "not closed by the wide cycle");
    // Anchors are the five members, not the members + abstraction.
    assert_eq!(org_wide.memories_input, 5, "computed rows are not anchors");
}

/// R9-1's anchor filter, exercised where it actually runs: a
/// GENERAL-typed region's cycle DOES see the General-carried abstraction
/// row in its working set, so the `Provenance::Computed` filter is the
/// only thing keeping the cycle from re-anchoring its own output. (The
/// org-wide test above uses a type-3 region, which filters the row at
/// the query and never reaches this code.)
#[tokio::test]
async fn general_region_cycles_exclude_computed_rows_from_anchors() {
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
        "dreams-general".into(),
    );
    // Produce the abstraction row (General-carried) via a type-3 cycle.
    let typed = engine
        .try_consolidate(&region())
        .await
        .expect("typed cycle");
    assert_eq!(typed.abstracted.len(), 1);
    let abstraction = typed.abstracted[0];

    // One General-typed ASSERTED row joins the region.
    let mut asserted = member(99, [0.1, 0.2, 0.3, 0.4]);
    asserted.memory_type = onto_kind_carrier();
    storage.upsert_memory(&asserted).await.unwrap();

    // The General region's working set now holds the computed
    // abstraction row AND the asserted row — only the asserted row may
    // anchor.
    let general = engine
        .try_consolidate(&RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: onto_kind_carrier(),
        })
        .await
        .expect("general cycle");
    assert_eq!(
        general.memories_input, 1,
        "the computed abstraction row is not an anchor in its own carrier region"
    );
    assert!(
        !general.merged.contains(&abstraction),
        "computed rows are never merge targets"
    );
}

/// R12: cross-domain proposals pair human-asserted rows only — a
/// General-carried abstraction row inherits its members' entities, and
/// machine rows pairing into CrossDomain proposals is evidence-free
/// pairing the finder's own doc disclaims.
#[tokio::test]
async fn cross_domain_proposals_skip_computed_rows() {
    use exocortex_dreams::DiscoveryKind;
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    // Two projects' rows sharing two entities (the finder's floor).
    let mut a = member(1, [0.9, 0.1, 0.0, 0.0]);
    a.memory_type = 3;
    a.context.project_id = Some("p0".into());
    a.context.entities = vec![
        exocortex_kernel::EntityId::from_parts("o", 4, "rust"),
        exocortex_kernel::EntityId::from_parts("o", 4, "falkordb"),
    ]
    .into();
    let mut b = member(2, [0.1, 0.9, 0.0, 0.0]);
    b.memory_type = 3;
    b.context.project_id = Some("p1".into());
    b.context.entities = a.context.entities.clone();
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();

    // A COMPUTED row of the region's own type carrying the same
    // entities (what machine-written rows look like to the finder;
    // abstractions carry the members' entities by construction).
    let mut computed = member(3, [0.5, 0.5, 0.0, 0.0]);
    computed.context.project_id = Some("p2".into());
    computed.context.entities = a.context.entities.clone();
    computed.provenance = Provenance::Computed {
        producer: exocortex_kernel::provenance::ComputedProducer::Abstraction,
        threshold: 0.9,
    };
    storage.upsert_memory(&computed).await.unwrap();

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-xdomain".into(),
    );
    let discoveries = engine
        .run_discovery(&RegionKey {
            org: "o".into(),
            project: "*".into(),
            memory_type: 3,
        })
        .await
        .expect("discovery");
    let cross: Vec<_> = discoveries
        .iter()
        .filter(|d| d.kind == DiscoveryKind::CrossDomain)
        .collect();
    assert!(!cross.is_empty(), "the asserted pair still proposes");
    for discovery in &cross {
        assert!(
            discovery.endpoints.0 != computed.id && discovery.endpoints.1 != computed.id,
            "computed rows never pair: {:?}",
            discovery.endpoints
        );
    }
}
