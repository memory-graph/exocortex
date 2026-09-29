//! D40 / R13-4: Dreams abstraction rows reach the READ surface. The
//! engine's own tests pin abstraction identity, membership, and
//! visibility against STORAGE (`abstractions.rs`); nothing pinned that
//! an agent can actually retrieve the abstraction through
//! `search_memories` / `find_related`. This test runs a real
//! consolidation cycle, serves the committed graph through the ops
//! registry over a LocalCache snapshot (the production read path's
//! handler), and reads the abstraction back the way a session would.

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

/// Mirrors `abstractions.rs`: type-3 members pairwise below the merge
/// bar so the class consolidates into ONE abstraction row.
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
    let c = 0.906_f32;
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

/// D40: after a real consolidation cycle, the abstraction row is
/// retrievable through the read surface an agent uses — searchable by
/// its title with content and a labeled type (S8), and reachable by
/// traversal from a member via the computed `Summarizes` membership.
#[tokio::test]
async fn abstraction_rows_are_readable_through_search_and_traversal() {
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
        "dreams-read-surface".into(),
    );
    let res = engine.try_consolidate(&region()).await.expect("cycle");
    assert_eq!(res.abstracted.len(), 1, "one class abstracted: {res:?}");

    // Serve the committed graph exactly as the read path consumes it:
    // a snapshot published into the local cache, the registry handlers
    // on top (the same entries MCP and HTTP dispatch through).
    let (cache, _writer) = exocortex_cache::LocalCache::new(16 * 1024 * 1024);
    let mut snap = exocortex_cache::GraphSnapshot::empty();
    for row in all_memories(&storage).await {
        snap.push_test_memory(row);
    }
    for edge in all_relationships(&storage).await {
        snap.push_test_relationship(edge);
    }
    cache.publish("o", Arc::new(snap));
    let ctx = exocortex_ops::OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc("o", "reader", Visibility::Org),
        audit_admin: false,
        storage: Arc::new(InMemoryStorage::new(onto.clone())),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: Some(onto.clone()),
        ingest_preflight: None,
        embedding_reindex: None,
    };

    // search: the abstraction is findable by its own title, carrying
    // the payload an agent reads (S8: content + labeled type).
    let entry = exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.search_memories")
        .unwrap();
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::to_value(exocortex_ops::operations::SearchInput {
            query: "Abstraction".into(),
            limit: 10,
        })
        .unwrap(),
    )
    .await
    .unwrap();
    let hits = out["memories"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "one abstraction hit: {out}");
    let hit = &hits[0];
    let abstraction_id = hit["id"].as_str().expect("hex id").to_string();
    assert!(
        hit["title"]
            .as_str()
            .is_some_and(|t| t.starts_with("Abstraction of 4")),
        "the class's own title: {hit}"
    );
    assert_eq!(
        hit["memory_type_label"].as_str(),
        Some("General"),
        "the carrier type is labeled: {hit}"
    );
    assert!(
        hit["content"].as_str().is_some_and(|c| !c.is_empty()),
        "S8: the abstraction carries readable content: {hit}"
    );

    // find_related: anchored on an Org member, the computed Summarizes
    // membership reaches the abstraction row — full rows, not ids.
    let member_id = all_memories(&storage)
        .await
        .into_iter()
        .find(|row| row.title.as_str() == "member 1")
        .expect("org member")
        .id;
    let member_hex = {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(32);
        for b in member_id.0 {
            let _ = write!(out, "{b:02x}");
        }
        out
    };
    let entry = exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.find_related")
        .unwrap();
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::to_value(exocortex_ops::operations::FindRelatedInput {
            anchor: member_hex,
            k: 1,
        })
        .unwrap(),
    )
    .await
    .unwrap();
    let related = out["memories"].as_array().expect("neighborhood");
    let abstraction = related
        .iter()
        .find(|row| row["id"].as_str() == Some(abstraction_id.as_str()))
        .unwrap_or_else(|| panic!("the Summarizes edge reaches the abstraction: {out}"));
    assert_eq!(
        abstraction["memory_type_label"].as_str(),
        Some("General"),
        "traversal rows carry the label too: {out}"
    );
    assert!(
        abstraction["content"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "traversal rows carry content: {out}"
    );
}
