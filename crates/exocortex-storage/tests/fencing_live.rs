// R-C3 fencing-token tests against a live FalkorDB: a write made under a
// lease that is no longer current must be rejected before any row commits;
// the current holder's writes go through. Requires FALKOR_URL (skips
// otherwise) — the docker-compose harness provides the backend.
#![cfg(feature = "integration")]

use chrono::Utc;
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::types::LeaseKey;
use exocortex_storage::{CypherQuery, FalkorStorage, FencedRestore, Storage, StorageError};
use std::sync::Arc;

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
}

fn hex(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(32), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

async fn falkor(tag: &str) -> FalkorStorage {
    let url = falkor_url().expect("FALKOR_URL set (checked by the itest gate)");
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let redis = url.replacen("falkor://", "redis://", 1);
    FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            falkor_url: url,
            redis_url: redis,
            graph_name: format!("fencing_live_{tag}_{}", std::process::id()),
            org_id: "org".into(),
            node_id: "fence-node".into(),
        },
        onto,
    )
    .await
    .expect("connect")
}

fn mem(seed: u8) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: format!("fencing probe {seed}").into(),
        content: "probe".into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "fence".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: Utc::now(),
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
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn rel(from: MemoryId, to: MemoryId) -> Relationship {
    Relationship {
        id: RelationshipId::derive(from, exocortex_kernel::kinds::SOLVES, to, None),
        kind: exocortex_kernel::kinds::SOLVES,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "fence".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    }
}

#[tokio::test]
async fn live_cycle_journal_survives_reconnect_and_accumulates_owned_lsns() {
    let s = falkor("cycle_journal").await;
    let original = mem(41);
    s.upsert_memory(&original).await.unwrap();
    let key = LeaseKey::Dreams {
        org: "org".into(),
        region: "journal".into(),
    };
    let lease = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let prepared = FencedRestore {
        memories: vec![original.clone()],
        ..FencedRestore::default()
    };
    let mut changed = original.clone();
    changed.title = "journaled".into();
    let committed = s
        .upsert_batch_fenced_journaled(&[changed], &[], &prepared, "cycle-live", &lease)
        .await
        .unwrap();

    let recovered = s
        .get_active_cycle_journal(&key)
        .await
        .unwrap()
        .expect("active journal is durable in Falkor");
    assert!(recovered.restore.owned_memory_lsns[&original.id].contains(&committed.records[0].lsn));
    s.restore_fenced(&recovered.restore, &lease).await.unwrap();
    s.complete_cycle_journal_fenced("cycle-live", &lease)
        .await
        .unwrap();
    assert!(s.get_active_cycle_journal(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_lease_write_is_fenced_live() {
    if falkor_url().is_none() {
        eprintln!("skipping stale_lease_write_is_fenced_live: FALKOR_URL not set");
        return;
    }
    let s = falkor("stale").await;
    let key = LeaseKey::Consolidation {
        org: "org".into(),
        region: "*".into(),
    };
    let old = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    // Re-election: release, then a new owner acquires (epoch bumps).
    s.release_lease(old.clone()).await.unwrap();
    let new = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(new.epoch > old.epoch, "epoch must be monotonic");

    // The stale owner attempts its consolidation write.
    let m = mem(1);
    let err = s.upsert_batch_fenced(&[m.clone()], &[], &old).await;
    assert!(
        matches!(err, Err(StorageError::FencedWriteRejected { lease_epoch }) if lease_epoch == old.epoch),
        "stale write must be fenced, got {err:?}"
    );
    assert!(
        s.get_memory(&m.id).await.unwrap().is_none(),
        "no row may land from a fenced write"
    );

    // The current holder writes fine.
    s.upsert_batch_fenced(&[mem(2)], &[], &new).await.unwrap();
    s.release_lease(new).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_lease_write_is_fenced_live() {
    if falkor_url().is_none() {
        eprintln!("skipping expired_lease_write_is_fenced_live: FALKOR_URL not set");
        return;
    }
    let s = falkor("expired").await;
    let key = LeaseKey::Backfill { org: "org".into() };
    let lease = s
        .acquire_lease(&key, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let err = s.upsert_batch_fenced(&[mem(3)], &[], &lease).await;
    assert!(matches!(err, Err(StorageError::FencedWriteRejected { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn held_lease_blocks_second_holder_live() {
    if falkor_url().is_none() {
        eprintln!("skipping held_lease_blocks_second_holder_live: FALKOR_URL not set");
        return;
    }
    let s = falkor("held").await;
    let key = LeaseKey::Cleanup { org: "org".into() };
    let a = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        s.acquire_lease(&key, std::time::Duration::from_secs(60))
            .await
            .is_err(),
        "the graph lease must refuse a second holder while the TTL lives"
    );
    // Renewal under the matching token succeeds; a mutated token fails.
    assert!(s.renew_lease(&a).await.is_ok());
    let mut forged = a.clone();
    forged.fencing_token = "someone-else:99".into();
    assert!(s.renew_lease(&forged).await.is_err());
    s.release_lease(a).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn later_graph_row_failure_rolls_back_earlier_rows_live() {
    if falkor_url().is_none() {
        eprintln!(
            "skipping later_graph_row_failure_rolls_back_earlier_rows_live: FALKOR_URL not set"
        );
        return;
    }
    let s = falkor("rollback").await;
    let staged = mem(4);
    let missing = MemoryId::new_v7();
    let err = s
        .upsert_batch(&[staged.clone()], &[rel(staged.id, missing)])
        .await;
    assert!(err.is_err(), "missing endpoint must reject the batch");
    assert!(
        s.get_memory(&staged.id).await.unwrap().is_none(),
        "the single modifying GRAPH.QUERY rolls every row back"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fenced_restore_is_one_live_atomic_preimage_swap() {
    if falkor_url().is_none() {
        eprintln!("skipping fenced_restore_is_one_live_atomic_preimage_swap: FALKOR_URL not set");
        return;
    }
    use futures::StreamExt;
    let s = falkor("restore").await;
    let mut preclosed = mem(20);
    preclosed.valid_until = Some(Utc::now());
    let existing = mem(21);
    let target = mem(22);
    let mut preclosed_edge = rel(preclosed.id, target.id);
    preclosed_edge.valid_until = preclosed.valid_until;
    let existing_edge = rel(existing.id, target.id);
    s.upsert_batch(
        &[preclosed.clone(), existing.clone(), target.clone()],
        &[preclosed_edge.clone(), existing_edge.clone()],
    )
    .await
    .unwrap();
    let lease = s
        .acquire_lease(
            &LeaseKey::Dreams {
                org: "org".into(),
                region: "*:*".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    let mut reopened = preclosed.clone();
    reopened.valid_until = None;
    let mut changed = existing.clone();
    changed.title = "changed".into();
    let mut changed_edge = existing_edge.clone();
    changed_edge.properties.evidence_count += 4;
    let created = mem(23);
    let mut created_edge = rel(created.id, target.id);
    created_edge.kind = exocortex_kernel::RelKindId(0x8000_0024);
    created_edge.id =
        RelationshipId::derive(created_edge.from, created_edge.kind, created_edge.to, None);
    let ontology = exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap();
    let mut created_relationships = vec![created_edge.clone()];
    if let Some(inverse) = exocortex_kernel::materialize_inverse(&ontology, &created_edge) {
        created_relationships.push(inverse);
    }
    let cycle_commit = s
        .upsert_batch_fenced(
            &[reopened, changed, created.clone()],
            &[changed_edge, created_edge.clone()],
            &lease,
        )
        .await
        .unwrap();
    s.restore_fenced(
        &FencedRestore {
            memories: vec![preclosed.clone(), existing.clone()],
            relationships: vec![preclosed_edge.clone(), existing_edge.clone()],
            created_memories: vec![created.clone()],
            created_relationships,
            owned_memory_lsns: cycle_commit.memory_lsns,
            owned_relationship_lsns: cycle_commit.relationship_lsns,
        },
        &lease,
    )
    .await
    .unwrap();
    assert_eq!(
        s.get_memory(&preclosed.id)
            .await
            .unwrap()
            .unwrap()
            .valid_until,
        preclosed.valid_until
    );
    assert_eq!(
        s.get_memory(&existing.id).await.unwrap().unwrap().title,
        existing.title
    );
    assert!(s.get_memory(&created.id).await.unwrap().is_none());
    let mut relationships = s.stream_all_relationships().await;
    let mut by_id = std::collections::HashMap::new();
    while let Some(row) = relationships.next().await {
        let relationship = row.unwrap();
        by_id.insert(relationship.id, relationship);
    }
    assert_eq!(
        by_id[&preclosed_edge.id].valid_until,
        preclosed_edge.valid_until
    );
    assert_eq!(
        by_id[&existing_edge.id].properties.evidence_count,
        existing_edge.properties.evidence_count
    );
    assert!(!by_id.contains_key(&created_edge.id));
    let state = s
        .get_state_at(Utc::now() + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(state.memory_count, 2, "only existing and target are live");
    assert_eq!(
        state.relationship_count, 2,
        "only existing edge and its required inverse are live"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fenced_restore_preserves_live_concurrent_versions() {
    if falkor_url().is_none() {
        eprintln!("skipping fenced_restore_preserves_live_concurrent_versions: FALKOR_URL not set");
        return;
    }
    use futures::StreamExt;
    let s = falkor("conditional_restore").await;
    let original = mem(30);
    let target = mem(31);
    let original_edge = rel(original.id, target.id);
    s.upsert_batch(
        &[original.clone(), target.clone()],
        &[original_edge.clone()],
    )
    .await
    .unwrap();
    let lease = s
        .acquire_lease(
            &LeaseKey::Dreams {
                org: "org".into(),
                region: "*:*".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    let mut cycle_memory = original.clone();
    cycle_memory.title = "cycle".into();
    let mut cycle_edge = original_edge.clone();
    cycle_edge.properties.evidence_count = 2;
    let mut cycle_commit = s
        .upsert_batch_fenced(&[cycle_memory], &[cycle_edge], &lease)
        .await
        .unwrap();

    let mut concurrent_memory = original.clone();
    concurrent_memory.title = "concurrent".into();
    let mut concurrent_edge = original_edge.clone();
    concurrent_edge.properties.evidence_count = 99;
    s.upsert_batch(&[concurrent_memory], &[concurrent_edge])
        .await
        .unwrap();
    // Model a still-running pre-history node: its current-row overwrite has a
    // valid LSN but no matching assertion row. The next new-node write must
    // preserve that outgoing current value atomically before replacing it.
    s.query_cypher(&CypherQuery {
        template_id: "integration_remove_current_memory_assertion",
        params: serde_json::json!({ "id": hex(&original.id.0) }),
        read_only: false,
        deadline: Utc::now() + chrono::Duration::seconds(5),
    })
    .await
    .unwrap();
    s.query_cypher(&CypherQuery {
        template_id: "integration_remove_current_relationship_assertion",
        params: serde_json::json!({ "rel_id": hex(&original_edge.id.0) }),
        read_only: false,
        deadline: Utc::now() + chrono::Duration::seconds(5),
    })
    .await
    .unwrap();
    let mut later_cycle_memory = original.clone();
    later_cycle_memory.title = "later-cycle".into();
    let mut later_cycle_edge = original_edge.clone();
    later_cycle_edge.properties.evidence_count = 3;
    let later_commit = s
        .upsert_batch_fenced(&[later_cycle_memory], &[later_cycle_edge], &lease)
        .await
        .unwrap();
    for (id, lsns) in later_commit.memory_lsns {
        cycle_commit.memory_lsns.entry(id).or_default().extend(lsns);
    }
    for (id, lsns) in later_commit.relationship_lsns {
        cycle_commit
            .relationship_lsns
            .entry(id)
            .or_default()
            .extend(lsns);
    }
    s.restore_fenced(
        &FencedRestore {
            memories: vec![original.clone()],
            relationships: vec![original_edge.clone()],
            owned_memory_lsns: cycle_commit.memory_lsns,
            owned_relationship_lsns: cycle_commit.relationship_lsns,
            ..Default::default()
        },
        &lease,
    )
    .await
    .unwrap();

    assert_eq!(
        s.get_memory(&original.id).await.unwrap().unwrap().title,
        "concurrent"
    );
    let mut relationships = s.stream_all_relationships().await;
    let mut found = None;
    while let Some(row) = relationships.next().await {
        let relationship = row.unwrap();
        if relationship.id == original_edge.id {
            found = Some(relationship);
        }
    }
    assert_eq!(found.unwrap().properties.evidence_count, 99);
}
