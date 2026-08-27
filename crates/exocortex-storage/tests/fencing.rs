// R-C3 fencing-token tests against the InMemory double: re-election bumps
// the epoch and stale-owner writes are rejected before any row commits.
use chrono::Utc;
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::types::LeaseKey;
use exocortex_storage::{InMemoryStorage, Storage, StorageError};
use std::sync::Arc;

fn store() -> InMemoryStorage {
    InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ))
}

fn mem(seed: u8) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 0, // Note (dev-v1 bucket order); fencing is type-agnostic
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
async fn stale_lease_write_is_fenced() {
    let s = store();
    let key = LeaseKey::Dreams {
        org: "org".into(),
        region: "*:*".into(),
    };
    let old = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    // The old holder loses the lease (release + re-election, epoch bumps).
    s.release_lease(old.clone()).await.unwrap();
    let new = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(new.epoch > old.epoch, "epoch must be monotonic");

    // Zombie write under the stale lease: rejected, nothing commits.
    let m = mem(1);
    let err = s.upsert_batch_fenced(&[m.clone()], &[], &old).await;
    assert!(
        matches!(err, Err(StorageError::FencedWriteRejected { lease_epoch }) if lease_epoch == old.epoch),
        "stale write must be fenced, got {err:?}"
    );
    assert!(s.get_memory(&m.id).await.unwrap().is_none());

    // Current holder commits fine.
    s.upsert_batch_fenced(&[mem(2)], &[], &new).await.unwrap();
}

#[tokio::test]
async fn held_lease_blocks_second_acquire_and_fenced_delete() {
    let s = store();
    let key = LeaseKey::Cleanup { org: "org".into() };
    let a = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        s.acquire_lease(&key, std::time::Duration::from_secs(60))
            .await
            .is_err(),
        "a held lease must not be grantable twice"
    );
    let m = mem(3);
    s.upsert_memory(&m).await.unwrap();
    // Stale delete (lease released under us) is fenced too.
    s.release_lease(a.clone()).await.unwrap();
    let b = s
        .acquire_lease(&key, std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(matches!(
        s.delete_memory_fenced(&m.id, &a).await,
        Err(StorageError::FencedWriteRejected { .. })
    ));
    s.delete_memory_fenced(&m.id, &b).await.unwrap();
}

#[tokio::test]
async fn batch_row_failure_rolls_back_every_memory_and_lsn() {
    let s = store();
    let before = s.last_lsn();
    let staged = mem(4);
    let missing = MemoryId::new_v7();

    let err = s
        .upsert_batch(&[staged.clone()], &[rel(staged.id, missing)])
        .await;
    assert!(err.is_err(), "missing endpoint must reject the batch");
    assert!(
        s.get_memory(&staged.id).await.unwrap().is_none(),
        "a later-row failure rolls the earlier memory back"
    );
    assert_eq!(s.last_lsn(), before, "a rejected batch allocates no LSN");
}
