// R-C3 fencing-token tests: a write made under a lease that is no longer
// current must be rejected before any row commits; the current holder's
// writes go through.
#![cfg(feature = "integration")]

use exocortex_kernel::{Memory, MemoryDraft, MemoryType, Ontology, Visibility};
use exocortex_storage::types::LeaseKey;
use exocortex_storage::{FalkorStorage, Storage, StorageError};

fn falkor() -> FalkorStorage {
    let url = std::env::var("FALKOR_URL").unwrap_or_else(|_| {
        eprintln!("skipping: FALKOR_URL not set");
        std::process::exit(0);
    });
    let onto: Ontology = exocortex_pack_dev_v1::dev_v1();
    FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            falkor_url: format!("falkor://{url}"),
            redis_url: format!("redis://{url}"),
            graph_name: format!("fencing-test-{}", std::process::id()),
            org_id: "org".into(),
            node_id: "fence-node".into(),
        },
        std::sync::Arc::new(onto),
    )
    .expect("connect")
}

fn mem(id_seed: u8) -> Memory {
    Memory::from_draft(
        &MemoryDraft {
            memory_type: MemoryType::Note,
            title: format!("fencing probe {id_seed}"),
            content: "probe".into(),
            visibility: Visibility::Org,
            ..Default::default()
        },
        &Ontology::default(),
    )
    .expect("draft")
}

#[tokio::test]
async fn stale_lease_write_is_fenced() {
    let s = falkor();
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

#[tokio::test]
async fn expired_lease_write_is_fenced() {
    let s = falkor();
    let key = LeaseKey::Backfill { org: "org".into() };
    let ttl_0 = std::time::Duration::from_millis(1);
    let lease = s.acquire_lease(&key, ttl_0).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let err = s.upsert_batch_fenced(&[mem(3)], &[], &lease).await;
    assert!(matches!(err, Err(StorageError::FencedWriteRejected { .. })));
}
