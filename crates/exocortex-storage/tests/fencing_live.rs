// R-C3 fencing-token tests against a live FalkorDB: a write made under a
// lease that is no longer current must be rejected before any row commits;
// the current holder's writes go through. Requires FALKOR_URL (skips
// otherwise) — the docker-compose harness provides the backend.
#![cfg(feature = "integration")]

use chrono::Utc;
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::types::LeaseKey;
use exocortex_storage::{FalkorStorage, Storage, StorageError};
use std::sync::Arc;

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
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
        "SET NX must refuse a second holder while the TTL lives"
    );
    // Renewal under the matching token succeeds; a mutated token fails.
    assert!(s.renew_lease(&a).await.is_ok());
    let mut forged = a.clone();
    forged.fencing_token = "someone-else:99".into();
    assert!(s.renew_lease(&forged).await.is_err());
    s.release_lease(a).await.unwrap();
}
