//! M5 cluster tests (§9.8): HMAC envelope signing/verification, peer
//! admission (wire version, ontology fingerprint, HMAC), lease races, and
//! the split-brain fencing semantics of the epoch counter.

use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::{ClusterError, ClusterNode};
use exocortex_kernel::{MemoryId, OntologyFingerprint};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Invalidation, LeaseKey, Storage};
use exocortex_wire::WIRE_VERSION;

fn ontology_fp() -> OntologyFingerprint {
    exocortex_kernel::Ontology::from_packs(vec![pack_def()])
        .unwrap()
        .fingerprint
}

fn node(storage: &InMemoryStorage, id: &str, key: [u8; 32]) -> ClusterNode<InMemoryStorage> {
    ClusterNode::new(Arc::new(storage.clone_dyn()), id.into(), ontology_fp(), key)
}

#[test]
fn envelope_hmac_verifies_and_rejects_tampering() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let n = node(&storage, "node-1", [7u8; 32]);
    let env = n.envelope(Invalidation::MemoryUpserted {
        id: MemoryId::new_v7(),
        lsn: 42,
    });
    assert!(n.verify_hmac(&env).is_ok());
    assert!(n.admit(&env).is_ok(), "self-consistent envelope admitted");

    let mut tampered = env.clone();
    tampered.inv.as_mut().unwrap().backend_lsn = 43;
    assert!(
        matches!(n.admit(&tampered), Err(ClusterError::HmacFailed)),
        "tampered payload fails HMAC (R-W4)"
    );
}

#[test]
fn peer_admission_rejects_wire_version_mismatch() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let n = node(&storage, "node-1", [7u8; 32]);
    let mut env = n.envelope(Invalidation::MemoryDeleted {
        id: MemoryId::new_v7(),
        lsn: 7,
    });
    env.wire_version = WIRE_VERSION + 1;
    let err = n.admit(&env).unwrap_err();
    assert!(matches!(err, ClusterError::WireMismatch), "{err:?}");
}

#[test]
fn peer_admission_rejects_ontology_mismatch_and_names_it() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let n = node(&storage, "node-1", [7u8; 32]);
    let mut env = n.envelope(Invalidation::RelationshipDeleted {
        id: exocortex_kernel::RelationshipId([1; 16]),
        lsn: 1,
    });
    env.ontology_fingerprint = [9u8; 32].to_vec();
    let err = n.admit(&env).unwrap_err();
    assert!(matches!(err, ClusterError::OntologyMismatch));
    assert!(
        err.to_string().contains("ontology mismatch"),
        "the error names the mismatch (CR-18)"
    );
}

#[test]
fn wrong_cluster_key_rejects() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let a = node(&storage, "node-1", [1u8; 32]);
    let b = node(&storage, "node-2", [2u8; 32]);
    let env = a.envelope(Invalidation::MemoryUpserted {
        id: MemoryId::new_v7(),
        lsn: 1,
    });
    assert!(matches!(b.admit(&env), Err(ClusterError::HmacFailed)));
}

#[tokio::test]
async fn lease_race_and_epoch_fencing_against_live_falkordb() {
    // The live lease race runs when the docker harness is up; against the
    // in-memory double the lease semantics are trivially permissive, so the
    // race assertions are gated on FALKOR_URL.
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let a = node(&storage, "node-A", [7u8; 32]);
    let key = LeaseKey::Dreams {
        org: "t".into(),
        region: "p:1".into(),
    };
    let lease = a
        .acquire(key.clone(), Duration::from_secs(30))
        .await
        .expect("lease");
    assert_eq!(lease.owner_node_id, "in-memory");
    assert!(lease.epoch >= 1, "fencing epoch present");
    if std::env::var("FALKOR_URL").is_err() {
        eprintln!("skipping live lease race: FALKOR_URL not set");
        return;
    }
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let url = std::env::var("FALKOR_URL").unwrap();
    let falkor_a = exocortex_storage::FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            falkor_url: url.clone(),
            redis_url: url.replacen("falkor://", "redis://", 1),
            graph_name: format!("cluster_chaos_{}", std::process::id()),
            org_id: "chaos".into(),
            node_id: "chaos-A".into(),
        },
        onto.clone(),
    )
    .await
    .unwrap();
    let falkor_b = exocortex_storage::FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            falkor_url: url.clone(),
            redis_url: url.replacen("falkor://", "redis://", 1),
            graph_name: format!("cluster_chaos_{}", std::process::id()),
            org_id: "chaos".into(),
            node_id: "chaos-B".into(),
        },
        onto,
    )
    .await
    .unwrap();

    let race_key = LeaseKey::Dreams {
        org: format!("race-{}", std::process::id()).into(),
        region: "p:1".into(),
    };
    let first = falkor_a
        .acquire_lease(&race_key, Duration::from_secs(5))
        .await
        .expect("A acquires");
    let loser = falkor_b
        .acquire_lease(&race_key, Duration::from_secs(5))
        .await;
    assert!(loser.is_err(), "exactly one lease holder (R-C1)");

    // Chaos: "kill" A (release), then B must acquire with a NEW epoch —
    // A's stale writes are fenced by the epoch change (R-C3).
    falkor_a.release_lease(first.clone()).await.unwrap();
    let second = falkor_b
        .acquire_lease(&race_key, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        second.epoch > first.epoch,
        "epoch fencing increments across owners: {} -> {}",
        first.epoch,
        second.epoch
    );
    // The old lease's fencing token no longer matches: renewal of the dead
    // lease must fail (zombie writes cannot commit).
    assert!(
        falkor_a.renew_lease(&first).await.is_err(),
        "split-brain: the partitioned old owner cannot renew after fencing"
    );
}

#[tokio::test]
async fn sse_hub_fans_out_signed_envelopes() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    ));
    let n = Arc::new(node(&storage, "node-1", [7u8; 32]));
    let mut rx = n.subscribe_local();
    let hub = n.clone();
    tokio::spawn(async move { hub.run().await });
    // InMemoryStorage's change feed is empty (no redis); drive the hub by
    // signing + broadcasting directly — the SSE path is envelope-driven.
    let env = n.envelope(Invalidation::MemoryUpserted {
        id: MemoryId::new_v7(),
        lsn: 5,
    });
    let _ = n.tx.send(env.clone());
    let got = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("envelope within 2s")
        .expect("channel live");
    assert_eq!(got.hmac, env.hmac);
    assert!(n.admit(&got).is_ok());
}
