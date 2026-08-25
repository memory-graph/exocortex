//! B8/B9 verification (docs/bug-prd-external-key-identity.md): raw-byte
//! external identity and snapshot schema-hash provenance round-trip
//! through the real submit path, and malformed widths are rejected.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_kernel::{MemoryId, Provenance};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalKey, ExternalSnapshotInfo, IngestBatch,
    MemoryDraft, ProducerIdentity, RegisterSourceRequest, RejectCode,
};

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
}

fn draft(key: &str, title: &str) -> MemoryDraft {
    MemoryDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: "General".into(),
        title: title.into(),
        content: "external row".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: Some(ExternalKey {
            table_uuid: [
                0x8fu8, 0x3a, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe,
                0xff, 0xfe, 0xff,
            ]
            .to_vec(),
            logical_pk: key.into(),
            mapping_version: 1,
        }),
    }
}

fn signed(mut b: IngestBatch) -> IngestBatch {
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    b
}

fn batch(snapshot: &ExternalSnapshotInfo, memories: Vec<MemoryDraft>) -> IngestBatch {
    let fp = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    signed(IngestBatch {
        org_id: "org".into(),
        source_uri: "iceberg://cat/db/orders".into(),
        producer_id: "external-sync".into(),
        batch_id: format!("b8b9-{}", std::process::id()),
        mapping_version: "orders:1.0.0".into(),
        ontology_fingerprint: fp.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: Some(snapshot.clone()),
        memories,
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
        }),
    })
}

async fn registered(srv: &IngestServer<InMemoryStorage>) {
    srv.register_source(tonic::Request::new(RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: "iceberg://cat/db/orders".into(),
        producer_id: "external-sync".into(),
        ceiling: 3,
        source_flavor: "external".into(),
    }))
    .await
    .unwrap();
}

fn snap(schema_hash: Vec<u8>) -> ExternalSnapshotInfo {
    ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash,
        source_flavor: "custom".into(),
    }
}

/// B9: the submitted snapshot schema_hash round-trips into the committed
/// row's provenance — non-zero, byte-exact.
#[tokio::test]
async fn schema_hash_round_trips_through_commit() {
    let srv = server();
    registered(&srv).await;
    let mut hash = [0u8; 32];
    for (i, b) in hash.iter_mut().enumerate() {
        *b = (0xa3 + i) as u8;
    }
    let b = batch(
        &snap(hash.to_vec()),
        vec![draft("order-7", "payments owned by team-payments")],
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "{:?}", ack.rejections);

    let committed = {
        use futures::StreamExt;
        let storage = srv.storage.clone();
        let mut ms = storage.stream_all_memories().await;
        let mut found = None;
        while let Some(Ok(m)) = ms.next().await {
            if m.title.contains("payments owned") {
                found = Some(m);
            }
        }
        found.expect("committed row")
    };
    match committed.provenance {
        Provenance::ExternalSnapshot(ext) => {
            assert_eq!(
                ext.schema_hash, hash,
                "B9: submitted schema_hash persists byte-exact"
            );
            assert!(
                ext.schema_hash != [0u8; 32],
                "the pre-B9 hard-coded zeros are gone"
            );
        }
        other => panic!("external batch must commit ExternalSnapshot: {other:?}"),
    }
}

/// B8: the committed row's id equals raw-byte derivation — the lossy
/// string path would have hashed a different input.
#[tokio::test]
async fn identity_derives_from_raw_uuid_bytes() {
    let srv = server();
    registered(&srv).await;
    let uuid = [
        0x8fu8, 0x3a, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe,
        0xff,
    ];
    let b = batch(
        &snap([0u8; 32].to_vec()),
        vec![draft("order-8", "raw uuid row")],
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "{:?}", ack.rejections);

    let expected = MemoryId::from_external("org", "iceberg://cat/db/orders", &uuid, b"order-8", 1);
    let storage = srv.storage.clone();
    assert!(
        storage.get_memory(&expected).await.unwrap().is_some(),
        "committed row lives at the raw-byte-derived id"
    );

    // And the lossy path's id does NOT hold the row (it would pre-B8).
    let lossy = String::from_utf8_lossy(&uuid).to_string();
    let lossy_id = MemoryId::from_external(
        "org",
        "iceberg://cat/db/orders",
        lossy.as_bytes(),
        b"order-8",
        1,
    );
    assert!(
        storage.get_memory(&lossy_id).await.unwrap().is_none(),
        "no row at the lossy-string-derived id"
    );
}

/// B8/B9 widths: malformed table_uuid (≠16B) and schema_hash (≠32B) are
/// rejected with INVALID_EXTERNAL_KEY, not coerced.
#[tokio::test]
async fn malformed_external_coordinates_are_rejected() {
    let srv = server();
    registered(&srv).await;

    // table_uuid too short.
    let mut b = batch(&snap([0u8; 32].to_vec()), vec![draft("k", "short uuid")]);
    if let Some(d) = b.memories.get_mut(0) {
        if let Some(k) = d.external_key.as_mut() {
            k.table_uuid = vec![1u8; 8];
        }
    }
    let ack = srv
        .submit(tonic::Request::new(re_sign(b)))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::InvalidExternalKey as i32),
        "short table_uuid rejected: {:?}",
        ack.rejections
    );

    // schema_hash wrong length.
    let b2 = batch(&snap(vec![1u8; 31]), vec![draft("k2", "short hash")]);
    let ack2 = srv
        .submit(tonic::Request::new(b2))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack2.rejections
            .iter()
            .any(|r| r.code == RejectCode::InvalidExternalKey as i32),
        "short schema_hash rejected: {:?}",
        ack2.rejections
    );
}

fn re_sign(mut b: IngestBatch) -> IngestBatch {
    if let Some(p) = b.producer.as_mut() {
        p.hmac_signature = vec![];
    }
    signed(b)
}

/// R5: the checksum is verified server-side — a corrupted checksum and an
/// empty checksum are both `BadChecksum`, never a bypass.
#[tokio::test]
async fn bad_checksum_is_rejected() {
    let srv = server();
    registered(&srv).await;

    // Valid batch, corrupt the checksum, then sign WITHOUT recomputing it
    // (sign_batch covers the checksum field; prepare_batch would fix it).
    let mut b = batch(
        &snap([0u8; 32].to_vec()),
        vec![draft("order-9", "corrupt checksum row")],
    );
    b.checksum = "deadbeef".into();
    exocortex_wire::signing::sign_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::BadChecksum as i32),
        "corrupted checksum rejected: {:?}",
        ack.rejections
    );

    // Empty checksum: also a mismatch (canonical checksum is never empty).
    let mut b2 = batch(
        &snap([0u8; 32].to_vec()),
        vec![draft("order-10", "empty checksum row")],
    );
    b2.checksum = String::new();
    exocortex_wire::signing::sign_batch(&[5u8; 32], &mut b2);
    let ack2 = srv
        .submit(tonic::Request::new(b2))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack2.rejections
            .iter()
            .any(|r| r.code == RejectCode::BadChecksum as i32),
        "empty checksum rejected: {:?}",
        ack2.rejections
    );
}

/// Round-3 C4: a pinned-org node rejects foreign-org batches and source
/// registrations outright — cross-org writes can neither commit nor
/// publish invalidations.
#[tokio::test]
async fn pinned_org_rejects_foreign_batches() {
    let srv = server().with_org("org");
    registered(&srv).await;

    // Foreign org batch: validly signed, same source — rejected.
    let mut b = batch(
        &snap([0u8; 32].to_vec()),
        vec![draft("order-x", "foreign org row")],
    );
    b.org_id = "evil-org".into();
    let ack = srv
        .submit(tonic::Request::new(re_sign(b)))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::UnknownSource as i32),
        "foreign org rejected: {:?}",
        ack.rejections
    );

    // Foreign registration errors.
    let err = srv
        .register_source(tonic::Request::new(RegisterSourceRequest {
            org_id: "evil-org".into(),
            source_uri: "custom://x".into(),
            producer_id: "p".into(),
            ceiling: 3,
            source_flavor: "custom".into(),
        }))
        .await;
    assert!(err.is_err(), "foreign org registration rejected");

    // And the row never landed.
    let committed = {
        use futures::StreamExt;
        let storage = srv.storage.clone();
        let mut ms = storage.stream_all_memories().await;
        let mut any = false;
        while let Some(Ok(m)) = ms.next().await {
            if m.title.contains("foreign org") {
                any = true;
            }
        }
        any
    };
    assert!(!committed, "no cross-org row committed");
}
