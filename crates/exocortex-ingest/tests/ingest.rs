//! M6 ingest tests (§18.8): HMAC-first rejection, the §7.13 pipeline order
//! (fingerprint, source admission, no-widening, triples, idempotency),
//! entity extraction, and end-to-end submit against `InMemoryStorage`.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_kernel::Visibility;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalKey, IngestBatch, MemoryDraft, ProducerIdentity,
    RegisterSourceRequest, RejectCode,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
}

fn draft(key: &str, mt: &str, vis: i32) -> MemoryDraft {
    MemoryDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: mt.into(),
        title: format!("title {key}"),
        content: format!("Fixed in src/auth.rs with cargo build"),
        tags: vec!["auth".into()],
        visibility: vis,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn signed_batch(srv: &IngestServer<InMemoryStorage>, memories: Vec<MemoryDraft>) -> IngestBatch {
    let mut b = batch(memories);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    sign(b, [5u8; 32])
}

fn batch(memories: Vec<MemoryDraft>) -> IngestBatch {
    IngestBatch {
        org_id: "org".into(),
        source_uri: "session://s1".into(),
        producer_id: "session-wrapup".into(),
        batch_id: format!("b-{}", std::process::id()),
        mapping_version: "session-wrapup:1.0.0".into(),
        ontology_fingerprint: Vec::new(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories,
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: "agent".into(),
            adapter_id: String::new(),
            hmac_signature: vec![],
        }),
    }
}

fn sign(mut b: IngestBatch, key: [u8; 32]) -> IngestBatch {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key).unwrap();
    mac.update(&prost::Message::encode_to_vec(&b));
    if let Some(p) = b.producer.as_mut() {
        p.hmac_signature = mac.finalize().into_bytes().to_vec();
    }
    b
}

async fn registered(srv: &IngestServer<InMemoryStorage>, ceiling: i32) {
    use tonic::Request;
    srv.register_source(Request::new(RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: "session://s1".into(),
        producer_id: "session-wrapup".into(),
        ceiling,
        source_flavor: "session".into(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn e2e_valid_batch_accepted_with_monotonic_lsn() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(
        &srv,
        vec![
            draft("k1", "Fix", 1),
            draft("k2", "Problem", 2),
            draft("k3", "Solution", 3),
        ],
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    if ack.accepted != 3 {
        panic!("rejections: {:#?}", ack.rejections);
    }
    assert!(ack.assigned_lsn > 0);
}

#[tokio::test]
async fn missing_hmac_rejected_before_anything() {
    let srv = server();
    registered(&srv, 3).await;
    let mut b = batch(vec![draft("k", "Fix", 1)]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    // Not signed.
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::Unauthorized as i32),
        "R-I8: HMAC first"
    );
}

#[tokio::test]
async fn fingerprint_mismatch_rejects_whole_batch() {
    let srv = server();
    registered(&srv, 3).await;
    // Sign with the wrong fingerprint already in place so the HMAC is
    // valid and the rejection isolates the fingerprint gate.
    let mut b = batch(vec![draft("k", "Fix", 1)]);
    b.ontology_fingerprint = vec![1, 2, 3];
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::IncompatibleOntology as i32));
}

#[tokio::test]
async fn unregistered_source_rejected() {
    let srv = server();
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::UnknownSource as i32));
}

#[tokio::test]
async fn visibility_widening_rejected_under_lowered_ceiling() {
    let srv = server();
    registered(&srv, 1).await; // Project ceiling
                               // Team visibility under a Project ceiling. The batch ceiling must
                               // equal the registered value (R-I3) so the rejection is the widening,
                               // not the source mismatch.
    let mut b = batch(vec![draft("k", "Fix", 2)]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.ceiling = 1;
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::VisibilityWidening as i32),
        "R-T11a: Team under a Project ceiling"
    );
}

#[tokio::test]
async fn unknown_memory_type_rejected() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "NotAType", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::UnknownMemoryType as i32));
}

#[tokio::test]
async fn external_batch_without_key_rejected_and_with_key_deterministic() {
    let srv = server();
    registered(&srv, 3).await;
    let mut d = draft("k", "Fix", 1);
    d.external_key = None;
    let mut b = batch(vec![d]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash: vec![0; 32],
        source_flavor: "iceberg".into(),
    });
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::MissingExternalKey as i32));

    // With the key: identity is deterministic (R-T18a).
    let mut d2 = draft("k", "Fix", 1);
    d2.external_key = Some(ExternalKey {
        table_uuid: vec![1; 16],
        logical_pk: "row-1".into(),
        mapping_version: 3,
    });
    let mut b2 = batch(vec![d2]);
    b2.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b2.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash: vec![0; 32],
        source_flavor: "iceberg".into(),
    });
    let b2 = sign(b2, [5u8; 32]);
    let ack2 = srv
        .submit(tonic::Request::new(b2))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack2.accepted, 1);

    let id_a = exocortex_kernel::MemoryId::from_external(
        "org",
        "session://s1",
        &String::from_utf8_lossy(&[1u8; 16]),
        b"row-1",
        3,
    );
    let id_b = exocortex_kernel::MemoryId::from_external(
        "org",
        "session://s1",
        &String::from_utf8_lossy(&[1u8; 16]),
        b"row-1",
        3,
    );
    assert_eq!(id_a, id_b, "deterministic identity");
    let id_c = exocortex_kernel::MemoryId::from_external(
        "org",
        "session://s1",
        &String::from_utf8_lossy(&[1u8; 16]),
        b"row-1",
        4,
    );
    assert_ne!(id_a, id_c, "mapping_version bump forks identity");
}

#[tokio::test]
async fn duplicate_batch_is_idempotent_replay() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let first = srv
        .submit(tonic::Request::new(b.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.accepted, 1);
    let second = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        second
            .rejections
            .iter()
            .any(|r| r.code == RejectCode::DuplicateBatch as i32),
        "replay short-circuits"
    );
}

#[tokio::test]
async fn entities_extracted_server_side() {
    let ex = exocortex_ingest::EntityExtractor::new("org");
    let ids = ex.entity_ids(
        "Fixed src/auth.rs via cargo build; see https://example.com/docs and tokio 1.40@1.40.1",
        &[],
    );
    assert!(!ids.is_empty(), "entities extracted from content");
    // Deterministic: same input -> same ids.
    let again = ex.entity_ids(
        "Fixed src/auth.rs via cargo build; see https://example.com/docs and tokio 1.40@1.40.1",
        &[],
    );
    assert_eq!(ids, again);
    assert!(exocortex_ingest::entities::table_is_complete());
}

#[tokio::test]
async fn stored_memories_carry_extracted_entities() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1);

    let storage = &srv.storage;
    let mut n = 0;
    use futures::StreamExt;
    let mut ms = storage.stream_all_memories().await;
    while let Some(Ok(m)) = ms.next().await {
        n += 1;
        assert!(
            !m.context.entities.is_empty(),
            "R-T18: backend extracted entities (content mentions src/auth.rs)"
        );
        assert_eq!(
            m.provenance,
            exocortex_kernel::Provenance::Asserted {
                author: "session-wrapup".into()
            }
        );
    }
    assert_eq!(n, 1);
}

#[tokio::test]
async fn ceiling_visibility_alone_is_not_widening() {
    // Batch ceiling Org with Private memory: fine (narrower than ceiling).
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 0)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "narrower-than-ceiling is allowed");
}
