//! OC-PRD S4 (docs/prd/ontology-compatibility-prd.md §6): a rolling
//! upgrade works and fails correctly. Node B runs `{dev-v1 + one
//! appended type}`; node A runs plain `{dev-v1}`. B accepts A's
//! batches through its recognized window, A rejects B's with a
//! legible error, and neither drops an invalidation envelope
//! silently — mismatched peer admission is a returned error.

use std::sync::Arc;

use exocortex_cluster::ClusterNode;
use exocortex_ingest::IngestServer;
use exocortex_kernel::Ontology;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Invalidation};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity, RejectCode,
};

fn small_ontology() -> Arc<Ontology> {
    Arc::new(Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn grown_ontology() -> Arc<Ontology> {
    let mut grown = pack_def();
    grown.memory_type_names.push("FutureThing".into());
    Arc::new(Ontology::from_packs(vec![grown]).unwrap())
}

fn draft(key: &str) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: key.into(),
        id: String::new(),
        memory_type: "Fix".into(),
        title: format!("title {key}"),
        content: "Fixed in src/auth.rs".to_string(),
        tags: vec!["auth".into()],
        visibility: 1,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn batch(key: &str, memories: Vec<MemoryDraft>, fingerprint: [u8; 32]) -> IngestBatch {
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "session://rolling".into(),
        producer_id: "rolling-producer".into(),
        batch_id: format!("rolling-{}-{key}", std::process::id()),
        mapping_version: "rolling:1.0.0".into(),
        ontology_fingerprint: fingerprint.to_vec(),
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
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    b
}

async fn register(srv: &IngestServer<InMemoryStorage>) {
    srv.register_source(tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://rolling",
        "rolling-producer",
        3,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    )))
    .await
    .unwrap();
}

/// B (superset, its pin advanced from A's fingerprint) accepts a
/// producer batch stamped with A's compatibility fingerprint; A
/// (subset) rejects the same producer's grown fingerprint loudly and
/// legibly.
#[tokio::test]
async fn superset_accepts_subset_producer_subset_rejects_legibly() {
    let small = small_ontology();
    let grown = grown_ontology();

    // Node B: its graph was pinned by A and advanced; the recognized
    // window carries A's fingerprint (OC-PRD D3 deployment order:
    // nodes first, then producers).
    let b_store = Arc::new(InMemoryStorage::with_recognized_ontology_history(
        grown.clone(),
        &[small.fingerprint.0],
    ));
    let b_server = IngestServer::new(b_store, grown.clone(), [5u8; 32]);
    register(&b_server).await;
    let ack = b_server
        .submit(tonic::Request::new(batch(
            "b1",
            vec![draft("b1")],
            small.fingerprint.0,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections.is_empty(),
        "superset node must accept the subset producer: {:?}",
        ack.rejections
    );
    assert_eq!(ack.accepted, 1);

    // Node A has never seen the grown fingerprint.
    let a_store = Arc::new(InMemoryStorage::new(small.clone()));
    let a_server = IngestServer::new(a_store, small.clone(), [5u8; 32]);
    register(&a_server).await;
    let ack = a_server
        .submit(tonic::Request::new(batch(
            "a1",
            vec![draft("a1")],
            grown.fingerprint.0,
        )))
        .await
        .unwrap()
        .into_inner();
    let rejection = ack
        .rejections
        .iter()
        .find(|r| r.code == RejectCode::IncompatibleOntology as i32)
        .expect("subset node rejects the superset producer");
    // Legible, not two opaque hashes: the rejection names what to do.
    assert!(
        rejection.detail.contains("re-negotiate"),
        "detail must be actionable: {}",
        rejection.detail
    );

    // The current fingerprint is of course still admitted by both.
    let ack = b_server
        .submit(tonic::Request::new(batch(
            "b2",
            vec![draft("b2")],
            grown.fingerprint.0,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(ack.rejections.is_empty());
}

/// Mixed-version peers do not admit each other's invalidation
/// envelopes, and the failure is a returned error — never a silent
/// drop. Same-version peers admit as before.
#[tokio::test]
async fn mismatched_peers_fail_loudly_in_both_directions() {
    let small = small_ontology();
    let grown = grown_ontology();
    let node_a = ClusterNode::new(
        Arc::new(InMemoryStorage::new(small.clone())),
        "node-a".into(),
        small.fingerprint,
        [7; 32],
    );
    let node_b = ClusterNode::new(
        Arc::new(InMemoryStorage::new(grown.clone())),
        "node-b".into(),
        grown.fingerprint,
        [7; 32],
    );

    let env_b = node_b.envelope(Invalidation::MemoryUpserted {
        lsn: 1,
        id: exocortex_kernel::MemoryId([1; 16]),
    });
    match node_a.admit(&env_b) {
        Err(exocortex_cluster::ClusterError::OntologyMismatch) => {}
        other => panic!("expected a loud OntologyMismatch, got {other:?}"),
    }

    let env_a = node_a.envelope(Invalidation::MemoryUpserted {
        lsn: 1,
        id: exocortex_kernel::MemoryId([2; 16]),
    });
    match node_b.admit(&env_a) {
        Err(exocortex_cluster::ClusterError::OntologyMismatch) => {}
        other => panic!("expected a loud OntologyMismatch, got {other:?}"),
    }

    // Control: same-ontology peers still admit each other.
    let node_a2 = ClusterNode::new(
        Arc::new(InMemoryStorage::new(small.clone())),
        "node-a2".into(),
        small.fingerprint,
        [7; 32],
    );
    node_a2
        .admit(&env_a)
        .expect("same-ontology envelope admitted");
}
