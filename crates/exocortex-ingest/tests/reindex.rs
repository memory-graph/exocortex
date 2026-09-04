//! D4 (§24 q5): the explicit reindex a model swap needs. The tests pin
//! the closed problem (MCR² rejects a mixed-revision graph, R-Mcr1) and
//! prove reindex restamps every row — fail-without-it.

use std::sync::Arc;

use exocortex_dreams::mcr2::{MCR2Engine, MemoryWithEmbedding};
use exocortex_ingest::{Embedder, IngestServer};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
};
use tonic::Request;

const HMAC_KEY: [u8; 32] = [0x4d; 32];

/// The swapped model: same deterministic vectors, a DIFFERENT model
/// identity — exactly what a blue/green cutover leaves behind.
struct SwappedModelEmbedder;

impl Embedder for SwappedModelEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        exocortex_ingest::FakeEmbedder::default().embed(text)
    }
    fn model_id(&self) -> &'static str {
        "fake-swapped"
    }
    fn model_version(&self) -> &'static str {
        "v2"
    }
    fn dim(&self) -> usize {
        64
    }
}

fn draft(index: usize) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: format!("row-{index}"),
        id: String::new(),
        memory_type: "Solution".into(),
        title: format!("reindex candidate {index}"),
        content: "deterministic text for the embedding bucket hash".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

async fn server_with_rows(rows: usize) -> (Arc<InMemoryStorage>, IngestServer<InMemoryStorage>) {
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let mut server = IngestServer::new(storage.clone(), ontology, HMAC_KEY)
        .with_embedder(Arc::new(exocortex_ingest::FakeEmbedder::default()));
    // Production nodes run the single-org guard (round-3 C4); the audit
    // org falls back to it on principal-less rows.
    server.org_guard = Some("org".into());
    server
        .register_source(Request::new(exocortex_wire::signing::registration(
            &HMAC_KEY,
            "org",
            "custom://reindex",
            "reindex-test",
            3,
            "custom",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://reindex".into(),
        producer_id: "reindex-test".into(),
        batch_id: "reindex-seed".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: (0..rows).map(draft).collect(),
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "test-node".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&HMAC_KEY, &mut batch);
    let ack = server
        .submit(Request::new(batch))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted as usize, rows, "seed rows accepted");
    (storage, server)
}

async fn current_rows(storage: &InMemoryStorage) -> Vec<exocortex_kernel::Memory> {
    use futures::StreamExt;
    let mut stream = storage.stream_all_memories().await;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    rows
}

fn mcr2_over(rows: &[exocortex_kernel::Memory]) -> Result<(), exocortex_dreams::mcr2::MCR2Error> {
    let anchors: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            row.embedding.as_ref().map(|embedding| MemoryWithEmbedding {
                id: row.id,
                class: row.memory_type,
                visibility: row.visibility,
                embedding: embedding.clone(),
            })
        })
        .collect();
    MCR2Engine::default().compute(&anchors).map(|_| ())
}

/// The fail-without-it core: a model swap leaves a mixed-revision graph
/// that MCR² refuses (R-Mcr1); reindex restamps every row and the graph
/// computes again. Audit rows carry the action.
#[tokio::test]
async fn reindex_restamps_a_swapped_graph_to_one_model() {
    let (storage, mut server) = server_with_rows(2).await;
    // The swap: same server, new model identity.
    server.embedder = Some(Arc::new(SwappedModelEmbedder));
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://reindex".into(),
        producer_id: "reindex-test".into(),
        batch_id: "post-swap".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![draft(90)],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "test-node".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&HMAC_KEY, &mut batch);
    server.submit(Request::new(batch)).await.unwrap();

    let rows = current_rows(&storage).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.iter().filter(|r| r.embedding.is_some()).count(), 3);
    // The closed problem: mixed revisions refuse consolidation.
    let err = mcr2_over(&rows).expect_err("mixed revisions must refuse MCR2");
    assert!(matches!(
        err,
        exocortex_dreams::mcr2::MCR2Error::CrossModelComparison
    ));

    let audit_before = storage.audit_range("org", 0, 1000).await.unwrap().len();
    let report = server.reindex_embeddings("admin").await.expect("reindex");
    assert_eq!(report.scanned, 3);
    // Only the two v1 rows change; the post-swap row is already v2.
    assert_eq!(report.reembedded, 2);
    assert_eq!(report.unchanged, 1);
    assert_eq!(report.model_name, "fake-swapped");
    assert_eq!(report.model_version, "v2");

    let rows = current_rows(&storage).await;
    for row in &rows {
        let embedding = row.embedding.as_ref().expect("vector present");
        assert_eq!(embedding.model.name.as_str(), "fake-swapped");
        assert_eq!(embedding.model.version.as_str(), "v2");
    }
    mcr2_over(&rows).expect("single-revision graph computes");
    // Every changed chunk left an audit row.
    let audit = storage.audit_range("org", 0, 1000).await.unwrap();
    assert!(audit.len() > audit_before);
    assert!(audit
        .iter()
        .any(|event| event["action"] == "reindex_embeddings"));
}

/// Reindex is idempotent (a deterministic model re-embeds to identical
/// vectors) and backfills rows that committed WITHOUT vectors after an
/// embedding failure.
#[tokio::test]
async fn reindex_is_idempotent_and_backfills_missing_vectors() {
    let (storage, server) = server_with_rows(3).await;
    let first = server.reindex_embeddings("admin").await.expect("reindex");
    assert_eq!(
        first.reembedded, 0,
        "same model, same deterministic vectors"
    );
    assert_eq!(first.unchanged, 3);
    let second = server.reindex_embeddings("admin").await.expect("reindex");
    assert_eq!(second.scanned, 3);
    assert_eq!(second.reembedded, 0, "deterministic model: nothing changes");
    assert_eq!(second.unchanged, 3);

    // A row that lost its vector (the commit-without-vectors path) is
    // repaired, not skipped.
    let rows = current_rows(&storage).await;
    let mut stripped = rows[0].clone();
    stripped.embedding = None;
    storage.upsert_memory(&stripped).await.unwrap();
    let third = server.reindex_embeddings("admin").await.expect("reindex");
    assert_eq!(third.reembedded, 1);
    assert_eq!(third.unchanged, 2);
    let rows = current_rows(&storage).await;
    assert!(rows.iter().all(|row| row.embedding.is_some()));
}

/// No embedder configured (R-Lat3 backend flag off): reindex refuses
/// rather than silently stripping vectors.
#[tokio::test]
async fn reindex_refuses_without_an_embedder() {
    let (storage, mut server) = server_with_rows(1).await;
    let _ = storage;
    server.embedder = None;
    let err = server
        .reindex_embeddings("admin")
        .await
        .expect_err("refused");
    assert!(err.contains("no embedder configured"), "{err}");
}
