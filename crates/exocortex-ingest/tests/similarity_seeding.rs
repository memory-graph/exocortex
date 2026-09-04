//! D5 (§24 q12): opt-in ingest-time SimilarTo seeding — the warm
//! skeleton before the first Dreams cycle. The fail-without-it control
//! is the seeding-OFF server: identical batches, zero SimilarTo edges.

use std::sync::Arc;

use exocortex_ingest::{Embedder, IngestServer};
use exocortex_kernel::Provenance;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
};
use tonic::Request;

const HMAC_KEY: [u8; 32] = [0x5e; 32];

/// Hand-placed 2-dim vectors so the cosine windows are exact:
/// axis=[1,0], half=30°-off (cos = 0.866, inside [0.85, 0.92)),
/// twin=[1,0] (cos = 1.0, merge territory, NOT seeded), far=[-1,0].
struct FixedVectorEmbedder;

impl Embedder for FixedVectorEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let vector = if text.contains("half") {
            vec![0.866, 0.5]
        } else if text.contains("far") {
            vec![-1.0, 0.0]
        } else {
            vec![1.0, 0.0]
        };
        Ok(vector)
    }
    fn model_id(&self) -> &'static str {
        "fake-seed"
    }
    fn model_version(&self) -> &'static str {
        "v1"
    }
    fn dim(&self) -> usize {
        2
    }
}

fn draft(key: &str, title: &str, visibility: i32) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: key.into(),
        id: String::new(),
        memory_type: "Solution".into(),
        title: title.into(),
        content: title.into(),
        tags: vec![],
        visibility,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

async fn build_server(seeding: bool) -> (Arc<InMemoryStorage>, IngestServer<InMemoryStorage>) {
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let mut server = IngestServer::new(storage.clone(), ontology, HMAC_KEY)
        .with_embedder(Arc::new(FixedVectorEmbedder))
        .with_ingest_similarity_seeding(seeding);
    server.org_guard = Some("org".into());
    server
        .register_source(Request::new(exocortex_wire::signing::registration(
            &HMAC_KEY,
            "org",
            "custom://seeding",
            "seeding-test",
            3,
            "custom",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    (storage, server)
}

async fn submit(server: &IngestServer<InMemoryStorage>, batch_id: &str, rows: Vec<MemoryDraft>) {
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://seeding".into(),
        producer_id: "seeding-test".into(),
        batch_id: batch_id.into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: rows,
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
    assert_eq!(ack.rejections.len(), 0, "{:?}", ack.rejections);
}

async fn relationships(storage: &InMemoryStorage) -> Vec<exocortex_kernel::Relationship> {
    use futures::StreamExt;
    let mut stream = storage.stream_all_relationships().await;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    rows
}

/// Fail-without-it: with seeding ON, a later row 30° from a ringed row
/// gets a server-computed SimilarTo edge in Dreams' own window; the
/// same batches under the DEFAULT (off) server produce none. Merge
/// territory (cos = 1.0) and unrelated rows (cos < 0) stay unseeded.
#[tokio::test]
async fn seeding_writes_computed_edges_in_the_dreams_window() {
    let (storage, server) = build_server(true).await;
    submit(
        &server,
        "b1",
        vec![draft("axis", "axis zero", 3), draft("far", "far away", 3)],
    )
    .await;
    let edges = relationships(&storage).await;
    assert!(edges.is_empty(), "first batch has no prior ring");

    // Private row against the Org ring row: the edge narrows to Private.
    submit(&server, "b2", vec![draft("half", "half window", 0)]).await;
    let edges = relationships(&storage).await;
    let similar: Vec<_> = edges
        .iter()
        .filter(|edge| {
            matches!(
                edge.provenance,
                Provenance::Computed {
                    producer: exocortex_kernel::provenance::ComputedProducer::SimilarityCosine,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(similar.len(), 1, "{edges:?}");
    let edge = similar[0];
    assert_eq!(edge.kind, server.ontology.kind_id("SimilarTo").unwrap());
    assert!(edge.bidirectional);
    assert_eq!(edge.visibility, exocortex_kernel::Visibility::Private);
    match &edge.provenance {
        Provenance::Computed {
            producer,
            threshold,
        } => {
            assert_eq!(
                *producer,
                exocortex_kernel::provenance::ComputedProducer::SimilarityCosine
            );
            assert!((threshold - 0.85).abs() < f32::EPSILON);
        }
        other => panic!("computed provenance expected, got {other:?}"),
    }
    assert!((edge.properties.strength - 0.866).abs() < 1e-3);
    assert!(edge.valid_until.is_none());
    // far (cos = -1 against axis) seeded nothing: still exactly one edge.
    assert_eq!(edges.len(), 1);

    // Merge territory is NOT seeded: a twin of the axis row (cos = 1.0
    // vs axis) consolidates through Dreams' merge path, not a seeded
    // edge. (The twin DOES seed against `half` — cos 0.866, a different
    // pair inside the window.)
    submit(&server, "b3", vec![draft("twin", "axis one", 3)]).await;
    let edges = relationships(&storage).await;
    use futures::StreamExt;
    let mut stream = storage.stream_all_memories().await;
    let mut title_of = std::collections::HashMap::new();
    while let Some(row) = stream.next().await {
        let row = row.unwrap();
        title_of.insert(row.id, row.title.to_string());
    }
    let axis_twin = edges.iter().any(|edge| {
        let pair = (
            title_of.get(&edge.from).map(String::as_str),
            title_of.get(&edge.to).map(String::as_str),
        );
        matches!(
            pair,
            (Some("axis zero"), Some("axis one")) | (Some("axis one"), Some("axis zero"))
        )
    });
    assert!(
        !axis_twin,
        "cos=1.0 is merge territory, never seeded: {edges:?}"
    );
    assert_eq!(
        edges.len(),
        2,
        "twin–half (0.866) seeded, twin–axis (1.0) not"
    );

    // The control: identical batches, seeding OFF (the default) — no
    // SimilarTo edges at all.
    let (plain_storage, plain) = build_server(false).await;
    submit(
        &plain,
        "b1",
        vec![draft("axis", "axis zero", 3), draft("far", "far away", 3)],
    )
    .await;
    submit(&plain, "b2", vec![draft("half", "half window", 0)]).await;
    submit(&plain, "b3", vec![draft("twin", "axis one", 3)]).await;
    let plain_edges = relationships(&plain_storage).await;
    assert!(
        plain_edges.is_empty(),
        "default server seeds nothing: {plain_edges:?}"
    );
}

/// The producer boundary is untouched: with seeding ON, a producer
/// forging a SimilarTo relationship is still rejected
/// (`ComputedKindRejected`) and the whole batch with it.
#[tokio::test]
async fn producer_submitted_similarto_stays_rejected_under_seeding() {
    use exocortex_wire::ingest::v1::RelationshipDraft;
    let (storage, server) = build_server(true).await;
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://seeding".into(),
        producer_id: "seeding-test".into(),
        batch_id: "forged".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![draft("a", "axis zero", 3), draft("b", "half window", 3)],
        relationships: vec![RelationshipDraft {
            from_draft_key: "a".into(),
            to_draft_key: "b".into(),
            kind: "SimilarTo".into(),
            strength: 0.95,
            confidence: 0.9,
            context: String::new(),
            visibility: 3,
            to_memory_id: String::new(),
        }],
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
    assert!(
        ack.rejections
            .iter()
            .any(|row| row.code
                == exocortex_wire::ingest::v1::RejectCode::ComputedKindRejected as i32),
        "{:?}",
        ack.rejections
    );
    // Batch-atomic: nothing committed, nothing seeded.
    use futures::StreamExt;
    let mut stream = storage.stream_all_memories().await;
    let mut rows = 0;
    while let Some(row) = stream.next().await {
        row.unwrap();
        rows += 1;
    }
    assert_eq!(rows, 0);
    assert!(relationships(&storage).await.is_empty());
}

/// A replayed batch never re-seeds: idempotency answers
/// `DuplicateBatch` before the seeding hook runs, so the edge set stays
/// exactly the first-run set.
#[tokio::test]
async fn replayed_batches_never_reseed() {
    let (storage, server) = build_server(true).await;
    submit(&server, "b1", vec![draft("axis", "axis zero", 3)]).await;
    submit(&server, "b2", vec![draft("half", "half window", 3)]).await;
    let before = relationships(&storage).await;
    assert_eq!(before.len(), 1);

    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://seeding".into(),
        producer_id: "seeding-test".into(),
        batch_id: "b2".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![draft("half", "half window", 3)],
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
    assert!(
        ack.rejections
            .iter()
            .any(|row| row.code == exocortex_wire::ingest::v1::RejectCode::DuplicateBatch as i32),
        "{:?}",
        ack.rejections
    );
    let after = relationships(&storage).await;
    assert_eq!(after.len(), 1, "no duplicate assertion rows: {after:?}");
}
