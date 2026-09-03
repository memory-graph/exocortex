use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use exocortex_ingest::{Embedder, IngestServer};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
};
use tonic::Request;

const HMAC_KEY: [u8; 32] = [0x79; 32];

struct BlockingBatchEmbedder {
    single_calls: AtomicUsize,
    batch_calls: AtomicUsize,
    last_batch_len: AtomicUsize,
}

impl BlockingBatchEmbedder {
    fn new() -> Self {
        Self {
            single_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
            last_batch_len: AtomicUsize::new(0),
        }
    }
}

impl Embedder for BlockingBatchEmbedder {
    fn embed(&self, _text: &str) -> Result<Vec<f32>, String> {
        if self.single_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(400));
        }
        Ok(vec![1.0, 0.0, 0.0, 0.0])
    }

    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        self.last_batch_len.store(texts.len(), Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(400));
        Ok(vec![vec![1.0, 0.0, 0.0, 0.0]; texts.len()])
    }

    fn model_id(&self) -> &'static str {
        "blocking-test"
    }

    fn model_version(&self) -> &'static str {
        "v1"
    }

    fn dim(&self) -> usize {
        4
    }
}

fn draft(index: usize) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: format!("row-{index}"),
        id: String::new(),
        memory_type: "Solution".into(),
        title: format!("blocking embedding {index}"),
        content: "the synchronous model must not occupy a Tokio worker".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn max_batch_embedding_is_one_blocking_invocation_without_worker_starvation() {
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let embedder = Arc::new(BlockingBatchEmbedder::new());
    let server = IngestServer::new(storage, ontology, HMAC_KEY).with_embedder(embedder.clone());
    server
        .register_source(Request::new(exocortex_wire::signing::registration(
            &HMAC_KEY,
            "org",
            "custom://blocking",
            "blocking-test",
            3,
            "custom",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://blocking".into(),
        producer_id: "blocking-test".into(),
        batch_id: "max-embedding-batch".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: server.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: (0..exocortex_wire::limits::MAX_MEMORIES_PER_BATCH)
            .map(draft)
            .collect(),
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

    let started = Instant::now();
    let submit = tokio::spawn(async move { server.submit(Request::new(batch)).await });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "Tokio heartbeat was starved by synchronous inference"
    );

    let ack = submit.await.unwrap().unwrap().into_inner();
    assert_eq!(
        ack.accepted as usize,
        exocortex_wire::limits::MAX_MEMORIES_PER_BATCH
    );
    assert_eq!(embedder.batch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(embedder.single_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        embedder.last_batch_len.load(Ordering::SeqCst),
        exocortex_wire::limits::MAX_MEMORIES_PER_BATCH
    );
}
