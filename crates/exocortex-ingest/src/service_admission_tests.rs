use std::sync::atomic::{AtomicUsize, Ordering};

use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::{IngestBatch, MemoryDraft, ProducerIdentity, RejectCode};

use super::*;

fn server(limit: usize) -> IngestServer<InMemoryStorage> {
    let ontology = Arc::new(Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorage::new(ontology.clone())),
        ontology,
        [5; 32],
    )
    .with_submit_concurrency_limit(limit)
}

fn one_row_batch() -> IngestBatch {
    IngestBatch {
        memories: vec![MemoryDraft::default()],
        producer: Some(ProducerIdentity {
            hmac_signature: vec![1],
            ..ProducerIdentity::default()
        }),
        ..IngestBatch::default()
    }
}

fn assert_reject_code(ack: &IngestAck, code: RejectCode) {
    assert!(!ack.rejections.is_empty());
    assert!(ack.rejections.iter().all(|row| row.code == code as i32));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_registry_persistence_serializes_snapshot_and_replace_order() {
    use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;

    let root = std::env::temp_dir().join(format!(
        "exocortex-source-order-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("sources.json");
    let mut persistent_server = server(4).with_sources_file(path.clone());
    let (paused_tx, paused_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    persistent_server.source_persist_hook = Some(Arc::new(move |rows| {
        if rows == 1 {
            paused_tx.send(()).unwrap();
            release_rx.lock().unwrap().take().unwrap().recv().unwrap();
        }
    }));

    let shared_server = Arc::new(persistent_server);
    let registration = |source: &str| {
        tonic::Request::new(exocortex_wire::signing::registration(
            &[5; 32],
            "org",
            source,
            "producer",
            3,
            "session",
            "node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        ))
    };
    let first_server = shared_server.clone();
    let first = tokio::spawn(async move {
        first_server
            .register_source(registration("session://first"))
            .await
    });
    tokio::task::spawn_blocking(move || paused_rx.recv().unwrap())
        .await
        .unwrap();

    let second_server = shared_server.clone();
    let mut second = tokio::spawn(async move {
        second_server
            .register_source(registration("session://second"))
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "a later registry mutation must wait for the earlier snapshot replacement"
    );
    release_tx.send(()).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();

    let restarted = server(1).with_sources_file(path.clone());
    let mut sources = restarted.sources.lock().unwrap();
    assert!(sources
        .get(&("org".into(), "session://first".into(), "producer".into()))
        .is_some());
    assert!(sources
        .get(&("org".into(), "session://second".into(), "producer".into()))
        .is_some());
    drop(sources);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn malformed_source_registry_blocks_reregistration_instead_of_widening() {
    use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;

    let root = std::env::temp_dir().join(format!(
        "exocortex-source-corrupt-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("sources.json");
    std::fs::write(&path, "not valid registry JSON").unwrap();
    let persistent_server = server(1).with_sources_file(path);
    let request = tonic::Request::new(exocortex_wire::signing::registration(
        &[5; 32],
        "org",
        "session://previously-private",
        "producer",
        3,
        "session",
        "node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    ));
    let error = persistent_server
        .register_source(request)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(persistent_server.sources.lock().unwrap().is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn failed_source_registry_replacement_never_publishes_live_authority() {
    use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;

    let root = std::env::temp_dir().join(format!(
        "exocortex-source-failure-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let blocked_parent = root.join("blocked");
    let path = blocked_parent.join("sources.json");
    let persistent_server = server(1).with_sources_file(path);
    std::fs::write(&blocked_parent, "not a directory").unwrap();
    let request = tonic::Request::new(exocortex_wire::signing::registration(
        &[5; 32],
        "org",
        "session://uncommitted",
        "producer",
        3,
        "session",
        "node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    ));
    let error = persistent_server
        .register_source(request)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(persistent_server.sources.lock().unwrap().is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn oversized_batch_is_rejected_before_hmac_and_checksum_work() {
    let server = server(1);
    let batch = IngestBatch {
        memories: vec![MemoryDraft::default(); exocortex_wire::limits::MAX_MEMORIES_PER_BATCH + 1],
        producer: Some(ProducerIdentity {
            hmac_signature: vec![1],
            ..ProducerIdentity::default()
        }),
        ..IngestBatch::default()
    };
    let hmac_calls = AtomicUsize::new(0);
    let checksum_calls = AtomicUsize::new(0);
    let ack = server
        .admit_batch_with(
            &batch,
            |_, _| {
                hmac_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            |_| {
                checksum_calls.fetch_add(1, Ordering::Relaxed);
                String::new()
            },
        )
        .err()
        .unwrap();

    assert_reject_code(&ack, RejectCode::ResourceLimitExceeded);
    assert_eq!(hmac_calls.load(Ordering::Relaxed), 0);
    assert_eq!(checksum_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn saturated_admission_is_rejected_before_hmac_and_checksum_work() {
    let server = server(0);
    let batch = one_row_batch();
    let hmac_calls = AtomicUsize::new(0);
    let checksum_calls = AtomicUsize::new(0);
    let ack = server
        .admit_batch_with(
            &batch,
            |_, _| {
                hmac_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            |_| {
                checksum_calls.fetch_add(1, Ordering::Relaxed);
                String::new()
            },
        )
        .err()
        .unwrap();

    assert_reject_code(&ack, RejectCode::RateLimited);
    assert_eq!(hmac_calls.load(Ordering::Relaxed), 0);
    assert_eq!(checksum_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn admitted_batches_still_authenticate_before_checksum_work() {
    let server = server(1);
    let batch = one_row_batch();
    let hmac_calls = AtomicUsize::new(0);
    let checksum_calls = AtomicUsize::new(0);
    let ack = server
        .admit_batch_with(
            &batch,
            |_, _| {
                hmac_calls.fetch_add(1, Ordering::Relaxed);
                Err(Status::unauthenticated("sentinel authentication failure"))
            },
            |_| {
                checksum_calls.fetch_add(1, Ordering::Relaxed);
                String::new()
            },
        )
        .err()
        .unwrap();

    assert_reject_code(&ack, RejectCode::Unauthorized);
    assert_eq!(hmac_calls.load(Ordering::Relaxed), 1);
    assert_eq!(checksum_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn missing_authentication_still_precedes_resource_admission() {
    let server = server(0);
    let batch = IngestBatch {
        memories: vec![MemoryDraft::default(); exocortex_wire::limits::MAX_MEMORIES_PER_BATCH + 1],
        ..IngestBatch::default()
    };
    let hmac_calls = AtomicUsize::new(0);
    let checksum_calls = AtomicUsize::new(0);
    let ack = server
        .admit_batch_with(
            &batch,
            |_, _| {
                hmac_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            |_| {
                checksum_calls.fetch_add(1, Ordering::Relaxed);
                String::new()
            },
        )
        .err()
        .unwrap();

    assert_reject_code(&ack, RejectCode::Unauthorized);
    assert_eq!(hmac_calls.load(Ordering::Relaxed), 0);
    assert_eq!(checksum_calls.load(Ordering::Relaxed), 0);
}
