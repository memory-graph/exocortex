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
