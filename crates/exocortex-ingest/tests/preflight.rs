//! D21-b (adapter-contract PRD §3.2): `Preflight` — the dry-run Submit.
//!
//! What is pinned here:
//! - preflighting a sample returns byte-identical verdicts to submitting
//!   it (the acceptance clause: one corpus, both paths, rows compared);
//! - preflight commits NOTHING — no LSN is consumed (the next real submit
//!   of the very same batch commits at LSN 1, not 2), no audit row is
//!   written, and no idempotency claim is made (that same submit is
//!   accepted, not replayed as `DUPLICATE_BATCH`);
//! - the dry run runs under the caller's own producer HMAC: an unsigned
//!   preflight is `UNAUTHORIZED` exactly as an unsigned submit is.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::Storage;
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalSnapshotInfo, IngestBatch, MemoryDraft,
    ProducerIdentity, RegisterSourceRequest, RelationshipDraft,
};

fn server() -> IngestServer<InMemoryStorageShim> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorageShim::new(onto.clone())),
        onto,
        [7u8; 32],
    )
}

type InMemoryStorageShim = exocortex_storage::InMemoryStorage;

async fn register(srv: &IngestServer<InMemoryStorageShim>, producer: &str) {
    let mut r = RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: "custom://probe".into(),
        producer_id: producer.into(),
        ceiling: 1,
        source_flavor: "custom".into(),
        producer_kind: 5,
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
        projection: None,
    };
    exocortex_wire::signing::sign_registration(&[7u8; 32], &mut r);
    srv.register_source(tonic::Request::new(r)).await.unwrap();
}

/// The mixed corpus: one valid row, one unknown memory type, one
/// visibility over the registered ceiling, one unknown kind edge, one
/// invalid type triple edge. Every reject class a mapping author hits.
fn session_batch(batch_id: &str) -> IngestBatch {
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://probe".into(),
        producer_id: "probe".into(),
        batch_id: batch_id.into(),
        mapping_version: "custom:1".into(),
        ontology_fingerprint: Vec::new(), // stamped below per-server
        ceiling: 1,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![
            MemoryDraft {
                draft_key: "good".into(),
                id: String::new(),
                memory_type: "Fix".into(),
                title: "Fixed login token race".into(),
                content: "refresh() awaited before read".into(),
                tags: vec![],
                visibility: 1,
                valid_from: None,
                valid_until: None,
                external_key: None,
            },
            MemoryDraft {
                draft_key: "bad-type".into(),
                id: String::new(),
                memory_type: "NotAType".into(),
                title: "unknown type row".into(),
                content: "c".into(),
                tags: vec![],
                visibility: 1,
                valid_from: None,
                valid_until: None,
                external_key: None,
            },
            MemoryDraft {
                draft_key: "too-wide".into(),
                id: String::new(),
                memory_type: "Fix".into(),
                title: "org row under project ceiling".into(),
                content: "c".into(),
                tags: vec![],
                visibility: 3,
                valid_from: None,
                valid_until: None,
                external_key: None,
            },
        ],
        relationships: vec![
            RelationshipDraft {
                from_draft_key: "good".into(),
                to_draft_key: "bad-type".into(),
                kind: "NotAKnownKind".into(),
                strength: 0.0,
                confidence: 0.0,
                context: String::new(),
                visibility: 1,
                to_memory_id: String::new(),
            },
            RelationshipDraft {
                from_draft_key: "good".into(),
                to_draft_key: String::new(),
                kind: "Fixes".into(),
                strength: 0.0,
                confidence: 0.0,
                context: String::new(),
                visibility: 1,
                to_memory_id: String::new(),
            },
        ],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    b.ontology_fingerprint = fingerprint_of(&b);
    exocortex_wire::signing::prepare_batch(&[7u8; 32], &mut b);
    b
}

/// The external-snapshot corpus: a snapshot row missing its ExternalKey.
fn snapshot_batch(batch_id: &str) -> IngestBatch {
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://probe".into(),
        producer_id: "probe-ext".into(),
        batch_id: batch_id.into(),
        mapping_version: "custom:1".into(),
        ontology_fingerprint: Vec::new(),
        ceiling: 1,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: "snap-1".into(),
            schema_hash: [9u8; 32].to_vec(),
            source_flavor: "custom".into(),
        }),
        memories: vec![MemoryDraft {
            draft_key: "no-key".into(),
            id: String::new(),
            memory_type: "Fix".into(),
            title: "snapshot row without an external key".into(),
            content: "c".into(),
            tags: vec![],
            visibility: 1,
            valid_from: None,
            valid_until: None,
            external_key: None,
        }],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    b.ontology_fingerprint = fingerprint_of(&b);
    exocortex_wire::signing::prepare_batch(&[7u8; 32], &mut b);
    b
}

fn fingerprint_of(b: &IngestBatch) -> Vec<u8> {
    // The org guard/fingerprint check needs the server's own value; the
    // batches are stamped by re-deriving it from the registered probe —
    // simplest truthful source: any IngestServer over the same pack set.
    let _ = b;
    exocortex_kernel::Ontology::from_packs(vec![pack_def()])
        .unwrap()
        .fingerprint
        .0
        .to_vec()
}

fn verdict_rows(ack: &exocortex_wire::ingest::v1::IngestAck) -> Vec<(String, i32, String)> {
    ack.rejections
        .iter()
        .map(|r| (r.draft_key.clone(), r.code, r.detail.clone()))
        .collect()
}

#[tokio::test]
async fn preflight_verdicts_match_submit_verdicts_row_for_row() {
    let srv = server();
    register(&srv, "probe").await;
    register(&srv, "probe-ext").await;

    for batch in [session_batch("mixed-1"), snapshot_batch("snap-1")] {
        let pre = srv
            .preflight(tonic::Request::new(batch.clone()))
            .await
            .unwrap()
            .into_inner();
        let sub = srv
            .submit(tonic::Request::new(batch))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            verdict_rows(&pre),
            verdict_rows(&sub),
            "preflight and submit verdicts must be byte-identical"
        );
        assert_eq!(pre.accepted, sub.accepted);
        assert_eq!(pre.rejected, sub.rejected);
        assert_eq!(pre.batch_id, sub.batch_id);
        assert_eq!(pre.assigned_lsn, 0, "preflight assigns no LSN");
        // A rejected batch assigns nothing on either path; the accepted
        // case is pinned by the clean-half check below.
    }

    // The clean half of the corpus proves acceptance parity too: a batch
    // with no rejects accepts the same count both ways.
    let clean = session_batch("clean-1");
    let mut clean = IngestBatch {
        memories: vec![clean.memories[0].clone()],
        relationships: vec![],
        batch_id: "clean-1".into(),
        ..clean
    };
    exocortex_wire::signing::prepare_batch(&[7u8; 32], &mut clean);
    let pre = srv
        .preflight(tonic::Request::new(clean.clone()))
        .await
        .unwrap()
        .into_inner();
    let sub = srv
        .submit(tonic::Request::new(clean))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pre.accepted, sub.accepted, "accepted counts agree");
    assert_eq!(pre.rejected, 0);
    assert_eq!(sub.rejected, 0);
    assert_eq!(pre.assigned_lsn, 0);
    assert!(
        sub.assigned_lsn > 0,
        "the real submit commits at a real LSN"
    );
}

#[tokio::test]
async fn preflight_commits_nothing_and_leaves_no_idempotency_claim() {
    let srv = server();
    register(&srv, "probe").await;
    let clean = session_batch("solo-1");
    let mut clean = IngestBatch {
        memories: vec![clean.memories[0].clone()],
        relationships: vec![],
        batch_id: "solo-1".into(),
        ..clean
    };
    exocortex_wire::signing::prepare_batch(&[7u8; 32], &mut clean);

    let audit_before = srv.storage.audit_range("org", 0, 1000).await.unwrap().len();
    let pre = srv
        .preflight(tonic::Request::new(clean.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pre.assigned_lsn, 0);
    assert_eq!(pre.accepted, 1);
    assert_eq!(
        srv.storage.audit_range("org", 0, 1000).await.unwrap().len(),
        audit_before,
        "preflight writes no audit row"
    );

    // The very same batch — same batch_id — submits FRESH: LSN 1 (nothing
    // was consumed) and no DUPLICATE_BATCH replay (no idempotency claim).
    let sub = srv
        .submit(tonic::Request::new(clean))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(sub.assigned_lsn, 1, "preflight consumed no LSN");
    assert_eq!(sub.accepted, 1);
    assert!(verdict_rows(&sub).is_empty(), "not replayed as duplicate");
}

#[tokio::test]
async fn unsigned_preflight_is_unauthorized_like_submit() {
    let srv = server();
    register(&srv, "probe").await;
    let mut batch = session_batch("unsigned-1");
    if let Some(p) = batch.producer.as_mut() {
        p.hmac_signature = vec![];
    }
    let pre = srv
        .preflight(tonic::Request::new(batch.clone()))
        .await
        .unwrap()
        .into_inner();
    let sub = srv
        .submit(tonic::Request::new(batch))
        .await
        .unwrap()
        .into_inner();
    for ack in [&pre, &sub] {
        assert!(!ack.rejections.is_empty(), "rejected");
        for row in &ack.rejections {
            assert_eq!(
                row.code,
                exocortex_wire::ingest::v1::RejectCode::Unauthorized as i32,
                "every row names the auth failure"
            );
        }
        assert_eq!(ack.accepted, 0);
    }
}
