//! D3 federation, ingest side (master plan Wave 5): a superset server
//! ADMITS a batch whose ontology claim is a canonical pack-PREFIX
//! fingerprint (the "knows less" producer, derived not negotiated) and
//! still revalidates every row against its own full rulebook; a
//! non-prefix claim is refused at the boundary exactly as before.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_kernel::Ontology;
use exocortex_pack_dev_v1::pack_def as dev;
use exocortex_pack_mortgage_v1::pack_def as mortgage;
use exocortex_pack_study_v1::pack_def as study;
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;
use exocortex_wire::ingest::v1::{MemoryDraft, ProducerIdentity, RejectCode};

fn composed_server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(
        Ontology::from_packs(vec![dev(), mortgage(), study()]).expect("composed set assembles"),
    );
    IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
}

fn row(key: &str, memory_type: &str) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: key.into(),
        id: String::new(),
        memory_type: memory_type.into(),
        title: format!("row {key}"),
        content: format!("content {key} mentions src/main.rs and cargo build"),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

async fn registered(server: &IngestServer<InMemoryStorage>) {
    server
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://fed",
            "federated-peer",
            3,
            "session",
            "fed-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .expect("registration lands");
}

fn batch(
    id: &str,
    fingerprint: Vec<u8>,
    rows: Vec<MemoryDraft>,
) -> exocortex_wire::ingest::v1::IngestBatch {
    let mut b = exocortex_wire::ingest::v1::IngestBatch {
        org_id: "org".into(),
        source_uri: "session://fed".into(),
        producer_id: "federated-peer".into(),
        batch_id: id.into(),
        mapping_version: "1".into(),
        ontology_fingerprint: fingerprint,
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: rows,
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "fed-node".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    b
}

/// The employer side of personal↔employer: a dev-only deployment's
/// fingerprint (the derived common subset) is admitted by the composed
/// server, and the rows still face the FULL rulebook — the dev row
/// lands, the mortgage row is rejected by name. A lying claim buys
/// nothing.
#[tokio::test]
async fn prefix_fingerprint_batches_are_admitted_and_rows_revalidated() {
    let server = composed_server();
    registered(&server).await;
    let dev_only = Ontology::from_packs(vec![dev()]).expect("dev alone assembles");
    // Cross-check the derivation the admission actually consults.
    assert!(
        exocortex_kernel::federation::prefix_fingerprints(&[dev(), mortgage(), study()])
            .contains(&dev_only.fingerprint.0),
        "the dev-only fingerprint IS the first canonical prefix"
    );

    // A pure common-subset batch under the prefix claim lands whole —
    // the federated exchange itself works.
    let ack = server
        .submit(tonic::Request::new(batch(
            "fed-clean",
            dev_only.fingerprint.0.to_vec(),
            vec![row("solution-1", "Solution")],
        )))
        .await
        .expect("the prefix claim is admitted at the boundary")
        .into_inner();
    assert_eq!(ack.accepted, 1, "the dev row lands: {ack:?}");
    assert_eq!(ack.rejected, 0);

    // And a lying claim buys nothing: validation is batch-atomic, so
    // one row outside the claimed subset rejects the WHOLE batch,
    // naming the foreign label. The projection contract is the
    // sender's to honor; the receiver's rulebook is the wall.
    let ack = server
        .submit(tonic::Request::new(batch(
            "fed-lying",
            dev_only.fingerprint.0.to_vec(),
            vec![row("solution-2", "Solution"), row("borrower-1", "Borrower")],
        )))
        .await
        .expect("submit answers with a rejection ack")
        .into_inner();
    assert_eq!(ack.accepted, 0, "nothing from a lying claim: {ack:?}");
    assert_eq!(ack.rejected, 2, "batch-atomic: both rows rejected");
    assert_eq!(ack.rejections[0].code, RejectCode::UnknownMemoryType as i32);
    assert_eq!(
        ack.rejections[0].draft_key, "borrower-1",
        "the rejection names the offending row"
    );
}

/// The boundary still refuses what it always refused: a foreign
/// fingerprint (and the naive mid-set subset, whose slot-remapped
/// fingerprint matches no prefix) rejects the WHOLE batch with
/// `IncompatibleOntology`.
#[tokio::test]
async fn non_prefix_fingerprints_reject_the_whole_batch() {
    let server = composed_server();
    registered(&server).await;
    // mortgage alone: never a legal composition (kernel constants stay
    // unbound), but its summary fingerprint is computable — the naive
    // "share only the pack we both name" peer offers exactly this.
    let mid_set =
        exocortex_kernel::compatibility::OntologySummary::of_canonical_packs(&[mortgage()])
            .compatibility_fingerprint()
            .0
            .to_vec();
    for offered in [mid_set, vec![7u8; 32]] {
        let ack = server
            .submit(tonic::Request::new(batch(
                "fed-refused",
                offered,
                vec![row("solution-1", "Solution")],
            )))
            .await
            .expect("submit answers with a rejection ack")
            .into_inner();
        assert_eq!(ack.accepted, 0, "nothing crosses the boundary");
        assert_eq!(ack.rejected, 1);
        assert_eq!(
            ack.rejections[0].code,
            RejectCode::IncompatibleOntology as i32
        );
    }
}
