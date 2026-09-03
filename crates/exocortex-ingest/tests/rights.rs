//! D24 (rights and consent provenance): per-record rights on the wire
//! land on the committed row; a source's registered default stamps
//! rows that carry none; a record's own rights override the default;
//! re-registration never overwrites the recorded default (the ceiling
//! pattern); and rows with no rights anywhere stay `None` so the
//! corpus exporter answers "not covered" (fail closed — proven in the
//! server's corpus_export suite).

use exocortex_ingest::IngestServer;
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::ingest_service_server::IngestService;
use tonic::Request;

fn ontology() -> std::sync::Arc<exocortex_kernel::Ontology> {
    std::sync::Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn server() -> (
    IngestServer<InMemoryStorage>,
    std::sync::Arc<InMemoryStorage>,
) {
    let onto = ontology();
    let storage = std::sync::Arc::new(InMemoryStorage::new(onto.clone()));
    let server = IngestServer::new(storage.clone(), onto, [5u8; 32]);
    (server, storage)
}

fn draft(
    key: &str,
    rights: Option<exocortex_wire::ingest::v1::Rights>,
) -> exocortex_wire::ingest::v1::MemoryDraft {
    exocortex_wire::ingest::v1::MemoryDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: "Fix".into(),
        title: format!("rights row {key}"),
        content: "body".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
        rights,
    }
}

fn wire_rights(licence: &str, consent: &str) -> exocortex_wire::ingest::v1::Rights {
    exocortex_wire::ingest::v1::Rights {
        licence: licence.into(),
        consent_basis: consent.into(),
        retention_until: None,
        redacted: false,
    }
}

fn register(
    srv: &IngestServer<InMemoryStorage>,
    source_uri: &str,
    default_rights: Option<exocortex_wire::ingest::v1::Rights>,
) {
    let mut request = exocortex_wire::ingest::v1::RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: source_uri.into(),
        producer_id: "p".into(),
        ceiling: 3,
        source_flavor: "custom".into(),
        producer_kind: 4,
        producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
        projection: None,
        default_rights,
    };
    exocortex_wire::signing::sign_registration(&[5u8; 32], &mut request);
    futures::executor::block_on(async {
        srv.register_source(Request::new(request)).await.unwrap();
    });
}

fn submit(
    srv: &IngestServer<InMemoryStorage>,
    source_uri: &str,
    drafts: Vec<exocortex_wire::ingest::v1::MemoryDraft>,
) -> exocortex_wire::ingest::v1::IngestAck {
    let mut batch = exocortex_wire::ingest::v1::IngestBatch {
        org_id: "org".into(),
        source_uri: source_uri.into(),
        producer_id: "p".into(),
        batch_id: format!("b-{}-{source_uri}", std::process::id()),
        mapping_version: "p:1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: drafts,
        relationships: vec![],
        producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
            node_id: "n".into(),
            agent_id: "a".into(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut batch);
    futures::executor::block_on(async {
        use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;
        srv.submit(Request::new(batch)).await.unwrap().into_inner()
    })
}

async fn committed_title(storage: &InMemoryStorage, title: &str) -> exocortex_kernel::Memory {
    use exocortex_storage::Storage as _;
    use futures::StreamExt;
    let rows: Vec<_> = storage
        .stream_all_memories()
        .await
        .filter_map(|row| async move { row.ok() })
        .collect::<Vec<_>>()
        .await;
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|memory| memory.title == title)
        .collect();
    rows.into_iter().next().expect("the row committed")
}

#[tokio::test(flavor = "multi_thread")]
async fn record_rights_override_source_defaults_and_absence_stays_none() {
    let (srv, storage) = server();
    register(
        &srv,
        "custom://rights",
        Some(wire_rights("CC-BY-4.0", "contractual")),
    );
    let ack = submit(
        &srv,
        "custom://rights",
        vec![
            // Per-record rights WIN over the source default.
            draft("own", Some(wire_rights("MIT", "legitimate-interest"))),
            // No per-record rights: the source default stamps it.
            draft("defaulted", None),
        ],
    );
    assert_eq!(
        ack.accepted, 2,
        "both rows accepted (rejections: {:#?})",
        ack.rejections
    );

    let own = committed_title(&storage, "rights row own").await;
    assert_eq!(own.rights.as_ref().unwrap().licence.as_deref(), Some("MIT"));
    assert_eq!(
        own.rights.as_ref().unwrap().consent_basis.as_deref(),
        Some("legitimate-interest")
    );

    let defaulted = committed_title(&storage, "rights row defaulted").await;
    assert_eq!(
        defaulted.rights.as_ref().unwrap().licence.as_deref(),
        Some("CC-BY-4.0"),
        "the source default stamps rows without their own rights"
    );

    // A source with NO default: rows stay rights-free — fail closed
    // downstream, never guessed here.
    register(&srv, "custom://no-rights", None);
    let ack = submit(&srv, "custom://no-rights", vec![draft("bare", None)]);
    assert_eq!(ack.accepted, 1);
    let bare = committed_title(&storage, "rights row bare").await;
    assert_eq!(bare.rights, None, "no claim anywhere: no rights invented");
}

#[tokio::test(flavor = "multi_thread")]
async fn re_registration_does_not_overwrite_recorded_default_rights() {
    let (srv, storage) = server();
    register(
        &srv,
        "custom://sticky",
        Some(wire_rights("Apache-2.0", "contractual")),
    );
    // A later registration for the same source tries to swap the
    // default to something wider.
    register(
        &srv,
        "custom://sticky",
        Some(wire_rights("Public-Domain", "none-claimed")),
    );
    submit(&srv, "custom://sticky", vec![draft("row", None)]);
    let row = committed_title(&storage, "rights row row").await;
    assert_eq!(
        row.rights.as_ref().unwrap().licence.as_deref(),
        Some("Apache-2.0"),
        "the FIRST registration's default sticks (the ceiling pattern)"
    );
}
