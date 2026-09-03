//! D21-c (adapter-contract PRD D3): the golden manifest-parity table.
//!
//! One corpus containing every locally-computable `RejectCode` class,
//! run through BOTH validators: the ingest Submit path (the server's own
//! verdict, arrived at through the D21-b preflight RPC so nothing
//! commits) and the SDK's manifest interpreter (the rulebook as data).
//! Verdicts must agree row for row — the same golden-table shape
//! `write_path_parity` uses for the kernel and ingest validators. A
//! third divergent path cannot appear without failing this.

use std::sync::Arc;

use exocortex_adapter_sdk::{manifest::validate_unit, BatchUnit};
use exocortex_ingest::IngestServer;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalKey, ExternalSnapshotInfo, IngestBatch,
    MemoryDraft, ProducerIdentity, RegisterSourceRequest, RejectCode, RelationshipDraft,
};

const KEY: [u8; 32] = [9u8; 32];

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(Arc::new(InMemoryStorage::new(onto.clone())), onto, KEY)
}

async fn registered_server() -> IngestServer<InMemoryStorage> {
    let srv = server();
    let mut r = RegisterSourceRequest {
        default_rights: None,
        org_id: "org".into(),
        source_uri: "custom://parity".into(),
        producer_id: "parity".into(),
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
    exocortex_wire::signing::sign_registration(&KEY, &mut r);
    srv.register_source(tonic::Request::new(r)).await.unwrap();
    srv
}

struct Row {
    name: &'static str,
    unit: BatchUnit,
    expected: Option<RejectCode>,
}

fn unit(memories: Vec<MemoryDraft>, relationships: Vec<RelationshipDraft>) -> BatchUnit {
    BatchUnit {
        batch_id_seed: "seed".into(),
        memories,
        relationships,
        snapshot: None,
        observed_at: std::time::UNIX_EPOCH,
    }
}

fn draft(key: &str, mt: &str, title: &str, vis: i32) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: key.into(),
        id: String::new(),
        memory_type: mt.into(),
        title: title.into(),
        content: "content".into(),
        tags: vec![],
        visibility: vis,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn rel(from: &str, to: &str, kind: &str) -> RelationshipDraft {
    RelationshipDraft {
        from_draft_key: from.into(),
        to_draft_key: to.into(),
        kind: kind.into(),
        strength: 0.0,
        confidence: 0.0,
        context: String::new(),
        visibility: 1,
        to_memory_id: String::new(),
    }
}

fn rows() -> Vec<Row> {
    vec![
        Row {
            name: "valid",
            unit: unit(vec![draft("k", "Fix", "a valid title", 1)], vec![]),
            expected: None,
        },
        Row {
            name: "unknown memory type",
            unit: unit(vec![draft("k", "NotAType", "a valid title", 1)], vec![]),
            expected: Some(RejectCode::UnknownMemoryType),
        },
        Row {
            name: "empty title",
            unit: unit(vec![draft("k", "Fix", "", 1)], vec![]),
            expected: Some(RejectCode::Unknown),
        },
        Row {
            name: "title over 200 chars",
            unit: unit(vec![draft("k", "Fix", &"x".repeat(201), 1)], vec![]),
            expected: Some(RejectCode::Unknown),
        },
        Row {
            name: "visibility widening",
            unit: unit(vec![draft("k", "Fix", "a valid title", 3)], vec![]),
            expected: Some(RejectCode::VisibilityWidening),
        },
        Row {
            name: "unknown visibility discriminant",
            unit: unit(vec![draft("k", "Fix", "a valid title", 99)], vec![]),
            expected: Some(RejectCode::VisibilityWidening),
        },
        Row {
            name: "unknown kind",
            unit: unit(
                vec![draft("a", "Fix", "a valid title", 1)],
                vec![rel("a", "a", "NotAKnownKind")],
            ),
            expected: Some(RejectCode::UnknownKind),
        },
        Row {
            name: "computed-only kind asserted by a producer",
            unit: unit(
                vec![draft("a", "Fix", "a valid title", 1)],
                vec![rel("a", "a", "SimilarTo")],
            ),
            expected: Some(RejectCode::ComputedKindRejected),
        },
        Row {
            name: "invalid type triple",
            unit: unit(
                vec![
                    draft("a", "Problem", "a valid title", 1),
                    draft("b", "Problem", "a valid title", 1),
                ],
                vec![rel("a", "b", "Fixes")],
            ),
            expected: Some(RejectCode::InvalidTypeTriple),
        },
    ]
}

/// One snapshot unit row: an external batch missing its ExternalKey.
fn missing_key_row() -> Row {
    let mut u = unit(vec![draft("k", "Fix", "a valid title", 1)], vec![]);
    u.snapshot = Some(ExternalSnapshotInfo {
        snapshot_id: "snap-1".into(),
        schema_hash: [1u8; 32].to_vec(),
        source_flavor: "custom".into(),
    });
    Row {
        name: "missing external key",
        unit: u,
        expected: Some(RejectCode::MissingExternalKey),
    }
}

/// The server-side verdict for one row's unit: submit-shaped batches fed
/// through the D21-b preflight RPC (the real Submit validators, zero
/// commit), reduced to the first rejection's code.
async fn server_code(srv: &IngestServer<InMemoryStorage>, row: &Row) -> Option<RejectCode> {
    let memories = row
        .unit
        .memories
        .iter()
        .map(|m| {
            let mut m = m.clone();
            if row.unit.snapshot.is_some() && m.external_key.is_none() {
                // Keep the missing-key case missing; give every OTHER
                // snapshot row its key so the row under test is the only
                // reject.
                if m.draft_key != "k" {
                    m.external_key = Some(ExternalKey {
                        table_uuid: vec![1u8; 16],
                        logical_pk: m.draft_key.clone(),
                        mapping_version: 1,
                    });
                }
            }
            m
        })
        .collect();
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://parity".into(),
        producer_id: "parity".into(),
        batch_id: format!("manifest-{}", row.name),
        mapping_version: "custom:1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 1,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: row.unit.snapshot.clone(),
        memories,
        relationships: row.unit.relationships.clone(),
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    let _ = ExternalKey {
        table_uuid: vec![],
        logical_pk: String::new(),
        mapping_version: 0,
    };
    exocortex_wire::signing::prepare_batch(&KEY, &mut b);
    let ack = srv
        .preflight(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    ack.rejections
        .first()
        .and_then(|r| RejectCode::try_from(r.code).ok())
}

#[tokio::test]
async fn manifest_verdicts_agree_row_for_row() {
    let srv = registered_server().await;
    // The manifest the SDK would hold: compiled by the server, parsed by
    // the wire reader — no kernel types cross the boundary.
    let manifest_doc = srv
        .get_validation_manifest(tonic::Request::new(
            exocortex_wire::ingest::v1::ManifestRequest {
                org_id: "org".into(),
                source_uri: "custom://parity".into(),
                producer_id: "parity".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let manifest = exocortex_wire::manifest::parse_manifest(&manifest_doc.manifest_json).unwrap();
    assert_eq!(
        manifest.registered_ceiling,
        Some(1),
        "the registered ceiling rides the manifest"
    );

    let mut all = rows();
    all.push(missing_key_row());
    for row in &all {
        let local: Option<RejectCode> = validate_unit(&manifest, 1, &row.unit)
            .first()
            .map(|r| r.code);
        let remote = server_code(&srv, row).await;
        assert_eq!(
            local, remote,
            "row `{}`: manifest interpreter and Submit verdicts must agree",
            row.name
        );
        assert_eq!(local, row.expected, "row `{}`: expected verdict", row.name);
    }
}

/// A manifest whose compatibility fingerprint does not match the
/// server's is refused by the READER contract: the envelope comparison
/// happens in the SDK session; here we pin that the manifest itself is
/// self-describing (its hex fingerprint is the server's) so a stale copy
/// is detectable, and that parse_manifest refuses unknown schemes.
#[tokio::test]
async fn manifest_is_fingerprinted_and_scheme_checked() {
    let srv = registered_server().await;
    let doc = srv
        .get_validation_manifest(tonic::Request::new(
            exocortex_wire::ingest::v1::ManifestRequest {
                org_id: "org".into(),
                source_uri: "custom://parity".into(),
                producer_id: "parity".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    // The envelope and the embedded document agree, and both equal the
    // fingerprint the Fingerprint RPC reports.
    assert_eq!(
        doc.compatibility_fingerprint,
        srv.ontology.fingerprint.0.to_vec()
    );
    let manifest = exocortex_wire::manifest::parse_manifest(&doc.manifest_json).unwrap();
    let mut hex = String::with_capacity(64);
    for byte in srv.ontology.fingerprint.0 {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    assert_eq!(manifest.compatibility_fingerprint, hex);
    // Unknown schemes are refused, never best-effort parsed.
    let bad = doc
        .manifest_json
        .replace("\"manifest_version\":1", "\"manifest_version\":99");
    assert!(exocortex_wire::manifest::parse_manifest(&bad).is_err());
}
