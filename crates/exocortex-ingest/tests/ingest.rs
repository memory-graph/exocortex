//! M6 ingest tests (§18.8): HMAC-first rejection, the §7.13 pipeline order
//! (fingerprint, source admission, no-widening, triples, idempotency),
//! entity extraction, and end-to-end submit against `InMemoryStorage`.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalKey, IngestBatch, MemoryDraft, ProducerIdentity,
    RegisterSourceRequest, RejectCode,
};

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
}

fn draft(key: &str, mt: &str, vis: i32) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: key.into(),
        id: String::new(),
        memory_type: mt.into(),
        title: format!("title {key}"),
        content: "Fixed in src/auth.rs with cargo build".to_string(),
        tags: vec!["auth".into()],
        visibility: vis,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn signed_batch(srv: &IngestServer<InMemoryStorage>, memories: Vec<MemoryDraft>) -> IngestBatch {
    let mut b = batch(memories);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    sign(b, [5u8; 32])
}

fn batch(memories: Vec<MemoryDraft>) -> IngestBatch {
    IngestBatch {
        org_id: "org".into(),
        source_uri: "session://s1".into(),
        producer_id: "session-wrapup".into(),
        batch_id: format!("b-{}", std::process::id()),
        mapping_version: "session-wrapup:1.0.0".into(),
        ontology_fingerprint: Vec::new(),
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
    }
}

fn sign(mut b: IngestBatch, key: [u8; 32]) -> IngestBatch {
    exocortex_wire::signing::prepare_batch(&key, &mut b);
    b
}

fn hex_id(id: exocortex_kernel::MemoryId) -> String {
    use std::fmt::Write as _;
    id.0.iter()
        .fold(String::with_capacity(32), |mut out, byte| {
            write!(out, "{byte:02x}").unwrap();
            out
        })
}

async fn registered(srv: &IngestServer<InMemoryStorage>, ceiling: i32) {
    use tonic::Request;
    srv.register_source(Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://s1",
        "session-wrapup",
        ceiling,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    )))
    .await
    .unwrap();
}

// D21-a: table-shaped sources register with a declared projection whose
// derived schema hash the test snapshots carry verbatim.
fn table_columns() -> Vec<(String, String)> {
    vec![
        ("row_key".to_string(), "string".to_string()),
        ("payload".to_string(), "string".to_string()),
    ]
}

fn table_schema_hash_bytes() -> Vec<u8> {
    let hex = exocortex_ingest::service::projection_schema_hash(&table_columns());
    (0..32)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

fn register_table_source(srv: &IngestServer<InMemoryStorage>, source_uri: &str, producer_id: &str) {
    use exocortex_wire::ingest::v1::{
        ProjectionBounds, ProjectionDescriptor, ProjectionField, SourceColumn,
    };
    let columns = table_columns();
    let mut r = RegisterSourceRequest {
        default_rights: None,
        org_id: "org".into(),
        source_uri: source_uri.into(),
        producer_id: producer_id.into(),
        ceiling: 3,
        source_flavor: "iceberg".into(),
        producer_kind: 4,
        producer: Some(ProducerIdentity {
            node_id: "test-node".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
        projection: Some(ProjectionDescriptor {
            selector: "table:all".into(),
            fields: vec![ProjectionField {
                source_field: "row_key".into(),
                memory_type: "Fix".into(),
                kind: String::new(),
            }],
            source_schema: columns
                .iter()
                .map(|(n, t)| SourceColumn {
                    name: n.clone(),
                    data_type: t.clone(),
                })
                .collect(),
            mapping_version: 1,
            bounds: Some(ProjectionBounds {
                max_rows_per_window: 256,
                max_rows_per_run: 100_000,
                max_graph_share_percent: 50,
            }),
            last_snapshot_id: "s1".into(),
        }),
    };
    exocortex_wire::signing::sign_registration(&[5u8; 32], &mut r);
    futures::executor::block_on(async {
        srv.register_source(tonic::Request::new(r)).await.unwrap();
    });
}

#[tokio::test]
async fn e2e_valid_batch_accepted_with_monotonic_lsn() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(
        &srv,
        vec![
            draft("k1", "Fix", 1),
            draft("k2", "Problem", 2),
            draft("k3", "Solution", 3),
        ],
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    if ack.accepted != 3 {
        panic!("rejections: {:#?}", ack.rejections);
    }
    assert!(ack.assigned_lsn > 0);
}

#[tokio::test]
async fn saturated_submit_capacity_emits_rate_limited_without_storage_work() {
    let srv = server().with_submit_concurrency_limit(0);
    registered(&srv, 3).await;
    let before = srv.storage.take_read_counts();
    let ack = srv
        .submit(tonic::Request::new(signed_batch(
            &srv,
            vec![draft("rate-limited", "Fix", 1)],
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert_eq!(ack.rejected, 1);
    assert!(!ack.rejections.is_empty());
    assert!(ack
        .rejections
        .iter()
        .all(|row| row.code == RejectCode::RateLimited as i32));
    assert_eq!(srv.storage.take_read_counts(), before);
}

#[tokio::test]
async fn concurrent_identical_submits_have_one_durable_winner() {
    let srv = Arc::new(server());
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("concurrent", "Fix", 1)]);
    let (left, right) = tokio::join!(
        srv.submit(tonic::Request::new(b.clone())),
        srv.submit(tonic::Request::new(b)),
    );
    let acks = [left.unwrap().into_inner(), right.unwrap().into_inner()];
    assert_eq!(
        acks.iter()
            .filter(|ack| ack
                .rejections
                .iter()
                .any(|row| row.code == RejectCode::DuplicateBatch as i32))
            .count(),
        1,
        "exactly one concurrent submit must replay the durable settlement"
    );
    assert_eq!(acks[0].assigned_lsn, acks[1].assigned_lsn);
}

#[tokio::test]
async fn external_target_resolution_is_one_bounded_backend_read() {
    let srv = server();
    registered(&srv, 3).await;

    let mut seed = signed_batch(&srv, vec![draft("target", "Problem", 1)]);
    seed.batch_id = "target-seed".into();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut seed);
    assert!(srv
        .submit(tonic::Request::new(seed))
        .await
        .unwrap()
        .into_inner()
        .rejections
        .is_empty());
    use futures::StreamExt;
    let mut memories = srv.storage.stream_all_memories().await;
    let mut target = None;
    while let Some(memory) = memories.next().await {
        let memory = memory.unwrap();
        if memory.title == "title target" {
            target = Some(memory.id);
            break;
        }
    }
    let target = target.expect("seed target persisted");
    use std::fmt::Write as _;
    let target_id = target.0.iter().fold(String::new(), |mut encoded, byte| {
        write!(encoded, "{byte:02x}").expect("writing to String is infallible");
        encoded
    });
    srv.storage.take_read_counts();

    let memories: Vec<_> = (0..32)
        .map(|index| draft(&format!("source-{index}"), "Fix", 1))
        .collect();
    let mut batch = signed_batch(&srv, memories);
    batch.batch_id = "bounded-resolution".into();
    batch.relationships = (0..32)
        .map(|index| exocortex_wire::ingest::v1::RelationshipDraft {
            from_draft_key: format!("source-{index}"),
            to_draft_key: String::new(),
            kind: "Fixes".into(),
            strength: 0.8,
            confidence: 0.8,
            context: String::new(),
            visibility: 1,
            to_memory_id: target_id.clone(),
        })
        .collect();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut batch);
    let ack = srv
        .submit(tonic::Request::new(batch))
        .await
        .unwrap()
        .into_inner();
    assert!(ack.rejections.is_empty(), "batch rejected: {ack:?}");
    assert_eq!(
        srv.storage.take_read_counts(),
        (0, 1),
        "target cardinality must not produce point-read N+1"
    );
}

#[tokio::test]
async fn missing_hmac_rejected_before_anything() {
    let srv = server();
    registered(&srv, 3).await;
    let mut b = batch(vec![draft("k", "Fix", 1)]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    // Not signed.
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::Unauthorized as i32),
        "R-I8: HMAC first"
    );
}

#[tokio::test]
async fn empty_content_emits_unknown_reject_code() {
    let srv = server();
    registered(&srv, 3).await;
    let mut invalid = draft("unknown", "Fix", 1);
    invalid.content.clear();
    let ack = srv
        .submit(tonic::Request::new(signed_batch(&srv, vec![invalid])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(ack
        .rejections
        .iter()
        .any(|row| row.code == RejectCode::Unknown as i32));
}

#[test]
fn reject_code_target_manifest_is_exhaustive_and_executable() {
    // §23 #27: each entry is exercised by a targeted Submit batch in this
    // integration-test target or the adjacent e2e/external-key targets. Keep
    // this match wildcard-free so adding a protocol code cannot silently
    // escape the executable rejection matrix.
    fn targeted_test(code: RejectCode) -> (&'static str, &'static str) {
        match code {
            RejectCode::Unknown => ("ingest", "empty_content_emits_unknown_reject_code"),
            RejectCode::IncompatibleOntology => {
                ("ingest", "fingerprint_mismatch_rejects_whole_batch")
            }
            RejectCode::UnknownSource => ("ingest", "unregistered_source_rejected"),
            RejectCode::UnknownMemoryType => ("ingest", "unknown_memory_type_rejected"),
            RejectCode::UnknownKind => ("e2e", "inverse_materialized_on_write"),
            RejectCode::InvalidTypeTriple => {
                ("e2e", "bad_triple_rejects_whole_batch_naming_the_key")
            }
            RejectCode::VisibilityWidening => (
                "ingest",
                "visibility_widening_rejected_under_lowered_ceiling",
            ),
            RejectCode::MissingExternalKey => (
                "ingest",
                "external_batch_without_key_rejected_and_with_key_deterministic",
            ),
            RejectCode::DuplicateBatch => ("ingest", "duplicate_batch_is_idempotent_replay"),
            RejectCode::BadChecksum => ("external_key", "bad_checksum_is_rejected"),
            RejectCode::Unauthorized => ("ingest", "missing_hmac_rejected_before_anything"),
            RejectCode::RateLimited => (
                "ingest",
                "saturated_submit_capacity_emits_rate_limited_without_storage_work",
            ),
            RejectCode::ComputedKindRejected => ("e2e", "computed_only_kind_rejected_at_ingest"),
            RejectCode::InvalidExternalKey => (
                "external_key",
                "malformed_external_coordinates_are_rejected",
            ),
            RejectCode::ResourceLimitExceeded => (
                "ingest",
                "resource_ceiling_rejects_before_ontology_or_storage_work",
            ),
            RejectCode::SourceRewound => (
                "schema_evolution",
                "rewound_snapshot_is_rejected_with_its_own_code",
            ),
            RejectCode::SchemaDrift => (
                "schema_evolution",
                "drifted_schema_hash_rejects_every_row_until_re_registration",
            ),
            RejectCode::ProjectionBoundExceeded => (
                "schema_evolution",
                "bound_exceeded_rejects_the_batch_naming_the_bound",
            ),
        }
    }

    let codes: Vec<_> = (0..=14)
        .filter_map(|value| RejectCode::try_from(value).ok())
        .collect();
    assert_eq!(
        codes.len(),
        15,
        "wire RejectCode discriminants must stay dense"
    );
    let sources = [
        ("ingest", include_str!("ingest.rs")),
        ("e2e", include_str!("e2e.rs")),
        ("external_key", include_str!("external_key.rs")),
    ];
    for code in codes {
        let (target, test) = targeted_test(code);
        let source = sources
            .iter()
            .find_map(|(name, source)| (*name == target).then_some(*source))
            .unwrap();
        let marker = format!("async fn {test}");
        let tail = source
            .split_once(&marker)
            .unwrap_or_else(|| panic!("{code:?} target {target}::{test} is missing"))
            .1;
        let body = tail.split("\n#[tokio::test]").next().unwrap();
        assert!(
            body.contains("submit("),
            "{code:?} target {target}::{test} does not execute Submit"
        );
        assert!(
            body.contains(&format!("RejectCode::{code:?}")),
            "{code:?} target {target}::{test} does not assert the exact code"
        );
    }
}

#[tokio::test]
async fn resource_ceiling_rejects_before_ontology_or_storage_work() {
    let srv = server();
    let edge = exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "a".into(),
        to_draft_key: "b".into(),
        kind: "Fixes".into(),
        strength: 1.0,
        confidence: 1.0,
        context: String::new(),
        visibility: 1,
        to_memory_id: String::new(),
    };

    let mut exact = batch(vec![]);
    exact.relationships = vec![edge.clone(); exocortex_wire::limits::MAX_EDGES_PER_BATCH];
    let exact = sign(exact, [5u8; 32]);
    let exact_ack = srv
        .submit(tonic::Request::new(exact))
        .await
        .unwrap()
        .into_inner();
    assert!(exact_ack
        .rejections
        .iter()
        .all(|row| row.code == RejectCode::IncompatibleOntology as i32));

    let mut oversized = batch(vec![]);
    oversized.relationships = vec![edge; exocortex_wire::limits::MAX_EDGES_PER_BATCH + 1];
    let oversized = sign(oversized, [5u8; 32]);
    let oversized_ack = srv
        .submit(tonic::Request::new(oversized))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(oversized_ack.accepted, 0);
    assert!(oversized_ack
        .rejections
        .iter()
        .all(|row| row.code == RejectCode::ResourceLimitExceeded as i32));
}

#[tokio::test]
async fn authenticated_principal_is_required_and_authoritatively_scopes_writes() {
    use futures::StreamExt;
    use tonic::Code;

    let srv = server().require_request_principal();
    let principal = exocortex_storage::VisibilityContext {
        user_id: "alice".into(),
        org_id: "org".into(),
        project_ids: ["allowed".into()].into_iter().collect(),
        team_ids: ["team-a".into()].into_iter().collect(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };

    let unauthenticated = srv
        .fingerprint(tonic::Request::new(
            exocortex_wire::ingest::v1::FingerprintRequest {},
        ))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let mut registration = tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://s1",
        "session-wrapup",
        3,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    ));
    registration.extensions_mut().insert(principal.clone());
    srv.register_source(registration).await.unwrap();

    let scoped_batch = |project_id: &str, batch_id: &str| {
        let mut b = batch(vec![draft("scoped", "Fix", 1)]);
        b.batch_id = batch_id.into();
        b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
        b.producer.as_mut().unwrap().client_metadata =
            Some(exocortex_wire::ingest::v1::ClientMetadata {
                playbook_version: String::new(),
                client_version: String::new(),
                harness_hint: String::new(),
                project_id: project_id.into(),
                team_id: String::new(),
            });
        sign(b, [5u8; 32])
    };

    let mut forbidden = tonic::Request::new(scoped_batch("forbidden", "scope-forbidden"));
    forbidden.extensions_mut().insert(principal.clone());
    let denied = srv.submit(forbidden).await.unwrap().into_inner();
    assert!(denied
        .rejections
        .iter()
        .any(|row| row.code == RejectCode::VisibilityWidening as i32));

    let mut allowed = tonic::Request::new(scoped_batch("allowed", "scope-allowed"));
    allowed.extensions_mut().insert(principal);
    let accepted = srv.submit(allowed).await.unwrap().into_inner();
    assert!(accepted.accepted > 0, "{:?}", accepted.rejections);

    let mut memories = srv.storage.stream_all_memories().await;
    let scoped = loop {
        let memory = memories.next().await.unwrap().unwrap();
        if memory.title.as_str() == "title scoped" {
            break memory;
        }
    };
    assert_eq!(scoped.context.tenant_id.as_deref(), Some("org"));
    assert_eq!(scoped.context.user_id.as_deref(), Some("alice"));
    assert_eq!(scoped.context.project_id.as_deref(), Some("allowed"));
}

#[tokio::test]
async fn personal_mode_accepts_authenticated_dynamic_project_scope() {
    let srv = server().allow_personal_scopes().require_request_principal();
    let principal = exocortex_storage::VisibilityContext {
        user_id: "personal-user".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let mut registration = tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://personal",
        "session-wrapup",
        3,
        "session",
        "personal-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    ));
    registration.extensions_mut().insert(principal.clone());
    srv.register_source(registration).await.unwrap();

    let mut batch = batch(vec![draft("personal", "Fix", 1)]);
    batch.source_uri = "session://personal".into();
    batch.batch_id = "personal-project".into();
    batch.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    batch.producer.as_mut().unwrap().client_metadata =
        Some(exocortex_wire::ingest::v1::ClientMetadata {
            playbook_version: String::new(),
            client_version: String::new(),
            harness_hint: String::new(),
            project_id: "project-created-locally".into(),
            team_id: String::new(),
        });
    let mut request = tonic::Request::new(sign(batch, [5u8; 32]));
    request.extensions_mut().insert(principal);
    let accepted = srv.submit(request).await.unwrap().into_inner();
    assert_eq!(accepted.accepted, 1, "{:?}", accepted.rejections);
}

#[tokio::test]
async fn authenticated_relationship_target_must_be_in_caller_membership() {
    use futures::StreamExt;

    let srv = server().require_request_principal();
    let broad = exocortex_storage::VisibilityContext {
        user_id: "alice".into(),
        org_id: "org".into(),
        project_ids: ["allowed".into(), "forbidden".into()].into_iter().collect(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let mut registration = tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://s1",
        "session-wrapup",
        3,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    ));
    registration.extensions_mut().insert(broad.clone());
    srv.register_source(registration).await.unwrap();

    let mut seed = batch(vec![draft("target", "Problem", 1)]);
    seed.batch_id = "membership-target".into();
    seed.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    seed.producer.as_mut().unwrap().client_metadata =
        Some(exocortex_wire::ingest::v1::ClientMetadata {
            project_id: "forbidden".into(),
            ..Default::default()
        });
    let mut seed = tonic::Request::new(sign(seed, [5u8; 32]));
    seed.extensions_mut().insert(broad);
    assert!(srv
        .submit(seed)
        .await
        .unwrap()
        .into_inner()
        .rejections
        .is_empty());

    let mut rows = srv.storage.stream_all_memories().await;
    let target = loop {
        let row = rows.next().await.unwrap().unwrap();
        if row.title.as_str() == "title target" {
            break row.id;
        }
    };
    use std::fmt::Write as _;
    let target_id = target.0.iter().fold(String::new(), |mut encoded, byte| {
        write!(encoded, "{byte:02x}").expect("writing to String is infallible");
        encoded
    });

    let mut link = batch(vec![draft("fix", "Fix", 1)]);
    link.batch_id = "membership-link".into();
    link.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    link.producer.as_mut().unwrap().client_metadata =
        Some(exocortex_wire::ingest::v1::ClientMetadata {
            project_id: "allowed".into(),
            ..Default::default()
        });
    link.relationships = vec![exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "fix".into(),
        to_memory_id: target_id,
        kind: "Fixes".into(),
        strength: 0.8,
        confidence: 0.8,
        visibility: 1,
        ..Default::default()
    }];
    let allowed = exocortex_storage::VisibilityContext {
        user_id: "alice".into(),
        org_id: "org".into(),
        project_ids: ["allowed".into()].into_iter().collect(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let mut link = tonic::Request::new(sign(link, [5u8; 32]));
    link.extensions_mut().insert(allowed);
    let ack = srv.submit(link).await.unwrap().into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(ack.rejections.iter().any(|row| {
        row.code == RejectCode::VisibilityWidening as i32
            && row.detail.contains("outside the authenticated membership")
    }));
}

#[tokio::test]
async fn unknown_source_ceiling_discriminants_fail_closed() {
    use tonic::Code;

    let srv = server();
    let registration = exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://s1",
        "session-wrapup",
        99,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    let error = srv
        .register_source(tonic::Request::new(registration))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    registered(&srv, 3).await;
    let mut submitted = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    submitted.ceiling = 99;
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut submitted);
    let ack = srv
        .submit(tonic::Request::new(submitted))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(ack.rejections.iter().all(|row| {
        row.code == RejectCode::UnknownSource as i32
            && row.detail.contains("unknown source ceiling")
    }));
}

#[tokio::test]
async fn fingerprint_mismatch_rejects_whole_batch() {
    let srv = server();
    registered(&srv, 3).await;
    // Sign with the wrong fingerprint already in place so the HMAC is
    // valid and the rejection isolates the fingerprint gate.
    let mut b = batch(vec![draft("k", "Fix", 1)]);
    b.ontology_fingerprint = vec![1, 2, 3];
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::IncompatibleOntology as i32));
}

#[tokio::test]
async fn unregistered_source_rejected() {
    let srv = server();
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::UnknownSource as i32));
}

#[tokio::test]
async fn visibility_widening_rejected_under_lowered_ceiling() {
    let srv = server();
    registered(&srv, 1).await; // Project ceiling
                               // Team visibility under a Project ceiling. The batch ceiling must
                               // equal the registered value (R-I3) so the rejection is the widening,
                               // not the source mismatch.
    let mut b = batch(vec![draft("k", "Fix", 2)]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.ceiling = 1;
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::VisibilityWidening as i32),
        "R-T11a: Team under a Project ceiling"
    );
}

#[tokio::test]
async fn relationship_visibility_is_derived_from_narrowest_endpoint() {
    use futures::StreamExt;

    let srv = server();
    registered(&srv, 3).await;
    let mut b = batch(vec![draft("fix", "Fix", 1), draft("problem", "Problem", 0)]);
    b.batch_id = "authoritative-relationship-visibility".into();
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.relationships = vec![exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "fix".into(),
        to_draft_key: "problem".into(),
        kind: "Fixes".into(),
        visibility: 3,
        strength: 0.8,
        confidence: 0.8,
        ..Default::default()
    }];
    let ack = srv
        .submit(tonic::Request::new(sign(b, [5u8; 32])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.rejected, 0, "{:?}", ack.rejections);

    let fixes = srv.ontology.kind_id("Fixes").unwrap();
    let mut relationships = srv.storage.stream_all_relationships().await;
    let edge = loop {
        let edge = relationships.next().await.unwrap().unwrap();
        if edge.kind == fixes {
            break edge;
        }
    };
    assert_eq!(edge.visibility, exocortex_kernel::Visibility::Private);
}

#[tokio::test]
async fn unknown_memory_type_rejected() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "NotAType", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::UnknownMemoryType as i32));
}

#[tokio::test]
async fn external_batch_without_key_rejected_and_with_key_deterministic() {
    let srv = server();
    register_table_source(&srv, "session://s1", "session-wrapup");
    let mut d = draft("k", "Fix", 1);
    d.external_key = None;
    let mut b = batch(vec![d]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash: table_schema_hash_bytes(),
        source_flavor: "iceberg".into(),
    });
    let b = sign(b, [5u8; 32]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .any(|r| r.code == RejectCode::MissingExternalKey as i32));

    // With the key: identity is deterministic (R-T18a).
    let mut d2 = draft("k", "Fix", 1);
    d2.external_key = Some(ExternalKey {
        table_uuid: vec![1; 16],
        logical_pk: "row-1".into(),
        mapping_version: 3,
    });
    let mut b2 = batch(vec![d2]);
    b2.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b2.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash: table_schema_hash_bytes(),
        source_flavor: "iceberg".into(),
    });
    let b2 = sign(b2, [5u8; 32]);
    let ack2 = srv
        .submit(tonic::Request::new(b2))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack2.accepted, 1);

    let id_a =
        exocortex_kernel::MemoryId::from_external("org", "session://s1", &[1u8; 16], b"row-1", 3);
    let id_b =
        exocortex_kernel::MemoryId::from_external("org", "session://s1", &[1u8; 16], b"row-1", 3);
    assert_eq!(id_a, id_b, "deterministic identity");
    let id_c =
        exocortex_kernel::MemoryId::from_external("org", "session://s1", &[1u8; 16], b"row-1", 4);
    assert_ne!(id_a, id_c, "mapping_version bump forks identity");
}

#[tokio::test]
async fn authenticated_temporal_and_external_edge_provenance_survive_exactly() {
    use futures::StreamExt;

    let srv = server();
    register_table_source(&srv, "iceberg://catalog/table", "iceberg-adapter");

    let observed = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
    let recorded = std::time::UNIX_EPOCH + std::time::Duration::from_secs(200);
    let valid_from = std::time::UNIX_EPOCH + std::time::Duration::from_secs(110);
    let valid_until = std::time::UNIX_EPOCH + std::time::Duration::from_secs(150);
    let mut from = draft("fix", "Fix", 3);
    from.valid_from = Some(valid_from.into());
    from.valid_until = Some(valid_until.into());
    from.external_key = Some(ExternalKey {
        table_uuid: vec![7; 16],
        logical_pk: "fix-row".into(),
        mapping_version: 4,
    });
    let mut to = draft("problem", "Problem", 3);
    to.external_key = Some(ExternalKey {
        table_uuid: vec![7; 16],
        logical_pk: "problem-row".into(),
        mapping_version: 4,
    });
    let mut b = batch(vec![from, to]);
    b.source_uri = "iceberg://catalog/table".into();
    b.producer_id = "iceberg-adapter".into();
    b.batch_id = "temporal-external".into();
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.observed_at = Some(observed.into());
    b.recorded_at = Some(recorded.into());
    b.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "snapshot-9".into(),
        schema_hash: table_schema_hash_bytes(),
        source_flavor: "iceberg".into(),
    });
    b.relationships = vec![exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "fix".into(),
        to_draft_key: "problem".into(),
        kind: "Fixes".into(),
        strength: 0.9,
        confidence: 0.8,
        visibility: 3,
        ..Default::default()
    }];
    let ack = srv
        .submit(tonic::Request::new(sign(b, [5u8; 32])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 3, "only two drafts and one authored edge");
    assert_eq!(ack.rejected, 0, "{:?}", ack.rejections);

    let expected_observed = chrono::DateTime::from_timestamp(100, 0).unwrap();
    let expected_recorded = chrono::DateTime::from_timestamp(200, 0).unwrap();
    let expected_from = chrono::DateTime::from_timestamp(110, 0).unwrap();
    let expected_until = chrono::DateTime::from_timestamp(150, 0).unwrap();
    let mut memory_stream = srv.storage.stream_all_memories().await;
    let mut producer_memories = Vec::new();
    while let Some(Ok(memory)) = memory_stream.next().await {
        producer_memories.push(memory);
    }
    assert_eq!(
        producer_memories.len(),
        2,
        "iceberg flavor does not session-group"
    );
    let fix = producer_memories
        .iter()
        .find(|memory| memory.title.as_str() == "title fix")
        .unwrap();
    assert_eq!(fix.context.timestamp, expected_observed);
    assert_eq!(fix.valid_from, expected_from);
    assert_eq!(fix.valid_until, Some(expected_until));
    assert_eq!(fix.recorded_at, expected_recorded);
    let exocortex_kernel::Provenance::ExternalSnapshot(snapshot) = &fix.provenance else {
        panic!("external memory lost snapshot provenance")
    };
    assert_eq!(snapshot.observed_at, expected_observed);
    assert_eq!(snapshot.snapshot_id.as_str(), "snapshot-9");
    assert_eq!(snapshot.external_key.logical_pk, b"fix-row");

    let fixes = srv.ontology.kind_id("Fixes").unwrap();
    let mut relationship_stream = srv.storage.stream_all_relationships().await;
    let edge = loop {
        let edge = relationship_stream.next().await.unwrap().unwrap();
        if edge.kind == fixes {
            break edge;
        }
    };
    assert_eq!(edge.valid_from, expected_recorded);
    assert_eq!(edge.recorded_at, expected_recorded);
    let exocortex_kernel::Provenance::ExternalSnapshot(snapshot) = edge.provenance else {
        panic!("external relationship lost snapshot provenance")
    };
    assert_eq!(snapshot.observed_at, expected_observed);
    assert_eq!(snapshot.external_key.logical_pk, b"fix-row");

    let mut malformed = batch(vec![draft("bad", "Fix", 3)]);
    malformed.source_uri = "iceberg://catalog/table".into();
    malformed.producer_id = "iceberg-adapter".into();
    malformed.batch_id = "malformed-temporal".into();
    malformed.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    malformed.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "snapshot-10".into(),
        schema_hash: vec![10; 32],
        source_flavor: "iceberg".into(),
    });
    malformed.memories[0].external_key = Some(ExternalKey {
        table_uuid: vec![8; 16],
        logical_pk: "bad".into(),
        mapping_version: 1,
    });
    malformed.recorded_at = Some(recorded.into());
    malformed.recorded_at.as_mut().unwrap().nanos = 1_000_000_000;
    let rejected = srv
        .submit(tonic::Request::new(sign(malformed, [5u8; 32])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((rejected.accepted, rejected.rejected), (0, 1));
    let mut after = srv.storage.stream_all_memories().await;
    let mut after_count = 0;
    while let Some(Ok(_)) = after.next().await {
        after_count += 1;
    }
    assert_eq!(after_count, 2, "malformed time mutates nothing");
}

#[tokio::test]
async fn grpc_storage_failures_do_not_expose_backend_details() {
    use futures::StreamExt;
    use tonic::Code;

    let srv = server();
    registered(&srv, 3).await;
    let mut seed = signed_batch(&srv, vec![draft("target", "Problem", 3)]);
    seed.batch_id = "redaction-seed".into();
    seed = sign(seed, [5u8; 32]);
    assert_eq!(
        srv.submit(tonic::Request::new(seed))
            .await
            .unwrap()
            .into_inner()
            .accepted,
        1
    );
    let mut memories = srv.storage.stream_all_memories().await;
    let target = loop {
        let memory = memories.next().await.unwrap().unwrap();
        if memory.title.as_str() == "title target" {
            break memory.id;
        }
    };

    srv.storage.fail_next_batch_read();
    let mut read = signed_batch(&srv, vec![draft("from", "Fix", 3)]);
    read.batch_id = "redaction-read".into();
    read.relationships = vec![exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "from".into(),
        to_memory_id: hex_id(target),
        kind: "Fixes".into(),
        strength: 0.8,
        confidence: 0.8,
        visibility: 3,
        ..Default::default()
    }];
    read = sign(read, [5u8; 32]);
    let read_error = srv.submit(tonic::Request::new(read)).await.unwrap_err();
    assert_eq!(read_error.code(), Code::Internal);
    assert_eq!(read_error.message(), "internal storage error");
    assert!(!read_error.message().contains("credential"));

    srv.storage.fail_next_ingest_commit();
    let mut commit = signed_batch(&srv, vec![draft("commit", "Fix", 3)]);
    commit.batch_id = "redaction-commit".into();
    commit = sign(commit, [5u8; 32]);
    let commit_error = srv.submit(tonic::Request::new(commit)).await.unwrap_err();
    assert_eq!(commit_error.code(), Code::Internal);
    assert_eq!(commit_error.message(), "internal storage error");
    assert!(!commit_error.message().contains("credential"));
}

#[tokio::test]
async fn duplicate_batch_is_idempotent_replay() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let first = srv
        .submit(tonic::Request::new(b.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.accepted, 1);
    let second = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        second
            .rejections
            .iter()
            .any(|r| r.code == RejectCode::DuplicateBatch as i32),
        "replay short-circuits"
    );
}

#[tokio::test]
async fn durable_post_ingest_effects_release_admission_and_drain_once() {
    let base = server().with_submit_concurrency_limit(1);
    let dreams = Arc::new(exocortex_dreams::DreamsEngine::new(
        base.storage.clone(),
        exocortex_dreams::trigger::DreamsTrigger {
            memory_threshold: u32::MAX,
            edge_threshold: u32::MAX,
            age_floor_days: u32::MAX,
            min_interval_hours: 0,
        },
        0.01,
        0.05,
        false,
        "outbox-test".into(),
    ));
    let srv = Arc::new(base.with_dreams(dreams.clone()));
    registered(&srv, 3).await;

    let mut first = batch(vec![draft("first", "Fix", 1)]);
    first.batch_id = "outbox-first".into();
    first.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    let first = sign(first, [5; 32]);
    let mut second = batch(vec![draft("second", "Fix", 1)]);
    second.batch_id = "outbox-second".into();
    second.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    let second = sign(second, [5; 32]);

    assert!(
        srv.submit(tonic::Request::new(first.clone()))
            .await
            .unwrap()
            .into_inner()
            .accepted
            > 0
    );
    assert!(
        srv.submit(tonic::Request::new(second))
            .await
            .unwrap()
            .into_inner()
            .accepted
            > 0
    );
    assert_eq!(
        srv.storage.pending_ingest_effects(10).await.unwrap().len(),
        2,
        "submit completion leaves durable effects for the supervised drainer"
    );

    let drainer = tokio::spawn(srv.clone().run_post_ingest_effects());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if srv
                .storage
                .pending_ingest_effects(10)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable effects drain");
    let memories: u32 = dreams
        .counters
        .iter()
        .map(|entry| entry.memories_since_last_cycle)
        .sum();
    assert_eq!(memories, 2);

    let duplicate = srv
        .submit(tonic::Request::new(first))
        .await
        .unwrap()
        .into_inner();
    assert!(duplicate
        .rejections
        .iter()
        .any(|row| row.code == RejectCode::DuplicateBatch as i32));
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;
    assert!(srv
        .storage
        .pending_ingest_effects(10)
        .await
        .unwrap()
        .is_empty());
    let memories_after_retry: u32 = dreams
        .counters
        .iter()
        .map(|entry| entry.memories_since_last_cycle)
        .sum();
    assert_eq!(memories_after_retry, 2);
    drainer.abort();
}

#[tokio::test]
async fn production_drainer_recovers_acknowledged_cleanup_after_failure() {
    let base = server();
    let dreams = Arc::new(exocortex_dreams::DreamsEngine::new(
        base.storage.clone(),
        exocortex_dreams::trigger::DreamsTrigger {
            memory_threshold: u32::MAX,
            edge_threshold: u32::MAX,
            age_floor_days: u32::MAX,
            min_interval_hours: 0,
        },
        0.01,
        0.05,
        false,
        "cleanup-recovery".into(),
    ));
    let srv = Arc::new(base.with_dreams(dreams.clone()));
    registered(&srv, 3).await;

    let mut submitted = batch(vec![draft("cleanup", "Fix", 1)]);
    submitted.batch_id = "cleanup-recovery".into();
    submitted.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    assert_eq!(
        srv.submit(tonic::Request::new(sign(submitted, [5; 32])))
            .await
            .unwrap()
            .into_inner()
            .accepted,
        1
    );
    let effect = srv.storage.pending_ingest_effects(1).await.unwrap()[0].clone();
    let claimed = srv
        .storage
        .claim_ingest_effect("crashed-worker", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.effect, effect);
    for delta in &effect.region_deltas {
        dreams
            .on_writes_once(
                effect.effect_id.as_str(),
                claimed.delivery_generation,
                delta.region.clone(),
                delta.memories,
                delta.relationships,
            )
            .await
            .unwrap();
    }
    assert!(srv
        .storage
        .acknowledge_ingest_effect(effect.effect_id.as_str(), "crashed-worker")
        .await
        .unwrap());
    assert_eq!(
        srv.storage
            .pending_ingest_effect_cleanups(1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(srv
        .storage
        .claim_ingest_effect("must-not-reexecute", 30_000)
        .await
        .unwrap()
        .is_none());

    srv.storage.fail_next_ingest_cleanup();
    let drainer = tokio::spawn(srv.clone().run_post_ingest_effects());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if srv
                .storage
                .pending_ingest_effect_cleanups(1)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("production drainer retries cleanup to completion");
    assert!(srv
        .storage
        .claim_ingest_effect("still-must-not-reexecute", 30_000)
        .await
        .unwrap()
        .is_none());
    let memories: u32 = dreams
        .counters
        .iter()
        .map(|entry| entry.memories_since_last_cycle)
        .sum();
    assert_eq!(memories, 1, "cleanup recovery must not replay the effect");
    drainer.abort();
}

#[tokio::test]
async fn durable_reasoning_effect_remains_pending_while_worker_queue_is_saturated() {
    let base = server();
    let reasoning = Arc::new(exocortex_reasoning::ReasoningEngine::new(
        base.storage.clone(),
        1,
        3,
    ));
    reasoning
        .enqueue(exocortex_reasoning::ReasoningWork::KHopOver {
            seed: exocortex_kernel::MemoryId::new_v7(),
            k: 1,
        })
        .await;
    let srv = Arc::new(base.with_reasoning(reasoning.clone()));
    registered(&srv, 3).await;

    let mut submitted = signed_batch(&srv, vec![draft("durable-reasoning", "Fix", 1)]);
    submitted.batch_id = "durable-reasoning-saturation".into();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut submitted);
    assert!(srv
        .submit(tonic::Request::new(submitted))
        .await
        .unwrap()
        .into_inner()
        .rejections
        .is_empty());

    let drainer = tokio::spawn(srv.clone().run_post_ingest_effects());
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;
    assert_eq!(
        srv.storage.pending_ingest_effects(10).await.unwrap().len(),
        1,
        "queue saturation must retain the outbox row until reasoning completes"
    );

    let worker = tokio::spawn(reasoning.run());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if srv
                .storage
                .pending_ingest_effects(10)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable reasoning completion acknowledges the outbox row");
    drainer.abort();
    worker.abort();
}

#[tokio::test(start_paused = true)]
async fn idle_effect_drainer_backs_off_and_new_commit_wakes_it() {
    let srv = Arc::new(server());
    registered(&srv, 3).await;
    srv.storage.take_pending_ingest_effect_reads();
    let drainer = tokio::spawn(srv.clone().run_post_ingest_effects());
    tokio::task::yield_now().await;

    tokio::time::advance(std::time::Duration::from_millis(900)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let idle_reads = srv.storage.take_pending_ingest_effect_reads();
    assert!(
        idle_reads <= 8,
        "exponential idle backoff should bound reads, observed {idle_reads}"
    );

    let mut submitted = signed_batch(&srv, vec![draft("notify-wakeup", "Fix", 1)]);
    submitted.batch_id = "notify-wakeup".into();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut submitted);
    assert!(srv
        .submit(tonic::Request::new(submitted))
        .await
        .unwrap()
        .into_inner()
        .rejections
        .is_empty());
    for _ in 0..32 {
        tokio::task::yield_now().await;
        if srv
            .storage
            .pending_ingest_effects(10)
            .await
            .unwrap()
            .is_empty()
        {
            drainer.abort();
            return;
        }
    }
    panic!("a commit notification must wake the idle drainer without advancing time");
}

/// H2 (§18.8.5): the ceiling registry persists across restarts — a source
/// registered with ceiling 1 in one process is still ceiling-limited in a
/// fresh process booted from the same sources file.
#[tokio::test]
async fn source_ceilings_persist_across_restart() {
    use tonic::Request;

    let dir = std::env::temp_dir().join(format!("exo-src-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sources.json");

    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let mk_server = || {
        IngestServer::new(
            Arc::new(InMemoryStorage::new(onto.clone())),
            onto.clone(),
            [5u8; 32],
        )
        .with_sources_file(path.clone())
    };

    {
        let srv = mk_server();
        srv.register_source(Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://persist",
            "test-adapter",
            1, // Private only
            "custom",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
        assert!(path.exists(), "ceiling registry written on registration");
    }

    // Fresh process stand-in: same sources file, no re-registration.
    let srv2 = mk_server();
    let mut b = batch(vec![draft("k1", "Fix", 1)]);
    b.ontology_fingerprint = srv2.ontology.fingerprint.0.to_vec();
    b.source_uri = "session://persist".into();
    b.producer_id = "test-adapter".into();
    b.ceiling = 3; // Org — above the persisted ceiling
    let b = sign(b, [5u8; 32]);
    let ack = srv2.submit(Request::new(b)).await.unwrap().into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::UnknownSource as i32),
        "persisted ceiling survives restart: {:?}",
        ack.rejections
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn entities_extracted_server_side() {
    let ex = exocortex_ingest::EntityExtractor::new("org");
    let ids = ex.entity_ids(
        "Fixed src/auth.rs via cargo build; see https://example.com/docs and tokio 1.40@1.40.1",
        &[],
    );
    assert!(!ids.is_empty(), "entities extracted from content");
    // Deterministic: same input -> same ids.
    let again = ex.entity_ids(
        "Fixed src/auth.rs via cargo build; see https://example.com/docs and tokio 1.40@1.40.1",
        &[],
    );
    assert_eq!(ids, again);
    assert!(exocortex_ingest::entities::table_is_complete());
}

#[tokio::test]
async fn stored_memories_carry_extracted_entities() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1);

    let storage = &srv.storage;
    let mut n = 0;
    use futures::StreamExt;
    let mut ms = storage.stream_all_memories().await;
    while let Some(Ok(m)) = ms.next().await {
        // D6: the grouping node is structural (Derived, no entities) —
        // the producer-row assertions below apply to asserted rows only.
        if matches!(m.provenance, exocortex_kernel::Provenance::Derived { .. }) {
            continue;
        }
        n += 1;
        assert!(
            !m.context.entities.is_empty(),
            "R-T18: backend extracted entities (content mentions src/auth.rs)"
        );
        // D8: the registered producer kind (CodingAgent) rides the row.
        assert_eq!(
            m.provenance,
            exocortex_kernel::Provenance::Asserted {
                author: "session-wrapup".into(),
                producer_kind: Some(exocortex_kernel::ProducerKind::CodingAgent),
            }
        );
    }
    assert_eq!(n, 1);
}

#[tokio::test]
async fn ceiling_visibility_alone_is_not_widening() {
    // Batch ceiling Org with Private memory: fine (narrower than ceiling).
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 0)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "narrower-than-ceiling is allowed");
}

/// WS1 (audit): RegisterSource requires the producer HMAC — an
/// unauthenticated caller can no longer overwrite registrations or
/// LRU-evict every registered producer from the registry Submit consults.
#[tokio::test]
async fn unsigned_registration_is_unauthenticated() {
    let srv = server();
    let err = srv
        .register_source(tonic::Request::new(RegisterSourceRequest {
            default_rights: None,
            org_id: "org".into(),
            source_uri: "session://evil".into(),
            producer_id: "attacker".into(),
            ceiling: 3,
            source_flavor: "custom".into(),
            projection: None,
            producer: None,
            producer_kind: 5,
        }))
        .await;
    assert!(err.is_err(), "unsigned registration rejected");

    // A wrong key is rejected too (a present-but-invalid signature is not
    // proof).
    let mut forged = exocortex_wire::signing::registration(
        &[9u8; 32],
        "org",
        "session://evil",
        "attacker",
        3,
        "custom",
        "attacker-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    forged.producer.as_mut().unwrap().hmac_signature = vec![1, 2, 3];
    let err = srv.register_source(tonic::Request::new(forged)).await;
    assert!(err.is_err(), "invalid signature rejected");
}

/// WS1: a flood of authenticated-but-distinct registrations cannot evict a
/// legitimate producer's entry (registrations with fresh producer_ids are
/// distinct keys; the point of this test is the LRU bound the audit
/// describes — the eviction half is unreachable now that unsigned calls
/// error before touching the registry).
#[tokio::test]
async fn legitimate_producer_survives_registration_flood() {
    let srv = server();
    registered(&srv, 3).await;
    // Unsigned flood (the unauthenticated attack from the audit): every
    // call errors without mutating the registry.
    for i in 0..1100 {
        let r = srv
            .register_source(tonic::Request::new(RegisterSourceRequest {
                default_rights: None,
                org_id: "org".into(),
                source_uri: format!("session://flood-{i}"),
                producer_id: format!("attacker-{i}"),
                ceiling: 3,
                source_flavor: "custom".into(),
                projection: None,
                producer: None,
                producer_kind: 5,
            }))
            .await;
        assert!(r.is_err(), "flood call {i} rejected");
    }
    // The legitimate producer still submits.
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "registered producer unaffected by flood");
}

/// WS1/WS2: re-registration never silently overwrites a different ceiling;
/// the existing value is echoed so the producer's R-I3 check fires.
#[tokio::test]
async fn re_registration_does_not_overwrite_ceiling() {
    let srv = server();
    registered(&srv, 1).await; // Private
    let echo = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://s1",
            "session-wrapup",
            3, // requests ORG over an existing Private registration
            "session",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(echo.ceiling, 1, "existing ceiling stands, not overwritten");

    // And the batch at ORG now fails R-I3 instead of being widened.
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::UnknownSource as i32),
        "ceiling mismatch surfaces: {:?}",
        ack.rejections
    );
}

/// WS2: an admin-configured ceiling is authoritative — the producer cannot
/// register above it, and the configured value is what gets registered.
#[tokio::test]
async fn admin_ceiling_caps_self_registration() {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let srv = IngestServer::new_with_admin_policies(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [(
            ("org".into(), "session://s1".into(), "session-wrapup".into()),
            exocortex_ingest::service::AdminSourcePolicy {
                ceiling: exocortex_kernel::Visibility::Project,
                kind: exocortex_kernel::ProducerKind::CodingAgent,
                signing_key: [5u8; 32],
            },
        )],
    );

    // Over the admin ceiling: rejected.
    let err = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://s1",
            "session-wrapup",
            3,
            "session",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await;
    assert!(err.is_err(), "over-ceiling registration rejected");

    // At or under the admin ceiling: registered at the ADMIN value (echoed
    // back), so the SDK's equality check fires CeilingMismatch when the
    // producer configured something narrower.
    let echo = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://s1",
            "session-wrapup",
            0, // requests Private; admin says Project
            "session",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        echo.ceiling, 1,
        "admin Project ceiling echoed, not the proposal"
    );

    // A fresh production server starts from the administrator-pinned kind;
    // registration cannot rewrite provenance authority after a restart.
    srv.register_source(tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://s1",
        "session-wrapup",
        1,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::DocsAdapter,
    )))
    .await
    .unwrap();
    let mut submitted = signed_batch(&srv, vec![draft("policy-kind", "Fix", 1)]);
    submitted.batch_id = "policy-kind".into();
    submitted.ceiling = 1;
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut submitted);
    assert!(srv
        .submit(tonic::Request::new(submitted))
        .await
        .unwrap()
        .into_inner()
        .rejections
        .is_empty());
    use futures::StreamExt as _;
    let rows = srv
        .storage
        .stream_all_memories()
        .await
        .collect::<Vec<_>>()
        .await;
    let stored = rows
        .into_iter()
        .map(Result::unwrap)
        .find(|memory| memory.title.as_str() == "title policy-kind")
        .unwrap();
    assert!(matches!(
        stored.provenance,
        exocortex_kernel::Provenance::Asserted {
            producer_kind: Some(exocortex_kernel::ProducerKind::CodingAgent),
            ..
        }
    ));
}

#[tokio::test]
async fn production_policy_selects_signing_key_by_exact_producer_identity() {
    use exocortex_ingest::service::AdminSourcePolicy;

    const ALPHA_KEY: [u8; 32] = [0xA1; 32];
    const BETA_KEY: [u8; 32] = [0xB2; 32];
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let srv = IngestServer::new_with_admin_policies(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [
            (
                ("org".into(), "session://alpha".into(), "producer".into()),
                AdminSourcePolicy {
                    ceiling: exocortex_kernel::Visibility::Org,
                    kind: exocortex_kernel::ProducerKind::CodingAgent,
                    signing_key: ALPHA_KEY,
                },
            ),
            (
                ("org".into(), "session://beta".into(), "producer".into()),
                AdminSourcePolicy {
                    ceiling: exocortex_kernel::Visibility::Org,
                    kind: exocortex_kernel::ProducerKind::CodingAgent,
                    signing_key: BETA_KEY,
                },
            ),
        ],
    );

    let forged = exocortex_wire::signing::registration(
        &ALPHA_KEY,
        "org",
        "session://beta",
        "producer",
        3,
        "session",
        "node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    let error = srv
        .register_source(tonic::Request::new(forged))
        .await
        .expect_err("another source's key must not impersonate beta");
    assert_eq!(error.code(), tonic::Code::Unauthenticated);

    let exact = exocortex_wire::signing::registration(
        &BETA_KEY,
        "org",
        "session://beta",
        "producer",
        3,
        "session",
        "node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    srv.register_source(tonic::Request::new(exact))
        .await
        .expect("the exact beta key is admitted");

    let mut wrong_batch = batch(vec![draft("wrong-key", "Fix", 1)]);
    wrong_batch.source_uri = "session://beta".into();
    wrong_batch.producer_id = "producer".into();
    wrong_batch.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    let ack = srv
        .submit(tonic::Request::new(sign(wrong_batch, ALPHA_KEY)))
        .await
        .unwrap()
        .into_inner();
    assert!(ack
        .rejections
        .iter()
        .all(|row| row.code == RejectCode::Unauthorized as i32));

    let mut exact_batch = batch(vec![draft("exact-key", "Fix", 1)]);
    exact_batch.source_uri = "session://beta".into();
    exact_batch.producer_id = "producer".into();
    exact_batch.batch_id = "exact-policy-key".into();
    exact_batch.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    let ack = srv
        .submit(tonic::Request::new(sign(exact_batch, BETA_KEY)))
        .await
        .unwrap()
        .into_inner();
    assert!(ack.rejections.is_empty(), "{:?}", ack.rejections);
}

#[tokio::test]
async fn production_policy_rejects_unknown_source_without_registration() {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let srv = IngestServer::new_with_admin_policies(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        std::iter::empty(),
    );
    let request = exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://unknown",
        "unknown",
        3,
        "session",
        "node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    let err = srv
        .register_source(tonic::Request::new(request))
        .await
        .expect_err("unknown source must be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(srv.sources.lock().unwrap().is_empty());
}

/// WS5 (audit): unknown Visibility discriminants are rejected — the old
/// fail-open coercion mapped 5, 99, -1 to PUBLIC silently.
#[tokio::test]
async fn unknown_visibility_discriminant_rejected() {
    let srv = server();
    let _ = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://ws5",
            "session-wrapup",
            3,
            "session",
            "t",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    let mut b = batch(vec![draft("k", "Fix", 99)]);
    b.org_id = "org".into();
    b.source_uri = "session://ws5".into();
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        ack.rejections
            .iter()
            .any(|r| r.code == RejectCode::VisibilityWidening as i32),
        "unknown discriminant rejected: {:?}",
        ack.rejections
    );
}

/// WS4 (audit): NaN strength is rejected, not persisted.
#[tokio::test]
async fn nan_strength_rejected() {
    let srv = server();
    let _ = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://ws4",
            "session-wrapup",
            3,
            "session",
            "t",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    let mut memories = vec![draft("a", "Fix", 1), draft("b", "Problem", 1)];
    for m in &mut memories {
        m.title = m.title.clone();
    }
    let mut b = batch(memories);
    b.org_id = "org".into();
    b.source_uri = "session://ws4".into();
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.relationships = vec![exocortex_wire::ingest::v1::RelationshipDraft {
        from_draft_key: "a".into(),
        to_draft_key: "b".into(),
        kind: "Fixes".into(),
        strength: f32::NAN,
        confidence: 0.5,
        context: String::new(),
        visibility: 1,

        to_memory_id: String::new(),
    }];
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0, "NaN strength must reject the batch");
}

/// W3 (audit): session-scoped sources stamp `MemoryContext.session_id`.
#[tokio::test]
async fn session_source_stamps_session_id() {
    let srv = server();
    // Register the s-42 source (the registry is keyed by source_uri).
    let _ = srv
        .register_source(tonic::Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://s-42",
            "session-wrapup",
            3,
            "session",
            "t",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();
    let mut b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    b.source_uri = "session://s-42".into();
    // Re-sign: the checksum and signature cover source_uri.
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "batch lands: {ack:?}");
    use futures::StreamExt;
    let mut ms = srv.storage.stream_all_memories().await;
    let mut any = false;
    while let Some(Ok(m)) = ms.next().await {
        any = true;
        assert_eq!(
            m.context.session_id.as_deref(),
            Some("s-42"),
            "W3: online path stamps the session id"
        );
    }
    assert!(any, "committed row present");
}

/// R6-B09: the idempotency registry survives a server restart because the
/// claim and settled result live in the same durable storage boundary as the
/// graph mutation, not in a best-effort process file.
#[tokio::test]
async fn duplicate_replay_dedup_survives_restart() {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mk = || IngestServer::new(storage.clone(), onto.clone(), [5u8; 32]);

    // Process #1: register, commit one batch.
    let srv = mk();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let ack = srv
        .submit(tonic::Request::new(b.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1);
    use futures::StreamExt;
    let mut before = 0;
    let mut memories = storage.stream_all_memories().await;
    while let Some(Ok(_)) = memories.next().await {
        before += 1;
    }
    drop(srv); // "restart"

    // Process #2: same batch replays -> DuplicateBatch, nothing commits.
    let srv2 = mk();
    registered(&srv2, 3).await;
    let replay = srv2
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert!(
        replay
            .rejections
            .iter()
            .any(|r| r.code == RejectCode::DuplicateBatch as i32),
        "W7: restart keeps the dedup set: {:?}",
        replay.rejections
    );
    assert_eq!(replay.batch_id, ack.batch_id);
    assert_eq!(replay.accepted, ack.accepted);
    assert_eq!(replay.rejected, ack.rejected);
    assert_eq!(replay.assigned_lsn, ack.assigned_lsn);
    let mut after = 0;
    let mut memories = storage.stream_all_memories().await;
    while let Some(Ok(_)) = memories.next().await {
        after += 1;
    }
    assert_eq!(after, before, "restart replay must not commit another row");
}

/// D23 (LLM boundary decision, option a): an OUT-OF-PROCESS extraction
/// producer registers under `PRODUCER_KIND_EXTRACTED` and its committed
/// rows carry that kind in provenance — distinguishable as a class, so
/// reads can filter or revoke extraction output without touching any
/// other producer's rows. The node itself stays deterministic (R-D6):
/// the extraction ran in an adapter, before the wire.
#[tokio::test]
async fn extraction_producers_are_stamped_distinguishably() {
    use exocortex_storage::Storage as _;
    let srv = server();
    srv.register_source(tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "custom://llm-extractor",
        "llm-extractor",
        3,
        "custom",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::Extracted,
    )))
    .await
    .unwrap();

    let memory = draft("x", "Fix", 1);
    let b = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://llm-extractor".into(),
        producer_id: "llm-extractor".into(),
        batch_id: "extracted-1".into(),
        mapping_version: "custom:1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![memory],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "llm-extractor".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    let ack = srv
        .submit(tonic::Request::new(sign(b, [5u8; 32])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "rejections: {:#?}", ack.rejections);

    use futures::StreamExt as _;
    let mut stream = srv.storage.stream_all_memories().await;
    let mut stored = Vec::new();
    while let Some(row) = stream.next().await {
        stored.push(row.unwrap());
    }
    assert_eq!(stored.len(), 1);
    match &stored[0].provenance {
        exocortex_kernel::Provenance::Asserted {
            producer_kind: Some(kind),
            ..
        } => assert_eq!(
            *kind,
            exocortex_kernel::ProducerKind::Extracted,
            "extraction output is distinguishable in provenance"
        ),
        other => panic!("expected asserted provenance with a kind, got {other:?}"),
    }
}

/// D19 (SaaS-API adapter family): a SaaS adapter registers under
/// `PRODUCER_KIND_SAAS_ADAPTER` and its committed rows carry that kind in
/// provenance — one stamp for the whole family (Linear, Jira, GitHub,
/// ServiceNow), so the class is distinguishable and revocable without
/// touching any other producer's rows. Additive wire value 7: a server
/// built before D19 fails `wire_kind_to_kernel` to None, so the
/// registration rejects fail-closed (the rolling-upgrade behavior).
#[tokio::test]
async fn saas_producers_are_stamped_distinguishably() {
    use exocortex_storage::Storage as _;
    let srv = server();
    srv.register_source(tonic::Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "linear://acme",
        "linear-adapter",
        3,
        "linear",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::SaasAdapter,
    )))
    .await
    .unwrap();

    let memory = draft("x", "Task", 1);
    let b = IngestBatch {
        org_id: "org".into(),
        source_uri: "linear://acme".into(),
        producer_id: "linear-adapter".into(),
        batch_id: "linear-1".into(),
        mapping_version: "linear:1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![memory],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "linear-adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    let ack = srv
        .submit(tonic::Request::new(sign(b, [5u8; 32])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 1, "rejections: {:#?}", ack.rejections);

    use futures::StreamExt as _;
    let mut stream = srv.storage.stream_all_memories().await;
    let mut stored = Vec::new();
    while let Some(row) = stream.next().await {
        stored.push(row.unwrap());
    }
    assert_eq!(stored.len(), 1);
    match &stored[0].provenance {
        exocortex_kernel::Provenance::Asserted {
            producer_kind: Some(kind),
            ..
        } => assert_eq!(
            *kind,
            exocortex_kernel::ProducerKind::SaaSAdapter,
            "SaaS adapter output is distinguishable in provenance"
        ),
        other => panic!("expected asserted provenance with a kind, got {other:?}"),
    }
}
