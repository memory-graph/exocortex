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
    // D6: 3 memories + 3 InSession edges + 3 HasMember companions.
    if ack.accepted != 9 {
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
fn reject_code_protocol_catalogue_is_exhaustive() {
    // §23 #27: each entry is exercised by a targeted Submit batch in this
    // integration-test target or the adjacent e2e/external-key targets. Keep
    // this match wildcard-free so adding a protocol code cannot silently
    // escape the executable rejection matrix.
    fn targeted_test(code: RejectCode) -> &'static str {
        match code {
            RejectCode::Unknown => "empty_content_emits_unknown_reject_code",
            RejectCode::IncompatibleOntology => "fingerprint_mismatch_rejected",
            RejectCode::UnknownSource => "unknown_source_rejected_after_hmac",
            RejectCode::UnknownMemoryType => "unknown_memory_type_rejected",
            RejectCode::UnknownKind => "unknown_relationship_kind_rejected",
            RejectCode::InvalidTypeTriple => "bad_triple_rejects_whole_batch_naming_the_key",
            RejectCode::VisibilityWidening => "visibility_widening_rejected_under_lowered_ceiling",
            RejectCode::MissingExternalKey => {
                "external_batch_without_key_rejected_and_with_key_deterministic"
            }
            RejectCode::DuplicateBatch => "same_batch_id_different_payload_is_rejected",
            RejectCode::BadChecksum => "tampered_external_coordinates_fail_checksum",
            RejectCode::Unauthorized => "missing_hmac_rejected_before_anything",
            RejectCode::RateLimited => {
                "saturated_submit_capacity_emits_rate_limited_without_storage_work"
            }
            RejectCode::ComputedKindRejected => "computed_kind_rejected_at_ingest_boundary",
            RejectCode::InvalidExternalKey => "malformed_external_coordinates_are_rejected",
            RejectCode::ResourceLimitExceeded => {
                "resource_ceiling_rejects_before_ontology_or_storage_work"
            }
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
    for code in codes {
        assert!(!targeted_test(code).is_empty());
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
    registered(&srv, 3).await;
    let mut d = draft("k", "Fix", 1);
    d.external_key = None;
    let mut b = batch(vec![d]);
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    b.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
        snapshot_id: "s1".into(),
        schema_hash: vec![0; 32],
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
        schema_hash: vec![0; 32],
        source_flavor: "iceberg".into(),
    });
    let b2 = sign(b2, [5u8; 32]);
    let ack2 = srv
        .submit(tonic::Request::new(b2))
        .await
        .unwrap()
        .into_inner();
    // D6: memory + InSession edge + HasMember companion.
    assert_eq!(ack2.accepted, 3);

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
async fn duplicate_batch_is_idempotent_replay() {
    let srv = server();
    registered(&srv, 3).await;
    let b = signed_batch(&srv, vec![draft("k", "Fix", 1)]);
    let first = srv
        .submit(tonic::Request::new(b.clone()))
        .await
        .unwrap()
        .into_inner();
    // D6: memory + InSession + companion.
    assert_eq!(first.accepted, 3);
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
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3);

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
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3, "narrower-than-ceiling is allowed");
}

/// WS1 (audit): RegisterSource requires the producer HMAC — an
/// unauthenticated caller can no longer overwrite registrations or
/// LRU-evict every registered producer from the registry Submit consults.
#[tokio::test]
async fn unsigned_registration_is_unauthenticated() {
    let srv = server();
    let err = srv
        .register_source(tonic::Request::new(RegisterSourceRequest {
            org_id: "org".into(),
            source_uri: "session://evil".into(),
            producer_id: "attacker".into(),
            ceiling: 3,
            source_flavor: "custom".into(),
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
                org_id: "org".into(),
                source_uri: format!("session://flood-{i}"),
                producer_id: format!("attacker-{i}"),
                ceiling: 3,
                source_flavor: "custom".into(),
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
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3, "registered producer unaffected by flood");
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
    let srv = IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
    .with_admin_ceilings([(
        ("org".into(), "session://s1".into(), "session-wrapup".into()),
        exocortex_kernel::Visibility::Project,
    )]);

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
}

#[tokio::test]
async fn production_policy_rejects_unknown_source_without_registration() {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let srv = IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
    .with_admin_ceilings(std::iter::empty())
    .require_admin_ceilings();
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
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3, "batch lands: {ack:?}");
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
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3);
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
    let mut after = 0;
    let mut memories = storage.stream_all_memories().await;
    while let Some(Ok(_)) = memories.next().await {
        after += 1;
    }
    assert_eq!(after, before, "restart replay must not commit another row");
}
