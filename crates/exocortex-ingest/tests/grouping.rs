//! A-PRD acceptance tests (agent-instructions PRD): D6 write grouping,
//! D8 producer kind, D9 registration surface, §4.5 cross-batch edges,
//! D10a derived-confidence supersession floor, D10c near-duplicate
//! hints. Each test fails without its deliverable.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
    RegisterSourceRequest, RejectCode, RelationshipDraft,
};
use futures::StreamExt;
use tonic::Request;

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let mut srv = IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    );
    srv = srv.with_embedder(Arc::new(
        exocortex_ingest::embedding::FakeEmbedder::default(),
    ));
    srv
}

fn draft(key: &str, mt: &str, title: &str) -> MemoryDraft {
    MemoryDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: mt.into(),
        title: title.into(),
        content: format!("{title}: body in src/auth.rs"),
        tags: vec![],
        visibility: 1,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn batch(session: &str, batch_id: &str, memories: Vec<MemoryDraft>) -> IngestBatch {
    IngestBatch {
        org_id: "org".into(),
        source_uri: format!("session://{session}"),
        producer_id: "session-wrapup".into(),
        batch_id: batch_id.into(),
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
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    }
}

async fn register(srv: &IngestServer<InMemoryStorage>, session: &str) {
    srv.register_source(Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        &format!("session://{session}"),
        "session-wrapup",
        3,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    )))
    .await
    .unwrap();
}

async fn submit(
    srv: &IngestServer<InMemoryStorage>,
    b: IngestBatch,
) -> exocortex_wire::ingest::v1::IngestAck {
    let mut b = b;
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    srv.submit(Request::new(b)).await.unwrap().into_inner()
}

async fn memories(srv: &IngestServer<InMemoryStorage>) -> Vec<exocortex_kernel::Memory> {
    let mut out = vec![];
    let mut ms = srv.storage.stream_all_memories().await;
    while let Some(Ok(m)) = ms.next().await {
        out.push(m);
    }
    out
}

async fn relationships(srv: &IngestServer<InMemoryStorage>) -> Vec<exocortex_kernel::Relationship> {
    let mut out = vec![];
    let mut rs = srv.storage.stream_all_relationships().await;
    while let Some(Ok(r)) = rs.next().await {
        out.push(r);
    }
    out
}

fn hex16(id: exocortex_kernel::MemoryId) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id.0 {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// D6 acceptance (§3.6): two batches, same session id, different batch
/// ids ⇒ ONE Conversation node, two stamped memories, two InSession
/// edges — and a restart between the batches changes nothing (W7's
/// concern, answered by deterministic ids).
#[tokio::test]
async fn two_batches_group_under_one_conversation_across_restart() {
    let srv = server();
    register(&srv, "abc").await;
    let a1 = submit(
        &srv,
        batch("abc", "b1", vec![draft("k1", "Fix", "Fixed auth race")]),
    )
    .await;
    assert_eq!(a1.rejected, 0, "{:?}", a1.rejections);

    // "Restart": a fresh server over the SAME storage; nothing in memory
    // but the rows on disk. Re-registration + second batch.
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let srv2 = IngestServer::new(Arc::clone(&srv.storage), onto, [5u8; 32]).with_embedder(
        Arc::new(exocortex_ingest::embedding::FakeEmbedder::default()),
    );
    register(&srv2, "abc").await;
    let a2 = submit(
        &srv2,
        batch("abc", "b2", vec![draft("k2", "Problem", "Pool exhaustion")]),
    )
    .await;
    assert_eq!(a2.rejected, 0, "{:?}", a2.rejections);

    let mems = memories(&srv2).await;
    let conversations: Vec<_> = mems
        .iter()
        .filter(|m| {
            srv2.ontology
                .memory_type_id("Conversation")
                .is_some_and(|t| m.memory_type == t)
        })
        .collect();
    assert_eq!(
        conversations.len(),
        1,
        "one Conversation node across two batches and a restart"
    );
    let conv = conversations[0];
    assert_eq!(conv.context.session_id.as_deref(), Some("abc"));
    assert!(matches!(
        conv.provenance,
        exocortex_kernel::Provenance::Derived { .. }
    ));

    // Two member memories stamped with the session id (the Conversation
    // itself also carries it; the count below is producer rows only).
    let stamped: Vec<_> = mems
        .iter()
        .filter(|m| m.context.session_id.as_deref() == Some("abc"))
        .collect();
    assert!(stamped.len() >= 3, "conversation + 2 members stamped");

    // InSession edges: member → node. Two submits re-minting the SAME
    // deterministic edges (k1's edge from batch 1; k2's from batch 2) —
    // distinct members, so exactly 2 InSession + companions.
    let rels = relationships(&srv2).await;
    let in_session = srv2.ontology.kind_id("InSession").unwrap();
    let has_member = srv2.ontology.kind_id("HasMember").unwrap();
    let member_edges: Vec<_> = rels.iter().filter(|r| r.kind == in_session).collect();
    let companion_edges: Vec<_> = rels.iter().filter(|r| r.kind == has_member).collect();
    assert_eq!(member_edges.len(), 2, "one InSession per member memory");
    assert_eq!(companion_edges.len(), 2, "R-T4 companions ride along");
    for e in &member_edges {
        assert_eq!(e.to, conv.id, "member edges point at the grouping node");
    }
}

/// D6 §3.6 acceptance: a synthetic second flavor groups under its own
/// node type without touching the session rule. Registered via the rule
/// table's extensibility seam (the public `grouping_rules` is read-only
/// in v1; the test proves the rule RESOLVES by flavor, not by magic
/// string matching on `session://` for non-session sources — a
/// non-session source_uri produces NO grouping).
#[tokio::test]
async fn non_session_source_does_not_group() {
    let srv = server();
    srv.register_source(Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "fixture://docs",
        "docs-adapter",
        3,
        "custom",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::DocsAdapter,
    )))
    .await
    .unwrap();
    let mut b = batch("abc", "b1", vec![draft("k1", "Technology", "Docs quirk")]);
    b.source_uri = "fixture://docs".into();
    b.producer_id = "docs-adapter".into();
    b.ontology_fingerprint = srv.ontology.fingerprint.0.to_vec();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv.submit(Request::new(b)).await.unwrap().into_inner();
    assert_eq!(ack.rejected, 0, "{:?}", ack.rejections);
    let mems = memories(&srv).await;
    assert!(
        mems.iter().all(|m| srv
            .ontology
            .memory_type_id("Conversation")
            .is_none_or(|t| m.memory_type != t)),
        "no grouping node for an unregistered flavor"
    );
}

#[tokio::test]
async fn custom_flavor_cannot_claim_session_semantics_from_its_uri() {
    let srv = server();
    srv.register_source(Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://custom-source",
        "custom-adapter",
        3,
        "custom",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::Custom,
    )))
    .await
    .unwrap();
    let changed = srv
        .register_source(Request::new(exocortex_wire::signing::registration(
            &[5u8; 32],
            "org",
            "session://custom-source",
            "custom-adapter",
            3,
            "session",
            "test-node",
            exocortex_wire::ingest::v1::ProducerKind::Custom,
        )))
        .await;
    assert!(
        changed.is_err(),
        "registered flavor is first-registration authority"
    );
    let mut b = batch(
        "custom-source",
        "custom-flavor",
        vec![draft("k", "Technology", "Custom row")],
    );
    b.producer_id = "custom-adapter".into();
    let ack = submit(&srv, b).await;
    assert_eq!((ack.accepted, ack.rejected), (1, 0));
    let mems = memories(&srv).await;
    assert_eq!(mems.len(), 1, "custom flavor creates no grouping node");
    assert!(mems[0].context.session_id.is_none());
}

#[test]
fn legacy_source_registry_rows_default_to_no_flavor() {
    let entry: exocortex_ingest::service::SourceEntry =
        serde_json::from_str(r#"{"ceiling":"Org","kind":"Custom"}"#).unwrap();
    assert!(
        entry.flavor.is_empty(),
        "old rows cannot infer grouping from URI"
    );
}

fn docs_grouping_key(batch: &IngestBatch) -> Option<String> {
    batch
        .source_uri
        .strip_prefix("fixture://docs/")
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
}

#[tokio::test]
async fn synthetic_second_flavor_groups_without_changing_commit_orchestration() {
    let mut rules = exocortex_ingest::grouping::grouping_rules().to_vec();
    rules.push(exocortex_ingest::grouping::GroupingRule {
        flavor: "docs",
        key_of: docs_grouping_key,
        node_type: "Technology",
        edge_kind: "InSession",
    });
    let srv = server().with_grouping_rules(rules);
    srv.register_source(Request::new(exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "fixture://docs/rust",
        "docs-adapter",
        3,
        "docs",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::DocsAdapter,
    )))
    .await
    .unwrap();
    let mut b = batch("unused", "docs-flavor", vec![draft("k", "Fix", "Docs fix")]);
    b.source_uri = "fixture://docs/rust".into();
    b.producer_id = "docs-adapter".into();
    let ack = submit(&srv, b).await;
    assert_eq!((ack.accepted, ack.rejected), (1, 0));
    let mems = memories(&srv).await;
    assert!(mems.iter().any(|memory| matches!(
        memory.provenance,
        exocortex_kernel::Provenance::Derived { .. }
    )));
    let rels = relationships(&srv).await;
    let in_session = srv.ontology.kind_id("InSession").unwrap();
    assert_eq!(
        rels.iter().filter(|edge| edge.kind == in_session).count(),
        1
    );
}

/// D8 (§3.8): UNSPECIFIED producer kind is rejected at registration —
/// the closed enum is enforced at the boundary, before any row lands.
#[tokio::test]
async fn unspecified_producer_kind_is_rejected() {
    let srv = server();
    let err = srv
        .register_source(Request::new(RegisterSourceRequest {
            org_id: "org".into(),
            source_uri: "session://d8".into(),
            producer_id: "session-wrapup".into(),
            ceiling: 3,
            source_flavor: "session".into(),
            producer_kind: 0,
            producer: Some(ProducerIdentity {
                node_id: "n".into(),
                agent_id: String::new(),
                adapter_id: String::new(),
                hmac_signature: vec![],
                client_metadata: None,
            }),
        }))
        .await;
    assert!(err.is_err(), "UNSPECIFIED is a refusal to declare");
    // A signed-but-unknown discriminant (forward-compat) also fails closed.
    let mut forged = exocortex_wire::signing::registration(
        &[5u8; 32],
        "org",
        "session://d8",
        "session-wrapup",
        3,
        "session",
        "test-node",
        exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
    );
    forged.producer_kind = 99;
    exocortex_wire::signing::sign_registration(&[5u8; 32], &mut forged);
    let err2 = srv.register_source(Request::new(forged)).await;
    assert!(err2.is_err(), "unknown discriminants fail closed");
}

/// D8: the kind is stored on provenance and survives re-registration
/// (first-registration-wins, like the ceiling).
#[tokio::test]
async fn producer_kind_rides_provenance() {
    let srv = server();
    register(&srv, "pk").await;
    let ack = submit(
        &srv,
        batch("pk", "b1", vec![draft("k", "Fix", "Fixed thing")]),
    )
    .await;
    assert_eq!(ack.rejected, 0, "{:?}", ack.rejections);
    let mems = memories(&srv).await;
    let producer_rows: Vec<_> = mems
        .iter()
        .filter(|m| matches!(m.provenance, exocortex_kernel::Provenance::Asserted { .. }))
        .collect();
    assert_eq!(producer_rows.len(), 1);
    assert_eq!(
        producer_rows[0].provenance,
        exocortex_kernel::Provenance::Asserted {
            author: "session-wrapup".into(),
            producer_kind: Some(exocortex_kernel::ProducerKind::CodingAgent),
        }
    );
}

/// §4.5: today's Fix links yesterday's Problem. The cross-batch edge
/// resolves the stored type and enforces the same R-T17 triple; an
/// unknown id rejects with the id named.
#[tokio::test]
async fn cross_batch_edge_resolves_and_rejects() {
    let srv = server();
    register(&srv, "x").await;
    // Batch 1: the Problem.
    let a1 = submit(
        &srv,
        batch("x", "b1", vec![draft("p", "Problem", "Pool exhausted")]),
    )
    .await;
    assert_eq!(a1.rejected, 0, "{:?}", a1.rejections);
    let problem = memories(&srv)
        .await
        .into_iter()
        .find(|m| matches!(m.provenance, exocortex_kernel::Provenance::Asserted { .. }))
        .unwrap();

    // Batch 2: the Fix, Fixes-linked by to_memory_id.
    let mut b2 = batch("x", "b2", vec![draft("f", "Fix", "Fixed pool exhaustion")]);
    b2.relationships = vec![RelationshipDraft {
        from_draft_key: "f".into(),
        to_draft_key: String::new(),
        kind: "Fixes".into(),
        strength: 0.9,
        confidence: 0.8,
        context: String::new(),
        visibility: 3,
        to_memory_id: hex16(problem.id),
    }];
    let a2 = submit(&srv, b2).await;
    assert_eq!(a2.rejected, 0, "{:?}", a2.rejections);
    let rels = relationships(&srv).await;
    let fixes = srv.ontology.kind_id("Fixes").unwrap();
    assert!(
        rels.iter().any(|r| r.kind == fixes && r.to == problem.id),
        "the cross-batch Fixes edge committed"
    );

    // Unknown id: rejected, named.
    let mut b3 = batch("x", "b3", vec![draft("g", "Fix", "Another fix")]);
    b3.relationships = vec![RelationshipDraft {
        from_draft_key: "g".into(),
        to_draft_key: String::new(),
        kind: "Fixes".into(),
        strength: 0.9,
        confidence: 0.8,
        context: String::new(),
        visibility: 3,
        to_memory_id: "f".repeat(32),
    }];
    let a3 = submit(&srv, b3).await;
    assert_eq!(a3.accepted, 0);
    assert!(
        a3.rejections
            .iter()
            .any(|r| r.detail.contains(&"f".repeat(32))),
        "the unknown id is named in the detail: {:?}",
        a3.rejections
    );

    // Bad triple against a REAL memory: Fixes requires (Fix, Error|Problem)
    // — target the grouping node (Conversation) instead.
    let conv = memories(&srv)
        .await
        .into_iter()
        .find(|m| {
            srv.ontology
                .memory_type_id("Conversation")
                .is_some_and(|t| m.memory_type == t)
        })
        .unwrap();
    let mut b4 = batch("x", "b4", vec![draft("h", "Fix", "Yet another fix")]);
    b4.relationships = vec![RelationshipDraft {
        from_draft_key: "h".into(),
        to_draft_key: String::new(),
        kind: "Fixes".into(),
        strength: 0.9,
        confidence: 0.8,
        context: String::new(),
        visibility: 3,
        to_memory_id: hex16(conv.id),
    }];
    let a4 = submit(&srv, b4).await;
    assert!(
        a4.rejections
            .iter()
            .any(|r| r.code == RejectCode::InvalidTypeTriple as i32),
        "stored to-type is enforced: {:?}",
        a4.rejections
    );
}

/// D10a (§4.9/§4.10): a Replaces edge pointing AT a memory floors that
/// memory's derived confidence — stale beliefs rank below their
/// successors the moment the supersession lands.
#[tokio::test]
async fn supersession_floors_target_confidence() {
    let srv = server();
    register(&srv, "sup").await;
    let a1 = submit(
        &srv,
        batch(
            "sup",
            "b1",
            vec![draft("old", "Technology", "Falkor needs a server")],
        ),
    )
    .await;
    assert_eq!(a1.rejected, 0, "{:?}", a1.rejections);
    let old = memories(&srv)
        .await
        .into_iter()
        .find(|m| matches!(m.provenance, exocortex_kernel::Provenance::Asserted { .. }))
        .unwrap();
    let before = old.confidence.get();

    let mut b2 = batch(
        "sup",
        "b2",
        vec![draft("new", "Technology", "FalkorDBLite works embedded")],
    );
    b2.relationships = vec![RelationshipDraft {
        from_draft_key: "new".into(),
        to_draft_key: String::new(),
        kind: "Replaces".into(),
        strength: 0.9,
        confidence: 0.8,
        context: String::new(),
        visibility: 3,
        to_memory_id: hex16(old.id),
    }];
    let a2 = submit(&srv, b2).await;
    assert_eq!(a2.rejected, 0, "{:?}", a2.rejections);

    let floored = srv.storage.get_memory(&old.id).await.unwrap().unwrap();
    assert!(
        floored.confidence.get() < before,
        "confidence {before} -> {} after the Replaces edge",
        floored.confidence.get()
    );
    let floor = exocortex_kernel::memory::derived_confidence(true, 0, 0);
    assert!((floored.confidence.get() - floor.get()).abs() < 1e-6);
}

/// D10c (§4.10b): a near-duplicate submit returns an advisory hint
/// naming the prior memory; the batch still commits.
#[tokio::test]
async fn near_duplicate_hint_rides_the_ack() {
    let srv = server();
    register(&srv, "dup").await;
    let body = "Fixed the connection pool in src/pool.rs";
    let a1 = submit(&srv, batch("dup", "b1", vec![draft("k", "Fix", body)])).await;
    assert_eq!(a1.rejected, 0, "{:?}", a1.rejections);

    // Same title + content: exact duplicate → cosine 1.0 against the ring.
    let a2 = submit(&srv, batch("dup", "b2", vec![draft("k2", "Fix", body)])).await;
    assert_eq!(a2.rejected, 0, "the hint is advisory — the batch commits");
    assert_eq!(a2.similar_to.len(), 1, "one hint for the duplicate draft");
    let hint = &a2.similar_to[0];
    assert_eq!(hint.draft_key, "k2");
    assert_eq!(hint.existing_title, body);
    assert_eq!(hint.suggestion, "duplicate");

    // Different-type near-duplicate → contradicts.
    let a3 = submit(&srv, batch("dup", "b3", vec![draft("k3", "Problem", body)])).await;
    assert_eq!(a3.rejected, 0);
    assert!(
        a3.similar_to.iter().any(|h| h.suggestion == "contradicts"),
        "{:?}",
        a3.similar_to
    );
}
