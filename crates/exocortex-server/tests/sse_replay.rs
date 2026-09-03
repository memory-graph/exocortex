//! R-C6 server-side replay tests + R-Sec7 SSE auth: the `/v1/changes`
//! handler honors `?since_lsn=` (replay, then live), answers `409 Resync
//! Required` past the buffer floor, and `401` without a token when auth is
//! required. In-memory backend; raw-socket SSE reading (no HTTP client
//! dependency in this crate — same pattern as `sse_e2e.rs`).

use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::ClusterNode;
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, RelKindId, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_storage::{InMemoryStorage, Invalidation, Storage, VisibilityContext};
use exocortex_wire::cluster::v1::InvalidationEnvelope;
use exocortex_wire::sse::v1::invalidation::Kind;
use prost::Message;

const HMAC_KEY: [u8; 32] = [7u8; 32];

fn cluster(cap: usize) -> Arc<ClusterNode<InMemoryStorage>> {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = ClusterNode::new(storage, "replay-node".into(), onto.fingerprint, HMAC_KEY)
        .with_replay_capacity(cap);
    Arc::new(node)
}

fn envelope(node: &ClusterNode<InMemoryStorage>, _id: u8, lsn: u64) -> InvalidationEnvelope {
    node.envelope(Invalidation::VisibilityAdvance { lsn })
}

async fn serve(
    node: Arc<ClusterNode<InMemoryStorage>>,
    authenticated: bool,
) -> std::net::SocketAddr {
    let app = exocortex_server::sse::sse_router(node);
    let app = if authenticated {
        app.layer(axum::Extension(exocortex_ops::operations::ops_vc(
            "org",
            "test-reader",
            Visibility::Org,
        )))
    } else {
        app
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn serve_for(
    node: Arc<ClusterNode<InMemoryStorage>>,
    visibility: VisibilityContext,
) -> std::net::SocketAddr {
    let app = exocortex_server::sse::sse_router(node).layer(axum::Extension(visibility));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

/// GET `path`; read until EOF, 400ms of quiet, or an overall 2s deadline
/// (SSE streams stay open by design — `read_to_end` would never return).
async fn get_status_and_body(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    get_status_and_body_auth(addr, path, true).await
}

async fn get_status_and_body_without_bearer(
    addr: std::net::SocketAddr,
    path: &str,
) -> (String, String) {
    get_status_and_body_auth(addr, path, false).await
}

async fn get_status_and_body_auth(
    addr: std::net::SocketAddr,
    path: &str,
    bearer: bool,
) -> (String, String) {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let authorization = if bearer {
        "Authorization: Bearer test-only-sse-bearer-token-00000000\r\n"
    } else {
        ""
    };
    sock.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}Accept: text/event-stream\r\nX-Exocortex-SSE-Version: 2\r\nConnection: close\r\n\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let quiet = tokio::time::timeout(Duration::from_millis(400), sock.read(&mut chunk)).await;
        match quiet {
            Ok(Ok(0)) => break, // EOF
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) | Err(_) => break, // error or quiet: done
        }
        if tokio::time::Instant::now() > deadline {
            break;
        }
    }
    let raw = String::from_utf8_lossy(&buf).to_string();
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (status, raw)
}

fn decoded_events(body: &str) -> Vec<exocortex_wire::sse::v1::Invalidation> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|payload| {
            let raw = exocortex_client::sync::b64_decode(payload.trim()).unwrap();
            InvalidationEnvelope::decode(raw.as_slice())
                .unwrap()
                .inv
                .expect("SSE envelope carries an invalidation")
        })
        .collect()
}

fn scoped_memory(id: u8, project: &str) -> Memory {
    let now = chrono::Utc::now();
    Memory {
        rights: None,
        id: MemoryId([id; 16]),
        memory_type: 3,
        title: format!("memory-{id}").into(),
        content: format!("content-{id}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Project,
        provenance: Provenance::Asserted {
            author: "author".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: now,
            project_id: Some(project.into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
            session_id: None,
            user_id: Some("author".into()),
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: Default::default(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn project_reader() -> VisibilityContext {
    let mut context = VisibilityContext {
        user_id: "reader".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    };
    context.project_ids.push("visible-project".into());
    context
}

fn relationship(id: u8, from: MemoryId, to: MemoryId) -> Relationship {
    let now = chrono::Utc::now();
    Relationship {
        id: RelationshipId([id; 16]),
        kind: RelKindId(1),
        from,
        to,
        visibility: Visibility::Project,
        provenance: Provenance::Asserted {
            author: "author".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: now,
        },
        description: None,
        bidirectional: false,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        lsn: LSN::new_local(0),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn since_lsn_replays_buffered_deltas_in_order() {
    let node = cluster(16);
    for lsn in 1..=3u64 {
        let _ = node.admit_and_publish(envelope(&node, lsn as u8, lsn));
    }
    let addr = serve(node.clone(), true).await;

    let (status, body) = get_status_and_body(addr, "/v1/changes?since_lsn=1").await;
    assert_eq!(status, "200");
    let replayed = body.matches("event: inv").count();
    assert!(
        replayed >= 2,
        "deltas 2 and 3 replay for since_lsn=1, got {replayed}"
    );

    // Exact-tail reconnect: nothing older than the frontier replays.
    let (status, body) = get_status_and_body(addr, "/v1/changes?since_lsn=3").await;
    assert_eq!(status, "200");
    assert_eq!(body.matches("event: inv").count(), 0, "frontier is quiet");
}

#[tokio::test(flavor = "multi_thread")]
async fn since_lsn_older_than_buffer_answers_409() {
    // Capacity 2: envelopes 4 and 5 evict 1..3.
    let node = cluster(2);
    for lsn in 1..=5u64 {
        let _ = node.admit_and_publish(envelope(&node, lsn as u8, lsn));
    }
    let addr = serve(node.clone(), true).await;

    let (status, body) = get_status_and_body(addr, "/v1/changes?since_lsn=0").await;
    assert_eq!(status, "409", "R-C6: {body}");
    assert!(body.contains("Resync Required"));
    assert!(
        body.to_lowercase().contains("x-exocortex-min-lsn"),
        "409 carries the replay floor header: {body}"
    );

    // The bridgeable reconnect still works.
    let (status, _) = get_status_and_body(addr, "/v1/changes?since_lsn=3").await;
    assert_eq!(status, "200");
}

#[tokio::test(flavor = "multi_thread")]
async fn required_token_mode_answers_401_without_token() {
    let node = cluster(4);
    let _ = node.admit_and_publish(envelope(&node, 1, 1));
    let addr = serve(node.clone(), false).await;

    let (status, body) = get_status_and_body_without_bearer(addr, "/v1/changes?since_lsn=0").await;
    assert_eq!(status, "401", "R-Sec7: {body}");

    // An EMPTY token value is no token either — fail closed.
    let (status, _) =
        get_status_and_body_without_bearer(addr, "/v1/changes?token=&since_lsn=0").await;
    assert_eq!(status, "401", "empty token is rejected");

    let (status, _) =
        get_status_and_body_without_bearer(addr, "/v1/changes?token=t&since_lsn=0").await;
    assert_eq!(
        status, "401",
        "a query token without an authenticated visibility context is rejected"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_replaces_hidden_rows_with_identifier_free_lsn_advances() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = Arc::new(
        ClusterNode::new(
            storage.clone(),
            "visibility-replay".into(),
            onto.fingerprint,
            HMAC_KEY,
        )
        .with_replay_capacity(16),
    );
    let hidden = scoped_memory(41, "hidden-project");
    let visible = scoped_memory(42, "visible-project");
    let hidden_commit = storage.upsert_memory(&hidden).await.unwrap();
    let visible_commit = storage.upsert_memory(&visible).await.unwrap();
    let _ = node.admit_and_publish(node.envelope(Invalidation::MemoryUpserted {
        id: hidden.id,
        lsn: hidden_commit.lsn,
    }));
    let _ = node.admit_and_publish(node.envelope(Invalidation::MemoryUpserted {
        id: visible.id,
        lsn: visible_commit.lsn,
    }));
    let addr = serve_for(node, project_reader()).await;

    let (status, body) = get_status_and_body(addr, "/v1/changes?token=reader&since_lsn=0").await;
    assert_eq!(status, "200", "authenticated replay is served: {body}");
    let events = decoded_events(&body);
    assert_eq!(events.len(), 2, "hidden commits still advance replay LSN");
    assert_eq!(events[0].backend_lsn, hidden_commit.lsn);
    assert!(matches!(events[0].kind, Some(Kind::VisibilityAdvance(_))));
    assert_eq!(events[1].backend_lsn, visible_commit.lsn);
    assert!(matches!(
        &events[1].kind,
        Some(Kind::MemoryUpserted(row)) if row.id == b"2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"
    ));
    let hidden_hex = b"29292929292929292929292929292929";
    assert!(
        !events[0]
            .encode_to_vec()
            .windows(hidden_hex.len())
            .any(|window| window == hidden_hex),
        "the no-op carries no hidden row identifier"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn live_delivery_filters_with_the_same_visibility_and_lsn_contract() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = Arc::new(ClusterNode::new(
        storage.clone(),
        "visibility-live".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    let hidden = scoped_memory(51, "hidden-project");
    let visible = scoped_memory(52, "visible-project");
    let hidden_commit = storage.upsert_memory(&hidden).await.unwrap();
    let visible_commit = storage.upsert_memory(&visible).await.unwrap();
    let addr = serve_for(node.clone(), project_reader()).await;

    let reader = tokio::spawn(get_status_and_body(
        addr,
        "/v1/changes?token=reader&since_lsn=0",
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = node.admit_and_publish(node.envelope(Invalidation::MemoryUpserted {
        id: hidden.id,
        lsn: hidden_commit.lsn,
    }));
    let _ = node.admit_and_publish(node.envelope(Invalidation::MemoryUpserted {
        id: visible.id,
        lsn: visible_commit.lsn,
    }));

    let (status, body) = reader.await.unwrap();
    assert_eq!(status, "200", "authenticated live stream is served: {body}");
    let events = decoded_events(&body);
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].kind, Some(Kind::VisibilityAdvance(_))));
    assert_eq!(events[0].backend_lsn, hidden_commit.lsn);
    assert!(matches!(events[1].kind, Some(Kind::MemoryUpserted(_))));
    assert_eq!(events[1].backend_lsn, visible_commit.lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_filters_relationship_upserts_and_deletes_by_endpoint_scope() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = Arc::new(ClusterNode::new(
        storage.clone(),
        "visibility-relationships".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    let hidden_endpoint = scoped_memory(61, "hidden-project");
    let visible_a = scoped_memory(62, "visible-project");
    let visible_b = scoped_memory(63, "visible-project");
    let mut unscoped_a = scoped_memory(66, "visible-project");
    unscoped_a.visibility = Visibility::Org;
    unscoped_a.context.project_id = None;
    let mut unscoped_b = scoped_memory(67, "visible-project");
    unscoped_b.visibility = Visibility::Org;
    unscoped_b.context.project_id = None;
    storage.upsert_memory(&hidden_endpoint).await.unwrap();
    storage.upsert_memory(&visible_a).await.unwrap();
    storage.upsert_memory(&visible_b).await.unwrap();
    storage.upsert_memory(&unscoped_a).await.unwrap();
    storage.upsert_memory(&unscoped_b).await.unwrap();
    let hidden = relationship(64, hidden_endpoint.id, visible_a.id);
    let visible = relationship(65, visible_a.id, visible_b.id);
    // A narrowed Project edge between Org endpoints has no project subject
    // to authorize against and must fail closed.
    let unscoped = relationship(68, unscoped_a.id, unscoped_b.id);
    let hidden_upsert = storage.upsert_relationship(&hidden).await.unwrap();
    let visible_upsert = storage.upsert_relationship(&visible).await.unwrap();
    let unscoped_upsert = storage.upsert_relationship(&unscoped).await.unwrap();
    let hidden_delete = storage.delete_relationship(&hidden.id).await.unwrap();
    let visible_delete = storage.delete_relationship(&visible.id).await.unwrap();
    for invalidation in [
        Invalidation::RelationshipUpserted {
            id: hidden.id,
            from: hidden.from,
            to: hidden.to,
            kind: hidden.kind,
            lsn: hidden_upsert.lsn,
        },
        Invalidation::RelationshipUpserted {
            id: visible.id,
            from: visible.from,
            to: visible.to,
            kind: visible.kind,
            lsn: visible_upsert.lsn,
        },
        Invalidation::RelationshipUpserted {
            id: unscoped.id,
            from: unscoped.from,
            to: unscoped.to,
            kind: unscoped.kind,
            lsn: unscoped_upsert.lsn,
        },
        Invalidation::RelationshipDeleted {
            id: hidden.id,
            lsn: hidden_delete.lsn,
        },
        Invalidation::RelationshipDeleted {
            id: visible.id,
            lsn: visible_delete.lsn,
        },
    ] {
        let _ = node.admit_and_publish(node.envelope(invalidation));
    }
    let relationship_streams_before = storage.reasoning_query_counts().1;
    let addr = serve_for(node, project_reader()).await;
    let (status, body) = get_status_and_body(
        addr,
        &format!(
            "/v1/changes?token=reader&since_lsn={}",
            hidden_upsert.lsn - 1
        ),
    )
    .await;
    assert_eq!(status, "200", "relationship replay is served: {body}");
    let events = decoded_events(&body);
    assert_eq!(events.len(), 5);
    assert!(matches!(events[0].kind, Some(Kind::VisibilityAdvance(_))));
    assert!(matches!(
        events[1].kind,
        Some(Kind::RelationshipUpserted(_))
    ));
    assert!(matches!(events[2].kind, Some(Kind::VisibilityAdvance(_))));
    assert!(matches!(events[3].kind, Some(Kind::VisibilityAdvance(_))));
    assert!(matches!(events[4].kind, Some(Kind::RelationshipDeleted(_))));
    assert_eq!(
        events
            .iter()
            .map(|event| event.backend_lsn)
            .collect::<Vec<_>>(),
        vec![
            hidden_upsert.lsn,
            visible_upsert.lsn,
            unscoped_upsert.lsn,
            hidden_delete.lsn,
            visible_delete.lsn
        ]
    );
    assert_eq!(
        storage.reasoning_query_counts().1,
        relationship_streams_before,
        "per-subscriber replay uses the relationship-id point seam, never a graph scan"
    );
}

/// CS1 (audit): on a backend-node router the SSE feed is merged INSIDE the
/// bearer layer, and a query `?token=` is never treated as proof of
/// identity — presence of a bearer header is.
#[tokio::test(flavor = "multi_thread")]
async fn backend_router_rejects_token_query_without_bearer() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = ClusterNode::new(storage, "auth-node".into(), onto.fingerprint, HMAC_KEY);
    let cluster = Arc::new(node);
    let _ = cluster.admit_and_publish(envelope(&cluster, 1, 1));

    let (cache, _rx) = exocortex_cache::LocalCache::new(1024);
    let ctx = Arc::new(exocortex_ops::OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc(
            "org",
            "backend",
            exocortex_kernel::Visibility::Org,
        ),
        audit_admin: false,
        storage: Arc::new(exocortex_storage::InMemoryStorage::new(onto.clone()))
            as Arc<dyn exocortex_storage::Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),

        ontology: None,
        ingest_preflight: None,
    });
    let bind = exocortex_server::http_bind::HttpBind::new(
        ctx,
        "test-only-secret-bearer-token-00000000".into(),
    );
    let sse = exocortex_server::sse::sse_router(cluster.clone());
    let app = bind.router(Some(sse));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Any non-empty ?token= value WITHOUT a bearer header: 401 (the pinned
    // defect asserted 200 here for a token that was never provisioned).
    let (status, body) =
        get_status_and_body_without_bearer(addr, "/v1/changes?token=forged&since_lsn=0").await;
    assert_eq!(
        status, "401",
        "CS1: token query is not authentication: {body}"
    );
    let (status, _) =
        get_status_and_body_without_bearer(addr, "/v1/changes?token=&since_lsn=0").await;
    assert_eq!(status, "401", "CS1: empty token without bearer is rejected");

    // With the bearer header the subscriber is served (token selects the
    // per-client key; presence still required in RequiredToken mode).
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(
        format!("GET /v1/changes?token=t&since_lsn=0 HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer test-only-secret-bearer-token-00000000\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(1), sock.read_to_end(&mut buf)).await;
    let raw = String::from_utf8_lossy(&buf);
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .unwrap_or_default();
    assert_eq!(
        status, "200",
        "bearer-authenticated subscriber served: {raw}"
    );
}

#[test]
fn envelope_lsn_extraction_feeds_the_floor_check() {
    let node = cluster(8);
    assert!(
        matches!(node.replay_since(0), exocortex_cluster::Replay::Fresh(v) if v.is_empty()),
        "empty ring bridges any since_lsn"
    );
    for lsn in 1..=3u64 {
        let _ = node.admit_and_publish(envelope(&node, lsn as u8, lsn));
    }
    assert!(matches!(node.replay_since(0), exocortex_cluster::Replay::Fresh(v) if v.len() == 3));
    assert!(matches!(node.replay_since(2), exocortex_cluster::Replay::Fresh(v) if v.len() == 1));
    let _ = InvalidationEnvelope::default().encode_to_vec();
}
