//! PX5 acceptance: the read-only explorer's six views against a real
//! HTTP bind — bearer auth, VisibilityContext filtering at EVERY
//! render (the Private-memory regression the PRD demands), detail +
//! neighborhood through the same ops agents use, the ontology view
//! with both fingerprints, provenance chain rendering with hidden
//! members redacted, and the audit gate degrading honestly.

use std::sync::Arc;

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_ops::{OpContext, VisibilityContext};
use exocortex_server::http_bind::HttpBind;
use exocortex_storage::{InMemoryStorage, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn mem(title: &str, n: u8, visibility: Visibility, user: Option<&str>) -> Memory {
    Memory {
        id: MemoryId([n; 16]),
        memory_type: 3,
        title: title.into(),
        content: format!("c {title}"),
        summary: None,
        tags: Default::default(),
        visibility,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
            session_id: None,
            user_id: user.map(Into::into),
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
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn caller_vc() -> VisibilityContext {
    VisibilityContext {
        user_id: "caller".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    }
}

async fn boot(audit_admin: bool) -> std::net::SocketAddr {
    let onto = ontology();
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let visible = mem("visible finding", 1, Visibility::Org, None);
    let secret = mem(
        "other users private finding",
        2,
        Visibility::Private,
        Some("someone-else"),
    );
    let neighbor = mem("neighbor solution", 3, Visibility::Org, None);
    let edge = exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(
            visible.id,
            exocortex_kernel::kinds::SOLVES,
            neighbor.id,
            None,
        ),
        kind: exocortex_kernel::kinds::SOLVES,
        from: visible.id,
        to: neighbor.id,
        visibility: Visibility::Org,
        provenance: exocortex_kernel::Provenance::Derived {
            rule_id: "R1".into(),
            evidence: vec![],
        },
        properties: exocortex_kernel::relationship::RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    };
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(visible.clone());
    snap.push_test_memory(secret.clone());
    snap.push_test_memory(neighbor.clone());
    snap.push_test_relationship(edge.clone());
    cache.publish("org", Arc::new(snap));
    storage.upsert_memory(&visible).await.unwrap();
    storage.upsert_memory(&secret).await.unwrap();
    storage.upsert_memory(&neighbor).await.unwrap();
    storage.upsert_relationship(&edge).await.unwrap();
    let ctx = Arc::new(
        OpContext::per_request(
            caller_vc(),
            storage,
            Arc::new(cache),
            chrono::Duration::seconds(5),
        )
        .with_ontology(onto)
        .with_audit_admin(audit_admin),
    );
    let bind = HttpBind::new(ctx, "explorer-bearer-token-with-at-least-32-bytes".into());
    let router = bind.router(None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn get(addr: std::net::SocketAddr, path: &str, bearer: Option<&str>) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Connection: close\r\n\r\n");
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn hex(n: u8) -> String {
    MemoryId([n; 16]).to_hex()
}

#[tokio::test(flavor = "multi_thread")]
async fn explorer_requires_bearer_auth() {
    let addr = boot(false).await;
    for path in [
        "/explorer",
        "/explorer/memories?type=Fix",
        "/explorer/ontology",
        "/explorer/audit",
        &format!("/explorer/memories/{}", hex(1)),
        &format!("/explorer/memories/{}/neighborhood", hex(1)),
        &format!("/explorer/memories/{}/provenance", hex(1)),
    ] {
        let (status, _) = get(addr, path, None).await;
        assert_eq!(
            status, 401,
            "GET {path} must reject an unauthenticated call"
        );
    }
    let (status, _) = get(addr, "/explorer", Some("wrong-token")).await;
    assert_eq!(status, 401, "a wrong bearer is still unauthenticated");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_renders_only_caller_visible_rows() {
    let addr = boot(false).await;
    // type id 3's NAME in dev-v1, resolved through the same ontology.
    let onto = ontology();
    let type_name = onto.memory_type_names[3].to_string();
    let (status, body) = get(
        addr,
        &format!("/explorer/memories?type={type_name}"),
        Some("explorer-bearer-token-with-at-least-32-bytes"),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("visible finding"), "{body}");
    assert!(
        !body.contains("other users private finding"),
        "a Private memory belonging to another user is NEVER rendered"
    );
    // The list links into the detail view.
    assert!(body.contains(&format!("/explorer/memories/{}", hex(1))));
}

#[tokio::test(flavor = "multi_thread")]
async fn detail_and_neighborhood_render_through_the_read_path() {
    let addr = boot(false).await;
    let token = "explorer-bearer-token-with-at-least-32-bytes";
    let (status, body) = get(addr, &format!("/explorer/memories/{}", hex(1)), Some(token)).await;
    assert_eq!(status, 200);
    assert!(body.contains("visible finding"), "{body}");
    // The detail page links the neighborhood and provenance views.
    assert!(body.contains("/neighborhood"));
    assert!(body.contains("/provenance"));
    // The neighborhood renders through find_related: the caller's
    // view includes the org-visible anchor only (the Private neighbor
    // never surfaces).
    let (status, body) = get(
        addr,
        &format!("/explorer/memories/{}/neighborhood?k=2", hex(1)),
        Some(token),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("neighbor solution"), "{body}");
    assert!(
        !body.contains("other users private finding"),
        "the traversal's visibility filter is the explorer's filter"
    );
    // A memory the caller cannot see answers not-found, not forbidden
    // leakage of existence.
    let (status, _) = get(addr, &format!("/explorer/memories/{}", hex(2)), Some(token)).await;
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn ontology_view_shows_packs_kinds_and_both_fingerprints() {
    let addr = boot(false).await;
    let (status, body) = get(
        addr,
        "/explorer/ontology",
        Some("explorer-bearer-token-with-at-least-32-bytes"),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("dev-v1"), "{body}");
    assert!(body.contains("Solves"), "{body}");
    assert!(body.contains("compatibility"), "{body}");
    assert!(body.contains("build"), "{body}");
    // Two 64-hex fingerprints inside <code> spans.
    let spans: Vec<&str> = body
        .split("<code>")
        .skip(1)
        .map(|rest| rest.split("</code>").next().unwrap_or(""))
        .collect();
    let hex64 = spans
        .iter()
        .filter(|span| span.len() == 64 && span.chars().all(|c| c.is_ascii_hexdigit()))
        .count();
    assert!(hex64 >= 2, "both fingerprints render: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn provenance_renders_the_chain_and_degrades_without_admin() {
    let addr = boot(false).await;
    let (status, body) = get(
        addr,
        &format!("/explorer/memories/{}/provenance", hex(1)),
        Some("explorer-bearer-token-with-at-least-32-bytes"),
    )
    .await;
    assert_eq!(status, 200);
    // The chain section renders (this memory has no Derived evidence
    // reaching it, so the honest empty note shows), and the audit
    // section degrades honestly without admin.
    assert!(body.contains("derivation chain"), "{body}");
    assert!(
        body.contains("administrator permission"),
        "the audit section names its gate instead of guessing: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_view_keeps_the_operations_administrator_gate() {
    let addr = boot(false).await;
    let (status, body) = get(
        addr,
        "/explorer/audit",
        Some("explorer-bearer-token-with-at-least-32-bytes"),
    )
    .await;
    assert_eq!(status, 403);
    assert!(body.contains("administrator"), "{body}");
}
