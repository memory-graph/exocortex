//! W6/H4/H5 acceptance: every registered operation answers over HTTP on a
//! running server (CR-9 parity, §21.3.6 over the wire), outputs are
//! byte-identical to the typed handler's, bearer auth is enforced (R-Sec7),
//! and the observability endpoints mount (R-O2/R-O4).

use std::sync::Arc;

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_ops::operations::ops_vc;
use exocortex_ops::{entries, OpContext};
use exocortex_server::http_bind::HttpBind;
use exocortex_storage::{InMemoryStorage, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn mem(title: &str, n: u8) -> Memory {
    Memory {
        id: MemoryId([n; 16]),
        memory_type: 3,
        title: title.into(),
        content: format!("c {title}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: None,
            user_id: None,
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

fn hex(id: &MemoryId) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id.0 {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Minimal HTTP/1.1 client over a TCP socket (no new dependency).
async fn http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&serde_json::Value>,
) -> (u16, serde_json::Value, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_str = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    let json = serde_json::from_str(&body_str).unwrap_or(serde_json::Value::Null);
    (status, json, body_str)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_operation_answers_over_http_with_auth() {
    let onto = ontology();
    let storage = Arc::new(InMemoryStorage::new(onto));
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let a = mem("parity-target", 1);
    let b = mem("parity-other", 2);
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(a.clone());
    snap.push_test_memory(b.clone());
    cache.publish("org", Arc::new(snap));
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    let ctx = Arc::new(OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        // D2: preflight needs the effective rulebook.
        ontology: Some(ontology()),
    });

    let bind = HttpBind::new(ctx.clone(), "secret-token".into());
    let app = bind.router(None);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let input_for = |name: &str| -> serde_json::Value {
        match name {
            "find_related" => serde_json::json!({ "anchor": hex(&a.id), "k": 2 }),
            "get_memory" => serde_json::json!({ "id": hex(&a.id) }),
            "search_memories" => serde_json::json!({ "query": "parity", "limit": 5 }),
            // D2: a small valid batch (Problem + Fix + Fixes edge).
            "preflight_wrapup" => serde_json::json!({
                "project_id": "p",
                "memories": [
                    { "draft_key": "p", "memory_type": "Problem", "title": "Pool exhausted",
                      "content": "c", "visibility": "project", "tags": [] },
                    { "draft_key": "f", "memory_type": "Fix", "title": "Fixed pool exhaustion",
                      "content": "c", "visibility": "project", "tags": [] }
                ],
                "edges": [
                    { "from_draft_key": "f", "to_draft_key": "p", "to_memory_id": "",
                      "kind": "Fixes", "strength": 0.9 }
                ]
            }),
            "promote_visibility" => {
                serde_json::json!({ "memory_id": hex(&b.id), "to": "org" })
            }
            "accept_discovery" => serde_json::json!({
                "discovery_id": "22222222-2222-2222-2222-222222222222",
                "from": hex(&a.id),
                "to": hex(&b.id),
                "kind": "RelatedTo",
            }),
            "list_audit_records" => serde_json::json!({ "since_lsn": 0 }),
            other => panic!("no test input crafted for op {other}"),
        }
    };

    // CR-9: for each registered op, HTTP output == typed-handler output.
    let all = entries();
    assert!(
        all.len() >= 6,
        "expected the registered op set, got {}",
        all.len()
    );
    for entry in &all {
        let input = input_for(entry.name);
        let expected = (entry.handler)(&ctx, input.clone())
            .await
            .unwrap_or_else(|e| panic!("{}: handler: {e}", entry.name));

        let (status, actual, _) = if (entry.http_method)() == axum::http::Method::GET {
            let qs = query_of(&input);
            http(
                addr,
                "GET",
                &format!("{}?{}", entry.http_path, qs),
                Some("secret-token"),
                None,
            )
            .await
        } else {
            http(
                addr,
                "POST",
                entry.http_path,
                Some("secret-token"),
                Some(&input),
            )
            .await
        };
        assert_eq!(status, 200, "{}: http status", entry.name);
        parity_check(entry.name, &actual, &expected);
    }

    // R-Sec7: no bearer -> 401 on every op route.
    let (status, _, _) = http(
        addr,
        "POST",
        "/v1/get_memory",
        None,
        Some(&serde_json::json!({ "id": hex(&a.id) })),
    )
    .await;
    assert_eq!(status, 401, "auth enforced");
    let (status, _, _) = http(
        addr,
        "POST",
        "/v1/get_memory",
        Some("wrong-token"),
        Some(&serde_json::json!({ "id": hex(&a.id) })),
    )
    .await;
    assert_eq!(status, 401, "wrong bearer rejected");

    // H4: observability endpoints.
    let (status, body, text) = http(addr, "GET", "/metrics", None, None).await;
    assert_eq!(status, 200);
    assert!(body.is_null(), "metrics is text, not JSON");
    assert!(text.contains("exocortex"), "prometheus text format: {text}");
    let (status, body, _) = http(addr, "GET", "/health/ready", None, None).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ready");
    for path in ["/health/cluster", "/health/sync", "/health/hydration"] {
        let (status, body, _) = http(addr, "GET", path, None, None).await;
        assert_eq!(status, 200, "{path} answers");
        assert!(body.is_object(), "{path} returns JSON");
    }
}

/// CR-9 parity per op: read-only ops must match byte-for-byte; stateful
/// ops append a fresh audit record per invocation, so their comparison
/// covers every stable field and requires a live audit LSN on both sides.
fn parity_check(name: &str, http_out: &serde_json::Value, direct: &serde_json::Value) {
    match name {
        "accept_discovery" => {
            assert_eq!(http_out["edge_id"], direct["edge_id"], "{name}: edge id");
            assert!(http_out["audit_lsn"].as_u64().unwrap_or(0) > 0);
            assert!(direct["audit_lsn"].as_u64().unwrap_or(0) > 0);
        }
        "promote_visibility" => {
            assert_eq!(http_out["memory_id"], direct["memory_id"], "{name}");
            assert_eq!(http_out["visibility"], direct["visibility"], "{name}");
            assert!(http_out["audit_lsn"].as_u64().unwrap_or(0) > 0);
        }
        "list_audit_records" => {
            // The audit log only grows; every direct record must appear in
            // the HTTP view.
            let direct_records = direct["records"].as_array().expect("records");
            let http_records = http_out["records"].as_array().expect("records");
            assert!(http_records.len() >= direct_records.len(), "{name}");
            for r in direct_records {
                assert!(
                    http_records.contains(r),
                    "{name}: direct record {r} present over HTTP"
                );
            }
        }
        other => {
            assert_eq!(
                http_out, direct,
                "{other}: HTTP output matches the typed handler (CR-9)"
            );
        }
    }
}

fn query_of(v: &serde_json::Value) -> String {
    let obj = v.as_object().expect("object input");
    obj.iter()
        .map(|(k, val)| match val {
            serde_json::Value::String(s) => format!("{k}={s}"),
            other => format!("{k}={other}"),
        })
        .collect::<Vec<_>>()
        .join("&")
}
