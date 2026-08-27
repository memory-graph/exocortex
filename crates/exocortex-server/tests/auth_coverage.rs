//! GATE1 / audit §2.3: auth coverage. Every network-reachable endpoint
//! on a backend-node router rejects an unauthenticated call — the op
//! routes AND the SSE change feed (which previously sat outside the
//! bearer layer).

use std::sync::Arc;

use exocortex_cluster::ClusterNode;
use exocortex_kernel::Ontology;
use exocortex_storage::{InMemoryStorage, Storage};

fn boot_router() -> (axum::Router, Arc<InMemoryStorage>) {
    let onto = Arc::new(Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap());
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = ClusterNode::new(
        storage.clone(),
        "auth-cov".into(),
        onto.fingerprint,
        [7u8; 32],
    );
    let cluster = Arc::new(node);
    let (cache, _rx) = exocortex_cache::LocalCache::new(1024);
    let ctx = Arc::new(exocortex_ops::OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc(
            "org",
            "backend",
            exocortex_kernel::Visibility::Org,
        ),
        storage: storage.clone() as Arc<dyn Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),

        ontology: None,
    });
    let bind = exocortex_server::http_bind::HttpBind::new(ctx, "secret-bearer".into());
    let sse =
        exocortex_server::sse::sse_router(cluster, exocortex_server::sse::SseAuth::RequiredToken);
    (bind.router(Some(sse)), storage)
}

async fn serve(router: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

/// Raw GET/POST without an Authorization header; returns the status.
async fn unauth(addr: std::net::SocketAddr, method: &str, path: &str, body: Option<&str>) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body_block = body
        .map(|b| {
            format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
                b.len()
            )
        })
        .unwrap_or_else(|| "\r\n".into());
    let req =
        format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n{body_block}");
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sock.read_to_end(&mut buf),
    )
    .await;
    let raw = String::from_utf8_lossy(&buf);
    raw.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// §2.3: EVERY registered operation route + the SSE feed answers 401
/// without a bearer header. A newly registered operation joins the table
/// automatically via `entries()`.
#[tokio::test(flavor = "multi_thread")]
async fn every_endpoint_rejects_unauthenticated_calls() {
    let (router, _storage) = boot_router();
    let addr = serve(router).await;

    // Enumerate the registered op routes from the registry itself.
    let ops: Vec<(http::Method, &'static str)> = exocortex_ops::entries()
        .iter()
        .map(|e| (method_of(e), e.http_path))
        .collect();
    assert!(!ops.is_empty(), "operations are registered");

    let body = r#"{"id":"00000000000000000000000000000000"}"#;
    for (method, path) in &ops {
        let status = unauth(addr, method.as_str(), path, Some(body)).await;
        assert_eq!(
            status, 401,
            "{method} {path} must reject an unauthenticated call"
        );
    }

    // The SSE feed — with AND without a token query value.
    for path in ["/v1/changes?token=x", "/v1/changes", "/v1/changes?token="] {
        let status = unauth(addr, "GET", path, None).await;
        assert_eq!(status, 401, "GET {path} must reject unauthenticated");
    }
    assert_eq!(
        unauth(addr, "GET", "/metrics", None).await,
        401,
        "metrics must not expose deployment data without authentication"
    );
}

/// The registry's own method declaration (http crate re-exported via axum).
fn method_of(e: &exocortex_ops::OperationEntry) -> http::Method {
    (e.http_method)()
}
