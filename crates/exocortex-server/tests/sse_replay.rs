//! R-C6 server-side replay tests + R-Sec7 SSE auth: the `/v1/changes`
//! handler honors `?since_lsn=` (replay, then live), answers `409 Resync
//! Required` past the buffer floor, and `401` without a token when auth is
//! required. In-memory backend; raw-socket SSE reading (no HTTP client
//! dependency in this crate — same pattern as `sse_e2e.rs`).

use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::ClusterNode;
use exocortex_kernel::MemoryId;
use exocortex_storage::{InMemoryStorage, Invalidation};
use exocortex_wire::cluster::v1::InvalidationEnvelope;
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

fn envelope(node: &ClusterNode<InMemoryStorage>, id: u8, lsn: u64) -> InvalidationEnvelope {
    node.envelope(Invalidation::MemoryUpserted {
        id: MemoryId([id; 16]),
        lsn,
    })
}

async fn serve(
    node: Arc<ClusterNode<InMemoryStorage>>,
    auth: exocortex_server::sse::SseAuth,
) -> std::net::SocketAddr {
    let app = exocortex_server::sse::sse_router(node, auth);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

/// GET `path`; read until EOF, 400ms of quiet, or an overall 2s deadline
/// (SSE streams stay open by design — `read_to_end` would never return).
async fn get_status_and_body(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n")
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
            Ok(Ok(0)) => break,                        // EOF
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) | Err(_) => break,             // error or quiet: done
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

#[tokio::test(flavor = "multi_thread")]
async fn since_lsn_replays_buffered_deltas_in_order() {
    let node = cluster(16);
    for lsn in 1..=3u64 {
        node.publish_envelope(envelope(&node, lsn as u8, lsn));
    }
    let addr = serve(node.clone(), exocortex_server::sse::SseAuth::OptionalToken).await;

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
        node.publish_envelope(envelope(&node, lsn as u8, lsn));
    }
    let addr = serve(node.clone(), exocortex_server::sse::SseAuth::OptionalToken).await;

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
    node.publish_envelope(envelope(&node, 1, 1));
    let addr = serve(node.clone(), exocortex_server::sse::SseAuth::RequiredToken).await;

    let (status, body) = get_status_and_body(addr, "/v1/changes?since_lsn=0").await;
    assert_eq!(status, "401", "R-Sec7: {body}");

    let (status, _) = get_status_and_body(addr, "/v1/changes?token=t&since_lsn=0").await;
    assert_eq!(status, "200", "token-bearing subscriber is served");
}

#[test]
fn envelope_lsn_extraction_feeds_the_floor_check() {
    let node = cluster(8);
    assert!(
        matches!(node.replay_since(0), exocortex_cluster::Replay::Fresh(v) if v.is_empty()),
        "empty ring bridges any since_lsn"
    );
    for lsn in 1..=3u64 {
        node.publish_envelope(envelope(&node, lsn as u8, lsn));
    }
    assert!(matches!(node.replay_since(0), exocortex_cluster::Replay::Fresh(v) if v.len() == 3));
    assert!(matches!(node.replay_since(2), exocortex_cluster::Replay::Fresh(v) if v.len() == 1));
    let _ = InvalidationEnvelope::default().encode_to_vec();
}
