//! R17 / CUJ-1+2: the FIRST out-of-process producer. Spawns the real
//! `exocortex-node` binary (`--storage memory`, a separate OS process),
//! connects the fixture adapter through a real gRPC socket, and runs
//! register → fingerprint → submit → replay → read-back.
//!
//! Gating: the test requires no live backing store (memory storage), so
//! it runs unconditionally — if the binary fails to boot, this FAILS
//! rather than skipping (the PRD's loud-gate rule).

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use exocortex_adapter_sdk::{AdapterConfig, AdapterSession, BatchUnit};
use exocortex_wire::ingest::v1::MemoryDraft;

struct Node {
    child: Child,
    addr: std::net::SocketAddr,
    _policy_dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_node() -> Node {
    let port = free_port();
    let gossip = free_port();
    let policy_dir = tempfile::tempdir().unwrap();
    let policy = policy_dir.path().join("sources.json");
    std::fs::write(
        &policy,
        r#"[{"org_id":"org","source_uri":"fixture://oop","producer_id":"oop-fixture","ceiling":3,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
    )
    .unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&policy, std::fs::Permissions::from_mode(0o600)).unwrap();
    let principals = policy_dir.path().join("principals.json");
    std::fs::write(
        &principals,
        r#"[{"bearer_token":"test-only-oop-bearer-token-00000000","org_id":"org","user_id":"oop","project_ids":[],"team_ids":[],"max_visibility":3}]"#,
    )
    .unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&principals, std::fs::Permissions::from_mode(0o600)).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_exocortex-node"))
        .args([
            "--mode",
            "backend-node",
            "--storage",
            "memory",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--allow-plaintext-loopback",
            "--gossip-addr",
            &format!("127.0.0.1:{gossip}"),
            "--principal-policy",
            principals.to_str().unwrap(),
            "--source-policy",
            policy.to_str().unwrap(),
        ])
        .env(
            "EXOCORTEX_CLUSTER_SECRET",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exocortex-node");
    Node {
        child,
        addr: format!("127.0.0.1:{port}").parse().unwrap(),
        _policy_dir: policy_dir,
    }
}

fn draft(k: &str) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: k.into(),
        id: String::new(),
        memory_type: "General".into(),
        title: format!("out-of-process {k}"),
        content: "committed by a real separate process".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fixture_adapter_completes_the_protocol_out_of_process() {
    let node = spawn_node();

    // Wait for the listener.
    let mut up = false;
    for _ in 0..100 {
        if std::net::TcpStream::connect(node.addr).is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up, "exocortex-node backend came up on {}", node.addr);

    let dir = tempfile::tempdir().unwrap();
    let cfg = AdapterConfig {
        org_id: "org".into(),
        source_uri: "fixture://oop".into(),
        producer_id: "oop-fixture".into(),
        adapter_id: "oop-fixture-adapter".into(),
        node_id: "oop-fixture-node".into(),
        source_flavor: "custom".into(),
        producer_kind: exocortex_wire::ingest::v1::ProducerKind::Custom,
        ceiling: 3,
        backend_url: format!("http://{}", node.addr),
        auth_token: "test-only-oop-bearer-token-00000000".into(),
        hmac_key: [0x42u8; 32],
        max_batch_bytes: 4 * 1024 * 1024,
        cursor_path: dir.path().join("oop.cursor"),
        retry: exocortex_adapter_sdk::RetryPolicy::default(),
        projection: None,
    };

    let mut session = AdapterSession::connect(cfg).await.expect("handshake");
    let unit = BatchUnit {
        batch_id_seed: "window-1".into(),
        memories: vec![draft("k1"), draft("k2"), draft("k3")],
        relationships: vec![],
        snapshot: None,
        observed_at: std::time::SystemTime::now(),
    };
    let out = session
        .submit_window(vec![unit.clone()], "window-1")
        .await
        .expect("first window");
    assert_eq!(out.accepted, 3, "accepted > 0 over a real socket");
    assert!(out.cursor_advanced);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("oop.cursor")).unwrap(),
        "window-1"
    );

    // CUJ-2: replay the same window — DuplicateBatch, no duplication.
    let replay = session
        .submit_window(vec![unit], "window-1")
        .await
        .expect("replay settles");
    assert_eq!(replay.duplicates, 1, "replay is DUPLICATE_BATCH");

    // Read-back over the authenticated HTTP ops surface: the memories
    // committed by the out-of-process producer are queryable.
    let body = search_http(node.addr, "out-of-process").await;
    assert_eq!(
        body.matches("out-of-process").count(),
        3,
        "read-back finds all three committed rows exactly once (replay duplicated nothing): {body}"
    );

    // R6-R47: these tag shapes collided under the delimiter-based checksum.
    // Reuse the adapter batch-id seed so content identity is the only
    // differentiator, then cross the real SDK -> gRPC -> ingest dedupe path.
    let mut one_tag = draft("tag-boundary");
    one_tag.tags = vec!["a,b".into()];
    let mut two_tags = one_tag.clone();
    two_tags.tags = vec!["a".into(), "b".into()];
    let tagged_unit = |memories| BatchUnit {
        batch_id_seed: "tag-boundary".into(),
        memories,
        relationships: vec![],
        snapshot: None,
        observed_at: std::time::SystemTime::UNIX_EPOCH,
    };
    let first = session
        .submit_window(vec![tagged_unit(vec![one_tag])], "tag-boundary-one")
        .await
        .expect("single-tag batch commits");
    let second = session
        .submit_window(vec![tagged_unit(vec![two_tags])], "tag-boundary-two")
        .await
        .expect("split-tag batch commits");
    assert_eq!((first.accepted, first.duplicates), (1, 0));
    assert_eq!(
        (second.accepted, second.duplicates),
        (1, 0),
        "distinct canonical tag encodings must not enter DuplicateBatch replay"
    );
}

/// Minimal authenticated HTTP client (no reqwest in this crate): open a
/// socket, speak HTTP/1.1 by hand, return the response body.
async fn search_http(addr: std::net::SocketAddr, query: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = format!("{{\"query\":\"{query}\",\"limit\":10}}");
    let req = format!(
        "POST /v1/search_memories HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer test-only-oop-bearer-token-00000000\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(400), sock.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            _ => {}
        }
    }
    let raw = String::from_utf8_lossy(&buf).to_string();
    raw.split("\r\n\r\n").nth(1).unwrap_or_default().to_string()
}
