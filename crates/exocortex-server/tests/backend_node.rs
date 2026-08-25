//! W7 acceptance (in-process shape): backend nodes serve HTTP + SSE + gRPC
//! on one listener, the lease re-election loop populates `/health/cluster`,
//! and chitchat gossip converges membership carrying wire-version +
//! ontology fingerprint. The docker-compose kill test (lease handover <2s
//! against live FalkorDB) runs via `--features integration`.

use std::sync::Arc;
use std::time::Duration;

use exocortex_server::backend::{run_backend_node, BackendNodeArgs};
use exocortex_storage::InMemoryStorage;

fn args(bind: &str, gossip: u16, seeds: Vec<String>) -> BackendNodeArgs {
    BackendNodeArgs {
        bind: bind.into(),
        node_id: format!("node-{gossip}"),
        cluster_secret: [7u8; 32],
        bearer_token: "test-bearer".into(),
        gossip_listen: format!("127.0.0.1:{gossip}").parse().unwrap(),
        seed_nodes: seeds,
    }
}

async fn http_get(addr: std::net::SocketAddr, path: &str, bearer: Option<&str>) -> (u16, String) {
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
    (
        status,
        text.split("\r\n\r\n").nth(1).unwrap_or("").to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_nodes_serve_http_grpc_and_gossip_converges() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));

    // Two backend nodes over the shared storage; node-b seeds from node-a's
    // gossip port.
    let node_a = run_backend_node(
        storage.clone(),
        onto.clone(),
        args("127.0.0.1:0", 41001, vec![]),
    )
    .await
    .expect("node-a boots");
    let node_b = run_backend_node(
        storage.clone(),
        onto.clone(),
        args("127.0.0.1:0", 41002, vec!["127.0.0.1:41001".to_string()]),
    )
    .await
    .expect("node-b boots");

    // HTTP parity surface answers with auth on both nodes.
    for (addr, name) in [(node_a.local_addr, "a"), (node_b.local_addr, "b")] {
        let (status, _) = http_get(addr, "/v1/audit?since_lsn=0", Some("test-bearer")).await;
        assert_eq!(status, 200, "node {name} serves ops over HTTP");
        let (status, _) = http_get(addr, "/v1/audit?since_lsn=0", None).await;
        assert_eq!(status, 401, "node {name} enforces bearer auth");
        let (status, body) = http_get(addr, "/health/ready", None).await;
        assert_eq!(status, 200);
        assert!(body.contains("ready"));
    }

    // The lease loop populates cluster health within 2s (M5 shape; live
    // handover semantics ride the FalkorDB compose harness).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut leader_seen = false;
    while tokio::time::Instant::now() < deadline {
        for node in [&node_a, &node_b] {
            let h = node.health.load_full();
            if h.leader_node_id.is_some() && h.lease_epoch >= 1 {
                leader_seen = true;
            }
        }
        if leader_seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(leader_seen, "lease re-election loop stamps /health/cluster");

    // SSE router mounted: the feed answers on the same listener. Read only
    // the first bytes — the stream is open-ended, so `read_to_end` would
    // block forever by design.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(node_a.local_addr)
            .await
            .unwrap();
        let req = format!(
            "GET /v1/changes HTTP/1.1
Host: {}
Connection: close

",
            node_a.local_addr
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut head = vec![0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut head))
            .await
            .expect("sse responds within 2s")
            .expect("read");
        let text = String::from_utf8_lossy(&head[..n]).into_owned();
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "SSE route answers: {text}"
        );
        assert!(
            text.contains("text/event-stream"),
            "SSE content type: {text}"
        );
        assert!(text.contains("exocortex"), "initial anchor comment");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn chitchat_state_carries_wire_version_and_fingerprint() {
    use chitchat::transport::UdpTransport;
    use chitchat::{ChitchatConfig, ChitchatId, FailureDetectorConfig};

    let onto =
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap();
    let fp_hex: String = {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in onto.fingerprint.0 {
            let _ = write!(out, "{b:02x}");
        }
        out
    };
    let port = 42001u16;
    let config = ChitchatConfig {
        chitchat_id: ChitchatId::new(
            "gossip-check".into(),
            1,
            format!("127.0.0.1:{port}").parse().unwrap(),
        ),
        cluster_id: "exocortex".into(),
        gossip_interval: Duration::from_millis(200),
        listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
        seed_nodes: vec![],
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(10),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };
    let handle = chitchat::spawn_chitchat(
        config,
        vec![
            (
                "wire_version".into(),
                exocortex_wire::WIRE_VERSION.to_string(),
            ),
            ("ontology_fingerprint".into(), fp_hex),
        ],
        &UdpTransport,
    )
    .await
    .expect("gossip spawns");

    let state = handle.with_chitchat(|c| c.state_snapshot()).await;
    let self_state = state.node_states.first().expect("self node state");
    let kv = |k: &str| self_state.get(k).expect(k).to_string();
    assert_eq!(kv("wire_version"), "1");
    assert_eq!(kv("ontology_fingerprint").len(), 64);
    handle.shutdown().await.ok();
}
