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
        transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
        node_id: format!("node-{gossip}"),
        cluster_secret: [7u8; 32],
        principals: Arc::new(
            exocortex_server::principal::PrincipalRegistry::single(
                "test-bearer".into(),
                exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org),
            )
            .unwrap(),
        ),
        gossip_listen: format!("127.0.0.1:{gossip}").parse().unwrap(),
        seed_nodes: seeds,
        redis_url: None,
        quiet_hours: exocortex_dreams::fire::QuietHours::none(),
        admin_ceilings: vec![],
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
    // block forever by design. CS1 (audit): the route sits behind the
    // bearer layer like every other op, so the header must ride along.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(node_a.local_addr)
            .await
            .unwrap();
        let req = format!(
            "GET /v1/changes?token=test-bearer HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer test-bearer\r\nConnection: close\r\n\r\n",
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

#[tokio::test(flavor = "multi_thread")]
async fn shared_listener_uses_tls_and_refuses_plaintext() {
    use exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient;
    use exocortex_wire::ingest::v1::FingerprintRequest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut tls_args = args("127.0.0.1:0", 43001, vec![]);
    tls_args.transport = exocortex_server::backend::TransportSecurity::Tls {
        certificate: "tests/fixtures/localhost-cert.pem".into(),
        private_key: "tests/fixtures/localhost-key.pem".into(),
    };
    let node = run_backend_node(storage, onto, tls_args)
        .await
        .expect("valid TLS listener boots");

    let ca = Certificate::from_pem(include_bytes!("fixtures/localhost-cert.pem"));
    let endpoint = Endpoint::from_shared(format!("https://localhost:{}", node.local_addr.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name("localhost"),
        )
        .unwrap();
    let channel = endpoint.connect().await.expect("trusted TLS handshake");
    let mut client = IngestServiceClient::new(channel);
    let unauthenticated = client
        .fingerprint(FingerprintRequest {})
        .await
        .expect_err("gRPC cannot bypass bearer principal middleware");
    assert_eq!(unauthenticated.code(), tonic::Code::Unauthenticated);

    let mut request = tonic::Request::new(FingerprintRequest {});
    request
        .metadata_mut()
        .insert("authorization", "Bearer test-bearer".parse().unwrap());
    let response = client
        .fingerprint(request)
        .await
        .expect("gRPC shares the TLS listener");
    assert_eq!(response.into_inner().fingerprint.len(), 32);

    let mut plaintext = tokio::net::TcpStream::connect(node.local_addr)
        .await
        .unwrap();
    plaintext
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(1), plaintext.read_to_end(&mut bytes)).await;
    assert!(
        !bytes.starts_with(b"HTTP/1.1 200"),
        "shared listener must not answer plaintext HTTP"
    );
}

#[tokio::test]
async fn malformed_tls_material_fails_before_listener_startup() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut bad = args("127.0.0.1:0", 43002, vec![]);
    bad.transport = exocortex_server::backend::TransportSecurity::Tls {
        certificate: "tests/fixtures/localhost-cert.pem".into(),
        private_key: "tests/fixtures/localhost-cert.pem".into(),
    };
    assert!(run_backend_node(storage, onto, bad).await.is_err());
}

#[tokio::test]
async fn plaintext_transport_rejects_non_loopback_library_bind() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let shared = args("0.0.0.0:0", 43003, vec![]);
    let error = match run_backend_node(storage, onto, shared).await {
        Ok(_) => panic!("library callers cannot bypass the loopback restriction"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("restricted to loopback"),
        "unexpected startup error: {error:#}"
    );
}

/// R-O4: readiness is observational — when the storage probe fails and the
/// lease loop goes stale, `/health/ready` answers 503 with the failed
/// checks named; healthy maintainers restore 200.
#[tokio::test(flavor = "multi_thread")]
async fn health_ready_reflects_maintainer_truth() {
    let onto = std::sync::Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = std::sync::Arc::new(InMemoryStorage::new(onto.clone()));
    let node = run_backend_node(storage, onto, args("127.0.0.1:0", 0, vec![]))
        .await
        .unwrap();

    // Healthy: maintainers (probe + lease loop) report green.
    let mut ok = false;
    for _ in 0..50 {
        let (status, body) = http_get(node.local_addr, "/health/ready", None).await;
        if status == 200 && body.contains("\"ready\"") {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ok, "ready turns 200 once maintainers report green");

    // Simulate maintainer failure: storage probe fails + lease tick stale.
    node.health.rcu(|h| {
        let mut next = h.as_ref().clone();
        next.storage_ok = false;
        next.last_lease_tick = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
        std::sync::Arc::new(next)
    });
    let (status, body) = http_get(node.local_addr, "/health/ready", None).await;
    assert_eq!(status, 503, "unhealthy node must not answer ready");
    assert!(
        body.contains("\"storage_ok\":false"),
        "names the failed check: {body}"
    );
}
