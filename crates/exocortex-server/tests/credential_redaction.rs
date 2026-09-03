//! R6-Q2: failure diagnostics and tracing must never reproduce credentials.

use std::io::Write as _;
use std::sync::{Arc, Mutex};

use exocortex_ingest::IngestServer;
use exocortex_storage::{FalkorConfig, FalkorStorage, InMemoryStorage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
    RegisterSourceRequest,
};

const SENTINEL: &str = "R6_Q2_CREDENTIAL_SENTINEL_7f16c84b";

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

async fn raw_http(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {SENTINEL}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

fn assert_redacted(surface: &str, value: &str) {
    assert!(
        !value.contains(SENTINEL),
        "{surface} reproduced the sentinel credential: {value}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn credentials_are_absent_from_feasible_failure_surfaces() {
    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_writer(captured.clone())
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);

    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let ingest = IngestServer::new(storage.clone(), ontology.clone(), [5; 32]);

    let backend_error = match FalkorStorage::connect(
        FalkorConfig {
            falkor_url: format!("falkor://sentinel:{SENTINEL}@["),
            redis_url: format!("redis://sentinel:{SENTINEL}@["),
            graph_name: "credential-redaction".into(),
            org_id: "org".into(),
            node_id: "redaction-node".into(),
        },
        ontology.clone(),
    )
    .await
    {
        Ok(_) => panic!("malformed credential-bearing backend URL must fail"),
        Err(error) => error,
    };
    assert_redacted(
        "malformed Falkor/Redis returned error",
        &format!("{backend_error:?}"),
    );
    tracing::error!(error = %backend_error, "expected malformed backend URL probe");

    let registration_error = ingest
        .register_source(tonic::Request::new(RegisterSourceRequest {
            default_rights: None,
            org_id: "org".into(),
            source_uri: "session://safe-source".into(),
            producer_id: "safe-producer".into(),
            ceiling: 3,
            source_flavor: "custom".into(),
            projection: None,
            producer: Some(ProducerIdentity {
                node_id: "safe-node".into(),
                agent_id: String::new(),
                adapter_id: String::new(),
                hmac_signature: SENTINEL.as_bytes().to_vec(),
                client_metadata: None,
            }),
            producer_kind: 5,
        }))
        .await
        .expect_err("unsigned sentinel registration fails");
    assert_redacted("registration status", &registration_error.to_string());

    let mut submission = IngestBatch {
        org_id: "org".into(),
        source_uri: "session://safe-source".into(),
        producer_id: "safe-producer".into(),
        batch_id: "safe-batch".into(),
        mapping_version: "test:1".into(),
        ontology_fingerprint: ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![MemoryDraft {
            draft_key: "safe-key".into(),
            memory_type: "Fix".into(),
            title: "safe title".into(),
            content: "safe content".into(),
            visibility: 1,
            ..Default::default()
        }],
        relationships: Vec::new(),
        producer: Some(ProducerIdentity {
            node_id: "safe-node".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: SENTINEL.as_bytes().to_vec(),
            client_metadata: None,
        }),
    };
    submission.checksum = exocortex_wire::signing::canonical_checksum(&submission);
    let submission_ack = ingest
        .submit(tonic::Request::new(submission))
        .await
        .expect("authentication rejection is a protocol ack")
        .into_inner();
    assert_redacted("submission ack", &format!("{submission_ack:?}"));

    let cluster = Arc::new(exocortex_cluster::ClusterNode::new(
        storage.clone(),
        "redaction-node".into(),
        ontology.fingerprint,
        [7; 32],
    ));
    let (cache, _writer) = exocortex_cache::LocalCache::new(1024 * 1024);
    let context = Arc::new(exocortex_ops::OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc(
            "org",
            "user",
            exocortex_kernel::Visibility::Org,
        ),
        audit_admin: false,
        storage: storage.clone(),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        ontology: Some(ontology.clone()),
        ingest_preflight: None,
    });
    let router = exocortex_server::http_bind::HttpBind::new(
        context,
        "test-only-valid-bearer-token-00000000".into(),
    )
    .router(Some(exocortex_server::sse::sse_router(cluster)));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let auth_response = raw_http(address, "/v1/get_memory?id=00").await;
    assert_redacted("malformed bearer HTTP response", &auth_response);
    let sse_response = raw_http(address, "/v1/changes?since_lsn=0").await;
    assert_redacted("SSE authentication response", &sse_response);
    server.abort();

    let key_path = std::env::temp_dir().join(format!(
        "exocortex-invalid-key-{}-{}.pem",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(SENTINEL.as_bytes())
        .unwrap();
    let tls_error = match exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: "org".into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::Tls {
                certificate: "tests/fixtures/localhost-cert.pem".into(),
                private_key: key_path.clone(),
            },
            node_id: "redaction-node".into(),
            cluster_secret: [7; 32],
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(
                    "test-only-valid-bearer-token-00000000".into(),
                    exocortex_ops::operations::ops_vc(
                        "org",
                        "user",
                        exocortex_kernel::Visibility::Org,
                    ),
                )
                .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: Vec::new(),
            redis_url: None,
            quiet_hours: exocortex_dreams::fire::QuietHours::none(),
            admin_source_policies: Vec::new(),
        },
    )
    .await
    {
        Ok(_) => panic!("sentinel private-key contents must be malformed"),
        Err(error) => error,
    };
    assert_redacted("TLS startup error", &format!("{tls_error:#}"));
    let _ = std::fs::remove_file(key_path);

    let startup = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-node"))
        .args(["--mode", "backend-node", "--storage", "memory"])
        .env("EXOCORTEX_CLUSTER_SECRET", SENTINEL)
        .output()
        .expect("execute backend startup failure");
    assert!(!startup.status.success());
    assert_redacted(
        "backend startup stderr",
        &String::from_utf8_lossy(&startup.stderr),
    );

    let policy_dir = tempfile::tempdir().unwrap();
    let principal_policy = policy_dir.path().join("principals.json");
    let source_policy = policy_dir.path().join("sources.json");
    std::fs::write(
        &principal_policy,
        r#"[{"bearer_token":"test-only-valid-bearer-token-00000000","org_id":"org","user_id":"user","project_ids":[],"team_ids":[],"max_visibility":3}]"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&principal_policy, std::fs::Permissions::from_mode(0o600))
            .unwrap();
    }
    std::fs::write(
        &source_policy,
        r#"[{"org_id":"org","source_uri":"session://redaction","producer_id":"redaction","ceiling":3}]"#,
    )
    .unwrap();
    let backend_url = format!("falkor://sentinel:{SENTINEL}@127.0.0.1:1");
    let connection_failure = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-node"))
        .args([
            "--mode",
            "backend-node",
            "--storage",
            &backend_url,
            "--bind",
            "127.0.0.1:0",
            "--allow-plaintext-loopback",
            "--gossip-addr",
            "127.0.0.1:0",
            "--principal-policy",
            principal_policy.to_str().unwrap(),
            "--source-policy",
            source_policy.to_str().unwrap(),
        ])
        .env(
            "EXOCORTEX_CLUSTER_SECRET",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .output()
        .expect("execute credential-bearing backend connection failure");
    assert!(!connection_failure.status.success());
    assert_redacted(
        "Falkor connection-failure stderr",
        &String::from_utf8_lossy(&connection_failure.stderr),
    );

    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert_redacted("captured tracing output", &logs);
}
