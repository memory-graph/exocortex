//! H1: `exocortex-worker --adapter noop` boots and idles without a live
//! backend (M6 AC; the lazy channel must not dial on startup).
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::process::Command;

#[test]
fn noop_adapter_boots_without_backend() {
    let bin = env!("CARGO_BIN_EXE_exocortex-worker");
    let mut child = Command::new(bin)
        .args(["--adapter", "noop", "--backend", "http://127.0.0.1:1"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn worker");
    std::thread::sleep(std::time::Duration::from_millis(750));
    let exited = child.try_wait().expect("child is alive or exited cleanly");
    assert!(exited.is_none(), "worker must idle, not exit: {exited:?}");
    child.kill().expect("kill");
    child.wait().expect("reaped");
}

#[test]
fn remote_plaintext_backend_is_rejected_before_worker_startup() {
    let out = Command::new(env!("CARGO_BIN_EXE_exocortex-worker"))
        .args([
            "--adapter",
            "noop",
            "--backend",
            "http://backend.example:50051",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("loopback"));
}

#[test]
fn backend_userinfo_is_rejected_without_reaching_startup_tracing() {
    let out = Command::new(env!("CARGO_BIN_EXE_exocortex-worker"))
        .args([
            "--adapter",
            "noop",
            "--backend",
            "https://sentinel-user:sentinel-password@backend.example:50051",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("must not contain userinfo"));
    assert!(!stderr.contains("sentinel-user"));
    assert!(!stderr.contains("sentinel-password"));
}

/// Round-3 C2 + PRD R16's second verify clause: the worker BINARY runs
/// `--adapter fixture` end-to-end against a real backend-node process
/// and its batches authenticate (default dev key matches the server's).
#[tokio::test(flavor = "multi_thread")]
async fn fixture_adapter_binary_submits_to_real_backend() {
    use std::process::{Command, Stdio};
    // The node binary belongs to exocortex-server; from this crate only
    // our own CARGO_BIN_EXE_ vars exist. Locate the built node relative
    // to this test binary (target/debug/deps -> target/debug).
    let node_bin = std::env::current_exe()
        .expect("test path")
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("exocortex-node"))
        .filter(|p| p.exists())
        .expect(
            "built exocortex-node binary (cargo build -p exocortex-server --bin exocortex-node)",
        );

    // 1. Boot the real backend (memory storage, default dev cluster
    //    secret [0x42;32] which the worker now falls back to).
    let node_port = {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let gossip_port = {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let policy_dir = tempfile::TempDir::new().unwrap();
    let source_policy = policy_dir.path().join("sources.json");
    std::fs::write(
        &source_policy,
        r#"[{"org_id":"org","source_uri":"fixture://fixture-e2e","producer_id":"fixture-e2e","ceiling":3,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
    )
    .unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&source_policy, std::fs::Permissions::from_mode(0o600)).unwrap();
    let principal_policy = policy_dir.path().join("principals.json");
    std::fs::write(
        &principal_policy,
        r#"[{"bearer_token":"test-only-fixture-bearer-token-00000000","org_id":"org","user_id":"fixture","project_ids":[],"team_ids":[],"max_visibility":3}]"#,
    )
    .unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&principal_policy, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut node = Command::new(&node_bin)
        .args([
            "--mode",
            "backend-node",
            "--storage",
            "memory",
            "--bind",
            &format!("127.0.0.1:{node_port}"),
            "--allow-plaintext-loopback",
            "--gossip-addr",
            &format!("127.0.0.1:{gossip_port}"),
            "--principal-policy",
            principal_policy.to_str().unwrap(),
            "--source-policy",
            source_policy.to_str().unwrap(),
        ])
        .env(
            "EXOCORTEX_CLUSTER_SECRET",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exocortex-node");
    let mut ready = false;
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", node_port)).is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        ready,
        "backend-node accepts connections before worker starts"
    );

    // 2. Fixture file: two memories + one edge.
    let dir = tempfile::TempDir::new().unwrap();
    let fixture = dir.path().join("fixture.json");
    std::fs::write(
        &fixture,
        serde_json::json!({
            "producer_id": "fixture-e2e",
            "seed": "w1",
            "cursor": "w1",
            "memories": [
                {"draft_key": "k1", "memory_type": "General",
                 "title": "fixture row one", "content": "c1", "visibility": 3},
                {"draft_key": "k2", "memory_type": "General",
                 "title": "fixture row two", "content": "c2", "visibility": 3}
            ],
            "relationships": [
                {"from": "k1", "to": "k2", "kind": "RelatedTo"}
            ]
        })
        .to_string(),
    )
    .unwrap();

    // 3. Run the worker BINARY with the fixture adapter.
    let out = Command::new(env!("CARGO_BIN_EXE_exocortex-worker"))
        .args([
            "--adapter",
            "fixture",
            "--backend",
            &format!("http://127.0.0.1:{node_port}"),
            "--fixture",
            fixture.to_str().unwrap(),
            "--cursor",
            dir.path().join("fx.cursor").to_str().unwrap(),
        ])
        .env(
            "EXOCORTEX_HMAC_KEY",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .env(
            "EXOCORTEX_AUTH_TOKEN",
            "test-only-fixture-bearer-token-00000000",
        )
        .output()
        .expect("run exocortex-worker fixture");
    let _ = node.kill();
    let _ = node.wait();
    assert!(
        out.status.success(),
        "worker fixture run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The worker logs the settled window; assert the cursor advanced.
    assert!(
        std::fs::read_to_string(dir.path().join("fx.cursor"))
            .map(|c| c == "w1")
            .unwrap_or(false),
        "durable cursor advanced to w1"
    );
}

/// C2: a malformed HMAC key is a hard error, never a silent fallback.
#[test]
fn malformed_hmac_key_fails_loudly() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-worker"))
        .args([
            "--adapter",
            "fixture",
            "--backend",
            "http://127.0.0.1:1",
            "--fixture",
            "/nonexistent.json",
        ])
        .env("EXOCORTEX_HMAC_KEY", "zzzz")
        .output()
        .unwrap();
    assert!(!out.status.success(), "bad hex must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("64 hex chars"),
        "names the problem (shared decode_hex32): {stderr}"
    );
}

#[test]
fn fixture_adapter_requires_explicit_hmac_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let fixture = dir.path().join("fixture.json");
    std::fs::write(&fixture, r#"{"producer_id":"p","memories":[]}"#).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-worker"))
        .args([
            "--adapter",
            "fixture",
            "--backend",
            "http://127.0.0.1:1",
            "--fixture",
            fixture.to_str().unwrap(),
        ])
        .env_remove("EXOCORTEX_HMAC_KEY")
        .output()
        .unwrap();
    assert!(!out.status.success(), "missing key must fail closed");
    assert!(String::from_utf8_lossy(&out.stderr).contains("EXOCORTEX_HMAC_KEY"));
}
