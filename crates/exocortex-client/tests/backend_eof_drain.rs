//! D27 regression (bug-prd-standalone-submit-hang): the deterministic,
//! bundled-runtime-free proof of the standalone submit path.
//!
//! rmcp's serve loop treats stdin EOF as immediate shutdown and drops
//! in-flight tool-call tasks, so an `end_session` submit still on the
//! wire lost its response — the harness saw a silent hang while the
//! node stayed idle. This test reproduces the exact race without any
//! standalone runtime: an in-process backend node (in-memory storage)
//! sits behind a delaying TCP proxy, the real client binary connects
//! through it, and stdin closes the instant the submit is dispatched.
//! The submit CANNOT complete before EOF arrives (every backend byte is
//! held 400ms), so without the EOF-draining stdio reader the client
//! exits and the response is dropped; with it, the client drains the
//! call, answers `accepted: 1`, and only then exits.

use std::io::{BufRead as _, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const CLUSTER_KEY: [u8; 32] = [11u8; 32];
const PRODUCER_KEY: [u8; 32] = [12u8; 32];
const TOKEN: &str = "test-only-eofdrain-bearer-token-00";
const BACKEND_DELAY: Duration = Duration::from_millis(400);

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Accept loop copying both directions; backend-to-client bytes are held
/// BACKEND_DELAY per chunk so responses cannot beat stdin EOF.
async fn delaying_proxy(node: std::net::SocketAddr) -> std::io::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let Ok(backend) = tokio::net::TcpStream::connect(node).await else {
                continue;
            };
            let (cr, mut cw) = client.into_split();
            let (mut br, bw) = backend.into_split();
            // client -> backend: immediate
            tokio::spawn(async move {
                let mut cr = cr;
                let mut bw = bw;
                let _ = tokio::io::copy(&mut cr, &mut bw).await;
            });
            // backend -> client: delayed
            tokio::spawn(async move {
                let mut chunk = [0u8; 8192];
                loop {
                    match br.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            tokio::time::sleep(BACKEND_DELAY).await;
                            if cw.write_all(&chunk[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    Ok(proxy_addr)
}

async fn boot_node() -> (
    exocortex_server::backend::BackendNode<InMemoryStorage>,
    std::net::SocketAddr,
) {
    let ontology = Arc::new(
        Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
        ])
        .expect("composed pack set assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let node = exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: "org".into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: "eof-drain-node".into(),
            cluster_secret: CLUSTER_KEY,
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(
                    TOKEN.into(),
                    exocortex_ops::operations::ops_vc("org", "eofu", Visibility::Org),
                )
                .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: Default::default(),
            admin_source_policies: vec![(
                (
                    "org".into(),
                    "session://eof-drain".into(),
                    "session-wrapup".into(),
                ),
                exocortex_ingest::service::AdminSourcePolicy {
                    ceiling: Visibility::Org,
                    kind: exocortex_kernel::ProducerKind::CodingAgent,
                    signing_key: PRODUCER_KEY,
                },
            )],
        },
    )
    .await
    .expect("boot in-process backend node");
    let addr = node.local_addr;
    (node, addr)
}

struct ClientProcess {
    child: Child,
    lines: std::sync::mpsc::Receiver<Result<String, String>>,
}

impl Drop for ClientProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_client(backend: std::net::SocketAddr, data_dir: &std::path::Path) -> ClientProcess {
    let sse_key = exocortex_wire::signing::derive_sse_client_key(&CLUSTER_KEY, TOKEN);
    let mut child = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
        .args([
            "--backend",
            &format!("http://{backend}"),
            "--org",
            "org",
            "--user",
            "eofu",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .env("EXOCORTEX_HMAC_KEY", hex(&PRODUCER_KEY))
        .env("EXOCORTEX_AUTH_TOKEN", TOKEN)
        .env("EXOCORTEX_SSE_KEY", hex(&sse_key))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exocortex-mcp-client");
    let stdout = child.stdout.take().unwrap();
    let (tx, lines) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let line = line.map_err(|e| e.to_string());
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    ClientProcess { child, lines }
}

#[tokio::test(flavor = "multi_thread")]
async fn stdin_eof_drains_an_in_flight_submit_instead_of_dropping_it() {
    // The node's serving tasks are abort-on-drop; `_node` holds the
    // listener open for the whole test.
    let (_node, node_addr) = boot_node().await;
    let proxy_addr = delaying_proxy(node_addr)
        .await
        .expect("bind delaying proxy");
    let data_dir = tempfile_dir();
    let mut client = spawn_client(proxy_addr, &data_dir);

    let mut input = client.child.stdin.take().expect("client stdin");
    let say = |input: &mut std::process::ChildStdin, msg: &str| {
        writeln!(input, "{msg}").unwrap();
        input.flush().unwrap();
    };

    say(
        &mut input,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"eof-drain","version":"1"}}}"#,
    );
    // Hydration streams through the delayed proxy; give it room.
    let init = client
        .lines
        .recv_timeout(Duration::from_secs(20))
        .expect("initialize answered")
        .expect("stdout UTF-8");
    let init: serde_json::Value = serde_json::from_str(&init).unwrap();
    assert!(init["result"].is_object(), "initialize ok: {init}");

    say(
        &mut input,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
    );
    say(
        &mut input,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"eof-drain","project_id":"eof-drain-project","team_id":null,"memories":[{"draft_key":"m1","memory_type":"General","title":"eof drain marker","content":"survives stdin EOF","visibility":"org","tags":[]}],"edges":[]}}}"#,
    );
    // The race: EOF lands immediately; the submit response is still held
    // by the proxy. Without the drain the client exits here and drops it.
    drop(input);

    let response = client
        .lines
        .recv_timeout(Duration::from_secs(20))
        .expect("the in-flight submit MUST be answered before exit")
        .expect("stdout UTF-8");
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["id"], 2, "response is for the submit: {response}");
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    assert!(
        text.contains(r#""accepted":1"#),
        "the submit committed and the ack reached the harness: {text}"
    );
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exo-eof-drain-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
