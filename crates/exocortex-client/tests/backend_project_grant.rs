//! D31 follow-up (round 12, iteration 7): the online half of the session
//! project grant. In `--backend` mode `end_session` acked fine, but the
//! batch's project never joined the client session's read scope, so the
//! project-visibility rows the server delivered over SSE were filtered
//! out of every local read — the same blindness D31 fixed for standalone.
//!
//! The in-process backend node registers its principal WITH the project in
//! scope (server-side project membership is PLT1's concern; this test
//! isolates the client half), so the row IS delivered — and without the
//! post-ack grant it is delivered into a cache the session cannot read.

use std::io::{BufRead as _, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;

const CLUSTER_KEY: [u8; 32] = [21u8; 32];
const PRODUCER_KEY: [u8; 32] = [22u8; 32];
const TOKEN: &str = "test-only-grant-bearer-token-0000000000";
const PROJECT: &str = "grant-proj";

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

async fn boot_node() -> (
    exocortex_server::backend::BackendNode<InMemoryStorage>,
    std::net::SocketAddr,
) {
    let ontology = Arc::new(
        Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
            exocortex_pack_study_v1::pack_def(),
        ])
        .expect("composed pack set assembles"),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    // The principal carries the project in scope so the server DELIVERS
    // project-visibility rows over SSE; the blindness under test is the
    // client's own read filter.
    let mut principal = exocortex_ops::operations::ops_vc("org", "grantu", Visibility::Org);
    principal.project_ids.push(PROJECT.into());
    let node = exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: "org".into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: "grant-probe-node".into(),
            cluster_secret: CLUSTER_KEY,
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(TOKEN.into(), principal)
                    .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: Default::default(),
            admin_source_policies: vec![(
                (
                    "org".into(),
                    "session://grant-probe".into(),
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
            "grantu",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .env("EXOCORTEX_HMAC_KEY", hex(&PRODUCER_KEY))
        .env("EXOCORTEX_AUTH_TOKEN", TOKEN)
        .env("EXOCORTEX_SSE_KEY", hex(&sse_key))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn exocortex-mcp-client");
    let stderr = child.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stderr)
            .lines()
            .map_while(Result::ok)
        {
            eprintln!("[client] {line}");
        }
    });
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

/// Next stdout line that is a JSON-RPC response with the given id.
fn response_for(client: &ClientProcess, id: i64, timeout: Duration) -> serde_json::Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for response id {id}"
        );
        let line = client
            .lines
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("stdout closed waiting for response id {id}"))
            .expect("stdout UTF-8");
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
            if value.get("id") == Some(&serde_json::json!(id)) {
                return value;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_project_visibility_write_is_searchable_after_ack() {
    // The node's serving tasks are abort-on-drop; `_node` holds the
    // listener open for the whole test.
    let (_node, node_addr) = boot_node().await;
    let data_dir = std::env::temp_dir().join(format!(
        "exo-grant-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut client = spawn_client(node_addr, &data_dir);

    let mut input = client.child.stdin.take().expect("client stdin");
    let say = |input: &mut std::process::ChildStdin, msg: &str| {
        writeln!(input, "{msg}").unwrap();
        input.flush().unwrap();
    };

    say(
        &mut input,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"grant-probe","version":"1"}}}"#,
    );
    let init = response_for(&client, 1, Duration::from_secs(20));
    assert!(init["result"].is_object(), "initialize ok: {init}");
    say(
        &mut input,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
    );

    say(
        &mut input,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"grant-probe","project_id":"grant-proj","team_id":null,"memories":[{"draft_key":"m1","memory_type":"Fix","title":"backend grant probe row","content":"project-visibility row written over the online path","visibility":"project","tags":[]}],"edges":[]}}}"#,
    );
    let ack = response_for(&client, 2, Duration::from_secs(20));
    let text = ack["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        text.contains(r#""accepted":1"#),
        "the submit committed: {text}"
    );

    // The SSE reseed is asynchronous; poll search until the row is served
    // or the budget expires. Without the post-ack grant the row sits in a
    // cache the session filter cannot read and this NEVER hits.
    for attempt in 0..40 {
        let id = 100 + attempt;
        say(
            &mut input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"backend grant probe row"}}}}}}"#
            ),
        );
        let response = response_for(&client, id, Duration::from_secs(10));
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default();
        if text.contains("backend grant probe row") {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!(
        "project-visibility write over --backend never became searchable: \
         the session read scope never joined the batch's project"
    );
}
