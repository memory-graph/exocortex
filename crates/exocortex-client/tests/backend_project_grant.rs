//! The online half of the D31 project grant: over `--backend`,
//! end_session acked but the batch's project never joined the client's
//! read scope, so server-delivered project-visibility rows were
//! filtered out of every local read. The node's principal carries the
//! projects in scope (server-side membership is PLT1's concern), so the
//! rows ARE delivered — the client's own filter is what's under test.

mod support;

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
const OTHER_PROJECT: &str = "other-proj";

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
    let mut principal = exocortex_ops::operations::ops_vc("org", "grantu", Visibility::Org);
    principal.project_ids.push(PROJECT.into());
    principal.project_ids.push(OTHER_PROJECT.into());
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
    reader: support::BoundedLineReader,
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
    let reader = support::BoundedLineReader::new(child.stdout.take().unwrap());
    ClientProcess { child, reader }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exo-grant-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn initialize(client: &mut ClientProcess, input: &mut std::process::ChildStdin) {
    say(
        input,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"grant-probe","version":"1"}}}"#,
    );
    let init = client.reader.read_json_id(1, Duration::from_secs(20));
    assert!(init["result"].is_object(), "initialize ok: {init}");
    say(
        input,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
    );
}

fn say(input: &mut std::process::ChildStdin, msg: &str) {
    writeln!(input, "{msg}").unwrap();
    input.flush().unwrap();
}

fn tool_text(response: &serde_json::Value) -> &str {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_project_visibility_write_is_searchable_after_ack() {
    // The node's serving tasks are abort-on-drop; `_node` holds the
    // listener open for the whole test.
    let (_node, node_addr) = boot_node().await;
    let mut client = spawn_client(node_addr, &data_dir("ack"));
    let mut input = client.child.stdin.take().expect("client stdin");
    initialize(&mut client, &mut input);

    say(
        &mut input,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"grant-probe","project_id":"grant-proj","team_id":null,"memories":[{"draft_key":"m1","memory_type":"Fix","title":"backend grant probe row","content":"project-visibility row written over the online path","visibility":"project","tags":[]}],"edges":[]}}}"#,
    );
    let ack = client.reader.read_json_id(2, Duration::from_secs(20));
    assert!(
        tool_text(&ack).contains(r#""accepted":1"#),
        "the submit committed: {}",
        tool_text(&ack)
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
        let response = client.reader.read_json_id(id, Duration::from_secs(10));
        if tool_text(&response).contains("backend grant probe row") {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!(
        "project-visibility write over --backend never became searchable: \
         the session read scope never joined the batch's project"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_batch_does_not_grant_its_project() {
    let (_node, node_addr) = boot_node().await;

    // A second client writes a row in OTHER_PROJECT; its own grant makes
    // the row searchable from the writer, proving it is server-side.
    let mut writer = spawn_client(node_addr, &data_dir("writer"));
    let mut writer_input = writer.child.stdin.take().expect("writer stdin");
    initialize(&mut writer, &mut writer_input);
    say(
        &mut writer_input,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"grant-probe","project_id":"other-proj","team_id":null,"memories":[{"draft_key":"w1","memory_type":"Fix","title":"other project row","content":"written by the writer client","visibility":"project","tags":[]}],"edges":[]}}}"#,
    );
    let ack = writer.reader.read_json_id(2, Duration::from_secs(20));
    assert!(
        tool_text(&ack).contains(r#""accepted":1"#),
        "writer submit committed: {}",
        tool_text(&ack)
    );
    for attempt in 0..40 {
        let id = 200 + attempt;
        say(
            &mut writer_input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"other project row"}}}}}}"#
            ),
        );
        let response = writer.reader.read_json_id(id, Duration::from_secs(10));
        if tool_text(&response).contains("other project row") {
            break;
        }
        assert!(
            attempt < 39,
            "writer never saw its own row — the e2e premise is broken"
        );
        std::thread::sleep(Duration::from_millis(250));
    }

    let mut reader = spawn_client(node_addr, &data_dir("reader"));
    let mut reader_input = reader.child.stdin.take().expect("reader stdin");
    initialize(&mut reader, &mut reader_input);
    // Positive control first: the reader's own valid write in grant-proj
    // must become searchable, proving its search, cache, and filter
    // machinery are live (so the other-proj absence below is the grant's,
    // not a broken harness).
    say(
        &mut reader_input,
        r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"grant-probe","project_id":"grant-proj","team_id":null,"memories":[{"draft_key":"r1","memory_type":"Fix","title":"reader control row","content":"granted and searchable","visibility":"project","tags":[]}],"edges":[]}}}"#,
    );
    let control = reader.reader.read_json_id(4, Duration::from_secs(20));
    assert!(
        tool_text(&control).contains(r#""accepted":1"#),
        "reader control submit committed: {}",
        tool_text(&control)
    );
    for attempt in 0..40 {
        let id = 400 + attempt;
        say(
            &mut reader_input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"reader control row"}}}}}}"#
            ),
        );
        let response = reader.reader.read_json_id(id, Duration::from_secs(10));
        if tool_text(&response).contains("reader control row") {
            break;
        }
        assert!(
            attempt < 39,
            "reader never saw its own control row — the e2e premise is broken"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    // The reader booted AFTER the other-project write, so its initial
    // hydration (which completes before initialize is answered) already
    // carries that row into its cache. A REJECTED batch in that project
    // must not widen the reader's read scope: without the accepted>0
    // guard the grant fires on the rejected ack and the row turns
    // searchable out of nowhere.
    say(
        &mut reader_input,
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"grant-probe","project_id":"other-proj","team_id":null,"memories":[{"draft_key":"bad1","memory_type":"NoSuchType","title":"rejected draft","content":"invalid","visibility":"project","tags":[]}],"edges":[]}}}"#,
    );
    let ack = reader.reader.read_json_id(3, Duration::from_secs(20));
    assert!(
        tool_text(&ack).contains(r#""accepted":0"#),
        "the batch must be rejected client-side: {}",
        tool_text(&ack)
    );
    for attempt in 0..12 {
        let id = 300 + attempt;
        say(
            &mut reader_input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"other project row"}}}}}}"#
            ),
        );
        let response = reader.reader.read_json_id(id, Duration::from_secs(10));
        assert!(
            !tool_text(&response).contains("other project row"),
            "a rejected batch granted its project: the row became searchable"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}
