//! D39 / R13-1+R13-3: the ONLINE end_session ack's consumer contract,
//! pinned through the real tool path — the shipped client binary over
//! gRPC against an in-process backend node. The ack's most
//! agent-actionable fields — rejections[] `code`+`detail`+`correction`,
//! `local_validation_failed`, `unverified[]`, and the `similar_to`
//! suggestions — were pinned at the wire/registry layers only; the
//! playbook's rejection loop ("read the code and detail, fix, resubmit
//! same turn") ran unpinned on the surface agents actually call.
//!
//! Needs the `testing` feature: the node attaches the deterministic
//! FakeEmbedder when fastembed is off (release builds keep the real
//! model; plain non-testing builds keep NO embedder — fail-closed).
//! The `distinct` suggestion value is deliberately NOT asserted: the
//! classifier never emits it — a recorded spec-vs-code gap (round 13),
//! not a pinnable behavior.
#![cfg(feature = "testing")]

use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;

const CLUSTER_KEY: [u8; 32] = [31u8; 32];
const PRODUCER_KEY: [u8; 32] = [32u8; 32];
const TOKEN: &str = "test-only-ack-bearer-token-00000000000000";

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn is_hex32(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
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
    principal.project_ids.push("ack-proj".into());
    let node = exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: "org".into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: "ack-contract-node".into(),
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
                    "session://ack-probe".into(),
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
    input: ChildStdin,
    output: std::sync::mpsc::Receiver<Result<String, String>>,
    stderr_path: String,
}

impl Drop for ClientProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("exo-ack-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn bounded_lines(
    mut stdout: std::process::ChildStdout,
) -> std::sync::mpsc::Receiver<Result<String, String>> {
    let (sender, output) = std::sync::mpsc::channel();
    std::thread::spawn(move || loop {
        use std::io::Read as _;
        let mut line = Vec::new();
        let result = loop {
            let mut byte = [0_u8; 1];
            match stdout.read(&mut byte) {
                Ok(0) if line.is_empty() => break None,
                Ok(0) => break Some(Err("MCP child closed stdout mid-response".into())),
                Ok(_) if byte[0] == b'\n' => {
                    break Some(
                        String::from_utf8(line)
                            .map_err(|_| "MCP child response was not UTF-8".into()),
                    )
                }
                Ok(_) if line.len() == exocortex_wire::limits::MAX_MCP_REQUEST_BYTES => {
                    break Some(Err("MCP child response exceeded 1 MiB".into()));
                }
                Ok(_) => line.push(byte[0]),
                Err(error) => {
                    break Some(Err(format!("MCP child stdout failed: {error}")));
                }
            }
        };
        let Some(result) = result else {
            break;
        };
        if sender.send(result).is_err() {
            break;
        }
    });
    output
}

fn spawn_client(backend: std::net::SocketAddr, tag: &str) -> ClientProcess {
    let sse_key = exocortex_wire::signing::derive_sse_client_key(&CLUSTER_KEY, TOKEN);
    let test_exe = std::env::current_exe().unwrap();
    let debug_dir = test_exe.parent().unwrap().parent().unwrap();
    let mut child = Command::new(debug_dir.join("exocortex-mcp-client"))
        .args([
            "--backend",
            &format!("http://{backend}"),
            "--org",
            "org",
            "--user",
            "grantu",
            "--data-dir",
        ])
        .arg(data_dir(tag))
        .env("EXOCORTEX_HMAC_KEY", hex(&PRODUCER_KEY))
        .env("EXOCORTEX_AUTH_TOKEN", TOKEN)
        .env("EXOCORTEX_SSE_KEY", hex(&sse_key))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the acceptance gate builds exocortex-mcp-client");
    let output = bounded_lines(child.stdout.take().unwrap());
    let input = child.stdin.take().unwrap();
    ClientProcess {
        child,
        input,
        output,
        stderr_path: data_dir_stderr(tag),
    }
}

fn data_dir_stderr(tag: &str) -> String {
    format!("/tmp/exo-ack-{tag}-{}.stderr", std::process::id())
}

fn say(input: &mut ChildStdin, msg: &str) {
    writeln!(input, "{msg}").unwrap();
    input.flush().unwrap();
}

fn read(client: &mut ClientProcess) -> serde_json::Value {
    match client.output.recv_timeout(Duration::from_secs(20)) {
        Ok(Ok(line)) => serde_json::from_str(&line).unwrap(),
        result => {
            let status = client.child.try_wait();
            let log = std::fs::read_to_string(client.stderr_path.clone()).unwrap_or_default();
            let _ = client.child.kill();
            let _ = client.child.wait();
            panic!("MCP child response failed: {result:?}; exit={status:?}; stderr:\n{log}");
        }
    }
}

fn initialize(client: &mut ClientProcess) {
    say(
        &mut client.input,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"ack-contract","version":"1"}}}"#,
    );
    let init = read(client);
    assert!(init["result"].is_object(), "initialize ok: {init}");
    say(
        &mut client.input,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
    );
}

/// One end_session call; returns the parsed ack payload.
fn end_session(
    client: &mut ClientProcess,
    id: i64,
    memories: &str,
    edges: &str,
) -> serde_json::Value {
    say(
        &mut client.input,
        &format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.end_session","arguments":{{"session_id":"ack-probe","project_id":"ack-proj","team_id":null,"memories":{memories},"edges":{edges}}}}}}}"#
        ),
    );
    let response = read(client);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("end_session answered: {response}"));
    serde_json::from_str(text).expect("ack payload is JSON")
}

/// Poll search until the query answers with hits; returns the first hit.
fn search_until(client: &mut ClientProcess, id_base: i64, query: &str) -> serde_json::Value {
    for attempt in 0..40 {
        let id = id_base + attempt;
        say(
            &mut client.input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"{query}"}}}}}}"#
            ),
        );
        let response = read(client);
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
        if let Ok(inner) = serde_json::from_str::<serde_json::Value>(text) {
            if let Some(hits) = inner["memories"].as_array() {
                if !hits.is_empty() {
                    return hits[0].clone();
                }
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("search never served a hit for {query}");
}

const A_TITLE: &str = "Xylophone queue overflow in parser";
const A_BODY: &str = "the xylophone parser queue grows unbounded under backpressure";

/// D39: every field of the online ack an agent acts on, through the
/// real tool path: the clean-batch shape, all three implemented
/// `similar_to` suggestions, the advisory `unverified` list beside the
/// server's structured rejection, and the local rejection loop's
/// code+detail+correction with `local_validation_failed`.
#[tokio::test(flavor = "multi_thread")]
async fn online_end_session_ack_serves_the_documented_contract() {
    let (_node, node_addr) = boot_node().await;
    let mut client = spawn_client(node_addr, "main");
    initialize(&mut client);

    // -- Clean batch: the ack's healthy shape.
    let ack = end_session(
        &mut client,
        2,
        &format!(
            r#"[{{"draft_key":"a","memory_type":"Problem","title":"{A_TITLE}","content":"{A_BODY}","visibility":"org","tags":[]}}]"#
        ),
        "[]",
    );
    assert_eq!(ack["accepted"].as_u64(), Some(1), "committed: {ack}");
    assert_eq!(ack["rejected"].as_u64(), Some(0), "{ack}");
    assert!(
        ack["assigned_lsn"].as_u64().is_some_and(|n| n > 0),
        "the ack carries the assigned LSN: {ack}"
    );
    assert_eq!(ack["local_validation_failed"].as_bool(), Some(false));
    assert_eq!(ack["rejections"].as_array().map(Vec::len), Some(0));
    // `unverified`/`similar_to` skip-serialize when empty: absence IS
    // the empty contract on the wire (EndSessionAck's serde attrs).
    assert!(
        ack.get("unverified")
            .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)),
        "clean batch names nothing unverified: {ack}"
    );
    assert!(
        ack.get("similar_to")
            .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)),
        "clean batch suggests nothing: {ack}"
    );

    let first_hit = search_until(&mut client, 100, "Xylophone");
    assert!(
        first_hit["id"].as_str().is_some_and(is_hex32),
        "the existing row is citable: {first_hit}"
    );

    // -- Exact near-duplicate: `duplicate`, naming the existing row.
    let ack = end_session(
        &mut client,
        3,
        &format!(
            r#"[{{"draft_key":"dup","memory_type":"Problem","title":"{A_TITLE}","content":"{A_BODY}","visibility":"org","tags":[]}}]"#
        ),
        "[]",
    );
    assert_eq!(
        ack["accepted"].as_u64(),
        Some(1),
        "hints are advisory: {ack}"
    );
    let hint = &ack["similar_to"][0];
    assert_eq!(hint["draft_key"].as_str(), Some("dup"), "{ack}");
    assert_eq!(hint["suggestion"].as_str(), Some("duplicate"), "{ack}");
    assert_eq!(hint["existing_title"].as_str(), Some(A_TITLE), "{ack}");
    assert!(
        hint["existing_memory_id"].as_str().is_some_and(is_hex32),
        "the hint names the existing row by citable id: {ack}"
    );

    // -- Cross-type near-duplicate: `contradicts` (the type disagreement
    //    IS the signal, §4.10b).
    let ack = end_session(
        &mut client,
        4,
        &format!(
            r#"[{{"draft_key":"con","memory_type":"Fix","title":"{A_TITLE}","content":"{A_BODY}","visibility":"org","tags":[]}}]"#
        ),
        "[]",
    );
    assert!(
        ack["similar_to"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|h| h["suggestion"] == "contradicts")),
        "cross-type near-duplicate is a refutation: {ack}"
    );

    // -- Same-type update: `replaces` (a one-word delta stays inside the
    //    0.92 hint window while the content hash differs).
    let ack = end_session(
        &mut client,
        5,
        &format!(
            r#"[{{"draft_key":"rep","memory_type":"Problem","title":"{A_TITLE}","content":"{A_BODY} now","visibility":"org","tags":[]}}]"#
        ),
        "[]",
    );
    assert!(
        ack["similar_to"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|h| h["suggestion"] == "replaces")),
        "same-type non-exact update suggests supersession: {ack}"
    );

    // -- Cross-batch edge to a row the client cache cannot see: the
    //    client names what it could not check in `unverified`, and the
    //    SERVER's verdict rides the same ack as a structured rejection —
    //    code + detail + correction either way, never a silent drop.
    //    The target id simply does not exist: hydration means every
    //    VISIBLE row is cached, so a cache-miss target is exactly the
    //    case `unverified` exists for, and the server's rejection comes
    //    back on the same ack — both halves of §3.2 r3's honesty
    //    contract deterministically.
    let ack = end_session(
        &mut client,
        6,
        r#"[{"draft_key":"src","memory_type":"Fix","title":"Cross batch edge source","content":"fixes a missing target by id","visibility":"org","tags":[]}]"#,
        r#"[{"from_draft_key":"src","to_memory_id":"abababababababababababababababab","kind":"Fixes"}]"#,
    );
    assert_eq!(ack["accepted"].as_u64(), Some(0), "nothing commits: {ack}");
    assert_eq!(
        ack["local_validation_failed"].as_bool(),
        Some(false),
        "the LOCAL pass was clean — this rejection is the server speaking: {ack}"
    );
    let unverified = ack["unverified"]
        .as_array()
        .unwrap_or_else(|| panic!("the unknown-target check is named, not skipped: {ack}"));
    assert!(
        unverified
            .iter()
            .any(|u| u["key"].as_str().is_some_and(|k| !k.is_empty())
                && u["reason"]
                    .as_str()
                    .is_some_and(|r| r.contains("server-side"))),
        "unverified names the row and the server-side reason: {ack}"
    );
    let row = &ack["rejections"][0];
    assert!(
        row["code"].as_str().is_some_and(|s| !s.is_empty()),
        "the server's rejection carries its code: {ack}"
    );
    assert!(
        row["detail"]
            .as_str()
            .is_some_and(|d| d.contains("abababababababababababababababab")),
        "the server's detail names the offending target: {ack}"
    );
    assert!(
        row["correction"].as_str().is_some_and(|s| !s.is_empty()),
        "the server rejection carries the fix: {ack}"
    );

    // -- Invalid triple: the local rejection loop the playbook teaches —
    //    code + detail + correction, `local_validation_failed`, nothing
    //    committed, all before any wire work.
    let ack = end_session(
        &mut client,
        7,
        r#"[{"draft_key":"bad","memory_type":"Solution","title":"Never accepted","content":"body","visibility":"org","tags":[]},{"draft_key":"bad-to","memory_type":"Problem","title":"Also never accepted","content":"body","visibility":"org","tags":[]}]"#,
        r#"[{"from_draft_key":"bad","to_draft_key":"bad-to","kind":"Fixes"}]"#,
    );
    assert_eq!(ack["accepted"].as_u64(), Some(0), "nothing commits: {ack}");
    assert_eq!(
        ack["local_validation_failed"].as_bool(),
        Some(true),
        "{ack}"
    );
    let row = &ack["rejections"][0];
    assert_eq!(
        row["code"].as_str(),
        Some("InvalidTypeTriple"),
        "the code matches the server vocabulary: {ack}"
    );
    assert!(
        row["detail"].as_str().is_some_and(|s| !s.is_empty()),
        "playbook: read the detail first — {ack}"
    );
    assert!(
        row["correction"].as_str().is_some_and(|s| !s.is_empty()),
        "P3: deterministic correction rides the rejection — {ack}"
    );
    assert!(
        row["draft_key"].as_str().is_some_and(|s| !s.is_empty()),
        "the rejection names its row: {ack}"
    );
}
