//! D55 fail-without-it: a long-lived MCP client session survives its
//! standalone owner being REPLACED (killed owner, successor boot with a
//! new endpoint and fresh credentials). Pre-fix the second end_session
//! failed `ingest: ... tcp connect error` and reads stayed frozen on
//! the dead owner's snapshot — reproduced live 2026-10-13 with the
//! shipped 0.5.1 client (three frozen session clients on one machine).
//!
//! The successor owner publishes `attach.json` exactly as the real
//! wrapper's node does (D44-S2); the client re-resolves it from the
//! wrapper-stamped `EXOCORTEX_STANDALONE_DATA_HOME` on both the submit
//! path (synchronous, retry once) and the SSE reconnect seam.

mod support;

use std::io::{BufRead as _, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::InMemoryStorage;

const ORG: &str = "org";
const USER: &str = "d55u";

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// One owner's full credential set — distinct per owner so the test
/// PROVES the client adopts the successor's credentials, not just its
/// endpoint (all three ride the attach record and all three change).
struct Owner {
    cluster_key: [u8; 32],
    producer_key: [u8; 32],
    token: String,
}

fn owner(seed: u8) -> Owner {
    Owner {
        cluster_key: [seed; 32],
        producer_key: [seed + 100; 32],
        token: hex(&[seed + 200; 32]),
    }
}

async fn boot_node(
    owner: &Owner,
) -> (
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
    let principal = exocortex_ops::operations::ops_vc(ORG, USER, Visibility::Org);
    let node = exocortex_server::backend::run_backend_node(
        storage,
        ontology,
        exocortex_server::backend::BackendNodeArgs {
            org: ORG.into(),
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: format!("d55-node-{}", owner.cluster_key[0]),
            cluster_secret: owner.cluster_key,
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(
                    owner.token.clone(),
                    principal,
                )
                .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: Default::default(),
            admin_source_policies: vec![(
                (
                    ORG.into(),
                    "session://d55-session".into(),
                    "session-wrapup".into(),
                ),
                exocortex_ingest::service::AdminSourcePolicy {
                    ceiling: Visibility::Org,
                    kind: exocortex_kernel::ProducerKind::CodingAgent,
                    signing_key: owner.producer_key,
                },
            )],
        },
    )
    .await
    .expect("boot in-process backend node");
    let addr = node.local_addr;
    (node, addr)
}

/// An owner node on its OWN runtime. Dropping this handle aborts every
/// task on that runtime and RSTs every established connection — the
/// faithful in-process equivalent of SIGKILLing a real owner. (Dropping
/// just the `BackendNode` closes the listener but leaves detached
/// per-connection tasks alive on a shared runtime, which kept acking
/// submits and heartbeating SSE long after the "kill" — the exact
/// unfaithfulness this harness fix removes.)
struct OwnedNode {
    _node: exocortex_server::backend::BackendNode<InMemoryStorage>,
    _rt: tokio::runtime::Runtime,
    addr: std::net::SocketAddr,
}

fn boot_owned_node(owner: &Owner) -> OwnedNode {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("owner runtime");
    let (node, addr) = rt.block_on(boot_node(owner));
    OwnedNode {
        _node: node,
        _rt: rt,
        addr,
    }
}

/// Publish the attach record the way the owner node's serve loop does
/// (its 0600 atomic write lives in the server binary's supervisor
/// module; the bytes are the same wire type).
fn publish_attach(owner: &Owner, addr: std::net::SocketAddr, data_home: &std::path::Path) {
    let sse_key = exocortex_wire::signing::derive_sse_client_key(&owner.cluster_key, &owner.token);
    let raw = serde_json::to_string(&exocortex_wire::attach::AttachInfo {
        backend: format!("http://{addr}"),
        org: ORG.into(),
        auth_token: owner.token.clone(),
        hmac_key: hex(&owner.producer_key),
        sse_key: hex(&sse_key),
    })
    .unwrap();
    std::fs::write(data_home.join("attach.json"), raw).unwrap();
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

fn spawn_client(
    owner: &Owner,
    addr: std::net::SocketAddr,
    dirs: &std::path::Path,
) -> ClientProcess {
    let sse_key = exocortex_wire::signing::derive_sse_client_key(&owner.cluster_key, &owner.token);
    let mut command = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
    command
        .args([
            "--backend",
            &format!("http://{addr}"),
            "--org",
            ORG,
            "--user",
            USER,
            "--data-dir",
            dirs.join("wal").to_str().unwrap(),
        ])
        .env("EXOCORTEX_HMAC_KEY", hex(&owner.producer_key))
        .env("EXOCORTEX_AUTH_TOKEN", &owner.token)
        .env("EXOCORTEX_SSE_KEY", hex(&sse_key))
        // The wrapper stamps both of these for every standalone session.
        .env("EXOCORTEX_STANDALONE", "1")
        .env("EXOCORTEX_STANDALONE_DATA_HOME", dirs)
        .env("RUST_LOG", "exocortex_client=debug,exocortex_wire=warn");
    let mut child = command
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

fn say(input: &mut std::process::ChildStdin, msg: &str) {
    writeln!(input, "{msg}").unwrap();
    input.flush().unwrap();
}

fn tool_text(response: &serde_json::Value) -> &str {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
}

fn initialize(client: &mut ClientProcess, input: &mut std::process::ChildStdin) {
    say(
        input,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"d55-probe","version":"1"}}}"#,
    );
    let init = client.reader.read_json_id(1, Duration::from_secs(20));
    assert!(init["result"].is_object(), "initialize ok: {init}");
    say(
        input,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
    );
}

/// One end_session call through the long-lived client; returns the ack
/// text (tool errors carry their message in the same text field).
fn end_session(
    input: &mut std::process::ChildStdin,
    reader: &support::BoundedLineReader,
    id: i64,
    title: &str,
) -> String {
    say(
        input,
        &format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.end_session","arguments":{{"session_id":"d55-session","project_id":"d55-proj","memories":[{{"draft_key":"m1","memory_type":"Fix","title":"{title}","content":"D55 owner-replacement row {title}","visibility":"org","tags":[]}}],"edges":[]}}}}}}"#
        ),
    );
    let response = reader.read_json_id(id, Duration::from_secs(60));
    tool_text(&response).to_owned()
}

fn port_is_dead(addr: std::net::SocketAddr) -> bool {
    std::net::TcpStream::connect(addr).is_err()
}

#[test]
fn long_lived_client_survives_owner_replacement() {
    let dirs = std::env::temp_dir().join(format!(
        "exo-d55-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(dirs.join("wal")).unwrap();

    // Owner A boots and publishes its record; the client session starts
    // against it and lands one write (the healthy baseline).
    let owner_a = owner(11);
    let node_a = boot_owned_node(&owner_a);
    publish_attach(&owner_a, node_a.addr, &dirs);
    let mut client = spawn_client(&owner_a, node_a.addr, &dirs);
    let mut input = client.child.stdin.take().expect("client stdin");
    initialize(&mut client, &mut input);
    let ack_a = end_session(&mut input, &client.reader, 2, "before-replacement");
    assert!(
        ack_a.contains("\"accepted\":1"),
        "baseline write on owner A must ack: {ack_a}"
    );

    // Owner A dies without cleanup (the SIGILL class) and is REPLACED by
    // a successor with a different endpoint and entirely fresh
    // credentials — exactly what a new wrapper boot publishes. Dropping
    // the OwnedNode aborts its whole runtime: listener AND established
    // connections die, the way a real process kill does.
    let addr_a = node_a.addr;
    drop(node_a);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !port_is_dead(addr_a) {
        assert!(std::time::Instant::now() < deadline, "owner A never died");
        std::thread::sleep(Duration::from_millis(100));
    }
    let owner_b = owner(22);
    let node_b = boot_owned_node(&owner_b);
    publish_attach(&owner_b, node_b.addr, &dirs);
    let addr_b = node_b.addr;

    // D55, submit path: the SAME session's next end_session must land on
    // the successor — pre-fix this failed `ingest: ... tcp connect error`.
    let ack_b = end_session(&mut input, &client.reader, 3, "after-replacement");
    assert!(
        ack_b.contains("\"accepted\":1"),
        "the replaced owner's write must land on the successor: {ack_b}"
    );
    let lsn = ack_b
        .split("\"assigned_lsn\":")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .expect("ack carries an LSN");
    assert!(lsn > 0, "the successor committed the batch: {ack_b}");

    // Disambiguator kept as a real assertion: the successor itself must
    // serve the row to a DIRECT fresh client — proving the batch landed
    // on B's graph, not a zombie of A.
    {
        let mut probe = spawn_client(&owner_b, addr_b, &dirs.join("probe"));
        let mut probe_input = probe.child.stdin.take().expect("probe stdin");
        initialize(&mut probe, &mut probe_input);
        let mut probe_found = false;
        for attempt in 0..20 {
            let id = 100 + attempt;
            say(
                &mut probe_input,
                &format!(
                    r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"after-replacement","limit":3}}}}}}"#
                ),
            );
            let response = probe.reader.read_json_id(id, Duration::from_secs(10));
            if tool_text(&response).contains("after-replacement") {
                probe_found = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        assert!(
            probe_found,
            "the successor's own graph must serve the batch a direct client"
        );
    }

    // D55, read path: the SSE loop adopts the successor's endpoint and
    // credentials between reconnects; the successor's row becomes
    // searchable through the SAME session (pre-fix: frozen snapshot).
    let mut adopted = false;
    for attempt in 0..160 {
        let id = 10 + attempt;
        say(
            &mut input,
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/call","params":{{"name":"exocortex.search_memories","arguments":{{"query":"after-replacement","limit":3}}}}}}"#
            ),
        );
        let response = client.reader.read_json_id(id, Duration::from_secs(10));
        if tool_text(&response).contains("after-replacement") {
            adopted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        adopted,
        "the SSE loop must re-resolve the successor and serve its rows"
    );

    drop(client);
    let _ = std::fs::remove_dir_all(&dirs);
}
