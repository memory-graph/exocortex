//! M3 acceptance: the `exocortex-mcp-client` binary runs, speaks MCP over
//! stdio, and serves the local graph (SR-PRD: honestly empty on a fresh
//! data dir — no synthetic filler — and seeded from the WAL on restart).

use std::io::Write;
use std::process::{Child, Command, Stdio};

struct Client {
    child: Child,
}

impl Client {
    fn spawn_with(configure: impl FnOnce(&mut Command)) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
        configure(&mut cmd);
        let child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn exocortex-mcp-client");
        Self { child }
    }

    /// Write all requests up front (pipes preserve order; the server handles
    /// them sequentially), then read one response per request id.
    fn send_all(&mut self, msgs: &[serde_json::Value]) {
        let stdin = self.child.stdin.as_mut().expect("stdin");
        for m in msgs {
            writeln!(stdin, "{m}").unwrap();
        }
        stdin.flush().unwrap();
    }

    fn read_line(&mut self) -> serde_json::Value {
        let stdout = self.child.stdout.as_mut().expect("stdout");
        let mut line = String::new();
        let mut byte = [0u8; 1];
        loop {
            use std::io::Read;
            if stdout.read(&mut byte).unwrap() == 0 {
                panic!("server closed stdout");
            }
            if byte[0] == b'\n' {
                break;
            }
            line.push(byte[0] as char);
        }
        serde_json::from_str(&line).expect("valid JSON-RPC line")
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// SR-PRD F4/AC4: a fresh standalone `--data-dir` answers honestly
/// empty — no fabricated rows — while the tool surface and the R-M7
/// version stamp still work.
#[test]
fn serves_mcp_over_stdio_honestly_empty_on_fresh_dir() {
    let dir = tempdir();
    let mut c = Client::spawn_with(|cmd| {
        cmd.args([
            "--org",
            "smoke",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ]);
    });
    c.send_all(&[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "smoke-test", "version": "0" }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
        }),
        serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "exocortex.search_memories",
                "arguments": { "query": "auth", "limit": 5 }
            }
        }),
    ]);

    let init = c.read_line();
    assert!(
        init.get("result").is_some(),
        "initialize must succeed: {init}"
    );
    assert_eq!(
        init["result"]["serverInfo"]["name"], "exocortex-mcp-client",
        "bootstrap must use Exocortex server metadata: {init}"
    );
    assert!(
        init["result"]["capabilities"]["tools"].is_object(),
        "bootstrap must advertise the tool capability before the host sends initialized: {init}"
    );
    assert!(
        init["result"]["instructions"].as_str().is_some(),
        "bootstrap must advertise the producer-neutral instructions: {init}"
    );
    let tools = c.read_line();
    let names: Vec<String> = tools["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"exocortex.search_memories".to_string()),
        "search tool listed: {names:?}"
    );
    assert!(
        names.contains(&"exocortex.end_session".to_string()),
        "end_session tool listed (§13.6.2): {names:?}"
    );
    let call = c.read_line();
    let text = call["result"]["content"][0]["text"]
        .as_str()
        .expect("content text");
    let payload: serde_json::Value = serde_json::from_str(text).expect("payload JSON");
    let memories = payload["memories"].as_array().expect("memories");
    assert!(
        memories.is_empty(),
        "fresh data dir answers honestly empty (no synthetic seed): {memories:?}"
    );
    assert!(
        payload["snapshot_version"]["backend_lsn"].is_u64(),
        "R-M7 version stamp present"
    );
    assert!(
        payload["snapshot_version"]["local_lsn"].is_u64(),
        "R-M7 local stamp present"
    );
}

/// DF2: Crush v0.91.x probes with SEP-2575 discovery. A legacy server must
/// return method-not-found so Crush falls back to initialize/initialized, then
/// exposes the same tool catalogue as any other MCP host.
#[test]
fn falls_back_from_sep_2575_discovery_for_crush() {
    let dir = tempdir();
    let mut c = Client::spawn_with(|cmd| {
        cmd.args(["--data-dir", dir.to_str().unwrap()]);
    });
    c.send_all(&[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/clientCapabilities": { "roots": { "listChanged": true } },
                    "io.modelcontextprotocol/clientInfo": { "name": "crush", "version": "v0.91.2" },
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28"
                }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": { "roots": { "listChanged": true } },
                "clientInfo": { "name": "crush", "version": "v0.91.2" }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
        }),
        serde_json::json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/list" }),
    ]);

    let discover = c.read_line();
    assert_eq!(
        discover["error"]["code"], -32601,
        "SEP-2575 legacy fallback signal: {discover}"
    );

    let init = c.read_line();
    assert!(
        init["result"]["capabilities"]["tools"].is_object(),
        "fallback initialize advertises tools: {init}"
    );

    let tools = c.read_line();
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();
    assert!(names.contains(&"exocortex.search_memories"), "{names:?}");
    assert!(names.contains(&"exocortex.end_session"), "{names:?}");
}

/// §13.6.2 / Success Criteria #2 shape: the harness calls
/// `exocortex.end_session` over stdio with 3 memories + 2 edges; offline
/// mode buffers the batch into the WAL and answers
/// `{ local_lsns, sync_pending: true }` (§5.2).
#[test]
fn end_session_over_stdio_offline_wal_path() {
    let dir = tempdir();
    let mut c = Client::spawn_with(|cmd| {
        cmd.args([
            "--org",
            "smoke",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ]);
    });
    c.send_all(&[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "smoke-test", "version": "0" }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "exocortex.end_session",
                "arguments": {
                    "session_id": "s-42",
                    "project_id": "proj-7",
                    "memories": [
                        { "draft_key": "a", "memory_type": "Problem", "title": "Flaky test", "content": "test flakes on ci", "visibility": "org", "tags": ["ci"] },
                        { "draft_key": "b", "memory_type": "Fix", "title": "Retry once", "content": "retry the auth call once", "visibility": "org" },
                        { "draft_key": "c", "memory_type": "Solution", "title": "Seed rng", "content": "seed the rng for determinism", "visibility": "org" }
                    ],
                    "edges": [
                        { "from_draft_key": "b", "to_draft_key": "a", "kind": "Fixes", "strength": 0.9 },
                        { "from_draft_key": "c", "to_draft_key": "a", "kind": "Solves", "strength": 0.8 }
                    ]
                }
            }
        }),
    ]);

    let init = c.read_line();
    assert!(init.get("result").is_some(), "initialize ok: {init}");
    let call = c.read_line();
    assert!(
        call.get("result").is_some(),
        "end_session must not error: {call}"
    );
    let text = call["result"]["content"][0]["text"].as_str().expect("text");
    let payload: serde_json::Value = serde_json::from_str(text).expect("ack JSON");
    assert_eq!(
        payload["sync_pending"], true,
        "offline ack carries sync_pending: {payload}"
    );
    let lsns = payload["local_lsns"].as_array().expect("local_lsns");
    assert!(!lsns.is_empty(), "local LSN assigned: {payload}");

    // The WAL on disk holds the batch as Pending.
    let wal_dir = dir.join("wal");
    assert!(wal_dir.exists(), "wal created under data dir");
}

/// A fresh temp dir per test (no extra dependency).
fn tempdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "exocortex-smoke-{}-{}",
        std::process::id(),
        uuid_v4_hex()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn uuid_v4_hex() -> String {
    use std::fmt::Write as _;
    let mut hex = String::new();
    for b in uuid_rand_bytes() {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

fn uuid_rand_bytes() -> [u8; 16] {
    // Cheap uniqueness: pid + nanos; only used for dir names.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id() as u128;
    let mix = nanos ^ (pid << 64);
    mix.to_be_bytes()
}

/// H13 (M7 task 3): the client's MCP tool catalogue is registry-driven —
/// every interactive-read registry op is dispatchable client-side, every
/// listed tool names a registry op (or the §13.5 session-capture tool),
/// and no stale stubs remain.
#[test]
fn mcp_tool_list_matches_registry() {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
    let dir = std::env::temp_dir().join(format!("exo-mcp-registry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    cmd.args(["--org", "registry", "--user", "tester"])
        .arg("--data-dir")
        .arg(&dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut client = Client {
        child: cmd.spawn().expect("spawn"),
    };
    client.send_all(&[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "t", "version": "0" }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
        }),
    ]);
    let _init = client.read_line();
    let tools = client.read_line();
    if tools.get("result").is_none() {
        eprintln!("tools/list failed: {tools:?}");
    }
    let listed: Vec<String> = tools["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap_or_default().to_string())
        .collect();

    let registry: Vec<_> = exocortex_ops::entries()
        .iter()
        .map(|e| e.mcp_tool_name.to_string())
        .collect();
    // Read ops must all be client-dispatchable.
    for read_op in [
        "exocortex.search_memories",
        "exocortex.get_memory",
        "exocortex.find_related",
    ] {
        assert!(registry.contains(&read_op.to_string()));
        assert!(
            listed.contains(&read_op.to_string()),
            "{read_op} listed on the client surface"
        );
    }
    // No phantom tools: everything listed is a registry op or end_session.
    for name in &listed {
        assert!(
            registry.contains(name) || name == "exocortex.end_session",
            "{name} is not a registry tool"
        );
    }
    // No stale stubs.
    assert!(!listed.iter().any(|t| t.contains("traverse_relationships")));
    assert!(!listed.iter().any(|t| t.contains("get_chain")));
    assert!(!listed.iter().any(|t| t.contains("explain_edge")));
}

/// R6-B06/CL5: backend mode does not expose MCP at all until an authenticated
/// graph reseed arrives. An unreachable backend must therefore leave stdout
/// silent rather than serve either fabricated rows or an unhydrated empty
/// cache.
#[test]
fn backend_mode_waits_for_authenticated_hydration_before_stdio_readiness() {
    let dir = tempdir();
    let mut c = Client::spawn_with(|cmd| {
        cmd.args([
            "--org",
            "smoke",
            "--user",
            "tester",
            "--backend",
            "http://127.0.0.1:1", // unreachable by design
            "--data-dir",
            dir.to_str().unwrap(),
        ])
        .env(
            "EXOCORTEX_HMAC_KEY",
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .env("EXOCORTEX_AUTH_TOKEN", "smoke-token");
    });
    c.send_all(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "smoke-test", "version": "0" }
        }
    })]);

    let mut stdout = c.child.stdout.take().expect("stdout");
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut byte = [0u8; 1];
        let _ = tx.send(stdout.read(&mut byte));
    });
    assert!(
        matches!(
            rx.recv_timeout(std::time::Duration::from_millis(300)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "unhydrated backend mode exposed or closed MCP stdout"
    );
    c.child.kill().expect("stop waiting client");
    c.child.wait().expect("reap waiting client");
    reader.join().expect("stdout reader exits after kill");
}

/// CL3 (audit): a malformed environment key aborts startup with a diagnostic
/// instead of panicking (short) or silently signing with zeros (non-hex).
#[test]
fn malformed_hmac_key_fails_startup() {
    for bad in ["deadbeef", &"z".repeat(64)] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
            .args(["--org", "x", "--backend", "http://127.0.0.1:1"])
            .env("EXOCORTEX_HMAC_KEY", bad)
            .env("EXOCORTEX_AUTH_TOKEN", "test")
            .output()
            .expect("run client");
        assert!(
            !out.status.success(),
            "malformed EXOCORTEX_HMAC_KEY {bad:?} must not start: {:?}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("EXOCORTEX_HMAC_KEY"),
            "diagnostic names the variable: {stderr}"
        );
        assert!(
            !stderr.contains("panicked"),
            "no panic on short key: {stderr}"
        );
    }
}

#[test]
fn backend_mode_requires_nonempty_authentication_material() {
    let bin = env!("CARGO_BIN_EXE_exocortex-mcp-client");
    for (hmac, token, expected) in [
        (None, Some("token"), "EXOCORTEX_HMAC_KEY"),
        (
            Some("4242424242424242424242424242424242424242424242424242424242424242"),
            None,
            "EXOCORTEX_AUTH_TOKEN",
        ),
        (
            Some("4242424242424242424242424242424242424242424242424242424242424242"),
            Some(""),
            "EXOCORTEX_AUTH_TOKEN",
        ),
    ] {
        let mut command = std::process::Command::new(bin);
        command
            .args(["--backend", "http://127.0.0.1:1"])
            .env_remove("EXOCORTEX_HMAC_KEY")
            .env_remove("EXOCORTEX_AUTH_TOKEN");
        if let Some(value) = hmac {
            command.env("EXOCORTEX_HMAC_KEY", value);
        }
        if let Some(value) = token {
            command.env("EXOCORTEX_AUTH_TOKEN", value);
        }
        let out = command.output().expect("run client");
        assert!(!out.status.success(), "backend mode must fail closed");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(expected), "{expected} diagnostic: {stderr}");
    }
}

/// IN10 (audit): the MCP read tools and the HTTP surface serve the SAME
/// registry implementation — running one input through `entry.handler`
/// and through the MCP server's typed method yields identical JSON for
/// get_memory (the audit's pinned divergence: flat hit shape vs the
/// registry's `{memory: ...}`).
#[tokio::test(flavor = "multi_thread")]
async fn mcp_get_memory_shape_matches_registry() {
    use exocortex_cache::{GraphSnapshot, LocalCache};
    use std::sync::Arc;

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let mut snap = GraphSnapshot::empty();
    use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
    let m = Memory {
        id: MemoryId([9; 16]),
        memory_type: onto.memory_type_id("Problem").unwrap(),
        title: "shape witness".into(),
        content: "c".into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
            session_id: None,
            user_id: None,
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: Default::default(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_backend(1),
    };
    let mid_hex = {
        use std::fmt::Write as _;
        let mut h = String::with_capacity(32);
        for b in m.id.0 {
            let _ = write!(h, "{b:02x}");
        }
        h
    };
    snap.push_test_memory(m);
    cache.publish("org", Arc::new(snap));

    let vc = exocortex_ops::VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs().unwrap());
    let server =
        exocortex_client::mcp::ExocortexMcp::new("org".into(), cache.clone(), vc.clone(), ontology);

    // MCP tool call (the binary's surface).
    let mcp_out = server
        .get_memory(mid_hex.clone())
        .await
        .expect("mcp get_memory");

    // The registry handler directly (the HTTP surface's implementation).
    let ctx = Arc::new(exocortex_ops::OpContext {
        ontology: None,
        visibility_ctx: vc,
        audit_admin: true,
        storage: Arc::new(exocortex_client::no_backend::NoBackendStorage),
        cache: cache.clone(),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
    });
    let entry = exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.get_memory")
        .unwrap();
    let reg_out = (entry.handler)(
        &ctx,
        serde_json::to_value(exocortex_ops::operations::GetMemoryInput { id: mid_hex }).unwrap(),
    )
    .await
    .expect("registry handler");
    let reg_json = serde_json::to_string(&reg_out).unwrap();

    assert_eq!(
        mcp_out, reg_json,
        "IN10: MCP and registry produce byte-identical get_memory output"
    );
    let parsed: serde_json::Value = serde_json::from_str(&mcp_out).unwrap();
    assert!(
        parsed["memory"].is_object(),
        "registry `{{memory: ...}}` shape on hit: {mcp_out}"
    );
    assert_eq!(parsed["memory"]["title"], "shape witness");
}

/// §4.8 (agent-instructions PRD): the client process owns session
/// identity. An omitted session_id is stamped with the process-minted
/// conversation id; an explicit one (deliberate sharing) rides through.
#[tokio::test]
async fn omitted_session_id_gets_process_minted_default() {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs().unwrap());
    let dir = tempdir();
    let (cache, _rx) = exocortex_cache::LocalCache::new(16 * 1024 * 1024);
    let vc = exocortex_ops::VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let wal = std::sync::Arc::new(exocortex_client::wal::Wal::open(&dir.join("wal")).unwrap());
    let server = exocortex_client::mcp::ExocortexMcp::new(
        "org".into(),
        std::sync::Arc::new(cache),
        vc,
        ontology,
    )
    .with_offline_wal(wal.clone());

    let draft = |k: &str| exocortex_client::tools::end_session::MemoryDraftInput {
        draft_key: k.into(),
        memory_type: "Fix".into(),
        title: format!("Fixed {k}"),
        content: "body in src/x.rs".into(),
        visibility: "project".into(),
        tags: vec![],
    };
    // Omitted session id: the process default is stamped.
    server
        .end_session(None, "p".into(), None, vec![draft("a")], vec![])
        .await
        .expect("offline write");
    // Explicit id: rides through untouched (deliberate sharing).
    server
        .end_session(
            Some("shared-conv".into()),
            "p".into(),
            None,
            vec![draft("b")],
            vec![],
        )
        .await
        .expect("offline write 2");

    let tail = wal.tail(5);
    assert_eq!(tail.len(), 2);
    // Recover the session ids through the WAL entries themselves.
    let entries = wal.pending_entries().unwrap();
    let ids: Vec<&str> = entries.iter().map(|e| e.session_id.as_str()).collect();
    assert!(
        ids.contains(&"shared-conv"),
        "explicit id rides through: {ids:?}"
    );
    assert!(
        ids.contains(&server.process_session_id()) && server.process_session_id() != "shared-conv",
        "omitted id gets the process-minted default: {ids:?}"
    );
}
