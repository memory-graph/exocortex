//! M3 acceptance: the `exocortex-mcp-client` binary runs, speaks MCP over
//! stdio, and returns synthetic data for `search_memories`.

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

#[test]
fn serves_mcp_over_stdio_with_synthetic_search_results() {
    let mut c = Client::spawn_with(|cmd| {
        cmd.args(["--org", "smoke", "--user", "tester"]);
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
    assert!(!memories.is_empty(), "synthetic data returned");
    assert!(
        memories[0]["title"].as_str().unwrap().contains("auth"),
        "ranked hit matches the query"
    );
    assert!(
        payload["snapshot_version"]["backend_lsn"].is_u64(),
        "R-M7 version stamp present"
    );
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
