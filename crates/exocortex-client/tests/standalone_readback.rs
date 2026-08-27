//! SR-PRD acceptance (docs/bug-prd-standalone-readback.md): the
//! zero-setup mode's write→read loop, driven end-to-end against the
//! real `exocortex-mcp-client` binary (boot, WAL, cache, stdio MCP).
//!
//! AC1 in-session read-back; AC2 cross-restart; AC3 edges + grouping;
//! AC4 honest-empty; AC6 all-states seeding; AC7 dangling edges.
//! (AC5 — backend mode unchanged — is `e2e_chain` + the CL5 stdio test.)

use std::io::Write;
use std::process::{Child, Command, Stdio};

struct Client {
    child: Child,
}

impl Client {
    fn spawn(dir: &std::path::Path) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
        cmd.args([
            "--org",
            "readback",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ]);
        let child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn exocortex-mcp-client");
        Self { child }
    }

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

    /// initialize + notifications/initialized prelude.
    fn init_msgs() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": { "name": "readback-test", "version": "0" }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
            }),
        ]
    }

    fn search(query: &str) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0", "id": 90, "method": "tools/call",
            "params": {
                "name": "exocortex.search_memories",
                "arguments": { "query": query, "limit": 50 }
            }
        })
    }

    fn end_session(memories: serde_json::Value, edges: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0", "id": 80, "method": "tools/call",
            "params": {
                "name": "exocortex.end_session",
                "arguments": {
                    "session_id": "s-readback",
                    "project_id": "proj",
                    "memories": memories,
                    "edges": edges
                }
            }
        })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-readback-{}-{}",
        std::process::id(),
        uuid_v4_hex()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn uuid_v4_hex() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn search_hits(payload: &serde_json::Value) -> Vec<serde_json::Value> {
    let text = payload["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    let inner: serde_json::Value = serde_json::from_str(text).expect("tool payload JSON");
    inner["memories"].as_array().cloned().unwrap_or_default()
}

/// AC1: write offline, then `search_memories` in the SAME process
/// returns it — plus the R-M7 stamp advances to the acked LSN. Fresh
/// `--data-dir`: also pins the F3 ordering rule (the org graph exists
/// before the first write).
#[test]
fn in_session_read_back_after_offline_write() {
    let dir = tempdir();
    let mut c = Client::spawn(&dir);
    let mut msgs = Client::init_msgs();
    msgs.push(Client::end_session(
        serde_json::json!([
            { "draft_key": "p", "memory_type": "Problem", "title": "Zebra race condition in cache swap", "content": "swap races under load", "visibility": "org", "tags": ["concurrency"] }
        ]),
        serde_json::json!([]),
    ));
    msgs.push(Client::search("zebra"));
    c.send_all(&msgs);
    let _init = c.read_line();
    let ack = c.read_line();
    assert!(ack.get("result").is_some(), "end_session ok: {ack}");
    let ack_text = ack["result"]["content"][0]["text"].as_str().unwrap();
    let ack_json: serde_json::Value = serde_json::from_str(ack_text).unwrap();
    let acked_lsn = ack_json["local_lsns"][0].as_u64().expect("acked local lsn");

    let search = c.read_line();
    let hits = search_hits(&search);
    assert_eq!(hits.len(), 1, "exactly the written memory: {hits:?}");
    assert!(hits[0]["title"]
        .as_str()
        .unwrap()
        .contains("Zebra race condition"));
    let search_text = search["result"]["content"][0]["text"].as_str().unwrap();
    let search_inner: serde_json::Value = serde_json::from_str(search_text).unwrap();
    let local_lsn = search_inner["snapshot_version"]["local_lsn"].as_u64();
    assert!(
        local_lsn.is_some_and(|n| n >= acked_lsn),
        "R-M7 stamp reflects the offline write (got {local_lsn:?}, acked {acked_lsn})"
    );
}
