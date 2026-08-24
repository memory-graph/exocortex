//! M3 acceptance: the `exocortex-mcp-client` binary runs, speaks MCP over
//! stdio, and returns synthetic data for `search_memories`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

struct Client {
    child: Child,
}

impl Client {
    fn spawn() -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
            .args(["--org", "smoke", "--user", "tester"])
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
        use std::io::BufRead;
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
    let mut c = Client::spawn();
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
