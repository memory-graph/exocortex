//! D37 / ai1: the consumer-contract test. Every agent-facing tool's
//! result must carry what the playbook and the README promise — pinned
//! through the REAL standalone binary over stdio MCP, not through a
//! shared handler (parity legs compare a surface with itself and
//! cannot catch a shared omission; the D35 lesson).
//!
//! Promises pinned here (playbook v1_0_0 + README tool table +
//! agent-instructions PRD §11):
//! - search/get/find_related memories carry `content` and
//!   `memory_type_label` (S8 / D35),
//! - read results carry ids an agent can cite (`to_memory_id`),
//! - supersession is visible on the read path (`superseded_by`),
//! - `end_session` rejections carry `code` AND `detail` ("read `detail`
//!   first"), locally, before any wire call,
//! - `preflight_wrapup` names rejections the same way,
//! - `playbook_version` reports version + content hashes.

use std::io::Write;
use std::process::{Child, Command, Stdio};

mod support;

struct Client {
    child: Child,
    responses: support::BoundedLineReader,
}

impl Client {
    fn spawn(dir: &std::path::Path) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
        cmd.args([
            "--org",
            "contracts",
            "--user",
            "tester",
            "--data-dir",
            dir.to_str().unwrap(),
        ]);
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn exocortex-mcp-client");
        let responses = support::BoundedLineReader::new(child.stdout.take().expect("stdout"));
        Self { child, responses }
    }

    fn send_all(&mut self, msgs: &[serde_json::Value]) {
        let stdin = self.child.stdin.as_mut().expect("stdin");
        for m in msgs {
            writeln!(stdin, "{m}").unwrap();
        }
        stdin.flush().unwrap();
    }

    fn read_line(&mut self) -> serde_json::Value {
        self.responses.read_json(&mut self.child)
    }

    fn init_msgs() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": { "name": "contracts-test", "version": "0" }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
            }),
        ]
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn call(id: i64, name: &str, arguments: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    })
}

/// The tool's inner payload: MCP content[0].text parsed as JSON.
fn payload(rpc: &serde_json::Value) -> serde_json::Value {
    let text = rpc["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result carries text: {rpc}"));
    serde_json::from_str(text).expect("tool payload is JSON")
}

fn draft(key: &str, type_label: &str, title: &str, content: &str) -> serde_json::Value {
    serde_json::json!({
        "draft_key": key,
        "memory_type": type_label,
        "title": title,
        "content": content,
        "visibility": "org"
    })
}

fn is_hex32(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// ai1: every agent-facing tool answers with the payload its consumer
/// was promised. Drives the shipped binary end to end.
#[test]
fn agent_tools_serve_the_documented_payload_contract() {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-contracts-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut c = Client::spawn(&dir);

    // -- end_session: the offline ack names the acked LSNs.
    let problem_body =
        "Bounds-check the token window before recursing; the off-by-one skips the final guard.";
    let solution_body =
        "Clamp the recursion depth against the token window length, not the window minus one.";
    c.send_all(&[
        Client::init_msgs()[0].clone(),
        Client::init_msgs()[1].clone(),
        call(
            10,
            "exocortex.end_session",
            serde_json::json!({
                "session_id": "s-contracts",
                "project_id": "proj",
                "memories": [
                    draft("prob", "Problem", "Quokka buffer overflow in token window", problem_body),
                    draft("sol", "Solution", "Quokka token window bounds fix", solution_body)
                ],
                "edges": [
                    { "from_draft_key": "sol", "to_draft_key": "prob", "kind": "Solves" }
                ]
            }),
        ),
    ]);
    let _init = c.read_line();
    let ack = c.read_line();
    assert!(ack.get("result").is_some(), "valid batch acks: {ack}");
    let ack = payload(&ack);
    let lsns = ack["local_lsns"]
        .as_array()
        .expect("ack carries local_lsns");
    assert!(
        !lsns.is_empty(),
        "the offline batch acks its WAL LSN: {ack}"
    );
    assert_eq!(
        ack["sync_pending"].as_bool(),
        Some(true),
        "offline writes say they await sync: {ack}"
    );

    // -- search_memories: hits carry everything an agent cites or reads.
    c.send_all(&[call(
        11,
        "exocortex.search_memories",
        serde_json::json!({ "query": "Quokka buffer overflow", "limit": 10 }),
    )]);
    let out = payload(&c.read_line());
    let hits = out["memories"].as_array().expect("search returns memories");
    assert_eq!(hits.len(), 1, "one hit: {out}");
    let hit = &hits[0];
    let prob_id = hit["id"].as_str().expect("hit carries an id").to_string();
    assert!(is_hex32(&prob_id), "citable 32-hex id: {hit}");
    assert_eq!(
        hit["title"].as_str(),
        Some("Quokka buffer overflow in token window")
    );
    assert_eq!(
        hit["content"].as_str(),
        Some(problem_body),
        "S8: full content"
    );
    assert_eq!(
        hit["memory_type_label"].as_str(),
        Some("Problem"),
        "S8: labeled type"
    );
    assert!(
        hit["memory_type"].as_u64().is_some(),
        "numeric type rides alongside the label: {hit}"
    );
    assert!(hit["visibility"].as_str().is_some(), "visibility label");
    assert_eq!(
        out["scores"].as_array().map(Vec::len),
        Some(hits.len()),
        "scores align with memories (§14.1): {out}"
    );
    assert!(
        out["snapshot_version"]["local_lsn"].as_u64().is_some(),
        "R-M7 stamp rides the client surface: {out}"
    );

    // -- get_memory: the point read returns the memory in full.
    c.send_all(&[call(
        12,
        "exocortex.get_memory",
        serde_json::json!({ "id": prob_id }),
    )]);
    let memory = payload(&c.read_line())["memory"].clone();
    assert_eq!(
        memory["content"].as_str(),
        Some(problem_body),
        "S8: {memory}"
    );
    assert_eq!(memory["memory_type_label"].as_str(), Some("Problem"));
    assert!(
        memory.get("superseded_by").is_none() || memory["superseded_by"].is_null(),
        "freshly written row is not superseded: {memory}"
    );

    // -- find_related: the neighborhood carries readable rows.
    c.send_all(&[call(
        13,
        "exocortex.find_related",
        serde_json::json!({ "anchor": prob_id, "k": 1 }),
    )]);
    let related = payload(&c.read_line())["memories"]
        .as_array()
        .expect("find_related returns memories")
        .clone();
    assert!(
        related.len() >= 2,
        "the Solves edge is traversed: {related:?}"
    );
    for row in &related {
        assert!(
            row["content"].as_str().is_some_and(|s| !s.is_empty()),
            "S8: {row}"
        );
        assert!(
            row["memory_type_label"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "S8: {row}"
        );
    }

    // -- supersession is visible on the read path (§4.10a / S7(b)),
    //    through the real tool, not only at the registry.
    c.send_all(&[call(
        14,
        "exocortex.end_session",
        serde_json::json!({
            "session_id": "s-contracts",
            "project_id": "proj",
            "memories": [
                draft("v2", "Solution", "Quokka token window bounds fix v2", "Window length, clamped once, shared by every recursion level.")
            ],
            "edges": [
                { "from_draft_key": "v2", "to_memory_id": prob_id, "kind": "Replaces" }
            ]
        }),
    )]);
    let sup_ack = c.read_line();
    assert!(
        sup_ack.get("result").is_some(),
        "supersession batch acks: {sup_ack}"
    );

    c.send_all(&[call(
        15,
        "exocortex.get_memory",
        serde_json::json!({ "id": prob_id }),
    )]);
    let superseded = payload(&c.read_line())["memory"].clone();
    assert!(
        superseded["superseded_by"].as_str().is_some_and(is_hex32),
        "the stale row names its successor (§4.10a): {superseded}"
    );

    // -- end_session validates locally BEFORE writing and tells the
    //    agent exactly what to fix (offline form: the call errors with
    //    a structured {error, message} naming the offending value;
    //    nothing reaches the WAL). The structured rejections[] ack is
    //    the ONLINE contract (round-13 finding: pinned at wire/registry,
    //    still unpinned through the tool).
    c.send_all(&[call(
        16,
        "exocortex.end_session",
        serde_json::json!({
            "session_id": "s-contracts",
            "project_id": "proj",
            "memories": [
                draft("nonsense", "Nonsense", "Unknown type", "body")
            ],
            "edges": []
        }),
    )]);
    let refused = c.read_line();
    let refused_text = refused.to_string();
    assert!(
        !refused_text.contains("local_lsns"),
        "an invalid batch is never written: {refused_text}"
    );
    assert!(
        refused_text.contains("Nonsense"),
        "the refusal names the offending value: {refused_text}"
    );

    // -- preflight_wrapup names rejections the same way, without writing:
    //    code + detail + deterministic correction (P3), in the server's
    //    vocabulary.
    c.send_all(&[call(
        17,
        "exocortex.preflight_wrapup",
        serde_json::json!({
            "project_id": "proj",
            "memories": [draft("nonsense", "Nonsense", "Unknown type", "body")],
            "edges": []
        }),
    )]);
    let pre = payload(&c.read_line());
    assert_eq!(
        pre["would_reject"].as_u64(),
        Some(1),
        "the verdict counts the rejected row: {pre}"
    );
    let row = &pre["rejections"][0];
    assert_eq!(
        row["code"].as_str(),
        Some("UnknownMemoryType"),
        "the code matches the server vocabulary: {pre}"
    );
    assert!(
        row["detail"]
            .as_str()
            .is_some_and(|s| s.contains("Nonsense")),
        "the detail names the offender: {pre}"
    );
    assert!(
        row["correction"].as_str().is_some_and(|s| !s.is_empty()),
        "P3: deterministic correction rides the rejection: {pre}"
    );

    // -- playbook_version: one version string, both content hashes.
    c.send_all(&[call(
        18,
        "exocortex.playbook_version",
        serde_json::json!({}),
    )]);
    let version = payload(&c.read_line());
    assert!(
        version["version"].as_str().is_some_and(|s| !s.is_empty()),
        "version is reported: {version}"
    );
    for key in ["playbook_hash", "block_hash"] {
        assert!(
            version[key].as_str().is_some_and(|s| s.len() >= 32),
            "{key} is reported: {version}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
