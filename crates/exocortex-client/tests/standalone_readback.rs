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

fn one_memory(draft_key: &str, type_label: &str, title: &str) -> serde_json::Value {
    serde_json::json!({
        "draft_key": draft_key,
        "memory_type": type_label,
        "title": title,
        "content": "body",
        "visibility": "org"
    })
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

/// AC2: restart over the same `--data-dir` — the write is still
/// searchable and the id is byte-stable (the WAL-stored id, not a
/// regeneration). Requires F3 boot seeding.
#[test]
fn cross_restart_read_back_with_stable_ids() {
    let dir = tempdir();
    let id = {
        let mut first = Client::spawn(&dir);
        let mut msgs = Client::init_msgs();
        msgs.push(Client::end_session(
            serde_json::json!([one_memory("d", "CodePattern", "Adopt blake3 for ids")]),
            serde_json::json!([]),
        ));
        msgs.push(Client::search("blake3"));
        first.send_all(&msgs);
        let _init = first.read_line();
        let ack = first.read_line();
        assert!(ack.get("result").is_some(), "write ok: {ack}");
        let search = first.read_line();
        let hits = search_hits(&search);
        assert_eq!(hits.len(), 1, "in-session hit (F2): {hits:?}");
        hits[0]["id"].as_str().unwrap().to_string()
    };
    let mut second = Client::spawn(&dir);
    let mut msgs = Client::init_msgs();
    msgs.push(Client::search("blake3"));
    second.send_all(&msgs);
    let _init = second.read_line();
    let search = second.read_line();
    let hits = search_hits(&search);
    assert_eq!(hits.len(), 1, "write survives restart: {hits:?}");
    assert_eq!(
        hits[0]["id"].as_str().unwrap(),
        id,
        "WAL-stored id byte-stable across restart"
    );
}

/// AC6: boot seeding covers EVERY WAL state — Pending, Synced, and
/// Failed entries are all searchable after a restart. In standalone
/// nothing else will ever deliver these rows server-side, so the WAL
/// is their only read path.
#[test]
fn boot_seeds_pending_synced_and_failed_entries() {
    let dir = tempdir();
    {
        let mut first = Client::spawn(&dir);
        let mut msgs = Client::init_msgs();
        for title in [
            "Palladium purge panics",
            "Silver sync stalls",
            "Flax failure flakes",
        ] {
            msgs.push(Client::end_session(
                serde_json::json!([one_memory("d", "Problem", title)]),
                serde_json::json!([]),
            ));
        }
        first.send_all(&msgs);
        let _init = first.read_line();
        for i in 0..3 {
            let ack = first.read_line();
            assert!(ack.get("result").is_some(), "write {i} ok: {ack}");
        }
    }
    {
        // Settle two of the three entries directly in the WAL (the
        // states a drain leaves behind), then drop the handle.
        let wal = exocortex_client::wal::Wal::open(&dir.join("wal")).unwrap();
        let states = wal.states_for_test();
        assert_eq!(states.len(), 3);
        wal.mark_synced(states[0].0, 500).unwrap();
        wal.mark_failed(states[1].0).unwrap();
        drop(wal);
    }
    let mut second = Client::spawn(&dir);
    let mut msgs = Client::init_msgs();
    for q in ["palladium", "silver", "flax"] {
        msgs.push(Client::search(q));
    }
    second.send_all(&msgs);
    let _init = second.read_line();
    for (i, title_part) in ["palladium", "silver", "flax"].into_iter().enumerate() {
        let search = second.read_line();
        let hits = search_hits(&search);
        assert_eq!(
            hits.len(),
            1,
            "search {i} (`{title_part}`) must hit its write in ANY wal state: {hits:?}"
        );
    }
}

/// AC3 + AC7: edges written in the batch are traversable
/// (`find_related` over the `Fixes` edge), the F5 `Conversation` node
/// and `InSession` edges exist with D6's shape, and a dangling
/// cross-batch `to_memory_id` neither fails the write nor the read.
#[test]
fn edges_and_grouping_readable_dangling_edge_harmless() {
    let dir = tempdir();
    let mut c = Client::spawn(&dir);
    let mut msgs = Client::init_msgs();
    msgs.push(Client::end_session(
        serde_json::json!([
            one_memory("prob", "Problem", "Oak overflow in parser"),
            one_memory("fix", "Fix", "Raise the oak ceiling"),
        ]),
        serde_json::json!([
            { "from_draft_key": "fix", "to_draft_key": "prob", "kind": "Fixes", "strength": 0.9 },
            // AC7: targets an id with no local row — accepted (existence
            // is drain-time per §4.5) but never materialized.
            { "from_draft_key": "fix", "to_memory_id": "0123456789abcdef0123456789abcdef", "kind": "Fixes" }
        ]),
    ));
    msgs.push(Client::search("ceiling"));
    c.send_all(&msgs);
    let _init = c.read_line();
    let ack = c.read_line();
    assert!(
        ack.get("result").is_some(),
        "dangling edge must not fail the write: {ack}"
    );
    let search = c.read_line();
    let hits = search_hits(&search);
    assert_eq!(hits.len(), 1, "the fix is searchable: {hits:?}");
    let fix_id = hits[0]["id"].as_str().unwrap().to_string();

    // find_related over the Fixes edge + the InSession edge (k=1).
    let mut q = Vec::new();
    q.push(serde_json::json!({
        "jsonrpc": "2.0", "id": 70, "method": "tools/call",
        "params": {
            "name": "exocortex.find_related",
            "arguments": { "anchor": fix_id, "k": 1 }
        }
    }));
    c.send_all(&q);
    let rel = c.read_line();
    assert!(
        rel.get("result").is_some(),
        "dangling edge must not fail reads: {rel}"
    );
    let text = rel["result"]["content"][0]["text"].as_str().unwrap();
    let inner: serde_json::Value = serde_json::from_str(text).unwrap();
    let titles: Vec<&str> = inner["memories"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["title"].as_str().unwrap())
        .collect();
    assert!(
        titles.contains(&"Oak overflow in parser"),
        "Fixes edge traversable: {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t.starts_with("Session ")),
        "Conversation node reachable via InSession: {titles:?}"
    );
}

/// F5 parity: the client-side grouping builders mint byte-identical
/// rows to the backend commit path's (`exocortex-ingest` grouping.rs)
/// for the same inputs — the W2 golden-table discipline applied to
/// builders. An agent that learns "my writes group into conversations"
/// in standalone must not lose that when a backend appears.
#[test]
fn grouping_parity_with_backend_commit_path() {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology = exocortex_kernel::pack::load_registered_packs().unwrap();
    let now = chrono::Utc::now();
    let org = "parity-org";
    let key = "session-key-1";

    let rule = &exocortex_ingest::grouping::grouping_rules()[0];
    assert_eq!(rule.flavor, "session", "the rule under parity");
    assert_eq!(rule.node_type, "Conversation");
    assert_eq!(rule.edge_kind, "InSession");

    let backend_node = exocortex_ingest::grouping::grouping_node(&ontology, org, rule, key, now)
        .expect("backend node");
    let local_node = exocortex_client::materialize::grouping_node_local(&ontology, org, key, now)
        .expect("local node");
    assert_eq!(
        serde_json::to_value(&backend_node).unwrap(),
        serde_json::to_value(&local_node).unwrap(),
        "grouping node identical to the backend mint"
    );

    let member = exocortex_client::materialize::grouping_node_local(&ontology, org, "other", now)
        .expect("member");
    let backend_edge =
        exocortex_ingest::grouping::grouping_edge(&ontology, rule, &member, &backend_node, now)
            .expect("backend edge");
    let local_edge =
        exocortex_client::materialize::grouping_edge_local(&ontology, &member, &local_node, now)
            .expect("local edge");
    assert_eq!(
        serde_json::to_value(&backend_edge).unwrap(),
        serde_json::to_value(&local_edge).unwrap(),
        "InSession edge identical to the backend mint (same derivation, same id)"
    );
}
