//! BR-PRD acceptance (docs/prd/backup-restore-prd.md): the WAL as a
//! portable file — round trip, idempotent import, fingerprint gate,
//! empty round trip, state preservation.

use std::io::Write;
use std::process::{Child, Command, Stdio};

mod support;

struct Client {
    child: Child,
    responses: support::BoundedLineReader,
}

impl Client {
    /// A one-shot mode (--export/--import): run to completion.
    fn run_oneshot(
        dir: &std::path::Path,
        flag: &str,
        file: &std::path::Path,
    ) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
            .args([
                "--org",
                "backup",
                "--user",
                "tester",
                "--data-dir",
                dir.to_str().unwrap(),
                flag,
                file.to_str().unwrap(),
            ])
            .output()
            .expect("run one-shot mode")
    }

    fn spawn_serving(dir: &std::path::Path) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"));
        cmd.args([
            "--org",
            "backup",
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
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exocortex-backup-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn init_msgs() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "backup-test", "version": "0" }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {}
        }),
    ]
}

fn search_msg(id: i64, query: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {
            "name": "exocortex.search_memories",
            "arguments": { "query": query, "limit": 50 }
        }
    })
}

fn hits_of(payload: &serde_json::Value) -> Vec<(String, String)> {
    let text = payload["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    let inner: serde_json::Value = serde_json::from_str(text).expect("payload JSON");
    inner["memories"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|m| {
            (
                m["id"].as_str().unwrap_or("").to_string(),
                m["title"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

fn end_session_msg(id: i64) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {
            "name": "exocortex.end_session",
            "arguments": {
                "session_id": "s-backup",
                "project_id": "proj",
                "memories": [
                    { "draft_key": "p", "memory_type": "Problem", "title": "Yak shave in release script", "content": "release script shaves yaks", "visibility": "org", "tags": ["release"] },
                    { "draft_key": "f", "memory_type": "Fix", "title": "Pin the toolchain instead", "content": "pin rust-toolchain", "visibility": "org" }
                ],
                "edges": [
                    { "from_draft_key": "f", "to_draft_key": "p", "kind": "Fixes", "strength": 0.9 }
                ]
            }
        }
    })
}

/// AC1: write, export, WIPE the data-dir, import, restart — every
/// memory searchable, every id byte-identical to pre-export.
#[test]
fn round_trip_preserves_ids_and_reads() {
    let dir = tempdir();
    let file = dir.join("memories.json");

    // Phase 1: write one batch (problem + fix + edge), capture ids.
    let ids = {
        let mut c = Client::spawn_serving(&dir);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        msgs.push(search_msg(82, "yak"));
        msgs.push(search_msg(83, "toolchain"));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some(), "write ok");
        let h1 = hits_of(&c.read_line());
        let h2 = hits_of(&c.read_line());
        assert_eq!(h1.len(), 1);
        assert_eq!(h2.len(), 1);
        vec![h1[0].0.clone(), h2[0].0.clone()]
    };

    // Phase 2: export, wipe, import.
    let out = Client::run_oneshot(&dir, "--export", &file);
    assert!(
        out.status.success(),
        "export ok: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::remove_dir_all(dir.join("wal")).expect("wipe wal");
    let out = Client::run_oneshot(&dir, "--import", &file);
    assert!(
        out.status.success(),
        "import ok: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Phase 3: same ids, same reads, on the restored WAL.
    let mut c = Client::spawn_serving(&dir);
    let mut msgs = init_msgs();
    msgs.push(search_msg(90, "yak"));
    msgs.push(search_msg(91, "toolchain"));
    c.send_all(&msgs);
    let _init = c.read_line();
    let h1 = hits_of(&c.read_line());
    let h2 = hits_of(&c.read_line());
    assert_eq!(h1.len(), 1, "yak write restored: {h1:?}");
    assert_eq!(h2.len(), 1, "toolchain write restored: {h2:?}");
    assert_eq!(h1[0].0, ids[0], "id byte-identical across wipe+restore");
    assert_eq!(h2[0].0, ids[1], "id byte-identical across wipe+restore");
}

/// AC2: importing the same backup twice is a no-op on the served graph.
#[test]
fn import_is_idempotent() {
    let dir = tempdir();
    let file = dir.join("memories.json");
    {
        let mut c = Client::spawn_serving(&dir);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some());
    }
    assert!(Client::run_oneshot(&dir, "--export", &file)
        .status
        .success());
    // Import the same file twice into the SAME dir (entries already there).
    assert!(Client::run_oneshot(&dir, "--import", &file)
        .status
        .success());
    assert!(Client::run_oneshot(&dir, "--import", &file)
        .status
        .success());
    let mut c = Client::spawn_serving(&dir);
    let mut msgs = init_msgs();
    msgs.push(search_msg(90, "yak"));
    c.send_all(&msgs);
    let _init = c.read_line();
    let hits = hits_of(&c.read_line());
    assert_eq!(hits.len(), 1, "no duplicates after double import: {hits:?}");
}

/// AC3: a fingerprint-mismatched backup aborts before anything is
/// written; the target WAL is untouched.
#[test]
fn fingerprint_mismatch_aborts_cleanly() {
    let src = tempdir();
    let dst = tempdir();
    let file = src.join("memories.json");
    {
        let mut c = Client::spawn_serving(&src);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some());
    }
    assert!(Client::run_oneshot(&src, "--export", &file)
        .status
        .success());

    // Tamper with the fingerprint.
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    doc["ontology_fingerprint"] = serde_json::json!("0".repeat(64));
    std::fs::write(&file, serde_json::to_string(&doc).unwrap()).unwrap();

    // Seed the destination with one real write, so "untouched" is
    // observable.
    {
        let mut c = Client::spawn_serving(&dst);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some());
    }
    let out = Client::run_oneshot(&dst, "--import", &file);
    assert!(!out.status.success(), "mismatched import must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("fingerprint"), "reason surfaces: {stderr}");

    let mut c = Client::spawn_serving(&dst);
    let mut msgs = init_msgs();
    msgs.push(search_msg(90, "yak"));
    c.send_all(&msgs);
    let _init = c.read_line();
    let hits = hits_of(&c.read_line());
    assert_eq!(hits.len(), 1, "target WAL untouched (its own write only)");
}

/// AC4: an empty WAL exports a valid empty backup; importing it is a
/// no-op success.
#[test]
fn empty_round_trip() {
    let dir = tempdir();
    let file = dir.join("memories.json");
    let out = Client::run_oneshot(&dir, "--export", &file);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("0 entries"), "summary says empty: {stdout}");
    let out = Client::run_oneshot(&dir, "--import", &file);
    assert!(out.status.success(), "empty import is a no-op success");
}

/// AC5: entry states ride verbatim — `Synced` before export is
/// `Synced` after import and cannot re-drain.
#[test]
fn states_ride_verbatim() {
    let dir = tempdir();
    let file = dir.join("memories.json");
    {
        let mut c = Client::spawn_serving(&dir);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some());
    }
    // Settle the entry directly, then export.
    {
        let wal = exocortex_client::wal::Wal::open(&dir.join("wal")).unwrap();
        let states = wal.states_for_test().unwrap();
        assert_eq!(states.len(), 1);
        wal.mark_synced(states[0].0, 777).unwrap();
        drop(wal);
    }
    assert!(Client::run_oneshot(&dir, "--export", &file)
        .status
        .success());
    std::fs::remove_dir_all(dir.join("wal")).expect("wipe wal");
    assert!(Client::run_oneshot(&dir, "--import", &file)
        .status
        .success());
    let wal = exocortex_client::wal::Wal::open(&dir.join("wal")).unwrap();
    let states = wal.states_for_test().unwrap();
    assert_eq!(
        states.len(),
        1,
        "one entry after wipe+import, no duplicates"
    );
    match states[0].1 {
        exocortex_client::wal::WalState::Synced { backend_lsn } => {
            assert_eq!(
                backend_lsn, 777,
                "Synced rides verbatim — will not re-drain"
            );
        }
        other => panic!("expected Synced, got {other:?}"),
    }
}

/// R6-R20: oversized files are rejected from metadata before allocation or
/// parsing, and the pre-existing WAL remains byte-for-byte usable.
#[test]
fn oversized_backup_is_rejected_without_partial_import() {
    let dir = tempdir();
    let file = dir.join("oversized.json");
    {
        let mut c = Client::spawn_serving(&dir);
        let mut msgs = init_msgs();
        msgs.push(end_session_msg(80));
        c.send_all(&msgs);
        let _init = c.read_line();
        assert!(c.read_line().get("result").is_some());
    }
    let oversized = std::fs::File::create(&file).unwrap();
    oversized
        .set_len(exocortex_client::backup::MAX_BACKUP_BYTES + 1)
        .unwrap();
    drop(oversized);

    let out = Client::run_oneshot(&dir, "--import", &file);
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("maximum supported size"),
        "resource rejection must be explicit: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut c = Client::spawn_serving(&dir);
    let mut msgs = init_msgs();
    msgs.push(search_msg(90, "yak"));
    c.send_all(&msgs);
    let _init = c.read_line();
    assert_eq!(hits_of(&c.read_line()).len(), 1, "existing WAL is intact");
}
