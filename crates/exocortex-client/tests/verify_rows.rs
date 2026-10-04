//! D49 (GitHub issue #6): `--verify` must SHOW the four checkable
//! breakages — an unwired harness (RED, per PRD S6, never absent), an
//! org/user partition mismatch against the WAL's ledger, orphaned store
//! processes on the data dir, and a stale store `port` artifact.

use std::process::Command;

use exocortex_client::wal::Wal;
use exocortex_kernel::MemoryId;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("exo-verify-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run_verify(data_dir: &std::path::Path, org: &str, user: &str) -> (String, Option<i32>) {
    let out = Command::new(env!("CARGO_BIN_EXE_exocortex-mcp-client"))
        .arg("--verify")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--org")
        .arg(org)
        .arg("--user")
        .arg(user)
        .env(
            "EXOCORTEX_VERIFY_HARNESS_CONFIG",
            data_dir.join("harness.cfg"),
        )
        .output()
        .expect("spawn exocortex-mcp-client --verify");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        out.status.code(),
    )
}

/// The PRD-promised row: absent harness wiring is RED, a wired custom
/// config is ok. Fail-without-it: pre-D49 verify printed no harness row
/// at all and exited 0 here.
#[test]
fn harness_row_is_red_when_unwired_and_green_when_wired() {
    let dir = temp_dir("harness");
    let (out, code) = run_verify(&dir, "org", "user");
    assert!(
        out.contains("RED   harness:"),
        "unwired harness must be a RED row, got:\n{out}"
    );
    assert!(code.unwrap_or(0) >= 1, "red rows must exit non-zero");
    // R14: the README contract is exit code == red count — pin both sides.
    let reds = out.matches("RED   ").count() as i32;
    assert_eq!(
        code.unwrap_or(0),
        reds,
        "exit code must equal the red-row count"
    );

    // Wire the config at this binary's own install directory.
    let exe = std::env::current_exe().unwrap();
    let install_dir = exe.parent().unwrap();
    let wired = format!(
        "mcp add exocortex --command {}/exocortex-mcp-client\n",
        install_dir.display()
    );
    std::fs::write(dir.join("harness.cfg"), wired).unwrap();
    let (out, _code) = run_verify(&dir, "org", "user");
    assert!(
        out.contains("ok    harness:") && !out.contains("RED   harness:"),
        "wired harness must be ok, got:\n{out}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The partition ledger: WAL writes stamped under personal/gregory, then
/// verify runs as my-org/me and must go RED naming the live partition.
#[test]
fn partition_mismatch_is_red_naming_the_live_partitions() {
    let dir = temp_dir("partition");
    let wal = Wal::open(&dir.join("wal")).unwrap();
    let draft = exocortex_kernel::MemoryDraft {
        memory_type: 3,
        title: "stamped write".into(),
        content: "ledger row".into(),
        summary: None,
        visibility: exocortex_kernel::Visibility::Org,
        context: exocortex_kernel::MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
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
        edge_hints: Default::default(),
        external_key: None,
    };
    wal.append_batch_full(
        "s",
        vec![draft],
        vec![MemoryId::new_v7()],
        "b1".into(),
        vec!["k".into()],
        vec![vec![]],
        "personal",
        "gregory",
    )
    .unwrap();
    // Sanity at the ledger level, then release the sled lock so the
    // child binary can open the same WAL.
    assert_eq!(
        wal.partitions().unwrap(),
        vec![("personal".to_string(), "gregory".to_string())]
    );
    drop(wal);

    let (out, code) = run_verify(&dir, "my-org", "me");
    assert!(
        out.contains("RED   partition:") && out.contains("personal/gregory"),
        "wrong pair must be RED naming the live partition, got:\n{out}"
    );
    assert_eq!(
        code.unwrap_or(0),
        out.lines()
            .filter(|l| l.trim_start().starts_with("RED"))
            .count() as i32,
        "exit code must equal the red-row count"
    );

    let (out, _code) = run_verify(&dir, "personal", "gregory");
    assert!(
        out.contains("ok    partition:"),
        "the live pair must be ok, got:\n{out}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A stale `port` artifact (nothing answers) is RED; a live listener
/// answering PING turns it ok.
#[test]
fn stale_port_file_is_red_until_a_server_answers() {
    let dir = temp_dir("port");
    std::fs::write(dir.join("port"), "1\n").unwrap(); // port 1: nothing there
    let (out, code) = run_verify(&dir, "org", "user");
    assert!(
        out.contains("RED   store: stale port file"),
        "a dead port must be RED, got:\n{out}"
    );
    assert!(code.unwrap_or(0) >= 1);

    // Now a real listener on the recorded port.
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::fs::write(dir.join("port"), format!("{port}\n")).unwrap();
    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 8];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"+PONG\r\n");
        }
    });
    // The single store-process check must not go RED on a store-less
    // dir: ps finds nothing on this temp dir.
    let (out, _code) = run_verify(&dir, "org", "user");
    assert!(
        out.contains("ok    store: port file answers PING"),
        "a live listener must be ok, got:\n{out}"
    );
    assert!(
        !out.contains("RED   store:"),
        "no store-process false positive on a clean dir, got:\n{out}"
    );
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
