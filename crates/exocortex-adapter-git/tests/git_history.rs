//! D18 integration: a real fixture git repository (created with the git
//! binary in a tempdir — offline, deterministic, no network) flows
//! through the mapper and the SDK mock server end to end: registration
//! carries the declared projection, every commit and path lands, re-runs
//! are idempotent, and the bound stops oversized windows.

use std::process::Command;

use exocortex_adapter_git::{parse_git_log, projection, GIT_LOG_FORMAT};
use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{AdapterSession, SdkError};

fn git(dir: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) {
    let status = Command::new("git")
        .current_dir(dir)
        .args(args)
        .envs(envs.iter().copied())
        .status()
        .expect("git binary present in the test environment");
    assert!(status.success(), "git {args:?} failed");
}

/// A fixture repository: three commits over two files, fixed identities
/// and timestamps (GIT_* env), so the mapped history is byte-stable.
fn fixture_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let envs = [
        ("GIT_AUTHOR_NAME", "Fixture Author"),
        ("GIT_AUTHOR_EMAIL", "fixture@example.invalid"),
        ("GIT_COMMITTER_NAME", "Fixture Author"),
        ("GIT_COMMITTER_EMAIL", "fixture@example.invalid"),
        ("GIT_AUTHOR_DATE", "2026-08-30T10:00:00+00:00"),
        ("GIT_COMMITTER_DATE", "2026-08-30T10:00:00+00:00"),
    ];
    git(dir.path(), &["init", "-q", "-b", "main"], &[]);
    git(
        dir.path(),
        &[
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "chore: seed repository",
        ],
        &envs,
    );
    std::fs::write(dir.path().join("README.md"), "# fixture\n").unwrap();
    git(dir.path(), &["add", "README.md"], &[]);
    git(
        dir.path(),
        &["commit", "-q", "-m", "docs: describe the fixture"],
        &envs,
    );
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src/drain.rs"), "fn main() {}\n").unwrap();
    git(dir.path(), &["add", "src/drain.rs"], &[]);
    git(
        dir.path(),
        &[
            "commit",
            "-q",
            "-m",
            "fix: keep the drain from losing entries",
        ],
        &envs,
    );
    dir
}

fn log_of(repo: &std::path::Path, range: &str) -> String {
    let out = Command::new("git")
        .current_dir(repo)
        .args([
            "log",
            "--reverse",
            &format!("--format={GIT_LOG_FORMAT}"),
            "--name-only",
            range,
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn config_for(
    url: &str,
    cursor: std::path::PathBuf,
    max_window: u64,
) -> exocortex_adapter_sdk::AdapterConfig {
    let mut config =
        exocortex_adapter_sdk::AdapterConfig::new("org", "git://fixture-repo", "git-adapter", url);
    config.source_flavor = "custom".into();
    config.auth_token = "test-bearer".into();
    config.hmac_key = [7u8; 32];
    config.cursor_path = cursor;
    config.projection = Some(projection(max_window));
    config
}

#[test]
fn fixture_history_maps_deterministically() {
    let repo = fixture_repo();
    let log = log_of(repo.path(), "--all");
    let (commits, skipped) = parse_git_log(&log);
    assert_eq!(skipped, 0);
    assert_eq!(commits.len(), 3);
    // The classifier saw one fix and two non-fix commits.
    let units = bounded_units("fixture-repo", &commits);
    let fixes: usize = units
        .iter()
        .flat_map(|u| u.memories.iter())
        .filter(|m| m.memory_type == "Fix")
        .count();
    let commands: usize = units
        .iter()
        .flat_map(|u| u.memories.iter())
        .filter(|m| m.memory_type == "Command")
        .count();
    assert_eq!((fixes, commands), (1, 2));
    // 3 commits + 2 distinct paths.
    assert_eq!(units.iter().map(|u| u.memories.len()).sum::<usize>(), 5);
    // README.md and src/drain.rs each carry Modifies edges.
    assert_eq!(
        units.iter().map(|u| u.relationships.len()).sum::<usize>(),
        2
    );
    // Running the mapper twice over the same log is identical.
    let again = bounded_units("fixture-repo", &commits);
    assert_eq!(
        units.iter().map(|u| u.memories.len()).collect::<Vec<_>>(),
        again.iter().map(|u| u.memories.len()).collect::<Vec<_>>()
    );
    assert_eq!(
        units
            .iter()
            .map(|u| u.relationships.len())
            .collect::<Vec<_>>(),
        again
            .iter()
            .map(|u| u.relationships.len())
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn history_flows_through_the_ingestion_protocol() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), cursor.clone(), 256),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();

    // D21-a: the declared projection rides the registration.
    let registrations = mock.registrations();
    assert_eq!(registrations.len(), 1);
    let wire_projection = registrations[0]
        .projection
        .as_ref()
        .expect("git adapter declares its projection");
    assert!(wire_projection.selector.contains("git log"));
    assert_eq!(
        wire_projection.bounds.as_ref().unwrap().max_rows_per_window,
        256
    );

    let (commits, _) = parse_git_log(&log_of(repo.path(), "--all"));
    let units = bounded_units("fixture-repo", &commits);
    let newest = commits.last().unwrap().sha.clone();
    mock.push_script(vec![MockSubmit::Accept; units.len()]);
    let mut accepted = 0;
    for unit in units {
        let outcome = session.submit_window(vec![unit], &newest).await.unwrap();
        accepted += outcome.accepted;
        assert!(outcome.cursor_advanced);
    }
    assert_eq!(accepted, 5, "commits plus file contexts");
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), newest);

    // The submitted batches carry the external coordinates the server
    // needs for identity-stable re-runs.
    let submitted = mock.submitted();
    assert!(!submitted.is_empty());
    assert!(submitted
        .iter()
        .flat_map(|b| b.memories.iter())
        .all(|m| m.external_key.is_some()));
    // §18.6: the snapshot schema_hash is the canonical 32-byte digest
    // over the declared column set — the exact value the server
    // derives from the registration (the 16-byte table uuid this
    // adapter once shipped was rejected by every real backend).
    let snapshot = submitted[0].snapshot.as_ref().unwrap();
    assert_eq!(snapshot.schema_hash.len(), 32);
    assert_eq!(
        snapshot.schema_hash,
        exocortex_wire::projection::schema_hash(&exocortex_adapter_git::git_source_columns())
            .to_vec()
    );
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn rerun_is_an_idempotent_replay() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), dir.path().join("c.cursor"), 256),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();
    let (commits, _) = parse_git_log(&log_of(repo.path(), "--all"));
    let units = bounded_units("fixture-repo", &commits);
    let n = units.len();
    let newest = commits.last().unwrap().sha.clone();
    mock.push_script(vec![MockSubmit::Accept; n * 2]);
    for unit in units.clone() {
        session.submit_window(vec![unit], &newest).await.unwrap();
    }
    // Same seed + same history: the batch ids are content-bound, so the
    // re-submission carries the SAME ids the server's idempotency
    // registry settles (DUPLICATE_BATCH disposition, §18.8.5).
    for unit in units {
        session.submit_window(vec![unit], &newest).await.unwrap();
    }
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), n * 2);
    let first: Vec<&str> = submitted[..submitted.len() / 2]
        .iter()
        .map(|b| b.batch_id.as_str())
        .collect();
    let second: Vec<&str> = submitted[submitted.len() / 2..]
        .iter()
        .map(|b| b.batch_id.as_str())
        .collect();
    assert_eq!(
        first, second,
        "re-runs derive the same content-bound batch ids"
    );
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_bound_stops_the_window() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), cursor.clone(), 2),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();
    // A hand-built over-bound unit (the bounded mapper cannot produce
    // one): the declared window bound must still stop it before the
    // wire, cursor untouched.
    let unit = oversized_unit();
    let err = session
        .submit_window(vec![unit], "whatever")
        .await
        .unwrap_err();
    match err {
        SdkError::ProjectionBoundExceeded {
            bound,
            value,
            declared,
        } => {
            assert_eq!(bound, "max_rows_per_window");
            assert_eq!((value, declared), (5, 2));
        }
        other => panic!("expected the bound error, got {other:?}"),
    }
    // No submit reached the server and the cursor never existed.
    assert!(!mock.calls().contains(&"submit".to_string()));
    assert!(!cursor.exists());
    mock.stop();
}

/// Round-11: the BINARY writes the operator-facing cursor after a
/// settled window (round-10 R10-1 found it never written). Spawns the
/// real adapter against the SDK mock and asserts the file.
#[tokio::test(flavor = "multi_thread")]
async fn the_binary_writes_the_operator_cursor() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("op.cursor");
    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--producer",
            "git-test",
            "--cursor",
            cursor.to_str().unwrap(),
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("spawn the git adapter binary");
    assert!(
        output.status.success(),
        "adapter exited clean: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = std::fs::read_to_string(&cursor).expect("operator cursor written");
    assert_eq!(written.trim().len(), 40, "a full sha: {}", written);
}

/// Round-11: a one-shot `--range` run must NOT advance the sequential
/// operator cursor (it may sit mid-history; writing its HEAD strands
/// the gap before it).
#[tokio::test(flavor = "multi_thread")]
async fn a_one_shot_range_does_not_clobber_the_sequential_cursor() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("op2.cursor");
    // Sequential run first: seeds the cursor.
    mock.push_script(vec![MockSubmit::Accept]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--producer",
            "git-test",
            "--cursor",
            cursor.to_str().unwrap(),
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("sequential run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sequential = std::fs::read_to_string(&cursor).expect("cursor after sequential run");
    // One-shot range run: the cursor must be untouched.
    mock.push_script(vec![MockSubmit::Accept]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--producer",
            "git-test",
            "--cursor",
            cursor.to_str().unwrap(),
            "--range",
            "HEAD~2..HEAD~1",
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("range run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap(),
        sequential,
        "--range is one-shot; it must not move the sequential cursor"
    );
}

/// The cursor test's value pin: the operator cursor must equal the
/// repo's actual HEAD sha (writing the OLDEST sha, or writing before a
/// window settles, must fail here).
#[tokio::test(flavor = "multi_thread")]
async fn the_operator_cursor_is_the_newest_settled_sha() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("op3.cursor");
    mock.push_script(vec![MockSubmit::Accept]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--producer",
            "git-test",
            "--cursor",
            cursor.to_str().unwrap(),
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("spawn the git adapter binary");
    assert!(output.status.success());
    let head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap().trim(),
        head,
        "the cursor is the newest settled commit, not merely some sha"
    );
}

/// R11-13: permanently rejected rows sit behind an advanced cursor and
/// never retry — a scheduler must see failure (exit 2), not
/// success-with-lost-rows.
#[tokio::test(flavor = "multi_thread")]
async fn permanent_rejections_exit_nonzero() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("op4.cursor");
    mock.push_script(vec![MockSubmit::RejectRows(
        exocortex_wire::ingest::v1::RejectCode::ResourceLimitExceeded as i32,
        "fixture rejection",
    )]);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--producer",
            "git-test",
            "--cursor",
            cursor.to_str().unwrap(),
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("spawn the git adapter binary");
    assert_eq!(
        output.status.code(),
        Some(2),
        "exit 2 names lost rows: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// R10-4's parse guard, which round 10 gave the SaaS adapters but not
/// git: `--max-window < 2` cannot represent a window and must be
/// rejected at parse, before any fetch.
#[tokio::test(flavor = "multi_thread")]
async fn max_window_below_two_is_rejected_at_parse() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_exocortex-adapter-git"))
        .args([
            "--repo",
            repo.path().to_str().unwrap(),
            "--backend",
            &mock.url(),
            "--org",
            "org",
            "--max-window",
            "1",
        ])
        .env("EXOCORTEX_AUTH_TOKEN", "test-bearer")
        .env("EXOCORTEX_HMAC_KEY", "42".repeat(32))
        .output()
        .expect("spawn the git adapter binary");
    assert!(!output.status.success(), "parse rejection is an error");
    assert!(!mock.calls().iter().any(|c| c == "submit"));
}

/// The binary's mapping path: bounded units for the fixture history.
fn bounded_units(
    repo_id: &str,
    commits: &[exocortex_adapter_git::GitCommit],
) -> Vec<exocortex_adapter_sdk::BatchUnit> {
    exocortex_adapter_git::map_history_bounded(repo_id, commits, "w", 256, 64)
        .into_iter()
        .map(|b| b.unit)
        .collect()
}

/// Five independent rows in one unit: over the declared max_rows_per_window=2.
fn oversized_unit() -> exocortex_adapter_sdk::BatchUnit {
    use exocortex_adapter_sdk::BatchUnit;
    use exocortex_wire::ingest::v1::MemoryDraft;
    BatchUnit {
        batch_id_seed: "oversized".into(),
        memories: (0..5)
            .map(|i| MemoryDraft {
                rights: None,
                draft_key: format!("row-{i}"),
                id: String::new(),
                memory_type: "Command".into(),
                title: format!("row {i}"),
                content: "oversized fixture row".into(),
                tags: vec![],
                visibility: 3,
                valid_from: None,
                valid_until: None,
                external_key: None,
            })
            .collect(),
        relationships: vec![],
        snapshot: None,
        observed_at: std::time::UNIX_EPOCH,
    }
}
