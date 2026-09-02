//! D18 integration: a real fixture git repository (created with the git
//! binary in a tempdir — offline, deterministic, no network) flows
//! through the mapper and the SDK mock server end to end: registration
//! carries the declared projection, every commit and path lands, re-runs
//! are idempotent, and the bound stops oversized windows.

use std::process::Command;

use exocortex_adapter_git::{map_history, parse_git_log, projection, GIT_LOG_FORMAT};
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
    let unit = map_history("fixture-repo", &commits, "seed");
    let fixes = unit
        .memories
        .iter()
        .filter(|m| m.memory_type == "Fix")
        .count();
    let commands = unit
        .memories
        .iter()
        .filter(|m| m.memory_type == "Command")
        .count();
    assert_eq!((fixes, commands), (1, 2));
    // 3 commits + 2 distinct paths.
    assert_eq!(unit.memories.len(), 5);
    // README.md and src/drain.rs each carry Modifies edges.
    assert_eq!(unit.relationships.len(), 2);
    // Running the mapper twice over the same log is identical.
    let again = map_history("fixture-repo", &commits, "seed");
    assert_eq!(unit.memories.len(), again.memories.len());
    assert_eq!(unit.relationships.len(), again.relationships.len());
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
    let unit = map_history("fixture-repo", &commits, "window-0");
    let newest = commits.last().unwrap().sha.clone();
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session.submit_window(vec![unit], &newest).await.unwrap();
    assert_eq!(outcome.accepted, 5, "commits plus file contexts");
    assert!(outcome.cursor_advanced);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), newest);

    // The submitted batch carries the external coordinates the server
    // needs for identity-stable re-runs.
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), 1);
    assert!(submitted[0]
        .memories
        .iter()
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
    let unit = map_history("fixture-repo", &commits, "window-0");
    let newest = commits.last().unwrap().sha.clone();
    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);
    session
        .submit_window(
            vec![map_history("fixture-repo", &commits, "window-0")],
            &newest,
        )
        .await
        .unwrap();
    // Same seed + same history: the batch id is content-bound, so the
    // re-submission carries the SAME id the server's idempotency
    // registry settles (DUPLICATE_BATCH disposition, §18.8.5).
    session.submit_window(vec![unit], &newest).await.unwrap();
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), 2);
    assert_eq!(
        submitted[0].batch_id, submitted[1].batch_id,
        "re-runs derive the same content-bound batch id"
    );
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_bound_stops_the_window() {
    let repo = fixture_repo();
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), cursor.clone(), 2),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();
    let (commits, _) = parse_git_log(&log_of(repo.path(), "--all"));
    let unit = map_history("fixture-repo", &commits, "window-0");
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
