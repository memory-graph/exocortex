//! D47: every workspace binary that installs a tracing subscriber must
//! default to WARN, not tracing's ERROR-only `from_default_env` default.
//! A local service that hides its own warnings unless the operator happens
//! to export RUST_LOG is undiagnosable by default — GitHub issue #5's (a).

use std::path::PathBuf;

fn workspace_crates() -> Vec<PathBuf> {
    let crates_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_dir(crates_dir.join("crates"))
        .expect("workspace crates directory")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().join("src/main.rs"))
        .filter(|path| path.is_file())
        .filter(|path| {
            std::fs::read_to_string(path)
                .expect("readable main.rs")
                .contains("tracing_subscriber")
        })
        .collect()
}

#[test]
fn binaries_log_warnings_by_default_rust_log_still_overrides() {
    let mains = workspace_crates();
    assert!(
        mains.len() >= 10,
        "expected every shipped binary (node, client, worker, 7 adapters) to be covered, found {}",
        mains.len()
    );
    for main in mains {
        let src = std::fs::read_to_string(&main).expect("readable main.rs");
        assert!(
            !src.contains("EnvFilter::from_default_env"),
            "{} still installs the ERROR-only default filter: WARN logging \
             stays invisible unless RUST_LOG is exported",
            main.display()
        );
        assert!(
            src.contains("with_default_directive")
                && src.contains("LevelFilter::WARN")
                && src.contains("from_env_lossy"),
            "{} must default the filter to WARN via the env builder so \
             RUST_LOG still overrides it",
            main.display()
        );
    }
}
