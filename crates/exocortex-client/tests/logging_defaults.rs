//! D47: every workspace binary that installs a tracing subscriber must
//! default to WARN, not tracing's ERROR-only `from_default_env` default.
//! A local service that hides its own warnings unless the operator happens
//! to export RUST_LOG is undiagnosable by default — GitHub issue #5's (a).

use std::path::PathBuf;

fn workspace_binaries() -> Vec<PathBuf> {
    let crates_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut mains: Vec<PathBuf> = std::fs::read_dir(crates_dir.join("crates"))
        .expect("workspace crates directory")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().join("src/main.rs"))
        .filter(|path| path.is_file())
        .collect();
    // R14: bin targets are binaries too — a service that emits tracing
    // from src/bin/*.rs without the subscriber is the same silent defect.
    let mut bins: Vec<PathBuf> = std::fs::read_dir(crates_dir.join("crates"))
        .expect("workspace crates directory")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let dir = entry.path().join("src/bin");
            std::fs::read_dir(dir).ok().map(|read| {
                read.filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "rs"))
                    .collect::<Vec<PathBuf>>()
            })
        })
        .flatten()
        .collect();
    mains.append(&mut bins);
    mains.sort();
    mains
}

/// R14 (renamed to what it checks): a SOURCE canary — every binary that
/// references tracing installs the WARN-default env builder. The
/// "RUST_LOG still overrides" half of the contract rides the
/// from_env_lossy builder idiom this asserts, not an executed binary.
#[test]
fn binaries_installing_tracing_default_it_to_warn() {
    let binaries = workspace_binaries();
    assert!(
        binaries.len() >= 11,
        "expected every shipped binary (node, client, worker, 7 adapters, cli bin) to be scanned, found {}",
        binaries.len()
    );
    let mut covered = 0;
    for bin in binaries {
        let src = std::fs::read_to_string(&bin).expect("readable binary source");
        let emits_tracing = src.contains("tracing_subscriber") || src.contains("tracing::");
        if !emits_tracing {
            continue; // a binary that never logs needs no filter
        }
        covered += 1;
        assert!(
            !src.contains("EnvFilter::from_default_env"),
            "{} still installs the ERROR-only default filter: WARN logging \
             stays invisible unless RUST_LOG is exported",
            bin.display()
        );
        assert!(
            src.contains("with_default_directive")
                && src.contains("LevelFilter::WARN")
                && src.contains("from_env_lossy"),
            "{} emits tracing but does not default the filter to WARN via \
             the env builder (RUST_LOG still overrides it)",
            bin.display()
        );
    }
    assert!(
        covered >= 10,
        "the ten WARN-default binaries must all be covered, found {covered}"
    );
}
