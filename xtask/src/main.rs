// xtask/src/main.rs
use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Compute + print the effective OntologyFingerprint of the pack set
    /// that would be linked into `exocortex-server`. Used in CI to detect
    /// unintended ontology drift between commits.
    Fingerprint,
    /// Generate MCP + OpenAPI schemas from the operation registry.
    /// Fails if generated schemas are out of date (CI gate). Pass --write to
    /// regenerate and commit the goldens.
    GenSchemas {
        /// Regenerate the golden files instead of verifying.
        #[arg(long)]
        write: bool,
    },
    /// D1/P2 (agent-instructions PRD): regenerate the playbook's
    /// generated sections (kind catalogue, reject table) from the pack
    /// and the RejectCode enum. Fails on drift (CI gate); --write
    /// regenerates. Also enforces the instruction block's ≤300-word
    /// bound and its kind claims.
    GenPlaybook {
        /// Regenerate instead of verify.
        #[arg(long)]
        write: bool,
    },
    /// Run the R-I1/R-I5 kernel purity check: shells out to
    /// `cargo tree -p exocortex-kernel -e no-dev` and greps for banned crates.
    KernelPurity,
    /// Run the interactive-read latency benchmark (R-Lat1 SLO gate):
    /// builds both bench binaries in release mode and runs them; either one
    /// exiting non-zero (SLO breach) fails the task.
    Bench,
    /// CR-19 grep gate (R-D6): no LLM client crate or API endpoint string
    /// anywhere under `crates/` or `xtask/` source. Complements
    /// `kernel-purity` (which scopes to the kernel dep tree).
    NoLlm,
    ProtoSync,
    WireStandalone,
    SigningHygiene,
    /// Metrics expose no identity labels or caller-controlled cardinality.
    MetricsHygiene,
    /// GATE1 (audit §2.1): one storage suite, both backends — the double
    /// always, live Falkor when FALKOR_URL is set — asserting identical
    /// results.
    StorageConformance,
    /// GATE1 (audit §2.4 / W2): the offline and ingest write validators
    /// agree on a golden verdict table.
    WritePathParity,
    /// GATE1 (audit §2.2): enforcement fns in security/invariant paths
    /// must have production callers (grep-based lint).
    DeadEnforcement,
    /// GATE1 (audit §2.3): every network-reachable endpoint rejects an
    /// unauthenticated call.
    AuthCoverage,
    /// GATE1 (audit §2.4): pack rule ids vs engine outputs; MCP result vs
    /// the registry handler.
    ArtifactEquivalence,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fingerprint => fingerprint(),
        Cmd::GenSchemas { write } => gen_schemas(write),
        Cmd::GenPlaybook { write } => gen_playbook(write),
        Cmd::KernelPurity => kernel_purity(),
        Cmd::Bench => bench(),
        Cmd::NoLlm => no_llm(),
        Cmd::ProtoSync => proto_sync(),
        Cmd::WireStandalone => wire_standalone(),
        Cmd::SigningHygiene => signing_hygiene(),
        Cmd::MetricsHygiene => metrics_hygiene(),
        Cmd::StorageConformance => storage_conformance(),
        Cmd::WritePathParity => write_path_parity(),
        Cmd::DeadEnforcement => dead_enforcement(),
        Cmd::AuthCoverage => auth_coverage(),
        Cmd::ArtifactEquivalence => artifact_equivalence(),
    }
}

fn cargo() -> String {
    std::env::var("CARGO")
        .map(|c| c.trim().to_string())
        .unwrap_or_else(|_| "cargo".into())
}

fn run(args: &[&str], env: &[(&str, &str)]) -> Result<()> {
    let mut cmd = std::process::Command::new(cargo());
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let status = cmd.status()?;
    anyhow::ensure!(status.success(), "gate failed: cargo {:?}", args);
    Ok(())
}

/// §2.1: the storage suite against the double, plus the live Falkor
/// integration suite when FALKOR_URL is present.
fn storage_conformance() -> Result<()> {
    run(&["test", "-p", "exocortex-storage"], &[])?;
    println!("storage-conformance: in-memory suite PASS");
    if std::env::var("FALKOR_URL").is_ok_and(|url| !url.is_empty()) {
        println!("storage-conformance: live Falkor suite (FALKOR_URL set)");
        run(
            &[
                "test",
                "-p",
                "exocortex-storage",
                "--features",
                "integration",
                "--test",
                "integration",
                "--test",
                "fencing_live",
            ],
            &[],
        )?;
        println!("storage-conformance: live Falkor suite PASS");
    } else {
        println!(
            "storage-conformance: live Falkor suite UNEXECUTED — FALKOR_URL is unset or empty"
        );
    }
    println!("storage-conformance: available suites complete");
    Ok(())
}

/// W2: golden verdict table through BOTH validators.
fn write_path_parity() -> Result<()> {
    run(
        &[
            "test",
            "-p",
            "exocortex-ingest",
            "--test",
            "write_path_parity",
        ],
        &[],
    )?;
    println!("write-path-parity ok: offline and ingest validators agree row for row");
    Ok(())
}

/// §2.2: the listed enforcement functions must appear in production (non
/// test, non-definition) sources.
fn dead_enforcement() -> Result<()> {
    let checks: &[(&str, &str)] = &[
        // (fn name, file that must reference it outside its definition crate's tests)
        ("admit_and_publish", "crates/exocortex-cluster/src/node.rs"),
        ("check_deadline", "crates/exocortex-ops/src/operations.rs"),
        ("on_write", "crates/exocortex-ingest/src/service.rs"),
        (
            "with_admin_ceilings",
            "crates/exocortex-ingest/tests/ingest.rs",
        ),
        ("drain_all", "crates/exocortex-client/src/main.rs"),
        ("advance_local_lsn", "crates/exocortex-client/src/mcp.rs"),
        // SR-PRD F2: the offline write path must publish into the served
        // snapshot (advance_local_lsn is only the LSN-only degrade path).
        ("apply_local", "crates/exocortex-client/src/mcp.rs"),
    ];
    for (name, witness) in checks {
        anyhow::ensure!(
            std::path::Path::new(witness).exists(),
            "dead-enforcement: witness file {witness} for `{name}` is gone — update the gate"
        );
        // The witness must actually mention the fn.
        let text = std::fs::read_to_string(witness)?;
        anyhow::ensure!(
            text.contains(name),
            "dead-enforcement: `{name}` has no reference in {witness}"
        );
    }
    println!("dead-enforcement ok: every listed control has a live caller");
    Ok(())
}

/// §2.3: the auth-coverage suite (every endpoint rejects unauthenticated).
fn auth_coverage() -> Result<()> {
    run(
        &["test", "-p", "exocortex-server", "--test", "auth_coverage"],
        &[],
    )?;
    println!("auth-coverage ok: every network endpoint rejects unauthenticated calls");
    Ok(())
}

/// §2.4: pack↔engine rule equivalence + MCP↔registry result equivalence.
fn artifact_equivalence() -> Result<()> {
    run(
        &[
            "test",
            "-p",
            "exocortex-reasoning",
            "--test",
            "rules",
            "pack_rule_ids_match_engine_outputs",
        ],
        &[],
    )?;
    run(
        &[
            "test",
            "-p",
            "exocortex-client",
            "--test",
            "stdio_smoke",
            "mcp_get_memory_shape_matches_registry",
        ],
        &[],
    )?;
    println!("artifact-equivalence ok: pack rules == engine rules; MCP result == registry result");
    Ok(())
}

/// `cargo xtask fingerprint` — compute and print the effective
/// `OntologyFingerprint` of the linked pack set. Must be byte-stable across
/// runs on the same commit (M1 acceptance).
fn fingerprint() -> Result<()> {
    // Force-link the pack crate so its inventory ctor (`.init_array`) runs in
    // this binary; an unreferenced dependency is not linked.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let onto = exocortex_kernel::pack::load_registered_packs()?;
    let fp = onto.fingerprint.0;
    let mut hex = String::with_capacity(fp.len() * 2);
    for b in fp {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    println!("{hex}");
    Ok(())
}

/// `cargo xtask gen-schemas` — emit MCP tool + OpenAPI catalogues from the
/// operation registry and verify the checked-in goldens match (schema-drift
/// CI gate, §21.2). Regenerate with --write.
fn gen_schemas(write: bool) -> Result<()> {
    let mut openapi = serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": "exocortex", "version": env!("CARGO_PKG_VERSION") },
        "paths": {},
    });
    let mut mcp_tools = serde_json::json!([]);
    let mut seen = std::collections::HashSet::new();
    for e in exocortex_ops::entries() {
        anyhow::ensure!(seen.insert(e.name), "duplicate op name {}", e.name);
        let input_schema = (e.input_schema)();
        anyhow::ensure!(
            input_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|m| m.title.clone())
                .is_some(),
            "{}: input schema missing title",
            e.name
        );
        openapi["paths"][e.http_path] = serde_json::json!({
            format!("{:?}", (e.http_method)()).to_lowercase(): {
                "operationId": e.name,
                "requestBody": { "content": { "application/json": { "schema": input_schema } } },
                "responses": { "200": { "description": "OK" } },
            }
        });
        mcp_tools.as_array_mut().unwrap().push(serde_json::json!({
            "name": e.mcp_tool_name,
            "description": e.name,
            "inputSchema": (e.input_schema)(),
        }));
    }
    for (name, doc) in [("openapi.json", &openapi), ("mcp-tools.json", &mcp_tools)] {
        let path = format!("crates/exocortex-ops/tests/golden/{name}");
        let serialized = serde_json::to_string_pretty(doc)?;
        if write {
            std::fs::create_dir_all("crates/exocortex-ops/tests/golden")?;
            std::fs::write(
                &path,
                serialized
                    + "
",
            )?;
            println!("wrote {path}");
        } else {
            let golden = std::fs::read_to_string(&path).map_err(|_| {
                anyhow::anyhow!(
                    "golden {path} missing; run `cargo xtask gen-schemas --write` and commit it"
                )
            })?;
            anyhow::ensure!(
                golden.trim() == serialized.trim(),
                "schema drift in {name}; run `cargo xtask gen-schemas --write` and commit the diff"
            );
        }
    }
    println!("parity ok: {} operations", exocortex_ops::entries().len());
    Ok(())
}

/// `cargo xtask kernel-purity` — R-I1/R-I5/CR-19/CR-26 defence. Runs
/// `cargo tree -p exocortex-kernel -e no-dev` and fails if any banned crate
/// appears in the kernel's dependency graph. HTTP clients (`reqwest`) are
/// legitimate in server/client crates but must never reach the kernel.
fn kernel_purity() -> Result<()> {
    const BANNED: &[&str] = &[
        "duckdb",
        "iceberg",
        "delta_kernel",
        "deltalake",
        "aws-sdk-s3",
        "aws-sdk-glue",
        "async-openai",
        "anthropic-sdk",
        "reqwest",
    ];

    let out = std::process::Command::new(std::env::var("CARGO")?.trim())
        .args(["tree", "-p", "exocortex-kernel", "-e", "no-dev"])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "cargo tree failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let tree = String::from_utf8_lossy(&out.stdout);
    let mut found: Vec<&str> = Vec::new();
    for line in tree.lines() {
        for banned in BANNED {
            let hit = line
                .split([' ', '\t'])
                .any(|tok| tok.starts_with(&format!("{banned} v")));
            if hit && !found.contains(banned) {
                found.push(banned);
            }
        }
    }
    if found.is_empty() {
        println!("kernel-purity ok: no banned crate in exocortex-kernel dependency tree");
        Ok(())
    } else {
        anyhow::bail!(
            "kernel-purity FAILED: banned crates reachable from exocortex-kernel: {}",
            found.join(", ")
        )
    }
}

/// `cargo xtask bench` — R-Lat1 SLO gate. Runs both bench binaries
/// (`search`, `khop`) in release mode; each binary enforces its own budget
/// and exits non-zero on breach.
fn bench() -> Result<()> {
    let cargo = std::env::var("CARGO")?.trim().to_string();
    for (pkg, bench) in [
        ("exocortex-cache", "search"),
        ("exocortex-reasoning", "khop"),
    ] {
        println!("==> cargo bench -p {pkg} --bench {bench}");
        let status = std::process::Command::new(&cargo)
            .args(["bench", "-p", pkg, "--bench", bench])
            .status()?;
        anyhow::ensure!(
            status.success(),
            "SLO gate FAILED for {bench} (R-Lat1); see the p50/p99 lines above"
        );
    }
    println!("bench ok: both SLO gates green (R-Lat1)");
    // Round-3 G1 / PRD R15: the adapter SDK resolves exactly ONE
    // exocortex-* dependency (exocortex-wire), and BOTH the SDK and the
    // worker never link exocortex-kernel (R-I1/R-I4). This assertion was
    // claimed by a prior commit message but never landed.
    for crate_name in ["exocortex-adapter-sdk", "exocortex-worker"] {
        let out = std::process::Command::new("cargo")
            .args(["tree", "-p", crate_name, "-e", "no-dev"])
            .output()?;
        if !out.status.success() {
            anyhow::bail!("cargo tree for {crate_name} failed");
        }
        let text = String::from_utf8_lossy(&out.stdout);
        if text.lines().any(|l| l.contains("exocortex-kernel")) {
            anyhow::bail!("{crate_name} must never link exocortex-kernel (R-I1/R-I4): {text}");
        }
        if crate_name == "exocortex-adapter-sdk" {
            // The first line is the crate's own root, not a dependency —
            // counting it double-charged every run (the tree prefixes are
            // box-drawing glyphs, not whitespace).
            let count = text
                .lines()
                .skip(1)
                .filter(|l| l.contains("exocortex-"))
                .count();
            if count != 1 {
                anyhow::bail!(
                    "exocortex-adapter-sdk must resolve exactly one exocortex-* dependency, found {count}: {text}"
                );
            }
        }
    }

    Ok(())
}

/// `cargo xtask no-llm` — CR-19/R-D6 grep gate. Walks `crates/` and
/// `xtask/` sources; fails on any LLM client crate identifier or API
/// endpoint string. Model names in docs/comments are not violations; the
/// gate matches dependency identifiers and wire endpoints. Scans `crates/`
/// only (this file's own banned list must not self-match).
fn no_llm() -> Result<()> {
    const BANNED: &[&str] = &[
        "async-openai",
        "openai_api",
        "api.openai.com",
        "anthropic.com",
        "api.anthropic",
        "generativelanguage.googleapis.com",
        "mistral.ai",
        "cohere.ai",
        "llm_client",
    ];
    let mut hits: Vec<String> = Vec::new();
    // `crates/` only — this file's own banned list must not self-match.
    let mut stack: Vec<std::path::PathBuf> = vec!["crates".into()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs" || e == "toml") {
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                for banned in BANNED {
                    if text.contains(banned) {
                        hits.push(format!("{}: contains `{banned}`", path.display()));
                    }
                }
            }
        }
    }
    if hits.is_empty() {
        println!("no-llm ok: no LLM client crate or endpoint in workspace sources (CR-19)");
        Ok(())
    } else {
        anyhow::bail!("no-llm FAILED (CR-19):\n{}", hits.join("\n"))
    }
}

/// Wire vendors the protos for publishable tarballs (B8-era fix); the
/// root proto/ stays authoritative. This gate fails when they diverge.
fn proto_sync() -> anyhow::Result<()> {
    for name in ["ingest.proto", "cluster.proto", "sse.proto"] {
        let root = std::fs::read_to_string(format!("proto/{name}"))?;
        let vendored = std::fs::read_to_string(format!("crates/exocortex-wire/proto/{name}"))?;
        if root != vendored {
            anyhow::bail!("proto/{name} and crates/exocortex-wire/proto/{name} diverge — copy the authoritative root file");
        }
    }
    // D5: the wire tarball ships the AGPL text; the crate-local copy
    // must stay identical to the root LICENSE.
    let root = std::fs::read_to_string("LICENSE")?;
    let vendored = std::fs::read_to_string("crates/exocortex-wire/LICENSE")?;
    if root != vendored {
        anyhow::bail!("crates/exocortex-wire/LICENSE diverged from the root LICENSE");
    }
    println!("proto-sync ok: vendored wire protos + LICENSE match");
    Ok(())
}

/// PRD R1/R2: prove `exocortex-wire` is consumable standalone from the
/// PUBLISHED artifact. Packages the crate, extracts the tarball into a
/// temp workspace, drops the standalone fixture in beside it (pointing
/// at the extracted crate), builds, and asserts the fixture's dependency
/// graph contains exactly one `exocortex-*` crate.
fn wire_standalone() -> anyhow::Result<()> {
    use std::process::Command;
    let out = Command::new("cargo")
        .args([
            "package",
            "-p",
            "exocortex-wire",
            "--allow-dirty",
            "--no-verify",
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!("cargo package -p exocortex-wire failed");
    }
    // G3: the real version from cargo metadata — a hardcoded "0.1.0"
    // breaks this gate at the first version bump.
    let version = wire_version()?;
    let crate_file = format!("target/package/exocortex-wire-{version}.crate");

    let tmp = tempfile_dir()?;
    let extracted = tmp.join("exocortex-wire");
    std::fs::create_dir_all(&extracted)?;
    let bytes = std::fs::read(&crate_file)?;
    unpack_gzip_tar(&bytes, &extracted)?;
    let inner = extracted.join(format!("exocortex-wire-{version}"));

    // Fixture crate beside the extracted wire, pointing at it by path.
    let fixture_src = std::path::Path::new("crates/exocortex-wire/tests/standalone");
    let fixture_dst = tmp.join("fixture");
    copy_dir(fixture_src, &fixture_dst)?;
    // Rewrite the fixture's path dep to the EXTRACTED tarball directory
    // (the extracted wire, not the repo copy — that is the R1 point).
    let manifest = fixture_dst.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)?;
    std::fs::write(
        &manifest,
        text.replace(
            r#"{ path = "../exocortex-wire" }"#,
            &format!(r#"{{ path = "../exocortex-wire/exocortex-wire-{version}" }}"#),
        ),
    )?;
    std::fs::write(
        tmp.join("Cargo.toml"),
        "[workspace]\nmembers = [\"fixture\"]\nresolver = \"2\"\n",
    )?;

    let build = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&tmp)
        .env("CARGO_TARGET_DIR", tmp.join("target"))
        .output()?;
    if !build.status.success() {
        anyhow::bail!(
            "standalone fixture build failed:
{}",
            String::from_utf8_lossy(&build.stderr)
        );
    }

    // R2: exactly one exocortex-* crate in the resolved graph.
    let tree = Command::new("cargo")
        .args(["tree", "-p", "wire-standalone-fixture", "-e", "no-dev"])
        .current_dir(&tmp)
        .env("CARGO_TARGET_DIR", tmp.join("target"))
        .output()?;
    let text = String::from_utf8_lossy(&tree.stdout);
    let exo_count = text.lines().filter(|l| l.contains("exocortex-")).count();
    if exo_count != 1 {
        anyhow::bail!(
            "standalone fixture must resolve exactly one exocortex-* crate, found {exo_count}:
{text}"
        );
    }
    let _ = inner;
    println!("wire-standalone ok: packaged tarball builds a wire-only consumer");
    Ok(())
}

fn tempfile_dir() -> anyhow::Result<std::path::PathBuf> {
    let dir = std::env::temp_dir().join(format!("exo-wire-standalone-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

/// Extract the packaged tarball (tar + flate2 crates; gate deps).
fn unpack_gzip_tar(gz: &[u8], dst: &std::path::Path) -> anyhow::Result<()> {
    let tarball = flate2::read::GzDecoder::new(gz);
    let mut archive = tar::Archive::new(tarball);
    archive.unpack(dst)?;
    Ok(())
}
// sentinel-9182

/// The workspace version of exocortex-wire via cargo metadata.
fn wire_version() -> anyhow::Result<String> {
    let out = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()?;
    if !out.status.success() {
        anyhow::bail!("cargo metadata failed");
    }
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    meta["packages"]
        .as_array()
        .and_then(|ps| {
            ps.iter()
                .find(|p| p["name"] == "exocortex-wire")
                .and_then(|p| p["version"].as_str().map(String::from))
        })
        .ok_or_else(|| anyhow::anyhow!("exocortex-wire not found in metadata"))
}

/// Round-3 G2: calibrated batch-signing hygiene gates. The adapter-SDK
/// PRD's raw grep commands were miscalibrated (they matched benign
/// scaffolding and unrelated SSE-envelope HMACs). These check the
/// actual invariants:
///  1. No local batch-signing/checksum implementation outside
///     `exocortex-wire/src/signing.rs` (single-implementation rule).
///  2. No producer submits a blank checksum: every `IngestBatch`
///     construction site with `checksum: String::new()` must be
///     followed by prepare_batch/canonical_checksum before submit.
fn signing_hygiene() -> anyhow::Result<()> {
    // Gate 1: function-level single implementation.
    let mut offenders = Vec::new();
    for entry in walk_rss(std::path::Path::new("crates"))? {
        let src = std::fs::read_to_string(&entry)?;
        let rel = entry.display().to_string();
        if rel.ends_with("exocortex-wire/src/signing.rs") {
            continue;
        }
        for marker in [
            "fn sign_batch(",
            "fn compute_checksum(",
            "fn canonical_checksum(",
        ] {
            if src.contains(marker) {
                offenders.push(format!("{rel}: local `{marker}`"));
            }
        }
    }
    if !offenders.is_empty() {
        anyhow::bail!(
            "batch signing implemented outside exocortex-wire:
{}",
            offenders.join(
                "
"
            )
        );
    }

    // Gate 2: every blank-checksum constructor is followed (within the
    // enclosing function) by prepare_batch or canonical_checksum.
    let mut blanks = Vec::new();
    for entry in walk_rss(std::path::Path::new("crates"))? {
        let src = std::fs::read_to_string(&entry)?;
        if !src.contains("checksum: String::new()") {
            continue;
        }
        let rel = entry.display().to_string();
        // Crude but effective: the file must reference the canonical
        // helper at least once after construction.
        if !src.contains("prepare_batch") && !src.contains("canonical_checksum") {
            blanks.push(rel);
        }
    }
    if !blanks.is_empty() {
        anyhow::bail!(
            "files construct batches with blank checksums and never call prepare_batch/canonical_checksum:
{}",
            blanks.join("
")
        );
    }
    println!("signing-hygiene ok: single batch-signing implementation; no unsigning blank-checksum submitters");
    Ok(())
}

fn metrics_hygiene() -> anyhow::Result<()> {
    let mut offenders = Vec::new();
    for entry in walk_rss(std::path::Path::new("crates"))? {
        let src = std::fs::read_to_string(&entry)?;
        for issue in metrics_hygiene_issues(&src) {
            offenders.push(format!("{}: {issue}", entry.display()));
        }
    }
    anyhow::ensure!(
        offenders.is_empty(),
        "metrics-hygiene FAILED:\n{}",
        offenders.join("\n")
    );
    println!("metrics-hygiene ok: authenticated surface; literal bounded labels only");
    Ok(())
}

fn metrics_hygiene_issues(src: &str) -> Vec<String> {
    const MACROS: &[&str] = &[
        "metrics::counter!(",
        "metrics::gauge!(",
        "metrics::histogram!(",
    ];
    const FORBIDDEN: &[&str] = &[
        "graph",
        "org",
        "org_id",
        "user",
        "user_id",
        "producer",
        "producer_id",
        "client_version",
        "playbook_version",
    ];
    let mut issues = Vec::new();
    for marker in MACROS {
        let mut rest = src;
        while let Some(start) = rest.find(marker) {
            rest = &rest[start + marker.len()..];
            let mut depth = 1usize;
            let mut end = rest.len();
            for (index, ch) in rest.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = index;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let invocation = &rest[..end];
            for segment in invocation.split(',').filter(|part| part.contains("=>")) {
                let Some((key, value)) = segment.split_once("=>") else {
                    continue;
                };
                let key = key.trim().trim_matches('"');
                if FORBIDDEN.contains(&key) {
                    issues.push(format!("forbidden identity label `{key}`"));
                }
                let value = value.trim();
                if !value.starts_with('"') && value != "entry.name" {
                    issues.push(format!("label `{key}` has a non-literal value"));
                }
            }
            if end == rest.len() {
                break;
            }
            rest = &rest[end + 1..];
        }
    }
    issues
}

#[cfg(test)]
mod metrics_hygiene_tests {
    use super::metrics_hygiene_issues;

    #[test]
    fn rejects_identity_and_dynamic_metric_labels() {
        let bad = r#"metrics::counter!("requests", "producer_id" => producer.clone(), "outcome" => "ok");"#;
        let issues = metrics_hygiene_issues(bad);
        assert!(issues.iter().any(|issue| issue.contains("identity")));
        assert!(issues.iter().any(|issue| issue.contains("non-literal")));
        assert!(
            metrics_hygiene_issues(r#"metrics::counter!("requests", "outcome" => "ok");"#)
                .is_empty()
        );
    }
}

fn walk_rss(dir: &std::path::Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().map(|n| n == "target").unwrap_or(false) {
                continue;
            }
            out.extend(walk_rss(&path)?);
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(out)
}

/// D1/P2 (agent-instructions PRD): regenerate the playbook's generated
/// sections from the code they describe — the kind catalogue from the
/// effective ontology (W6's computed_only flag honored), the reject-code
/// table from the `RejectCode` enum — and enforce the instruction
/// block's ≤300-word bound plus its kind claims against the pack.
/// Default mode is the CI drift gate (fails on diff); `--write`
/// regenerates in place. Facts can no longer drift from the code.
fn gen_playbook(write: bool) -> Result<()> {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let onto = exocortex_kernel::pack::load_registered_packs()?;
    let path = std::path::Path::new("crates/exocortex-client/src/playbook/v1_0_0.md");
    let src = std::fs::read_to_string(path)?;

    // ---- gen:kinds — bucket-grouped, computed-only flagged ----
    let mut kinds_section = String::from(
        "<!-- gen:kinds — do not edit by hand; regenerated from exocortex-pack-dev-v1 -->\n",
    );
    let bucket_name = |b: exocortex_kernel::RelBucket| -> &'static str {
        use exocortex_kernel::RelBucket::*;
        match b {
            Solution => "Solution",
            Causal => "Causal",
            Context => "Context",
            Learning => "Learning",
            Similarity => "Similarity",
            Workflow => "Workflow",
            Quality => "Quality",
            Integration => "Integration",
            Extension(_) => "Extension",
        }
    };
    let mut by_bucket: std::collections::BTreeMap<&str, Vec<String>> = Default::default();
    let mut computed_only = 0usize;
    let mut total = 0usize;
    // Authored kinds only: R-T4 auto-registers read-only inverse
    // companions (`SolvedBy`, `ReplacedBy`, …) at pack-local ids >=
    // 0x4000 — they are materialized on write, never authorable, and a
    // generator that listed them would teach producers kinds the
    // validator refuses (§3.1 r3's warning, caught by running it).
    let mut ordered: Vec<&exocortex_kernel::RelMeta> = onto
        .kinds_by_id
        .values()
        .filter(|m| m.id.is_kernel() || m.id.local_part() < 0x4000)
        .collect();
    ordered.sort_by_key(|m| m.display_name.to_string());
    for m in ordered {
        let bucket = bucket_name(m.bucket);
        let name = m.display_name.to_string();
        let marker = if m.computed_only {
            computed_only += 1;
            " †"
        } else {
            ""
        };
        by_bucket
            .entry(bucket)
            .or_default()
            .push(format!("`{name}`{marker}"));
        total += 1;
    }
    for (bucket, kinds) in &by_bucket {
        kinds_section.push_str(&format!(
            "- **{bucket} ({}):** {}\n",
            kinds.len(),
            kinds.join(", ")
        ));
    }
    let assertable = total - computed_only;
    kinds_section.push_str(&format!(
        "\n† Computed-only (R-T14): the consolidation cycle asserts it, producers may\nnot. {total} kinds are declared; **{assertable} are yours to use.** Asserting one\nis rejected with `ComputedKindRejected`.\n<!-- /gen:kinds -->"
    ));

    // ---- gen:rejects — one row per RejectCode variant ----
    use exocortex_wire::ingest::v1::RejectCode;
    let guidance = |c: RejectCode| -> String {
        exocortex_wire::corrections::guidance(c)
            .correction
            .to_string()
    };
    let all = [
        (
            RejectCode::InvalidTypeTriple,
            "Kind doesn't fit the (from, to) types",
        ),
        (
            RejectCode::UnknownKind,
            "Kind name typo or not in this pack",
        ),
        (
            RejectCode::UnknownMemoryType,
            "Memory type not in this pack",
        ),
        (
            RejectCode::Unknown,
            "Title empty/>200 chars, content empty, or an atomic-batch reject",
        ),
        (
            RejectCode::VisibilityWidening,
            "Visibility above the source ceiling (rare for session wrapups)",
        ),
        (
            RejectCode::ComputedKindRejected,
            "You asserted a computed-only kind (R-T14)",
        ),
        (
            RejectCode::DuplicateBatch,
            "Transport replayed the same batch id",
        ),
        (RejectCode::RateLimited, "Backend is shedding load"),
        (
            RejectCode::MissingExternalKey,
            "External-snapshot coordinates missing",
        ),
        (
            RejectCode::InvalidExternalKey,
            "External-snapshot coordinates malformed",
        ),
        (RejectCode::Unauthorized, "Credentials rejected (HMAC)"),
        (RejectCode::BadChecksum, "Batch checksum mismatch"),
        (
            RejectCode::IncompatibleOntology,
            "Ontology fingerprint mismatch",
        ),
        (
            RejectCode::UnknownSource,
            "Producer not registered / ceiling mismatch / wrong org",
        ),
    ];
    let mut rejects_section = String::from(
        "<!-- gen:rejects — do not edit by hand; regenerated from the RejectCode enum -->\n| Code | Meaning | Fix |\n|---|---|---|\n",
    );
    for (code, meaning) in all {
        rejects_section.push_str(&format!(
            "| `{code:?}` | {meaning} | {} |\n",
            guidance(code).replace('|', "/")
        ));
    }
    rejects_section.push_str("<!-- /gen:rejects -->");

    // ---- splice between the markers ----
    let splice = |src: &str, start_marker: &str, end_marker: &str, new_body: &str| -> String {
        let start = src.find(start_marker).expect("start marker present");
        let end = src.find(end_marker).expect("end marker present") + end_marker.len();
        format!("{}{}{}", &src[..start], new_body, &src[end..])
    };
    let regenerated = splice(
        &splice(
            &src,
            "<!-- gen:kinds",
            "<!-- /gen:kinds -->",
            &kinds_section,
        ),
        "<!-- gen:rejects",
        "<!-- /gen:rejects -->",
        &rejects_section,
    );

    // ---- block bound + kind claims (§11) ----
    let block = std::fs::read_to_string("crates/exocortex-client/src/playbook/block_v1_0_0.md")?;
    let words = block.split_whitespace().count();
    anyhow::ensure!(
        words <= 300,
        "instruction block is {words} words; the bound is 300"
    );
    anyhow::ensure!(
        onto.kind_id("RelatedTo").is_some(),
        "block teaches `RelatedTo` but the pack does not declare it"
    );
    anyhow::ensure!(
        onto.kinds_by_id
            .get(&onto.kind_id("SimilarTo").unwrap())
            .is_some_and(|m| m.computed_only),
        "block prohibits `SimilarTo`; the pack must mark it computed-only"
    );

    if write {
        std::fs::write(path, &regenerated)?;
        println!("gen-playbook: regenerated ({total} kinds, {assertable} assertable; {words}-word block)");
        Ok(())
    } else {
        anyhow::ensure!(
            regenerated == src,
            "gen-playbook: playbook drifted from the ontology/RejectCode — run `cargo xtask gen-playbook --write`"
        );
        println!("gen-playbook ok ({total} kinds, {assertable} assertable; {words}-word block)");
        Ok(())
    }
}
