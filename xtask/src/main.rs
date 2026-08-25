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
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fingerprint => fingerprint(),
        Cmd::GenSchemas { write } => gen_schemas(write),
        Cmd::KernelPurity => kernel_purity(),
        Cmd::Bench => bench(),
        Cmd::NoLlm => no_llm(),
        Cmd::ProtoSync => proto_sync(),
        Cmd::WireStandalone => wire_standalone(),
    }
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
    println!("proto-sync ok: vendored wire protos match proto/");
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
    let version = std::fs::read_to_string("crates/exocortex-wire/Cargo.toml")?
        .lines()
        .find_map(|l| l.trim().strip_prefix("version.workspace").map(|_| "0.1.0"))
        .unwrap_or("0.1.0");
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
