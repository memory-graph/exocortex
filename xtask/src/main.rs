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
    /// Run the interactive-read latency benchmark (R-Lat1 SLO gate).
    Bench,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fingerprint => fingerprint(),
        Cmd::GenSchemas { write } => gen_schemas(write),
        Cmd::KernelPurity => kernel_purity(),
        Cmd::Bench => todo!(),
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
