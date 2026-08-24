//! `exocortex-node` — the single-artifact node binary (§4.2).
//!
//! `--mode mcp-standalone`: local, no backend; process-local FalkorDB via the
//! supervisor (§4.3). `--mode backend-node` / `--mode embedded` land with M5+.

mod supervisor;

use clap::Parser;

/// Node deployment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    /// Local single-user server with supervised embedded storage.
    McpStandalone,
    /// Cluster peer (M5+).
    BackendNode,
    /// In-process library/tests (M5+).
    Embedded,
}

/// Node options (§4.2).
#[derive(Debug, Parser)]
#[command(name = "exocortex-node", version)]
struct Args {
    /// Deployment mode.
    #[arg(long, value_enum, default_value = "mcp-standalone")]
    mode: Mode,
    /// Storage selection: embedded falkordb or a networked URL.
    #[arg(long, default_value = "falkordb-embedded")]
    storage: String,
    /// Bind address for networked modes.
    #[arg(long, default_value = ":8080")]
    bind: String,
    /// Cluster seed endpoints (backend-node).
    #[arg(long)]
    cluster_endpoints: Option<String>,
    /// redis-server binary for the embedded supervisor.
    #[arg(long)]
    redis_server_bin: Option<std::path::PathBuf>,
    /// FalkorDB module path for the embedded supervisor.
    #[arg(long)]
    falkordb_module: Option<std::path::PathBuf>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    // Force-link the pack so its inventory registration runs in this binary.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let _ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);

    match args.mode {
        Mode::McpStandalone => {
            if args.storage != "falkordb-embedded" {
                anyhow::bail!("mcp-standalone supports --storage=falkordb-embedded only");
            }
            let (bin, module) = supervisor::resolve_paths(
                args.redis_server_bin.clone(),
                args.falkordb_module.clone(),
            )?;
            let port = supervisor::free_port()?;
            let cfg = supervisor::SupervisorConfig {
                redis_server_bin: bin,
                falkordb_module: module,
                data_dir: data_home()?,
                port,
                max_restarts: 3,
            };
            let server = supervisor::spawn_supervised(&cfg)?;
            tracing::info!(port = server.port, "exocortex-node mcp-standalone ready");
            // Serve until interrupted; the supervisor owns the child lifetime.
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
        Mode::BackendNode | Mode::Embedded => {
            anyhow::bail!("--mode {:?} arrives with M5 (cluster)", args.mode);
        }
    }
}

/// Data dir under the user's data home (§4.3).
fn data_home() -> anyhow::Result<std::path::PathBuf> {
    let home = std::env::var("HOME").map_err(|_| anyhow::anyhow!("no HOME"))?;
    let dir = if cfg!(target_os = "macos") {
        std::path::Path::new(&home)
            .join("Library")
            .join("Application Support")
            .join("exocortex")
    } else {
        std::path::Path::new(&home)
            .join(".local")
            .join("share")
            .join("exocortex")
    };
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
