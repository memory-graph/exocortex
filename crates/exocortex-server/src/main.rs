//! `exocortex-node` — the single-artifact node binary (§4.2).
//!
//! `--mode mcp-standalone`: local, no backend; process-local FalkorDB via the
//! supervisor (§4.3). `--mode backend-node` / `--mode embedded` land with M5+.

mod supervisor;

use exocortex_server::backend;

use std::net::SocketAddr;

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
    /// Chitchat gossip listen address (backend-node).
    #[arg(long, default_value = "0.0.0.0:8100")]
    gossip_addr: String,
    /// Bearer token guarding the HTTP op surface (R-Sec7). No default in
    /// release builds: backend-node refuses to start without it (a shipped
    /// credential is worse than a startup error). Debug builds keep the
    /// loopback dev token for local iteration.
    #[arg(long)]
    bearer_token: Option<String>,
    /// Cluster-shared HMAC secret (64 hex chars; defaults to a dev key).
    #[arg(long)]
    cluster_secret: Option<String>,
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
        Mode::BackendNode => backend_node_main(args),
        Mode::Embedded => {
            anyhow::bail!("--mode embedded is the in-process path used by tests");
        }
    }
}

/// `--mode backend-node` (M5): storage + cluster + ingest + HTTP + SSE +
/// gossip + lease re-election on one process.
fn backend_node_main(args: Args) -> anyhow::Result<()> {
    use std::str::FromStr;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let ontology =
            std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);
        let storage = if let Some(url) = args.storage.strip_prefix("falkor://") {
            std::sync::Arc::new(
                exocortex_storage::FalkorStorage::connect(
                    exocortex_storage::FalkorConfig {
                        falkor_url: format!("falkor://{url}"),
                        redis_url: format!("redis://{url}"),
                        graph_name: format!("exocortex-node-{}", std::process::id()),
                        org_id: "org".into(),
                        node_id: format!("node-{}", std::process::id()).into(),
                    },
                    ontology.clone(),
                )
                .await?,
            )
        } else {
            anyhow::bail!(
                "backend-node needs --storage=falkor://host:port (embedded storage is mcp-standalone)"
            );
        };
        let cluster_secret = args
            .cluster_secret
            .as_deref()
            .and_then(|hex| decode_hex32(hex).ok())
            .unwrap_or([0x42u8; 32]);
        let bearer_token = resolve_bearer(&args)?;
        let node_args = backend::BackendNodeArgs {
            bind: args.bind.clone(),
            node_id: format!("node-{}", std::process::id()),
            cluster_secret,
            bearer_token,
            gossip_listen: SocketAddr::from_str(&args.gossip_addr)
                .map_err(|e| anyhow::anyhow!("bad --gossip-addr: {e}"))?,
            seed_nodes: args
                .cluster_endpoints
                .map(|eps| eps.split(',').map(str::to_string).collect())
                .unwrap_or_default(),
        };
        let node = backend::run_backend_node(storage, ontology, node_args).await?;
        tracing::info!(addr = %node.local_addr, "backend-node up; serving until interrupted");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    })
}

/// Decode 64 hex chars into a 32-byte key.
fn decode_hex32(hex: &str) -> Result<[u8; 32], anyhow::Error> {
    let bytes = (0..32)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<Vec<_>, _>>()?;
    bytes.try_into().map_err(|_| anyhow::anyhow!("bad hex"))
}

/// R-Sec7: the op surface never ships a default credential. Release
/// builds fail fast when `--bearer-token` is absent; debug builds fall
/// back to the loopback dev token with a loud warning.
fn resolve_bearer(args: &Args) -> anyhow::Result<String> {
    match &args.bearer_token {
        Some(t) => Ok(t.clone()),
        None => {
            #[cfg(debug_assertions)]
            {
                tracing::warn!(
                    "--bearer-token absent; using the DEBUG-ONLY dev token (never in release)"
                );
                Ok("exocortex-dev-bearer".to_string())
            }
            #[cfg(not(debug_assertions))]
            {
                anyhow::bail!("--bearer-token is required in release builds (R-Sec7: no default credential)")
            }
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
