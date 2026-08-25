//! `exocortex-worker` — the out-of-process adapter host (§18.2). Links
//! `exocortex-wire` ONLY (never the kernel, R-I1); adapters arrive in v2;
//! v1 ships the no-op host that parses `--adapter <name>` and pumps an empty
//! frame loop. M6 AC: `--adapter noop` starts cleanly WITHOUT a live
//! backend — the channel connects lazily and retries in the background;
//! real (v2) adapters keep the hard `--backend` connect.

use clap::Parser;

/// Adapter host options.
#[derive(Debug, Parser)]
#[command(name = "exocortex-worker", version)]
struct Args {
    /// Adapter name (v1: `noop` only).
    #[arg(long, default_value = "noop")]
    adapter: String,
    /// Path to the adapter configuration file.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// Backend IngestService endpoint.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    backend: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    if args.adapter != "noop" {
        anyhow::bail!("unknown adapter `{}` (v1 ships noop only)", args.adapter);
    }
    // Deliberately no exocortex-kernel usage here (R-I1). The noop pump
    // never submits, so the backend channel is lazy: connect lazily, probe
    // in the background, and stay idle when unreachable (M6 AC — the worker
    // must start without a live backend).
    let endpoint = tonic::transport::Endpoint::from_shared(args.backend.clone())
        .map_err(|e| anyhow::anyhow!("bad --backend {}: {e}", args.backend))?;
    let _channel = endpoint.connect_lazy();
    tracing::info!(adapter = %args.adapter, backend = %args.backend, "exocortex-worker ready (no-op, lazy backend)");
    // Idle pump: nothing to submit until a real adapter loads (v2).
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        tracing::debug!("noop frame tick");
    }
}
