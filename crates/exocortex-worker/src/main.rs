//! `exocortex-worker` — the out-of-process adapter host (§18.2). Links
//! `exocortex-wire` ONLY (never the kernel, R-I1); adapters arrive in v2;
//! v1 ships the no-op host that parses `--adapter <name>` and pumps an empty
//! frame loop.

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
    // Deliberately no exocortex-kernel usage here (R-I1). The pump connects
    // to the IngestService but submits nothing until a real adapter loads.
    let _client = exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::connect(
        args.backend.clone(),
    )
    .await?;
    tracing::info!(adapter = %args.adapter, backend = %args.backend, "exocortex-worker ready (no-op)");
    Ok(())
}
