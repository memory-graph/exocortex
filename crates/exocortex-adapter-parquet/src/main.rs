//! D1: the parquet-directory adapter binary. Scans a directory of
//! Parquet files under a declared column mapping, and submits through
//! the signed Ingestion Protocol under the `parquet-dir` source flavor
//! (the server enforces the declared projection, the schema hash, and
//! rewind). Secrets come from the environment (never argv):
//! `EXOCORTEX_AUTH_TOKEN` (bearer) and `EXOCORTEX_HMAC_KEY` (64 hex).
//!
//! `--validate` performs the local check only (mapping shape, column
//! presence, row counts, run bound) — no backend, no secrets — for CI
//! against a docs/table fixture the way the Mintlify adapter's
//! `validate` does for pages.

use clap::Parser;

/// Import a directory of Parquet files into the exocortex graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-parquet", version)]
struct Args {
    /// Directory containing the .parquet files (one directory is one table).
    #[arg(long)]
    dir: std::path::PathBuf,
    /// Column-mapping JSON (see Mapping).
    #[arg(long)]
    mapping: std::path::PathBuf,
    /// Local check only: validate the mapping against the directory,
    /// print the counts, touch no backend. Exits non-zero on any
    /// problem.
    #[arg(long, default_value_t = false)]
    validate: bool,
    /// Backend IngestService base URL.
    #[arg(long)]
    backend: Option<String>,
    /// Owning org.
    #[arg(long)]
    org: Option<String>,
    /// Producer identity for registration.
    #[arg(long, default_value = "parquet-adapter")]
    producer: String,
    /// Stable table identity for external keys (scopes row identity
    /// across runs; the operator pins it once).
    #[arg(long)]
    table_id: Option<String>,
    /// Durable cursor file (stores the last ingested file-set hash).
    #[arg(long, default_value = "parquet-adapter.cursor")]
    cursor: std::path::PathBuf,
    /// Maximum rows per submit window (D21-a bound).
    #[arg(long, default_value = "256")]
    max_window: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let mapping = exocortex_adapter_parquet::Mapping::load(&args.mapping)?;
    let scan = exocortex_adapter_parquet::scan_directory(&args.dir)?;
    exocortex_adapter_parquet::validate_mapping(&mapping, &scan)?;
    let declared = exocortex_adapter_parquet::declared_columns(&mapping, &scan);
    let (rows, skipped) = exocortex_adapter_parquet::read_rows(&args.dir, &mapping)?;
    if skipped > 0 {
        tracing::warn!(skipped, "rows without a usable pk skipped");
    }
    let max_run = args.max_window.saturating_mul(100);
    if rows.len() as u64 > max_run {
        anyhow::bail!(
            "{} rows exceed the declared max_rows_per_run {} — narrow the projection (a directory is a bounded import, not a firehose)",
            rows.len(),
            max_run
        );
    }
    tracing::info!(
        files = scan.files.len(),
        rows = rows.len(),
        snapshot = %scan.file_set_hash,
        "directory scanned"
    );

    if args.validate {
        println!(
            "ok: {} files, {} rows, {} columns mapped (snapshot {})",
            scan.files.len(),
            rows.len(),
            declared.len(),
            scan.file_set_hash
        );
        return Ok(());
    }

    // An unchanged file set is a no-op: the cursor holds the last
    // settled snapshot id, so an unchanged directory never touches the
    // wire.
    let last = std::fs::read_to_string(&args.cursor)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if last.as_deref() == Some(scan.file_set_hash.as_str()) {
        println!(
            "nothing to ingest (file set unchanged, snapshot {})",
            scan.file_set_hash
        );
        return Ok(());
    }

    let backend = args.backend.clone().unwrap_or_default();
    let org = args.org.clone().unwrap_or_default();
    if backend.is_empty() || org.is_empty() {
        anyhow::bail!("--backend and --org are required unless --validate is set");
    }
    let auth_token = std::env::var("EXOCORTEX_AUTH_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("EXOCORTEX_AUTH_TOKEN is required"))?;
    let hmac_key = exocortex_wire::signing::decode_hex32(
        &std::env::var("EXOCORTEX_HMAC_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("EXOCORTEX_HMAC_KEY is required (64 hex chars)"))?,
    )
    .map_err(anyhow::Error::msg)?;

    let table_id = args
        .table_id
        .clone()
        .unwrap_or_else(|| args.dir.to_string_lossy().trim_end_matches('/').to_string());

    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        &org,
        &format!("parquet-dir://{table_id}"),
        &args.producer,
        &backend,
    );
    // The honest flavor: table-shaped, so the server enforces the
    // declared projection, the schema hash, and rewind on THIS source.
    config.source_flavor = "parquet-dir".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::Custom;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    config.projection = Some(exocortex_adapter_parquet::projection(
        &args.dir.to_string_lossy(),
        &mapping,
        &scan,
        args.max_window,
    ));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    let table = exocortex_adapter_parquet::table_uuid_for(&table_id);
    for (index, chunk) in rows.chunks(args.max_window as usize).enumerate() {
        let (unit, skipped_parents) = exocortex_adapter_parquet::map_rows(
            &mapping,
            &table,
            &declared,
            chunk,
            &format!("window-{index}"),
        );
        if skipped_parents > 0 {
            tracing::warn!(skipped_parents, "parent links outside this window skipped");
        }
        let unit = exocortex_adapter_parquet::with_snapshot_id(unit, &scan.file_set_hash);
        let outcome = session
            .submit_window(vec![unit], &scan.file_set_hash)
            .await?;
        tracing::info!(
            accepted = outcome.accepted,
            duplicates = outcome.duplicates,
            rejected = outcome.permanent_rejections.len(),
            cursor = %scan.file_set_hash,
            "window settled"
        );
        for rejection in &outcome.permanent_rejections {
            tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
        }
    }
    println!(
        "ingested {} rows from {} files (snapshot {})",
        rows.len(),
        scan.files.len(),
        scan.file_set_hash
    );
    Ok(())
}
