//! D1: the Iceberg-table adapter binary. Scans a local Iceberg
//! table's current snapshot under a declared column mapping, and
//! submits through the signed Ingestion Protocol under the `iceberg`
//! source flavor (the server enforces the declared projection, the
//! canonical schema hash, and rewind). Secrets come from the
//! environment (never argv): `EXOCORTEX_AUTH_TOKEN` (bearer) and
//! `EXOCORTEX_HMAC_KEY` (64 hex).
//!
//! `--validate` performs the local check only (mapping shape, column
//! presence, transform discipline, row counts, run bound) — no
//! backend, no secrets — for CI against a table fixture the way the
//! Mintlify adapter's `validate` does for pages.

use clap::Parser;

/// Import a local Iceberg table's current snapshot into the exocortex graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-iceberg", version)]
struct Args {
    /// Root directory of the Iceberg table (the one containing metadata/).
    #[arg(long)]
    table: std::path::PathBuf,
    /// Column-mapping JSON (see exocortex_adapter_table::Mapping).
    #[arg(long)]
    mapping: std::path::PathBuf,
    /// Local check only: validate the mapping against the table,
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
    #[arg(long, default_value = "iceberg-adapter")]
    producer: String,
    /// Durable cursor file (stores the last ingested snapshot id).
    #[arg(long, default_value = "iceberg-adapter.cursor")]
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

    let mapping = exocortex_adapter_table::Mapping::load(&args.mapping)?;
    let scan = exocortex_adapter_iceberg::scan_table(&args.table)?;
    exocortex_adapter_iceberg::validate_mapping(&mapping, &scan)?;
    let Some(snapshot) = scan.snapshot_id_string() else {
        // An empty table is not an error: there is nothing to ingest.
        println!(
            "table {} has no current snapshot — nothing to ingest",
            scan.table_uuid
        );
        return Ok(());
    };
    let declared = exocortex_adapter_iceberg::declared_columns(&mapping, &scan);
    let (rows, skipped) = exocortex_adapter_iceberg::read_rows(&mapping, &scan)?;
    if skipped > 0 {
        tracing::warn!(skipped, "rows without a usable pk skipped");
    }
    let max_run = args.max_window.saturating_mul(100);
    if rows.len() as u64 > max_run {
        anyhow::bail!(
            "{} rows exceed the declared max_rows_per_run {} — narrow the projection (a snapshot import is a bounded import, not a firehose)",
            rows.len(),
            max_run
        );
    }
    tracing::info!(
        files = scan.files.len(),
        rows = rows.len(),
        snapshot = %snapshot,
        table_uuid = %scan.table_uuid,
        "iceberg table scanned"
    );

    if args.validate {
        println!(
            "ok: {} files, {} rows, {} columns mapped (snapshot {})",
            scan.files.len(),
            rows.len(),
            declared.len(),
            snapshot
        );
        return Ok(());
    }

    // An unchanged current snapshot is a no-op: the cursor holds the
    // last settled snapshot id, so an unchanged table never touches
    // the wire.
    let last = std::fs::read_to_string(&args.cursor)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if last.as_deref() == Some(snapshot.as_str()) {
        println!("nothing to ingest (snapshot {snapshot} already settled)");
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

    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        &org,
        &format!("iceberg://{}", scan.table_uuid),
        &args.producer,
        &backend,
    );
    // The honest flavor: table-shaped, so the server enforces the
    // declared projection, the schema hash, and rewind on THIS source.
    config.source_flavor = "iceberg".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::Custom;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    config.projection = Some(exocortex_adapter_iceberg::projection(
        &mapping,
        &scan,
        args.max_window,
    ));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    let table = exocortex_adapter_iceberg::table_uuid_for(&scan.table_uuid);
    for (index, chunk) in rows.chunks(args.max_window as usize).enumerate() {
        let (unit, skipped_parents) = exocortex_adapter_iceberg::map_rows(
            &mapping,
            &table,
            &declared,
            chunk,
            &format!("window-{index}"),
        );
        if skipped_parents > 0 {
            tracing::warn!(skipped_parents, "parent links outside this window skipped");
        }
        let unit = exocortex_adapter_iceberg::with_snapshot_id(unit, &snapshot);
        let outcome = session.submit_window(vec![unit], &snapshot).await?;
        tracing::info!(
            accepted = outcome.accepted,
            duplicates = outcome.duplicates,
            rejected = outcome.permanent_rejections.len(),
            cursor = %snapshot,
            "window settled"
        );
        for rejection in &outcome.permanent_rejections {
            tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
        }
    }
    println!(
        "ingested {} rows from {} files (snapshot {snapshot})",
        rows.len(),
        scan.files.len()
    );
    Ok(())
}
