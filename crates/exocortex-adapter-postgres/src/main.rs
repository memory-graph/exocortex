//! D20: the Postgres-CDC adapter binary. Connects to logical
//! replication (wal2json), maps changes under the declared column
//! mapping, and submits windows through the signed Ingestion Protocol
//! under the `cdc-postgres` source flavor. Secrets from the
//! environment only: `POSTGRES_URL` (postgres://user:pass@host),
//! `EXOCORTEX_AUTH_TOKEN`, `EXOCORTEX_HMAC_KEY` (64 hex).
//!
//! `--validate` checks the mapping shape locally (no server, no
//! secrets), mirroring the other adapters' CI mode. LSNs are the
//! snapshot identity: `lsn-<16 hex>`, monotonic; a stream below the
//! settled cursor is refused locally (a recreated slot, not a replay).

use clap::Parser;

/// Stream Postgres logical-replication changes into the exocortex graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-postgres", version)]
struct Args {
    /// CDC mapping JSON (table + column mapping + declared types).
    #[arg(long)]
    mapping: std::path::PathBuf,
    /// Logical replication slot (durable; created if absent).
    #[arg(long, default_value = "exocortex_cdc")]
    slot: String,
    /// Local check only: validate the mapping, touch nothing.
    #[arg(long, default_value_t = false)]
    validate: bool,
    /// Backend IngestService base URL.
    #[arg(long)]
    backend: Option<String>,
    /// Owning org.
    #[arg(long)]
    org: Option<String>,
    /// Producer identity for registration.
    #[arg(long, default_value = "postgres-cdc-adapter")]
    producer: String,
    /// Durable cursor file (stores the last settled LSN).
    #[arg(long, default_value = "postgres-cdc-adapter.cursor")]
    cursor: std::path::PathBuf,
    /// Maximum rows per submit window (D21-a bound).
    #[arg(long, default_value = "256")]
    max_window: u64,
    /// How long to wait for more changes before submitting a partial
    /// window (seconds).
    #[arg(long, default_value = "5")]
    flush_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let mapping = exocortex_adapter_postgres::CdcMapping::load(&args.mapping)?;
    mapping.validate()?;
    let declared = mapping.declared_columns();
    if args.validate {
        println!(
            "ok: cdc mapping for {} ({} columns declared)",
            mapping.table,
            declared.len()
        );
        return Ok(());
    }

    let backend = args.backend.clone().unwrap_or_default();
    let org = args.org.clone().unwrap_or_default();
    if backend.is_empty() || org.is_empty() {
        anyhow::bail!("--backend and --org are required unless --validate is set");
    }
    let dsn = std::env::var("POSTGRES_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("POSTGRES_URL is required (postgres://user:pass@host)"))?;
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

    // The settled cursor: LSNs are monotonic, so a slot streaming
    // below it is a REGRESSED slot — refused locally, which a cold
    // session's in-memory history could never see.
    let settled = std::fs::read_to_string(&args.cursor)
        .ok()
        .and_then(|text| text.trim().strip_prefix("lsn-").map(str::to_string))
        .and_then(|hex| u64::from_str_radix(&hex, 16).ok());

    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        &org,
        &format!("cdc-postgres://{}?slot={}", mapping.table, args.slot),
        &args.producer,
        &backend,
    );
    config.source_flavor = "cdc-postgres".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::Custom;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    let last_snapshot = settled
        .map(|lsn| format!("lsn-{lsn:016x}"))
        .unwrap_or_default();
    config.projection = Some(exocortex_adapter_postgres::projection(
        &mapping,
        args.max_window,
        &last_snapshot,
    ));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    let table_uuid = exocortex_adapter_postgres::table_uuid_for_slot(&format!(
        "{}/{}",
        mapping.table, args.slot
    ));

    let mut replication =
        exocortex_adapter_postgres::replication::ReplicationSession::connect(&dsn).await?;
    replication.create_slot_if_not_exists(&args.slot).await?;

    let slot = args.slot.clone();
    let flush_seconds = args.flush_seconds;
    let max_window = args.max_window;
    let (rows_tx, mut rows_rx) = tokio::sync::mpsc::channel::<exocortex_adapter_table::Row>(1024);
    // R8-3: the stream position, shared between the replication task
    // and the submitter so each window's snapshot id is the LSN it
    // actually reached — not the PREVIOUS settled cursor.
    let stream_lsn = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(settled.unwrap_or(0)));
    let stream_mapping = mapping.clone();

    // The replication task: parse every change, forward mapped rows.
    let task_lsn = stream_lsn.clone();
    let stream_task = tokio::spawn(async move {
        let mut deletes = 0usize;
        let mut skipped = 0usize;
        let mut other_tables = 0usize;
        let result = replication
            .stream_changes(&slot, 0, &[stream_mapping.table.clone()], |event| {
                match event {
                    exocortex_adapter_postgres::replication::StreamEvent::Change {
                        payload,
                        lsn,
                    } => {
                        task_lsn.fetch_max(lsn, std::sync::atomic::Ordering::Relaxed);
                        let change = exocortex_adapter_postgres::parse_change(&payload)?;
                        match exocortex_adapter_postgres::map_change(&stream_mapping, &change)? {
                            exocortex_adapter_postgres::MappedChange::Row(row) => {
                                rows_tx.blocking_send(row).ok();
                            }
                            exocortex_adapter_postgres::MappedChange::Delete { .. } => {
                                deletes += 1;
                            }
                            exocortex_adapter_postgres::MappedChange::SkippedNoPk => skipped += 1,
                            exocortex_adapter_postgres::MappedChange::OtherTable => {
                                other_tables += 1
                            }
                        }
                    }
                    exocortex_adapter_postgres::replication::StreamEvent::KeepAlive => {}
                }
                Ok(())
            })
            .await;
        let _ = (result, deletes, skipped, other_tables);
        anyhow::Ok(())
    });

    // The submitter: window rows and settle them under the LSN of the
    // moment each window fills (or the flush interval elapses).
    let mut window: Vec<exocortex_adapter_table::Row> = Vec::new();
    let mut window_index = 0usize;
    loop {
        let tick = tokio::time::sleep(std::time::Duration::from_secs(flush_seconds));
        tokio::select! {
            row = rows_rx.recv() => match row {
                Some(row) => {
                    window.push(row);
                    if window.len() >= max_window as usize {
                        if let Err(error) = submit_window(
                            &mut session, &mapping, &table_uuid, &declared, &mut window_index,
                            std::mem::take(&mut window), &args.cursor, &stream_lsn,
                        ).await {
                            stream_task.abort();
                            return Err(error);
                        }
                    }
                }
                None => {
                    // Stream ended; settle what remains and stop.
                    if !window.is_empty() {
                        submit_window(
                            &mut session, &mapping, &table_uuid, &declared, &mut window_index,
                            std::mem::take(&mut window), &args.cursor, &stream_lsn,
                        ).await?;
                    }
                    break;
                }
            },
            _ = tick => {
                if !window.is_empty() {
                    if let Err(error) = submit_window(
                        &mut session, &mapping, &table_uuid, &declared, &mut window_index,
                        std::mem::take(&mut window), &args.cursor, &stream_lsn,
                    ).await {
                        stream_task.abort();
                        return Err(error);
                    }
                }
            }
        }
    }
    stream_task.await??;
    println!("cdc stream ended");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_window(
    session: &mut exocortex_adapter_sdk::AdapterSession,
    mapping: &exocortex_adapter_postgres::CdcMapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    window_index: &mut usize,
    rows: Vec<exocortex_adapter_table::Row>,
    cursor: &std::path::Path,
    stream_lsn: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> anyhow::Result<()> {
    // The snapshot id is the settled cursor's next LSN — the SDK
    // advances its durable cursor when the window settles, and the
    // local file below records the same identity for the cold-start
    // regression gate.
    let (unit, skipped_parents) = exocortex_adapter_postgres::map_rows(
        mapping,
        table_uuid,
        declared,
        &rows,
        &format!("window-{window_index}"),
    );
    if skipped_parents > 0 {
        tracing::warn!(skipped_parents, "parent links outside this window skipped");
    }
    *window_index += 1;
    // R8-3: the window's snapshot id is the LSN the stream has
    // actually reached (monotonic, seeded with the settled cursor).
    let lsn = stream_lsn.load(std::sync::atomic::Ordering::Relaxed);
    let snapshot = format!("lsn-{lsn:016x}");
    let unit = exocortex_adapter_postgres::with_snapshot_id(unit, &snapshot);
    let outcome = session.submit_window(vec![unit], &snapshot).await?;
    tracing::info!(
        accepted = outcome.accepted,
        duplicates = outcome.duplicates,
        rejected = outcome.permanent_rejections.len(),
        cursor = %snapshot,
        "cdc window settled"
    );
    for rejection in &outcome.permanent_rejections {
        tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
    }
    if let Some(settled) = session.cursor() {
        std::fs::write(cursor, settled)?;
    }
    Ok(())
}
