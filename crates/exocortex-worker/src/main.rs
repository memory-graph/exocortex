//! `exocortex-worker` — the out-of-process adapter host (§18.2). Links
//! `exocortex-wire` and `exocortex-adapter-sdk` ONLY (never the kernel,
//! R-I1). v1 ships two adapters:
//!
//! - `noop` — the idle host (M6 AC: starts without a live backend).
//! - `fixture` — the reference adapter (PRD R16): reads canned rows from
//!   a JSON file and submits them through the SDK. It is the first
//!   producer to speak the Ingestion Protocol from outside the kernel's
//!   address space.

use clap::Parser;

/// Adapter host options.
#[derive(Debug, Parser)]
#[command(name = "exocortex-worker", version)]
struct Args {
    /// Adapter name: `noop` or `fixture`.
    #[arg(long, default_value = "noop")]
    adapter: String,
    /// Path to the adapter configuration file.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// Backend IngestService endpoint.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    backend: String,
    /// `--adapter fixture`: path to the fixture file (JSON).
    #[arg(long)]
    fixture: Option<std::path::PathBuf>,
    /// `--adapter fixture`: where the durable cursor lives.
    #[arg(long)]
    cursor: Option<std::path::PathBuf>,
    /// `--adapter fixture`: org to submit into.
    #[arg(long, default_value = "org")]
    org: String,
    /// `--adapter fixture`: producer HMAC key, 64 hex chars. Falls back
    /// to `$EXOCORTEX_HMAC_KEY`, then the shared dev key `[0x42; 32]` —
    /// which must match the backend's ingest key (round-3 C2: the
    /// hardcoded `[5u8; 32]` could never authenticate).
    #[arg(long)]
    hmac_key: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
        let args = Args::parse();
        match args.adapter.as_str() {
            "noop" => {
                // Deliberately no exocortex-kernel usage here (R-I1). The
                // noop pump never submits, so the backend channel is lazy:
                // connect lazily, probe in the background, and stay idle
                // when unreachable (M6 AC — the worker must start without
                // a live backend).
                let endpoint =
                    tonic::transport::Endpoint::from_shared(args.backend.clone()).map_err(
                        |e| anyhow::anyhow!("bad --backend {}: {e}", args.backend),
                    )?;
                let _channel = endpoint.connect_lazy();
                tracing::info!(adapter = "noop", backend = %args.backend, "exocortex-worker ready (no-op, lazy backend)");
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    tracing::debug!("noop frame tick");
                }
            }
            "fixture" => run_fixture(args).await,
            other => anyhow::bail!("unknown adapter `{other}` (v1 ships noop, fixture)"),
        }
    })
}

/// The fixture file format (documented for adapter authors):
///
/// ```json
/// {
///   "producer_id": "fixture",
///   "seed": "window-1",
///   "cursor": "window-1",
///   "memories": [
///     { "draft_key": "k1", "memory_type": "General",
///       "title": "…", "content": "…", "visibility": 3, "tags": [] }
///   ],
///   "relationships": [
///     { "from": "k1", "to": "k2", "kind": "Solves" }
///   ]
/// }
/// ```
async fn run_fixture(args: Args) -> anyhow::Result<()> {
    use exocortex_adapter_sdk::{AdapterConfig, AdapterSession, BatchUnit};
    use exocortex_wire::ingest::v1::{MemoryDraft, RelationshipDraft};

    // Key resolution order: --hmac-key > $EXOCORTEX_HMAC_KEY. Shared-mode
    // producers never fall back to a published key.
    let hmac_key_hex = args
        .hmac_key
        .clone()
        .or_else(|| std::env::var("EXOCORTEX_HMAC_KEY").ok())
        .ok_or_else(|| anyhow::anyhow!("--hmac-key or EXOCORTEX_HMAC_KEY is required"))?;
    let hmac_key =
        exocortex_wire::signing::decode_hex32(&hmac_key_hex).map_err(anyhow::Error::msg)?;
    let path = args
        .fixture
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--adapter fixture requires --fixture <path>"))?;
    let raw: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let producer = raw["producer_id"].as_str().unwrap_or("fixture").to_string();
    let seed = raw["seed"].as_str().unwrap_or("window-1").to_string();
    let cursor = raw["cursor"].as_str().unwrap_or(&seed).to_string();

    let mut memories = Vec::new();
    for m in raw["memories"].as_array().cloned().unwrap_or_default() {
        memories.push(MemoryDraft {
            draft_key: m["draft_key"].as_str().unwrap_or_default().into(),
            id: String::new(),
            memory_type: m["memory_type"].as_str().unwrap_or("General").into(),
            title: m["title"].as_str().unwrap_or_default().into(),
            content: m["content"].as_str().unwrap_or_default().into(),
            tags: m["tags"]
                .as_array()
                .map(|t| {
                    t.iter()
                        .filter_map(|x| x.as_str().map(Into::into))
                        .collect()
                })
                .unwrap_or_default(),
            visibility: m["visibility"].as_i64().unwrap_or(3) as i32,
            valid_from: None,
            valid_until: None,
            external_key: None,
        });
    }
    let mut relationships = Vec::new();
    for r in raw["relationships"].as_array().cloned().unwrap_or_default() {
        relationships.push(RelationshipDraft {
            from_draft_key: r["from"].as_str().unwrap_or_default().into(),
            to_draft_key: r["to"].as_str().unwrap_or_default().into(),
            kind: r["kind"].as_str().unwrap_or("RelatedTo").into(),
            strength: r["strength"].as_f64().unwrap_or(0.5) as f32,
            confidence: r["confidence"].as_f64().unwrap_or(0.8) as f32,
            context: r["context"].as_str().unwrap_or_default().into(),
            visibility: r["visibility"].as_i64().unwrap_or(3) as i32,
            to_memory_id: String::new(),
        });
    }

    let cursor_path = args.cursor.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("exocortex-fixture-{producer}.cursor"))
    });
    let config = AdapterConfig {
        org_id: args.org.clone(),
        source_uri: format!("fixture://{producer}"),
        producer_id: producer.clone(),
        adapter_id: format!("{producer}-adapter"),
        node_id: format!("{producer}-node"),
        source_flavor: "custom".into(),
        producer_kind: exocortex_wire::ingest::v1::ProducerKind::AnalyticsAdapter,
        ceiling: 3,
        backend_url: args.backend.clone(),
        hmac_key,
        max_batch_bytes: 4 * 1024 * 1024,
        cursor_path,
        retry: exocortex_adapter_sdk::RetryPolicy::default(),
    };

    let mut session = AdapterSession::connect(config).await?;
    tracing::info!(producer = %producer, "fixture adapter connected");
    let unit = BatchUnit {
        batch_id_seed: seed.clone(),
        memories,
        relationships,
        snapshot: None,
        observed_at: std::time::SystemTime::now(),
    };
    let outcome = session.submit_window(vec![unit], &cursor).await?;
    tracing::info!(
        accepted = outcome.accepted,
        duplicates = outcome.duplicates,
        rejected = outcome.permanent_rejections.len(),
        cursor_advanced = outcome.cursor_advanced,
        "fixture window settled"
    );
    if !outcome.permanent_rejections.is_empty() {
        for r in &outcome.permanent_rejections {
            eprintln!(
                "permanent rejection: {} code={} {}",
                r.draft_key, r.code, r.detail
            );
        }
    }
    Ok(())
}
