//! `exocortex-mcp-client` — the local MCP server binary (§4.2).
//!
//! M3: stdio MCP surface over the ArcSwap cache; WAL for offline writes;
//! SSE subscription arrives at M5. Startup: load the effective ontology
//! (fail on fingerprint mismatch with the stored cache state), reseed the
//! cache from the backend (or synthetic data in standalone dev mode), serve.

use exocortex_client::{mcp, wal};

use std::sync::Arc;

use clap::Parser;
use rmcp::ServiceExt;

use exocortex_cache::LocalCache;
use exocortex_kernel::{Ontology, Visibility};
use exocortex_storage::VisibilityContext;

/// Local MCP server options (§4.2).
#[derive(Debug, Parser)]
#[command(name = "exocortex-mcp-client", version)]
struct Args {
    /// Backend base URL (M5+). Omitted: serve synthetic data.
    #[arg(long)]
    backend: Option<String>,
    /// Bearer token for the backend.
    #[arg(long)]
    auth_token: Option<String>,
    /// Org id (defaults to the single-user org).
    #[arg(long, default_value = "personal")]
    org: String,
    /// User id for visibility filtering.
    #[arg(long, default_value = "dev")]
    user: String,
    /// Data directory for the WAL (defaults to the user's data home).
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// Producer HMAC key (64 hex chars) for backend submits.
    #[arg(long)]
    hmac_key: Option<String>,
}

/// Decode 64 hex chars into a 32-byte key.
fn decode_hex32(hex: &str) -> Result<[u8; 32], anyhow::Error> {
    let bytes = (0..32)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<Vec<_>, _>>()?;
    bytes.try_into().map_err(|_| anyhow::anyhow!("bad hex"))
}

fn org_visibility(org: &str, user: &str) -> VisibilityContext {
    VisibilityContext {
        user_id: user.into(),
        org_id: org.into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    }
}

fn synth_snapshot() -> Arc<exocortex_cache::GraphSnapshot> {
    use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, LSN};
    let mut snap = exocortex_cache::GraphSnapshot::empty();
    for (i, (title, tag)) in [
        ("Fix flaky auth test", "auth"),
        ("Parser handles nested generics", "parser"),
        ("Cache snapshot swap", "cache"),
        ("Cluster lease fencing", "cluster"),
    ]
    .into_iter()
    .enumerate()
    {
        let m = Memory {
            id: MemoryId::new_v7(),
            memory_type: 7,
            title: title.into(),
            content: format!("{title}: synthetic body {i}"),
            summary: None,
            tags: [tag].into_iter().map(Into::into).collect(),
            visibility: Visibility::Org,
            provenance: Provenance::Asserted {
                author: "synthetic".into(),
            },
            context: MemoryContext {
                timestamp: chrono::Utc::now(),
                project_id: Some("demo".into()),
                project_path: None,
                team_id: None,
                tenant_id: None,
                session_id: None,
                user_id: Some("dev".into()),
                created_by: None,
                files_involved: Default::default(),
                languages: Default::default(),
                frameworks: Default::default(),
                technologies: Default::default(),
                git_commit: None,
                git_branch: None,
                working_directory: None,
                entities: Default::default(),
                additional_metadata: serde_json::Value::Null,
            },
            importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
            confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
            effectiveness: None,
            usage_count: 0,
            valid_from: chrono::Utc::now(),
            valid_until: None,
            recorded_at: chrono::Utc::now(),
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_backend(i as u64 + 1),
        };
        snap.push_test_memory(m);
    }
    Arc::new(snap)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    // Ontology: fail fast if the linked pack set does not assemble. The
    // black_box reference force-links the pack so its inventory registration
    // runs in this binary.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology: Arc<Ontology> = Arc::new(exocortex_kernel::pack::load_registered_packs()?);

    // Cache: v1 seeds synthetic data until backend sync (M5) lands.
    let (cache, _writer_rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let cache = Arc::new(cache);
    cache.publish(&args.org.clone(), synth_snapshot());

    // WAL: offline write buffer (buffer-only at M3).
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        dirs_fallback().unwrap_or_else(|_| std::env::temp_dir().join("exocortex"))
    });
    let wal = Arc::new(wal::Wal::open(&data_dir.join("wal"))?);
    if wal.near_full() {
        tracing::warn!("WAL Near Full (R-Sc8)");
    }

    let mut server = mcp::ExocortexMcp::new(
        args.org.clone().into(),
        cache.clone(),
        org_visibility(&args.org, &args.user),
    )
    .with_offline_wal(wal.clone(), ontology.clone());

    // Online end_session (§13.6.2): a gRPC channel to the backend, plus the
    // producer HMAC key. Connect lazily at first call when unreachable.
    if let Some(backend) = args.backend.clone() {
        let hmac_key = args
            .hmac_key
            .as_deref()
            .and_then(|hex| decode_hex32(hex).ok())
            .unwrap_or([0u8; 32]);
        let endpoint = tonic::transport::Endpoint::from_shared(backend.clone())
            .map_err(|e| anyhow::anyhow!("bad --backend {backend}: {e}"))?;
        let channel = endpoint.connect_lazy();
        let tool = exocortex_client::tools::end_session::EndSessionTool {
            client: exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::new(
                channel,
            ),
            org_id: args.org.clone(),
            fingerprint: ontology.fingerprint.0,
            hmac_key,
            node_id: format!("exocortex-mcp-client-{}", std::process::id()),
            agent_id: args.user.clone(),
        };
        server = server.with_end_session(Arc::new(tool));
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        tracing::info!(org = %args.org, "exocortex-mcp-client serving MCP over stdio");
        let service = server
            .serve((tokio::io::stdin(), tokio::io::stdout()))
            .await?;
        service.waiting().await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

/// Minimal data-home resolution without a new dependency.
fn dirs_fallback() -> Result<std::path::PathBuf, anyhow::Error> {
    if let Ok(home) = std::env::var("HOME") {
        let dir = std::path::Path::new(&home)
            .join("Library")
            .join("Application Support")
            .join("exocortex");
        if cfg!(not(target_os = "macos")) {
            return Ok(std::path::Path::new(&home)
                .join(".local")
                .join("share")
                .join("exocortex"));
        }
        std::fs::create_dir_all(&dir)?;
        return Ok(dir);
    }
    anyhow::bail!("no HOME")
}
