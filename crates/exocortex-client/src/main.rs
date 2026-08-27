//! `exocortex-mcp-client` — the local MCP server binary (§4.2).
//!
//! M3: stdio MCP surface over the ArcSwap cache; WAL for offline writes;
//! SSE subscription arrives at M5. Startup: load the effective ontology
//! (fail on fingerprint mismatch with the stored cache state), reseed the
//! cache from the backend (or synthetic data in standalone dev mode),
//! install the Agent Playbook (D5), serve.

use exocortex_client::{mcp, wal};

use std::sync::Arc;

use clap::Parser;
use rmcp::ServiceExt;

use exocortex_cache::LocalCache;
use exocortex_kernel::{Ontology, Visibility};
use exocortex_ops::VisibilityContext;

/// Local MCP server options (§4.2) + the D5 playbook/install/verify
/// surface (agent-instructions PRD §3.5).
#[derive(Debug, Parser)]
#[command(name = "exocortex-mcp-client", version)]
struct Args {
    /// Backend base URL (M5+). Omitted: serve synthetic data.
    #[arg(long)]
    backend: Option<String>,
    /// Bearer token for the backend (attached to every ingest call;
    /// audit CL4: parsed-and-unused is an inert credential surface).
    #[arg(long)]
    auth_token: Option<String>,
    /// Org id (defaults to the single-user org).
    #[arg(long, default_value = "personal")]
    org: String,
    /// User id for visibility filtering.
    #[arg(long, default_value = "dev")]
    user: String,
    /// Data directory for the WAL and the playbook (defaults to the
    /// user's data home; `~/.exocortex` when unset).
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// Producer HMAC key (64 hex chars) for backend submits.
    #[arg(long)]
    hmac_key: Option<String>,
    /// D5: print the compiled Agent Playbook to stdout and exit.
    #[arg(long)]
    dump_playbook: bool,
    /// D5: print just the `CLAUDE.md`/`AGENTS.md` instruction block to
    /// stdout and exit (`exocortex-mcp-client --dump-block >> CLAUDE.md`).
    #[arg(long)]
    dump_block: bool,
    /// D5 (S6): sanity-check the local install. Prints a checklist with
    /// green/red rows; exits with the number of red rows. Never returns
    /// green on a failed known precondition, and never attempts a
    /// polluting probe write.
    #[arg(long)]
    verify: bool,
    /// D5: print the N most recent local writes (WAL, newest first) with
    /// timestamps, draft keys, and sync state. Read-only.
    #[arg(long = "tail-audit")]
    tail_audit: bool,
    /// `--tail-audit` row count (default 5).
    #[arg(long, default_value = "5")]
    last: usize,
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
                producer_kind: None,
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

    // D5 one-shot modes (no server, no cache, exit immediately).
    if args.dump_playbook {
        print!("{}", exocortex_client::playbook::PLAYBOOK);
        return Ok(());
    }
    if args.dump_block {
        print!("{}", exocortex_client::playbook::BLOCK);
        return Ok(());
    }

    // CL3 (audit): validate --hmac-key BEFORE anything opens (a malformed
    // key is a startup error, never a silent all-zero key signing every
    // wrapup batch).
    let hmac_key = match args.hmac_key.as_deref() {
        Some(hex) => Some(
            exocortex_wire::signing::decode_hex32(hex)
                .map_err(|e| anyhow::anyhow!("--hmac-key: {e}"))?,
        ),
        None => None,
    };

    // Ontology: fail fast if the linked pack set does not assemble. The
    // black_box reference force-links the pack so its inventory registration
    // runs in this binary.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology: Arc<Ontology> = Arc::new(exocortex_kernel::pack::load_registered_packs()?);

    // WAL + playbook home (D5: `~/.exocortex` by default; --data-dir for
    // tests and multi-tenant setups).
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        dirs_fallback().unwrap_or_else(|_| std::env::temp_dir().join("exocortex"))
    });

    if args.tail_audit {
        return tail_audit(&data_dir.join("wal"), args.last);
    }

    // D5: install the playbook on first run / upgrade; notify on stderr
    // (never stdout — that is the MCP channel).
    match exocortex_client::playbook::install(&data_dir) {
        Ok(Some(notice)) => eprintln!("{notice}"),
        Ok(None) => {}
        Err(e) => tracing::warn!(?e, "playbook install failed (serving continues)"),
    }

    if args.verify {
        return verify(&args, &ontology, &data_dir);
    }

    // Cache seed. CL5 (audit): synthetic data is STANDALONE DEV MODE only —
    // with `--backend` configured, reads must be honestly empty rather than
    // fabricated ("Fix flaky auth test" & co. are startup filler, not
    // memories).
    let (cache, _writer_rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let cache = Arc::new(cache);
    match args.backend.as_ref() {
        None => cache.publish(&args.org.clone(), synth_snapshot()),
        Some(_) => cache.publish(
            &args.org.clone(),
            Arc::new(exocortex_cache::GraphSnapshot::empty()),
        ),
    }

    // WAL: offline write buffer (buffer-only at M3).
    let wal = Arc::new(wal::Wal::open(&data_dir.join("wal"))?);
    if wal.near_full() {
        tracing::warn!("WAL Near Full (R-Sc8)");
    }

    let server = mcp::ExocortexMcp::new(
        args.org.clone().into(),
        cache.clone(),
        org_visibility(&args.org, &args.user),
        ontology.clone(),
    )
    .with_offline_wal(wal.clone());

    // Online end_session (§13.6.2): a gRPC channel to the backend, plus the
    // producer HMAC key. Connect lazily at first call when unreachable. The
    // channel is built INSIDE the runtime (connect_lazy needs a reactor).
    let backend = args.backend.clone();
    let hmac_key = hmac_key.unwrap_or([0u8; 32]);
    let org = args.org.clone();
    let user = args.user.clone();
    let auth_token = args.auth_token.clone();
    let fingerprint = ontology.fingerprint.0;
    let ontology_for_drain = ontology.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let mut server = server;
        if let Some(backend) = backend {
            let endpoint = tonic::transport::Endpoint::from_shared(backend.clone())
                .map_err(|e| anyhow::anyhow!("bad --backend {backend}: {e}"))?;
            let channel = endpoint.connect_lazy();
            // W1 (audit): drain buffered offline writes to the backend —
            // each Pending entry is rebuilt, signed fresh, and settled via
            // the R13 classify table. Runs at startup and retries
            // transport-failed entries every 30s until quiescent.
            if wal.pending_count() > 0 {
                let wal = wal.clone();
                let mut drain_client =
                    exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::new(
                        channel.clone(),
                    );
                let org_id = org.clone();
                let token = auth_token.clone();
                let onto = ontology_for_drain.clone();
                let node = format!("exocortex-mcp-client-{}", std::process::id());
                tokio::spawn(async move {
                    exocortex_client::drain::drain_all(
                        wal,
                        &mut drain_client,
                        hmac_key,
                        fingerprint,
                        org_id,
                        token,
                        onto,
                        node,
                    )
                    .await;
                });
            }
            let tool = exocortex_client::tools::end_session::EndSessionTool {
                client: exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::new(
                    channel,
                ),
                org_id: org.clone(),
                fingerprint,
                hmac_key,
                node_id: format!("exocortex-mcp-client-{}", std::process::id()),
                agent_id: user.clone(),
                // CL4 (audit): the bearer token rides every ingest call.
                auth_token,
                // r4 self-preflight + §4.5 cache lookups.
                ontology: ontology_for_drain,
                cache: Some(cache.clone()),
                vc: org_visibility(&org, &user),
            };
            server = server.with_end_session(Arc::new(tool));
        }
        tracing::info!(org = %org, "exocortex-mcp-client serving MCP over stdio");
        let service = server
            .serve((tokio::io::stdin(), tokio::io::stdout()))
            .await?;
        service.waiting().await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

/// D5 `--tail-audit [--last N]`: the most recent local writes from the
/// WAL (pending + settled), newest first. Read-only.
fn tail_audit(wal_dir: &std::path::Path, last: usize) -> anyhow::Result<()> {
    let wal = wal::Wal::open(wal_dir)?;
    let rows = wal.tail(last);
    if rows.is_empty() {
        println!("no local writes recorded in {}", wal_dir.display());
        return Ok(());
    }
    for r in rows {
        println!(
            "{}  [{}]  batch={}  drafts={}  keys=[{}]",
            r.recorded_at,
            if r.pending { "pending" } else { "settled" },
            r.batch_id,
            r.memory_count,
            r.draft_keys.join(", "),
        );
    }
    Ok(())
}

/// D5/S6 `--verify`: every CLIENT-CHECKABLE precondition of a successful
/// write, each row green or red; exit code = red count. It must never
/// return green when a known precondition fails, and never attempts a
/// polluting probe write.
fn verify(args: &Args, ontology: &Ontology, data_dir: &std::path::Path) -> anyhow::Result<()> {
    let mut red = 0usize;

    // 1. Playbook installed and current.
    let version_file = data_dir.join("version.txt");
    let installed = std::fs::read_to_string(&version_file).unwrap_or_default();
    let ok = installed.contains(&format!(
        "playbook={}",
        exocortex_client::playbook::PLAYBOOK_VERSION
    ));
    if ok {
        println!(
            "  ok    playbook: v{} at {}",
            exocortex_client::playbook::PLAYBOOK_VERSION,
            data_dir.join("playbook.md").display()
        );
    } else {
        red += 1;
        println!(
            "  RED   playbook: current version not installed at {}",
            data_dir.display()
        );
    }

    // 2. Ontology assembles and reports its fingerprint.
    println!(
        "  ok    ontology: fingerprint {}",
        hex_prefix(&ontology.fingerprint.0)
    );

    // 3. HMAC key shape (when a backend is configured, submits need it).
    match (&args.backend, &args.hmac_key) {
        (Some(_), None) => {
            red += 1;
            println!("  RED   hmac-key: --backend set but --hmac-key missing");
        }
        (Some(_), Some(k)) => {
            let ok = exocortex_wire::signing::decode_hex32(k).is_ok();
            if !ok {
                red += 1;
            }
            println!(
                "  {}  hmac-key: {}",
                if ok { "ok" } else { "RED" },
                if ok { "64 hex chars" } else { "malformed" }
            );
        }
        _ => println!("  ok    hmac-key: not required (no backend configured)"),
    }

    // 4. Backend reachable + fingerprint matches (read-only Fingerprint
    //    RPC; no probe write).
    if let Some(backend) = &args.backend {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let outcome: Result<bool, String> = rt.block_on(async {
            let mut client =
                exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::connect(
                    backend.clone(),
                )
                .await
                .map_err(|e| e.to_string())?;
            let mut req = tonic::Request::new(exocortex_wire::ingest::v1::FingerprintRequest {});
            if let Some(token) = &args.auth_token {
                if let Ok(v) = format!("Bearer {token}").parse() {
                    req.metadata_mut().insert("authorization", v);
                }
            }
            let fp = client
                .fingerprint(req)
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            Ok(fp.fingerprint.as_slice() == ontology.fingerprint.0.as_slice())
        });
        match outcome {
            Ok(true) => println!("  ok    backend: {backend} reachable, fingerprint matches"),
            Ok(false) => {
                red += 1;
                println!("  RED   backend: {backend} reachable, ONTOLOGY FINGERPRINT MISMATCH");
            }
            Err(e) => {
                red += 1;
                println!("  RED   backend: {backend} unreachable: {e}");
            }
        }
    } else {
        println!("  ok    backend: none configured (offline WAL mode)");
    }

    // 5. WAL health: pending entries would drift on a future write.
    let wal_dir = data_dir.join("wal");
    if wal_dir.exists() {
        let wal = wal::Wal::open(&wal_dir)?;
        let pending = wal.pending_count();
        if pending > 0 {
            red += 1;
        }
        println!(
            "  {}  wal: {pending} pending entries (drained at startup when a backend is configured)",
            if pending == 0 { "ok" } else { "RED" }
        );
    } else {
        println!("  ok    wal: no local WAL yet (nothing buffered)");
    }

    println!(
        "\n{}",
        if red == 0 {
            "verify: all client-checkable preconditions hold (a future write is not guaranteed — server-side admission runs at submit)"
        } else {
            "verify: RED rows above are known-failed preconditions"
        }
    );
    std::process::exit(red as i32);
}

fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("")
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
