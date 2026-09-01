//! `exocortex-mcp-client` — the local MCP server binary (§4.2).
//!
//! M3: stdio MCP surface over the ArcSwap cache; WAL for offline writes;
//! SSE subscription arrives at M5. Startup: load the effective ontology
//! (fail on fingerprint mismatch with the stored cache state), seed the
//! cache (backend: reseed from the server over SSE; standalone: from the
//! local WAL, SR-PRD F3), install the Agent Playbook (D5), serve.

use exocortex_client::{mcp, wal};

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use clap::Parser;
use rmcp::ServiceExt;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use exocortex_cache::LocalCache;
use exocortex_kernel::{Ontology, Visibility};
use exocortex_ops::VisibilityContext;

/// Local MCP server options (§4.2) + the D5 playbook/install/verify
/// surface (agent-instructions PRD §3.5).
#[derive(Debug, Parser)]
#[command(name = "exocortex-mcp-client", version)]
struct Args {
    /// Internal acceptance probe: execute all nine rules in this artifact.
    #[arg(long, hide = true)]
    verify_rules: bool,
    /// Backend base URL (M5+). Omitted: standalone personal mode —
    /// writes buffer to the local WAL and are readable immediately and
    /// across restarts (SR-PRD F1-F5).
    #[arg(long)]
    backend: Option<String>,
    /// Org id (defaults to the single-user org).
    #[arg(long, default_value = "personal")]
    org: String,
    /// User id for visibility filtering.
    #[arg(long, default_value = "dev")]
    user: String,
    /// Data directory for the WAL and the playbook (defaults to the
    /// platform's user data home).
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// D5: print the compiled Agent Playbook to stdout and exit.
    #[arg(long)]
    dump_playbook: bool,
    /// D5: print just the `CLAUDE.md`/`AGENTS.md` instruction block to
    /// stdout and exit (`exocortex-mcp-client --dump-block >> CLAUDE.md`).
    #[arg(long)]
    dump_block: bool,
    /// PX2: print every registered operation — kernel ops AND
    /// pack-registered verbs — with its pack identity, surfaces, and
    /// typed input schema, one JSON line each, then exit.
    #[arg(long)]
    dump_tools: bool,
    /// PX2: print the two-level ontology fingerprint (compatibility +
    /// build) and the loaded pack set, then exit.
    #[arg(long)]
    dump_fingerprint: bool,
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
    /// BR-PRD: one-shot backup — dump every WAL entry (all states, LSN
    /// order) to a versioned, fingerprint-stamped JSON file, then exit.
    #[arg(long)]
    export: Option<std::path::PathBuf>,
    /// BR-PRD: one-shot restore — import a backup file into this
    /// data-dir's WAL (all-or-nothing; fingerprint-gated; idempotent),
    /// then exit.
    #[arg(long)]
    import: Option<std::path::PathBuf>,
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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    if let Some(backend) = &args.backend {
        exocortex_wire::transport::validate_backend_url(backend)
            .map_err(|error| anyhow::anyhow!("--backend: {error}"))?;
    }

    // D5 one-shot modes (no server, no cache, exit immediately).
    if args.dump_playbook {
        print!("{}", exocortex_client::playbook::PLAYBOOK);
        return Ok(());
    }
    if args.dump_block {
        print!("{}", exocortex_client::playbook::BLOCK);
        return Ok(());
    }
    // PX2 one-shot modes (registry + fingerprint surfaces).
    if args.dump_tools {
        for entry in exocortex_ops::entries() {
            println!(
                "{}",
                serde_json::json!({
                    "name": entry.name,
                    "pack": entry.pack,
                    "mcp_tool": entry.mcp_tool_name,
                    "http": format!("{} {}", (entry.http_method)(), entry.http_path),
                    "input_schema": (entry.input_schema)(),
                })
            );
        }
        return Ok(());
    }
    if args.dump_fingerprint {
        let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
        let _ = std::hint::black_box(exocortex_pack_mortgage_v1::pack_def().name.clone());
        let onto = exocortex_kernel::pack::load_registered_packs()?;
        let hex = |bytes: &[u8; 32]| -> String {
            use std::fmt::Write as _;
            let mut out = String::with_capacity(64);
            for b in bytes {
                let _ = write!(out, "{b:02x}");
            }
            out
        };
        println!("compatibility {}", hex(&onto.fingerprint.0));
        println!("build {}", hex(&onto.build_fingerprint.0));
        println!("packs {}", onto.packs.len());
        println!(
            "verbs {}",
            exocortex_kernel::verbs::registered_pack_actions().len()
                + exocortex_kernel::verbs::registered_pack_functions().len()
        );
        return Ok(());
    }

    // CL3 (audit): validate EXOCORTEX_HMAC_KEY BEFORE anything opens (a malformed
    // key is a startup error, never a silent all-zero key signing every
    // wrapup batch).
    let hmac_key_hex = std::env::var("EXOCORTEX_HMAC_KEY")
        .ok()
        .filter(|value| !value.is_empty());
    let auth_token = std::env::var("EXOCORTEX_AUTH_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    let sse_key = std::env::var("EXOCORTEX_SSE_KEY")
        .ok()
        .filter(|value| !value.is_empty())
        .map(|hex| {
            exocortex_wire::signing::decode_hex32(&hex)
                .map_err(|error| anyhow::anyhow!("EXOCORTEX_SSE_KEY: {error}"))
        })
        .transpose()?;
    let hmac_key = match hmac_key_hex.as_deref() {
        Some(hex) => Some(
            exocortex_wire::signing::decode_hex32(hex)
                .map_err(|e| anyhow::anyhow!("EXOCORTEX_HMAC_KEY: {e}"))?,
        ),
        None => None,
    };
    if args.backend.is_some() && hmac_key.is_none() {
        anyhow::bail!("EXOCORTEX_HMAC_KEY is required when --backend is configured");
    }
    if args.backend.is_some() && auth_token.is_none() {
        anyhow::bail!("EXOCORTEX_AUTH_TOKEN is required when --backend is configured");
    }
    if args.backend.is_some() && sse_key.is_none() {
        anyhow::bail!("EXOCORTEX_SSE_KEY is required when --backend is configured");
    }

    // Ontology: fail fast if the linked pack set does not assemble. The
    // black_box reference force-links the pack so its inventory registration
    // runs in this binary.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let ontology: Arc<Ontology> = Arc::new(exocortex_kernel::pack::load_registered_packs()?);
    // WAL + playbook home (D5: OS data home by default; --data-dir for
    // tests and multi-tenant setups).
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        dirs_fallback().unwrap_or_else(|_| std::env::temp_dir().join("exocortex"))
    });

    if args.tail_audit {
        return tail_audit(&data_dir.join("wal"), args.last);
    }

    // BR-PRD one-shot modes (no server, no cache, exit immediately).
    let fingerprint_hex = {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(64);
        for b in ontology.fingerprint.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    };
    if let Some(path) = &args.export {
        let wal = wal::Wal::open(&data_dir.join("wal"))?;
        let n = exocortex_client::backup::export(&wal, &fingerprint_hex, &ontology.summary, path)?;
        println!("{n} entries -> {}", path.display());
        return Ok(());
    }
    if let Some(path) = &args.import {
        let wal = wal::Wal::open(&data_dir.join("wal"))?;
        let report = exocortex_client::backup::import(&wal, &ontology, path)?;
        println!(
            "{} entries restored (first local_lsn {})",
            report.imported, report.first_local_lsn
        );
        return Ok(());
    }

    // D5: install the playbook on first run / upgrade; notify on stderr
    // (never stdout — that is the MCP channel).
    match exocortex_client::playbook::install(&data_dir) {
        Ok(Some(notice)) => eprintln!("{notice}"),
        Ok(None) => {}
        Err(e) => tracing::warn!(?e, "playbook install failed (serving continues)"),
    }

    if args.verify {
        return verify(
            &args,
            &ontology,
            &data_dir,
            hmac_key_hex.as_deref(),
            auth_token.as_deref(),
        );
    }

    // Cache seed. CL5 (audit): with `--backend` configured, reads must be
    // honestly empty until the SSE feed delivers server rows ("Fix flaky
    // auth test" & co. were startup filler, not memories). SR-PRD F3:
    // standalone seeds from ALL WAL entries — every state, because in
    // standalone nothing else will ever deliver these rows server-side.
    let (cache, writer_rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let cache = Arc::new(cache);

    // WAL: offline write buffer + (standalone) the embedded store.
    let wal = Arc::new(wal::Wal::open(&data_dir.join("wal"))?);
    if wal.near_full()? {
        tracing::warn!("WAL Near Full (R-Sc8)");
    }

    match args.backend.as_ref() {
        None => {
            let entries = wal.entries()?;
            let last_lsn = entries.last().map(|e| e.local_lsn).unwrap_or(0);
            let rows =
                exocortex_client::materialize::materialize_all(&ontology, &args.org, &entries);
            if !rows.dropped_edges.is_empty() {
                tracing::warn!(?rows.dropped_edges, "standalone seed dropped edges");
            }
            cache.seed_local(&args.org, &rows.memories, &rows.edges, last_lsn);
            tracing::info!(
                memories = rows.memories.len(),
                batches = entries.len(),
                "standalone boot seeded from WAL"
            );
        }
        // F3: backend mode seeds NOTHING — the drain commits rows
        // server-side under server ids, SSE/reseed delivers them, and WAL
        // ids differ, so seeding would duplicate. Mode switching is clean:
        // standalone rows only ever exist in a standalone snapshot.
        Some(_) => cache.seed_local(&args.org, &[], &[], 0),
    }

    let server = mcp::ExocortexMcp::new(
        args.org.clone().into(),
        cache.clone(),
        org_visibility(&args.org, &args.user),
        ontology.clone(),
    )
    .with_offline_wal(wal.clone());

    // The acceptance probe deliberately runs after the selected topology has
    // opened its WAL, materialized its cache, and assembled the MCP operation
    // runtime. This prevents a linked-catalogue-only shortcut from blessing a
    // deployment mode that cannot actually initialize.
    if args.verify_rules {
        exocortex_reasoning::acceptance::verify_nine_catalogued_rules(&ontology)
            .map_err(anyhow::Error::msg)?;
        println!(
            "rules-ok mode={} count=9 artifact=exocortex-mcp-client",
            std::env::var("EXOCORTEX_DEPLOYMENT_MODE").unwrap_or_else(|_| "mcp-client".into())
        );
        return Ok(());
    }

    // Online end_session (§13.6.2): a gRPC channel to the backend, plus the
    // producer HMAC key. Connect lazily at first call when unreachable. The
    // channel is built INSIDE the runtime (connect_lazy needs a reactor).
    let backend = args.backend.clone();
    // Backend mode was validated above; standalone never submits this value.
    let hmac_key = hmac_key.unwrap_or([0u8; 32]);
    let org = args.org.clone();
    let user = args.user.clone();
    let auth_token = auth_token.clone();
    let sse_key = sse_key.unwrap_or([0; 32]);
    let fingerprint = ontology.fingerprint.0;
    let ontology_for_drain = ontology.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let mut server = server;
        if let Some(backend) = backend {
            // R6-B06: backend reads are not ready until a caller-filtered
            // graph image is installed. Retain the single writer, start the
            // authenticated continuous SSE loop, and wait for its first
            // atomic reseed before exposing MCP over stdio.
            let bearer = auth_token
                .clone()
                .expect("backend authentication validated before runtime startup");
            let mut sync_config =
                exocortex_client::sync::SseSyncConfig::new(backend.clone(), sse_key, fingerprint);
            sync_config.bearer = Some(bearer.clone());
            sync_config.client_key = Some(sse_key);
            sync_config.org = org.clone().into();
            let _sync = exocortex_client::sync::hydrate_and_start_backend_sync(
                sync_config,
                cache.clone(),
                writer_rx,
            )
            .await?;

            let endpoint = tonic::transport::Endpoint::from_shared(backend.clone())
                .map_err(|e| anyhow::anyhow!("bad --backend {backend}: {e}"))?;
            let channel = endpoint.connect_lazy();
            // W1 (audit): drain buffered offline writes to the backend —
            // each Pending entry is rebuilt, signed fresh, and settled via
            // the R13 classify table. Runs at startup and retries
            // transport-failed entries every 30s until quiescent.
            if wal.pending_count()? > 0 {
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
        serve_mcp_stdio(server).await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

/// Serve the MCP 2024-11-05 initialize flow used by rmcp 0.1.x while remaining
/// discoverable by SEP-2575 clients such as Crush v0.91.x. A method-not-found
/// response to `server/discover` is the specified signal for those clients to
/// fall back to the legacy initialize handshake.
async fn serve_mcp_stdio(server: mcp::ExocortexMcp) -> anyhow::Result<()> {
    // D27 (bug-prd-standalone-submit-hang): rmcp treats stdin EOF as an
    // immediate shutdown and drops in-flight tool-call tasks — a submit
    // still on the wire loses its response and the harness sees a hang.
    // The draining reader withholds EOF until every in-flight call has
    // answered (bounded by the end_session deadlines plus slack).
    let in_flight = server.in_flight_calls();
    let input = FrameLimitedReader::new(
        exocortex_client::eof_drain::EofDrainReader::new(
            tokio::io::stdin(),
            in_flight,
            exocortex_client::eof_drain::EOF_DRAIN_BUDGET,
        ),
        exocortex_wire::limits::MAX_MCP_REQUEST_BYTES,
    );
    let mut input = tokio::io::BufReader::new(input);
    let mut first = String::new();
    if input.read_line(&mut first).await? == 0 {
        anyhow::bail!("expect initialize or server/discover request");
    }

    let request: serde_json::Value = serde_json::from_str(&first)?;
    if request["method"] == "server/discover" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request.get("id").cloned().unwrap_or(serde_json::Value::Null),
            "error": {
                "code": -32601,
                "message": "Method not found"
            }
        });
        let mut output = tokio::io::stdout();
        output.write_all(response.to_string().as_bytes()).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;

        let service = server.serve((input, output)).await?;
        service.waiting().await?;
    } else {
        // rmcp owns the legacy handshake. Replay the line already consumed by
        // protocol detection, then preserve any bytes buffered behind it.
        let replay = std::io::Cursor::new(first.into_bytes()).chain(input);
        let service = server.serve((replay, tokio::io::stdout())).await?;
        service.waiting().await?;
    }
    Ok(())
}

/// Reject an oversized newline-delimited MCP frame while it is being read, so
/// an attacker cannot make the stdio server allocate an unbounded `String`
/// before JSON decoding. The byte count excludes the line-feed delimiter.
struct FrameLimitedReader<R> {
    inner: R,
    frame_bytes: usize,
    max_frame_bytes: usize,
}

impl<R> FrameLimitedReader<R> {
    fn new(inner: R, max_frame_bytes: usize) -> Self {
        Self {
            inner,
            frame_bytes: 0,
            max_frame_bytes,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for FrameLimitedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                for byte in &buf.filled()[filled_before..] {
                    if *byte == b'\n' {
                        self.frame_bytes = 0;
                    } else {
                        self.frame_bytes = self.frame_bytes.saturating_add(1);
                        if self.frame_bytes > self.max_frame_bytes {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "MCP request exceeds fixed byte ceiling",
                            )));
                        }
                    }
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
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
fn verify(
    args: &Args,
    ontology: &Ontology,
    data_dir: &std::path::Path,
    hmac_key_hex: Option<&str>,
    auth_token: Option<&str>,
) -> anyhow::Result<()> {
    let mut red = 0usize;

    // 0. Ontology identity (OC-PRD D1): the compatibility fingerprint
    // gates; the build fingerprint reports. Both printed so two
    // installs can be compared field by field.
    {
        use std::fmt::Write as _;
        let mut compat = String::with_capacity(64);
        for b in ontology.fingerprint.0 {
            let _ = write!(compat, "{b:02x}");
        }
        let mut build = String::with_capacity(64);
        for b in ontology.build_fingerprint.0 {
            let _ = write!(build, "{b:02x}");
        }
        println!("  ok    ontology: compatibility {compat}");
        println!("  ok    ontology: build         {build}");
    }

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
    match (&args.backend, hmac_key_hex) {
        (Some(_), None) => {
            red += 1;
            println!("  RED   hmac-key: --backend set but EXOCORTEX_HMAC_KEY missing");
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
            if let Some(token) = auth_token {
                if let Ok(v) = format!("Bearer {token}").parse() {
                    req.metadata_mut().insert("authorization", v);
                }
            }
            let fp = client
                .fingerprint(req)
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            // R-D5 client↔backend admission; the rule itself is the
            // kernel's peer policy (OC-PRD D2) — exact
            // compatibility-fingerprint equality.
            Ok(exocortex_kernel::admit_peer(&fp.fingerprint, &ontology.fingerprint.0).is_ok())
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
        let pending = wal.pending_count()?;
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

#[cfg(test)]
mod tests {
    use super::FrameLimitedReader;
    use tokio::io::AsyncBufReadExt;

    #[tokio::test]
    async fn mcp_frame_limit_accepts_just_under_and_boundary_then_rejects_plus_one() {
        let mut under_input = vec![b'x'; 7];
        under_input.push(b'\n');
        let mut reader = tokio::io::BufReader::new(FrameLimitedReader::new(
            std::io::Cursor::new(under_input),
            8,
        ));
        let mut line = String::new();
        assert_eq!(reader.read_line(&mut line).await.unwrap(), 8);

        let exact = vec![b'x'; 8];
        let mut exact_input = exact.clone();
        exact_input.push(b'\n');
        let mut reader = tokio::io::BufReader::new(FrameLimitedReader::new(
            std::io::Cursor::new(exact_input),
            8,
        ));
        let mut line = String::new();
        assert_eq!(reader.read_line(&mut line).await.unwrap(), 9);

        let mut oversized = exact;
        oversized.push(b'x');
        oversized.push(b'\n');
        let mut reader =
            tokio::io::BufReader::new(FrameLimitedReader::new(std::io::Cursor::new(oversized), 8));
        let mut line = String::new();
        let err = reader.read_line(&mut line).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
