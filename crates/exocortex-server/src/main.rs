//! `exocortex-node` — the single-artifact node binary (§4.2).
//!
//! `--mode mcp-standalone`: local, no backend; process-local FalkorDB via the
//! supervisor (§4.3). `--mode backend-node` / `--mode embedded` land with M5+.

mod corpus_export;
mod org_backup;
mod supervisor;

use exocortex_server::backend;

use std::net::SocketAddr;

use clap::Parser;

extern "C" {
    fn exocortex_required_ontology_pack_anchor();
}

fn require_linked_ontology_pack() {
    // SAFETY: the shipped ontology pack exports this no-argument anchor. It
    // has no inputs, output, or mutable state; its only purpose is linkage.
    unsafe { exocortex_required_ontology_pack_anchor() }
}

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
#[derive(Clone, Debug, Parser)]
#[command(name = "exocortex-node", version)]
struct Args {
    /// Internal acceptance probe: execute all nine rules in this artifact.
    #[arg(long, hide = true)]
    verify_rules: bool,
    /// Internal release probe: acquire, load, and execute the production model.
    #[arg(long, hide = true)]
    verify_embedder: bool,
    /// Deployment mode.
    #[arg(long, value_enum, default_value = "mcp-standalone")]
    mode: Mode,
    /// Node identity (lease tokens, envelopes, gossip). Defaults to
    /// `node-{pid}`; containers pass an explicit id (PIDs collide at 1).
    #[arg(long)]
    node_id: Option<String>,
    /// Storage selection: embedded falkordb, a networked URL
    /// (`falkor://host:port`), or `memory` — the non-durable in-memory
    /// backend for tests and throwaway dev topologies.
    #[arg(long, default_value = "falkordb-embedded")]
    storage: String,
    /// Exact organization served by this backend node and graph.
    #[arg(long, default_value = "org")]
    org: String,
    /// Bind address for networked modes. Non-loopback/shared binds require
    /// `--tls-cert` and `--tls-key`.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,
    /// PEM certificate chain for the shared HTTP/SSE/gRPC listener.
    #[arg(
        long,
        requires = "tls_key",
        conflicts_with = "allow_plaintext_loopback"
    )]
    tls_cert: Option<std::path::PathBuf>,
    /// PEM private key matching `--tls-cert`.
    #[arg(
        long,
        requires = "tls_cert",
        conflicts_with = "allow_plaintext_loopback"
    )]
    tls_key: Option<std::path::PathBuf>,
    /// Explicit local-development exception: allow plaintext only when
    /// `--bind` is an IP loopback address. Never valid for 0.0.0.0 or a LAN.
    #[arg(long)]
    allow_plaintext_loopback: bool,
    /// Cluster seed endpoints (backend-node).
    #[arg(long)]
    cluster_endpoints: Option<String>,
    /// Redis URL for the Dreams fire queue (backend-node; §12.2). Without
    /// it the node runs Dreams on the in-process fire channel only.
    #[arg(long)]
    redis_url: Option<String>,
    /// Explicit private-network exception for plaintext Falkor/Redis data
    /// planes. Public or untrusted networks must use falkors:// / rediss://.
    #[arg(long)]
    allow_private_network_plaintext_data_plane: bool,
    /// Preferred Dreams consolidation window in the org's canonical timezone
    /// (backend-node; R-Dr14, two-digit START-END).
    #[arg(long, default_value = "02-06")]
    quiet_hours: exocortex_dreams::fire::QuietHours,
    /// Fixed UTC offset, in minutes, for the org's canonical timezone.
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    quiet_hours_utc_offset_minutes: i16,
    /// Chitchat gossip listen address (backend-node).
    #[arg(long, default_value = "0.0.0.0:8100")]
    gossip_addr: String,
    /// Administrator-owned JSON mapping bearer credentials of at least 32
    /// bytes to org/user, project/team memberships, and maximum visibility.
    #[arg(long)]
    principal_policy: Option<std::path::PathBuf>,
    /// Administrator-owned JSON source policy. Required in backend-node
    /// mode, including when the policy is intentionally empty (`[]`).
    #[arg(long)]
    source_policy: Option<std::path::PathBuf>,
    /// Personal-mode user identity supplied by the installed wrapper.
    #[arg(long, hide = true, default_value = "dev")]
    standalone_user: String,
    /// Owner-only shell fragment used to hand the selected endpoint and SSE
    /// key to the installed wrapper.
    #[arg(long, hide = true)]
    standalone_runtime_file: Option<std::path::PathBuf>,
    /// Override the embedded Falkor data directory for isolated installs and
    /// acceptance tests.
    #[arg(long, hide = true)]
    standalone_data_dir: Option<std::path::PathBuf>,
    /// redis-server binary for the embedded supervisor.
    #[arg(long)]
    redis_server_bin: Option<std::path::PathBuf>,
    /// FalkorDB module path for the embedded supervisor.
    #[arg(long)]
    falkordb_module: Option<std::path::PathBuf>,
    /// FalkorDB graph name (backend-node and one-shot modes). Durable
    /// deployments MUST pin one: the default is stable per org, so a
    /// restart serves the same graph.
    #[arg(long)]
    graph_name: Option<String>,
    /// BR2 one-shot: export the org's graph to a JSON file, exit.
    #[arg(long)]
    export_org: Option<std::path::PathBuf>,
    /// BR2 one-shot: restore an org backup file into storage, exit.
    #[arg(long)]
    import_org: Option<std::path::PathBuf>,
    /// D22 one-shot: export a training-corpus cut (memories/edges/
    /// lineage JSONL + manifest) into a directory, exit.
    #[arg(long)]
    export_corpus: Option<std::path::PathBuf>,
    /// D22: the cut time for --export-corpus (RFC3339); absent = now.
    #[arg(long)]
    corpus_as_of: Option<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    // Refuse to link a production server with no ontology pack (§23 #25), then
    // force-link the pack's inventory registration.
    require_linked_ontology_pack();
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    // PX1 regression (found by the 0.3.0 release validation): a pack
    // crate nothing in the binary references has its inventory
    // registration dropped by the linker, so the node silently loaded
    // ONLY dev-v1 while every client loaded the composed set — the SSE
    // fingerprint check then fail-closed every standalone/backend client
    // at hydration. Every shipped pack needs a live reference here.
    let _ = std::hint::black_box(exocortex_pack_mortgage_v1::pack_def().name.clone());
    let _ = std::hint::black_box(exocortex_pack_study_v1::pack_def().name.clone());
    let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);
    if args.verify_embedder {
        return verify_production_embedder();
    }
    // BR2 one-shot modes: org backup/restore against the selected
    // storage, then exit (no cluster, no serving).
    if args.export_org.is_some() || args.import_org.is_some() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        return runtime.block_on(org_backup_main(args));
    }
    // D22 one-shot: the training-corpus cut, then exit.
    if let Some(dir) = args.export_corpus.clone() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        return runtime.block_on(corpus_export_main(args, dir));
    }

    match args.mode {
        Mode::McpStandalone => {
            if args.storage != "falkordb-embedded" {
                anyhow::bail!("mcp-standalone supports --storage=falkordb-embedded only");
            }
            standalone_main(args, ontology)
        }
        Mode::BackendNode => backend_node_main(args),
        Mode::Embedded => {
            anyhow::bail!("--mode embedded is the in-process path used by tests");
        }
    }
}

fn standalone_main(
    args: Args,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
) -> anyhow::Result<()> {
    let (bin, module) =
        supervisor::resolve_paths(args.redis_server_bin.clone(), args.falkordb_module.clone())?;
    // The data home is resolved FIRST: the attach decision (D44-S2) needs
    // to know where a live owner would have published its record.
    let data_home = args.standalone_data_dir.clone().unwrap_or(data_home()?);
    let cluster_secret =
        resolve_cluster_secret(std::env::var("EXOCORTEX_CLUSTER_SECRET").ok().as_deref())?;
    // §4.3 data-plane privacy: the store token is derived before spawning
    // so the server's --requirepass and the client URLs share one source.
    let store_token = exocortex_wire::signing::content_digest_hex(
        &exocortex_wire::signing::derive_supervised_store_token(&cluster_secret),
    );
    let port = supervisor::free_port()?;
    let cfg = supervisor::SupervisorConfig {
        redis_server_bin: bin,
        falkordb_module: module,
        port_file: Some(data_home.join("port")),
        data_dir: data_home.clone(),
        port,
        max_restarts: 3,
        auth_token: Some(store_token),
        supervisor_pid: None,
        startup_timeout: None,
        save_policy: None,
    };
    let mut supervised = match supervisor::spawn_supervised(&cfg) {
        Ok(supervised) => supervised,
        Err(error) => {
            // D44-S2: another live instance owns this data dir — ATTACH to
            // it in client mode instead of failing (or corrupting) — but
            // only once the owner's endpoint verifiably answers.
            if error.downcast_ref::<supervisor::DataDirOwned>().is_some() {
                return attach_to_live_node(&args, &ontology, &data_home);
            }
            return Err(error);
        }
    };
    tracing::info!(port = supervised.port, "embedded FalkorDB ready");
    if args.verify_rules {
        return verify_deployed_rules(&ontology, "mcp-standalone");
    }

    let producer_key = exocortex_wire::signing::decode_hex32(
        &std::env::var("EXOCORTEX_HMAC_KEY")
            .map_err(|_| anyhow::anyhow!("EXOCORTEX_HMAC_KEY is required for mcp-standalone"))?,
    )
    .map_err(anyhow::Error::msg)?;
    let bearer = std::env::var("EXOCORTEX_AUTH_TOKEN")
        .map_err(|_| anyhow::anyhow!("EXOCORTEX_AUTH_TOKEN is required for mcp-standalone"))?;
    let principal = exocortex_server::principal::PrincipalRegistry::single(
        bearer.clone(),
        exocortex_storage::VisibilityContext {
            user_id: args.standalone_user.clone().into(),
            org_id: args.org.clone().into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        },
    )?;
    let bind = if args.bind == "0.0.0.0:8080" {
        "127.0.0.1:0".to_owned()
    } else {
        args.bind.clone()
    };
    let address: std::net::SocketAddr = bind
        .parse()
        .map_err(|error| anyhow::anyhow!("bad standalone --bind: {error}"))?;
    anyhow::ensure!(
        address.ip().is_loopback(),
        "mcp-standalone backend bind must be loopback"
    );
    let (falkor_url, redis_url) =
        supervisor::supervised_store_urls(supervised.port, cfg.auth_token.as_deref());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let storage = std::sync::Arc::new(
            exocortex_storage::FalkorStorage::connect(
                exocortex_storage::FalkorConfig {
                    falkor_url,
                    redis_url: redis_url.clone(),
                    graph_name: args
                        .graph_name
                        .clone()
                        .unwrap_or_else(|| format!("exocortex-{}", args.org)),
                    org_id: args.org.clone().into(),
                    node_id: format!("standalone-{}", std::process::id()).into(),
                },
                ontology.clone(),
            )
            .await?,
        );
        let node_args = backend::BackendNodeArgs {
            org: args.org.clone(),
            bind,
            transport: backend::TransportSecurity::PlaintextLoopback,
            node_id: format!("standalone-{}", std::process::id()),
            cluster_secret,
            principals: std::sync::Arc::new(principal),
            gossip_listen: "127.0.0.1:0".parse().expect("literal socket address"),
            seed_nodes: Vec::new(),
            redis_url: Some(redis_url),
            quiet_hours: Default::default(),
            admin_source_policies: Vec::new(),
        };
        let mut node =
            backend::run_standalone_backend_node(storage, ontology, node_args, producer_key)
                .await?;
        // R14-final (SO-1): the publish and its removal guard are
        // declared at SERVE-LOOP scope — an earlier cut scoped the guard
        // inside the publish block, dropping it at startup and deleting
        // attach.json milliseconds after writing it (attach could never
        // succeed; only the final adversarial pass caught it).
        struct RemoveAttachOnExit(std::path::PathBuf);
        impl Drop for RemoveAttachOnExit {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(self.0.join("attach.json"));
            }
        }
        let _attach_guard = if let Some(path) = args.standalone_runtime_file.as_deref() {
            let sse_key = exocortex_wire::signing::derive_sse_client_key(&cluster_secret, &bearer);
            use std::fmt::Write as _;
            let mut sse_key_hex = String::with_capacity(64);
            for byte in sse_key {
                write!(sse_key_hex, "{byte:02x}").expect("writing to a string cannot fail");
            }
            let contents = format!(
                "EXOCORTEX_BACKEND='http://{}'\nEXOCORTEX_SSE_KEY='{sse_key_hex}'\n",
                node.local_addr
            );
            exocortex_storage::bounded_io::atomic_write_private(
                path,
                contents.as_bytes(),
                "standalone runtime",
            )?;
            // D44-S2: publish the attach record so a concurrent session
            // can ATTACH (client mode against this node) instead of
            // failing on the data-dir lock. 0600 beside the graph; the
            // data home is single-user in standalone mode. The org rides
            // the record (R14-final): an attacher running a different
            // --org must refuse, not hydrate the owner's graph.
            supervisor::AttachInfo {
                backend: format!("http://{}", node.local_addr),
                org: args.org.clone(),
                auth_token: bearer.clone(),
                hmac_key: std::env::var("EXOCORTEX_HMAC_KEY")?,
                sse_key: sse_key_hex,
            }
            .write(&data_home)?;
            Some(RemoveAttachOnExit(data_home.clone()))
        } else {
            None
        };
        tracing::info!(addr = %node.local_addr, "exocortex-node mcp-standalone ready");
        loop {
            tokio::select! {
                result = node.wait_for_ingress() => return result,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    supervised.poll(&cfg)?;
                }
            }
        }
    })
}

/// D44-S2: another live instance owns the data dir — attach to it. Reads
/// the owner's attach record, verifies the owner's node actually answers
/// with a matching ontology fingerprint (retrying while the owner is
/// still binding), publishes the wrapper's runtime env with the OWNER's
/// credentials, and exits 0: the wrapper then runs its MCP client
/// against the existing backend with a per-session WAL slot instead of
/// starting a second store. Fails closed if the owner never answers —
/// never starts a second store on one data dir.
fn attach_to_live_node(
    args: &Args,
    ontology: &std::sync::Arc<exocortex_kernel::Ontology>,
    data_home: &std::path::Path,
) -> anyhow::Result<()> {
    // Testable and tunable on slow machines (large real-data AOF boots).
    let deadline_ms: u64 = std::env::var("EXOCORTEX_ATTACH_DEADLINE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(deadline_ms);
    // R14: one runtime hoisted above the retry loop (a throwaway per
    // attempt was churn), and every attempt is time-bounded — a hung
    // owner (accepts, never answers) must not park block_on past the
    // 20s deadline the bail message promises.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("attach probe runtime: {e}"))?;
    loop {
        let attempt = supervisor::AttachInfo::read(data_home)
            .map_err(|e| e.to_string())
            .and_then(|info| {
                let info_copy = info.backend.clone();
                let probe = async {
                    let mut client =
                        exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::connect(
                            info.backend.clone(),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    let mut req =
                        tonic::Request::new(exocortex_wire::ingest::v1::FingerprintRequest {});
                    if let Ok(value) = format!("Bearer {}", info.auth_token).parse() {
                        req.metadata_mut().insert("authorization", value);
                    }
                    let fp = client
                        .fingerprint(req)
                        .await
                        .map_err(|e| e.to_string())?
                        .into_inner();
                    // OC-PRD D2: the ONE admission policy (the gate
                    // rejects raw fingerprint comparisons — and the
                    // round-14 cleanup flagged this copy diverging from
                    // verify()'s).
                    if let Err(error) = exocortex_kernel::admit_peer(
                        &fp.fingerprint,
                        &ontology.fingerprint.0,
                    ) {
                        return Err(format!("attach refused: owner ontology {error}"));
                    }
                    Ok(info)
                };
                // The timeout future must be CONSTRUCTED inside the
                // runtime (its Sleep needs a reactor at creation).
                match rt.block_on(async {
                    tokio::time::timeout(std::time::Duration::from_secs(3), probe).await
                }) {
                    Ok(outcome) => outcome,
                    Err(_) => Err(format!("owner at {info_copy} did not answer the probe within 3s")),
                }
            });
        match attempt {
            Ok(info) => {
                // R14-final (UQ4): the record names the owner's org — a
                // different --org must refuse, not attach into the
                // owner's graph under a phantom partition.
                if info.org != args.org {
                    anyhow::bail!(
                        "attach refused: the live instance on {} serves org {:?}, not {:?}",
                        data_home.display(),
                        info.org,
                        args.org
                    );
                }
                if let Some(path) = args.standalone_runtime_file.as_deref() {
                    exocortex_storage::bounded_io::atomic_write_private(
                        path,
                        info.runtime_env_lines().as_bytes(),
                        "standalone attach runtime",
                    )?;
                }
                tracing::warn!(
                    backend = %info.backend,
                    "attached to the live standalone node on this data dir (client mode; no second store)"
                );
                return Ok(());
            }
            Err(error) => {
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "cannot attach to the live instance on {}: {error} (the owner never answered within {deadline_ms}ms)",
                        data_home.display()
                    );
                }
                tracing::debug!(%error, "attach attempt failed; retrying");
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }
    }
}

fn verify_production_embedder() -> anyhow::Result<()> {
    #[cfg(feature = "fastembed")]
    {
        let embedder = exocortex_ingest::embedding::FastEmbedder::bge_small()
            .map_err(|error| anyhow::anyhow!("initialize bge-small embedder: {error}"))?;
        let vector = exocortex_ingest::embedding::Embedder::embed(
            &embedder,
            "exocortex production embedding probe",
        )
        .map_err(|error| anyhow::anyhow!("execute bge-small embedder: {error}"))?;
        anyhow::ensure!(
            vector.len() == 384 && vector.iter().all(|value| value.is_finite()),
            "bge-small embedder returned an invalid production vector"
        );
        // Golden output from the exact artifact revision above. This catches a
        // valid-but-wrong ONNX/tokenizer/pooling combination that shape and
        // digest checks alone cannot distinguish.
        const EXPECTED_PREFIX: [f32; 8] = [
            -0.049_560_662,
            0.057_678_916,
            0.072_846_055,
            -0.028_968_032,
            0.036_817_014,
            -0.001_249_432_3,
            -0.072_116_114,
            0.009_108_414,
        ];
        let max_error = vector
            .iter()
            .zip(EXPECTED_PREFIX)
            .map(|(actual, expected)| (actual - expected).abs())
            .fold(0.0_f32, f32::max);
        anyhow::ensure!(
            max_error <= 1.0e-4,
            "bge-small known-output mismatch (max prefix error {max_error})"
        );
        // The named-model constructor used before the offline artifact pin had
        // a 512-token window. Exercise content beyond token 384 so a mistaken
        // output-dimension/input-window substitution fails the release probe.
        let long_prefix = "the ".repeat(400);
        let long_vectors = exocortex_ingest::embedding::Embedder::embed_batch(
            &embedder,
            &[
                format!("{long_prefix}left-boundary"),
                format!("{long_prefix}right-boundary"),
            ],
        )
        .map_err(|error| anyhow::anyhow!("execute bge-small long-input probe: {error}"))?;
        anyhow::ensure!(
            long_vectors.len() == 2
                && long_vectors[0]
                    .iter()
                    .zip(&long_vectors[1])
                    .any(|(left, right)| left.to_bits() != right.to_bits()),
            "bge-small long-input truncation probe did not observe tokens beyond position 384"
        );
        println!(
            "embedder-ok model=bge-small version={} dim=384 max_tokens={}",
            exocortex_ingest::embedding::BGE_SMALL_VERSION,
            exocortex_ingest::embedding::BGE_SMALL_MAX_LENGTH
        );
        Ok(())
    }
    #[cfg(not(feature = "fastembed"))]
    anyhow::bail!("--verify-embedder requires the fastembed release feature")
}

/// BR2: one-shot org backup/restore against the selected storage.
/// Runs without the cluster (no leases to contend with) — the DR model
/// is an admin operation against quiesced storage.
async fn org_backup_main(args: Args) -> anyhow::Result<()> {
    let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);
    let fingerprint = {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(64);
        for b in ontology.fingerprint.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    };
    let org = args.org.as_str();
    let graph_name = args
        .graph_name
        .clone()
        .unwrap_or_else(|| format!("exocortex-{org}"));
    if let Some(path) = &args.export_org {
        if let Some((falkor_url, redis_url)) = resolve_falkor_urls(
            &args.storage,
            args.allow_private_network_plaintext_data_plane,
        )? {
            let storage = exocortex_storage::FalkorStorage::connect(
                exocortex_storage::FalkorConfig {
                    falkor_url,
                    redis_url,
                    graph_name: graph_name.clone(),
                    org_id: org.into(),
                    node_id: format!("node-{}", std::process::id()).into(),
                },
                ontology.clone(),
            )
            .await?;
            let (m, r) =
                org_backup::export_org(&storage, org, &fingerprint, &ontology.summary, path)
                    .await?;
            println!("{m} memories, {r} relationships -> {}", path.display());
        } else if args.storage == "memory" {
            anyhow::bail!("--storage=memory is non-durable; export from falkor:// instead");
        } else {
            anyhow::bail!("one-shot export needs --storage=falkor://host:port");
        }
        return Ok(());
    }
    if let Some(path) = &args.import_org {
        if let Some((falkor_url, redis_url)) = resolve_falkor_urls(
            &args.storage,
            args.allow_private_network_plaintext_data_plane,
        )? {
            let storage = exocortex_storage::FalkorStorage::connect(
                exocortex_storage::FalkorConfig {
                    falkor_url,
                    redis_url,
                    graph_name,
                    org_id: org.into(),
                    node_id: format!("node-{}", std::process::id()).into(),
                },
                ontology.clone(),
            )
            .await?;
            let report = org_backup::import_org(&storage, &ontology, org, path).await?;
            println!(
                "{} memories, {} relationships restored from {}",
                report.memories,
                report.relationships,
                path.display()
            );
        } else if args.storage == "memory" {
            anyhow::bail!("--storage=memory is non-durable; import targets falkor:// instead");
        } else {
            anyhow::bail!("one-shot import needs --storage=falkor://host:port");
        }
    }
    Ok(())
}

/// D22: `--export-corpus <dir> [--corpus-as-of RFC3339]` — stream the
/// bi-temporal cut out of the selected storage and exit.
async fn corpus_export_main(args: Args, dir: std::path::PathBuf) -> anyhow::Result<()> {
    require_linked_ontology_pack();
    let ontology = std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs()?);
    let as_of = match &args.corpus_as_of {
        Some(raw) => Some(
            chrono::DateTime::parse_from_rfc3339(raw)
                .map_err(|e| anyhow::anyhow!("--corpus-as-of is not RFC3339: {e}"))?
                .with_timezone(&chrono::Utc),
        ),
        None => None,
    };
    let org = args.org.as_str();
    let graph_name = args
        .graph_name
        .clone()
        .unwrap_or_else(|| format!("exocortex-{org}"));
    let Some((falkor_url, redis_url)) = resolve_falkor_urls(
        &args.storage,
        args.allow_private_network_plaintext_data_plane,
    )?
    else {
        anyhow::bail!("one-shot corpus export needs --storage=falkor://host:port")
    };
    let storage = exocortex_storage::FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            falkor_url,
            redis_url,
            graph_name,
            org_id: org.into(),
            node_id: format!("node-{}", std::process::id()).into(),
        },
        ontology.clone(),
    )
    .await?;
    let manifest = crate::corpus_export::export_corpus(&storage, &ontology, as_of, &dir).await?;
    println!(
        "{} memories, {} edges -> {} (as of {})",
        manifest.memories,
        manifest.edges,
        dir.display(),
        manifest.as_of.as_deref().unwrap_or("now")
    );
    Ok(())
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
        // Storage arms stay concrete (run_backend_node is generic over the
        // backend); a shared tail serves whichever arm won.
        let cluster_secret_value = std::env::var("EXOCORTEX_CLUSTER_SECRET").ok();
        let cluster_secret = resolve_cluster_secret(cluster_secret_value.as_deref())?;
        let principal_policy = args.principal_policy.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--principal-policy is required for backend-node")
        })?;
        let principals = std::sync::Arc::new(
            exocortex_server::principal::PrincipalRegistry::load(principal_policy)?,
        );
        principals.ensure_org(&args.org)?;
        let admin_source_policies = load_source_policy(args.source_policy.as_deref())?;
        ensure_source_policy_org(&admin_source_policies, &args.org)?;
        let transport = resolve_transport(
            &args.bind,
            args.tls_cert.as_deref(),
            args.tls_key.as_deref(),
            args.allow_plaintext_loopback,
        )?;
        let node_id = args
            .node_id
            .clone()
            .unwrap_or_else(|| format!("node-{}", std::process::id()));
        let graph_name = args
            .graph_name
            .clone()
            .unwrap_or_else(|| format!("exocortex-{}", args.org));
        let resolved_storage = resolve_falkor_urls(
            &args.storage,
            args.allow_private_network_plaintext_data_plane,
        )?;
        let redis_url = backend_redis_url(args.redis_url.as_deref(), resolved_storage.as_ref());
        if let Some(redis_url) = redis_url.as_deref() {
            validate_redis_url(
                redis_url,
                args.allow_private_network_plaintext_data_plane,
            )?;
        }
        let node_args = backend::BackendNodeArgs {
            org: args.org.clone(),
            bind: args.bind.clone(),
            transport,
            node_id: node_id.clone(),
            cluster_secret,
            principals,
            gossip_listen: SocketAddr::from_str(&args.gossip_addr)
                .map_err(|e| anyhow::anyhow!("bad --gossip-addr: {e}"))?,
            seed_nodes: args
                .cluster_endpoints
                .map(|eps| eps.split(',').map(str::to_string).collect())
                .unwrap_or_default(),
            redis_url,
            quiet_hours: args
                .quiet_hours
                .with_utc_offset_minutes(args.quiet_hours_utc_offset_minutes)?,
            admin_source_policies,
        };
        if let Some((falkor_url, redis_url)) = resolved_storage {
            let storage = std::sync::Arc::new(
                exocortex_storage::FalkorStorage::connect(
                    exocortex_storage::FalkorConfig {
                        falkor_url,
                        redis_url,
                        graph_name,
                        org_id: args.org.clone().into(),
                        node_id: node_id.into(),
                    },
                    ontology.clone(),
                )
                .await?,
            );
            serve_forever(storage, ontology, node_args, args.verify_rules).await
        } else if args.storage == "memory" {
            // Non-durable topology: same InMemoryStorage the in-process
            // tests use. CI/dev only — never production.
            let storage =
                std::sync::Arc::new(exocortex_storage::InMemoryStorage::new(ontology.clone()));
            serve_forever(storage, ontology, node_args, args.verify_rules).await
        } else {
            anyhow::bail!(
                "backend-node needs --storage=falkors://host:port, an explicitly admitted private falkor:// endpoint, or memory"
            );
        }
    })
}

/// Shared backend-node tail: run the node and idle until interrupted.
async fn serve_forever<S: exocortex_storage::Storage + 'static>(
    storage: std::sync::Arc<S>,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
    node_args: backend::BackendNodeArgs,
    verify_rules: bool,
) -> anyhow::Result<()> {
    let mut node = backend::run_backend_node(storage, ontology.clone(), node_args).await?;
    tracing::info!(addr = %node.local_addr, "backend-node up; serving until interrupted");
    if verify_rules {
        verify_deployed_rules(&ontology, "backend-node")?;
        return Ok(());
    }
    node.wait_for_ingress().await
}

fn verify_deployed_rules(
    ontology: &exocortex_kernel::Ontology,
    fallback_mode: &str,
) -> anyhow::Result<()> {
    exocortex_reasoning::acceptance::verify_nine_catalogued_rules(ontology)
        .map_err(anyhow::Error::msg)?;
    println!(
        "rules-ok mode={} count=9 artifact=exocortex-node",
        std::env::var("EXOCORTEX_DEPLOYMENT_MODE").unwrap_or_else(|_| fallback_mode.into())
    );
    Ok(())
}

/// Shared/backend mode never derives authentication material from a public
/// fallback. Local tests and fixtures pass explicit known values.
fn resolve_cluster_secret(secret: Option<&str>) -> anyhow::Result<[u8; 32]> {
    let secret = secret.filter(|value| !value.is_empty()).ok_or_else(|| {
        anyhow::anyhow!("EXOCORTEX_CLUSTER_SECRET is required for backend-node (64 hex chars)")
    })?;
    exocortex_wire::signing::decode_hex32(secret)
        .map_err(|e| anyhow::anyhow!("EXOCORTEX_CLUSTER_SECRET: {e}"))
}

fn data_plane_host_is_loopback(endpoint: &str) -> bool {
    use std::net::IpAddr;

    let authority = endpoint.split(['/', '?', '#']).next().unwrap_or(endpoint);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split_once(']').map(|(host, _)| host).unwrap_or("")
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn resolve_falkor_urls(
    storage: &str,
    allow_private_plaintext: bool,
) -> anyhow::Result<Option<(String, String)>> {
    if let Some(endpoint) = storage.strip_prefix("falkors://") {
        anyhow::ensure!(!endpoint.is_empty(), "falkors URL has no authority");
        return Ok(Some((storage.to_owned(), format!("rediss://{endpoint}"))));
    }
    let Some(endpoint) = storage.strip_prefix("falkor://") else {
        return Ok(None);
    };
    anyhow::ensure!(!endpoint.is_empty(), "falkor URL has no authority");
    anyhow::ensure!(
        data_plane_host_is_loopback(endpoint) || allow_private_plaintext,
        "remote plaintext Falkor requires --allow-private-network-plaintext-data-plane; prefer falkors://"
    );
    Ok(Some((storage.to_owned(), format!("redis://{endpoint}"))))
}

fn backend_redis_url(
    explicit: Option<&str>,
    storage_urls: Option<&(String, String)>,
) -> Option<String> {
    explicit
        .map(str::to_owned)
        .or_else(|| storage_urls.map(|(_, redis_url)| redis_url.clone()))
}

fn validate_redis_url(url: &str, allow_private_plaintext: bool) -> anyhow::Result<()> {
    if let Some(endpoint) = url.strip_prefix("rediss://") {
        anyhow::ensure!(!endpoint.is_empty(), "rediss URL has no authority");
        return Ok(());
    }
    let endpoint = url
        .strip_prefix("redis://")
        .ok_or_else(|| anyhow::anyhow!("Dreams Redis URL must use rediss:// or redis://"))?;
    anyhow::ensure!(
        data_plane_host_is_loopback(endpoint) || allow_private_plaintext,
        "remote plaintext Redis requires --allow-private-network-plaintext-data-plane; prefer rediss://"
    );
    Ok(())
}

fn resolve_transport(
    bind: &str,
    certificate: Option<&std::path::Path>,
    private_key: Option<&std::path::Path>,
    allow_plaintext_loopback: bool,
) -> anyhow::Result<backend::TransportSecurity> {
    match (certificate, private_key, allow_plaintext_loopback) {
        (Some(certificate), Some(private_key), false) => {
            Ok(backend::TransportSecurity::Tls {
                certificate: certificate.to_owned(),
                private_key: private_key.to_owned(),
            })
        }
        (None, None, true) => {
            let addr: SocketAddr = bind.parse().map_err(|_| {
                anyhow::anyhow!(
                    "--allow-plaintext-loopback requires an explicit IP loopback bind"
                )
            })?;
            if !addr.ip().is_loopback() {
                anyhow::bail!(
                    "--allow-plaintext-loopback refuses non-loopback bind {bind}; configure --tls-cert and --tls-key"
                );
            }
            Ok(backend::TransportSecurity::PlaintextLoopback)
        }
        (None, None, false) => anyhow::bail!(
            "backend-node requires --tls-cert and --tls-key; local plaintext requires --allow-plaintext-loopback with a loopback bind"
        ),
        _ => anyhow::bail!(
            "configure both --tls-cert and --tls-key, or neither with --allow-plaintext-loopback"
        ),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePolicyRow {
    org_id: String,
    source_uri: String,
    producer_id: String,
    ceiling: u8,
    producer_kind: i32,
    hmac_key: String,
}

type SourcePolicyKey = (String, String, String);
type SourcePolicyEntry = (
    SourcePolicyKey,
    exocortex_ingest::service::AdminSourcePolicy,
);

fn load_source_policy(path: Option<&std::path::Path>) -> anyhow::Result<Vec<SourcePolicyEntry>> {
    use std::io::Read as _;

    let path = path.ok_or_else(|| {
        anyhow::anyhow!("--source-policy is required for backend-node (use [] for no producers)")
    })?;
    let mut file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("open --source-policy {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = file
            .metadata()
            .map_err(|e| anyhow::anyhow!("inspect --source-policy {}: {e}", path.display()))?
            .permissions()
            .mode();
        anyhow::ensure!(
            mode & 0o077 == 0,
            "source policy {} must be owner-only (mode 0600 or stricter)",
            path.display()
        );
    }
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .map_err(|e| anyhow::anyhow!("read --source-policy {}: {e}", path.display()))?;
    let rows: Vec<SourcePolicyRow> = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parse --source-policy {}: {e}", path.display()))?;
    let mut seen = std::collections::HashSet::new();
    let mut seen_signing_keys = std::collections::HashSet::new();
    rows.into_iter()
        .map(|row| {
            anyhow::ensure!(
                !row.org_id.is_empty() && !row.source_uri.is_empty() && !row.producer_id.is_empty(),
                "source policy identities must be non-empty"
            );
            let visibility = match row.ceiling {
                0 => exocortex_kernel::Visibility::Private,
                1 => exocortex_kernel::Visibility::Project,
                2 => exocortex_kernel::Visibility::Team,
                3 => exocortex_kernel::Visibility::Org,
                4 => exocortex_kernel::Visibility::Public,
                other => anyhow::bail!("source policy ceiling {other} is outside 0..=4"),
            };
            let key = (row.org_id, row.source_uri, row.producer_id);
            anyhow::ensure!(
                seen.insert(key.clone()),
                "duplicate source policy entry: {key:?}"
            );
            let signing_key = exocortex_wire::signing::decode_hex32(&row.hmac_key)
                .map_err(|error| anyhow::anyhow!("source policy hmac_key: {error}"))?;
            anyhow::ensure!(
                seen_signing_keys.insert(signing_key),
                "source policy signing keys must be unique across producer identities"
            );
            let kind = match row.producer_kind {
                1 => exocortex_kernel::ProducerKind::CodingAgent,
                2 => exocortex_kernel::ProducerKind::ResearchAgent,
                3 => exocortex_kernel::ProducerKind::DocsAdapter,
                4 => exocortex_kernel::ProducerKind::AnalyticsAdapter,
                5 => exocortex_kernel::ProducerKind::Custom,
                6 => exocortex_kernel::ProducerKind::Extracted,
                7 => exocortex_kernel::ProducerKind::SaaSAdapter,
                _ => anyhow::bail!("source policy producer_kind is invalid"),
            };
            Ok((
                key,
                exocortex_ingest::service::AdminSourcePolicy {
                    ceiling: visibility,
                    kind,
                    signing_key,
                },
            ))
        })
        .collect()
}

fn ensure_source_policy_org(rows: &[SourcePolicyEntry], expected: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        rows.iter().all(|((org_id, _, _), _)| org_id == expected),
        "source policy contains an org other than node org {expected}"
    );
    Ok(())
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

#[cfg(test)]
mod tests {

    // §23 #25's anchor pins dev-v1; THIS pins the whole shipped set. The
    // linker drops an unreferenced pack crate's inventory registration
    // (the PX1 standalone regression), and only a test compiled into the
    // binary's own target links exactly like the binary — so this is the
    // one place a dropped pack is caught before release.
    #[test]
    fn the_binary_registers_every_shipped_pack() {
        let loaded = exocortex_kernel::pack::load_registered_packs().unwrap();
        let composed = exocortex_kernel::Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
            exocortex_pack_study_v1::pack_def(),
        ])
        .unwrap();
        let names = |o: &exocortex_kernel::Ontology| {
            o.packs
                .iter()
                .map(|p| p.name.to_string())
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(
            names(&loaded),
            names(&composed),
            "the binary must register every shipped pack"
        );
        assert_eq!(
            loaded.fingerprint, composed.fingerprint,
            "the binary's fingerprint must equal the composed set's"
        );
    }

    use super::{
        backend_redis_url, ensure_source_policy_org, load_source_policy, resolve_cluster_secret,
        resolve_falkor_urls, resolve_transport, validate_redis_url, Args,
    };
    use clap::Parser;

    #[test]
    fn quiet_hours_cli_preserves_window_default_and_canonical_timezone() {
        let defaults = Args::try_parse_from(["exocortex-node"]).unwrap();
        assert_eq!(defaults.quiet_hours.start_hour, 2);
        assert_eq!(defaults.quiet_hours.end_hour, 6);
        assert_eq!(defaults.quiet_hours_utc_offset_minutes, 0);

        let configured = Args::try_parse_from([
            "exocortex-node",
            "--quiet-hours",
            "23-07",
            "--quiet-hours-utc-offset-minutes",
            "-360",
        ])
        .unwrap();
        let configured = configured
            .quiet_hours
            .with_utc_offset_minutes(configured.quiet_hours_utc_offset_minutes)
            .unwrap();
        assert_eq!(configured.start_hour, 23);
        assert_eq!(configured.end_hour, 7);
        assert_eq!(configured.utc_offset_minutes, -360);

        assert!(Args::try_parse_from(["exocortex-node", "--quiet-hours", "2-6",]).is_err());
    }

    #[test]
    fn backend_credentials_fail_closed_when_missing_empty_or_malformed() {
        assert!(resolve_cluster_secret(None).is_err());
        assert!(resolve_cluster_secret(Some("")).is_err());
        assert!(resolve_cluster_secret(Some("42")).is_err());
        assert_eq!(
            resolve_cluster_secret(Some(&"42".repeat(32))).unwrap(),
            [0x42; 32]
        );
    }

    #[test]
    fn source_policy_is_required_and_validated_before_startup() {
        assert!(load_source_policy(None).is_err());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        std::fs::write(
            &path,
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":3,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
            let error = load_source_policy(Some(&path)).unwrap_err().to_string();
            assert!(error.contains("owner-only"), "{error}");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let rows = load_source_policy(Some(&path)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.signing_key, [0x42; 32]);
        assert!(ensure_source_policy_org(&rows, "org").is_ok());
        assert!(ensure_source_policy_org(&rows, "foreign").is_err());
        std::fs::write(
            &path,
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":3}]"#,
        )
        .unwrap();
        assert!(load_source_policy(Some(&path)).is_err());
        std::fs::write(
            &path,
            r#"[
                {"org_id":"org","source_uri":"s1","producer_id":"p1","ceiling":3,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"},
                {"org_id":"org","source_uri":"s2","producer_id":"p2","ceiling":3,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}
            ]"#,
        )
        .unwrap();
        let error = load_source_policy(Some(&path)).unwrap_err().to_string();
        assert!(error.contains("signing keys must be unique"), "{error}");
        std::fs::write(
            &path,
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":9,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
        )
        .unwrap();
        let error = load_source_policy(Some(&path)).unwrap_err().to_string();
        assert!(error.contains("ceiling"), "{error}");
        // R11-9: the admin table must accept every shipped kind and map
        // it to the right kernel variant — the exhaustive pin (a
        // renumbered proto or a swapped mapping fails here, not in
        // production provenance); kind 8+ stays fail-closed.
        for (kind, expected) in [
            (1, exocortex_kernel::ProducerKind::CodingAgent),
            (2, exocortex_kernel::ProducerKind::ResearchAgent),
            (3, exocortex_kernel::ProducerKind::DocsAdapter),
            (4, exocortex_kernel::ProducerKind::AnalyticsAdapter),
            (5, exocortex_kernel::ProducerKind::Custom),
            (6, exocortex_kernel::ProducerKind::Extracted),
            (7, exocortex_kernel::ProducerKind::SaaSAdapter),
        ] {
            std::fs::write(
                &path,
                format!(
                    r#"[{{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":3,"producer_kind":{kind},"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}}]"#
                ),
            )
            .unwrap();
            let rows = load_source_policy(Some(&path))
                .unwrap_or_else(|e| panic!("kind {kind} must provision: {e}"));
            assert_eq!(rows[0].1.kind, expected, "kind {kind} maps correctly");
        }
        std::fs::write(
            &path,
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":3,"producer_kind":8,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
        )
        .unwrap();
        assert!(
            load_source_policy(Some(&path)).is_err(),
            "kind 8 fails closed"
        );
    }

    #[test]
    fn shared_transport_requires_tls_and_plaintext_is_loopback_only() {
        use std::path::Path;

        assert!(resolve_transport("0.0.0.0:8080", None, None, false).is_err());
        assert!(resolve_transport("0.0.0.0:8080", None, None, true).is_err());
        assert!(resolve_transport("192.0.2.10:8080", None, None, true).is_err());
        assert!(resolve_transport("localhost:8080", None, None, true).is_err());
        assert!(matches!(
            resolve_transport("127.0.0.1:0", None, None, true).unwrap(),
            exocortex_server::backend::TransportSecurity::PlaintextLoopback
        ));
        assert!(matches!(
            resolve_transport(
                "0.0.0.0:8080",
                Some(Path::new("cert.pem")),
                Some(Path::new("key.pem")),
                false,
            )
            .unwrap(),
            exocortex_server::backend::TransportSecurity::Tls { .. }
        ));
        assert!(
            resolve_transport("0.0.0.0:8080", Some(Path::new("cert.pem")), None, false,).is_err()
        );
    }

    #[test]
    fn data_plane_urls_preserve_tls_and_require_an_explicit_plaintext_exception() {
        assert_eq!(
            resolve_falkor_urls("falkors://db.example:6379", false).unwrap(),
            Some((
                "falkors://db.example:6379".into(),
                "rediss://db.example:6379".into()
            ))
        );
        assert!(resolve_falkor_urls("falkor://db.example:6379", false).is_err());
        assert!(resolve_falkor_urls("falkor://db.example:6379", true).is_ok());
        assert!(resolve_falkor_urls("falkor://127.0.0.1:6379", false).is_ok());
        assert!(resolve_falkor_urls("falkor://user:secret@127.0.0.1:6379", false).is_ok());
        assert!(validate_redis_url("rediss://queue.example:6379", false).is_ok());
        assert!(validate_redis_url("redis://queue.example:6379", false).is_err());
        assert!(validate_redis_url("redis://queue.example:6379", true).is_ok());
    }

    #[test]
    fn falkor_backend_enables_its_shared_dreams_transport_by_default() {
        let storage_urls = resolve_falkor_urls("falkors://db.example:6379", false)
            .unwrap()
            .unwrap();
        assert_eq!(
            backend_redis_url(None, Some(&storage_urls)).as_deref(),
            Some("rediss://db.example:6379")
        );
        assert_eq!(
            backend_redis_url(Some("rediss://queue.example:6380"), Some(&storage_urls)).as_deref(),
            Some("rediss://queue.example:6380")
        );
        assert_eq!(backend_redis_url(None, None), None);
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;

    fn make_test_args() -> Args {
        use clap::Parser as _;
        Args::parse_from(["exocortex-node", "--org", "attach-org"])
    }

    /// R14 (A2): `attach_to_live_node` is covered WITHOUT the bundled
    /// runtime — an in-process owner node answers the gRPC probe, the
    /// runtime env is published with the owner's credentials, and a
    /// fingerprint mismatch fails closed instead of attaching.
    #[tokio::test]
    async fn attach_publishes_owner_env_and_refuses_mismatched_owners() {
        let ontology =
            std::sync::Arc::new(exocortex_kernel::pack::load_registered_packs().unwrap());
        let storage =
            std::sync::Arc::new(exocortex_storage::InMemoryStorage::new(ontology.clone()));
        let token = "1".repeat(64);
        let principal = exocortex_storage::VisibilityContext {
            user_id: "owner".into(),
            org_id: "attach-org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        };
        let args = backend::BackendNodeArgs {
            org: "attach-org".into(),
            bind: "127.0.0.1:0".into(),
            transport: backend::TransportSecurity::PlaintextLoopback,
            node_id: "attach-test-owner".into(),
            cluster_secret: [7u8; 32],
            principals: std::sync::Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(token.clone(), principal)
                    .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: Default::default(),
            admin_source_policies: vec![],
        };
        let node = backend::run_backend_node(storage, ontology.clone(), args)
            .await
            .unwrap();
        let dir = std::env::temp_dir().join(format!("exo-attach-live-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let runtime_file = dir.join("runtime.env");
        let client_args = Args {
            mode: Mode::McpStandalone,
            standalone_runtime_file: Some(runtime_file.clone()),
            ..make_test_args()
        };

        // Matching owner: the attach publishes the owner's env and exits 0.
        supervisor::AttachInfo {
            backend: format!("http://{}", node.local_addr),
            org: "attach-org".into(),
            auth_token: token.clone(),
            hmac_key: "2".repeat(64),
            sse_key: "3".repeat(64),
        }
        .write(&dir)
        .unwrap();
        // attach_to_live_node owns a current-thread runtime — production
        // calls it from sync main; the test must keep it off this async
        // context (dropping a runtime inside one panics).
        let attach_args = client_args.clone();
        let attach_dir = dir.clone();
        let attach_ontology = ontology.clone();
        tokio::task::spawn_blocking(move || {
            attach_to_live_node(&attach_args, &attach_ontology, &attach_dir)
        })
        .await
        .unwrap()
        .unwrap();
        let published = std::fs::read_to_string(&runtime_file).unwrap();
        assert!(published.contains("EXOCORTEX_ATTACHED='1'"));
        assert!(published.contains(&format!("EXOCORTEX_AUTH_TOKEN='{token}'")));
        assert!(published.contains(&format!("EXOCORTEX_BACKEND='http://{}'", node.local_addr)));

        // Mismatched owner (a DIFFERENT ontology): fails closed, publishes
        // nothing new, never attaches.
        let other = std::sync::Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![
                exocortex_pack_dev_v1::pack_def(),
                exocortex_pack_mortgage_v1::pack_def(),
            ])
            .unwrap(),
        );
        std::fs::remove_file(&runtime_file).unwrap();
        std::env::set_var("EXOCORTEX_ATTACH_DEADLINE_MS", "300");
        let refused_dir = dir.clone();
        let refused = tokio::task::spawn_blocking(move || {
            attach_to_live_node(&client_args, &other, &refused_dir)
        })
        .await
        .unwrap();
        std::env::remove_var("EXOCORTEX_ATTACH_DEADLINE_MS");
        assert!(refused.is_err(), "a fingerprint mismatch must refuse");
        assert!(
            !runtime_file.exists(),
            "a refused attach must not publish a runtime env"
        );
        drop(node); // Drop shuts the node's tasks down.
        let _ = std::fs::remove_dir_all(&dir);
    }
}
