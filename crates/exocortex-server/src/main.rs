//! `exocortex-node` — the single-artifact node binary (§4.2).
//!
//! `--mode mcp-standalone`: local, no backend; process-local FalkorDB via the
//! supervisor (§4.3). `--mode backend-node` / `--mode embedded` land with M5+.

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
#[derive(Debug, Parser)]
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
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    // Refuse to link a production server with no ontology pack (§23 #25), then
    // force-link the pack's inventory registration.
    require_linked_ontology_pack();
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
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
    let port = supervisor::free_port()?;
    let data_home = args.standalone_data_dir.clone().unwrap_or(data_home()?);
    let cfg = supervisor::SupervisorConfig {
        redis_server_bin: bin,
        falkordb_module: module,
        port_file: Some(data_home.join("port")),
        data_dir: data_home,
        port,
        max_restarts: 3,
    };
    let mut supervised = supervisor::spawn_supervised(&cfg)?;
    tracing::info!(port = supervised.port, "embedded FalkorDB ready");
    if args.verify_rules {
        return verify_deployed_rules(&ontology, "mcp-standalone");
    }

    let cluster_secret =
        resolve_cluster_secret(std::env::var("EXOCORTEX_CLUSTER_SECRET").ok().as_deref())?;
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
    let falkor_url = format!("falkor://127.0.0.1:{}", supervised.port);
    let redis_url = format!("redis://127.0.0.1:{}", supervised.port);
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
        if let Some(path) = args.standalone_runtime_file.as_deref() {
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
        }
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
            let (m, r) = org_backup::export_org(&storage, org, &fingerprint, path).await?;
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
        assert!(load_source_policy(Some(&path)).is_err());
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
