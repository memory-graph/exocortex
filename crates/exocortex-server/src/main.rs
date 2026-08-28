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
    /// Quiet hours for Dreams firing (backend-node; R-Dr14, e.g. 23-7).
    #[arg(long)]
    quiet_hours: Option<u8>,
    /// Chitchat gossip listen address (backend-node).
    #[arg(long, default_value = "0.0.0.0:8100")]
    gossip_addr: String,
    /// Administrator-owned JSON mapping bearer credentials to org/user,
    /// project/team memberships, and maximum visibility.
    #[arg(long)]
    principal_policy: Option<std::path::PathBuf>,
    /// Administrator-owned JSON source policy. Required in backend-node
    /// mode, including when the policy is intentionally empty (`[]`).
    #[arg(long)]
    source_policy: Option<std::path::PathBuf>,
    /// redis-server binary for the embedded supervisor.
    #[arg(long)]
    redis_server_bin: Option<std::path::PathBuf>,
    /// FalkorDB module path for the embedded supervisor.
    #[arg(long)]
    falkordb_module: Option<std::path::PathBuf>,
    /// FalkorDB graph name (backend-node and one-shot modes). Durable
    /// deployments MUST pin one: the default is stable per org, so a
    /// restart serves the same graph.
    #[arg(long, default_value = "exocortex-org")]
    graph_name: String,
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
    if args.verify_rules {
        exocortex_reasoning::acceptance::verify_nine_catalogued_rules(&ontology)
            .map_err(anyhow::Error::msg)?;
        println!(
            "rules-ok mode={} count=9 artifact=exocortex-node",
            std::env::var("EXOCORTEX_DEPLOYMENT_MODE").unwrap_or_else(|_| "backend-node".into())
        );
        return Ok(());
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
            let (bin, module) = supervisor::resolve_paths(
                args.redis_server_bin.clone(),
                args.falkordb_module.clone(),
            )?;
            let port = supervisor::free_port()?;
            let data_home = data_home()?;
            let cfg = supervisor::SupervisorConfig {
                redis_server_bin: bin,
                falkordb_module: module,
                port_file: Some(data_home.join("port")),
                data_dir: data_home,
                port,
                max_restarts: 3,
            };
            let mut server = supervisor::spawn_supervised(&cfg)?;
            tracing::info!(port = server.port, "exocortex-node mcp-standalone ready");
            // CS5 (audit): a REAL supervision loop — try_wait, restart
            // within the policy, exit non-zero when the budget is spent.
            // (Drop kills the child, so the parent never orphans it.)
            let outcome: anyhow::Result<()> = (|| {
                server.supervise(&cfg)?;
                Ok(())
            })();
            match outcome {
                Ok(()) => unreachable!("supervise only returns on give-up"),
                Err(e) => {
                    tracing::error!(%e, "supervision gave up; exiting");
                    std::process::exit(1);
                }
            }
        }
        Mode::BackendNode => backend_node_main(args),
        Mode::Embedded => {
            anyhow::bail!("--mode embedded is the in-process path used by tests");
        }
    }
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
    let org = "org";
    if let Some(path) = &args.export_org {
        if let Some(url) = args.storage.strip_prefix("falkor://") {
            let storage = exocortex_storage::FalkorStorage::connect(
                exocortex_storage::FalkorConfig {
                    falkor_url: format!("falkor://{url}"),
                    redis_url: format!("redis://{url}"),
                    graph_name: args.graph_name.clone(),
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
        if let Some(url) = args.storage.strip_prefix("falkor://") {
            let storage = exocortex_storage::FalkorStorage::connect(
                exocortex_storage::FalkorConfig {
                    falkor_url: format!("falkor://{url}"),
                    redis_url: format!("redis://{url}"),
                    graph_name: args.graph_name.clone(),
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
        principals.ensure_org("org")?;
        let admin_source_policies = load_source_policy(args.source_policy.as_deref())?;
        ensure_source_policy_org(&admin_source_policies, "org")?;
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
        let node_args = backend::BackendNodeArgs {
            bind: args.bind.clone(),
            transport,
            node_id,
            cluster_secret,
            principals,
            gossip_listen: SocketAddr::from_str(&args.gossip_addr)
                .map_err(|e| anyhow::anyhow!("bad --gossip-addr: {e}"))?,
            seed_nodes: args
                .cluster_endpoints
                .map(|eps| eps.split(',').map(str::to_string).collect())
                .unwrap_or_default(),
            redis_url: args.redis_url.clone(),
            quiet_hours: match args.quiet_hours {
                Some(_) => exocortex_dreams::fire::QuietHours::nightly(),
                None => exocortex_dreams::fire::QuietHours::none(),
            },
            admin_source_policies,
        };
        if let Some(url) = args.storage.strip_prefix("falkor://") {
            let storage = std::sync::Arc::new(
                exocortex_storage::FalkorStorage::connect(
                    exocortex_storage::FalkorConfig {
                        falkor_url: format!("falkor://{url}"),
                        redis_url: format!("redis://{url}"),
                        graph_name: args.graph_name.clone(),
                        org_id: "org".into(),
                        node_id: format!("node-{}", std::process::id()).into(),
                    },
                    ontology.clone(),
                )
                .await?,
            );
            serve_forever(storage, ontology, node_args).await
        } else if args.storage == "memory" {
            // Non-durable topology: same InMemoryStorage the in-process
            // tests use. CI/dev only — never production.
            let storage =
                std::sync::Arc::new(exocortex_storage::InMemoryStorage::new(ontology.clone()));
            serve_forever(storage, ontology, node_args).await
        } else {
            anyhow::bail!(
                "backend-node needs --storage=falkor://host:port or memory (embedded storage is mcp-standalone)"
            );
        }
    })
}

/// Shared backend-node tail: run the node and idle until interrupted.
async fn serve_forever<S: exocortex_storage::Storage + 'static>(
    storage: std::sync::Arc<S>,
    ontology: std::sync::Arc<exocortex_kernel::Ontology>,
    node_args: backend::BackendNodeArgs,
) -> anyhow::Result<()> {
    let node = backend::run_backend_node(storage, ontology, node_args).await?;
    tracing::info!(addr = %node.local_addr, "backend-node up; serving until interrupted");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
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
    hmac_key: String,
}

type SourcePolicyKey = (String, String, String);
type SourcePolicyEntry = (
    SourcePolicyKey,
    exocortex_ingest::service::AdminSourcePolicy,
);

fn load_source_policy(path: Option<&std::path::Path>) -> anyhow::Result<Vec<SourcePolicyEntry>> {
    let path = path.ok_or_else(|| {
        anyhow::anyhow!("--source-policy is required for backend-node (use [] for no producers)")
    })?;
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read --source-policy {}: {e}", path.display()))?;
    let rows: Vec<SourcePolicyRow> = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parse --source-policy {}: {e}", path.display()))?;
    let mut seen = std::collections::HashSet::new();
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
            Ok((
                key,
                exocortex_ingest::service::AdminSourcePolicy {
                    ceiling: visibility,
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
        ensure_source_policy_org, load_source_policy, resolve_cluster_secret, resolve_transport,
    };

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
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":3,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
        )
        .unwrap();
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
            r#"[{"org_id":"org","source_uri":"s","producer_id":"p","ceiling":9,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
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
}
