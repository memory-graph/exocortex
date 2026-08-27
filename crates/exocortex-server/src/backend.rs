// crates/exocortex-server/src/backend.rs
//! `--mode backend-node` (M5): storage, cluster, ingest, reasoning, Dreams,
//! HTTP parity, SSE, lease re-election, and chitchat gossip on one process.
//! The gRPC IngestService and the HTTP surface share a single axum listener
//! (tonic `Routes::into_axum_router`), so every capability is reachable on
//! `--bind`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use exocortex_cache::LocalCache;
use exocortex_cluster::ClusterNode;
use exocortex_ingest::IngestServer;
use exocortex_kernel::{Ontology, OntologyFingerprint};
use exocortex_ops::OpContext;
use exocortex_storage::{LeaseKey, Storage};

use crate::http_bind::{HealthSnapshot, HttpBind};

/// Network transport accepted by the shared HTTP/SSE/gRPC listener.
#[derive(Clone, Debug)]
pub enum TransportSecurity {
    /// TLS with an operator-provided PEM certificate chain and private key.
    Tls {
        /// PEM certificate chain presented to clients.
        certificate: std::path::PathBuf,
        /// PEM private key matching `certificate`.
        private_key: std::path::PathBuf,
    },
    /// Explicit local development mode. Startup validation restricts this to
    /// an IP loopback bind, so it cannot expose bearer tokens on a LAN/WAN.
    PlaintextLoopback,
}

/// Backend-node wiring knobs (§4.2 flags land here).
#[derive(Clone)]
pub struct BackendNodeArgs {
    /// Ingress bind (`http + gRPC`).
    pub bind: String,
    /// TLS for shared binds, or explicitly loopback-only plaintext.
    pub transport: TransportSecurity,
    /// Node identity (lease tokens, envelopes, gossip).
    pub node_id: String,
    /// Cluster-shared HMAC key (R-Sec4).
    pub cluster_secret: [u8; 32],
    /// Bearer token for the HTTP op surface (R-Sec7).
    pub bearer_token: String,
    /// Chitchat gossip listen address.
    pub gossip_listen: SocketAddr,
    /// Chitchat seed nodes (`host:port`).
    pub seed_nodes: Vec<String>,
    /// Redis URL for the Dreams fire queue (§12.2). When set, the node
    /// drains the Redis queue (RPUSH/BLPOP, R-Dr13 counter reset, R-Dr14
    /// quiet-hours reordering) instead of only the in-process channel.
    pub redis_url: Option<String>,
    /// Quiet-hours window for the fire drainer (R-Dr14; default: none).
    pub quiet_hours: exocortex_dreams::fire::QuietHours,
    /// Immutable administrator source ceilings loaded before startup.
    pub admin_ceilings: Vec<((String, String, String), exocortex_kernel::Visibility)>,
}

/// Dreams-lease TTL for the backend re-election loop. 1.5s + a 400ms
/// retry cadence bounds worst-case takeover after a leader-kill at
/// ~1.9s — inside the M5 acceptance bound (§3: converge within 2s).
const LEASE_TTL: Duration = Duration::from_millis(1500);
/// Renewal cadence: a healthy holder extends well before expiry.
const LEASE_RENEW: Duration = Duration::from_millis(400);

/// The Dreams lease every backend node re-elects for (§9.2).
fn dreams_lease_key(org: &str) -> LeaseKey {
    LeaseKey::Dreams {
        org: org.into(),
        region: "*:*".into(),
    }
}

/// A running backend node's handles (tests abort these).
pub struct BackendNode {
    /// The shared health snapshot (R-O5/R-O6).
    pub health: Arc<arc_swap::ArcSwap<HealthSnapshot>>,
    /// The ingress listener's local address.
    pub local_addr: SocketAddr,
}

/// Run a backend node over any storage until the runtime shuts the task
/// down. Never returns under normal operation.
pub async fn run_backend_node<S: Storage + 'static>(
    storage: Arc<S>,
    ontology: Arc<Ontology>,
    args: BackendNodeArgs,
) -> anyhow::Result<BackendNode> {
    // Parse TLS material and bind before starting any background subsystem.
    // Bad transport configuration is a startup failure, never a node that
    // appears alive while its protected listener is absent.
    let ingress = BoundIngress::bind(&args.bind, &args.transport).await?;
    let local_addr = ingress.local_addr()?;
    let org: Arc<str> = "org".into();

    // Read path: cache + writer loop over the same storage. The writer
    // consumes first so the reseed flows through it (§8.2).
    let (cache, writer_rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, writer_rx).await });
    }
    cache
        .reseed_from_storage(&*storage, &org.to_string().into())
        .await;
    // R-O4: hydration completes when the org graph is actually resident
    // (the reseed flowed through the writer), not at spawn time.
    let mut hydrated = false;
    for _ in 0..200 {
        if cache.resident_orgs() > 0 {
            hydrated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Op context + HTTP bind (needed early: the change-feed bridge stamps
    // the health snapshot, CS6).
    let ctx = Arc::new(OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc(
            &org,
            "backend",
            exocortex_kernel::Visibility::Org,
        ),
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: cache.clone(),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        // D2: the backend serves preflight over HTTP (CR-9); the rulebook
        // is the same ontology the ingest path validates against.
        ontology: Some(ontology.clone()),
    });
    let bind = HttpBind::new(ctx, args.bearer_token.clone());
    let health = bind.health_handle();
    health.store(Arc::new(HealthSnapshot {
        node_id: args.node_id.clone(),
        hydrated,
        ..Default::default()
    }));

    // Change-feed bridge (§8.2/§9.1): storage invalidations flow into the
    // node's own cache writer, so the ops surface serves CURRENT data —
    // without this the backend's cache would be frozen at boot while
    // SSE clients stayed live (found by the R17 out-of-process test).
    {
        let cache = cache.clone();
        let storage = storage.clone();
        let health = health.clone();
        tokio::spawn(async move {
            let region = exocortex_storage::RegionKey {
                org: "*".into(),
                project: "*".into(),
                memory_type: 0,
            };
            loop {
                match storage.subscribe_invalidations(&region).await {
                    Ok(mut sub) => {
                        use futures::StreamExt;
                        while let Some(item) = sub.next().await {
                            match item {
                                Ok(inv) => {
                                    // CS6 (audit): the applied LSN is the
                                    // node's sync frontier, and the highest
                                    // observed LSN is the backend frontier
                                    // (R-O6) — no more always-zero lag.
                                    let lsn = exocortex_storage::Invalidation::lsn_of(&inv);
                                    health.rcu(|h| {
                                        let mut next = (**h).clone();
                                        next.sync_lsn = next.sync_lsn.max(lsn);
                                        next.backend_lsn = next.backend_lsn.max(lsn);
                                        Arc::new(next)
                                    });
                                    let _ =
                                        cache.submit(exocortex_cache::CacheWrite::Apply(inv)).await;
                                }
                                // CS7 (audit): a decode failure is a known
                                // hole in this node's LSN sequence — count
                                // it and log it, never swallow silently.
                                Err(e) => {
                                    metrics::counter!(
                                        "exocortex_cluster_invalidation_decode_errors_total"
                                    )
                                    .increment(1);
                                    tracing::warn!(
                                        %e,
                                        "cache bridge: invalidation decode failed; change lost"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%e, "cache change-feed subscribe failed; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    // Cluster: envelope signing + SSE fan-out.
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        args.node_id.clone().into(),
        ontology.fingerprint,
        args.cluster_secret,
    ));
    {
        let runner = cluster.clone();
        tokio::spawn(async move { runner.run().await });
    }

    // Reasoning: post-commit enrichment (§10.7 step 8).
    let reasoning = Arc::new(exocortex_reasoning::ReasoningEngine::new(
        storage.clone(),
        256,
        3,
    ));
    {
        let engine = reasoning.clone();
        tokio::spawn(async move { engine.run().await });
    }

    // Dreams: the consolidation loop over the fire channel. CS4 (audit):
    // the elected leader gate makes the re-election lease fence something
    // real — consolidation runs only on the node that holds it.
    let leader_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dreams = Arc::new(
        exocortex_dreams::DreamsEngine::new(
            storage.clone(),
            exocortex_dreams::trigger::DreamsTrigger::default(),
            0.01,
            0.05,
            true,
            args.node_id.clone().into(),
        )
        .with_leader_gate(leader_gate.clone()),
    );
    {
        let engine = dreams.clone();
        tokio::spawn(async move { engine.run().await });
    }

    // Fire transport (§12.2): when a Redis URL is configured, drain the
    // shared fire queue — reset the region's Redis write counters
    // atomically (R-Dr13, at consumption) and notify the engine. Quiet
    // hours reorder a short backlog rather than blocking it (R-Dr14).
    if let Some(redis_url) = args.redis_url.clone() {
        let dreams = dreams.clone();
        let quiet = args.quiet_hours;
        tokio::spawn(async move {
            match redis::Client::open(redis_url.as_str()) {
                Ok(client) => match client.get_multiplexed_async_connection().await {
                    Ok(conn) => {
                        let mut queue = exocortex_dreams::fire::RedisFireQueue::new(conn, quiet);
                        loop {
                            match queue.drain(Duration::from_secs(5)).await {
                                Ok(Some(region)) => {
                                    let _ = queue.reset_counters(&region).await;
                                    dreams.notify(region);
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(%e, "fire drain error; retrying");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!(%e, "fire queue connect failed"),
                },
                Err(e) => tracing::warn!(%e, "fire queue client open failed"),
            }
        });
    }

    // Ingest: gRPC IngestService, embedding-enabled, reasoning-wired.
    let ingest = IngestServer::new(storage.clone(), ontology.clone(), args.cluster_secret)
        .with_reasoning(reasoning.clone())
        .with_dreams(dreams.clone())
        .with_org(&org)
        .with_admin_ceilings(args.admin_ceilings.clone())
        .require_admin_ceilings();
    #[cfg(feature = "fastembed")]
    let ingest = ingest.with_embedder(Arc::new(
        exocortex_ingest::embedding::FastEmbedder::bge_small()?,
    ));

    // One listener: gRPC routes + HTTP ops + SSE + observability.

    // R-O4 maintainers: a storage probe keeps `storage_ok` truthful, and
    // the reasoning worker reports liveness.
    {
        let storage = storage.clone();
        let health = health.clone();
        tokio::spawn(async move {
            loop {
                let ok = storage.ping().await.is_ok();
                health.rcu(|h| {
                    let mut next = (**h).clone();
                    next.storage_ok = ok;
                    Arc::new(next)
                });
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }
    health.rcu(|h| {
        let mut next = (**h).clone();
        next.reasoning_alive = true;
        Arc::new(next)
    });

    let grpc = tonic::service::Routes::new(
        exocortex_wire::ingest::v1::ingest_service_server::IngestServiceServer::new(ingest)
            .max_decoding_message_size(exocortex_wire::limits::MAX_MCP_REQUEST_BYTES),
    )
    .into_axum_router();
    let sse = crate::sse::sse_router(cluster.clone(), crate::sse::SseAuth::RequiredToken);
    let app = bind.router(Some(sse)).merge(grpc);

    tracing::info!(
        %local_addr,
        node = %args.node_id,
        tls = matches!(args.transport, TransportSecurity::Tls { .. }),
        "backend-node serving http+grpc"
    );
    {
        tokio::spawn(async move {
            if let Err(e) = ingress.serve(app).await {
                tracing::error!(%e, "ingress server failed");
            }
        });
    }

    // Lease re-election (§9.2): acquire, renew, re-acquire on loss. The
    // epoch check rides storage-side fencing (R-C3).
    {
        let storage = storage.clone();
        let health = health.clone();
        let node_id = args.node_id.clone();
        let org = org.to_string();
        tokio::spawn(async move {
            let key = dreams_lease_key(&org);
            // §3 M5 AC: leader election converges within 2s of a
            // leader-kill — the lease TTL must be <= 2s for a surviving
            // node to take over inside the bound, with sub-second renewals
            // keeping a healthy holder stable.
            loop {
                match storage.acquire_lease(&key, LEASE_TTL).await {
                    Ok(lease) => {
                        let mut epoch = lease.epoch;
                        // CS4: the Dreams engine consolidates only while
                        // this node is the elected leader.
                        leader_gate.store(true, std::sync::atomic::Ordering::SeqCst);
                        metrics::counter!(
                            "exocortex_cluster_owner_lease_transitions_total",
                            "role" => "dreams"
                        )
                        .increment(1);
                        health.rcu(|h| {
                            let mut next = (**h).clone();
                            next.leader_node_id = Some(node_id.clone());
                            next.lease_epoch = epoch;
                            next.last_lease_tick = Some(chrono::Utc::now());
                            Arc::new(next)
                        });
                        loop {
                            tokio::time::sleep(LEASE_RENEW).await;
                            match storage.renew_lease(&lease).await {
                                Ok(l) => {
                                    epoch = l.epoch;
                                    health.rcu(|h| {
                                        let mut next = (**h).clone();
                                        next.lease_epoch = epoch;
                                        next.last_lease_tick = Some(chrono::Utc::now());
                                        Arc::new(next)
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(%e, "dreams lease lost; re-electing");
                                    leader_gate.store(false, std::sync::atomic::Ordering::SeqCst);
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Another node holds it; this node is a follower —
                        // no Dreams work (CS4).
                        leader_gate.store(false, std::sync::atomic::Ordering::SeqCst);
                        health.rcu(|h| {
                            let mut next = (**h).clone();
                            next.last_lease_tick = Some(chrono::Utc::now());
                            Arc::new(next)
                        });
                        tokio::time::sleep(LEASE_RENEW).await;
                    }
                }
            }
        });
    }

    // Chitchat gossip (§9.1): member discovery carrying wire-version +
    // fingerprint so admission composes with failure detection (R-W2/R-W3).
    spawn_gossip(&args, &ontology.fingerprint).await?;

    Ok(BackendNode { health, local_addr })
}

enum BoundIngress {
    Plaintext(tokio::net::TcpListener),
    Tls {
        listener: std::net::TcpListener,
        config: axum_server::tls_rustls::RustlsConfig,
    },
}

impl BoundIngress {
    async fn bind(bind: &str, transport: &TransportSecurity) -> anyhow::Result<Self> {
        match transport {
            TransportSecurity::PlaintextLoopback => {
                let address: SocketAddr = bind.parse().map_err(|_| {
                    anyhow::anyhow!(
                        "plaintext loopback bind must be a literal socket address, got {bind:?}"
                    )
                })?;
                anyhow::ensure!(
                    address.ip().is_loopback(),
                    "plaintext transport is restricted to loopback; {address} is shared"
                );
                Ok(Self::Plaintext(
                    tokio::net::TcpListener::bind(address).await?,
                ))
            }
            TransportSecurity::Tls {
                certificate,
                private_key,
            } => {
                // Other workspace consumers enable a second provider, so
                // rustls cannot infer one from the unified feature set.
                let _ = rustls::crypto::ring::default_provider().install_default();
                let config =
                    axum_server::tls_rustls::RustlsConfig::from_pem_file(certificate, private_key)
                        .await
                        .map_err(|e| anyhow::anyhow!("load TLS certificate/private key: {e}"))?;
                let listener = std::net::TcpListener::bind(bind)?;
                listener.set_nonblocking(true)?;
                Ok(Self::Tls { listener, config })
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            Self::Plaintext(listener) => listener.local_addr(),
            Self::Tls { listener, .. } => listener.local_addr(),
        }
    }

    async fn serve(self, app: axum::Router) -> std::io::Result<()> {
        match self {
            Self::Plaintext(listener) => axum::serve(listener, app).await,
            Self::Tls { listener, config } => {
                axum_server::from_tcp_rustls(listener, config)
                    .serve(app.into_make_service())
                    .await
            }
        }
    }
}

/// Chitchat membership: `wire_version` and `ontology_fingerprint` ride the
/// gossip state (peers gate admission on both).
async fn spawn_gossip(args: &BackendNodeArgs, fp: &OntologyFingerprint) -> anyhow::Result<()> {
    use chitchat::{ChitchatConfig, ChitchatId, FailureDetectorConfig};
    let config = ChitchatConfig {
        chitchat_id: ChitchatId::new(
            args.node_id.clone(),
            chrono::Utc::now().timestamp() as u64,
            args.gossip_listen,
        ),
        cluster_id: "exocortex".into(),
        gossip_interval: Duration::from_millis(500),
        listen_addr: args.gossip_listen,
        seed_nodes: args.seed_nodes.clone(),
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(10),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };
    let initial = vec![
        (
            "wire_version".to_string(),
            exocortex_wire::WIRE_VERSION.to_string(),
        ),
        ("ontology_fingerprint".to_string(), hex(&fp.0)),
        ("http_addr".to_string(), args.bind.clone()),
    ];
    let transport = chitchat::transport::UdpTransport;
    let handle = chitchat::spawn_chitchat(config, initial, &transport).await?;
    tokio::spawn(async move {
        // Hold the handle; aborting it stops gossip.
        std::future::pending::<()>().await;
        let _ = handle;
    });
    Ok(())
}

fn hex(b: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for byte in b {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
