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
use futures::{FutureExt, Stream, StreamExt};

use crate::http_bind::{HealthSnapshot, HttpBind};
use crate::principal::PrincipalRegistry;

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
    /// Exact organization served by this one-graph backend node.
    pub org: String,
    /// Ingress bind (`http + gRPC`).
    pub bind: String,
    /// TLS for shared binds, or explicitly loopback-only plaintext.
    pub transport: TransportSecurity,
    /// Node identity (lease tokens, envelopes, gossip).
    pub node_id: String,
    /// Cluster-shared HMAC key (R-Sec4).
    pub cluster_secret: [u8; 32],
    /// Immutable administrator credential-to-principal policy.
    pub principals: Arc<PrincipalRegistry>,
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
    /// Immutable exact producer signing/visibility policies loaded before startup.
    pub admin_source_policies: Vec<(
        exocortex_ingest::service::SourcePolicyKey,
        exocortex_ingest::service::AdminSourcePolicy,
    )>,
}

/// Dreams-lease TTL for the backend re-election loop. 1.2s + a 250ms
/// retry cadence bounds worst-case takeover after a leader-kill at
/// ~1.45s, leaving scheduler headroom inside the M5 acceptance bound
/// (§3: converge within 2s).
const LEASE_TTL: Duration = Duration::from_millis(1200);
/// Renewal cadence: a healthy holder extends well before expiry.
const LEASE_RENEW: Duration = Duration::from_millis(250);
const CACHE_BRIDGE_BURST: usize = 256;
const CACHE_RESEED_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const CACHE_RESEED_MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Default)]
struct BackgroundTasks(Vec<tokio::task::JoinHandle<()>>);

impl BackgroundTasks {
    fn push(&mut self, task: tokio::task::JoinHandle<()>) {
        self.0.push(task);
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for task in self.0.drain(..) {
            task.abort();
        }
    }
}

/// The Dreams lease every backend node re-elects for (§9.2).
fn dreams_lease_key(org: &str) -> LeaseKey {
    LeaseKey::Dreams {
        org: org.into(),
        region: "*:*".into(),
    }
}

fn mark_dreams_follower(
    elected: &std::sync::atomic::AtomicBool,
    health: &arc_swap::ArcSwap<HealthSnapshot>,
) {
    elected.store(false, std::sync::atomic::Ordering::SeqCst);
    health.rcu(|snapshot| {
        let mut next = (**snapshot).clone();
        next.leader_node_id = None;
        next.lease_epoch = 0;
        next.last_lease_tick = Some(chrono::Utc::now());
        Arc::new(next)
    });
}

/// A running backend node's handles (tests abort these).
pub struct BackendNode<S: Storage> {
    /// The shared health snapshot (R-O5/R-O6).
    pub health: Arc<arc_swap::ArcSwap<HealthSnapshot>>,
    /// The ingress listener's local address.
    pub local_addr: SocketAddr,
    /// Node-local cache, exposed for deterministic acceptance/readiness probes.
    pub cache: Arc<LocalCache>,
    /// True only while this node owns the cluster Dreams lease.
    pub leader_gate: Arc<std::sync::atomic::AtomicBool>,
    /// Production Dreams engine, exposed for lifecycle readiness and health.
    pub dreams: Arc<exocortex_dreams::DreamsEngine<S>>,
    /// Supervised reasoning engine, exposed for deterministic lifecycle tests.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub reasoning: Arc<exocortex_reasoning::ReasoningEngine<S>>,
    /// Live gossip handle retained for the backend process lifetime.
    pub gossip: chitchat::ChitchatHandle,
    cache_bridge: Option<tokio::task::JoinHandle<()>>,
    cluster_feed: Option<tokio::task::JoinHandle<()>>,
    post_ingest_effects: Option<tokio::task::JoinHandle<()>>,
    leader_election: Option<tokio::task::JoinHandle<()>>,
    ingress: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _background_tasks: BackgroundTasks,
}

impl<S: Storage> BackendNode<S> {
    /// Simulate leader process loss while leaving peer runtimes alive.
    pub fn stop_leader_election(&mut self) {
        mark_dreams_follower(&self.leader_gate, &self.health);
        if let Some(task) = self.leader_election.take() {
            task.abort();
        }
    }

    /// Wait for the ingress task to terminate. A running backend is expected
    /// to serve indefinitely, so every termination is a supervision failure.
    pub async fn wait_for_ingress(&mut self) -> anyhow::Result<()> {
        let task = self
            .ingress
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("backend ingress task is not running"))?;
        let result = task.await;
        self.ingress.take();
        match result {
            Ok(Ok(())) => anyhow::bail!("backend ingress stopped unexpectedly"),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(anyhow::anyhow!("backend ingress task failed: {error}")),
        }
    }
}

impl<S: Storage> Drop for BackendNode<S> {
    fn drop(&mut self) {
        self.leader_gate
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(task) = self.ingress.take() {
            task.abort();
        }
        if let Some(task) = self.cache_bridge.take() {
            task.abort();
        }
        if let Some(task) = self.cluster_feed.take() {
            task.abort();
        }
        if let Some(task) = self.post_ingest_effects.take() {
            task.abort();
        }
        if let Some(task) = self.leader_election.take() {
            task.abort();
        }
    }
}

async fn retry_with_capped_backoff<F, Fut, T, E>(
    mut operation: F,
    initial_delay: Duration,
    max_delay: Duration,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let ceiling = max_delay;
    let mut delay = initial_delay.min(ceiling);
    loop {
        match operation().await {
            Ok(value) => return value,
            Err(error) => {
                tracing::warn!(%error, ?delay, "cache reseed failed; retrying");
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(ceiling);
            }
        }
    }
}

async fn reseed_cache_with_retry<S: Storage>(
    cache: &LocalCache,
    storage: &S,
    org: &str,
    health: &arc_swap::ArcSwap<HealthSnapshot>,
    initial_delay: Duration,
    max_delay: Duration,
    observed_lsn: Option<u64>,
) {
    let org_id = org.to_owned();
    retry_with_capped_backoff(
        || {
            let org_id = org_id.clone();
            async move {
                cache
                    .reseed_from_storage(storage, &org_id.as_str().into())
                    .await
            }
        },
        initial_delay,
        max_delay,
    )
    .await;
    let published_lsn = cache
        .graphs_snapshot(org)
        .map_or(0, |snapshot| snapshot.last_backend_lsn);
    let synchronized_lsn = observed_lsn.map_or(published_lsn, |lsn| lsn.max(published_lsn));
    health.rcu(|current| {
        let mut next = (**current).clone();
        next.backend_lsn = next.backend_lsn.max(synchronized_lsn);
        next.sync_lsn = next.sync_lsn.max(synchronized_lsn);
        Arc::new(next)
    });
}

/// Apply a bounded observed feed burst as one acknowledged cache generation.
/// A point-hydration failure is repaired by an authoritative reseed before the
/// bridge acknowledges any event in the burst.
#[doc(hidden)]
pub async fn apply_cache_invalidations_with_retry<S: Storage>(
    cache: &LocalCache,
    storage: &S,
    org: &str,
    health: &arc_swap::ArcSwap<HealthSnapshot>,
    invalidations: Vec<exocortex_storage::Invalidation>,
    initial_delay: Duration,
    max_delay: Duration,
) {
    let Some(lsn) = invalidations
        .iter()
        .map(exocortex_storage::Invalidation::lsn_of)
        .max()
    else {
        return;
    };
    health.rcu(|current| {
        let mut next = (**current).clone();
        next.backend_lsn = next.backend_lsn.max(lsn);
        Arc::new(next)
    });
    match cache.apply_invalidations(invalidations).await {
        Ok(()) => {
            health.rcu(|current| {
                let mut next = (**current).clone();
                next.sync_lsn = next.sync_lsn.max(lsn);
                Arc::new(next)
            });
        }
        Err(error) => {
            tracing::warn!(%error, lsn, "cache invalidation burst failed; reseeding");
            reseed_cache_with_retry(
                cache,
                storage,
                org,
                health,
                initial_delay,
                max_delay,
                Some(lsn),
            )
            .await;
        }
    }
}

/// Consume one subscription epoch. Any decode error or clean termination is a
/// discontinuity: this returns only after an authoritative reseed succeeds, so
/// the caller cannot subscribe to and progress a later epoch from stale state.
#[doc(hidden)]
pub async fn consume_cache_subscription<S, St>(
    cache: &LocalCache,
    storage: &S,
    org: &str,
    health: &arc_swap::ArcSwap<HealthSnapshot>,
    mut subscription: St,
    initial_delay: Duration,
    max_delay: Duration,
) where
    S: Storage,
    St: Stream<Item = exocortex_storage::Result<exocortex_storage::Invalidation>> + Unpin,
{
    loop {
        let first = match subscription.next().await {
            Some(Ok(invalidation)) => invalidation,
            Some(Err(error)) => {
                metrics::counter!("exocortex_cluster_invalidation_decode_errors_total")
                    .increment(1);
                tracing::warn!(%error, "cache bridge stream failed; reseeding");
                reseed_cache_with_retry(
                    cache,
                    storage,
                    org,
                    health,
                    initial_delay,
                    max_delay,
                    None,
                )
                .await;
                return;
            }
            None => {
                tracing::warn!("cache bridge stream terminated; reseeding");
                reseed_cache_with_retry(
                    cache,
                    storage,
                    org,
                    health,
                    initial_delay,
                    max_delay,
                    None,
                )
                .await;
                return;
            }
        };

        let mut burst = Vec::with_capacity(CACHE_BRIDGE_BURST);
        burst.push(first);
        while burst.len() < CACHE_BRIDGE_BURST {
            match subscription.next().now_or_never() {
                Some(Some(Ok(invalidation))) => burst.push(invalidation),
                Some(Some(Err(error))) => {
                    metrics::counter!("exocortex_cluster_invalidation_decode_errors_total")
                        .increment(1);
                    tracing::warn!(%error, "cache bridge burst failed; reseeding");
                    reseed_cache_with_retry(
                        cache,
                        storage,
                        org,
                        health,
                        initial_delay,
                        max_delay,
                        None,
                    )
                    .await;
                    return;
                }
                Some(None) => {
                    tracing::warn!("cache bridge stream terminated during burst; reseeding");
                    reseed_cache_with_retry(
                        cache,
                        storage,
                        org,
                        health,
                        initial_delay,
                        max_delay,
                        None,
                    )
                    .await;
                    return;
                }
                None => break,
            }
        }
        apply_cache_invalidations_with_retry(
            cache,
            storage,
            org,
            health,
            burst,
            initial_delay,
            max_delay,
        )
        .await;
    }
}

/// Run a backend node over any storage until the runtime shuts the task
/// down. Never returns under normal operation.
pub async fn run_backend_node<S: Storage + 'static>(
    storage: Arc<S>,
    ontology: Arc<Ontology>,
    args: BackendNodeArgs,
) -> anyhow::Result<BackendNode<S>> {
    // Parse TLS material and bind before starting any background subsystem.
    // Bad transport configuration is a startup failure, never a node that
    // appears alive while its protected listener is absent.
    let ingress = BoundIngress::bind(&args.bind, &args.transport).await?;
    let local_addr = ingress.local_addr()?;
    let org: Arc<str> = args.org.clone().into();
    let mut background_tasks = BackgroundTasks::default();

    // Read path: cache + writer loop over the same storage. The writer
    // consumes first so the reseed flows through it (§8.2).
    let (cache, writer_rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        background_tasks.push(tokio::spawn(
            async move { cache.run(storage, writer_rx).await },
        ));
    }
    // Op context + HTTP bind are created before hydration because the
    // change-feed bridge stamps its acknowledged publication frontier here.
    let ctx = Arc::new(OpContext {
        visibility_ctx: exocortex_ops::operations::ops_vc(
            &org,
            "backend",
            exocortex_kernel::Visibility::Org,
        ),
        audit_admin: false,
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: cache.clone(),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        // D2: the backend serves preflight over HTTP (CR-9); the rulebook
        // is the same ontology the ingest path validates against.
        ontology: Some(ontology.clone()),
    });
    let bind = HttpBind::with_principals(ctx, args.principals.clone());
    let health = bind.health_handle();
    health.store(Arc::new(HealthSnapshot {
        node_id: args.node_id.clone(),
        ..Default::default()
    }));

    // Change-feed bridge (§8.2/§9.1): storage invalidations flow into the
    // node's own cache writer, so the ops surface serves CURRENT data —
    // without this the backend's cache would be frozen at boot while
    // SSE clients stayed live (found by the R17 out-of-process test).
    let (subscription_ready_tx, subscription_ready_rx) = tokio::sync::oneshot::channel();
    let (start_consuming_tx, start_consuming_rx) = tokio::sync::oneshot::channel();
    let cache_bridge = {
        let cache = cache.clone();
        let storage = storage.clone();
        let health = health.clone();
        let org = org.to_string();
        tokio::spawn(async move {
            let region = exocortex_storage::RegionKey {
                org: "*".into(),
                project: "*".into(),
                memory_type: 0,
            };
            let mut subscription_ready = Some(subscription_ready_tx);
            let mut start_consuming = Some(start_consuming_rx);
            loop {
                match storage.subscribe_invalidations(&region).await {
                    Ok(sub) => {
                        if let Some(ready) = subscription_ready.take() {
                            let _ = ready.send(());
                        }
                        // Retain the established subscription while the main
                        // startup path installs its authoritative image. Any
                        // concurrent deltas buffer behind this boundary and
                        // are drained immediately afterward.
                        if let Some(start) = start_consuming.take() {
                            if start.await.is_err() {
                                return;
                            }
                        }
                        consume_cache_subscription(
                            &cache,
                            &*storage,
                            &org,
                            &health,
                            sub,
                            CACHE_RESEED_INITIAL_BACKOFF,
                            CACHE_RESEED_MAX_BACKOFF,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(%e, "cache change-feed subscribe failed; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    };
    if subscription_ready_rx.await.is_err() {
        cache_bridge.abort();
        anyhow::bail!("cache change-feed supervisor stopped before subscription");
    }
    if let Err(error) = cache
        .reseed_from_storage(&*storage, &org.to_string().into())
        .await
    {
        cache_bridge.abort();
        return Err(error.into());
    }
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
    health.rcu(|snapshot| {
        let mut next = (**snapshot).clone();
        next.hydrated = hydrated;
        Arc::new(next)
    });
    let _ = start_consuming_tx.send(());

    // Cluster: envelope signing + SSE fan-out.
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        args.node_id.clone().into(),
        ontology.fingerprint,
        args.cluster_secret,
    ));
    let cluster_feed = {
        let runner = cluster.clone();
        let health = health.clone();
        let mut feed_health = cluster.subscribe_feed_health();
        tokio::spawn(async move {
            let monitor = async {
                loop {
                    let state = *feed_health.borrow_and_update();
                    health.rcu(|snapshot| {
                        let mut next = (**snapshot).clone();
                        next.cluster_feed_ready = state.ready;
                        next.cluster_feed_epoch = state.epoch;
                        next.cluster_feed_failures = state.failures;
                        Arc::new(next)
                    });
                    if feed_health.changed().await.is_err() {
                        break;
                    }
                }
            };
            let run = runner.run();
            tokio::pin!(monitor);
            tokio::pin!(run);
            tokio::select! {
                () = &mut monitor => {
                    tracing::error!("cluster feed health channel ended");
                }
                result = &mut run => {
                    if let Err(error) = result {
                        tracing::error!(%error, "cluster invalidation supervisor stopped");
                    }
                }
            }
            health.rcu(|snapshot| {
                let mut next = (**snapshot).clone();
                next.cluster_feed_ready = false;
                next.cluster_feed_failures = next.cluster_feed_failures.saturating_add(1);
                Arc::new(next)
            });
        })
    };

    // Reasoning: post-commit enrichment (§10.7 step 8).
    let reasoning = Arc::new(exocortex_reasoning::ReasoningEngine::new(
        storage.clone(),
        256,
        3,
    ));
    {
        let engine = reasoning.clone();
        let reasoning_health = health.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                reasoning_health.rcu(|snapshot| {
                    let mut next = (**snapshot).clone();
                    next.reasoning_alive = true;
                    Arc::new(next)
                });
                let outcome = std::panic::AssertUnwindSafe(engine.clone().run())
                    .catch_unwind()
                    .await;
                reasoning_health.rcu(|snapshot| {
                    let mut next = (**snapshot).clone();
                    next.reasoning_alive = false;
                    Arc::new(next)
                });
                match outcome {
                    Ok(()) => tracing::error!("reasoning worker exited; restarting"),
                    Err(_) => tracing::error!("reasoning worker panicked; restarting"),
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }));
    }

    // Shared Dreams transport uses separate Redis connections for blocking
    // drain and producer/ack traffic. A configured transport is mandatory:
    // startup fails rather than silently falling back to node-local fires.
    let (distributed_fire, fire_drainer) = if let Some(redis_url) = &args.redis_url {
        let client = redis::Client::open(redis_url.as_str())?;
        let producer = client.get_multiplexed_async_connection().await?;
        let drainer = client.get_multiplexed_async_connection().await?;
        (
            Some(Arc::new(tokio::sync::Mutex::new(
                exocortex_dreams::fire::RedisFireQueue::new(
                    producer,
                    args.quiet_hours,
                    args.org.clone(),
                ),
            ))),
            Some(exocortex_dreams::fire::RedisFireQueue::new(
                drainer,
                args.quiet_hours,
                args.org.clone(),
            )),
        )
    } else {
        (None, None)
    };

    // Dreams: the consolidation loop over the fire channel. CS4 (audit):
    // the elected leader gate makes the re-election lease fence something
    // real — consolidation runs only on the node that holds it.
    let leader_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut dreams_engine = exocortex_dreams::DreamsEngine::new(
        storage.clone(),
        exocortex_dreams::trigger::DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        args.node_id.clone().into(),
    )
    .with_leader_gate(leader_gate.clone());
    if let Some(queue) = &distributed_fire {
        dreams_engine = dreams_engine.with_distributed_fire(queue.clone());
    }
    let dreams = Arc::new(dreams_engine);
    {
        let engine = dreams.clone();
        background_tasks.push(tokio::spawn(async move { engine.run().await }));
    }

    // Only the elected owner drains shared fires. Notifications move through
    // a durable processing list; a successor requeues a dead owner's items
    // before advertising leadership below.
    if let Some(mut queue) = fire_drainer {
        let dreams = dreams.clone();
        let elected = leader_gate.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                if !elected.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(LEASE_RENEW).await;
                    continue;
                }
                match queue.drain(Duration::from_secs(5)).await {
                    Ok(exocortex_dreams::fire::DrainResult::Ready(notification)) => {
                        dreams.notify_distributed(notification);
                    }
                    Ok(exocortex_dreams::fire::DrainResult::Deferred) => {
                        tracing::debug!("Dreams fire durably reordered");
                    }
                    Ok(exocortex_dreams::fire::DrainResult::TimedOut) => {}
                    Err(e) => {
                        tracing::warn!(%e, "fire drain error; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }));
    }

    // Ingest: gRPC IngestService, embedding-enabled, reasoning-wired.
    let ingest = IngestServer::new_with_admin_policies(
        storage.clone(),
        ontology.clone(),
        args.admin_source_policies.clone(),
    )
    .with_reasoning(reasoning.clone())
    .with_dreams(dreams.clone())
    .with_org(&org)
    .require_request_principal();
    #[cfg(feature = "fastembed")]
    let ingest = ingest.with_embedder(Arc::new(
        exocortex_ingest::embedding::FastEmbedder::bge_small()
            .map_err(|error| anyhow::anyhow!("initialize bge-small embedder: {error}"))?,
    ));
    let post_ingest_effects = {
        let ingest = Arc::new(ingest.clone());
        tokio::spawn(async move { ingest.run_post_ingest_effects().await })
    };

    // One listener: gRPC routes + HTTP ops + SSE + observability.

    // R-O4 maintainers: a storage probe keeps `storage_ok` truthful, and
    // the reasoning worker reports liveness.
    {
        let storage = storage.clone();
        let health = health.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                let ok = storage.ping().await.is_ok();
                health.rcu(|h| {
                    let mut next = (**h).clone();
                    next.storage_ok = ok;
                    Arc::new(next)
                });
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }));
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
    let sse = crate::sse::sse_router(cluster.clone());
    // HTTP operations, SSE, metrics, and every gRPC method share the same
    // credential-to-principal middleware. Merging gRPC after `router` would
    // silently bypass authentication.
    let app = bind.router(Some(sse.merge(grpc)));

    tracing::info!(
        %local_addr,
        node = %args.node_id,
        tls = matches!(args.transport, TransportSecurity::Tls { .. }),
        "backend-node serving http+grpc"
    );
    let ingress =
        tokio::spawn(async move { ingress.serve(app).await.map_err(anyhow::Error::from) });

    // Lease re-election (§9.2): acquire, renew, re-acquire on loss. The
    // epoch check rides storage-side fencing (R-C3).
    let leader_election = {
        let storage = storage.clone();
        let health = health.clone();
        let node_id = args.node_id.clone();
        let org = org.to_string();
        let elected = leader_gate.clone();
        let distributed_fire = distributed_fire.clone();
        tokio::spawn(async move {
            let key = dreams_lease_key(&org);
            // §3 M5 AC: leader election converges within 2s of a
            // leader-kill — the lease TTL must be <= 2s for a surviving
            // node to take over inside the bound, with sub-second renewals
            // keeping a healthy holder stable.
            loop {
                match storage.acquire_lease(&key, LEASE_TTL).await {
                    Ok(lease) => {
                        if let Some(queue) = &distributed_fire {
                            if let Err(error) = queue.lock().await.recover_inflight().await {
                                tracing::warn!(%error, "Dreams in-flight recovery failed; refusing leadership");
                                let _ = storage.release_lease(lease).await;
                                tokio::time::sleep(LEASE_RENEW).await;
                                continue;
                            }
                        }
                        let mut epoch = lease.epoch;
                        // CS4: the Dreams engine consolidates only while
                        // this node is the elected leader.
                        elected.store(true, std::sync::atomic::Ordering::SeqCst);
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
                                    mark_dreams_follower(&elected, &health);
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Another node holds it; this node is a follower —
                        // no Dreams work (CS4).
                        mark_dreams_follower(&elected, &health);
                        tokio::time::sleep(LEASE_RENEW).await;
                    }
                }
            }
        })
    };

    // Chitchat gossip (§9.1): member discovery carrying wire-version +
    // fingerprint so admission composes with failure detection (R-W2/R-W3).
    let gossip = spawn_gossip(&args, &ontology.fingerprint).await?;

    Ok(BackendNode {
        health,
        local_addr,
        cache,
        leader_gate,
        dreams,
        #[cfg(debug_assertions)]
        reasoning,
        gossip,
        cache_bridge: Some(cache_bridge),
        cluster_feed: Some(cluster_feed),
        post_ingest_effects: Some(post_ingest_effects),
        leader_election: Some(leader_election),
        ingress: Some(ingress),
        _background_tasks: background_tasks,
    })
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
async fn spawn_gossip(
    args: &BackendNodeArgs,
    fp: &OntologyFingerprint,
) -> anyhow::Result<chitchat::ChitchatHandle> {
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
    chitchat::spawn_chitchat(config, initial, &transport).await
}

fn hex(b: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for byte in b {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod cache_bridge_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn failed_reseeds_back_off_exponentially_and_cap() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(exocortex_storage::InMemoryStorage::new(ontology));
        let (cache, writer_rx) = LocalCache::new(1024 * 1024);
        let cache = Arc::new(cache);
        let writer = tokio::spawn({
            let cache = cache.clone();
            let storage = storage.clone();
            async move { cache.run(storage, writer_rx).await }
        });
        let attempts = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        retry_with_capped_backoff(
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let cache = cache.clone();
                let storage = storage.clone();
                async move {
                    if attempt < 4 {
                        storage.fail_next_stream_after(Some(0), None);
                    }
                    cache.reseed_from_storage(&*storage, &"org".into()).await
                }
            },
            Duration::from_millis(10),
            Duration::from_millis(25),
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 5);
        assert_eq!(started.elapsed(), Duration::from_millis(80));
        assert_eq!(cache.resident_orgs(), 1);
        writer.abort();
    }
}
