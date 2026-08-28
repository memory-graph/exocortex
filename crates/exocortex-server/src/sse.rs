//! The client-facing SSE change feed (§9.3, §9.7): one long-lived stream per
//! subscription; events carry protobuf-encoded envelopes as base64 payloads
//! with heartbeats so clients detect stalls (R-C5).
//!
//! R-Sec5: per-client SSE HMAC. For an authenticated subscriber the handler
//! derives a per-client key from its Authorization bearer
//! `HMAC(cluster_key, "sse-client:" || token)` and re-signs every envelope
//! with it. Compromised credentials revoke one subscriber, not the fleet.

use axum::extract::{RawQuery, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use futures::StreamExt;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::{ClusterError, ClusterNode, Replay};
use exocortex_kernel::{Memory, MemoryId, Relationship, RelationshipId};
use exocortex_storage::{
    memory_visible, relationship_visible, Invalidation, Storage, StorageError, VisibilityContext,
};
use exocortex_wire::cluster::v1::InvalidationEnvelope;
use exocortex_wire::sse::v1::invalidation::Kind;

#[derive(serde::Serialize)]
struct ClientGraphSnapshot {
    memories: Vec<Memory>,
    relationships: Vec<Relationship>,
}

const MAX_RESEED_ROWS: usize = 50_000;
const MAX_RESEED_BYTES: usize = 32 * 1024 * 1024;
const MAX_CONCURRENT_RESEEDS: usize = 2;
const MAX_LIVE_STREAMS_GLOBAL: usize = 256;
const MAX_LIVE_STREAMS_PER_PRINCIPAL: usize = 8;

struct SseState<S: Storage> {
    cluster: Arc<ClusterNode<S>>,
    reseeds: Arc<tokio::sync::Semaphore>,
    live_streams: LiveStreamAdmission,
    hydration: SharedEventHydration,
}

#[derive(Clone)]
struct LiveStreamAdmission {
    global: Arc<tokio::sync::Semaphore>,
    per_principal:
        Arc<std::sync::Mutex<std::collections::BTreeMap<[u8; 32], Arc<tokio::sync::Semaphore>>>>,
    per_principal_limit: usize,
}

struct LiveStreamPermits {
    _global: tokio::sync::OwnedSemaphorePermit,
    _principal: tokio::sync::OwnedSemaphorePermit,
}

impl LiveStreamAdmission {
    fn new(global_limit: usize, per_principal_limit: usize) -> Self {
        Self {
            global: Arc::new(tokio::sync::Semaphore::new(global_limit)),
            per_principal: Arc::new(std::sync::Mutex::new(Default::default())),
            per_principal_limit,
        }
    }

    fn try_admit(&self, principal_key: [u8; 32]) -> Result<LiveStreamPermits, ()> {
        let global = self.global.clone().try_acquire_owned().map_err(|_| ())?;
        let principal = {
            let mut entries = self
                .per_principal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .entry(principal_key)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(self.per_principal_limit)))
                .clone()
        };
        let principal = principal.try_acquire_owned().map_err(|_| ())?;
        Ok(LiveStreamPermits {
            _global: global,
            _principal: principal,
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HydrationKey {
    lsn: u64,
    kind: u8,
    id: Vec<u8>,
}

#[derive(Clone)]
enum HydratedEvent {
    Memory(Option<Memory>),
    Relationship(Option<(Relationship, Vec<Memory>)>),
    Discovery(Option<(exocortex_storage::DiscoveryRecord, Vec<Memory>)>),
}

#[derive(Default)]
struct SharedEventHydration {
    entries: std::sync::Mutex<HydrationEntries>,
}

type HydrationCell = Arc<tokio::sync::OnceCell<Result<Arc<HydratedEvent>, String>>>;
type HydrationEntries = std::collections::BTreeMap<HydrationKey, HydrationCell>;

/// The `/v1/changes` SSE router over a cluster node's local hub.
pub fn sse_router<S: Storage + 'static>(cluster: Arc<ClusterNode<S>>) -> axum::Router {
    sse_router_with_limits(
        cluster,
        MAX_LIVE_STREAMS_GLOBAL,
        MAX_LIVE_STREAMS_PER_PRINCIPAL,
    )
}

/// Build the SSE router with explicit live-stream limits for deterministic
/// admission/cancellation tests. Production callers use [`sse_router`].
#[doc(hidden)]
pub fn sse_router_with_limits<S: Storage + 'static>(
    cluster: Arc<ClusterNode<S>>,
    global_limit: usize,
    per_principal_limit: usize,
) -> axum::Router {
    axum::Router::new()
        .route("/v1/changes", axum::routing::get(handler))
        .with_state(Arc::new(SseState {
            cluster,
            reseeds: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RESEEDS)),
            live_streams: LiveStreamAdmission::new(global_limit, per_principal_limit),
            hydration: SharedEventHydration::default(),
        }))
}

/// Derive the per-client SSE key (R-Sec5): provisioned clients receive this
/// key out-of-band alongside their token.
pub fn derive_client_sse_key(cluster_key: &[u8; 32], token: &str) -> [u8; 32] {
    exocortex_wire::signing::derive_sse_client_key(cluster_key, token)
}

async fn handler<S: Storage + 'static>(
    State(state): State<Arc<SseState<S>>>,
    RawQuery(q): RawQuery,
    headers: http::HeaderMap,
    principal: Option<Extension<VisibilityContext>>,
) -> Response {
    let cluster = state.cluster.clone();
    let mut since_lsn = None;
    let mut seed = false;
    if let Some(qs) = q.as_deref() {
        for pair in qs.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                match k {
                    "since_lsn" => since_lsn = v.parse::<u64>().ok(),
                    "seed" => seed = v == "true",
                    _ => {}
                }
            }
        }
    }
    // R-Sec7 posture: on the backend op surface the feed is authenticated.
    let bearer = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty());
    let supports_additive_events = headers
        .get("x-exocortex-sse-version")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u32>().ok())
        .is_some_and(|version| version >= exocortex_wire::SSE_EVENT_VERSION);
    if principal.is_none() || bearer.is_none() {
        return (http::StatusCode::UNAUTHORIZED, "missing SSE credentials").into_response();
    }
    let principal_visibility = &principal.as_ref().expect("checked principal").0;
    let principal_identity = format!(
        "{}:{}{}:{}",
        principal_visibility.org_id.len(),
        principal_visibility.org_id,
        principal_visibility.user_id.len(),
        principal_visibility.user_id
    );
    let principal_key = derive_client_sse_key(&cluster.hmac_key, &principal_identity);
    let live_permits = match state.live_streams.try_admit(principal_key) {
        Ok(permits) => permits,
        Err(()) => {
            return (
                http::StatusCode::TOO_MANY_REQUESTS,
                "SSE live-stream concurrency limit reached",
            )
                .into_response();
        }
    };
    // Subscribe before building an initial image so commits concurrent with
    // hydration are buffered for delivery immediately after the snapshot.
    let rx = cluster.subscribe_local();
    let visibility = principal.map(|Extension(vc)| vc);
    let _reseed_permit = if seed {
        match state.reseeds.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return (
                    http::StatusCode::TOO_MANY_REQUESTS,
                    "SSE hydration concurrency limit reached",
                )
                    .into_response();
            }
        }
    } else {
        None
    };
    let initial_snapshot = if seed {
        match graph_reseed_envelope(&cluster, visibility.as_ref()).await {
            Ok(envelope) => Some(envelope),
            Err(error) => {
                tracing::warn!(?error, "SSE initial hydration failed");
                return http::StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    } else {
        None
    };
    // R-C6: a reconnect older than the replay window must reseed, not
    // silently skip deltas. The 409 carries the buffer floor so the client
    // can resume (and rehydrate the gap via its resync hook).
    let replay = if seed {
        vec![]
    } else {
        match since_lsn {
            Some(n) => match cluster.replay_since(n) {
                Replay::Fresh(envs) => envs,
                Replay::TooOld => {
                    let mut resp = (http::StatusCode::CONFLICT, "Resync Required").into_response();
                    if let Ok(v) = http::HeaderValue::from_str(&cluster.replay_floor().to_string())
                    {
                        resp.headers_mut().insert("x-exocortex-min-lsn", v);
                    }
                    return resp;
                }
            },
            None => vec![],
        }
    };

    let node_id = cluster.node_id.to_string();
    // R-Sec5: token → per-client key; envelopes re-sign with it.
    let client_key = bearer.map(|token| derive_client_sse_key(&cluster.hmac_key, token));
    let stream = async_stream::stream! {
        // Owned permits live exactly as long as the response stream. Client
        // cancellation drops the stream and releases both limits immediately.
        let _live_permits = live_permits;
        let mut rx = tokio_stream::wrappers::BroadcastStream::new(rx);
        // Initial comment anchors the connection before the first delta.
        yield Ok::<Event, Infallible>(Event::default().comment(format!("exocortex {node_id}")));
        if let Some(env) = initial_snapshot {
            let envelopes = match finalize_for_subscriber(
                &cluster,
                env,
                client_key.as_ref(),
                supports_additive_events,
            ) {
                Ok(envelopes) => envelopes,
                Err(error) => {
                    tracing::warn!(?error, "closing SSE stream after invalid initial envelope");
                    return;
                }
            };
            for env in envelopes {
                let payload = B64::encode(&prost_encode(&env));
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
        // R-C6 replay first (LSN order); the client's LSN gate dedups any
        // overlap with the live stream that follows.
        for env in replay {
            if let Err(error) = cluster.verify_hmac(&env) {
                tracing::warn!(?error, "closing SSE stream after invalid replay envelope");
                return;
            }
            let env = match prepare_for_subscriber(
                &cluster,
                &state.hydration,
                env,
                visibility.as_ref(),
            ).await {
                Ok(env) => env,
                Err(error) => {
                    tracing::warn!(?error, "closing SSE stream after visibility lookup failure");
                    return;
                }
            };
            let envelopes = match finalize_for_subscriber(
                &cluster,
                env,
                client_key.as_ref(),
                supports_additive_events,
            ) {
                Ok(envelopes) => envelopes,
                Err(error) => {
                    tracing::warn!(?error, "closing SSE stream after invalid prepared envelope");
                    return;
                }
            };
            for env in envelopes {
                let payload = B64::encode(&prost_encode(&env));
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
        while let Some(item) = rx.next().await {
            let env = match item {
                Ok(env) => env,
                Err(error) => {
                    tracing::warn!(?error, "closing lagged SSE stream for replay recovery");
                    return;
                }
            };
            if let Err(error) = cluster.verify_hmac(&env) {
                tracing::warn!(?error, "closing SSE stream after invalid live envelope");
                return;
            }
            let env = match prepare_for_subscriber(
                &cluster,
                &state.hydration,
                env,
                visibility.as_ref(),
            ).await {
                Ok(env) => env,
                Err(error) => {
                    tracing::warn!(?error, "closing SSE stream after visibility lookup failure");
                    return;
                }
            };
            let envelopes = match finalize_for_subscriber(
                &cluster,
                env,
                client_key.as_ref(),
                supports_additive_events,
            ) {
                Ok(envelopes) => envelopes,
                Err(error) => {
                    tracing::warn!(?error, "closing SSE stream after invalid prepared envelope");
                    return;
                }
            };
            for env in envelopes {
                let payload = B64::encode(&prost_encode(&env));
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(5)))
        .into_response()
}

fn finalize_for_subscriber<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    mut env: InvalidationEnvelope,
    client_key: Option<&[u8; 32]>,
    supports_additive_events: bool,
) -> Result<Vec<InvalidationEnvelope>, ClusterError> {
    // Never authenticate an envelope that did not arrive authenticated.
    cluster.verify_hmac(&env)?;
    let needs_progress_companion = env.inv.as_ref().is_some_and(|invalidation| {
        matches!(
            invalidation.kind.as_ref(),
            Some(Kind::VisibilityAdvance(_))
                | Some(Kind::GraphReseed(_))
                | Some(Kind::DiscoveryAvailable(_))
        )
    });
    if needs_progress_companion && !supports_additive_events {
        let lsn = env
            .inv
            .as_ref()
            .map(|invalidation| invalidation.backend_lsn)
            .unwrap_or(0);
        // UUIDv7 cannot be all zero, so this known pre-R6 deletion is a
        // harmless progress carrier. Pre-R6 clients never receive an unknown
        // oneof arm that would force a permanent reseed loop.
        let mut companion = cluster.envelope(Invalidation::MemoryDeleted {
            id: MemoryId([0; 16]),
            lsn,
        });
        if let Some(key) = client_key {
            resign(key, &mut companion);
        }
        return Ok(vec![companion]);
    }
    if let Some(key) = client_key {
        resign(key, &mut env);
    }
    Ok(vec![env])
}

/// Replace an invisible row event with a signed, identifier-free LSN advance.
/// Lookup failures close the stream instead: advancing past a row whose
/// visibility could not be established would permanently hide a visible
/// change from this subscriber.
async fn prepare_for_subscriber<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    hydration: &SharedEventHydration,
    env: InvalidationEnvelope,
    visibility: Option<&VisibilityContext>,
) -> Result<InvalidationEnvelope, StorageError> {
    let inv = env
        .inv
        .as_ref()
        .ok_or_else(|| StorageError::Backend("SSE envelope missing invalidation".into()))?;
    let lsn = inv.backend_lsn;
    let kind = inv.kind.clone();
    match kind.as_ref() {
        Some(Kind::MemoryUpserted(row)) => {
            let hydrated = hydration
                .load(
                    cluster.storage.as_ref(),
                    HydrationKey {
                        lsn,
                        kind: 1,
                        id: row.id.clone(),
                    },
                )
                .await?;
            let HydratedEvent::Memory(memory) = hydrated.as_ref() else {
                return Err(StorageError::Backend("SSE hydration kind mismatch".into()));
            };
            match memory
                .as_ref()
                .filter(|memory| visibility.is_none_or(|vc| memory_visible(memory, vc)))
            {
                Some(memory) => {
                    let mut memory = memory.clone();
                    // Embeddings are a storage/search implementation detail.
                    // They are intentionally absent from both initial seeds
                    // and live snapshots so client caches never retain them.
                    memory.embedding = None;
                    Ok(cluster.envelope(Invalidation::MemorySnapshotUpserted {
                        memory: Box::new(memory),
                        lsn,
                    }))
                }
                None => Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn })),
            }
        }
        Some(Kind::MemoryDeleted(row)) => {
            let hydrated = hydration
                .load(
                    cluster.storage.as_ref(),
                    HydrationKey {
                        lsn,
                        kind: 1,
                        id: row.id.clone(),
                    },
                )
                .await?;
            let HydratedEvent::Memory(memory) = hydrated.as_ref() else {
                return Err(StorageError::Backend("SSE hydration kind mismatch".into()));
            };
            if visibility.is_none_or(|vc| memory.as_ref().is_some_and(|m| memory_visible(m, vc))) {
                Ok(env)
            } else {
                Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn }))
            }
        }
        Some(Kind::RelationshipUpserted(row)) => {
            let hydrated = hydration
                .load(
                    cluster.storage.as_ref(),
                    HydrationKey {
                        lsn,
                        kind: 2,
                        id: row.id.clone(),
                    },
                )
                .await?;
            let HydratedEvent::Relationship(relationship) = hydrated.as_ref() else {
                return Err(StorageError::Backend("SSE hydration kind mismatch".into()));
            };
            let visible = relationship.as_ref().filter(|(relationship, endpoints)| {
                visibility.is_none_or(|vc| {
                    relationship_visible_with_endpoints(relationship, endpoints, vc)
                })
            });
            match visible {
                Some((relationship, _)) => Ok(cluster.envelope(
                    Invalidation::RelationshipSnapshotUpserted {
                        relationship: Box::new(relationship.clone()),
                        lsn,
                    },
                )),
                None => Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn })),
            }
        }
        Some(Kind::RelationshipDeleted(row)) => {
            let hydrated = hydration
                .load(
                    cluster.storage.as_ref(),
                    HydrationKey {
                        lsn,
                        kind: 2,
                        id: row.id.clone(),
                    },
                )
                .await?;
            let HydratedEvent::Relationship(relationship) = hydrated.as_ref() else {
                return Err(StorageError::Backend("SSE hydration kind mismatch".into()));
            };
            let visible = visibility.is_none_or(|vc| {
                relationship
                    .as_ref()
                    .is_some_and(|(relationship, endpoints)| {
                        relationship_visible_with_endpoints(relationship, endpoints, vc)
                    })
            });
            if visible {
                Ok(env)
            } else {
                Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn }))
            }
        }
        Some(Kind::DiscoveryAvailable(row)) => {
            let encoded_record: exocortex_storage::DiscoveryRecord =
                serde_json::from_slice(&row.record_json).map_err(|error| {
                    StorageError::Backend(format!("SSE discovery record malformed: {error}"))
                })?;
            let hydrated = hydration
                .load(
                    cluster.storage.as_ref(),
                    HydrationKey {
                        lsn,
                        kind: 3,
                        id: encoded_record.discovery_id.as_bytes().to_vec(),
                    },
                )
                .await?;
            let HydratedEvent::Discovery(discovery) = hydrated.as_ref() else {
                return Err(StorageError::Backend("SSE hydration kind mismatch".into()));
            };
            let visible = discovery.as_ref().is_some_and(|(record, endpoints)| {
                record == &encoded_record
                    && visibility.is_none_or(|vc| discovery_visible(record, endpoints, vc))
            });
            if visible {
                Ok(env)
            } else {
                Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn }))
            }
        }
        Some(Kind::VisibilityAdvance(_)) | Some(Kind::GraphReseed(_)) => Ok(env),
        None => Err(StorageError::Backend(
            "SSE invalidation missing event kind".into(),
        )),
    }
}

impl SharedEventHydration {
    async fn load<S: Storage>(
        &self,
        storage: &S,
        key: HydrationKey,
    ) -> Result<Arc<HydratedEvent>, StorageError> {
        self.load_with(
            key.clone(),
            || async move { hydrate_event(storage, &key).await },
        )
        .await
    }

    async fn load_with<F, Fut>(
        &self,
        key: HydrationKey,
        loader: F,
    ) -> Result<Arc<HydratedEvent>, StorageError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<HydratedEvent, StorageError>>,
    {
        const MAX_SHARED_EVENTS: usize = 1_024;
        let cell = {
            let mut entries = self.entries.lock().expect("SSE hydration cache poisoned");
            if let Some(cell) = entries.get(&key) {
                cell.clone()
            } else {
                if entries.len() >= MAX_SHARED_EVENTS {
                    if let Some(oldest) = entries.keys().next().cloned() {
                        entries.remove(&oldest);
                    }
                }
                let cell = Arc::new(tokio::sync::OnceCell::new());
                entries.insert(key.clone(), cell.clone());
                cell
            }
        };
        let result = cell
            .get_or_init(|| async {
                loader()
                    .await
                    .map(Arc::new)
                    .map_err(|error| error.to_string())
            })
            .await
            .clone();
        if result.is_err() {
            // A backend outage is recoverable. Do not turn one failed lookup
            // into a permanent reconnect failure for this LSN.
            let mut entries = self.entries.lock().expect("SSE hydration cache poisoned");
            if entries
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &cell))
            {
                entries.remove(&key);
            }
        }
        result.map_err(StorageError::Backend)
    }
}

async fn hydrate_event<S: Storage>(
    storage: &S,
    key: &HydrationKey,
) -> Result<HydratedEvent, StorageError> {
    match key.kind {
        1 => Ok(HydratedEvent::Memory(
            storage.get_memory(&MemoryId(decode_id(&key.id)?)).await?,
        )),
        2 => {
            let Some(relationship) = storage
                .get_relationship(&RelationshipId(decode_id(&key.id)?))
                .await?
            else {
                return Ok(HydratedEvent::Relationship(None));
            };
            let endpoints = storage
                .get_memories(&[relationship.from, relationship.to])
                .await?;
            Ok(HydratedEvent::Relationship(Some((relationship, endpoints))))
        }
        3 => {
            let discovery_id = std::str::from_utf8(&key.id)
                .map_err(|_| StorageError::Backend("discovery id is not UTF-8".into()))?;
            let Some(record) = storage.get_discovery(discovery_id).await? else {
                return Ok(HydratedEvent::Discovery(None));
            };
            let endpoints = storage.get_memories(&[record.from, record.to]).await?;
            Ok(HydratedEvent::Discovery(Some((record, endpoints))))
        }
        _ => Err(StorageError::Backend(
            "unsupported SSE hydration event kind".into(),
        )),
    }
}

fn discovery_visible(
    record: &exocortex_storage::DiscoveryRecord,
    endpoints: &[Memory],
    vc: &VisibilityContext,
) -> bool {
    if record.region.org != "*" && record.region.org.as_str() != vc.org_id.as_str() {
        return false;
    }
    if record.region.project != "*" && !vc.project_ids.contains(&record.region.project) {
        return false;
    }
    let from = endpoints.iter().find(|memory| memory.id == record.from);
    let to = endpoints.iter().find(|memory| memory.id == record.to);
    matches!((from, to), (Some(from), Some(to)) if memory_visible(from, vc) && memory_visible(to, vc))
}

fn relationship_visible_with_endpoints(
    relationship: &Relationship,
    endpoints: &[Memory],
    vc: &VisibilityContext,
) -> bool {
    let from = endpoints
        .iter()
        .find(|memory| memory.id == relationship.from);
    let to = endpoints.iter().find(|memory| memory.id == relationship.to);
    matches!((from, to), (Some(from), Some(to)) if relationship_visible(relationship, from, to, vc))
}

async fn graph_reseed_envelope<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    visibility: Option<&VisibilityContext>,
) -> Result<InvalidationEnvelope, StorageError> {
    let now = chrono::Utc::now();
    // Capture the authoritative frontier before either scan. The subscription
    // is already live, so every later commit is buffered. Advancing this
    // frontier from rows observed during the two scans could otherwise discard
    // a buffered endpoint upsert while retaining its later relationship.
    let frontier = cluster.storage.get_state_at(now).await?.backend_lsn;
    graph_reseed_envelope_at(cluster, visibility, now, frontier).await
}

async fn graph_reseed_envelope_at<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    visibility: Option<&VisibilityContext>,
    now: chrono::DateTime<chrono::Utc>,
    frontier: u64,
) -> Result<InvalidationEnvelope, StorageError> {
    // Keep endpoint indices into the output vector rather than cloning full
    // rows into a second map. Invisible/dead rows need no retained metadata:
    // an edge with either endpoint absent cannot be visible to this caller.
    let mut visible_memory_indices = std::collections::HashMap::new();
    let mut memories = Vec::new();
    // Conservative JSON framing allowance; individual rows are measured
    // before they are retained so the aggregate ceiling is an allocation
    // boundary, not merely a post-serialization response check.
    let mut retained_json_bytes = 64usize;
    let mut memory_rows_seen = 0usize;
    let mut memory_rows = cluster.storage.stream_all_memories().await;
    while let Some(row) = memory_rows.next().await {
        let mut memory = row?;
        memory_rows_seen += 1;
        ensure_reseed_budget(memory_rows_seen, retained_json_bytes)?;
        let live =
            memory.valid_until.is_none_or(|until| until > now) && memory.invalidated_by.is_none();
        let visible = visibility.is_none_or(|vc| memory_visible(&memory, vc));
        if live && visible {
            memory.embedding = None;
            retained_json_bytes = retained_json_bytes
                .checked_add(encoded_json_len(&memory)?)
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or_else(|| StorageError::Backend("SSE hydration byte count overflow".into()))?;
            ensure_reseed_budget(memory_rows_seen, retained_json_bytes)?;
            visible_memory_indices.insert(memory.id, memories.len());
            memories.push(memory);
        }
    }
    let mut relationships = Vec::new();
    let mut relationship_rows_seen = 0usize;
    let mut relationship_rows = cluster.storage.stream_all_relationships().await;
    while let Some(row) = relationship_rows.next().await {
        let relationship = row?;
        relationship_rows_seen += 1;
        let total_rows = memory_rows_seen
            .checked_add(relationship_rows_seen)
            .ok_or_else(|| StorageError::Backend("SSE hydration row count overflow".into()))?;
        ensure_reseed_budget(total_rows, retained_json_bytes)?;
        let live = relationship.valid_until.is_none_or(|until| until > now)
            && relationship.invalidated_by.is_none();
        if !live {
            continue;
        }
        let visible = match visibility {
            None => true,
            Some(vc) => match (
                visible_memory_indices.get(&relationship.from),
                visible_memory_indices.get(&relationship.to),
            ) {
                (Some(from), Some(to)) => {
                    relationship_visible(&relationship, &memories[*from], &memories[*to], vc)
                }
                _ => false,
            },
        };
        if visible {
            retained_json_bytes = retained_json_bytes
                .checked_add(encoded_json_len(&relationship)?)
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or_else(|| StorageError::Backend("SSE hydration byte count overflow".into()))?;
            ensure_reseed_budget(total_rows, retained_json_bytes)?;
            relationships.push(relationship);
        }
    }
    let snapshot_json = serde_json::to_vec(&ClientGraphSnapshot {
        memories,
        relationships,
    })
    .map_err(|error| StorageError::Backend(error.to_string()))?;
    ensure_reseed_budget(0, snapshot_json.len())?;
    Ok(cluster.envelope(Invalidation::GraphReseed {
        snapshot_json,
        lsn: frontier,
    }))
}

#[derive(Default)]
struct CountingWriter {
    len: usize,
    largest_external_write: usize,
    borrowed_range: Option<(usize, usize)>,
}

impl std::io::Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.len = self.len.checked_add(bytes.len()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::FileTooLarge, "JSON length overflow")
        })?;
        let start = bytes.as_ptr() as usize;
        let end = start.saturating_add(bytes.len());
        let borrowed = self
            .borrowed_range
            .is_some_and(|(source_start, source_end)| start >= source_start && end <= source_end);
        if !borrowed {
            self.largest_external_write = self.largest_external_write.max(bytes.len());
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encoded_json_metrics<T: serde::Serialize>(
    value: &T,
    borrowed_range: Option<(usize, usize)>,
) -> Result<(usize, usize), StorageError> {
    let mut counter = CountingWriter {
        borrowed_range,
        ..CountingWriter::default()
    };
    serde_json::to_writer(&mut counter, value)
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    Ok((counter.len, counter.largest_external_write))
}

fn encoded_json_len<T: serde::Serialize>(value: &T) -> Result<usize, StorageError> {
    encoded_json_metrics(value, None).map(|(len, _)| len)
}

fn ensure_reseed_budget(rows: usize, bytes: usize) -> Result<(), StorageError> {
    if rows > MAX_RESEED_ROWS {
        return Err(StorageError::Backend(format!(
            "SSE hydration exceeds {MAX_RESEED_ROWS} rows"
        )));
    }
    if bytes > MAX_RESEED_BYTES {
        return Err(StorageError::Backend(format!(
            "SSE hydration exceeds {MAX_RESEED_BYTES} bytes"
        )));
    }
    Ok(())
}

fn decode_id(raw: &[u8]) -> Result<[u8; 16], StorageError> {
    if raw.len() == 16 {
        return Ok(raw.try_into().expect("length checked"));
    }
    if raw.len() != 32 {
        return Err(StorageError::Backend(
            "SSE invalidation id has invalid width".into(),
        ));
    }
    let mut out = [0u8; 16];
    for (index, byte) in out.iter_mut().enumerate() {
        let hi = (raw[index * 2] as char).to_digit(16);
        let lo = (raw[index * 2 + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => *byte = ((hi << 4) | lo) as u8,
            _ => {
                return Err(StorageError::Backend(
                    "SSE invalidation id is not hexadecimal".into(),
                ));
            }
        }
    }
    Ok(out)
}

/// Re-sign an envelope in place with a per-client key (R-Sec5).
fn resign(key: &[u8; 32], env: &mut exocortex_wire::cluster::v1::InvalidationEnvelope) {
    exocortex_wire::signing::sign_invalidation_envelope(key, env);
}

// Local base64 (standard library has none; hand-rolled 30-liner to avoid a
// new dependency — recorded in the M5 report).
struct B64;

impl B64 {
    const T: &'static [u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    fn encode(data: &[u8]) -> String {
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let b = [
                chunk[0],
                chunk.get(1).copied().unwrap_or(0),
                chunk.get(2).copied().unwrap_or(0),
            ];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            out.push(Self::T[(n >> 18) as usize & 63] as char);
            out.push(Self::T[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                Self::T[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                Self::T[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }
}
fn prost_encode(env: &exocortex_wire::cluster::v1::InvalidationEnvelope) -> Vec<u8> {
    use prost::Message;
    env.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_reseed_budget, finalize_for_subscriber, graph_reseed_envelope_at, prost_encode,
        MAX_RESEED_BYTES, MAX_RESEED_ROWS,
    };
    use exocortex_cluster::ClusterNode;
    use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
    use exocortex_storage::{
        DiscoveryRecord, InMemoryStorage, Invalidation, RegionKey, Storage, VisibilityContext,
    };
    use std::sync::Arc;

    #[derive(Clone, PartialEq, prost::Message)]
    struct FrozenEnvelope {
        #[prost(message, optional, tag = "4")]
        inv: Option<FrozenInvalidation>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FrozenInvalidation {
        #[prost(oneof = "frozen_invalidation::Kind", tags = "1, 2, 3, 4")]
        kind: Option<frozen_invalidation::Kind>,
        #[prost(uint64, tag = "10")]
        backend_lsn: u64,
    }

    mod frozen_invalidation {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Kind {
            #[prost(message, tag = "1")]
            MemoryUpserted(super::FrozenMemoryUpserted),
            #[prost(message, tag = "2")]
            MemoryDeleted(super::FrozenMemoryDeleted),
            #[prost(message, tag = "3")]
            RelationshipUpserted(super::FrozenEmpty),
            #[prost(message, tag = "4")]
            RelationshipDeleted(super::FrozenEmpty),
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FrozenMemoryUpserted {
        #[prost(bytes = "vec", tag = "1")]
        id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        snapshot_json: Vec<u8>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FrozenMemoryDeleted {
        #[prost(bytes = "vec", tag = "1")]
        id: Vec<u8>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FrozenEmpty {}

    #[test]
    fn hydration_budget_is_inclusive_and_fail_closed() {
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS, MAX_RESEED_BYTES).is_ok());
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS + 1, 0).is_err());
        assert!(ensure_reseed_budget(0, MAX_RESEED_BYTES + 1).is_err());
    }

    #[test]
    fn additive_events_preserve_progress_for_a_frozen_pre_r6_decoder() {
        use prost::Message as _;

        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let cluster = ClusterNode::new(
            Arc::new(InMemoryStorage::new(ontology.clone())),
            "compat".into(),
            ontology.fingerprint,
            [8; 32],
        );
        let now = chrono::Utc::now();
        let record = DiscoveryRecord {
            discovery_id: "compat-discovery".into(),
            region: RegionKey {
                org: "org".into(),
                project: "project".into(),
                memory_type: 3,
            },
            from: MemoryId::new_v7(),
            to: MemoryId::new_v7(),
            discovery_type: "transitive".into(),
            quality: 0.8,
            via_types: [1, 2],
            discovery_cycle_id: "cycle".into(),
            discovered_at: now,
        };
        let events = [
            Invalidation::VisibilityAdvance { lsn: 31 },
            Invalidation::GraphReseed {
                snapshot_json: br#"{"memories":[],"relationships":[]}"#.to_vec(),
                lsn: 32,
            },
            Invalidation::DiscoveryAvailable { record, lsn: 33 },
        ];

        for (index, event) in events.into_iter().enumerate() {
            let expected_lsn = 31 + index as u64;
            let typed_envelope = cluster.envelope(event);
            let typed = finalize_for_subscriber(&cluster, typed_envelope.clone(), None, true)
                .expect("current-client envelope");
            assert_eq!(typed.len(), 1);
            assert!(matches!(
                typed[0]
                    .inv
                    .as_ref()
                    .and_then(|invalidation| invalidation.kind.as_ref()),
                Some(
                    exocortex_wire::sse::v1::invalidation::Kind::VisibilityAdvance(_)
                        | exocortex_wire::sse::v1::invalidation::Kind::GraphReseed(_)
                        | exocortex_wire::sse::v1::invalidation::Kind::DiscoveryAvailable(_)
                )
            ));

            let envelopes = finalize_for_subscriber(&cluster, typed_envelope, None, false)
                .expect("authenticated compatibility envelope");
            assert_eq!(envelopes.len(), 1);
            for envelope in &envelopes {
                cluster.verify_hmac(envelope).expect("bridge HMAC");
            }
            let companion = FrozenEnvelope::decode(prost_encode(&envelopes[0]).as_slice())
                .expect("pre-R6 decoder");
            let companion = companion.inv.expect("progress invalidation");
            assert_eq!(companion.backend_lsn, expected_lsn);
            let frozen_invalidation::Kind::MemoryDeleted(carrier) =
                companion.kind.expect("known progress arm")
            else {
                panic!("new event lacked a frozen-schema progress companion");
            };
            assert_eq!(carrier.id, b"00000000000000000000000000000000");
        }
    }

    #[test]
    fn unsigned_envelopes_are_not_resigned_for_subscribers() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let cluster = ClusterNode::new(
            Arc::new(InMemoryStorage::new(ontology.clone())),
            "auth".into(),
            ontology.fingerprint,
            [8; 32],
        );
        let mut envelope = cluster.envelope(Invalidation::VisibilityAdvance { lsn: 41 });
        envelope.hmac.clear();
        assert!(finalize_for_subscriber(&cluster, envelope, Some(&[9; 32]), true).is_err());
    }

    #[test]
    fn reseed_byte_counter_matches_serialized_output_without_retaining_it() {
        let payload = "x".repeat(512 * 1024);
        let value = serde_json::json!({"memories": [payload], "relationships": []});
        let encoded = serde_json::to_vec(&value).unwrap();
        let source = value["memories"][0].as_str().unwrap();
        let source_start = source.as_ptr() as usize;
        let (measured, largest_external_write) =
            super::encoded_json_metrics(&value, Some((source_start, source_start + source.len())))
                .unwrap();

        assert_eq!(measured, encoded.len());
        assert!(
            largest_external_write < encoded.len() / 8,
            "length-only encoding materialized a {largest_external_write}-byte output chunk outside the borrowed source for a {}-byte payload",
            encoded.len()
        );
    }

    #[tokio::test]
    async fn hydration_never_advances_past_its_captured_frontier() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
        let now = chrono::Utc::now();
        let memory = Memory {
            id: MemoryId::new_v7(),
            memory_type: 3,
            title: "concurrent".into(),
            content: "row".into(),
            summary: None,
            tags: Default::default(),
            visibility: Visibility::Org,
            provenance: Provenance::Asserted {
                author: "test".into(),
                producer_kind: None,
            },
            context: MemoryContext {
                timestamp: now,
                project_id: None,
                project_path: None,
                team_id: None,
                tenant_id: Some("org".into()),
                session_id: None,
                user_id: None,
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
            valid_from: now,
            valid_until: None,
            recorded_at: now,
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_local(0),
        };
        let captured = storage.get_state_at(now).await.unwrap().backend_lsn;
        let later = storage.upsert_memory(&memory).await.unwrap().lsn;
        assert!(later > captured);
        let cluster = ClusterNode::new(storage, "seed-race".into(), ontology.fingerprint, [4; 32]);
        let envelope = graph_reseed_envelope_at(&cluster, None, now, captured)
            .await
            .unwrap();
        assert_eq!(envelope.inv.unwrap().backend_lsn, captured, "a row observed after capture is replayed from the live subscription, never acknowledged by the snapshot frontier");
    }

    #[tokio::test]
    async fn hydration_rejects_aggregate_bytes_while_harvesting_rows() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
        let now = chrono::Utc::now();
        let rows = (0..520)
            .map(|index| Memory {
                id: MemoryId::new_v7(),
                memory_type: 3,
                title: format!("oversized-{index}").into(),
                content: "x".repeat(exocortex_wire::limits::MAX_MEMORY_CONTENT_BYTES),
                summary: None,
                tags: Default::default(),
                visibility: Visibility::Org,
                provenance: Provenance::Asserted {
                    author: "test".into(),
                    producer_kind: None,
                },
                context: MemoryContext {
                    timestamp: now,
                    project_id: None,
                    project_path: None,
                    team_id: None,
                    tenant_id: Some("org".into()),
                    session_id: None,
                    user_id: None,
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
                valid_from: now,
                valid_until: None,
                recorded_at: now,
                invalidated_by: None,
                embedding: None,
                lsn: LSN::new_local(0),
            })
            .collect::<Vec<_>>();
        storage.upsert_batch(&rows, &[]).await.unwrap();
        let cluster =
            ClusterNode::new(storage, "seed-budget".into(), ontology.fingerprint, [5; 32]);

        let error = graph_reseed_envelope_at(&cluster, None, now, 0)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("hydration exceeds"));
    }

    #[tokio::test]
    async fn subscribers_share_event_hydration_and_discoveries_are_visibility_filtered() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
        let now = chrono::Utc::now();
        let make_memory = |title: &str| Memory {
            id: MemoryId::new_v7(),
            memory_type: 3,
            title: title.into(),
            content: "row".into(),
            summary: None,
            tags: Default::default(),
            visibility: Visibility::Project,
            provenance: Provenance::Asserted {
                author: "test".into(),
                producer_kind: None,
            },
            context: MemoryContext {
                timestamp: now,
                project_id: Some("project-a".into()),
                project_path: None,
                team_id: None,
                tenant_id: Some("org".into()),
                session_id: None,
                user_id: Some("user".into()),
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
            valid_from: now,
            valid_until: None,
            recorded_at: now,
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_local(0),
        };
        let mut from = make_memory("from");
        from.embedding = Some(exocortex_kernel::Embedding {
            model: exocortex_kernel::EmbeddingModel {
                name: "test-model".into(),
                version: "v1".into(),
            },
            vector: vec![0.25, 0.75],
        });
        let to = make_memory("to");
        storage
            .upsert_batch(&[from.clone(), to.clone()], &[])
            .await
            .unwrap();
        let record = DiscoveryRecord {
            discovery_id: "discovery-shared".into(),
            region: RegionKey {
                org: "org".into(),
                project: "project-a".into(),
                memory_type: 3,
            },
            from: from.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.8,
            via_types: [1, 2],
            discovery_cycle_id: "cycle".into(),
            discovered_at: now,
        };
        storage.store_discovery(&record).await.unwrap();
        let cluster = ClusterNode::new(
            storage.clone(),
            "shared".into(),
            ontology.fingerprint,
            [7; 32],
        );
        let hydration = super::SharedEventHydration::default();
        let mut visible = VisibilityContext {
            user_id: "user".into(),
            org_id: "org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: Visibility::Org,
        };
        visible.project_ids.push("project-a".into());
        let mut hidden = visible.clone();
        hidden.project_ids.clear();

        storage.take_read_counts();
        let memory_env = cluster.envelope(Invalidation::MemoryUpserted {
            id: from.id,
            lsn: 20,
        });
        for _ in 0..2 {
            let delivered = super::prepare_for_subscriber(
                &cluster,
                &hydration,
                memory_env.clone(),
                Some(&visible),
            )
            .await
            .unwrap();
            let Some(exocortex_wire::sse::v1::invalidation::Kind::MemoryUpserted(snapshot)) =
                delivered.inv.unwrap().kind
            else {
                panic!("visible memory must be delivered as a snapshot")
            };
            let delivered_memory: Memory = serde_json::from_slice(&snapshot.snapshot_json).unwrap();
            assert!(
                delivered_memory.embedding.is_none(),
                "live SSE snapshots must strip stored embeddings before serialization"
            );
        }
        assert_eq!(
            storage.take_read_counts(),
            (1, 0),
            "subscriber count must not multiply backend reads"
        );

        let discovery_env = cluster.envelope(Invalidation::DiscoveryAvailable {
            record: record.clone(),
            lsn: 21,
        });
        let delivered = super::prepare_for_subscriber(
            &cluster,
            &hydration,
            discovery_env.clone(),
            Some(&visible),
        )
        .await
        .unwrap();
        assert!(matches!(
            delivered.inv.unwrap().kind,
            Some(exocortex_wire::sse::v1::invalidation::Kind::DiscoveryAvailable(_))
        ));
        let hidden =
            super::prepare_for_subscriber(&cluster, &hydration, discovery_env, Some(&hidden))
                .await
                .unwrap();
        assert!(matches!(
            hidden.inv.unwrap().kind,
            Some(exocortex_wire::sse::v1::invalidation::Kind::VisibilityAdvance(_))
        ));
    }

    #[tokio::test]
    async fn shared_hydration_retries_after_a_transient_failure() {
        let hydration = super::SharedEventHydration::default();
        let key = super::HydrationKey {
            lsn: 9,
            kind: 1,
            id: vec![1; 16],
        };
        let first = hydration
            .load_with(key.clone(), || async {
                Err(exocortex_storage::StorageError::Backend("temporary".into()))
            })
            .await;
        assert!(first.is_err());

        let second = hydration
            .load_with(key, || async { Ok(super::HydratedEvent::Memory(None)) })
            .await;
        assert!(
            second.is_ok(),
            "a transient lookup failure must not poison reconnects"
        );
    }

    #[test]
    fn live_stream_caps_are_per_principal_global_and_release_on_cancellation() {
        let admission = super::LiveStreamAdmission::new(2, 1);
        let first = admission.try_admit([1; 32]).unwrap();
        assert!(
            admission.try_admit([1; 32]).is_err(),
            "one credential cannot consume a second live stream"
        );
        let second = admission.try_admit([2; 32]).unwrap();
        assert!(
            admission.try_admit([3; 32]).is_err(),
            "the global cap applies across credentials"
        );

        drop(first); // models cancellation dropping the response body/stream.
        let replacement = admission.try_admit([1; 32]).unwrap();
        drop(second);
        drop(replacement);
        assert!(admission.try_admit([3; 32]).is_ok());
    }
}
