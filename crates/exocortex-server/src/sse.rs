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

use exocortex_cluster::{ClusterNode, Replay};
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

struct SseState<S: Storage> {
    cluster: Arc<ClusterNode<S>>,
    reseeds: Arc<tokio::sync::Semaphore>,
    hydration: SharedEventHydration,
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
    axum::Router::new()
        .route("/v1/changes", axum::routing::get(handler))
        .with_state(Arc::new(SseState {
            cluster,
            reseeds: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RESEEDS)),
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
    if principal.is_none() {
        return (http::StatusCode::UNAUTHORIZED, "missing SSE credentials").into_response();
    }
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
        let mut rx = tokio_stream::wrappers::BroadcastStream::new(rx);
        // Initial comment anchors the connection before the first delta.
        yield Ok::<Event, Infallible>(Event::default().comment(format!("exocortex {node_id}")));
        if let Some(mut env) = initial_snapshot {
            if let Some(key) = &client_key {
                resign(key, &mut env);
            }
            let payload = B64::encode(&prost_encode(&env));
            yield Ok(Event::default().event("inv").data(payload));
        }
        // R-C6 replay first (LSN order); the client's LSN gate dedups any
        // overlap with the live stream that follows.
        for env in replay {
            let mut env = match prepare_for_subscriber(
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
            if let Some(key) = &client_key {
                if env.hmac.is_empty() || cluster.verify_hmac(&env).is_ok() {
                    resign(key, &mut env);
                }
            }
            let payload = B64::encode(&prost_encode(&env));
            yield Ok(Event::default().event("inv").data(payload));
        }
        while let Some(item) = rx.next().await {
            let env = match item {
                Ok(env) => env,
                Err(error) => {
                    tracing::warn!(?error, "closing lagged SSE stream for replay recovery");
                    return;
                }
            };
            let mut env = match prepare_for_subscriber(
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
            if let Some(key) = &client_key {
                if env.hmac.is_empty() || cluster.verify_hmac(&env).is_ok() {
                    resign(key, &mut env);
                }
            }
            let payload = B64::encode(&prost_encode(&env));
            yield Ok(Event::default().event("inv").data(payload));
        }
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(5)))
        .into_response()
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
                Some(memory) => Ok(cluster.envelope(Invalidation::MemorySnapshotUpserted {
                    memory: Box::new(memory.clone()),
                    lsn,
                })),
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

fn encoded_json_len<T: serde::Serialize>(value: &T) -> Result<usize, StorageError> {
    serde_json::to_vec(value)
        .map(|encoded| encoded.len())
        .map_err(|error| StorageError::Backend(error.to_string()))
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
        ensure_reseed_budget, graph_reseed_envelope_at, MAX_RESEED_BYTES, MAX_RESEED_ROWS,
    };
    use exocortex_cluster::ClusterNode;
    use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
    use exocortex_storage::{
        DiscoveryRecord, InMemoryStorage, Invalidation, RegionKey, Storage, VisibilityContext,
    };
    use std::sync::Arc;

    #[test]
    fn hydration_budget_is_inclusive_and_fail_closed() {
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS, MAX_RESEED_BYTES).is_ok());
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS + 1, 0).is_err());
        assert!(ensure_reseed_budget(0, MAX_RESEED_BYTES + 1).is_err());
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
        let from = make_memory("from");
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
            super::prepare_for_subscriber(&cluster, &hydration, memory_env.clone(), Some(&visible))
                .await
                .unwrap();
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
}
