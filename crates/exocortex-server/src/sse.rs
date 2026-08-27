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
}

/// The `/v1/changes` SSE router over a cluster node's local hub.
pub fn sse_router<S: Storage + 'static>(cluster: Arc<ClusterNode<S>>) -> axum::Router {
    axum::Router::new()
        .route("/v1/changes", axum::routing::get(handler))
        .with_state(Arc::new(SseState {
            cluster,
            reseeds: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RESEEDS)),
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
            let mut env = match prepare_for_subscriber(&cluster, env, visibility.as_ref()).await {
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
            let mut env = match prepare_for_subscriber(&cluster, env, visibility.as_ref()).await {
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
    env: InvalidationEnvelope,
    visibility: Option<&VisibilityContext>,
) -> Result<InvalidationEnvelope, StorageError> {
    let Some(vc) = visibility else {
        return hydrate_upsert(cluster, env, None).await;
    };
    let inv = env
        .inv
        .as_ref()
        .ok_or_else(|| StorageError::Backend("SSE envelope missing invalidation".into()))?;
    let lsn = inv.backend_lsn;
    let kind = inv.kind.clone();
    let visible = match kind.as_ref() {
        Some(Kind::MemoryUpserted(row)) => {
            return match memory_event_row(cluster.storage.as_ref(), &row.id, Some(vc)).await? {
                Some(memory) => Ok(cluster.envelope(Invalidation::MemorySnapshotUpserted {
                    memory: Box::new(memory),
                    lsn,
                })),
                None => Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn })),
            };
        }
        Some(Kind::MemoryDeleted(row)) => {
            memory_event_visible(cluster.storage.as_ref(), &row.id, vc).await?
        }
        Some(Kind::RelationshipUpserted(row)) => {
            return match relationship_event_row(cluster.storage.as_ref(), &row.id, Some(vc)).await?
            {
                Some(relationship) => Ok(cluster.envelope(
                    Invalidation::RelationshipSnapshotUpserted {
                        relationship: Box::new(relationship),
                        lsn,
                    },
                )),
                None => Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn })),
            };
        }
        Some(Kind::RelationshipDeleted(row)) => {
            relationship_event_visible(cluster.storage.as_ref(), &row.id, vc).await?
        }
        Some(Kind::VisibilityAdvance(_)) => true,
        Some(Kind::GraphReseed(_)) => true,
        None => {
            return Err(StorageError::Backend(
                "SSE invalidation missing event kind".into(),
            ));
        }
    };
    if visible {
        hydrate_upsert(cluster, env, Some(vc)).await
    } else {
        Ok(cluster.envelope(Invalidation::VisibilityAdvance { lsn }))
    }
}

async fn hydrate_upsert<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    env: InvalidationEnvelope,
    vc: Option<&VisibilityContext>,
) -> Result<InvalidationEnvelope, StorageError> {
    let kind = env.inv.as_ref().and_then(|inv| inv.kind.clone());
    let lsn = env.inv.as_ref().map(|inv| inv.backend_lsn).unwrap_or(0);
    match kind {
        Some(Kind::MemoryUpserted(row)) => {
            let memory = memory_event_row(cluster.storage.as_ref(), &row.id, vc)
                .await?
                .ok_or_else(|| StorageError::Backend("visible SSE memory disappeared".into()))?;
            Ok(cluster.envelope(Invalidation::MemorySnapshotUpserted {
                memory: Box::new(memory),
                lsn,
            }))
        }
        Some(Kind::RelationshipUpserted(row)) => {
            let relationship = relationship_event_row(cluster.storage.as_ref(), &row.id, vc)
                .await?
                .ok_or_else(|| {
                    StorageError::Backend("visible SSE relationship disappeared".into())
                })?;
            Ok(
                cluster.envelope(Invalidation::RelationshipSnapshotUpserted {
                    relationship: Box::new(relationship),
                    lsn,
                }),
            )
        }
        _ => Ok(env),
    }
}

async fn memory_event_visible<S: Storage>(
    storage: &S,
    raw_id: &[u8],
    vc: &VisibilityContext,
) -> Result<bool, StorageError> {
    let id = MemoryId(decode_id(raw_id)?);
    Ok(storage
        .get_memory(&id)
        .await?
        .as_ref()
        .is_some_and(|memory| memory_visible(memory, vc)))
}

async fn memory_event_row<S: Storage>(
    storage: &S,
    raw_id: &[u8],
    vc: Option<&VisibilityContext>,
) -> Result<Option<Memory>, StorageError> {
    let id = MemoryId(decode_id(raw_id)?);
    Ok(storage
        .get_memory(&id)
        .await?
        .filter(|memory| vc.is_none_or(|vc| memory_visible(memory, vc))))
}

async fn relationship_event_visible<S: Storage>(
    storage: &S,
    raw_id: &[u8],
    vc: &VisibilityContext,
) -> Result<bool, StorageError> {
    let id = RelationshipId(decode_id(raw_id)?);
    let Some(relationship) = storage.get_relationship(&id).await? else {
        return Ok(false);
    };
    let endpoints = storage
        .get_memories(&[relationship.from, relationship.to])
        .await?;
    let from = endpoints
        .iter()
        .find(|memory| memory.id == relationship.from);
    let to = endpoints.iter().find(|memory| memory.id == relationship.to);
    Ok(match (from, to) {
        (Some(from), Some(to)) => relationship_visible(&relationship, from, to, vc),
        _ => false,
    })
}

async fn relationship_event_row<S: Storage>(
    storage: &S,
    raw_id: &[u8],
    vc: Option<&VisibilityContext>,
) -> Result<Option<Relationship>, StorageError> {
    let id = RelationshipId(decode_id(raw_id)?);
    let Some(relationship) = storage.get_relationship(&id).await? else {
        return Ok(None);
    };
    let Some(vc) = vc else {
        return Ok(Some(relationship));
    };
    let endpoints = storage
        .get_memories(&[relationship.from, relationship.to])
        .await?;
    let from = endpoints
        .iter()
        .find(|memory| memory.id == relationship.from);
    let to = endpoints.iter().find(|memory| memory.id == relationship.to);
    Ok(match (from, to) {
        (Some(from), Some(to)) if relationship_visible(&relationship, from, to, vc) => {
            Some(relationship)
        }
        _ => None,
    })
}

async fn graph_reseed_envelope<S: Storage + 'static>(
    cluster: &ClusterNode<S>,
    visibility: Option<&VisibilityContext>,
) -> Result<InvalidationEnvelope, StorageError> {
    let now = chrono::Utc::now();
    let mut all_memories = std::collections::HashMap::new();
    let mut memories = Vec::new();
    let mut frontier = 0;
    let mut memory_rows = cluster.storage.stream_all_memories().await;
    while let Some(row) = memory_rows.next().await {
        let mut memory = row?;
        ensure_reseed_budget(all_memories.len() + 1, 0)?;
        frontier = frontier.max(memory.lsn.value);
        let live =
            memory.valid_until.is_none_or(|until| until > now) && memory.invalidated_by.is_none();
        let visible = visibility.is_none_or(|vc| memory_visible(&memory, vc));
        all_memories.insert(memory.id, memory.clone());
        if live && visible {
            memory.embedding = None;
            memories.push(memory);
        }
    }
    let mut relationships = Vec::new();
    let mut relationship_rows_seen = 0usize;
    let mut relationship_rows = cluster.storage.stream_all_relationships().await;
    while let Some(row) = relationship_rows.next().await {
        let relationship = row?;
        relationship_rows_seen += 1;
        ensure_reseed_budget(all_memories.len() + relationship_rows_seen, 0)?;
        frontier = frontier.max(relationship.lsn.value);
        let live = relationship.valid_until.is_none_or(|until| until > now)
            && relationship.invalidated_by.is_none();
        if !live {
            continue;
        }
        let visible = match visibility {
            None => true,
            Some(vc) => match (
                all_memories.get(&relationship.from),
                all_memories.get(&relationship.to),
            ) {
                (Some(from), Some(to)) => relationship_visible(&relationship, from, to, vc),
                _ => false,
            },
        };
        if visible {
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
    use super::{ensure_reseed_budget, MAX_RESEED_BYTES, MAX_RESEED_ROWS};

    #[test]
    fn hydration_budget_is_inclusive_and_fail_closed() {
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS, MAX_RESEED_BYTES).is_ok());
        assert!(ensure_reseed_budget(MAX_RESEED_ROWS + 1, 0).is_err());
        assert!(ensure_reseed_budget(0, MAX_RESEED_BYTES + 1).is_err());
    }
}
