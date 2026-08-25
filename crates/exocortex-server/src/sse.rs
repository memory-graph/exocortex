// crates/exocortex-server/src/sse.rs
//! The client-facing SSE change feed (§9.3, §9.7): one long-lived stream per
//! subscription; events carry protobuf-encoded envelopes as base64 payloads
//! with heartbeats so clients detect stalls (R-C5).
//!
//! R-Sec5: per-client SSE HMAC. When a subscriber connects with
//! `?token=<t>`, the handler derives a per-client key
//! `HMAC(cluster_key, "sse-client:" || token)` and re-signs every envelope
//! with it — token-less connections keep the cluster-key HMAC. Compromised
//! tokens revoke one subscriber, not the fleet.

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, Mac};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::{ClusterNode, Replay};
use exocortex_storage::Storage;

/// Whether `/v1/changes` demands a per-client token (R-Sec5).
/// `backend-node` requires one; `mcp-standalone` (loopback-only) keeps
/// the cluster-key default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SseAuth {
    /// Token-less subscribers still receive cluster-key-signed envelopes.
    OptionalToken,
    /// No token, no stream: the handler answers 401 (R-Sec7 posture —
    /// the change feed is an authenticated surface).
    RequiredToken,
}

/// The `/v1/changes` SSE router over a cluster node's local hub.
pub fn sse_router<S: Storage + 'static>(
    cluster: Arc<ClusterNode<S>>,
    auth: SseAuth,
) -> axum::Router {
    axum::Router::new()
        .route("/v1/changes", axum::routing::get(handler))
        .with_state((cluster, auth))
}

/// Derive the per-client SSE key (R-Sec5): provisioned clients receive this
/// key out-of-band alongside their token.
pub fn derive_client_sse_key(cluster_key: &[u8; 32], token: &str) -> [u8; 32] {
    let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(cluster_key)
        .expect("HMAC accepts any key length");
    mac.update(b"sse-client:");
    mac.update(token.as_bytes());
    mac.finalize().into_bytes().into()
}

async fn handler<S: Storage + 'static>(
    State((cluster, auth)): State<(Arc<ClusterNode<S>>, SseAuth)>,
    axum::extract::RawQuery(q): axum::extract::RawQuery,
) -> Response {
    let mut token = None;
    let mut since_lsn = None;
    if let Some(qs) = q.as_deref() {
        for pair in qs.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                match k {
                    "token" => token = Some(v.to_string()),
                    "since_lsn" => since_lsn = v.parse::<u64>().ok(),
                    _ => {}
                }
            }
        }
    }
    // R-Sec7 posture: on the backend op surface the feed is authenticated.
    if auth == SseAuth::RequiredToken && token.is_none() {
        return (http::StatusCode::UNAUTHORIZED, "missing SSE token").into_response();
    }
    // R-C6: a reconnect older than the replay window must reseed, not
    // silently skip deltas. The 409 carries the buffer floor so the client
    // can resume (and rehydrate the gap via its resync hook).
    let replay = match since_lsn {
        Some(n) => match cluster.replay_since(n) {
            Replay::Fresh(envs) => envs,
            Replay::TooOld => {
                let mut resp = (http::StatusCode::CONFLICT, "Resync Required").into_response();
                if let Ok(v) = http::HeaderValue::from_str(&cluster.replay_floor().to_string()) {
                    resp.headers_mut().insert("x-exocortex-min-lsn", v);
                }
                return resp;
            }
        },
        None => vec![],
    };

    let rx = cluster.subscribe_local();
    let node_id = cluster.node_id.to_string();
    // R-Sec5: token → per-client key; envelopes re-sign with it.
    let client_key = token.map(|t| derive_client_sse_key(&cluster.hmac_key, &t));
    let stream = async_stream::stream! {
        let mut rx = tokio_stream::wrappers::BroadcastStream::new(rx);
        use futures::StreamExt;
        // Initial comment anchors the connection before the first delta.
        yield Ok::<Event, Infallible>(Event::default().comment(format!("exocortex {node_id}")));
        // R-C6 replay first (LSN order); the client's LSN gate dedups any
        // overlap with the live stream that follows.
        for mut env in replay {
            if let Some(key) = &client_key {
                if env.hmac.is_empty() || cluster.verify_hmac(&env).is_ok() {
                    resign(key, &mut env);
                }
            }
            let payload = B64::encode(&prost_encode(&env));
            yield Ok(Event::default().event("inv").data(payload));
        }
        while let Some(item) = rx.next().await {
            if let Ok(mut env) = item {
                if let Some(key) = &client_key {
                    if env.hmac.is_empty() || cluster.verify_hmac(&env).is_ok() {
                        resign(key, &mut env);
                    }
                }
                let payload = B64::encode(&prost_encode(&env));
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(5)))
        .into_response()
}

/// Re-sign an envelope in place with a per-client key (R-Sec5).
fn resign(key: &[u8; 32], env: &mut exocortex_wire::cluster::v1::InvalidationEnvelope) {
    use prost::Message;
    env.hmac = vec![];
    let mut mac =
        <Hmac<sha2::Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(&env.encode_to_vec());
    env.hmac = mac.finalize().into_bytes().to_vec();
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
