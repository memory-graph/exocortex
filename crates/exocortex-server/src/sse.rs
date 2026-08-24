// crates/exocortex-server/src/sse.rs
//! The client-facing SSE change feed (§9.3, §9.7): one long-lived stream per
//! subscription; events carry protobuf-encoded envelopes as base64 payloads
//! with heartbeats so clients detect stalls (R-C5).

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::ClusterNode;
use exocortex_storage::Storage;

/// The `/v1/changes` SSE router over a cluster node's local hub.
pub fn sse_router<S: Storage + 'static>(cluster: Arc<ClusterNode<S>>) -> axum::Router {
    axum::Router::new()
        .route("/v1/changes", axum::routing::get(handler))
        .with_state(cluster)
}

async fn handler<S: Storage + 'static>(
    State(cluster): State<Arc<ClusterNode<S>>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = cluster.subscribe_local();
    let node_id = cluster.node_id.to_string();
    let stream = async_stream::stream! {
        let mut rx = tokio_stream::wrappers::BroadcastStream::new(rx);
        use futures::StreamExt;
        // Initial comment anchors the connection before the first delta.
        yield Ok(Event::default().comment(format!("exocortex {node_id}")));
        while let Some(item) = rx.next().await {
            if let Ok(env) = item {
                let payload = B64::encode(&prost_encode(&env));
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(5)))
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
