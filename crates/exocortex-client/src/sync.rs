//! The reconnecting SSE subscriber (M5.4, §9.3): connects to
//! `$backend/v1/changes?since_lsn=<last>`, decodes base64 protobuf
//! invalidation envelopes, verifies the envelope HMAC and ontology
//! fingerprint (R-W4/R-W3), and feeds `CacheWrite::Apply` into the cache
//! writer strictly in backend-LSN order. Out-of-order envelopes buffer in a
//! hold-back gate; a gap past `gap_timeout` resubscribes from the earlier
//! LSN (R-C6). Heartbeats arrive as SSE comments (R-C5); silence past
//! `stall_timeout` forces a reconnect.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eventsource_client as es;
use eventsource_client::Client as _;
use futures::StreamExt;
use prost::Message;

use exocortex_cache::{CacheWrite, LocalCache};
use exocortex_kernel::{MemoryId, RelationshipId};
use exocortex_ops::Invalidation;
use exocortex_wire::cluster::v1::InvalidationEnvelope;
use exocortex_wire::WIRE_VERSION;

#[derive(serde::Deserialize)]
struct ClientGraphSnapshot {
    memories: Vec<exocortex_kernel::Memory>,
    relationships: Vec<exocortex_kernel::Relationship>,
}

enum DecodedEvent {
    Change(Invalidation, u64),
    Reseed(ClientGraphSnapshot, u64),
}

/// Errors surfaced by the subscriber.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// Bad base64 or protobuf payload.
    #[error("bad envelope payload: {0}")]
    BadPayload(String),
    /// Envelope failed admission (wire version / fingerprint / HMAC).
    #[error("envelope rejected: {0}")]
    Rejected(String),
}

/// Configuration for the SSE subscriber.
#[derive(Clone)]
pub struct SseSyncConfig {
    /// Backend base URL (`http://host:port`).
    pub backend: String,
    /// Bearer token for the backend's authenticated surfaces (R-Sec7;
    /// audit CS1: `/v1/changes` sits behind the same auth layer as the op
    /// surface). Sent as `Authorization: Bearer <token>`.
    pub bearer: Option<String>,
    /// Cluster-shared HMAC key (R-Sec4).
    pub hmac_key: [u8; 32],
    /// Per-client SSE key derived from the Authorization bearer (R-Sec5).
    /// The credential itself is carried only in the header, never in a URL.
    pub client_key: Option<[u8; 32]>,
    /// Expected ontology fingerprint (R-W3 peer admission).
    pub fingerprint: [u8; 32],
    /// Silence window before a reconnect (R-C5; heartbeats every 5s).
    pub stall_timeout: Duration,
    /// How long a missing LSN may gap before resubscribing from the earlier
    /// LSN (R-C6).
    pub gap_timeout: Duration,
    /// Reconnect backoff (capped at 10s).
    pub backoff: Duration,
    /// Optional first-connection signal for supervisors and deterministic
    /// integration tests. A permit is emitted after the SSE handshake.
    pub connection_ready: Option<Arc<tokio::sync::Notify>>,
    /// Signal emitted only after an initial/recovery snapshot is visible.
    pub hydration_ready: Option<Arc<tokio::sync::Notify>>,
    /// Org graph replaced by a full SSE reseed.
    pub org: smol_str::SmolStr,
}

impl SseSyncConfig {
    /// Defaults: 15s stall, 2s gap, 1s backoff, cluster-key verification.
    pub fn new(backend: impl Into<String>, hmac_key: [u8; 32], fingerprint: [u8; 32]) -> Self {
        Self {
            backend: backend.into(),
            bearer: None,
            hmac_key,
            client_key: None,
            fingerprint,
            stall_timeout: Duration::from_secs(15),
            gap_timeout: Duration::from_secs(2),
            backoff: Duration::from_secs(1),
            connection_ready: None,
            hydration_ready: None,
            org: "org".into(),
        }
    }
}

/// Optional targeted-rehydration hook (R-C6 `409 Resync Required` /
/// unrecoverable gap): invoked before a resubscribe so the caller can
/// reseed the cache from storage.
pub type ResyncFn = Box<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send>;

/// Start the production backend cache lifecycle and return only after the
/// authenticated SSE subscriber has atomically installed its first graph
/// image. Both spawned loops remain live for continuous updates/recovery.
pub async fn hydrate_and_start_backend_sync(
    mut cfg: SseSyncConfig,
    cache: Arc<LocalCache>,
    writer_rx: tokio::sync::mpsc::Receiver<CacheWrite>,
) -> tokio::task::JoinHandle<()> {
    let writer_cache = cache.clone();
    tokio::spawn(async move {
        writer_cache
            .run(Arc::new(crate::no_backend::NoBackendStorage), writer_rx)
            .await;
    });
    let hydrated = Arc::new(tokio::sync::Notify::new());
    cfg.hydration_ready = Some(hydrated.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, cache, 0, None));
    hydrated.notified().await;
    sync
}

/// Verify an envelope: wire version, fingerprint, HMAC-SHA256 over fields
/// 1..4 (R-W4). Same scheme as `ClusterNode::admit`, inlined so the client
/// crate needs no cluster dependency.
pub fn verify_envelope(
    hmac_key: &[u8; 32],
    fingerprint: &[u8; 32],
    env: &InvalidationEnvelope,
) -> Result<(), SyncError> {
    if env.wire_version != WIRE_VERSION {
        return Err(SyncError::Rejected("wire version mismatch".into()));
    }
    if env.ontology_fingerprint.as_slice() != fingerprint {
        return Err(SyncError::Rejected("ontology fingerprint mismatch".into()));
    }
    if !exocortex_wire::signing::verify_invalidation_envelope(hmac_key, env) {
        return Err(SyncError::Rejected("hmac verification failed".into()));
    }
    Ok(())
}

/// Decode one SSE `inv` payload (base64 protobuf envelope) into the storage
/// invalidation plus its backend LSN.
pub fn decode_envelope(
    hmac_key: &[u8; 32],
    fingerprint: &[u8; 32],
    payload: &str,
) -> Result<(Invalidation, u64), SyncError> {
    match decode_event(hmac_key, fingerprint, payload)? {
        DecodedEvent::Change(invalidation, lsn) => Ok((invalidation, lsn)),
        DecodedEvent::Reseed(_, _) => Err(SyncError::BadPayload(
            "graph reseed is not a row invalidation".into(),
        )),
    }
}

fn decode_event(
    hmac_key: &[u8; 32],
    fingerprint: &[u8; 32],
    payload: &str,
) -> Result<DecodedEvent, SyncError> {
    let raw = b64_decode(payload).ok_or_else(|| SyncError::BadPayload("base64".into()))?;
    let env = InvalidationEnvelope::decode(raw.as_slice())
        .map_err(|e| SyncError::BadPayload(e.to_string()))?;
    verify_envelope(hmac_key, fingerprint, &env)?;
    let inv_pb = env
        .inv
        .ok_or_else(|| SyncError::BadPayload("no inv".into()))?;
    let lsn = inv_pb.backend_lsn;
    let id16 = |b: &[u8]| -> Result<[u8; 16], SyncError> {
        if b.len() == 16 {
            return Ok(b.try_into().expect("len checked"));
        }
        if b.len() == 32 {
            let mut out = [0u8; 16];
            for (i, byte) in out.iter_mut().enumerate() {
                let hi = (b[i * 2] as char).to_digit(16);
                let lo = (b[i * 2 + 1] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => *byte = ((h << 4) | l) as u8,
                    _ => return Err(SyncError::BadPayload("bad hex id".into())),
                }
            }
            return Ok(out);
        }
        Err(SyncError::BadPayload("id not 16/32 bytes".into()))
    };
    let inv = match inv_pb.kind {
        Some(exocortex_wire::sse::v1::invalidation::Kind::MemoryUpserted(m)) => {
            let id = MemoryId(id16(&m.id)?);
            let memory: exocortex_kernel::Memory = serde_json::from_slice(&m.snapshot_json)
                .map_err(|error| SyncError::BadPayload(format!("memory snapshot: {error}")))?;
            if memory.id != id {
                return Err(SyncError::BadPayload("memory snapshot id mismatch".into()));
            }
            Invalidation::MemorySnapshotUpserted {
                memory: Box::new(memory),
                lsn,
            }
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::MemoryDeleted(m)) => {
            Invalidation::MemoryDeleted {
                id: MemoryId(id16(&m.id)?),
                lsn,
            }
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::RelationshipUpserted(r)) => {
            let id = RelationshipId(id16(&r.id)?);
            let relationship: exocortex_kernel::Relationship =
                serde_json::from_slice(&r.snapshot_json).map_err(|error| {
                    SyncError::BadPayload(format!("relationship snapshot: {error}"))
                })?;
            if relationship.id != id
                || relationship.from != MemoryId(id16(&r.from)?)
                || relationship.to != MemoryId(id16(&r.to)?)
                || relationship.kind != exocortex_kernel::RelKindId(r.kind)
            {
                return Err(SyncError::BadPayload(
                    "relationship snapshot identity mismatch".into(),
                ));
            }
            Invalidation::RelationshipSnapshotUpserted {
                relationship: Box::new(relationship),
                lsn,
            }
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::RelationshipDeleted(r)) => {
            Invalidation::RelationshipDeleted {
                id: RelationshipId(id16(&r.id)?),
                lsn,
            }
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::VisibilityAdvance(_)) => {
            Invalidation::VisibilityAdvance { lsn }
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::GraphReseed(snapshot)) => {
            let snapshot = serde_json::from_slice(&snapshot.snapshot_json)
                .map_err(|error| SyncError::BadPayload(format!("graph snapshot: {error}")))?;
            return Ok(DecodedEvent::Reseed(snapshot, lsn));
        }
        None => return Err(SyncError::BadPayload("no kind".into())),
    };
    Ok(DecodedEvent::Change(inv, lsn))
}

/// The LSN hold-back gate: releases invalidations strictly in
/// `next..next+1..` order; buffers ahead-of-order envelopes; reports the
/// oldest missing LSN once `gap_timeout` elapses (caller resubscribes from
/// there, R-C6).
#[derive(Debug, Default)]
pub struct LsnGate {
    next: u64,
    anchored: bool,
    held: BTreeMap<u64, Invalidation>,
    gap_since: Option<Instant>,
}

impl LsnGate {
    /// New gate expecting `next` as the next LSN to apply.
    pub fn new(next: u64) -> Self {
        Self {
            next,
            anchored: false,
            held: BTreeMap::new(),
            gap_since: None,
        }
    }

    /// The next LSN the gate awaits.
    pub fn next_lsn(&self) -> u64 {
        self.next
    }

    /// Push one envelope; returns the invalidations now releasable in order.
    pub fn push(&mut self, lsn: u64, inv: Invalidation) -> Vec<Invalidation> {
        if !self.anchored {
            // Servers do not replay below `since_lsn`; the first envelope at
            // or beyond `next` anchors the sequence. Ordering and gap
            // detection then apply strictly within the stream.
            if lsn < self.next {
                return vec![];
            }
            self.next = lsn;
            self.anchored = true;
        }
        if lsn < self.next {
            // Stale replay from a resubscribe: already applied.
            return vec![];
        }
        if self.held.is_empty() {
            self.gap_since = Some(Instant::now());
        }
        self.held.insert(lsn, inv);
        self.release()
    }

    fn release(&mut self) -> Vec<Invalidation> {
        let mut out = Vec::new();
        while let Some(inv) = self.held.remove(&self.next) {
            out.push(inv);
            self.next += 1;
        }
        if self.held.is_empty() {
            self.gap_since = None;
        } else {
            self.gap_since.get_or_insert_with(Instant::now);
        }
        out
    }

    /// True when a gap has persisted past `timeout`; `next` is the missing
    /// LSN to resubscribe from.
    pub fn gap_expired(&self, timeout: Duration) -> Option<u64> {
        match (self.held.is_empty(), self.gap_since) {
            (false, Some(t)) if t.elapsed() > timeout => Some(self.next),
            _ => None,
        }
    }
}

/// Run the reconnecting subscriber until the process exits. Each connection
/// applies envelopes through the gate; gaps and stalls both trigger a
/// resubscribe from the last applied LSN (R-C6), invoking `resync` first
/// when provided (targeted rehydration).
pub async fn run_sse_sync(
    cfg: SseSyncConfig,
    cache: Arc<LocalCache>,
    mut next_lsn: u64,
    resync: Option<ResyncFn>,
) {
    let mut backoff = cfg.backoff;
    let mut needs_seed = next_lsn == 0;
    loop {
        let since = next_lsn.saturating_sub(1);
        let url = subscription_url(&cfg.backend, since, needs_seed);
        tracing::info!(backend = %cfg.backend, since_lsn = since, seed = needs_seed, "sse subscribe");
        let mut gate = LsnGate::new(next_lsn);
        let mut reconnect_reason = "stream ended";
        {
            let mut builder = match es::ClientBuilder::for_url(&url).map_err(|e| e.to_string()) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(%e, "sse client build failed; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                    continue;
                }
            };
            if let Some(bearer) = &cfg.bearer {
                match builder.header("authorization", &format!("Bearer {bearer}")) {
                    Ok(b) => builder = b,
                    Err(e) => {
                        tracing::warn!(%e, "sse bearer header rejected; backing off");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(10));
                        continue;
                    }
                }
            }
            let client = builder.read_timeout(cfg.stall_timeout).build();
            let mut stream = client.stream();
            while let Some(item) = stream.next().await {
                match item {
                    // R-C5: heartbeats/connect anchors arrive as comments;
                    // transport-level silence trips `read_timeout` below.
                    Ok(es::SSE::Connected(_)) => {
                        if let Some(ready) = &cfg.connection_ready {
                            ready.notify_one();
                        }
                    }
                    Ok(es::SSE::Comment(_)) => {}
                    Ok(es::SSE::Event(ev)) => {
                        if ev.event_type == "inv" {
                            let verify_key = cfg.client_key.unwrap_or(cfg.hmac_key);
                            match decode_event(&verify_key, &cfg.fingerprint, &ev.data) {
                                Ok(DecodedEvent::Change(inv, lsn)) => {
                                    let visibility_changed =
                                        matches!(&inv, Invalidation::VisibilityAdvance { .. });
                                    for released in gate.push(lsn, inv) {
                                        next_lsn = next_lsn.max(lsn + 1);
                                        metrics::counter!("exocortex_sync_envelopes_applied_total")
                                            .increment(1);
                                        cache.submit(CacheWrite::Apply(released)).await;
                                    }
                                    next_lsn = next_lsn.max(gate.next_lsn());
                                    if visibility_changed {
                                        // An identifier-free advance may represent a row that
                                        // became invisible. Only an authenticated replacement
                                        // image can evict a previously cached wider version
                                        // without disclosing the hidden row id.
                                        needs_seed = true;
                                        reconnect_reason = "visibility changed";
                                        break;
                                    }
                                }
                                Ok(DecodedEvent::Reseed(snapshot, lsn)) => {
                                    cache
                                        .reseed_rows(
                                            cfg.org.clone(),
                                            snapshot.memories,
                                            snapshot.relationships,
                                            lsn,
                                        )
                                        .await;
                                    next_lsn = lsn.saturating_add(1);
                                    gate = LsnGate::new(next_lsn);
                                    needs_seed = false;
                                    backoff = cfg.backoff;
                                    if let Some(ready) = &cfg.hydration_ready {
                                        ready.notify_one();
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(%e, "envelope rejected; full reseed required");
                                    needs_seed = true;
                                    reconnect_reason = "invalid hydrated envelope";
                                    break;
                                }
                            }
                        }
                        if let Some(missing) = gate.gap_expired(cfg.gap_timeout) {
                            tracing::warn!(missing, "lsn gap; resubscribing");
                            next_lsn = missing;
                            needs_seed = true;
                            reconnect_reason = "lsn gap";
                            break;
                        }
                    }
                    Err(es::Error::UnexpectedResponse(resp, _)) if resp.status() == 409 => {
                        // R-C6: Resync Required -> targeted rehydration via
                        // the hook, then resume from the server's replay
                        // floor. Without advancing `next_lsn` the client
                        // would 409-loop on the same un-bridgeable gap.
                        tracing::warn!("409 resync required");
                        needs_seed = true;
                        reconnect_reason = "409 resync";
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(%e, "sse stream error");
                        reconnect_reason = "stream error";
                        break;
                    }
                }
            }
        }
        if let Some(resync) = &resync {
            (resync)().await;
        }
        tracing::info!(reason = reconnect_reason, next_lsn, "sse reconnecting");
        metrics::counter!("exocortex_sync_reconnects_total").increment(1);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

fn subscription_url(backend: &str, since: u64, seed: bool) -> String {
    let mut url = format!(
        "{}/v1/changes?since_lsn={since}",
        backend.trim_end_matches('/')
    );
    if seed {
        url.push_str("&seed=true");
    }
    url
}

/// Base64 (standard alphabet) decode; rejects padding errors.
pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let _ = T;
    let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let unpadded: Vec<u8> = bytes.iter().copied().take_while(|b| *b != b'=').collect();
    if bytes.len() % 4 != 0 || bytes.len() - unpadded.len() > 2 {
        return None;
    }
    let mut out = Vec::with_capacity(unpadded.len() * 3 / 4);
    for chunk in unpadded.chunks(4) {
        let mut n: u32 = 0;
        for (i, c) in chunk.iter().enumerate() {
            n |= (val(*c)? as u32) << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::subscription_url;

    #[test]
    fn subscription_url_never_contains_credentials() {
        let url = subscription_url("https://backend.example", 41, true);
        assert_eq!(
            url,
            "https://backend.example/v1/changes?since_lsn=41&seed=true"
        );
        assert!(!url.contains("token"));
        assert!(!url.contains("bearer"));
    }
}
