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

use futures::StreamExt;
use hyper::body::HttpBody as _;
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

#[derive(Debug)]
enum SseFrame {
    Activity,
    Event { event_type: String, data: String },
}

#[derive(Debug)]
enum SseReadError {
    ResyncRequired,
    Other(String),
}

struct BoundedSseParser {
    line: Vec<u8>,
    event_type: String,
    data: String,
    pending_cr: bool,
    max_data_bytes: usize,
}

impl BoundedSseParser {
    fn new(max_data_bytes: usize) -> Self {
        Self {
            line: Vec::new(),
            event_type: String::new(),
            data: String::new(),
            pending_cr: false,
            max_data_bytes,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseFrame>, String> {
        let mut frames = Vec::new();
        for &byte in bytes {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.finish_line(&mut frames)?;
                    self.pending_cr = true;
                }
                b'\n' => self.finish_line(&mut frames)?,
                _ => {
                    if self.line.len() >= self.max_data_bytes.saturating_add(16) {
                        return Err("SSE line exceeds the encoded event ceiling".into());
                    }
                    self.line.push(byte);
                }
            }
        }
        Ok(frames)
    }

    fn finish_line(&mut self, frames: &mut Vec<SseFrame>) -> Result<(), String> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if !self.data.is_empty() || !self.event_type.is_empty() {
                frames.push(SseFrame::Event {
                    event_type: std::mem::take(&mut self.event_type),
                    data: std::mem::take(&mut self.data),
                });
            }
            return Ok(());
        }
        if line[0] == b':' {
            frames.push(SseFrame::Activity);
            return Ok(());
        }
        let (field, value) = line.iter().position(|byte| *byte == b':').map_or(
            (line.as_slice(), &[][..]),
            |separator| {
                let value = &line[separator + 1..];
                (
                    &line[..separator],
                    value.strip_prefix(b" ").unwrap_or(value),
                )
            },
        );
        match field {
            b"event" => {
                self.event_type = std::str::from_utf8(value)
                    .map_err(|_| "SSE event type is not UTF-8")?
                    .to_owned();
            }
            b"data" => {
                let separator = usize::from(!self.data.is_empty());
                let next_len = self
                    .data
                    .len()
                    .checked_add(separator)
                    .and_then(|len| len.checked_add(value.len()))
                    .ok_or("SSE event size overflow")?;
                if next_len > self.max_data_bytes {
                    return Err("SSE data exceeds the encoded event ceiling".into());
                }
                if separator != 0 {
                    self.data.push('\n');
                }
                self.data
                    .push_str(std::str::from_utf8(value).map_err(|_| "SSE data is not UTF-8")?);
            }
            _ => {}
        }
        Ok(())
    }
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
    /// The peer accepted SSE but did not implement the required initial seed
    /// contract, so exposing an empty backend cache would be unsafe.
    #[error("backend did not provide an initial graph seed within {0:?}")]
    InitialHydrationTimeout(Duration),
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
    /// Maximum time an already-live cache may run without an authoritative
    /// replacement image. This repairs durable commits whose best-effort
    /// change-feed publication was lost.
    pub reconcile_interval: Duration,
    /// Maximum startup wait for the authenticated initial graph image. This
    /// bounds incompatibility with older servers that ignore `seed=true`.
    pub initial_hydration_timeout: Duration,
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
            reconcile_interval: Duration::from_secs(60),
            initial_hydration_timeout: Duration::from_secs(15),
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
) -> Result<tokio::task::JoinHandle<()>, SyncError> {
    let hydration_timeout = cfg.initial_hydration_timeout;
    let writer_cache = cache.clone();
    let writer = tokio::spawn(async move {
        writer_cache
            .run(Arc::new(crate::no_backend::NoBackendStorage), writer_rx)
            .await;
    });
    let hydrated = Arc::new(tokio::sync::Notify::new());
    cfg.hydration_ready = Some(hydrated.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, cache, 0, None));
    match tokio::time::timeout(hydration_timeout, hydrated.notified()).await {
        Ok(()) => Ok(sync),
        Err(_) => {
            sync.abort();
            writer.abort();
            Err(SyncError::InitialHydrationTimeout(hydration_timeout))
        }
    }
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
    let raw = exocortex_wire::transport::base64_decode_bounded(
        payload,
        exocortex_wire::limits::MAX_SSE_EVENT_DATA_BYTES,
    )
    .ok_or_else(|| SyncError::BadPayload("base64 or encoded event size".into()))?;
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
            let mut memory: exocortex_kernel::Memory = serde_json::from_slice(&m.snapshot_json)
                .map_err(|error| SyncError::BadPayload(format!("memory snapshot: {error}")))?;
            if memory.id != id {
                return Err(SyncError::BadPayload("memory snapshot id mismatch".into()));
            }
            // Older peers may still include vectors. Never retain them in the
            // client cache even while rolling through such a mixed-version
            // deployment.
            memory.embedding = None;
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
            let mut snapshot: ClientGraphSnapshot = serde_json::from_slice(&snapshot.snapshot_json)
                .map_err(|error| SyncError::BadPayload(format!("graph snapshot: {error}")))?;
            for memory in &mut snapshot.memories {
                memory.embedding = None;
            }
            return Ok(DecodedEvent::Reseed(snapshot, lsn));
        }
        Some(exocortex_wire::sse::v1::invalidation::Kind::DiscoveryAvailable(discovery)) => {
            let record = serde_json::from_slice(&discovery.record_json)
                .map_err(|error| SyncError::BadPayload(format!("discovery record: {error}")))?;
            Invalidation::DiscoveryAvailable { record, lsn }
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
    held: BTreeMap<u64, Invalidation>,
    gap_since: Option<Instant>,
}

impl LsnGate {
    /// New gate expecting `next` as the next LSN to apply.
    pub fn new(next: u64) -> Self {
        Self {
            next,
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
            (false, Some(t)) if t.elapsed() >= timeout => Some(self.next),
            _ => None,
        }
    }

    fn gap_deadline(&self, timeout: Duration) -> Option<Instant> {
        if self.held.is_empty() {
            None
        } else {
            self.gap_since.map(|started| started + timeout)
        }
    }
}

fn bounded_sse_stream(
    url: String,
    bearer: Option<String>,
    stall_timeout: Duration,
) -> impl futures::Stream<Item = Result<SseFrame, SseReadError>> {
    async_stream::stream! {
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = hyper::Client::builder().build::<_, hyper::Body>(connector);
        let mut request = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(&url)
            .header(hyper::header::ACCEPT, "text/event-stream")
            .header(hyper::header::CACHE_CONTROL, "no-cache")
            .header("x-exocortex-sse-version", exocortex_wire::SSE_EVENT_VERSION.to_string());
        if let Some(token) = bearer {
            request = request.header(hyper::header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = match request.body(hyper::Body::empty()) {
            Ok(request) => request,
            Err(error) => {
                yield Err(SseReadError::Other(format!("invalid SSE request: {error}")));
                return;
            }
        };
        let response = match tokio::time::timeout(stall_timeout, client.request(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                yield Err(SseReadError::Other(format!("SSE request failed: {error}")));
                return;
            }
            Err(_) => {
                yield Err(SseReadError::Other("SSE response headers timed out".into()));
                return;
            }
        };
        if response.status() == hyper::StatusCode::CONFLICT {
            yield Err(SseReadError::ResyncRequired);
            return;
        }
        if !response.status().is_success() {
            yield Err(SseReadError::Other(format!(
                "SSE backend returned HTTP {}",
                response.status()
            )));
            return;
        }
        yield Ok(SseFrame::Activity);
        let mut body = response.into_body();
        let mut parser = BoundedSseParser::new(
            exocortex_wire::limits::MAX_SSE_EVENT_DATA_BYTES,
        );
        loop {
            let chunk = match tokio::time::timeout(stall_timeout, body.data()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(Some(Err(error))) => {
                    yield Err(SseReadError::Other(format!("SSE body failed: {error}")));
                    return;
                }
                Ok(None) => return,
                Err(_) => {
                    yield Err(SseReadError::Other("SSE stream stalled".into()));
                    return;
                }
            };
            match parser.push(&chunk) {
                Ok(frames) => {
                    for frame in frames {
                        yield Ok(frame);
                    }
                }
                Err(error) => {
                    yield Err(SseReadError::Other(error));
                    return;
                }
            }
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
        let reconnect_reason;
        {
            let stream = bounded_sse_stream(url, cfg.bearer.clone(), cfg.stall_timeout);
            tokio::pin!(stream);
            let mut reconcile_deadline =
                (!needs_seed).then(|| tokio::time::Instant::now() + cfg.reconcile_interval);
            loop {
                let gap_deadline = gate.gap_deadline(cfg.gap_timeout);
                let gap_timer = async move {
                    match gap_deadline {
                        Some(deadline) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                        }
                        None => futures::future::pending::<()>().await,
                    }
                };
                tokio::pin!(gap_timer);
                let armed_reconcile_deadline = reconcile_deadline;
                let reconcile_timer = async move {
                    match armed_reconcile_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => futures::future::pending::<()>().await,
                    }
                };
                tokio::pin!(reconcile_timer);
                let item = tokio::select! {
                    _ = &mut reconcile_timer => {
                        needs_seed = true;
                        reconnect_reason = "periodic authoritative reconciliation";
                        break;
                    }
                    _ = &mut gap_timer => {
                        let missing = gate
                            .gap_expired(cfg.gap_timeout)
                            .expect("gap timer is armed only for a held gap");
                        tracing::warn!(missing, "lsn gap; resubscribing");
                        next_lsn = missing;
                        needs_seed = true;
                        reconnect_reason = "lsn gap";
                        break;
                    }
                    item = stream.next() => item,
                };
                let Some(item) = item else {
                    // A peer can commit durably and then disappear before its
                    // delta is published. Replaying from the old frontier is
                    // insufficient in that case; reconnect with an
                    // authoritative replacement image.
                    needs_seed = true;
                    reconnect_reason = "stream ended before publication";
                    break;
                };
                if item.is_ok() {
                    if let Some(ready) = &cfg.connection_ready {
                        ready.notify_one();
                    }
                }
                match item {
                    // R-C5: heartbeats/connect anchors prove transport-level
                    // activity; silence still trips `read_timeout` below.
                    Ok(SseFrame::Activity) => {}
                    Ok(SseFrame::Event { event_type, data }) => {
                        if event_type == "inv" {
                            let verify_key = cfg.client_key.unwrap_or(cfg.hmac_key);
                            match decode_event(&verify_key, &cfg.fingerprint, &data) {
                                Ok(DecodedEvent::Change(inv, lsn)) => {
                                    let mut visibility_changed = false;
                                    for released in gate.push(lsn, inv) {
                                        visibility_changed |= matches!(
                                            &released,
                                            Invalidation::VisibilityAdvance { .. }
                                        );
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
                                    backoff = cfg.backoff;
                                    reconcile_deadline =
                                        Some(tokio::time::Instant::now() + cfg.reconcile_interval);
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
                    Err(SseReadError::ResyncRequired) => {
                        // R-C6: Resync Required -> targeted rehydration via
                        // the hook, then resume from the server's replay
                        // floor. Without advancing `next_lsn` the client
                        // would 409-loop on the same un-bridgeable gap.
                        tracing::warn!("409 resync required");
                        needs_seed = true;
                        reconnect_reason = "409 resync";
                        break;
                    }
                    Err(SseReadError::Other(e)) => {
                        tracing::warn!(%e, "sse stream error");
                        needs_seed = true;
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

pub use exocortex_wire::transport::base64_decode as b64_decode;

#[cfg(test)]
mod tests {
    use super::{subscription_url, BoundedSseParser, SseFrame};

    #[test]
    fn sse_parser_rejects_oversized_fragmented_data_before_event_completion() {
        let mut parser = BoundedSseParser::new(8);
        assert!(parser.push(b"event: inv\ndata: 1234").unwrap().is_empty());
        let error = parser.push(b"56789\n").unwrap_err();
        assert!(error.contains("ceiling"));
    }

    #[test]
    fn sse_parser_preserves_fragmented_crlf_event_framing() {
        let mut parser = BoundedSseParser::new(8);
        assert!(parser
            .push(b": hi\r")
            .unwrap()
            .iter()
            .any(|frame| matches!(frame, SseFrame::Activity)));
        let frames = parser.push(b"\nevent: inv\r\ndata: Zg==\r\n\r\n").unwrap();
        assert!(matches!(
            frames.as_slice(),
            [SseFrame::Event { event_type, data }]
                if event_type == "inv" && data == "Zg=="
        ));
    }

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
