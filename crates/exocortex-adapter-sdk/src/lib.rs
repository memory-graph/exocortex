// crates/exocortex-adapter-sdk/src/lib.rs
//! The Exocortex adapter protocol core (Layer A).
//!
//! `docs/prd/exocortex-core-prd.md` §18.2 defines seven normative adapter
//! obligations. Six are identical protocol concerns for every adapter —
//! registration, snapshot stamping, identity, idempotency, rate handling,
//! process isolation — and this crate makes them executable so an adapter
//! author writes only source-reading and mapping code:
//!
//! - [`AdapterSession::connect`] performs the `Fingerprint →
//!   RegisterSource` handshake and enforces the ceiling contract (R-I3).
//! - [`AdapterSession::submit_window`] splits units to the R-I2 byte
//!   budget without ever severing a relationship from its endpoint
//!   drafts, signs and checksums every batch via the single
//!   `exocortex_wire::signing` implementation, retries transient failures
//!   with exponential backoff, and advances the durable cursor only when
//!   the whole window has settled.
//! - [`classify`] triages every `RejectCode` variant into retry /
//!   permanent / fatal / success — exhaustively, so a new variant fails
//!   compilation here rather than defaulting to silent success.
//!
//! The seventh obligation — change detection — is deliberately absent: a
//! `Source` trait written before a second adapter exists would be fiction.
//! It gets extracted at N=2 (see the adapter-SDK PRD).
//!
//! This crate depends on exactly one `exocortex-*` crate, `exocortex-wire`
//! (R-I4); `cargo xtask kernel-purity` enforces that.

pub mod classify;
pub mod retry;
pub mod split;

#[cfg(feature = "testing")]
pub mod testing;

use std::path::PathBuf;

pub use classify::{classify, Disposition};
use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, IngestAck, RejectRow,
};
pub use retry::{instant_sleep, real_sleep, SleepFn};

/// Adapter configuration (§18.2 obligations 1, 6, 7).
#[derive(Clone, Debug)]
pub struct AdapterConfig {
    /// Owning org.
    pub org_id: String,
    /// Source URI registered with the backend (R-I3 identity).
    pub source_uri: String,
    /// Producer identity for RegisterSource.
    pub producer_id: String,
    /// Adapter id stamped on the producer identity.
    pub adapter_id: String,
    /// Node identity for lease/envelope attribution.
    pub node_id: String,
    /// §18.6 source flavor: `"iceberg" | "delta" | "parquet-dir" | "custom"`.
    pub source_flavor: String,
    /// Wire Visibility discriminant ceiling (§17).
    pub ceiling: i32,
    /// Backend IngestService endpoint (`http://host:port`).
    pub backend_url: String,
    /// Shared producer HMAC key (R-I8 as implemented; per-producer keys
    /// are a recorded deviation, not this crate's concern).
    pub hmac_key: [u8; 32],
    /// Maximum encoded batch size (R-I2). Default 4 MiB.
    pub max_batch_bytes: usize,
    /// Durable cursor file path. Advances only after a fully-settled
    /// window (§18.2 obligation 5).
    pub cursor_path: PathBuf,
    /// Retry policy for transient failures (§18.2 obligation 6).
    pub retry: RetryPolicy,
}

impl AdapterConfig {
    /// Defaults per the PRD: 4 MiB batches (R-I2), 250ms→60s backoff,
    /// 8 attempts, jitter on.
    pub fn new(org_id: &str, source_uri: &str, producer_id: &str, backend_url: &str) -> Self {
        Self {
            org_id: org_id.into(),
            source_uri: source_uri.into(),
            producer_id: producer_id.into(),
            adapter_id: format!("{producer_id}-adapter"),
            node_id: format!("{producer_id}-node"),
            source_flavor: "custom".into(),
            ceiling: 3,
            backend_url: backend_url.into(),
            hmac_key: [0u8; 32],
            max_batch_bytes: 4 * 1024 * 1024,
            cursor_path: std::env::temp_dir().join(format!("{producer_id}.cursor")),
            retry: RetryPolicy::default(),
        }
    }
}

/// Transient-failure retry behaviour (CUJ-4).
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// First backoff delay. Default 250ms.
    pub base: std::time::Duration,
    /// Delay ceiling. Default 60s.
    pub max: std::time::Duration,
    /// Maximum attempts per batch before `RetriesExhausted`. Default 8.
    pub max_attempts: u32,
    /// Jitter (default on) decorrelates adapters sharing a backend.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: std::time::Duration::from_millis(250),
            max: std::time::Duration::from_secs(60),
            max_attempts: 8,
            jitter: true,
        }
    }
}

/// One logical change unit. The SDK splits it into ≥1 `IngestBatch`
/// (R9/R10); `batch_id_seed` feeds the stable, monotonic batch ids (R11).
#[derive(Clone, Debug)]
pub struct BatchUnit {
    /// Caller-stable seed; batch ids are `"{producer_id}:{seed}:{index}"`.
    pub batch_id_seed: String,
    /// Memory drafts.
    pub memories: Vec<exocortex_wire::ingest::v1::MemoryDraft>,
    /// Relationship drafts (endpoints must be within `memories`).
    pub relationships: Vec<exocortex_wire::ingest::v1::RelationshipDraft>,
    /// Snapshot coordinates for external sources (§18.6).
    pub snapshot: Option<exocortex_wire::ingest::v1::ExternalSnapshotInfo>,
    /// When the unit was observed.
    pub observed_at: std::time::SystemTime,
}

/// The settled outcome of one submitted window (CUJ-2).
#[derive(Clone, Debug, Default)]
pub struct WindowOutcome {
    /// Rows the backend accepted.
    pub accepted: u32,
    /// Batches that were idempotent replays (§18.2 obligation 5).
    pub duplicates: u32,
    /// Permanently rejected rows, surfaced for operator triage (CUJ-3).
    pub permanent_rejections: Vec<RejectRow>,
    /// True when the durable cursor advanced.
    pub cursor_advanced: bool,
}

/// SDK errors. Every failure a protocol-level actor can produce; nothing
/// here is retryable-and-swallowed — transient states return `Err` and
/// leave the cursor untouched (R12).
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// gRPC transport failure (retryable at the caller's loop level).
    #[error("transport: {0}")]
    Transport(#[from] tonic::Status),
    /// Channel establishment failed (transport-level; fatal for connect).
    #[error("transport connect: {0}")]
    TransportConnect(String),
    /// Transient retries exhausted; the window aborted before the cursor
    /// advanced.
    #[error("retries exhausted after {attempts} attempts")]
    RetriesExhausted {
        /// Attempts made.
        attempts: u32,
    },
    /// The server's registered ceiling differs from the configuration
    /// (R-I3). Correct the config or re-register the source.
    #[error("ceiling mismatch: configured {configured}, registered {registered}")]
    CeilingMismatch {
        /// Locally configured ceiling.
        configured: i32,
        /// Ceiling the server returned at registration.
        registered: i32,
    },
    /// The backend's ontology fingerprint drifted mid-run. Resubmitting
    /// would corrupt identity; the session is unusable.
    #[error("ontology fingerprint drift")]
    FingerprintMismatch {
        /// Fingerprint negotiated at connect.
        expected: [u8; 32],
        /// Fingerprint the backend now reports.
        got: [u8; 32],
    },
    /// A connected component exceeds `max_batch_bytes` (R-I2) and cannot
    /// be split without severing a relationship from its endpoints
    /// (§18.1 forbids cross-batch draft references).
    #[error("component exceeds max_batch_bytes and cannot be split")]
    Unsplittable {
        /// The component's draft keys.
        draft_keys: Vec<String>,
    },
    /// The unit itself is malformed (dangling draft reference, snapshot
    /// row without external key) — rejected before the wire.
    #[error("invalid batch unit: {detail}")]
    InvalidUnit {
        /// What was wrong.
        detail: String,
    },
    /// Fatal session-level rejection (`Unauthorized`, `UnknownSource`,
    /// `IncompatibleOntology`).
    #[error("fatal: {code} {detail}")]
    Fatal {
        /// The wire reject code.
        code: i32,
        /// Server detail.
        detail: String,
    },
    /// Cursor I/O. A corrupt cursor file is a hard error — silently
    /// resetting it would re-ingest the world.
    #[error("cursor io: {0}")]
    Cursor(#[from] std::io::Error),
    /// The cursor file exists but is not valid UTF-8.
    #[error("cursor file corrupt: {0}")]
    CursorCorrupt(String),
}

/// A connected adapter session. One per process per producer — concurrent
/// instances sharing a `producer_id` are unsupported (§18.1 R-I1 puts
/// cross-stream ordering on the producer).
pub struct AdapterSession {
    config: AdapterConfig,
    client: IngestServiceClient<tonic::transport::Channel>,
    fingerprint: [u8; 32],
    ceiling: i32,
    cursor: Option<String>,
    sleep: SleepFn,
}

impl AdapterSession {
    /// Handshake (R7): `Fingerprint` then `RegisterSource`, ceiling
    /// verified against the configuration (R-I3). The negotiated
    /// fingerprint is stored and stamped on every subsequent batch —
    /// never a compiled-in constant (R8).
    pub async fn connect(config: AdapterConfig) -> Result<Self, SdkError> {
        Self::connect_with(config, real_sleep()).await
    }

    /// `connect` with an injected sleep function (tests; R14).
    pub async fn connect_with(config: AdapterConfig, sleep: SleepFn) -> Result<Self, SdkError> {
        let mut client = IngestServiceClient::connect(config.backend_url.clone())
            .await
            .map_err(|e| SdkError::TransportConnect(e.to_string()))?;
        let fp = client
            .fingerprint(exocortex_wire::ingest::v1::FingerprintRequest {})
            .await?
            .into_inner()
            .fingerprint;
        let fingerprint = fp.try_into().map_err(|v: Vec<u8>| SdkError::InvalidUnit {
            detail: format!("server fingerprint is {} bytes, expected 32", v.len()),
        })?;
        let registered = client
            .register_source(exocortex_wire::ingest::v1::RegisterSourceRequest {
                org_id: config.org_id.clone(),
                source_uri: config.source_uri.clone(),
                producer_id: config.producer_id.clone(),
                ceiling: config.ceiling,
                source_flavor: config.source_flavor.clone(),
            })
            .await?
            .into_inner()
            .ceiling;
        if registered != config.ceiling {
            return Err(SdkError::CeilingMismatch {
                configured: config.ceiling,
                registered,
            });
        }
        let cursor = load_cursor(&config.cursor_path)?;
        Ok(Self {
            config,
            client,
            fingerprint,
            ceiling: registered,
            cursor,
            sleep,
        })
    }

    /// The server-negotiated ontology fingerprint.
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    /// The verified registered ceiling.
    pub fn ceiling(&self) -> i32 {
        self.ceiling
    }

    /// The durable cursor, if a window has ever settled.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Submit one window of units (R9-R14). Returns the settled outcome;
    /// the on-disk cursor advances exactly once, only after every batch
    /// has been accepted or permanently rejected. Any transient failure
    /// returns `Err` with the cursor untouched, so a restart replays the
    /// window idempotently (`DUPLICATE_BATCH`).
    pub async fn submit_window(
        &mut self,
        units: Vec<BatchUnit>,
        cursor: &str,
    ) -> Result<WindowOutcome, SdkError> {
        let span =
            tracing::info_span!("adapter_submit_window", producer = %self.config.producer_id);
        let _guard = span.enter();
        // Split first — an unsplittable/invalid unit must fail before any
        // wire traffic. Then STAMP and SIGN, and verify the R-I2 budget
        // against the actual submitted bytes: the split-time estimate
        // cannot know org_id/source_uri/fingerprint/signature widths
        // (round-3 C3 — the old post-check ran pre-stamp and was dead
        // code). Over-budget batches re-split with the observed
        // headroom subtracted.
        let mut budget = self.config.max_batch_bytes;
        let mut all_batches;
        loop {
            all_batches = Vec::new();
            for unit in &units {
                all_batches.extend(split::split_unit(&self.config.producer_id, unit, budget)?);
            }
            use prost::Message;
            let mut headroom_violation = None;
            for b in &mut all_batches {
                b.org_id = self.config.org_id.clone();
                b.source_uri = self.config.source_uri.clone();
                b.producer_id = self.config.producer_id.clone();
                b.mapping_version = self.config.source_flavor.clone();
                b.ceiling = self.ceiling;
                b.ontology_fingerprint = self.fingerprint.to_vec();
                if let Some(p) = b.producer.as_mut() {
                    p.node_id = self.config.node_id.clone();
                    p.agent_id = self.config.producer_id.clone();
                    p.adapter_id = self.config.adapter_id.clone();
                }
                exocortex_wire::signing::prepare_batch(&self.config.hmac_key, b);
                if b.encoded_len() > self.config.max_batch_bytes {
                    headroom_violation = Some(b.encoded_len() - self.config.max_batch_bytes);
                    break;
                }
            }
            match headroom_violation {
                None => break,
                Some(over) => {
                    // Subtract the worst observed overshoot (plus slack)
                    // and re-split smaller. Bounded: each iteration
                    // shrinks the budget by >= 1 byte; a single memory
                    // that cannot fit even alone surfaces as
                    // `Unsplittable` from split_unit.
                    let next = budget.saturating_sub(over + 16).max(64);
                    if next == budget {
                        // Cannot shrink further: genuinely unsplittable.
                        let keys: Vec<String> = all_batches
                            .iter()
                            .flat_map(|b| b.memories.iter().map(|m| m.draft_key.clone()))
                            .collect();
                        return Err(SdkError::Unsplittable { draft_keys: keys });
                    }
                    tracing::debug!(over, next, "re-splitting under tightened R-I2 budget");
                    budget = next;
                }
            }
        }

        let mut outcome = WindowOutcome::default();
        {
            let mut client = self.client.clone();
            for batch in &mut all_batches {
                let mut attempt: u32 = 0;
                loop {
                    attempt += 1;
                    let ack = match client.submit(batch.clone()).await {
                        Ok(ack) => ack.into_inner(),
                        Err(status) => {
                            let disp = classify_status(&status);
                            if disp == Disposition::Retry
                                && attempt < self.config.retry.max_attempts
                            {
                                let delay = retry::next_delay(
                                    &self.config.retry,
                                    attempt,
                                    &mut rand_state(),
                                );
                                metrics::counter!(
                                    "exocortex_adapter_batches_total",
                                    "outcome" => "retried"
                                )
                                .increment(1);
                                (self.sleep)(delay).await;
                                continue;
                            }
                            if disp == Disposition::Retry {
                                return Err(SdkError::RetriesExhausted { attempts: attempt });
                            }
                            return Err(SdkError::Fatal {
                                code: status.code() as i32,
                                detail: status.message().to_string(),
                            });
                        }
                    };
                    match self.triage_ack(ack, &mut outcome, &mut client).await? {
                        Triage::Settled => break,
                        Triage::Retry => {
                            if attempt < self.config.retry.max_attempts {
                                let delay = retry::next_delay(
                                    &self.config.retry,
                                    attempt,
                                    &mut rand_state(),
                                );
                                metrics::counter!(
                                    "exocortex_adapter_batches_total",
                                    "outcome" => "retried"
                                )
                                .increment(1);
                                (self.sleep)(delay).await;
                                continue;
                            }
                            return Err(SdkError::RetriesExhausted { attempts: attempt });
                        }
                    }
                }
            }
        }

        // Every batch settled: advance the cursor exactly once (R12).
        save_cursor(&self.config.cursor_path, cursor)?;
        self.cursor = Some(cursor.to_string());
        outcome.cursor_advanced = true;
        Ok(outcome)
    }

    /// Classify an ack. Fatal rows abort the session; a rate-limited ack
    /// requests a retry; everything else is settled (accepted, permanent,
    /// or idempotent duplicate).
    async fn triage_ack(
        &self,
        ack: IngestAck,
        outcome: &mut WindowOutcome,
        client: &mut IngestServiceClient<tonic::transport::Channel>,
    ) -> Result<Triage, SdkError> {
        if ack.rejections.is_empty() {
            outcome.accepted += ack.accepted;
            metrics::counter!("exocortex_adapter_batches_total", "outcome" => "accepted")
                .increment(1);
            return Ok(Triage::Settled);
        }
        let mut retry = false;
        let mut duplicate = false;
        for row in &ack.rejections {
            let code = exocortex_wire::ingest::v1::RejectCode::try_from(row.code)
                .unwrap_or(exocortex_wire::ingest::v1::RejectCode::Unknown);
            match classify(code) {
                // DUPLICATE_BATCH is a batch-level verdict: counted once.
                Disposition::Success => duplicate = true,
                Disposition::Retry => retry = true,
                Disposition::Permanent => {
                    outcome.permanent_rejections.push(row.clone());
                }
                Disposition::Fatal => {
                    if code == exocortex_wire::ingest::v1::RejectCode::IncompatibleOntology {
                        // R8: re-negotiate to name both fingerprints.
                        let got = client
                            .fingerprint(exocortex_wire::ingest::v1::FingerprintRequest {})
                            .await?
                            .into_inner()
                            .fingerprint;
                        let got: [u8; 32] =
                            got.try_into().map_err(|v: Vec<u8>| SdkError::InvalidUnit {
                                detail: format!("fingerprint len {}", v.len()),
                            })?;
                        return Err(SdkError::FingerprintMismatch {
                            expected: self.fingerprint,
                            got,
                        });
                    }
                    return Err(SdkError::Fatal {
                        code: row.code,
                        detail: row.detail.clone(),
                    });
                }
            }
        }
        if retry {
            return Ok(Triage::Retry);
        }
        if duplicate {
            outcome.duplicates += 1;
            outcome.accepted += ack.accepted;
            metrics::counter!("exocortex_adapter_batches_total", "outcome" => "duplicate")
                .increment(1);
            return Ok(Triage::Settled);
        }
        metrics::counter!("exocortex_adapter_batches_total", "outcome" => "rejected").increment(1);
        Ok(Triage::Settled)
    }
}

enum Triage {
    Settled,
    Retry,
}

/// Transport statuses map onto the same triage table (round-3 C5): the
/// ingest server surfaces TRANSIENT storage failures as
/// `Status::internal("storage: …")`, so `Internal` and `Unknown` are
/// retryable — one backend blip must not kill the session. Genuinely
/// fatal classes (auth, permissions, malformed request, not-found)
/// stay fatal.
fn classify_status(status: &tonic::Status) -> Disposition {
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::ResourceExhausted
        | tonic::Code::Aborted
        | tonic::Code::Internal
        | tonic::Code::Unknown => Disposition::Retry,
        _ => Disposition::Fatal,
    }
}

fn rand_state() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(0x9e3779b97f4a7c15)
}

fn load_cursor(path: &std::path::Path) -> Result<Option<String>, SdkError> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(SdkError::CursorCorrupt(e.to_string())),
    }
}

fn save_cursor(path: &std::path::Path, cursor: &str) -> Result<(), SdkError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, cursor)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod feature_canary {
    /// Round-3 C1: the mock-driven integration tests are
    /// `#[cfg(feature = "testing")]`. A bare `cargo test -p
    /// exocortex-adapter-sdk` used to report green while those suites
    /// compiled to nothing. This canary inverts that: it FAILS unless
    /// the feature is enabled, so the dark-suite state is loud. Run
    /// `cargo test-sdk` (alias) or
    /// `cargo test -p exocortex-adapter-sdk --features testing`.
    #[test]
    #[cfg(not(feature = "testing"))]
    fn testing_feature_is_enabled_for_this_test_run() {
        panic!(
            "integration suites are dark: re-run with --features testing              (or `cargo test-sdk`) — see round-3 review C1"
        );
    }
}
