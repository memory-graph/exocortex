//! D25 (palantir-expansion PRD §3.3 S7): the change-feed seam, named.
//!
//! Append-at-LSN lived in `node.rs`, feed mounting and auth in the
//! server's SSE handler, and consumption plus gap detection in the
//! client's sync loop: three crates holding three partial ideas of one
//! contract, which is exactly how CS1 happened (an empty replay ring
//! answering "you are current" instead of `TooOld`, silently dropping
//! every invalidation in the gap). This module is the one written
//! contract: four operations over an LSN-ordered log of signed
//! invalidation envelopes.
//!
//! The in-process [`RingChangeLog`] is today's only implementation and
//! the default forever — a durable broker (D26) would slot in behind
//! the same port as an opt-in, never a replacement. The conformance
//! suite in `tests/change_log.rs` runs against every implementation;
//! a new one costs a green suite, not an edit to a constant.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use exocortex_wire::cluster::v1::InvalidationEnvelope;

/// R-C6 replay outcome for a reconnecting subscriber.
#[derive(Debug, Clone)]
pub enum Replay {
    /// Envelopes after `since_lsn`, oldest first (possibly empty).
    Fresh(Vec<InvalidationEnvelope>),
    /// `since_lsn` precedes the buffer floor: the client must reseed.
    TooOld,
}

/// The change-log seam (D25). Implementations MUST preserve:
///
/// - **LSN order**: `append` accepts envelopes in strictly ascending
///   backend-LSN order; `replay_since` returns them oldest-first.
/// - **Floor truth**: `replay_floor` is the oldest still-buffered LSN
///   (1 when nothing is buffered) — never the newest observed LSN, or
///   a `409` would instruct clients to skip events the log still
///   holds.
/// - **No silent gaps**: an empty log answers `Fresh([])` only when
///   nothing has EVER been appended; once anything was observed, a
///   `since_lsn` below the high-water mark is `TooOld` even after
///   eviction (CS1).
/// - **Saturation**: `since_lsn = u64::MAX` answers, it does not
///   overflow (CS8).
pub trait ChangeLog: Send + Sync + 'static {
    /// Track one envelope. Capacity-bounded implementations evict
    /// oldest-first; the high-water mark survives eviction.
    fn append(&self, envelope: InvalidationEnvelope);
    /// Envelopes with `backend_lsn > since_lsn`, oldest first.
    fn replay_since(&self, since_lsn: u64) -> Replay;
    /// The oldest buffered LSN (1 when empty): the floor a `409`/`TooOld`
    /// tells the client to resume from.
    fn replay_floor(&self) -> u64;
    /// The highest appended LSN, once anything was appended.
    fn frontier(&self) -> Option<u64>;
}

/// Default ring depth (envelopes). The PRD's "last 15 minutes"
/// Redis-Streams window is the production backend; the bounded ring is
/// the in-process default that keeps single-binary mode honest.
pub const REPLAY_CAPACITY_DEFAULT: usize = 4096;

fn envelope_lsn(env: &InvalidationEnvelope) -> u64 {
    env.inv.as_ref().map(|i| i.backend_lsn).unwrap_or(0)
}

/// The in-process [`ChangeLog`]: a bounded ring plus the eviction-proof
/// observation markers. Exactly the mechanics `ClusterNode` owned
/// before the seam was named — no behaviour change.
pub struct RingChangeLog {
    ring: Mutex<VecDeque<InvalidationEnvelope>>,
    cap: usize,
    observed_anything: AtomicBool,
    max_observed_lsn: AtomicU64,
}

impl RingChangeLog {
    /// A ring with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(REPLAY_CAPACITY_DEFAULT)
    }

    /// A ring with an explicit capacity (tests pin a small floor to
    /// exercise `TooOld`).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ring: Mutex::new(VecDeque::with_capacity(cap.max(1))),
            cap: cap.max(1),
            observed_anything: AtomicBool::new(false),
            max_observed_lsn: AtomicU64::new(0),
        }
    }
}

impl Default for RingChangeLog {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeLog for RingChangeLog {
    fn append(&self, envelope: InvalidationEnvelope) {
        let lsn = envelope_lsn(&envelope);
        self.observed_anything.store(true, Ordering::SeqCst);
        self.max_observed_lsn.fetch_max(lsn, Ordering::SeqCst);
        let mut ring = self.ring.lock().unwrap();
        if ring.len() == self.cap {
            ring.pop_front();
        }
        ring.push_back(envelope);
    }

    fn replay_since(&self, since_lsn: u64) -> Replay {
        let ring = self.ring.lock().unwrap();
        let Some(oldest) = ring.front() else {
            // Empty ring: fresh only if we have never observed anything
            // (CS1: a restarted load-balanced peer must never silently
            // drop a gap).
            if self.observed_anything.load(Ordering::SeqCst)
                && since_lsn < self.max_observed_lsn.load(Ordering::SeqCst)
            {
                return Replay::TooOld;
            }
            return Replay::Fresh(vec![]);
        };
        let floor = envelope_lsn(oldest);
        // CS8 (audit): saturating — since_lsn = u64::MAX must answer.
        if since_lsn.saturating_add(1) < floor {
            return Replay::TooOld;
        }
        Replay::Fresh(
            ring.iter()
                .filter(|e| envelope_lsn(e) > since_lsn)
                .cloned()
                .collect(),
        )
    }

    fn replay_floor(&self) -> u64 {
        self.ring
            .lock()
            .unwrap()
            .front()
            .map(envelope_lsn)
            .unwrap_or(1)
    }

    fn frontier(&self) -> Option<u64> {
        self.observed_anything
            .load(Ordering::SeqCst)
            .then(|| self.max_observed_lsn.load(Ordering::SeqCst))
    }
}
