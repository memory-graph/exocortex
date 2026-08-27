// crates/exocortex-cluster/src/node.rs
//! `ClusterNode` (§9.7): signs storage invalidations into protobuf
//! envelopes, fans them out to the local SSE hub (and peers via Redis
//! pub-sub), and verifies peer admission — wire version, ontology
//! fingerprint, and HMAC — before accepting inbound envelopes.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use tokio::sync::broadcast;

use exocortex_kernel::OntologyFingerprint;
use exocortex_storage::{Invalidation, LeaseKey, OwnerLease, Storage};
use exocortex_wire::cluster::v1::InvalidationEnvelope;
use exocortex_wire::WIRE_VERSION;
use prost::Message;

/// Peer-admission and transport errors (§9.7 — names are pinned).
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// Peer runs a different wire-schema version (R-W2/R-W3).
    #[error("wire version mismatch")]
    WireMismatch,
    /// Peer runs a different effective ontology (R-T21/CR-18).
    #[error("ontology mismatch")]
    OntologyMismatch,
    /// Envelope HMAC did not verify (R-W4/R-Sec4).
    #[error("hmac verification failed")]
    HmacFailed,
    /// Storage-layer failure underneath the cluster node.
    #[error("storage: {0}")]
    Storage(String),
}

/// A backend cluster node: storage-backed, envelope-signing, SSE-fanning.
pub struct ClusterNode<S: Storage> {
    /// Durable storage (also the lease coordinator surface).
    pub storage: Arc<S>,
    /// This node's identity.
    pub node_id: smol_str::SmolStr,
    /// The effective-ontology fingerprint this node admits peers against.
    pub fp: OntologyFingerprint,
    /// Cluster-shared HMAC key (R-Sec4).
    pub hmac_key: [u8; 32],
    /// Local fan-out hub backing the SSE router.
    pub tx: broadcast::Sender<InvalidationEnvelope>,
    /// Bounded replay buffer for `?since_lsn` reconnects (R-C6): the last
    /// `replay_cap` envelopes in backend-LSN order. When a reconnect's
    /// `since_lsn` is older than the buffer floor the server answers
    /// `409 Resync Required` instead of silently skipping deltas.
    replay: Mutex<VecDeque<InvalidationEnvelope>>,
    /// CS1 (audit): whether this node has ever observed an envelope — an
    /// empty ring alone must not read as "nothing ever happened".
    observed_anything: std::sync::atomic::AtomicBool,
    /// CS1 (audit): highest LSN ever observed (survives ring eviction).
    max_observed_lsn: std::sync::atomic::AtomicU64,
    /// Ring capacity (default [`REPLAY_CAPACITY_DEFAULT`]).
    replay_cap: usize,
}

/// Default replay-buffer depth (envelopes). The PRD's "last 15 minutes"
/// Redis-Streams window is the production backend; the bounded ring is the
/// embedded default with the same contract.
pub const REPLAY_CAPACITY_DEFAULT: usize = 1024;

/// R-C6 replay outcome for a reconnecting subscriber.
#[derive(Debug, Clone)]
pub enum Replay {
    /// Envelopes after `since_lsn`, oldest first (possibly empty).
    Fresh(Vec<InvalidationEnvelope>),
    /// `since_lsn` precedes the buffer floor: the client must reseed.
    TooOld,
}

impl<S: Storage + 'static> ClusterNode<S> {
    /// Build a node over a storage backend.
    pub fn new(
        storage: Arc<S>,
        node_id: smol_str::SmolStr,
        fp: OntologyFingerprint,
        hmac_key: [u8; 32],
    ) -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            storage,
            node_id,
            fp,
            hmac_key,
            tx,
            replay: Mutex::new(VecDeque::with_capacity(REPLAY_CAPACITY_DEFAULT)),
            replay_cap: REPLAY_CAPACITY_DEFAULT,
            observed_anything: std::sync::atomic::AtomicBool::new(false),
            max_observed_lsn: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Shrink the replay ring (tests pin a small floor to exercise 409s).
    pub fn with_replay_capacity(mut self, cap: usize) -> Self {
        self.replay_cap = cap.max(1);
        self
    }

    /// R-C6: envelopes with `backend_lsn > since_lsn`, oldest first.
    /// `Replay::TooOld` when `since_lsn + 1` precedes the buffer's oldest
    /// entry — a gap the buffer can no longer bridge. CS1 (audit): an
    /// EMPTY ring is only bridgeable when nothing has ever been published
    /// — a node that has observed envelopes but lost its ring answers
    /// `TooOld` instead of "you are current", so a restarted load-balanced
    /// peer can never silently drop a gap.
    pub fn replay_since(&self, since_lsn: u64) -> Replay {
        let ring = self.replay.lock().unwrap();
        let Some(oldest) = ring.front() else {
            // Empty ring: fresh only if we have never observed anything.
            if self
                .observed_anything
                .load(std::sync::atomic::Ordering::SeqCst)
                && since_lsn
                    < self
                        .max_observed_lsn
                        .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Replay::TooOld;
            }
            return Replay::Fresh(vec![]);
        };
        let floor = envelope_lsn(oldest);
        // CS8 (audit): saturating — since_lsn = u64::MAX must answer, not
        // overflow-panic in debug or wrap in release.
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

    /// The oldest buffered LSN (1 when the ring is empty): the floor a
    /// `409` tells the client to resume from.
    pub fn replay_floor(&self) -> u64 {
        self.replay
            .lock()
            .unwrap()
            .front()
            .map(envelope_lsn)
            .unwrap_or(1)
            .max(
                self.max_observed_lsn
                    .load(std::sync::atomic::Ordering::SeqCst),
            )
    }

    /// Track one envelope in the replay ring (LSN-ordered; the ring is
    /// fed from the same storage stream the hub fans out).
    fn record_replay(&self, env: InvalidationEnvelope) {
        let mut ring = self.replay.lock().unwrap();
        if ring.len() == self.replay_cap {
            ring.pop_front();
        }
        ring.push_back(env);
    }

    /// Fan one envelope out through the hub AND the replay ring. This is
    /// the single publish path: whatever the hub serves, `?since_lsn`
    /// reconnects can replay (R-C6).
    fn publish_envelope(&self, env: InvalidationEnvelope) {
        let lsn = envelope_lsn(&env);
        self.observed_anything
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.max_observed_lsn
            .fetch_max(lsn, std::sync::atomic::Ordering::SeqCst);
        self.record_replay(env.clone());
        metrics::counter!("exocortex_cluster_invalidations_published_total").increment(1);
        let _ = self.tx.send(env);
    }

    /// WS3/CS2 (audit): the ONLY public way to fan out an envelope from a
    /// peer path. `publish_envelope` is private so future wiring cannot
    /// bypass admission — an unadmitted envelope must never be signed with
    /// this node's cluster key and served as authentic.
    pub fn admit_and_publish(&self, env: InvalidationEnvelope) -> Result<(), ClusterError> {
        self.admit(&env)?;
        self.publish_envelope(env);
        Ok(())
    }

    /// Join the cluster (§9.1): verify our own fingerprint is pinned, then
    /// subscribe to storage invalidations, sign them, and fan out. In
    /// production the Redis pub-sub publish happens inside the run loop;
    /// the local broadcast always feeds the SSE hub.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let region = exocortex_storage::RegionKey {
            org: "*".into(),
            project: "*".into(),
            memory_type: 0,
        };
        let mut sub = self.storage.subscribe_invalidations(&region).await?;
        use futures::StreamExt;
        while let Some(inv) = sub.next().await {
            // CS7 (audit): a decode failure means the LSN sequence on this
            // node is known-incomplete — count it, log it, and re-anchor
            // (subscribers will gap-detect and reseed), never swallow it.
            let inv = match inv {
                Ok(inv) => inv,
                Err(e) => {
                    metrics::counter!("exocortex_cluster_invalidation_decode_errors_total")
                        .increment(1);
                    tracing::warn!(%e, "storage invalidation decode failed; change lost");
                    continue;
                }
            };
            let env = self.envelope(inv);
            // Keep local storage events on the same admission path as peer
            // envelopes. This makes the public security control a live
            // production boundary and prevents a future caller from adding a
            // second, unchecked fan-out path.
            self.admit_and_publish(env)?;
            // Peer fan-out over Redis pub-sub is wired at M5 server start
            // (same instance as FalkorDB, §9.1); the storage subscribe
            // already crosses nodes through FalkorDB replication in the
            // docker-compose topology.
        }
        Ok(())
    }

    /// Sign an invalidation into a wire envelope (R-W4: HMAC-SHA256 over
    /// fields 1..4).
    pub fn envelope(&self, inv: Invalidation) -> InvalidationEnvelope {
        let inv_pb = crate::sse::invalidation_to_pb(&inv);
        let mut env = InvalidationEnvelope {
            wire_version: WIRE_VERSION,
            ontology_fingerprint: self.fp.0.to_vec(),
            emitter_node_id: self.node_id.to_string(),
            inv: Some(inv_pb),
            hmac: vec![],
        };
        let mut mac = <Hmac<Sha256Mac> as Mac>::new_from_slice(&self.hmac_key)
            .expect("HMAC accepts any key length");
        mac.update(&env.encode_to_vec());
        env.hmac = mac.finalize().into_bytes().to_vec();
        env
    }

    /// Verify an envelope's HMAC in constant time (R-Sec4).
    pub fn verify_hmac(&self, env: &InvalidationEnvelope) -> Result<(), ClusterError> {
        let mut unsigned = env.clone();
        unsigned.hmac = vec![];
        let mut mac = <Hmac<Sha256Mac> as Mac>::new_from_slice(&self.hmac_key)
            .expect("HMAC accepts any key length");
        mac.update(&unsigned.encode_to_vec());
        let expected = mac.finalize().into_bytes();
        if expected.len() != env.hmac.len()
            || !bool::from(subtle::ConstantTimeEq::ct_eq(
                expected.as_slice(),
                env.hmac.as_slice(),
            ))
        {
            return Err(ClusterError::HmacFailed);
        }
        Ok(())
    }

    /// Peer admission (§9.1): wire version, ontology fingerprint, HMAC.
    pub fn admit(&self, env: &InvalidationEnvelope) -> Result<(), ClusterError> {
        if env.wire_version != WIRE_VERSION {
            return Err(ClusterError::WireMismatch);
        }
        if env.ontology_fingerprint.as_slice() != self.fp.0.as_slice() {
            return Err(ClusterError::OntologyMismatch);
        }
        self.verify_hmac(env)?;
        Ok(())
    }

    /// Owner-only lease acquisition (§9.2). Storage rejects writes with a
    /// stale epoch (R-C3 fencing).
    pub async fn acquire(
        &self,
        key: LeaseKey,
        ttl: std::time::Duration,
    ) -> Result<OwnerLease, ClusterError> {
        self.storage
            .acquire_lease(&key, ttl)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }

    /// Subscribe to the local fan-out (backs the SSE hub).
    pub fn subscribe_local(&self) -> broadcast::Receiver<InvalidationEnvelope> {
        self.tx.subscribe()
    }
}

type Sha256Mac = sha2::Sha256;

/// The backend LSN an envelope carries (0 when malformed).
fn envelope_lsn(env: &InvalidationEnvelope) -> u64 {
    env.inv.as_ref().map(|i| i.backend_lsn).unwrap_or(0)
}
