//! `ClusterNode` (§9.7): signs storage invalidations into protobuf
//! envelopes, fans them out to the local SSE hub (and peers via Redis
//! pub-sub), and verifies peer admission — wire version, ontology
//! fingerprint, and HMAC — before accepting inbound envelopes.

use std::sync::Arc;

use tokio::sync::{broadcast, watch};

use crate::change_log::ChangeLog as _;
use exocortex_kernel::OntologyFingerprint;
use exocortex_storage::{Invalidation, LeaseKey, OwnerLease, Storage};
use exocortex_wire::cluster::v1::InvalidationEnvelope;
use exocortex_wire::WIRE_VERSION;

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
    tx: broadcast::Sender<InvalidationEnvelope>,
    /// D25: the change-feed seam. The bounded replay ring, the
    /// eviction-proof observation markers, and the floor/frontier
    /// contract live behind one trait; `ClusterNode` appends, the SSE
    /// handler replays, the client consumes the same `Replay` verdicts.
    change_log: crate::change_log::RingChangeLog,
    feed_health: watch::Sender<FeedHealth>,
}

/// Observable lifecycle state for the storage invalidation subscription.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FeedHealth {
    /// Increments after each successful subscription.
    pub epoch: u64,
    /// True only while the current subscription is live.
    pub ready: bool,
    /// Decode, subscription, and clean-termination failures observed.
    pub failures: u64,
}

/// Default replay-buffer depth (envelopes). The PRD's "last 15 minutes"
/// Redis-Streams window is the production backend; the bounded ring is the
/// embedded default with the same contract.
pub const REPLAY_CAPACITY_DEFAULT: usize = 1024;

pub use crate::change_log::Replay;

impl<S: Storage + 'static> ClusterNode<S> {
    /// Build a node over a storage backend.
    pub fn new(
        storage: Arc<S>,
        node_id: smol_str::SmolStr,
        fp: OntologyFingerprint,
        hmac_key: [u8; 32],
    ) -> Self {
        let (tx, _) = broadcast::channel(4096);
        let (feed_health, _) = watch::channel(FeedHealth::default());
        Self {
            storage,
            node_id,
            fp,
            hmac_key,
            tx,
            change_log: crate::change_log::RingChangeLog::new(),
            feed_health,
        }
    }

    /// Shrink the replay ring (tests pin a small floor to exercise 409s).
    pub fn with_replay_capacity(mut self, cap: usize) -> Self {
        self.change_log = crate::change_log::RingChangeLog::with_capacity(cap);
        self
    }

    /// D25: the change-log seam this node appends through and the SSE
    /// handler replays through.
    pub fn change_log(&self) -> &dyn crate::change_log::ChangeLog {
        &self.change_log
    }

    /// R-C6 (D25 seam): delegate to the change log.
    pub fn replay_since(&self, since_lsn: u64) -> Replay {
        self.change_log.replay_since(since_lsn)
    }

    /// The oldest buffered LSN (D25 seam): the floor a `409` tells the
    /// client to resume from.
    pub fn replay_floor(&self) -> u64 {
        self.change_log.replay_floor()
    }

    /// Fan one envelope out through the hub AND the change log (D25:
    /// the ring is fed from the same storage stream the hub fans out).
    /// This is the single publish path: whatever the hub serves,
    /// `?since_lsn` reconnects can replay (R-C6).
    fn publish_envelope(&self, env: InvalidationEnvelope) {
        metrics::counter!("exocortex_cluster_invalidations_published_total").increment(1);
        self.change_log.append(env.clone());
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
        let mut delay = std::time::Duration::from_millis(100);
        loop {
            let mut sub = match self.storage.subscribe_invalidations(&region).await {
                Ok(sub) => sub,
                Err(error) => {
                    self.mark_feed_failed();
                    tracing::warn!(%error, "storage invalidation subscribe failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                    continue;
                }
            };
            self.mark_feed_ready();
            delay = std::time::Duration::from_millis(100);
            self.consume_feed_epoch(&mut sub).await?;
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(5));
        }
    }

    async fn consume_feed_epoch(
        &self,
        sub: &mut futures::stream::BoxStream<'_, exocortex_storage::Result<Invalidation>>,
    ) -> Result<(), ClusterError> {
        use futures::StreamExt as _;
        while let Some(inv) = sub.next().await {
            let inv = match inv {
                Ok(inv) => inv,
                Err(error) => {
                    metrics::counter!("exocortex_cluster_invalidation_decode_errors_total")
                        .increment(1);
                    self.mark_feed_failed();
                    tracing::warn!(%error, "storage invalidation decode failed; reconnecting");
                    return Ok(());
                }
            };
            self.admit_and_publish(self.envelope(inv))?;
        }
        self.mark_feed_failed();
        tracing::warn!("storage invalidation stream ended; reconnecting");
        Ok(())
    }

    fn mark_feed_ready(&self) {
        let mut state = *self.feed_health.borrow();
        state.epoch = state.epoch.saturating_add(1);
        state.ready = true;
        self.feed_health.send_replace(state);
    }

    fn mark_feed_failed(&self) {
        let mut state = *self.feed_health.borrow();
        state.ready = false;
        state.failures = state.failures.saturating_add(1);
        self.feed_health.send_replace(state);
    }

    /// Snapshot the feed state for readiness and supervision.
    pub fn feed_health(&self) -> FeedHealth {
        *self.feed_health.borrow()
    }

    /// Subscribe to feed epoch/readiness changes without timing sleeps.
    pub fn subscribe_feed_health(&self) -> watch::Receiver<FeedHealth> {
        self.feed_health.subscribe()
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
        exocortex_wire::signing::sign_invalidation_envelope(&self.hmac_key, &mut env);
        env
    }

    /// Verify an envelope's HMAC in constant time (R-Sec4).
    pub fn verify_hmac(&self, env: &InvalidationEnvelope) -> Result<(), ClusterError> {
        if !exocortex_wire::signing::verify_invalidation_envelope(&self.hmac_key, env) {
            return Err(ClusterError::HmacFailed);
        }
        Ok(())
    }

    /// Peer admission (§9.1): wire version, ontology fingerprint, HMAC.
    /// The fingerprint rule is the kernel's cluster-peer policy
    /// (OC-PRD D2): exact compatibility-fingerprint equality, because
    /// an invalidation cannot be revalidated.
    pub fn admit(&self, env: &InvalidationEnvelope) -> Result<(), ClusterError> {
        if env.wire_version != WIRE_VERSION {
            return Err(ClusterError::WireMismatch);
        }
        exocortex_kernel::admit_peer(&env.ontology_fingerprint, &self.fp.0)
            .map_err(|_| ClusterError::OntologyMismatch)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use exocortex_kernel::MemoryId;
    use exocortex_pack_dev_v1::pack_def;
    use exocortex_storage::{InMemoryStorage, StorageError};
    use futures::StreamExt as _;

    fn node() -> ClusterNode<InMemoryStorage> {
        let ontology =
            Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology"));
        ClusterNode::new(
            Arc::new(InMemoryStorage::new(ontology.clone())),
            "feed-test".into(),
            ontology.fingerprint,
            [5; 32],
        )
    }

    #[tokio::test]
    async fn replay_floor_reports_the_oldest_buffered_lsn() {
        let ontology =
            Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology"));
        let node = ClusterNode::new(
            Arc::new(InMemoryStorage::new(ontology.clone())),
            "floor-test".into(),
            ontology.fingerprint,
            [7; 32],
        )
        .with_replay_capacity(3);
        assert_eq!(
            node.replay_floor(),
            1,
            "an empty ring reports the documented default"
        );
        for lsn in 10u64..=13 {
            node.admit_and_publish(node.envelope(Invalidation::MemoryUpserted {
                id: MemoryId::new_v7(),
                lsn,
            }))
            .unwrap();
        }
        assert!(matches!(node.replay_since(0), crate::Replay::TooOld));
        assert_eq!(
            node.replay_floor(),
            11,
            "a wrapped ring reports its oldest buffered LSN, never the newest observed"
        );
    }

    #[tokio::test]
    async fn decode_failure_and_eof_end_the_epoch_and_are_observable() {
        let node = node();
        let mut receiver = node.subscribe_local();
        node.mark_feed_ready();
        let mut first = futures::stream::iter(vec![
            Ok(Invalidation::MemoryDeleted {
                id: MemoryId::new_v7(),
                lsn: 11,
            }),
            Err(StorageError::Backend("corrupt stream row".into())),
        ])
        .boxed();

        node.consume_feed_epoch(&mut first).await.expect("epoch");
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("published")
                .inv
                .unwrap()
                .backend_lsn,
            11
        );
        assert_eq!(
            node.feed_health(),
            FeedHealth {
                epoch: 1,
                ready: false,
                failures: 1,
            }
        );

        node.mark_feed_ready();
        let mut second = futures::stream::empty().boxed();
        node.consume_feed_epoch(&mut second).await.expect("epoch");
        assert_eq!(
            node.feed_health(),
            FeedHealth {
                epoch: 2,
                ready: false,
                failures: 2,
            }
        );
    }

    #[tokio::test]
    async fn run_resubscribes_after_decode_failure_and_clean_eof() {
        let ontology =
            Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).expect("ontology"));
        let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
        storage.fail_next_invalidation_epoch();
        storage.end_next_invalidation_epoch();
        let node = Arc::new(ClusterNode::new(
            storage.clone(),
            "run-retry".into(),
            ontology.fingerprint,
            [6; 32],
        ));
        let mut feed_health = node.subscribe_feed_health();
        let mut delivered = node.subscribe_local();
        let runner = tokio::spawn(node.clone().run());

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let state = *feed_health.borrow_and_update();
                if state.epoch >= 3 && state.ready && state.failures >= 2 {
                    break;
                }
                feed_health
                    .changed()
                    .await
                    .expect("run supervisor remains live");
            }
        })
        .await
        .expect("real run loop reaches a healthy third subscription");

        let id = MemoryId::new_v7();
        let commit = storage.delete_memory(&id).await.unwrap();
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(1), delivered.recv())
            .await
            .expect("recovered feed publishes")
            .expect("local subscriber remains open");
        let invalidation = envelope.inv.unwrap();
        assert_eq!(invalidation.backend_lsn, commit.lsn);
        assert!(matches!(
            invalidation.kind,
            Some(exocortex_wire::sse::v1::invalidation::Kind::MemoryDeleted(
                _
            ))
        ));
        runner.abort();
    }
}
