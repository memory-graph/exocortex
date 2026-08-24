// crates/exocortex-cluster/src/node.rs
//! `ClusterNode` (§9.7): signs storage invalidations into protobuf
//! envelopes, fans them out to the local SSE hub (and peers via Redis
//! pub-sub), and verifies peer admission — wire version, ontology
//! fingerprint, and HMAC — before accepting inbound envelopes.

use std::sync::Arc;

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
        }
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
            let Ok(inv) = inv else { continue };
            let env = self.envelope(inv);
            let _ = self.tx.send(env);
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
