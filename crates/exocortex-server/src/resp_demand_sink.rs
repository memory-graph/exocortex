//! D62: the RESP-side ontology-demand ledger. The backend installs
//! this on the ingest server; every ontology-pressure rejection
//! (`InvalidTypeTriple` / `UnknownKind`) HINCRBYs an org-scoped hash
//! on the redis plane — the fire-queue precedent. `exocortex-cli
//! ontology-demand` reads the same hash.

use exocortex_ingest::demand::{demand_hash_key, DemandEvent, OntologyDemandSink};
use redis::aio::ConnectionManager;
use std::sync::Arc;

/// Writes ontology-demand events to the org-scoped RESP hash
/// (`exocortex:<org>:ontology-demand`) on the fire-queue plane.
pub struct RespDemandSink {
    conn: Arc<tokio::sync::Mutex<ConnectionManager>>,
}

impl RespDemandSink {
    /// Wrap a managed connection (fire-queue precedent).
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
        }
    }
}

impl OntologyDemandSink for RespDemandSink {
    fn record(&self, event: DemandEvent) {
        // A tally must never fail a batch and never blocks the caller's
        // async context: the write rides a detached task.
        let conn = self.conn.clone();
        tokio::spawn(async move {
            let key = demand_hash_key(&event.org);
            let mut conn = (*conn.lock().await).clone();
            let _: Result<i64, _> = redis::cmd("HINCRBY")
                .arg(&key)
                .arg(&event.key)
                .arg(1)
                .query_async(&mut conn)
                .await;
        });
    }
}
