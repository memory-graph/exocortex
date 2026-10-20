//! D62: the ontology demand ledger. Ingest rejections that carry
//! ontology pressure — a KNOWN kind whose triple the ontology lacks
//! (`InvalidTypeTriple`), or a kind that does not exist at all
//! (`UnknownKind`) — are demand signals, not noise: D61's widening was
//! discovered by accident in a coding session. The sink records them
//! deterministically (no LLM, R-D6 untouched) so the pressure becomes
//! visible (`--verify`/CLI) instead of latent.
//!
//! The ingest crate stays storage-free: implementations live with the
//! embedder. The server installs a RESP-hash writer; tests use
//! [`RecordingSink`].

use std::sync::Mutex;

/// One recorded ontology-demand event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DemandEvent {
    /// `Kind/FromLabel->ToLabel` for a rejected triple;
    /// `Kind/<unknown>` for an unknown kind.
    pub key: String,
    /// The org the rejecting batch declared.
    pub org: String,
}

impl DemandEvent {
    /// Field name for a triple the ontology does not admit (yet).
    pub fn triple_key(kind_name: &str, from_label: &str, to_label: &str) -> String {
        format!("{kind_name}/{from_label}->{to_label}")
    }

    /// Field name for a kind that is not registered at all.
    pub fn unknown_kind_key(kind_name: &str) -> String {
        format!("{kind_name}/<unknown>")
    }
}

/// Where demand events land. Ingest calls this exactly where the
/// rejection is produced; failures are the implementer's to absorb
/// (a tally must never fail a batch).
pub trait OntologyDemandSink: Send + Sync {
    /// Record one demand event; failures must be absorbed.
    fn record(&self, event: DemandEvent);
}

/// The default no-op sink: demand tracking off.
pub struct NoDemandSink;

/// (See [`DemandEvent`].)
impl OntologyDemandSink for NoDemandSink {
    fn record(&self, _event: DemandEvent) {}
}

/// Test sink: captures events for assertions.
#[derive(Default)]
pub struct RecordingSink {
    events: Mutex<Vec<DemandEvent>>,
}

impl RecordingSink {
    /// An empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the recorded events for assertions.
    pub fn events(&self) -> Vec<DemandEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl OntologyDemandSink for RecordingSink {
    fn record(&self, event: DemandEvent) {
        self.events.lock().unwrap().push(event);
    }
}

/// RESP hash field format shared by writer and reader: the org-scoped
/// key an operator inspects (`exocortex:<org>:ontology-demand`).
pub fn demand_hash_key(org: &str) -> String {
    format!("exocortex:{org}:ontology-demand")
}
