// provenance.rs — the canonical six-variant provenance (semantics: §7.9).
// This is the ONLY definition; packs cannot add variants (R-Pk5).
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Six-variant provenance (§7.9). `Proposed` never persists (R-T16).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Provenance {
    /// Direct assertion by a session harness or an accepted discovery.
    Asserted {
        /// Author identity (user or agent).
        author: SmolStr,
        /// D8 (agent-instructions PRD §3.8): the declared producer kind,
        /// stored at registration and stamped on every assertion. `None`
        /// on rows predating the field; reads back as "custom".
        #[serde(default)]
        producer_kind: Option<ProducerKind>,
    },
    /// Materialized by a Crepe rule; `evidence` is the supporting edge set.
    Derived {
        /// The rule that produced the assertion (e.g. "R1", "D1").
        rule_id: SmolStr,
        /// Supporting `RelationshipId`s.
        evidence: Vec<crate::RelationshipId>,
    },
    /// Internal computed producer (similarity, co-occurrence).
    Computed {
        /// Which internal producer ran.
        producer: ComputedProducer,
        /// The threshold the producer applied.
        threshold: f32,
    },
    /// Extracted from raw text at ingest.
    Extracted {
        /// Extractor identity.
        extractor: SmolStr,
        /// Confidence of the extraction.
        extraction_confidence: f32,
    },
    /// Discovery proposal; never persists as an edge (§12).
    Proposed {
        /// The discovery that proposed this assertion.
        discovery_id: uuid::Uuid,
        /// Discovery quality score.
        score: f32,
    },
    /// Ingested from an external source snapshot (§7.13, §18).
    ExternalSnapshot(ExternalSnapshot),
}

/// Internal computed producers (§7.9).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ComputedProducer {
    /// Ingest-time embedding compare (deferred; §24 q12).
    SimilarityCosine,
    /// Dreams-time ANN.
    SimilarityHnsw,
    /// Two memories share >=N typed entities.
    EntityCoOccurrence,
    /// Two memories from the same session_id.
    SessionCoOccurrence,
}

/// D8: the closed producer-kind set, mirrored from the wire enum. The
/// kernel owns the stored shape; the wire owns the transport shape; the
/// two are held in sync by a unit test in this crate's test suite
/// (values, not names — the wire enum is prost-generated).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub enum ProducerKind {
    /// Registration rejected it; stored rows never carry it. The serde
    /// default exists so old rows (missing the field) read back as
    /// `Custom`-equivalent, per PRD §3.8.
    #[default]
    Unspecified,
    /// A coding-agent harness (Claude Code, Codex, Cursor).
    CodingAgent,
    /// A research/planning agent.
    ResearchAgent,
    /// A docs-site adapter.
    DocsAdapter,
    /// An analytics-table adapter.
    AnalyticsAdapter,
    /// Anything else; the escape hatch.
    Custom,
}

/// External-system coordinates carried on every ingested assertion (R-T16a).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalSnapshot {
    /// e.g. "iceberg://catalog/db/table".
    pub source_uri: SmolStr,
    /// Source-specific snapshot handle.
    pub snapshot_id: SmolStr,
    /// Source column schema at snapshot.
    pub schema_hash: [u8; 32],
    /// When this snapshot was observed.
    pub observed_at: chrono::DateTime<chrono::Utc>,
    /// Stable coordinates for identity derivation (R-T18a).
    pub external_key: ExternalKey,
    /// Adapter id.
    pub producer_id: SmolStr,
}

/// Stable coordinates for identity derivation (R-T18a).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalKey {
    /// Logical table identity, path-independent.
    pub table_uuid: SmolStr,
    /// Primary key bytes.
    pub logical_pk: Vec<u8>,
    /// Bumped when the adapter changes column mapping.
    pub mapping_version: u32,
}
