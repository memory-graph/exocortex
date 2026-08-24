// relationship.rs — the canonical Relationship (semantics: §7.8).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{MemoryId, Provenance, RelKindId, RelationshipId, Visibility, LSN};

/// The canonical typed edge: first-class, provenance-stamped, bi-temporal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Relationship {
    /// Deterministic identity (`RelationshipId::derive`).
    pub id: RelationshipId,
    /// Interned kind handle resolved via the effective ontology.
    pub kind: RelKindId,
    /// Source memory.
    pub from: MemoryId,
    /// Target memory.
    pub to: MemoryId,
    /// Required; no default (R-T6).
    pub visibility: Visibility,
    /// §7.9.
    pub provenance: Provenance,
    /// Edge intelligence — strength, evidence counts, validation counters.
    pub properties: RelationshipProperties,
    /// Human-readable, optional.
    pub description: Option<SmolStr>,
    /// Derived from RelMeta at ingest (R-T4).
    pub bidirectional: bool,
    /// Bi-temporal validity start.
    pub valid_from: DateTime<Utc>,
    /// Bi-temporal validity end; `None` while valid.
    pub valid_until: Option<DateTime<Utc>>,
    /// When the system learned this row.
    pub recorded_at: DateTime<Utc>,
    /// Identity of the edge that superseded this one, if any.
    pub invalidated_by: Option<RelationshipId>,
    /// Storage-assigned log sequence number (§6.2).
    pub lsn: LSN,
}

/// Edge property bag — the intelligence lives here, not in the node payload
/// (§7.8).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationshipProperties {
    /// Base strength in [0,1].
    pub strength: f32,
    /// Confidence in [0,1].
    pub confidence: f32,
    /// Optional context string.
    pub context: Option<SmolStr>,
    /// Monotonic non-decreasing evidence count (R-T12).
    pub evidence_count: u32,
    /// Solution-bucket edges: how often it worked.
    pub success_rate: Option<f32>,
    /// Monotonic non-decreasing validation count (R-T12).
    pub validation_count: u32,
    /// Monotonic non-decreasing counter-evidence count (R-T12).
    pub counter_evidence_count: u32,
    /// Last time this edge was validated.
    pub last_validated: DateTime<Utc>,
}
