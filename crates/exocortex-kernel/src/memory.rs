// memory.rs — the canonical Memory and MemoryContext (semantics: §7.5, §7.6).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{EntityId, MemoryId, Provenance, Visibility, LSN};

/// A score clamped to [0.0, 1.0] at construction (used by §14).
/// KP4 (audit): `Deserialize` funnels through `F01::new` — a hand-rolled
/// impl rejects out-of-range and NaN values instead of reconstructing the
/// raw inner f32 (which let a props_json `importance: 1000.0` pin every
/// search result forever).
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct F01(f32);

impl<'de> Deserialize<'de> for F01 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = f32::deserialize(d)?;
        F01::new(v).map_err(serde::de::Error::custom)
    }
}
impl F01 {
    /// Construct a clamped score; errors outside `[0.0, 1.0]`.
    pub fn new(v: f32) -> Result<Self, crate::KernelError> {
        if (0.0..=1.0).contains(&v) {
            Ok(Self(v))
        } else {
            Err(crate::KernelError::ScoreOutOfRange(v))
        }
    }
    /// Read the raw score value.
    pub fn get(self) -> f32 {
        self.0
    }
    /// Order by the raw score.
    pub fn partial_cmp_score(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// The canonical memory: heavy envelope, single-string content (§7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Memory {
    /// 128-bit deterministic identity.
    pub id: MemoryId,
    /// Resolved via effective ontology.
    pub memory_type: u8,
    /// 1..=200 chars (R-T5).
    pub title: SmolStr,
    /// >=1 char, harness-produced (R-T5).
    pub content: String,
    /// <=500 chars (R-T5).
    pub summary: Option<SmolStr>,
    /// Lowercased, trimmed, deduped.
    pub tags: SmallVec<[SmolStr; 4]>,
    /// R-T6: required, no default.
    pub visibility: Visibility,
    /// §7.9.
    pub provenance: Provenance,
    /// Session/git/entity linkage (§7.6).
    pub context: MemoryContext,
    /// Defaults 0.5 at ingest.
    pub importance: F01,
    /// Defaults 0.8 at ingest.
    pub confidence: F01,
    /// Set by Dreams or explicit outcome.
    pub effectiveness: Option<F01>,
    /// Incremented on read.
    pub usage_count: u32,
    /// Bi-temporal validity start.
    pub valid_from: DateTime<Utc>,
    /// Bi-temporal validity end; `None` while valid.
    pub valid_until: Option<DateTime<Utc>>,
    /// When the system learned this row.
    pub recorded_at: DateTime<Utc>,
    /// Identity of the memory that superseded this one, if any.
    pub invalidated_by: Option<MemoryId>,
    /// R-T8: stripped before cache/SSE.
    pub embedding: Option<Vec<f32>>,
    /// Storage-assigned log sequence number (§6.2).
    pub lsn: LSN,
}

/// The entity linkage layer (§7.6) — session, git, project, and extracted
/// entities surrounding a memory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryContext {
    /// Mandatory; defaults to now() at ingest (R-T9).
    pub timestamp: DateTime<Utc>,
    /// Project identifier, if known.
    pub project_id: Option<SmolStr>,
    /// Project working-copy path, if known.
    pub project_path: Option<SmolStr>,
    /// Team identifier, if known.
    pub team_id: Option<SmolStr>,
    /// Tenant identifier, if known.
    pub tenant_id: Option<SmolStr>,
    /// Session identifier, if known.
    pub session_id: Option<SmolStr>,
    /// Author.
    pub user_id: Option<SmolStr>,
    /// May differ from user_id for agent authorship.
    pub created_by: Option<SmolStr>,
    /// Files the memory touches.
    pub files_involved: SmallVec<[SmolStr; 4]>,
    /// Languages the memory involves.
    pub languages: SmallVec<[SmolStr; 2]>,
    /// Frameworks the memory involves.
    pub frameworks: SmallVec<[SmolStr; 2]>,
    /// Technologies the memory involves.
    pub technologies: SmallVec<[SmolStr; 2]>,
    /// Git commit hash, if known.
    pub git_commit: Option<SmolStr>,
    /// Git branch name, if known.
    pub git_branch: Option<SmolStr>,
    /// Working directory, if known.
    pub working_directory: Option<SmolStr>,
    /// Extracted by backend (R-T18).
    pub entities: SmallVec<[EntityId; 8]>,
    /// <= 8 KiB serialized (R-T10).
    pub additional_metadata: serde_json::Value,
}

/// Normalize one tag (§7.5 comment: tags are "lowercased, trimmed,
/// deduped"): trim, lowercase, drop empties. Applied at every
/// draft→memory conversion so storage never sees raw-cased tags.
pub fn normalize_tag(tag: &str) -> Option<SmolStr> {
    let normalized = tag.trim().to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.into())
    }
}

/// Normalize a tag list: lowercase/trim each, drop empties, dedupe
/// preserving first-seen order.
pub fn normalize_tags<I, S>(tags: I) -> SmallVec<[SmolStr; 4]>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut out: SmallVec<[SmolStr; 4]> = SmallVec::new();
    for t in tags {
        if let Some(n) = normalize_tag(t.as_ref()) {
            if !out.contains(&n) {
                out.push(n);
            }
        }
    }
    out
}

/// D10a / §4.9 (agent-instructions PRD): evidence-derived confidence.
///
/// The prior deployment's optional, producer-reported `confidence` was a
/// constant (0.8 on 117/117 memories) — an aspiration, not a signal.
/// Exocortex never solicits the field; the backend derives it from
/// evidence events:
///  - the F01 default at commit (nothing yet observed),
///  - `+step` per validation outcome that exercised the claim,
///  - `-step` per counter-evidence event,
///  - the floor the moment a live Replaces/Contradicts edge points at
///    the memory (stale beliefs rank below their successors).
///
/// Constants live HERE — one definition, unit-tested — not as magic
/// numbers in consumer crates. Validation/counter-evidence events wire
/// in as the ops that produce them land; the supersession floor is live
/// from the first committed supersession edge.
pub fn derived_confidence(
    superseded: bool,
    validations: u32,
    counter_evidence: u32,
) -> crate::memory::F01 {
    const BASE: f32 = 0.8;
    const STEP: f32 = 0.05;
    const FLOOR: f32 = 0.1;
    let v = if superseded {
        FLOOR
    } else {
        (BASE + validations as f32 * STEP - counter_evidence as f32 * STEP).clamp(FLOOR, 1.0)
    };
    crate::memory::F01::new(v).expect("clamped to [FLOOR, 1.0] ⊂ [0, 1]")
}

#[cfg(test)]
mod confidence_tests {
    use super::derived_confidence;

    #[test]
    fn base_step_and_floor() {
        assert!((derived_confidence(false, 0, 0).get() - 0.8).abs() < 1e-6);
        assert!((derived_confidence(false, 2, 0).get() - 0.9).abs() < 1e-6);
        assert!((derived_confidence(false, 0, 2).get() - 0.7).abs() < 1e-6);
        // Clamped, never negative or >1.
        assert!((derived_confidence(false, 99, 0).get() - 1.0).abs() < 1e-6);
        assert!((derived_confidence(false, 0, 99).get() - 0.1).abs() < 1e-6);
        // Superseded: the floor regardless of history.
        assert!((derived_confidence(true, 99, 0).get() - 0.1).abs() < 1e-6);
    }
}
