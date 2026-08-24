// memory.rs — the canonical Memory and MemoryContext (semantics: §7.5, §7.6).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{EntityId, MemoryId, Provenance, Visibility, LSN};

/// A score clamped to [0.0, 1.0] at construction (used by §14).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct F01(f32);
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
