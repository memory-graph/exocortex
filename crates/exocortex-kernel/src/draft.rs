// draft.rs — the write-path input (§7.14)
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{MemoryContext, MemoryId, RelKindId, Visibility};

/// The harness-facing write shape. The harness never constructs `Memory`
/// directly; the backend produces one from a draft (§7.14).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryDraft {
    /// Resolved via effective ontology.
    pub memory_type: u8,
    /// 1..=200 chars (R-T5).
    pub title: SmolStr,
    /// >=1 char, harness-produced (R-T5).
    pub content: String,
    /// <=500 chars (R-T5).
    pub summary: Option<SmolStr>,
    /// R-T6: required, no default.
    pub visibility: Visibility,
    /// Session/git context (§7.6).
    pub context: MemoryContext,
    /// Typed relationships to other memories.
    pub edge_hints: SmallVec<[EdgeHint; 4]>,
    /// If present, the draft carries the source-derived identity coordinates.
    /// Absence forces content-hash fallback (see `MemoryId::from_content_hash`).
    pub external_key: Option<crate::ExternalKey>,
}

/// A typed relationship declared inside a `MemoryDraft` (§7.14).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeHint {
    /// Interned kind handle.
    pub kind: RelKindId,
    /// Target memory identity.
    pub to: MemoryId,
    /// Strength; `None` applies the `RelMeta` default.
    pub strength: Option<f32>,
    /// Confidence; `None` applies the ingest default.
    pub confidence: Option<f32>,
}
