// kinds.rs
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Interned handle for a relationship kind (§7.3).
///
/// - High bit clear: kernel space (constants below).
/// - High bit set:   pack space (assigned by pack at register time,
///                   `RelKindId((pack_id << 16) | local_id | 0x8000_0000)`).
///
/// `RelMeta.display_name` doubles as the stable ASCII Cypher label (R-T2).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RelKindId(pub u32);

impl RelKindId {
    /// Construct a kernel-space kind id (high bit clear).
    pub const fn from_kernel(local: u16) -> Self {
        Self(local as u32)
    }
    /// Construct a pack-space kind id from the pack's registry slot and a
    /// pack-local index.
    pub fn from_pack(pack: crate::PackId, local: u16) -> Self {
        Self(0x8000_0000 | ((pack.0 as u32) << 16) | (local as u32))
    }
    /// True iff this id lies in kernel space (high bit clear).
    pub fn is_kernel(self) -> bool {
        self.0 & 0x8000_0000 == 0
    }
    /// The pack-local index portion of a pack-space id (0 for kernel ids).
    pub fn local_part(self) -> u32 {
        self.0 & 0x0000_FFFF
    }
}

// Kernel constants — the closed list referenced by kernel rules R1–R9
// and the ingest validator. Additive-only across kernel major versions.
/// Kernel constant: bound to the `Solves` kind by a pack (R-Pk2).
pub const SOLVES: RelKindId = RelKindId::from_kernel(0);
/// Kernel constant: bound to the `Fixes` kind by a pack (R-Pk2).
pub const FIXES: RelKindId = RelKindId::from_kernel(1);
/// Kernel constant: bound to the `Causes` kind by a pack (R-Pk2).
pub const CAUSES: RelKindId = RelKindId::from_kernel(2);
/// Kernel constant: bound to the `InSession` kind by a pack (R-Pk2).
pub const IN_SESSION: RelKindId = RelKindId::from_kernel(3);

/// Eight buckets (§0.3). `Extension` is reserved for pack-defined buckets that
/// don't map into one of the seven kernel-canonical buckets.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RelBucket {
    /// Causal-bucket kinds (`Causes`, `Prevents`, …).
    Causal,
    /// Solution-bucket kinds (`Solves`, `Addresses`, …).
    Solution,
    /// Context-bucket kinds (`Uses`, `Requires`, …).
    Context,
    /// Learning-bucket kinds (`Teaches`, `BuildsOn`, …).
    Learning,
    /// Similarity-bucket kinds (`SimilarTo`, …).
    Similarity,
    /// Workflow-bucket kinds (`Precedes`, `Executes`, …).
    Workflow,
    /// Quality-bucket kinds (`Validates`, `Tests`, …).
    Quality,
    /// Integration-bucket kinds (`IntegratesWith`, …).
    Integration,
    /// Pack-defined bucket that doesn't map into a kernel-canonical bucket.
    Extension(u16),
}

/// Metadata a pack attaches to every registered kind.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelMeta {
    /// Interned id for the kind.
    pub id: RelKindId,
    /// Stable display name; doubles as the Cypher label (R-T2).
    pub display_name: SmolStr,
    /// Bucket the kind belongs to.
    pub bucket: RelBucket,
    /// The auto-registered inverse companion kind, if any (R-T4).
    pub inverse: Option<RelKindId>,
    /// Whether the kind is symmetric/bidirectional.
    pub bidirectional: bool,
    /// Strength applied when an `EdgeHint` omits strength.
    pub default_strength: f32,
}
