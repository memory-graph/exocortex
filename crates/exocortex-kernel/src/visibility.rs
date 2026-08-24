// visibility.rs
use serde::{Deserialize, Serialize};

/// Every memory and relationship carries an explicit `Visibility`. No default
/// (R-T6, CR-22).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum Visibility {
    /// Visible to the author only (resolves against `MemoryContext.user_id`).
    Private = 0,
    /// Visible to members of the memory's project.
    Project = 1,
    /// Visible to members of the memory's team.
    Team = 2,
    /// Visible to any org member.
    Org = 3,
    /// Reserved for v2 cross-org sharing; v1 read paths treat as `Org` (R-T11).
    Public = 4,
}

impl Visibility {
    /// True iff `self` is not wider than `ceiling`. Used by the ingest
    /// validator to enforce R-T11a (no-widening rule).
    pub fn within(self, ceiling: Visibility) -> bool {
        (self as u8) <= (ceiling as u8)
    }
}
