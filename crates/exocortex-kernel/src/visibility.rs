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

/// A relationship can never be visible more broadly than either endpoint.
pub fn relationship_visibility(from: Visibility, to: Visibility) -> Visibility {
    from.min(to)
}

/// Fold endpoint/evidence visibility into the narrowest authorized result.
pub fn narrowest_visibility(
    visibilities: impl IntoIterator<Item = Visibility>,
) -> Option<Visibility> {
    visibilities.into_iter().min()
}

#[cfg(test)]
mod tests {
    use super::{narrowest_visibility, relationship_visibility, Visibility};

    #[test]
    fn derived_visibility_is_never_wider_than_endpoints_or_evidence() {
        for from in [
            Visibility::Private,
            Visibility::Project,
            Visibility::Team,
            Visibility::Org,
        ] {
            for to in [
                Visibility::Private,
                Visibility::Project,
                Visibility::Team,
                Visibility::Org,
            ] {
                assert_eq!(relationship_visibility(from, to), from.min(to));
            }
        }
        assert_eq!(
            narrowest_visibility([Visibility::Org, Visibility::Team, Visibility::Project,]),
            Some(Visibility::Project)
        );
    }
}
