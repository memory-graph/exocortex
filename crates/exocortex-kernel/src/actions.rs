// actions.rs — typed writes (§7.11). Handler bodies live in exocortex-ops.
use crate::{MemoryDraft, MemoryId, RelationshipId, Visibility};
use serde::{Deserialize, Serialize};

/// Typed write surface. Every write path in Exocortex is a named Action with
/// a typed input, a typed output, a permission check, and a provenance stamp
/// (§7.11).
pub trait Action: Send + Sync + 'static {
    /// Typed input shape.
    type Input: Serialize + for<'de> Deserialize<'de>;
    /// Typed output shape.
    type Output: Serialize + for<'de> Deserialize<'de>;
    /// Stable, human-readable name.
    const NAME: &'static str;
    /// Author must be within source ceiling.
    const REQUIRED_VISIBILITY_CEILING: Visibility;
}

/// Session-wrapup batch write — the first ingestion adapter (§7.13, §13).
pub struct CommitWrapup;
/// Promote a Dreams proposal to a stored edge (§12).
pub struct AcceptDiscovery;
/// Human-authored visibility widening; only path around R-T11a.
pub struct PromoteVisibility;
/// Close `valid_until` on an edge with reason.
pub struct RetractEdge;

impl Action for CommitWrapup {
    type Input = Vec<MemoryDraft>;
    type Output = Vec<MemoryId>;
    const NAME: &'static str = "commit_wrapup";
    const REQUIRED_VISIBILITY_CEILING: Visibility = Visibility::Org;
}

// (identical shape for the others — bodies in exocortex-ops)
impl Action for AcceptDiscovery {
    type Input = AcceptDiscoveryInput;
    type Output = RelationshipId;
    const NAME: &'static str = "accept_discovery";
    const REQUIRED_VISIBILITY_CEILING: Visibility = Visibility::Org;
}

impl Action for PromoteVisibility {
    type Input = PromoteVisibilityInput;
    type Output = MemoryId;
    const NAME: &'static str = "promote_visibility";
    const REQUIRED_VISIBILITY_CEILING: Visibility = Visibility::Org;
}

impl Action for RetractEdge {
    type Input = RetractEdgeInput;
    type Output = RelationshipId;
    const NAME: &'static str = "retract_edge";
    const REQUIRED_VISIBILITY_CEILING: Visibility = Visibility::Org;
}

/// Input for the `accept_discovery` Action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptDiscoveryInput {
    /// The discovery being accepted.
    pub discovery_id: uuid::Uuid,
    /// Memory the accepted edge originates from.
    pub from: MemoryId,
    /// Memory the accepted edge points to.
    pub to: MemoryId,
    /// Kind of the accepted edge.
    pub kind: crate::RelKindId,
}

/// Input for the `promote_visibility` Action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromoteVisibilityInput {
    /// The memory being widened.
    pub memory_id: MemoryId,
    /// The new, wider visibility.
    pub to: Visibility,
}

/// Input for the `retract_edge` Action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetractEdgeInput {
    /// The edge being retracted.
    pub edge_id: RelationshipId,
    /// Human-readable reason, kept in the audit log.
    pub reason: String,
}
