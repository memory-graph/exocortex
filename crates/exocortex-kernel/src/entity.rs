// entity.rs
use crate::EntityId;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// A typed entity a memory is about (§7.2). Entities are cross-cutting and
/// extracted at ingest by the backend (R-T18).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity {
    /// Deterministic entity identity.
    pub id: EntityId,
    /// Resolved via effective ontology.
    pub entity_type: u8,
    /// Canonical name of the entity.
    pub canonical_name: SmolStr,
    /// Alternative spellings encountered at extraction.
    pub aliases: Vec<SmolStr>,
}
