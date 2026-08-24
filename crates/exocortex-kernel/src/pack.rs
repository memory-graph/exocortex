// pack.rs — registration and the `pack!` macro (§7.0)
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{RelKindId, RelMeta};

/// Compiled result of a `pack!` invocation. Registered with `inventory::submit!`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackDef {
    /// Unique pack name (R-Pk1).
    pub name: SmolStr,
    /// Pack version.
    pub version: PackVersion,
    /// Minimum kernel version the pack supports.
    pub kernel_min: PackVersion,
    /// Memory type names in declaration order.
    pub memory_type_names: Vec<SmolStr>,
    /// Entity type names in declaration order.
    pub entity_type_names: Vec<SmolStr>,
    /// All registered kinds — authored kinds plus auto-registered inverse
    /// companions (R-T4).
    pub kinds: Vec<RelMeta>,
    /// Type-triple rules (R-T17).
    pub type_triples: Vec<TypeTriple>,
    // Rules are compiled into the reasoning crate at build time, not shipped
    // in PackDef. PackDef only carries the rule-id list for fingerprinting.
    /// Rule ids for fingerprinting.
    pub rule_ids: Vec<SmolStr>,
}

/// Semantic version triple for packs and kernel compatibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackVersion {
    /// Major version.
    pub major: u16,
    /// Minor version.
    pub minor: u16,
    /// Patch version.
    pub patch: u16,
}

/// A type-triple rule: which `(from_type, kind, to_type)` combinations are
/// permitted (§7.15).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeTriple {
    /// The kind the rule governs.
    pub kind: RelKindId,
    /// `None` matches any memory type. Otherwise matches any listed type.
    pub from_types: Option<Vec<u8>>,
    /// `None` matches any memory type. Otherwise matches any listed type.
    pub to_types: Option<Vec<u8>>,
}

inventory::collect!(PackDef);

/// Called once at process startup. Consumes every `inventory::submit!` in the
/// linked binary and produces the effective ontology. Fails if:
/// - two packs share a name (R-Pk1)
/// - some kernel-constant `RelKindId` has no concrete kind bound (R-Pk2)
pub fn load_registered_packs() -> Result<crate::Ontology, crate::KernelError> {
    // Implementation in ontology.rs — this fn is the entry point.
    crate::ontology::Ontology::from_registered_packs()
}
