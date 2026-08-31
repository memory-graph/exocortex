// pack.rs — registration and the `pack!` macro (§7.0)
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::verbs::{GuidanceEntry, PackActionDef, PackFunctionDef};
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
    /// Pack-registered Actions (PX2 §4.1): signature level only — name,
    /// ceiling, typed input/output names. Bodies live in the `inventory`
    /// registrations, never here, so patching a body moves neither
    /// fingerprint level.
    #[serde(default)]
    pub actions: Vec<PackActionDef>,
    /// Pack-registered Functions (PX2 §4.1): signature level plus budgets.
    /// Body sources live in the registrations only.
    #[serde(default)]
    pub functions: Vec<PackFunctionDef>,
    /// Structured agent guidance (PX2 §4.2). Excluded from the
    /// compatibility summary (instructions, not stored meaning); covered
    /// by the build fingerprint.
    #[serde(default)]
    pub guidance: Vec<GuidanceEntry>,
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

inventory::collect!(PackRegistration);

/// Registration hook emitted by the `pack!` macro. `inventory` only accepts
/// const-constructible values and `PackDef` carries heap data (`Vec`, heap
/// `SmolStr`), so packs register a builder function instead; the ontology
/// assembly invokes each builder once at load time.
#[derive(Clone, Copy)]
pub struct PackRegistration {
    /// Builds the pack's `PackDef`. Must be deterministic.
    pub build: fn() -> PackDef,
}

/// One row of the const kind table the `pack!` macro emits. Authored kinds
/// carry `companion: false`; auto-registered inverse companions (R-T4) carry
/// `companion: true`. Companions get no type triples and therefore cannot be
/// authored directly (the R-T17 lookup fails for them).
pub struct KindRow {
    /// Display name of the kind (also its stable Cypher label, R-T2).
    pub name: &'static str,
    /// Bucket the kind belongs to.
    pub bucket: crate::RelBucket,
    /// Name of the inverse kind (`None` when the kind has no inverse).
    /// Self-inverse kinds point at their own name.
    pub inverse_name: Option<&'static str>,
    /// Whether the kind is symmetric/bidirectional.
    pub bidirectional: bool,
    /// Default strength applied when an `EdgeHint` omits strength.
    pub default_strength: f32,
    /// Name of the kernel constant this kind binds ("" for none / companions).
    pub kernel_const_name: &'static str,
    /// `true` for auto-registered inverse companion rows.
    pub companion: bool,
}

/// Resolve a kernel-constant name (as written in a `kernel_const:` DSL field)
/// to its `RelKindId`. The closed kernel-constant list from `kinds.rs`.
pub fn kernel_const_by_name(name: &str) -> Option<crate::RelKindId> {
    match name {
        "SOLVES" => Some(crate::kinds::SOLVES),
        "FIXES" => Some(crate::kinds::FIXES),
        "CAUSES" => Some(crate::kinds::CAUSES),
        "IN_SESSION" => Some(crate::kinds::IN_SESSION),
        _ => None,
    }
}

/// Extract rule ids from a `crepe_rules!` block source. Rules are
/// `pred(args) <- body;` — the id is the leading identifier of each
/// `;`-terminated rule. Deterministic; used by the `pack!` builder.
pub fn rule_ids_from_source(src: &'static str) -> Vec<&'static str> {
    let mut out = Vec::new();
    for chunk in src.split(';') {
        let chunk = chunk.trim_start();
        if chunk.is_empty() {
            continue;
        }
        match chunk.split('(').next() {
            Some(pred) if !pred.trim().is_empty() && !pred.contains('<') => {
                out.push(pred.trim());
            }
            _ => continue,
        }
    }
    out
}

impl PackVersion {
    /// Parse a `"major.minor.patch"` literal. Used by `pack!` at expansion
    /// time; the input is always a literal, so parsing cannot fail in
    /// practice — malformed input yields zeros.
    pub const fn parse(s: &'static str) -> Self {
        let bytes = s.as_bytes();
        let mut field: usize = 0;
        let mut idx: usize = 0;
        let mut values: [u16; 3] = [0, 0, 0];
        while idx < bytes.len() {
            let b = bytes[idx];
            if b >= b'0' && b <= b'9' {
                values[field] = values[field] * 10 + (b - b'0') as u16;
            } else if b == b'.' && field < 2 {
                field += 1;
            }
            idx += 1;
        }
        Self {
            major: values[0],
            minor: values[1],
            patch: values[2],
        }
    }
}

/// Called once at process startup. Consumes every `inventory::submit!` in the
/// linked binary and produces the effective ontology. Fails if:
/// - two packs share a name (R-Pk1)
/// - some kernel-constant `RelKindId` has no concrete kind bound (R-Pk2)
pub fn load_registered_packs() -> Result<crate::Ontology, crate::KernelError> {
    // Implementation in ontology.rs — this fn is the entry point.
    crate::ontology::Ontology::from_registered_packs()
}
