//! D21-c (adapter-contract PRD D3): the rulebook as data. The server
//! compiles its ontology into a versioned, compatibility-fingerprinted
//! document; the SDK interprets it and validates drafts locally before
//! the wire. The SDK links no kernel code — it reads this document.
//!
//! Stamped with the COMPATIBILITY fingerprint (OC-PRD): that is what
//! makes a manifest verifiable — an adapter can tell whether the
//! rulebook it holds still describes the server it is talking to, and a
//! stale manifest is detected rather than silently trusted (A3).

use serde::{Deserialize, Serialize};

/// Manifest scheme; bump on breaking shape changes. Readers refuse
/// unknown schemes rather than guessing (A3).
pub const MANIFEST_VERSION: u32 = 1;

/// The compiled rulebook: everything an adapter needs to compute, LOCALLY
/// and HONESTLY, every verdict that does not depend on server state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidationManifest {
    /// Shape version ([`MANIFEST_VERSION`]).
    pub manifest_version: u32,
    /// 64-hex compatibility fingerprint of the ontology this manifest
    /// was compiled from — the value `Fingerprint` returns.
    pub compatibility_fingerprint: String,
    /// Memory types by name and id (the positional-id map, §1.4 OC-PRD).
    pub memory_types: Vec<ManifestMemoryType>,
    /// The kind table (R-T2 display names + canonical ids).
    pub kinds: Vec<ManifestKind>,
    /// Type-triple rules (R-T17).
    pub type_triples: Vec<ManifestTriple>,
    /// Title length bounds in CHARS (R-T5 / KP3 — byte bounds are a
    /// different, looser, thing).
    pub title_min_chars: usize,
    pub title_max_chars: usize,
    /// The REGISTERED ceiling for the requesting source, when the request
    /// named one (wire Visibility value; the registration is
    /// authoritative — R-I3/audit WS2). `None` when the source was not
    /// found: the adapter degrades to server-side validation rather than
    /// guessing a ceiling.
    pub registered_ceiling: Option<i32>,
}

/// One memory-type row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestMemoryType {
    /// Registered type label (what a draft's `memory_type` must match).
    pub name: String,
    /// Canonical id.
    pub id: u8,
}

/// One kind row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestKind {
    /// Canonical id.
    pub id: u32,
    /// Stable display name (what a draft's `kind` must match).
    pub name: String,
    /// R-T14: produced exclusively by backend computation (Dreams); a
    /// producer asserting it directly is rejected.
    pub computed_only: bool,
    /// Strength applied when a draft omits strength (informational).
    pub default_strength: f32,
}

/// One type-triple rule: `kind` may run `from_types` -> `to_types` (R-T17).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestTriple {
    /// The kind the rule governs.
    pub kind: u32,
    /// `None` matches any memory type.
    pub from_types: Option<Vec<u8>>,
    /// `None` matches any memory type.
    pub to_types: Option<Vec<u8>>,
}

/// Parse and scheme-check a manifest document. Unknown versions and
/// malformed JSON are errors — never a best-effort guess (A3).
pub fn parse_manifest(json: &str) -> Result<ValidationManifest, String> {
    let manifest: ValidationManifest =
        serde_json::from_str(json).map_err(|e| format!("manifest is not valid: {e}"))?;
    if manifest.manifest_version != MANIFEST_VERSION {
        return Err(format!(
            "manifest scheme {} is not this reader's {} — update the SDK",
            manifest.manifest_version, MANIFEST_VERSION
        ));
    }
    Ok(manifest)
}
