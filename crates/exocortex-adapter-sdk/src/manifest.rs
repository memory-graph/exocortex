//! D21-c (adapter-contract PRD D3): the manifest interpreter. The SDK
//! validates every draft LOCALLY against the server's compiled rulebook
//! before the wire — the same verdicts the server would produce, for
//! everything that does not depend on server state (A3: fail as early as
//! you can HONESTLY fail; never guess a verdict you cannot compute).
//!
//! Coverage boundary (stated, not silent): idempotency, LSN assignment,
//! cross-batch `to_memory_id` stored types, principal membership, and
//! graph-share bounds are server-only — they surface at submit or
//! preflight, never as a silent local pass.

use crate::{BatchUnit, SdkError};
use exocortex_wire::ingest::v1::RejectCode;
use exocortex_wire::manifest::ValidationManifest;
use std::collections::HashMap;

/// One local verdict row (the wire `RejectRow` shape).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalReject {
    /// Producer-local key of the offending row.
    pub draft_key: String,
    /// The wire reject code (the shared vocabulary).
    pub code: RejectCode,
    /// What exactly failed.
    pub detail: String,
}

fn visibility_order(v: i32) -> Option<u8> {
    match v {
        0..=4 => Some(v as u8),
        _ => None,
    }
}

/// Validate one unit against the manifest. Returns every violation the
/// rulebook can name locally; an empty vec is a clean local pass. The
/// ceiling comes from the manifest's REGISTERED value when present and
/// falls back to the configured one (registration-verified at connect,
/// R-I3) — never a guess beyond that.
pub fn validate_unit(
    manifest: &ValidationManifest,
    configured_ceiling: i32,
    unit: &BatchUnit,
) -> Vec<LocalReject> {
    let mut rejects = Vec::new();
    let ceiling = manifest.registered_ceiling.unwrap_or(configured_ceiling);
    let ceiling_order = visibility_order(ceiling).unwrap_or(3);

    let type_ids: HashMap<&str, u8> = manifest
        .memory_types
        .iter()
        .map(|t| (t.name.as_str(), t.id))
        .collect();
    let kind_ids: HashMap<&str, u32> = manifest
        .kinds
        .iter()
        .map(|k| (k.name.as_str(), k.id))
        .collect();
    let computed_only: HashMap<&str, bool> = manifest
        .kinds
        .iter()
        .map(|k| (k.name.as_str(), k.computed_only))
        .collect();

    let mut resolved: HashMap<String, u8> = HashMap::new();
    for draft in &unit.memories {
        let Some(&type_id) = type_ids.get(draft.memory_type.as_str()) else {
            rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::UnknownMemoryType,
                detail: format!("unknown memory type `{}`", draft.memory_type),
            });
            continue;
        };
        resolved.insert(draft.draft_key.clone(), type_id);
        let chars = draft.title.chars().count();
        if chars < manifest.title_min_chars || chars > manifest.title_max_chars {
            rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::Unknown,
                detail: format!(
                    "title is {chars} chars; the rulebook bounds are {}..={}",
                    manifest.title_min_chars, manifest.title_max_chars
                ),
            });
            continue;
        }
        if draft.content.is_empty() {
            rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::Unknown,
                detail: "content is empty".into(),
            });
            continue;
        }
        match visibility_order(draft.visibility) {
            None => rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::VisibilityWidening,
                detail: format!("unknown visibility discriminant {}", draft.visibility),
            }),
            Some(order) if order > ceiling_order => rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::VisibilityWidening,
                detail: format!(
                    "visibility {order} exceeds the registered ceiling {ceiling_order}"
                ),
            }),
            Some(_) => {}
        }
        if unit.snapshot.is_some() && draft.external_key.is_none() {
            rejects.push(LocalReject {
                draft_key: draft.draft_key.clone(),
                code: RejectCode::MissingExternalKey,
                detail: "snapshot batch without an ExternalKey (R-T16a)".into(),
            });
        }
    }

    for rel in &unit.relationships {
        if rel.to_memory_id.is_empty() && rel.to_draft_key.is_empty() {
            rejects.push(LocalReject {
                draft_key: format!("{}->", rel.from_draft_key),
                code: RejectCode::InvalidTypeTriple,
                detail: "edge names neither to_draft_key nor to_memory_id".into(),
            });
            continue;
        }
        // Cross-batch targets need the STORED type — server-only; the
        // local pass honestly skips them (preflight names them).
        if !rel.to_memory_id.is_empty() {
            continue;
        }
        let Some(&kind_id) = kind_ids.get(rel.kind.as_str()) else {
            rejects.push(LocalReject {
                draft_key: format!("{}->{}", rel.from_draft_key, rel.to_draft_key),
                code: RejectCode::UnknownKind,
                detail: format!("unknown kind `{}`", rel.kind),
            });
            continue;
        };
        if computed_only
            .get(rel.kind.as_str())
            .copied()
            .unwrap_or(false)
        {
            rejects.push(LocalReject {
                draft_key: format!("{}->{}", rel.from_draft_key, rel.to_draft_key),
                code: RejectCode::ComputedKindRejected,
                detail: format!(
                    "`{}` is computed-only (R-T14): produced exclusively by Dreams",
                    rel.kind
                ),
            });
            continue;
        }
        let (Some(&from_type), Some(&to_type)) = (
            resolved.get(&rel.from_draft_key),
            resolved.get(&rel.to_draft_key),
        ) else {
            // Dangling references are the structural validator's
            // (split.rs); nothing to add here.
            continue;
        };
        let permitted = manifest.type_triples.iter().any(|triple| {
            triple.kind == kind_id
                && triple
                    .from_types
                    .as_ref()
                    .is_none_or(|types| types.contains(&from_type))
                && triple
                    .to_types
                    .as_ref()
                    .is_none_or(|types| types.contains(&to_type))
        });
        if !permitted {
            rejects.push(LocalReject {
                draft_key: format!("{}->{}", rel.from_draft_key, rel.to_draft_key),
                code: RejectCode::InvalidTypeTriple,
                detail: format!(
                    "kind `{}` does not permit this endpoint pair (R-T17)",
                    rel.kind
                ),
            });
        }
    }
    rejects
}

/// Validate a whole window of units; the first unit with local rejects
/// becomes an [`SdkError::LocalRejections`] (mapping errors stop the
/// window before any wire traffic, cursor untouched — the same verdicts
/// the server would return, arrived at honestly and early).
pub fn validate_units(
    manifest: &ValidationManifest,
    configured_ceiling: i32,
    units: &[BatchUnit],
) -> Result<(), SdkError> {
    for unit in units {
        let rejects = validate_unit(manifest, configured_ceiling, unit);
        if !rejects.is_empty() {
            return Err(SdkError::LocalRejections {
                rejects: rejects
                    .iter()
                    .map(|r| {
                        (
                            r.draft_key.clone(),
                            format!("{:?}", r.code),
                            r.detail.clone(),
                        )
                    })
                    .collect(),
            });
        }
    }
    Ok(())
}
