// crates/exocortex-client/src/preflight.rs
//! D2 (agent-instructions PRD §3.2): the client-local validation pass.
//! One implementation, used by BOTH `end_session`'s self-preflight (r4)
//! and the `preflight_wrapup` operation — the same kernel functions
//! `IngestService::Submit` runs (W2 consolidated them), so a local
//! verdict matches the server's for everything a client can see.
//!
//! Coverage boundary (stated in the playbook, §3.2 r3): fingerprint
//! drift, source registration, server-side ceiling changes,
//! `DUPLICATE_BATCH`, and the stored type of a `to_memory_id` target
//! absent from the local cache are server-only checks — they surface in
//! `unverified`, never as a silent pass.

use exocortex_kernel::{Ontology, Visibility};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One proposed memory draft (§13.5 shape; the client's MCP arg type
/// re-exports this so the tool schema and the registry schema are ONE).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PreflightMemoryDraft {
    /// Links edges within this batch.
    pub draft_key: String,
    /// MUST match a registered MemoryType label.
    pub memory_type: String,
    /// 1..=200 chars (R-T5).
    pub title: String,
    /// Free-text content.
    pub content: String,
    /// "private"|"project"|"team"|"org" (R-T6).
    pub visibility: String,
    /// Lowercase tags (normalized at commit).
    #[serde(default)]
    pub tags: Vec<String>,
}

/// One proposed edge (§13.5 + §4.5 shape).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PreflightEdgeHint {
    /// Source draft key.
    pub from_draft_key: String,
    /// Target draft key (within this batch) — or empty when `to_memory_id` is set.
    #[serde(default)]
    pub to_draft_key: String,
    /// §4.5: an EXISTING memory by 32-hex id (cross-batch edge).
    #[serde(default)]
    pub to_memory_id: String,
    /// MUST match a registered kind display_name.
    pub kind: String,
    /// 0 = RelMeta default.
    #[serde(default)]
    pub strength: f32,
}

/// One client-detected problem, shaped like the wire `RejectRow` plus the
/// deterministic correction text (P3).
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct LocalRejection {
    /// Producer-local key of the offending row.
    pub draft_key: String,
    /// `RejectCode` name (matches the server's vocabulary).
    pub code: String,
    /// What exactly failed.
    pub detail: String,
    /// Deterministic remediation (from the wire guidance table; never LLM).
    pub correction: String,
}

/// A check the client could not run (§3.2 r3) — reported, not guessed.
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct UnverifiedCheck {
    /// Which draft or edge, by producer-local key.
    pub key: String,
    /// What could not be checked.
    pub reason: String,
}

/// The local verdict over a proposed batch.
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct PreflightResult {
    /// Rows that would commit.
    pub would_accept: u32,
    /// Rows the server would reject for client-visible reasons.
    pub would_reject: u32,
    /// The client-detected problems (empty ⇒ clean local pass).
    pub rejections: Vec<LocalRejection>,
    /// Checks only the backend can run, named honestly.
    pub unverified: Vec<UnverifiedCheck>,
}

/// Resolve a memory-type label to its id (None ⇒ unknown type).
fn memory_type(ontology: &Ontology, label: &str) -> Option<u8> {
    ontology.memory_type_id(label)
}

fn parse_visibility(s: &str) -> Option<Visibility> {
    match s.to_lowercase().as_str() {
        "private" => Some(Visibility::Private),
        "project" => Some(Visibility::Project),
        "team" => Some(Visibility::Team),
        "org" => Some(Visibility::Org),
        _ => None,
    }
}

/// The session-wrapup ceiling (§18.2): `Org`. All four labels are legal;
/// only validity and no-widening are enforced (P4's stated exception).
const CEILING: Visibility = Visibility::Org;

/// Run the local pass. `lookup_type` resolves an existing memory id
/// (32-hex) to its stored memory type through the local cache; `None`
/// means "not cached" and the edge lands in `unverified`.
pub fn validate_batch(
    ontology: &Ontology,
    memories: &[PreflightMemoryDraft],
    edges: &[PreflightEdgeHint],
    mut lookup_type: impl FnMut(&str) -> Option<u8>,
) -> PreflightResult {
    let mut rejections: Vec<LocalRejection> = Vec::new();
    let mut unverified: Vec<UnverifiedCheck> = Vec::new();

    if memories.is_empty() || memories.len() > 5 {
        rejections.push(LocalRejection {
            draft_key: "*".into(),
            code: "Unknown".into(),
            detail: format!("memories: expected 1..=5, got {}", memories.len()),
            correction: "Submit between 1 and 5 memory drafts; pick the highest-signal.".into(),
        });
        return PreflightResult {
            would_accept: 0,
            would_reject: (memories.len() + edges.len()) as u32,
            rejections,
            unverified,
        };
    }

    let mut types: Vec<(String, u8)> = Vec::with_capacity(memories.len());
    for m in memories {
        let Some(mt) = memory_type(ontology, &m.memory_type) else {
            rejections.push(LocalRejection {
                draft_key: m.draft_key.clone(),
                code: "UnknownMemoryType".into(),
                detail: format!("unknown memory type `{}`", m.memory_type),
                correction: exocortex_wire::corrections::guidance(
                    exocortex_wire::ingest::v1::RejectCode::UnknownMemoryType,
                )
                .correction
                .into(),
            });
            continue;
        };
        let Some(vis) = parse_visibility(&m.visibility) else {
            rejections.push(LocalRejection {
                draft_key: m.draft_key.clone(),
                code: "Unknown".into(),
                detail: format!(
                    "unknown visibility `{}` (expected private|project|team|org)",
                    m.visibility
                ),
                correction: "Use one of: private, project, team, org. Default `project`.".into(),
            });
            continue;
        };
        if !vis.within(CEILING) {
            rejections.push(LocalRejection {
                draft_key: m.draft_key.clone(),
                code: "VisibilityWidening".into(),
                detail: format!("{vis:?} exceeds the session-wrapup ceiling {CEILING:?}"),
                correction: exocortex_wire::corrections::guidance(
                    exocortex_wire::ingest::v1::RejectCode::VisibilityWidening,
                )
                .correction
                .into(),
            });
            continue;
        }
        // W2: the kernel owns the rulebook — same call Submit makes.
        let probe = exocortex_kernel::MemoryDraft {
            memory_type: mt,
            title: m.title.clone().into(),
            content: m.content.clone(),
            summary: None,
            visibility: vis,
            context: exocortex_kernel::MemoryContext {
                timestamp: chrono::Utc::now(),
                project_id: None,
                project_path: None,
                team_id: None,
                tenant_id: None,
                session_id: None,
                user_id: None,
                created_by: None,
                files_involved: Default::default(),
                languages: Default::default(),
                frameworks: Default::default(),
                technologies: Default::default(),
                git_commit: None,
                git_branch: None,
                working_directory: None,
                entities: Default::default(),
                additional_metadata: serde_json::Value::Null,
            },
            edge_hints: Default::default(),
            external_key: None,
        };
        if let Err(e) = exocortex_kernel::validator::validate_draft(
            ontology,
            &probe,
            exocortex_kernel::validator::SourceCeiling {
                source: "preflight",
                ceiling: CEILING,
            },
        ) {
            let code = match &e {
                exocortex_kernel::KernelError::TitleBounds
                | exocortex_kernel::KernelError::EmptyContent
                | exocortex_kernel::KernelError::SummaryBounds
                | exocortex_kernel::KernelError::MetadataTooLarge => "Unknown",
                exocortex_kernel::KernelError::VisibilityWidening { .. } => "VisibilityWidening",
                _ => "Unknown",
            };
            rejections.push(LocalRejection {
                draft_key: m.draft_key.clone(),
                code: code.into(),
                detail: e.to_string(),
                correction: kernel_correction(&e),
            });
            continue;
        }
        types.push((m.draft_key.clone(), mt));
    }

    let type_of =
        |key: &str| -> Option<u8> { types.iter().find(|(k, _)| k == key).map(|(_, t)| *t) };

    for e in edges {
        let row_key = if e.to_memory_id.is_empty() {
            format!("{}->{}", e.from_draft_key, e.to_draft_key)
        } else {
            format!("{}->#{}", e.from_draft_key, e.to_memory_id)
        };
        let Some(from_type) = type_of(&e.from_draft_key) else {
            rejections.push(LocalRejection {
                draft_key: row_key,
                code: "Unknown".into(),
                detail: format!("edge references unknown draft_key `{}`", e.from_draft_key),
                correction: "Edges must reference a draft_key present in this batch, or target an existing memory by to_memory_id.".into(),
            });
            continue;
        };
        // §4.5: exactly one of to_draft_key / to_memory_id.
        let to_type = if !e.to_draft_key.is_empty() {
            if !e.to_memory_id.is_empty() {
                rejections.push(LocalRejection {
                    draft_key: row_key,
                    code: "Unknown".into(),
                    detail: "edge sets both to_draft_key and to_memory_id; exactly one is allowed"
                        .into(),
                    correction:
                        "Pick one target: within-batch draft_key, or an existing memory id.".into(),
                });
                continue;
            }
            match type_of(&e.to_draft_key) {
                Some(t) => Some(t),
                None => {
                    rejections.push(LocalRejection {
                        draft_key: row_key,
                        code: "Unknown".into(),
                        detail: format!("edge references unknown draft_key `{}`", e.to_draft_key),
                        correction: "Edges must reference a draft_key present in this batch."
                            .into(),
                    });
                    continue;
                }
            }
        } else if !e.to_memory_id.is_empty() {
            if e.to_memory_id.len() != 32 || !e.to_memory_id.chars().all(|c| c.is_ascii_hexdigit())
            {
                rejections.push(LocalRejection {
                    draft_key: row_key,
                    code: "InvalidTypeTriple".into(),
                    detail: format!(
                        "to_memory_id `{}` is not a 32-hex memory id",
                        e.to_memory_id
                    ),
                    correction:
                        "Use the 32-hex id exactly as search_memories / get_memory returned it."
                            .into(),
                });
                continue;
            }
            match lookup_type(&e.to_memory_id) {
                Some(t) => Some(t),
                None => {
                    unverified.push(UnverifiedCheck {
                        key: row_key,
                        reason: "to_memory_id target not in local cache; the type triple is checked server-side".into(),
                    });
                    continue;
                }
            }
        } else {
            rejections.push(LocalRejection {
                draft_key: row_key,
                code: "Unknown".into(),
                detail: "edge has neither to_draft_key nor to_memory_id".into(),
                correction:
                    "Set exactly one target: within-batch draft_key, or an existing memory id."
                        .into(),
            });
            continue;
        };

        let Some(kind) = ontology.kind_id(&e.kind) else {
            rejections.push(LocalRejection {
                draft_key: row_key,
                code: "UnknownKind".into(),
                detail: format!("unknown relationship kind `{}`", e.kind),
                correction: exocortex_wire::corrections::guidance(
                    exocortex_wire::ingest::v1::RejectCode::UnknownKind,
                )
                .correction
                .into(),
            });
            continue;
        };
        if ontology
            .kinds_by_id
            .get(&kind)
            .is_some_and(|m| m.computed_only)
        {
            rejections.push(LocalRejection {
                draft_key: row_key,
                code: "ComputedKindRejected".into(),
                detail: format!(
                    "`{}` is computed-only (R-T14); producers may not assert it",
                    e.kind
                ),
                correction: exocortex_wire::corrections::guidance(
                    exocortex_wire::ingest::v1::RejectCode::ComputedKindRejected,
                )
                .correction
                .into(),
            });
            continue;
        }
        if let Some(to_type) = to_type {
            if let Err(err) =
                exocortex_kernel::validator::validate_triple(ontology, from_type, kind, to_type)
            {
                rejections.push(LocalRejection {
                    draft_key: row_key,
                    code: "InvalidTypeTriple".into(),
                    detail: err.to_string(),
                    correction: exocortex_wire::corrections::guidance(
                        exocortex_wire::ingest::v1::RejectCode::InvalidTypeTriple,
                    )
                    .correction
                    .into(),
                });
            }
        }
    }

    let would_reject = rejections.len() as u32;
    PreflightResult {
        would_accept: (memories.len() + edges.len()) as u32
            - would_reject.min((memories.len() + edges.len()) as u32),
        would_reject,
        rejections,
        unverified,
    }
}

/// Kernel-side correction text (§4.2's kernel table; the exhaustive
/// `KernelError` match a new variant must extend).
fn kernel_correction(e: &exocortex_kernel::KernelError) -> String {
    use exocortex_kernel::KernelError;
    match e {
        KernelError::TitleBounds => {
            "Titles are 1..=200 chars. Trim to a subject-verb-object sentence.".to_string()
        }
        KernelError::EmptyContent => {
            "Content must be non-empty. Write what a future session needs.".to_string()
        }
        KernelError::SummaryBounds => "Summaries are <=500 chars. Shorten.".to_string(),
        KernelError::MetadataTooLarge => {
            "additional_metadata exceeds 8 KiB serialized. Move detail into content.".to_string()
        }
        KernelError::VisibilityWidening { .. } => {
            "Visibility above the source ceiling. Drop to a lower label.".to_string()
        }
        KernelError::InvalidTypeTriple { kind, from } => format!(
            "Kind {kind:?} does not accept from-type {from}; check the kind catalogue — `RelatedTo` is the always-valid fallback."
        ),
        KernelError::UnknownKind(_) => {
            "Kind name not in this pack. Check the spelling against the catalogue.".to_string()
        }
        KernelError::ScoreOutOfRange(_) => "Scores must be within [0.0, 1.0].".to_string(),
        KernelError::DuplicatePack(_)
        | KernelError::DuplicateKind(_)
        | KernelError::DuplicateTypeName(_)
        | KernelError::UnboundKernelConstant(_) => {
            "Ontology assembly error; surface to the user.".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ontology() -> Ontology {
        let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
        exocortex_kernel::pack::load_registered_packs().expect("pack set assembles")
    }

    fn draft(key: &str, ty: &str, title: &str) -> PreflightMemoryDraft {
        PreflightMemoryDraft {
            draft_key: key.into(),
            memory_type: ty.into(),
            title: title.into(),
            content: format!("{title}: body"),
            visibility: "project".into(),
            tags: vec![],
        }
    }

    fn edge(from: &str, to: &str, kind: &str) -> PreflightEdgeHint {
        PreflightEdgeHint {
            from_draft_key: from.into(),
            to_draft_key: to.into(),
            to_memory_id: String::new(),
            kind: kind.into(),
            strength: 0.0,
        }
    }

    #[test]
    fn clean_batch_passes_with_full_triples() {
        let onto = ontology();
        let mems = [
            draft("p", "Problem", "Pool exhausted"),
            draft("f", "Fix", "Fixed pool exhaustion"),
        ];
        let edges = [edge("f", "p", "Fixes")];
        let r = validate_batch(&onto, &mems, &edges, |_| None);
        assert!(r.rejections.is_empty(), "{:?}", r.rejections);
        assert_eq!(r.would_reject, 0);
    }

    #[test]
    fn bad_triple_and_unknown_kind_reject_with_corrections() {
        let onto = ontology();
        let mems = [
            draft("c", "Command", "cargo test"),
            draft("t", "Technology", "falkor"),
        ];
        // Fixes requires (Fix, Error|Problem); Command->Technology is invalid.
        let mut r = validate_batch(&onto, &mems, &[edge("c", "t", "Fixes")], |_| None);
        assert_eq!(r.rejections[0].code, "InvalidTypeTriple");
        assert!(r.rejections[0].correction.contains("RelatedTo"));
        r = validate_batch(&onto, &mems, &[edge("c", "t", "Extends")], |_| None);
        assert_eq!(r.rejections[0].code, "UnknownKind");
    }

    #[test]
    fn computed_only_kind_rejected_locally() {
        let onto = ontology();
        let mems = [draft("a", "Technology", "a"), draft("b", "Technology", "b")];
        let r = validate_batch(&onto, &mems, &[edge("a", "b", "SimilarTo")], |_| None);
        assert_eq!(r.rejections[0].code, "ComputedKindRejected");
    }

    #[test]
    fn cross_batch_edge_checked_when_cached_else_unverified() {
        let onto = ontology();
        let mems = [draft("f", "Fix", "Fixed it")];
        let mut e = edge("f", "", "Fixes");
        e.to_draft_key = String::new();
        e.to_memory_id = "0".repeat(32);
        // Cached as Error → triple OK.
        let r = validate_batch(&onto, &mems, &[e.clone()], |_| {
            Some(onto.memory_type_by_name["Error"])
        });
        assert!(r.rejections.is_empty(), "{:?}", r.rejections);
        // Cached as Technology → triple fails server-side-equivalently.
        let r = validate_batch(&onto, &mems, &[e.clone()], |_| {
            Some(onto.memory_type_by_name["Technology"])
        });
        assert_eq!(r.rejections[0].code, "InvalidTypeTriple");
        // Not cached → unverified, never a silent pass.
        let r = validate_batch(&onto, &mems, &[e], |_| None);
        assert!(r.rejections.is_empty());
        assert_eq!(r.unverified.len(), 1);
        assert!(r.unverified[0].reason.contains("server-side"));
    }

    #[test]
    fn bound_violations_reject() {
        let onto = ontology();
        let long = "x".repeat(201);
        let mems = [draft("a", "Fix", &long)];
        let r = validate_batch(&onto, &mems, &[], |_| None);
        assert_eq!(r.rejections[0].code, "Unknown");
        assert!(r.rejections[0].detail.contains("200"));
    }
}
