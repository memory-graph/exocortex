// validator.rs — the type-triple validator and no-widening enforcer.
use crate::{KernelError, KernelResult, MemoryDraft, Ontology, Visibility};

/// The per-source visibility ceiling registered at admission time (R-T11a).
#[derive(Clone, Copy, Debug)]
pub struct SourceCeiling {
    /// Name of the ingestion source (e.g. "session-wrapup").
    pub source: &'static str,
    /// The maximum visibility memories from this source may carry.
    pub ceiling: Visibility,
}

/// Validate a single draft against the effective ontology.
///
/// Enforces:
///  - R-T5..R-T10 field bounds
///  - R-T11a no-widening
///  - R-T17 type-triple rules
pub fn validate_draft(
    onto: &Ontology,
    draft: &MemoryDraft,
    ceiling: SourceCeiling,
) -> KernelResult<()> {
    // R-T5
    if draft.title.is_empty() || draft.title.len() > 200 {
        return Err(KernelError::TitleBounds);
    }
    if draft.content.is_empty() {
        return Err(KernelError::EmptyContent);
    }
    if let Some(s) = &draft.summary {
        if s.len() > 500 {
            return Err(KernelError::SummaryBounds);
        }
    }
    if draft.context.additional_metadata.to_string().len() > 8 * 1024 {
        return Err(KernelError::MetadataTooLarge);
    }
    // R-T11a
    if !draft.visibility.within(ceiling.ceiling) {
        return Err(KernelError::VisibilityWidening {
            source: ceiling.source,
            ceiling: ceiling.ceiling,
            attempted: draft.visibility,
        });
    }
    // R-T17
    for hint in &draft.edge_hints {
        let triples = onto
            .triples_by_kind
            .get(&hint.kind)
            .ok_or(KernelError::UnknownKind(hint.kind))?;
        if !triples
            .iter()
            .any(|t| matches_triple(t, draft.memory_type, /* to_type */ None))
        {
            return Err(KernelError::InvalidTypeTriple {
                kind: hint.kind,
                from: draft.memory_type,
            });
        }
    }
    Ok(())
}

fn matches_triple(t: &crate::pack::TypeTriple, from: u8, to: Option<u8>) -> bool {
    let from_ok = t.from_types.as_deref().is_none_or(|xs| xs.contains(&from));
    let to_ok = match (t.to_types.as_deref(), to) {
        (None, _) => true,
        (Some(_), None) => true, // to-side deferred to the peer draft
        (Some(xs), Some(v)) => xs.contains(&v),
    };
    from_ok && to_ok
}
