// error.rs — the kernel error surface.
//
// NOTE (M0 deviation, recorded in the milestone report): PRD §2.6.1 shows this
// enum with `#[derive(Debug, Error)]`. thiserror reserves a variant field
// named `source` for the error-cause chain and requires it to implement
// `std::error::Error`, which `&'static str` does not — the derived form does
// not compile. The enum shape, field names, and message strings below are
// byte-identical to §2.6.1; only `Display`/`Error` are implemented by hand.
use crate::{RelKindId, Visibility};

/// Result alias for kernel operations.
pub type KernelResult<T> = Result<T, KernelError>;

/// The kernel error surface.
#[derive(Debug)]
pub enum KernelError {
    /// Two packs share a name (R-Pk1).
    DuplicatePack(smol_str::SmolStr),
    /// Duplicate RelKindId across packs.
    DuplicateKind(RelKindId),
    /// Kernel constant not bound by any registered pack (R-Pk2).
    UnboundKernelConstant(RelKindId),
    /// Unknown RelKindId in effective ontology.
    UnknownKind(RelKindId),
    /// Invalid (from-type, kind, to-type) combination (R-T17).
    InvalidTypeTriple {
        /// The offending kind.
        kind: RelKindId,
        /// The offending source memory type.
        from: u8,
    },
    /// Title must be 1..=200 chars (R-T5).
    TitleBounds,
    /// Content must be non-empty (R-T5).
    EmptyContent,
    /// Visibility wider than the source ceiling (R-T11a).
    VisibilityWidening {
        /// The ingestion source whose ceiling was exceeded.
        source: &'static str,
        /// The registered ceiling.
        ceiling: Visibility,
        /// The visibility the batch attempted.
        attempted: Visibility,
    },
    /// Summary must be <=500 chars (R-T5).
    SummaryBounds,
    /// additional_metadata exceeds 8 KiB serialized (R-T10).
    MetadataTooLarge,
    /// Score out of [0.0, 1.0].
    ScoreOutOfRange(f32),
    /// Two packs declare the same memory/entity type name (KP2: silently
    /// re-resolving the shared name to the later pack's id made every
    /// first-pack type triple evaluate against the wrong ids).
    DuplicateTypeName(smol_str::SmolStr),
}

impl std::fmt::Display for KernelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KernelError::DuplicatePack(name) => write!(f, "two packs share the name `{name}` (R-Pk1)"),
            KernelError::DuplicateKind(id) => write!(f, "duplicate RelKindId {id:?} across packs"),
            KernelError::UnboundKernelConstant(id) => {
                write!(f, "kernel constant {id:?} is not bound by any registered pack (R-Pk2)")
            }
            KernelError::UnknownKind(id) => write!(f, "unknown RelKindId {id:?} in effective ontology"),
            KernelError::InvalidTypeTriple { kind, from } => {
                write!(f, "invalid type triple: kind {kind:?} on memory_type {from}")
            }
            KernelError::TitleBounds => write!(f, "title must be 1..=200 chars (R-T5)"),
            KernelError::EmptyContent => write!(f, "content must be non-empty (R-T5)"),
            KernelError::VisibilityWidening { source, ceiling, attempted } => write!(
                f,
                "visibility widening rejected: source={source} ceiling={ceiling:?} attempted={attempted:?}"
            ),
            KernelError::SummaryBounds => write!(f, "summary must be <=500 chars (R-T5)"),
            KernelError::MetadataTooLarge => {
                write!(f, "additional_metadata exceeds 8 KiB serialized (R-T10)")
            }
            KernelError::ScoreOutOfRange(v) => write!(f, "score {v} out of [0.0, 1.0]"),
            KernelError::DuplicateTypeName(name) => write!(
                f,
                "two packs declare the memory/entity type name `{name}` (KP2)"
            ),
        }
    }
}

impl std::error::Error for KernelError {}
