// crates/exocortex-adapter-sdk/src/classify.rs
//! Reject triage (R13): every `RejectCode` variant maps to exactly one
//! disposition, and the match is exhaustive with no wildcard — a new
//! variant added to the wire fails compilation here instead of silently
//! defaulting to success.

use exocortex_wire::ingest::v1::RejectCode;

/// What the adapter does with a rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Idempotent replay — treat as success.
    Success,
    /// Transient — retry with backoff.
    Retry,
    /// Permanent for these rows — record, never retry, cursor may settle.
    Permanent,
    /// Session-fatal — abort; the cursor does not advance.
    Fatal,
}

/// The exhaustive triage table.
///
/// Variants not enumerated by the PRD's R13 are classified by their
/// semantics and documented: `Unknown` is a generic validation failure
/// (retrying identical bytes cannot help) → Permanent; B8/B9's
/// `InvalidExternalKey` is malformed producer data → Permanent.
pub fn classify(code: RejectCode) -> Disposition {
    use RejectCode::*;
    match code {
        // Success.
        DuplicateBatch => Disposition::Success,
        // Transient.
        RateLimited => Disposition::Retry,
        // Permanent: these bytes will never validate.
        Unknown => Disposition::Permanent,
        UnknownMemoryType => Disposition::Permanent,
        UnknownKind => Disposition::Permanent,
        InvalidTypeTriple => Disposition::Permanent,
        VisibilityWidening => Disposition::Permanent,
        MissingExternalKey => Disposition::Permanent,
        BadChecksum => Disposition::Permanent,
        ComputedKindRejected => Disposition::Permanent,
        InvalidExternalKey => Disposition::Permanent,
        // Fatal: the session itself is no longer viable.
        Unauthorized => Disposition::Fatal,
        UnknownSource => Disposition::Fatal,
        IncompatibleOntology => Disposition::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_is_classified() {
        // Exhaustiveness is compile-checked by `classify`; this test pins
        // the specified mapping so a regrouping is caught.
        let cases = [
            (RejectCode::DuplicateBatch, Disposition::Success),
            (RejectCode::RateLimited, Disposition::Retry),
            (RejectCode::UnknownMemoryType, Disposition::Permanent),
            (RejectCode::UnknownKind, Disposition::Permanent),
            (RejectCode::InvalidTypeTriple, Disposition::Permanent),
            (RejectCode::VisibilityWidening, Disposition::Permanent),
            (RejectCode::MissingExternalKey, Disposition::Permanent),
            (RejectCode::BadChecksum, Disposition::Permanent),
            (RejectCode::ComputedKindRejected, Disposition::Permanent),
            (RejectCode::Unknown, Disposition::Permanent),
            (RejectCode::InvalidExternalKey, Disposition::Permanent),
            (RejectCode::Unauthorized, Disposition::Fatal),
            (RejectCode::UnknownSource, Disposition::Fatal),
            (RejectCode::IncompatibleOntology, Disposition::Fatal),
        ];
        for (code, expected) in cases {
            assert_eq!(classify(code), expected, "{code:?}");
        }
    }
}
