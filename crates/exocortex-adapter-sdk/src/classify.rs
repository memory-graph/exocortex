// crates/exocortex-adapter-sdk/src/classify.rs
//! Reject triage (R13), thin over the ONE wire table (agent-instructions
//! PRD §4.2). `exocortex_wire::corrections` owns the exhaustive
//! `RejectCode → { disposition, correction }` mapping; this module keeps
//! the SDK's historical `classify` surface so adapters keep compiling,
//! and re-exports the shared types so the drift a second table would
//! create cannot exist.

pub use exocortex_wire::corrections::Disposition;

/// The exhaustive triage table (thin wrapper: `guidance(code).disposition`).
///
/// Variants not enumerated by the PRD's R13 are classified by their
/// semantics in the wire table and documented there.
pub fn classify(code: exocortex_wire::ingest::v1::RejectCode) -> Disposition {
    exocortex_wire::corrections::guidance(code).disposition
}

#[cfg(test)]
mod tests {
    use super::*;
    use exocortex_wire::ingest::v1::RejectCode;

    #[test]
    fn classify_matches_the_wire_table() {
        // The wrapper cannot disagree with the table it wraps; this pins
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
