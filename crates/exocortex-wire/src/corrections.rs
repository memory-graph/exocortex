// crates/exocortex-wire/src/corrections.rs
//! The ONE reject-guidance table (agent-instructions PRD §4.2): every
//! `RejectCode` maps to exactly one disposition AND one correction
//! template. Exhaustive, no wildcard — a new variant added to the proto
//! fails compilation here instead of silently defaulting.
//!
//! `exocortex-adapter-sdk::classify` is a thin
//! `guidance(code).disposition` over this table (R-I4 single-dependency
//! holds: the SDK already depends on wire). Adapters keep their triage
//! dispositions and gain remediation text; the MCP client's preflight
//! surfaces the same strings the playbook's generated table teaches.

use crate::ingest::v1::RejectCode;

/// What a producer does with a rejection (R13 triage).
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

/// One row of guidance: disposition + the deterministic correction text.
#[derive(Clone, Copy, Debug)]
pub struct RejectGuidance {
    /// Triage disposition (R13).
    pub disposition: Disposition,
    /// Static, deterministic remediation text. Never LLM-generated.
    pub correction: &'static str,
}

/// The exhaustive guidance table. New `RejectCode` variant → new row in
/// the same PR (compile-enforced).
pub fn guidance(code: RejectCode) -> RejectGuidance {
    use RejectCode::*;
    match code {
        DuplicateBatch => RejectGuidance {
            disposition: Disposition::Success,
            correction: "Transport replayed the same batch id. Harmless — do NOT resubmit.",
        },
        RateLimited => RejectGuidance {
            disposition: Disposition::Retry,
            correction: "Backend is shedding load. Transient; the client retries. Do not resubmit by hand.",
        },
        Unknown => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Generic validation failure (often title empty/>200 chars or empty content). Read `detail`; usually trim the title.",
        },
        UnknownMemoryType => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Memory type not in this pack. Fix the type name; use only the types the playbook lists.",
        },
        UnknownKind => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Kind name typo or not in this pack. Fix the spelling; use only the assertable kinds the playbook lists.",
        },
        InvalidTypeTriple => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Kind does not fit the (from, to) memory types. Check the kind catalogue; `RelatedTo` is the always-valid fallback.",
        },
        VisibilityWidening => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Visibility above the source ceiling. Drop to a lower label (`project` is the default).",
        },
        MissingExternalKey => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "External-snapshot batch without ExternalKey coordinates. Cannot occur for session wrapups; if seen, report it as a bug.",
        },
        InvalidExternalKey => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Malformed external coordinates (table_uuid must be 16 bytes, schema_hash 32). Cannot occur for session wrapups; report it as a bug.",
        },
        ResourceLimitExceeded => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Reduce request bytes, content, tags, or edges to the advertised fixed limits.",
        },
        BadChecksum => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "Batch checksum mismatch. The batch was mutated after signing; rebuild it from the drafts.",
        },
        ComputedKindRejected => RejectGuidance {
            disposition: Disposition::Permanent,
            correction: "You asserted a computed-only kind (e.g. `SimilarTo`); only the consolidation cycle may. Use `RelatedTo` or `AnalogousTo`.",
        },
        Unauthorized => RejectGuidance {
            disposition: Disposition::Fatal,
            correction: "Credentials rejected (HMAC missing/invalid). Surface to the user; not fixable by you.",
        },
        UnknownSource => RejectGuidance {
            disposition: Disposition::Fatal,
            correction: "Producer not registered, ceiling mismatch, or wrong org. Surface to the user; not fixable by you.",
        },
        IncompatibleOntology => RejectGuidance {
            disposition: Disposition::Fatal,
            correction: "Ontology fingerprint mismatch — client and backend run different packs. Surface to the user; not fixable by you.",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_has_guidance() {
        // Exhaustiveness is compile-checked by `guidance`; this pins the
        // dispositions so a regrouping is caught.
        use RejectCode::*;
        let cases = [
            (DuplicateBatch, Disposition::Success),
            (RateLimited, Disposition::Retry),
            (UnknownMemoryType, Disposition::Permanent),
            (UnknownKind, Disposition::Permanent),
            (InvalidTypeTriple, Disposition::Permanent),
            (VisibilityWidening, Disposition::Permanent),
            (MissingExternalKey, Disposition::Permanent),
            (BadChecksum, Disposition::Permanent),
            (ComputedKindRejected, Disposition::Permanent),
            (Unknown, Disposition::Permanent),
            (InvalidExternalKey, Disposition::Permanent),
            (ResourceLimitExceeded, Disposition::Permanent),
            (Unauthorized, Disposition::Fatal),
            (UnknownSource, Disposition::Fatal),
            (IncompatibleOntology, Disposition::Fatal),
        ];
        for (code, expected) in cases {
            let g = guidance(code);
            assert_eq!(g.disposition, expected, "{code:?}");
            assert!(!g.correction.is_empty(), "{code:?} correction text");
        }
    }
}
