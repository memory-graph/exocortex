// fingerprint.rs — OntologyFingerprint (§7.17, refined by OC-PRD D1)
//
// The enforced hash is the COMPATIBILITY level: it covers exactly the
// ontology's meaning (type-name→id mappings, kind rows, triples, rule
// ids) and excludes release metadata. The full-build hash lives beside
// it as `BuildFingerprint` and never gates anything. See
// `compatibility.rs` and docs/prd/ontology-compatibility-prd.md.
pub use crate::compatibility::CompatibilityFingerprint as OntologyFingerprint;
