//! D3 federation (§24 q15): the derived common-subset plan — DERIVED,
//! never negotiated (OC-PRD §5.2 amendment: no handshake agreeing on a
//! reduced ontology; each side computes the same answer from the same
//! pinned identities, exactly like the fingerprint itself).
//!
//! The rule: canonical packs sort by name, and the common subset of two
//! pack lists is the maximal shared PREFIX of that order. Memory-type
//! ids are running offsets and kind ids carry a per-pack slot
//! (`0x8000_0000 | index<<16 | local`) assigned in canonical order, so
//! a prefix's ids are identical whether the packs compose alone or
//! in-situ — the OC-PRD D3 subset-is-prefix rule, made computational.
//! A mid/tail subset (e.g. `[mortgage]` alone against
//! `[dev, mortgage]`) re-slots its kind ids and therefore hashes
//! differently: it is refused, never guessed.
//!
//! The policy lives in [`crate::compatibility`] (the D2 policy home —
//! every fingerprint comparison stays there, as the
//! compatibility-policy gate enforces); this module is the named
//! surface callers and docs cite.

pub use crate::compatibility::{common_subset, prefix_fingerprints, CommonSubsetPlan};
