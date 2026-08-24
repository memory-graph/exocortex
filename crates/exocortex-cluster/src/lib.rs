//! Backend-only cluster machinery: gossip membership, Redis coherence, Chubby-style leases, HMAC-signed invalidation transport.
//!
//! See PRD §9; populated at M5.
#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

// Modules populated by the milestone that owns this crate.
// Empty compilable scaffold at M0:
/// Compile-time placeholder; replaced when the owning milestone lands.
pub fn __placeholder() {}
