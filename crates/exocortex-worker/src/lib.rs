//! Out-of-process adapter host. Deliberately does NOT link exocortex-kernel (R-I1); talks the Ingestion Protocol over the network.
//!
//! See PRD §18; populated at M6.
#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

// Modules populated by the milestone that owns this crate.
// Empty compilable scaffold at M0:
/// Compile-time placeholder; replaced when the owning milestone lands.
pub fn __placeholder() {}
