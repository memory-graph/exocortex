//! The two-language reasoning layer (§10): Crepe (compile-time Datalog) for
//! derivation — kernel rules R1-R5 and R7-R9 plus dev-v1 pack rules D1-D6 —
//! and Steel (embedded Scheme) for belief evolution and explanation traces.
//! R6 `reverse_solves` is the one Steel rule in the catalogue (§10.4).
//!
//! No LLM anywhere (CR-19). No serialization on the reasoning read path
//! (CR-8; asserted by a workspace test).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod engine;
pub mod explain;
pub mod rules;

pub use engine::{ReasoningEngine, ReasoningWork};
pub use explain::{EdgeFacts, ExplainEngine, EXPLAIN_SCM};
pub use rules::{prime, Derived};
