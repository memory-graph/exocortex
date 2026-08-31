//! Exocortex kernel — universal ontology machinery.
//!
//! The kernel defines the shape of a `Memory`, a `Relationship`, and the rules
//! by which one may enter the graph. It defines *no* concrete `MemoryType`,
//! `EntityType`, or named relationship kind — those come from packs
//! (supplied by linked ontology-pack crates). See PRD §7.
//!
//! # Invariants enforced here
//! - R-Pk1..R-Pk5 pack constraints (see `pack::registry`)
//! - R-T1..R-T18a memory/relationship rules
//! - R-I1..R-I7 ingestion protocol invariants (validator half — the wire half
//!   lives in `exocortex-wire`)
//! - CR-19: no LLM. Depend on this crate transitively at your peril if you
//!   want to add one.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

mod macros;

/// Typed writes — the four kernel Actions (§7.11).
pub mod actions;
/// OC-PRD: the two-level fingerprint and the per-boundary
/// compatibility policy (compat/build hashes, structured summary,
/// subset/superset verdicts, the six boundary rules).
pub mod compatibility;
/// The write-path input shapes: `MemoryDraft`, `EdgeHint` (§7.14).
pub mod draft;
/// Typed entities a memory is about (§7.2).
pub mod entity;
/// The kernel error surface.
pub mod error;
/// `OntologyFingerprint` — SHA-256 over the effective ontology (§7.17).
pub mod fingerprint;
/// Typed reads — the four kernel Functions (§7.12).
pub mod functions;
/// Deterministic identity types: `MemoryId`, `RelationshipId`, `EntityId`, `PackId`, `LSN`.
pub mod ids;
/// Relationship-kind handles, buckets, metadata, and kernel constants (§7.3, §7.4).
pub mod kinds;
/// The canonical `Memory` envelope and `MemoryContext` (§7.5, §7.6).
pub mod memory;
/// The effective ontology assembled from registered packs.
pub mod ontology;
/// Pack registration: `PackDef` and `load_registered_packs` (§7.0).
pub mod pack;
/// The six-variant provenance enum and external-snapshot coordinates (§7.9).
pub mod provenance;
/// The canonical typed `Relationship` and its property bag (§7.8).
pub mod relationship;
/// The type-triple validator and no-widening enforcer (§7.15, R-T11a).
pub mod validator;
/// Pack-registered Actions and Functions — the `actions!`/`functions!`/
/// `guidance!` `pack!` sections and their registrations (PX2).
pub mod verbs;
/// The required `Visibility` label and its ordering (§7.7).
pub mod visibility;

pub use compatibility::{
    admit_backup, admit_node_graph, admit_peer, admit_producer_batch, BackupOntology,
    BuildFingerprint, CompatibilityError, CompatibilityFingerprint, NodeGraphDecision,
    OntologySummary, PersistedPin, PinnedOntology, MAX_ACCEPTED_FINGERPRINTS,
};
pub use draft::{EdgeHint, MemoryDraft};
pub use error::{KernelError, KernelResult};
pub use fingerprint::OntologyFingerprint;
pub use ids::{EntityId, MemoryId, PackId, RelationshipId, LSN};
pub use kinds::{RelBucket, RelKindId, RelMeta};
pub use memory::{normalize_tag, normalize_tags, Embedding, EmbeddingModel, Memory, MemoryContext};
pub use ontology::Ontology;
pub use pack::{PackDef, PackVersion};
pub use provenance::{ExternalKey, ExternalSnapshot, ProducerKind, Provenance};
pub use relationship::{materialize_inverse, Relationship, RelationshipProperties};
pub use verbs::{
    registered_pack_actions, registered_pack_functions, ActionContext, ActionEdge, ActionMemory,
    ActionProduct, ActionTarget, GuidanceEntry, GuidanceLink, PackActionDef, PackFunctionDef,
};
pub use visibility::{narrowest_visibility, relationship_visibility, Visibility};

// PX2 hidden re-exports: the `actions!`/`functions!` munchers derive the
// generated `Input` structs through these paths so a pack crate stays
// single-dependency (kernel only).
#[doc(hidden)]
pub use schemars;
#[doc(hidden)]
pub use serde;
