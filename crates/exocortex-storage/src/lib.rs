// crates/exocortex-storage/src/lib.rs
//! The storage seam: the `Storage` trait, the FalkorDB adapter, and the
//! `InMemoryStorage` test double (§6). Cypher never leaves this crate (CR-10).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

/// The compile-time Cypher template catalogue (§6.4, CR-10).
pub mod cypher;
/// The FalkorDB adapter (§6.5).
pub mod falkor;
/// The deterministic in-memory test double (§6.6).
pub mod in_memory;
/// The one deliberate seam: the `Storage` trait (§6.1) and `StorageError`.
pub mod trait_;
/// Support types referenced by the `Storage` signatures (§6.3).
pub mod types;

pub use falkor::{FalkorConfig, FalkorStorage};
pub use in_memory::InMemoryStorage;
pub use trait_::{Storage, StorageError};
pub use types::{
    memory_visible, CommitRecord, CypherQuery, Direction, Embedding, GraphSnapshot, Invalidation,
    LeaseKey, MemoryFilter, OwnerLease, RegionKey, ResultSet, StorageBackendId,
    StorageCapabilities, TraversalSpec, VisibilityContext,
};

/// Result alias for storage operations (§6.1).
pub type Result<T> = std::result::Result<T, StorageError>;
