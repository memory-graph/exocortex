//! The Ingestion Protocol server (§7.13, §18): the single validated write
//! path. `IngestService.Submit` runs the kernel validation pipeline
//! (fingerprint, source admission + ceiling, R-T11a no-widening, R-T17
//! triples, idempotency by (producer_id, batch_id), bounded admission before
//! full-batch HMAC authentication (R-I8), and commits atomically.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod embedding;
pub mod entities;
pub mod grouping;
pub mod service;

pub use embedding::{Embedder, EmbedderRef, FakeEmbedder};
pub use entities::EntityExtractor;
pub use service::{IngestServer, ReindexReport};
