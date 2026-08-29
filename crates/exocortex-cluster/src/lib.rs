//! Backend-only cluster machinery (§9): Chubby-style Redis leases with
//! fencing epochs, HMAC-signed invalidation envelopes, peer admission gated
//! on wire version + ontology fingerprint (§9.1), and the SSE surface used
//! by subscribed clients.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod change_log;
pub mod node;
pub mod sse;

pub use change_log::{ChangeLog, Replay, RingChangeLog};
pub use node::{ClusterError, ClusterNode, FeedHealth};
