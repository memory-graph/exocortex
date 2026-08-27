//! `exocortex-server` library surface: the SSE hub router (§9.7) and the
//! HTTP parity binding (§21.1) reused by the `exocortex-node` binary and
//! integration tests.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod backend;
pub mod http_bind;
pub mod org_backup;
pub mod sse;
