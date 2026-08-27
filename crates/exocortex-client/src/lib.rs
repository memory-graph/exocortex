//! `exocortex-client` library surface: the MCP server handler and the local
//! WAL, reused by the `exocortex-mcp-client` binary (§4.2) and by tests.

pub mod backup;
pub mod drain;
pub mod materialize;
pub mod mcp;
pub mod no_backend;
pub mod ops_registrations;
pub mod playbook;
pub mod preflight;
pub mod sync;
pub mod tools;
pub mod wal;
