//! `exocortex-client` library surface: the MCP server handler and the local
//! WAL, reused by the `exocortex-mcp-client` binary (§4.2) and by tests.

pub mod mcp;
pub mod tools;
pub mod wal;
