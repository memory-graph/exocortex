//! MCP tools the client exposes beyond the kernel Functions (§13.5).

pub mod end_session;

pub use end_session::{EndSessionAck, EndSessionArgs, EndSessionTool};
