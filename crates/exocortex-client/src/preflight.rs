//! Client-side facade over the ONE preflight implementation, which
//! lives in `exocortex-ops` (D2/CR-9): the same registry handler serves
//! the client's MCP dispatch and the backend's HTTP bind, running the
//! same kernel rulebook the ingest server runs (W2). This module keeps
//! the client's historical import path stable.

pub use exocortex_ops::preflight::{
    validate_batch, LocalRejection, PreflightEdgeHint, PreflightMemoryDraft, PreflightResult,
    UnverifiedCheck,
};
