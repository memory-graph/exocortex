//! D2/D3 through the operation registry (CR-9): `preflight_wrapup` and
//! `playbook_version` register as `OperationEntry`s here so every
//! surface that links this crate — the stdio MCP server and any HTTP
//! bind — enumerates and dispatches the SAME handlers. The MCP methods
//! in `mcp.rs` route through these entries rather than re-implementing.

use schemars::JsonSchema;
use serde::Serialize;

use exocortex_ops::{OpContext, OpError};

/// `playbook_version` output (D3 §3.3).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PlaybookVersionOutput {
    /// The compiled playbook version.
    pub version: String,
    /// `sha256:…` content hash of the playbook.
    pub playbook_hash: String,
    /// `sha256:…` content hash of the instruction block.
    pub block_hash: String,
}

async fn playbook_version_handle(
    _ctx: &OpContext,
    _input: serde_json::Value,
) -> Result<PlaybookVersionOutput, OpError> {
    Ok(PlaybookVersionOutput {
        version: crate::playbook::PLAYBOOK_VERSION.into(),
        playbook_hash: crate::playbook::playbook_hash(),
        block_hash: crate::playbook::block_hash(),
    })
}

inventory::submit! {
    exocortex_ops::OperationEntry {
        name: "playbook_version",
        mcp_tool_name: "exocortex.playbook_version",
        http_method: || http::Method::POST,
        http_path: "/v1/playbook_version",
        input_schema: || schemars::schema_for!(serde_json::Value),
        output_schema: || schemars::schema_for!(PlaybookVersionOutput),
        handler: |ctx, v| Box::pin(async move {
            let out = playbook_version_handle(ctx, v).await?;
            serde_json::to_value(out).map_err(|e| OpError::Other(e.to_string()))
        }),
    }
}
