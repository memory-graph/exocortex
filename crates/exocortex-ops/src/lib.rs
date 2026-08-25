// crates/exocortex-ops/src/lib.rs
//! The operation registry (§21): every capability implements `Operation`,
//! registers one `OperationEntry` via `inventory::submit!`, and both the MCP
//! tool catalogue and the OpenAPI surface enumerate the SAME registry
//! (R-P1/R-P2: no operation exists on only one surface).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod audit;
pub mod operations;

/// Client-facing shared types (§2.5 routes the client through ops; these
/// re-exports let the client crate consume the visibility/delta vocabulary
/// without a direct edge into the storage adapter crate).
pub use exocortex_storage::{Direction, Invalidation, TraversalSpec, VisibilityContext};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

/// Operation context: the typed handles every operation runs against.
pub struct OpContext {
    /// The caller's identity + visibility scope.
    pub visibility_ctx: exocortex_storage::VisibilityContext,
    /// Durable storage.
    pub storage: std::sync::Arc<dyn exocortex_storage::Storage>,
    /// The local cache (client + backend read path).
    pub cache: std::sync::Arc<exocortex_cache::LocalCache>,
    /// Deadline for this operation (R-R3 budget enforcement).
    pub deadline: chrono::DateTime<chrono::Utc>,
}

/// Operation errors.
#[derive(Debug, thiserror::Error)]
pub enum OpError {
    /// Bad input shape.
    #[error("bad input: {0}")]
    BadInput(String),
    /// Caller not permitted.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Target missing.
    #[error("not found")]
    NotFound,
    /// Deadline exceeded (R-R2).
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// Storage failure.
    #[error("storage: {0}")]
    Storage(String),
    /// Anything else.
    #[error("{0}")]
    Other(String),
}

/// The typed operation surface (§21.1).
#[async_trait]
pub trait Operation: Send + Sync + 'static {
    /// Input shape.
    type Input: DeserializeOwned + JsonSchema + Send;
    /// Output shape.
    type Output: Serialize + JsonSchema + Send;
    /// Stable operation name.
    fn name(&self) -> &'static str;
    /// MCP tool name (`exocortex.*`).
    fn mcp_tool_name(&self) -> &'static str;
    /// HTTP method.
    fn http_method(&self) -> http::Method;
    /// HTTP path.
    fn http_path(&self) -> &'static str;
    /// Execute.
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError>;
}

/// Type-erased registration for inventory. Each Operation impl submits one
/// of these; MCP and HTTP surfaces iterate `inventory::iter::<OperationEntry>`
/// at startup.
pub struct OperationEntry {
    /// Stable operation name.
    pub name: &'static str,
    /// MCP tool name.
    pub mcp_tool_name: &'static str,
    /// HTTP method constructor.
    pub http_method: fn() -> http::Method,
    /// HTTP path.
    pub http_path: &'static str,
    /// JSON Schema for the input.
    pub input_schema: fn() -> schemars::schema::RootSchema,
    /// JSON Schema for the output.
    pub output_schema: fn() -> schemars::schema::RootSchema,
    /// Type-erased handler: JSON in, JSON out.
    #[allow(clippy::type_complexity)]
    pub handler: for<'a> fn(
        &'a OpContext,
        serde_json::Value,
    )
        -> futures::future::BoxFuture<'a, Result<serde_json::Value, OpError>>,
}

inventory::collect!(OperationEntry);

/// Helper to register an operation: implements the name methods the registry
/// reads and submits one `OperationEntry`.
#[macro_export]
macro_rules! register_operation {
    ($op:ident, $name:literal, $mcp:literal, $method:ident, $path:literal, $input:ty, $output:ty) => {
        impl $crate::OperationNames for $op {
            const NAME_OVERRIDE: &'static str = $name;
            const MCP_NAME_OVERRIDE: &'static str = $mcp;
            const HTTP_PATH_OVERRIDE: &'static str = $path;
            fn http_method_override() -> http::Method {
                http::Method::$method
            }
        }

        inventory::submit! {
            $crate::OperationEntry {
                name: <$op as $crate::OperationNames>::NAME_OVERRIDE,
                mcp_tool_name: <$op as $crate::OperationNames>::MCP_NAME_OVERRIDE,
                http_method: <$op as $crate::OperationNames>::http_method_override,
                http_path: <$op as $crate::OperationNames>::HTTP_PATH_OVERRIDE,
                input_schema: || schemars::schema_for!($input),
                output_schema: || schemars::schema_for!($output),
                handler: |ctx, v| Box::pin(async move {
                    let input: $input =
                        serde_json::from_value(v).map_err(|e| $crate::OpError::BadInput(e.to_string()))?;
                    let out = $op::default().handle(ctx, input).await?;
                    serde_json::to_value(out).map_err(|e| $crate::OpError::Other(e.to_string()))
                }),
            }
        }
    };
}

/// Name-override surface the `register_operation!` macro fills in.
pub trait OperationNames {
    /// Stable operation name.
    const NAME_OVERRIDE: &'static str;
    /// MCP tool name.
    const MCP_NAME_OVERRIDE: &'static str;
    /// HTTP path.
    const HTTP_PATH_OVERRIDE: &'static str;
    /// HTTP method constructor.
    fn http_method_override() -> http::Method;
}

/// Enumerate every registered operation (sorted by name; duplicates reject
/// in the parity check).
pub fn entries() -> Vec<&'static OperationEntry> {
    let mut all: Vec<&'static OperationEntry> =
        inventory::iter::<OperationEntry>.into_iter().collect();
    all.sort_by_key(|e| e.name);
    all
}
