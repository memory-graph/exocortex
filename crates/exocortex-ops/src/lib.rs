//! The operation registry (§21): every capability implements `Operation`,
//! registers one `OperationEntry` via `inventory::submit!`, and both the MCP
//! tool catalogue and the OpenAPI surface enumerate the SAME registry
//! (R-P1/R-P2: no operation exists on only one surface).

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod audit;
pub use pack_verbs::{eval_pack_function, eval_pack_function_cached};
pub mod operations;
pub mod pack_verbs;
pub mod preflight;

/// Client-facing shared types (§2.5 routes the client through ops; these
/// re-exports let the client crate consume the visibility/delta vocabulary
/// without a direct edge into the storage adapter crate).
pub use exocortex_storage::{Direction, Invalidation, TraversalSpec, VisibilityContext};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

/// Operation context: the typed handles every operation runs against.
#[derive(Clone)]
pub struct OpContext {
    /// The caller's identity + visibility scope.
    pub visibility_ctx: exocortex_storage::VisibilityContext,
    /// Explicit administrator permission for the org-wide audit ledger.
    pub audit_admin: bool,
    /// Durable storage.
    pub storage: std::sync::Arc<dyn exocortex_storage::Storage>,
    /// The local cache (client + backend read path).
    pub cache: std::sync::Arc<exocortex_cache::LocalCache>,
    /// Deadline for this operation (R-R3 budget enforcement).
    pub deadline: chrono::DateTime<chrono::Utc>,
    /// The effective ontology (D2 preflight's rulebook). `None` on
    /// surfaces that never validate writes; the preflight operation
    /// fails loudly rather than guessing when it is unset.
    pub ontology: Option<std::sync::Arc<exocortex_kernel::Ontology>>,
    /// D21-b (adapter-contract PRD D2): the backend's ingest service, for
    /// `preflight_batch`'s dry run of the real Submit path. `None` on
    /// surfaces with no ingest path (standalone MCP); the operation fails
    /// loudly rather than approximating with a second validator.
    pub ingest_preflight: Option<std::sync::Arc<dyn IngestPreflight>>,
    /// D4 (§24 q5): the backend's embedding runtime, for
    /// `reindex_embeddings`' whole-graph re-embed. `None` on surfaces
    /// with no embedder; the operation fails loudly.
    pub embedding_reindex: Option<std::sync::Arc<dyn EmbeddingReindex>>,
}

/// D21-b (adapter-contract PRD D2): the handle `preflight_batch` uses to
/// run the ingest service's own admission + validation over a
/// representative sample. The implementation signs the batch server-side
/// as its registered producer (the caller is an authenticated principal,
/// not a producer) and commits nothing. Implemented by the backend node;
/// deliberately NOT in this crate — the trait object is wiring, the
/// verdict semantics live with Submit.
#[async_trait]
pub trait IngestPreflight: Send + Sync {
    /// Dry-run `batch`: stamp registration-derived fields, sign as the
    /// registered producer, and run the full Submit verdict path without
    /// committing. Returns the ack a real submission would produce with
    /// `assigned_lsn` 0.
    async fn preflight_signed(
        &self,
        principal: &VisibilityContext,
        batch: exocortex_wire::ingest::v1::IngestBatch,
    ) -> Result<exocortex_wire::ingest::v1::IngestAck, OpError>;
}

/// D4 (§24 q5): whole-graph re-embed under the configured model — the
/// explicit reindex operation a model swap needs (blue/green corpora:
/// swap the model, reindex, the graph is single-revision again).
/// Implemented by the backend node over its ingest server; `None` on
/// surfaces with no embedding runtime (standalone MCP), where the
/// operation fails loudly rather than approximating.
#[async_trait]
pub trait EmbeddingReindex: Send + Sync {
    /// Re-embed every stored memory under the current model, committing
    /// changed rows with their audit events. `actor` names the
    /// administrator for the audit ledger.
    async fn reindex_embeddings(&self, actor: &str) -> Result<ReindexStats, OpError>;
}

/// D4: the reindex outcome shared by the handle and the operation
/// surface. Ops cannot link the ingest crate (the dependency arrow runs
/// the other way through the server), so the counts cross as data.
#[derive(
    Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct ReindexStats {
    /// Rows examined.
    pub scanned: u64,
    /// Rows re-embedded and committed (stamp or vector changed).
    pub reembedded: u64,
    /// Rows already at the target model and vector.
    pub unchanged: u64,
    /// The model every row now carries.
    pub model_name: String,
    /// Its revision.
    pub model_version: String,
}

impl OpContext {
    /// IN11 (audit): a per-request context with a fresh R-R3 budget.
    /// The backend used to build ONE shared context at startup, so every
    /// request after the first 30s ran with an always-expired deadline —
    /// and nothing read it.
    pub fn per_request(
        visibility_ctx: exocortex_storage::VisibilityContext,
        storage: std::sync::Arc<dyn exocortex_storage::Storage>,
        cache: std::sync::Arc<exocortex_cache::LocalCache>,
        budget: chrono::Duration,
    ) -> Self {
        Self {
            visibility_ctx,
            audit_admin: false,
            storage,
            cache,
            deadline: chrono::Utc::now() + budget,
            ontology: None,
            ingest_preflight: None,
            embedding_reindex: None,
        }
    }

    /// Attach the authenticated principal's audit-administrator capability.
    pub fn with_audit_admin(mut self, audit_admin: bool) -> Self {
        self.audit_admin = audit_admin;
        self
    }

    /// Attach the effective ontology (preflight-capable surfaces).
    pub fn with_ontology(mut self, ontology: std::sync::Arc<exocortex_kernel::Ontology>) -> Self {
        self.ontology = Some(ontology);
        self
    }

    /// Attach the backend ingest service (D21-b `preflight_batch`).
    pub fn with_ingest_preflight(mut self, handle: std::sync::Arc<dyn IngestPreflight>) -> Self {
        self.ingest_preflight = Some(handle);
        self
    }

    /// Attach the backend embedding runtime (D4 `reindex_embeddings`).
    pub fn with_embedding_reindex(mut self, handle: std::sync::Arc<dyn EmbeddingReindex>) -> Self {
        self.embedding_reindex = Some(handle);
        self
    }

    /// R-R3: `DeadlineExceeded` once the budget is spent. Handlers call
    /// this before doing work; it is cheap (a DateTime compare).
    pub fn check_deadline(&self) -> Result<(), OpError> {
        if chrono::Utc::now() > self.deadline {
            Err(OpError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
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
/// at startup. Pack-registered verbs (PX2 §4.3) join the SAME registry
/// through `entries()` — one enumeration serves every surface (R-P1/R-P2).
pub struct OperationEntry {
    /// Stable operation name.
    pub name: &'static str,
    /// MCP tool name.
    pub mcp_tool_name: &'static str,
    /// Owning pack for pack-registered verbs; `None` for kernel ops.
    pub pack: Option<&'static str>,
    /// HTTP method constructor.
    pub http_method: fn() -> http::Method,
    /// HTTP path.
    pub http_path: &'static str,
    /// JSON Schema for the input.
    pub input_schema: fn() -> schemars::schema::RootSchema,
    /// JSON Schema for the output.
    pub output_schema: fn() -> schemars::schema::RootSchema,
    /// Type-erased handler: JSON in, JSON out. Receives the entry itself
    /// so shared handlers (the pack-verb dispatchers) can read their
    /// `(pack, verb)` identity without capturing state in a fn pointer.
    #[allow(clippy::type_complexity)]
    pub handler: for<'a> fn(
        &'static OperationEntry,
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
                pack: ::core::option::Option::None,
                http_method: <$op as $crate::OperationNames>::http_method_override,
                http_path: <$op as $crate::OperationNames>::HTTP_PATH_OVERRIDE,
                input_schema: || schemars::schema_for!($input),
                output_schema: || schemars::schema_for!($output),
                handler: |_entry, ctx, v| Box::pin(async move {
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
/// in the parity check). Pack-registered verbs (PX2) are materialized ONCE
/// into the same registry: the leaked per-verb entries are bounded by the
/// number of declared verbs, constructed a single time per process, and
/// reachable from every surface that walks `entries()` — MCP tooling,
/// OpenAPI goldens, and the parity suites pick them up for free.
pub fn entries() -> Vec<&'static OperationEntry> {
    static PACK_ENTRIES: std::sync::OnceLock<Vec<OperationEntry>> = std::sync::OnceLock::new();
    let pack_entries = PACK_ENTRIES.get_or_init(crate::pack_verbs::registry_entries);
    let mut all: Vec<&'static OperationEntry> =
        inventory::iter::<OperationEntry>.into_iter().collect();
    all.extend(pack_entries.iter());
    all.sort_by_key(|e| e.name);
    all
}
