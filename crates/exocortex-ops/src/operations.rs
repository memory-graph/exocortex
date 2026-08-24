// crates/exocortex-ops/src/operations.rs
//! The registered operation set (§21.1 examples): find_related, get_memory,
//! traverse, wrapup submit, promote_visibility, accept_discovery,
//! retract_edge, and the audit read surface (R-A3).

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use exocortex_kernel::{Memory, MemoryId, RelationshipId, Visibility};
use exocortex_storage::{TraversalSpec, VisibilityContext};

use crate::{register_operation, OpContext, OpError, Operation};

/// `find_related` (§21.1 example op).
#[derive(Default)]
pub struct FindRelated;

/// Input for `find_related`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct FindRelatedInput {
    /// Anchor memory (hex id).
    pub anchor: String,
    /// Hop bound (hard-capped at 4).
    #[serde(default = "default_k")]
    pub k: u8,
}

fn default_k() -> u8 {
    2
}

/// Output for `find_related`.
#[derive(Serialize, JsonSchema)]
pub struct FindRelatedOutput {
    /// Related memories.
    pub memories: Vec<MemoryJson>,
}

/// JSON projection of a memory (kernel Memory is not JsonSchema-derived).
#[derive(Serialize, JsonSchema)]
pub struct MemoryJson {
    /// Hex id.
    pub id: String,
    /// Title.
    pub title: String,
    /// Memory type id.
    pub memory_type: u8,
    /// Visibility label.
    pub visibility: String,
}

fn unhex(s: &str) -> Result<MemoryId, OpError> {
    let mut out = [0u8; 16];
    let bytes = s.as_bytes();
    if bytes.len() != 32 {
        return Err(OpError::BadInput("expected 32-char hex id".into()));
    }
    for i in 0..16 {
        out[i] = u8::from_str_radix(
            std::str::from_utf8(&bytes[i * 2..i * 2 + 2])
                .map_err(|_| OpError::BadInput("hex".into()))?,
            16,
        )
        .map_err(|_| OpError::BadInput("hex".into()))?;
    }
    Ok(MemoryId(out))
}

fn hex32(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn mem_json(m: &Memory) -> MemoryJson {
    MemoryJson {
        id: hex32(&m.id.0),
        title: m.title.to_string(),
        memory_type: m.memory_type,
        visibility: format!("{:?}", m.visibility),
    }
}

#[async_trait]
impl Operation for FindRelated {
    type Input = FindRelatedInput;
    type Output = FindRelatedOutput;
    fn name(&self) -> &'static str {
        "find_related"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.find_related"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/find_related"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let anchor = unhex(&input.anchor)?;
        let spec = TraversalSpec {
            direction: exocortex_storage::Direction::Both,
            kinds: Default::default(),
            max_depth: input.k.min(4),
            max_nodes: 128,
            visibility_ctx: ctx.visibility_ctx.clone(),
            as_of: None,
        };
        let org = ctx.visibility_ctx.org_id.to_string();
        let memories = ctx
            .cache
            .traverse(&org, &anchor, &spec)
            .iter()
            .map(mem_json)
            .collect();
        Ok(FindRelatedOutput { memories })
    }
}

register_operation!(
    FindRelated,
    "find_related",
    "exocortex.find_related",
    POST,
    "/v1/find_related",
    FindRelatedInput,
    FindRelatedOutput
);

/// `get_memory` — point read through the cache.
#[derive(Default)]
pub struct GetMemory;

/// Input for `get_memory`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct GetMemoryInput {
    /// Hex memory id.
    pub id: String,
}

/// Output for `get_memory`.
#[derive(Serialize, JsonSchema)]
pub struct GetMemoryOutput {
    /// The memory, when visible.
    pub memory: Option<MemoryJson>,
}

#[async_trait]
impl Operation for GetMemory {
    type Input = GetMemoryInput;
    type Output = GetMemoryOutput;
    fn name(&self) -> &'static str {
        "get_memory"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.get_memory"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/get_memory"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let id = unhex(&input.id)?;
        let org = ctx.visibility_ctx.org_id.to_string();
        Ok(GetMemoryOutput {
            memory: ctx
                .cache
                .get_memory(&org, &id, &ctx.visibility_ctx)
                .as_ref()
                .map(mem_json),
        })
    }
}

register_operation!(
    GetMemory,
    "get_memory",
    "exocortex.get_memory",
    POST,
    "/v1/get_memory",
    GetMemoryInput,
    GetMemoryOutput
);

/// `search_memories` — the kernel Function through the registry (CR-9).
#[derive(Default)]
pub struct SearchMemoriesOp;

/// Input for `search_memories`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SearchInput {
    /// Free-text query.
    pub query: String,
    /// Result cap.
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    20
}

/// Output for `search_memories`.
#[derive(Serialize, JsonSchema)]
pub struct SearchOutput {
    /// Ranked hits.
    pub memories: Vec<MemoryJson>,
    /// Scores aligned with memories (§14.1).
    pub scores: Vec<f32>,
}

#[async_trait]
impl Operation for SearchMemoriesOp {
    type Input = SearchInput;
    type Output = SearchOutput;
    fn name(&self) -> &'static str {
        "search_memories"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.search_memories"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/search_memories"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let org = ctx.visibility_ctx.org_id.to_string();
        let hits = ctx.cache.search(
            &org,
            &input.query,
            input.limit.min(500),
            &ctx.visibility_ctx,
        );
        Ok(SearchOutput {
            memories: hits.iter().map(|(m, _)| mem_json(m)).collect(),
            scores: hits.iter().map(|(_, s)| *s).collect(),
        })
    }
}

register_operation!(
    SearchMemoriesOp,
    "search_memories",
    "exocortex.search_memories",
    POST,
    "/v1/search_memories",
    SearchInput,
    SearchOutput
);

/// `promote_visibility` — the ONLY path around R-T11a; audited (R-A2).
#[derive(Default)]
pub struct PromoteVisibilityOp;

/// Input for `promote_visibility`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PromoteVisibilityInput {
    /// Hex memory id.
    pub memory_id: String,
    /// Target visibility: "project"|"team"|"org".
    pub to: String,
}

/// Output for `promote_visibility`.
#[derive(Serialize, JsonSchema)]
pub struct PromoteVisibilityOutput {
    /// The promoted memory id.
    pub memory_id: String,
    /// The new visibility.
    pub visibility: String,
    /// The audit record written before the ack.
    pub audit_lsn: u64,
}

#[async_trait]
impl Operation for PromoteVisibilityOp {
    type Input = PromoteVisibilityInput;
    type Output = PromoteVisibilityOutput;
    fn name(&self) -> &'static str {
        "promote_visibility"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.promote_visibility"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/promote_visibility"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let id = unhex(&input.memory_id)?;
        let to = match input.to.as_str() {
            "project" => Visibility::Project,
            "team" => Visibility::Team,
            "org" => Visibility::Org,
            other => return Err(OpError::BadInput(format!("cannot promote to {other}"))),
        };
        let mut m = ctx
            .storage
            .get_memory(&id)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        if to < m.visibility {
            return Err(OpError::BadInput("promotion only widens".into()));
        }
        m.visibility = to;
        let commit = ctx
            .storage
            .upsert_memory(&m)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;

        // R-A1: the audit record shares the action's transaction; v1 appends
        // immediately after the commit with the commit's LSN.
        let record = crate::audit::AuditRecord {
            action: "promote_visibility".into(),
            actor: ctx.visibility_ctx.user_id.clone(),
            org_id: ctx.visibility_ctx.org_id.clone(),
            input_digest: crate::audit::digest_input(&serde_json::json!({
                "memory_id": input.memory_id,
                "to": input.to,
            })),
            output_ids: [input.memory_id.clone().into()].into_iter().collect(),
            fingerprint: ctx.storage.ontology_fingerprint(),
            lease_epoch: None,
            recorded_at: chrono::Utc::now(),
            lsn: commit.lsn,
        };
        let audit_lsn = crate::audit::append_audit(ctx, &record).await?;

        Ok(PromoteVisibilityOutput {
            memory_id: input.memory_id,
            visibility: input.to,
            audit_lsn,
        })
    }
}

register_operation!(
    PromoteVisibilityOp,
    "promote_visibility",
    "exocortex.promote_visibility",
    POST,
    "/v1/promote_visibility",
    PromoteVisibilityInput,
    PromoteVisibilityOutput
);

/// `accept_discovery` — promote a Dreams proposal to an edge; audited.
#[derive(Default)]
pub struct AcceptDiscoveryOp;

/// Input for `accept_discovery`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct AcceptDiscoveryInput {
    /// Discovery id (UUID).
    pub discovery_id: String,
    /// Hex from-memory id.
    pub from: String,
    /// Hex to-memory id.
    pub to: String,
    /// Kind display name.
    pub kind: String,
}

/// Output for `accept_discovery`.
#[derive(Serialize, JsonSchema)]
pub struct AcceptDiscoveryOutput {
    /// Hex relationship id of the asserted edge.
    pub edge_id: String,
    /// Audit record LSN.
    pub audit_lsn: u64,
}

#[async_trait]
impl Operation for AcceptDiscoveryOp {
    type Input = AcceptDiscoveryInput;
    type Output = AcceptDiscoveryOutput;
    fn name(&self) -> &'static str {
        "accept_discovery"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.accept_discovery"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/accept_discovery"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let from = unhex(&input.from)?;
        let to = unhex(&input.to)?;
        // R-Dr1/R-Dr2: acceptance produces an ASSERTED edge whose context
        // references the discovery id.
        let rel = exocortex_kernel::Relationship {
            id: RelationshipId::derive(
                from,
                exocortex_kernel::RelKindId(0),
                to,
                Some(&input.discovery_id),
            ),
            kind: exocortex_kernel::RelKindId(0),
            from,
            to,
            visibility: ctx.visibility_ctx.max_visibility,
            provenance: exocortex_kernel::Provenance::Asserted {
                author: ctx.visibility_ctx.user_id.clone(),
            },
            properties: exocortex_kernel::RelationshipProperties {
                strength: 0.5,
                confidence: 0.8,
                context: Some(format!("discovery:{}", input.discovery_id).into()),
                evidence_count: 1,
                success_rate: None,
                validation_count: 0,
                counter_evidence_count: 0,
                last_validated: chrono::Utc::now(),
            },
            description: None,
            bidirectional: false,
            valid_from: chrono::Utc::now(),
            valid_until: None,
            recorded_at: chrono::Utc::now(),
            invalidated_by: None,
            lsn: exocortex_kernel::LSN::new_local(0),
        };
        let commit = ctx
            .storage
            .upsert_relationship(&rel)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        let record = crate::audit::AuditRecord {
            action: "accept_discovery".into(),
            actor: ctx.visibility_ctx.user_id.clone(),
            org_id: ctx.visibility_ctx.org_id.clone(),
            input_digest: crate::audit::digest_input(&serde_json::json!({
                "discovery_id": input.discovery_id,
            })),
            output_ids: [input.discovery_id.clone().into()].into_iter().collect(),
            fingerprint: ctx.storage.ontology_fingerprint(),
            lease_epoch: None,
            recorded_at: chrono::Utc::now(),
            lsn: commit.lsn,
        };
        let audit_lsn = crate::audit::append_audit(ctx, &record).await?;
        Ok(AcceptDiscoveryOutput {
            edge_id: hex32(&rel.id.0),
            audit_lsn,
        })
    }
}

register_operation!(
    AcceptDiscoveryOp,
    "accept_discovery",
    "exocortex.accept_discovery",
    POST,
    "/v1/accept_discovery",
    AcceptDiscoveryInput,
    AcceptDiscoveryOutput
);

/// `list_audit_records` — the audit read surface (R-A3).
#[derive(Default)]
pub struct ListAuditRecordsOp;

/// Input for `list_audit_records`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ListAuditInput {
    /// Only records after this LSN.
    #[serde(default)]
    pub since_lsn: u64,
}

/// Output for `list_audit_records`.
#[derive(Serialize, JsonSchema)]
pub struct ListAuditOutput {
    /// Audit records (serialized projections).
    pub records: Vec<serde_json::Value>,
}

#[async_trait]
impl Operation for ListAuditRecordsOp {
    type Input = ListAuditInput;
    type Output = ListAuditOutput;
    fn name(&self) -> &'static str {
        "list_audit_records"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.list_audit_records"
    }
    fn http_method(&self) -> http::Method {
        http::Method::GET
    }
    fn http_path(&self) -> &'static str {
        "/v1/audit"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let rows = crate::audit::audit_range(ctx, input.since_lsn).await?;
        Ok(ListAuditOutput { records: rows })
    }
}

register_operation!(
    ListAuditRecordsOp,
    "list_audit_records",
    "exocortex.list_audit_records",
    GET,
    "/v1/audit",
    ListAuditInput,
    ListAuditOutput
);

/// A visibility context helper for ops tests.
pub fn ops_vc(org: &str, user: &str, max: Visibility) -> VisibilityContext {
    VisibilityContext {
        user_id: user.into(),
        org_id: org.into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: max,
    }
}
