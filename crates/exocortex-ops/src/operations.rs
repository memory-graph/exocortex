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
#[derive(Debug, Serialize, JsonSchema)]
pub struct MemoryJson {
    /// Hex id.
    pub id: String,
    /// Title.
    pub title: String,
    /// Memory type id.
    pub memory_type: u8,
    /// Visibility label.
    pub visibility: String,
    /// D10b (§4.10a): the hex id of this memory's SUCCESSOR, when a live
    /// `Replaces`/`Contradicts` edge points at it. Stale beliefs are
    /// marked where every reader already looks; absent means not
    /// superseded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
}

fn unhex(s: &str) -> Result<MemoryId, OpError> {
    MemoryId::parse_hex(s).ok_or(OpError::BadInput("expected 32-char hex id".into()))
}

fn hex32(bytes: &[u8; 16]) -> String {
    MemoryId(*bytes).to_hex()
}

fn mem_json(m: &Memory) -> MemoryJson {
    MemoryJson {
        id: hex32(&m.id.0),
        title: m.title.to_string(),
        memory_type: m.memory_type,
        visibility: format!("{:?}", m.visibility),
        superseded_by: None,
    }
}

/// D10b: resolve a memory's successor through the local cache. Kind ids
/// come from the context's ontology (both surfaces attach it); without
/// one, the annotation degrades to absent — never wrong.
fn superseded_by(ctx: &OpContext, org: &str, id: &MemoryId) -> Option<String> {
    let kinds = ctx.ontology.as_ref()?;
    let supersedes: Vec<exocortex_kernel::RelKindId> = ["Replaces", "Contradicts"]
        .iter()
        .filter_map(|n| kinds.kind_id(n))
        .collect();
    if supersedes.is_empty() {
        return None;
    }
    ctx.cache
        .superseded_by(org, id, &ctx.visibility_ctx, &supersedes)
        .map(|m| hex32(&m.id.0))
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
        // IN11 (audit): R-R3 budget is enforced, not declared.
        ctx.check_deadline()?;
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
#[derive(Debug, Serialize, JsonSchema)]
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
        // IN11 (audit): R-R3 budget is enforced, not declared.
        ctx.check_deadline()?;
        let id = unhex(&input.id)?;
        let org = ctx.visibility_ctx.org_id.to_string();
        // Cache first (zero-cost hit). On a miss, distinguish the three
        // cases through the caller-visibility storage read (R-MT4 /
        // CR-22): a row the caller may not see is `PermissionDenied`,
        // never a silent None — and a real hit also fills the cache-miss
        // case (R-C8).
        if let Some(m) = ctx.cache.get_memory(&org, &id, &ctx.visibility_ctx) {
            return Ok(GetMemoryOutput {
                memory: Some(MemoryJson {
                    superseded_by: superseded_by(ctx, &org, &id),
                    ..mem_json(&m)
                }),
            });
        }
        match ctx.storage.get_memory_for(&id, &ctx.visibility_ctx).await {
            Ok(Some(m)) => {
                ctx.cache.hydrate_memory(&org, m.clone());
                Ok(GetMemoryOutput {
                    memory: Some(MemoryJson {
                        superseded_by: superseded_by(ctx, &org, &id),
                        ..mem_json(&m)
                    }),
                })
            }
            Ok(None) => Ok(GetMemoryOutput { memory: None }),
            Err(exocortex_storage::StorageError::PermissionDenied) => Err(OpError::Unauthorized(
                "memory outside caller visibility".into(),
            )),
            Err(e) => Err(OpError::Storage(e.to_string())),
        }
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
        // IN11 (audit): R-R3 budget is enforced, not declared.
        ctx.check_deadline()?;
        let org = ctx.visibility_ctx.org_id.to_string();
        let mut hits = ctx.cache.search(
            &org,
            &input.query,
            input.limit.min(500),
            &ctx.visibility_ctx,
        );
        // D10b: mark superseded hits and rank them below their
        // successors — a stale belief never outranks its correction.
        let mut annotated: Vec<(MemoryJson, f32)> = Vec::with_capacity(hits.len());
        for (m, score) in hits.drain(..) {
            let sup = superseded_by(ctx, &org, &m.id);
            let rank = if sup.is_some() { score * 0.1 } else { score };
            annotated.push((
                MemoryJson {
                    superseded_by: sup,
                    ..mem_json(&m)
                },
                rank,
            ));
        }
        annotated.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let (memories, scores): (Vec<_>, Vec<_>) = annotated.into_iter().unzip();
        Ok(SearchOutput { memories, scores })
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
        // IN11 (audit): R-R3 budget is enforced, not declared.
        ctx.check_deadline()?;
        let id = unhex(&input.memory_id)?;
        let to = match input.to.as_str() {
            "project" => Visibility::Project,
            "team" => Visibility::Team,
            "org" => Visibility::Org,
            other => return Err(OpError::BadInput(format!("cannot promote to {other}"))),
        };
        // KP5 (audit): the R-T11a ceiling this op enforces comes from the
        // kernel's typed Action surface — the only place it is declared.
        use exocortex_kernel::actions::Action as _;
        let max = exocortex_kernel::actions::PromoteVisibility::REQUIRED_VISIBILITY_CEILING;
        if to > max {
            return Err(OpError::BadInput(format!(
                "promotion capped at {max:?} (R-T11a)"
            )));
        }
        if to > ctx.visibility_ctx.max_visibility {
            return Err(OpError::Unauthorized(
                "promotion exceeds caller visibility ceiling".into(),
            ));
        }
        // IN2 (audit): load through the CALLER-SCOPED read. The unscoped
        // `get_memory` reads at the historical Org ceiling, so a caller who
        // cannot even see the row could otherwise widen it to the whole org.
        // Matching GetMemory: an invisible row is Unauthorized, not absent.
        let mut m = match ctx.storage.get_memory_for(&id, &ctx.visibility_ctx).await {
            Ok(Some(m)) => m,
            Ok(None) => return Err(OpError::NotFound),
            Err(exocortex_storage::StorageError::PermissionDenied) => {
                return Err(OpError::Unauthorized(
                    "caller may not read this memory".into(),
                ))
            }
            Err(e) => return Err(OpError::Storage(e.to_string())),
        };
        if to < m.visibility {
            return Err(OpError::BadInput("promotion only widens".into()));
        }
        m.visibility = to;
        // R-A1: storage commits the mutation and audit record together.
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
        };
        let commit = ctx
            .storage
            .promote_memory_visibility_audited(&m, &record)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        let audit_lsn = commit.lsn;

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

/// `reindex_embeddings` — D4 (§24 q5): the explicit model-swap step.
/// Re-embeds every stored memory under the configured model, restamping
/// model name/version; admin-only, audited per chunk.
#[derive(Default)]
pub struct ReindexEmbeddingsOp;

/// Input for `reindex_embeddings` (no parameters — the whole graph is
/// the unit; MCR² forbids mixed revisions, so a partial reindex is not
/// a thing this operation offers).
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ReindexEmbeddingsInput {}

/// Output for `reindex_embeddings`.
#[derive(Serialize, JsonSchema)]
pub struct ReindexEmbeddingsOutput {
    /// Rows examined.
    pub scanned: u64,
    /// Rows re-embedded and committed.
    pub reembedded: u64,
    /// Rows already at the target model.
    pub unchanged: u64,
    /// The model every row now carries.
    pub model_name: String,
    /// Its revision.
    pub model_version: String,
}

#[async_trait]
impl Operation for ReindexEmbeddingsOp {
    type Input = ReindexEmbeddingsInput;
    type Output = ReindexEmbeddingsOutput;
    fn name(&self) -> &'static str {
        "reindex_embeddings"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.reindex_embeddings"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/reindex_embeddings"
    }
    async fn handle(&self, ctx: &OpContext, _input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        // Org-wide maintenance over every stored row: the audit
        // administrator capability, the same gate the audit ledger uses.
        if !ctx.audit_admin {
            return Err(OpError::Unauthorized(
                "reindex_embeddings requires the audit administrator capability".into(),
            ));
        }
        let handle = ctx.embedding_reindex.as_ref().ok_or_else(|| {
            OpError::Other("no embedding runtime on this surface (backend node only)".into())
        })?;
        let stats = handle
            .reindex_embeddings(&ctx.visibility_ctx.user_id)
            .await?;
        Ok(ReindexEmbeddingsOutput {
            scanned: stats.scanned,
            reembedded: stats.reembedded,
            unchanged: stats.unchanged,
            model_name: stats.model_name,
            model_version: stats.model_version,
        })
    }
}

register_operation!(
    ReindexEmbeddingsOp,
    "reindex_embeddings",
    "exocortex.reindex_embeddings",
    POST,
    "/v1/reindex_embeddings",
    ReindexEmbeddingsInput,
    ReindexEmbeddingsOutput
);

/// `list_discoveries` — present durable, unasserted Dreams candidates.
#[derive(Default)]
pub struct ListDiscoveriesOp;

/// Input for `list_discoveries`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ListDiscoveriesInput {
    /// Maximum rows to return (hard-capped at 100).
    #[serde(default = "default_discovery_limit")]
    pub limit: u32,
}

fn default_discovery_limit() -> u32 {
    20
}

/// Caller-visible discovery projection.
#[derive(Serialize, JsonSchema)]
pub struct DiscoveryJson {
    /// Opaque discovery id accepted by `issue_discovery`.
    pub discovery_id: String,
    /// Source memory id.
    pub from: String,
    /// Destination memory id.
    pub to: String,
    /// Finder taxonomy value.
    pub discovery_type: String,
    /// Quality stamped by the finder.
    pub quality: f32,
    /// Supporting relationship kinds.
    pub via_types: [u32; 2],
}

/// Output for `list_discoveries`.
#[derive(Serialize, JsonSchema)]
pub struct ListDiscoveriesOutput {
    /// Candidates whose endpoints are visible to the current caller.
    pub discoveries: Vec<DiscoveryJson>,
}

#[async_trait]
impl Operation for ListDiscoveriesOp {
    type Input = ListDiscoveriesInput;
    type Output = ListDiscoveriesOutput;
    fn name(&self) -> &'static str {
        "list_discoveries"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.list_discoveries"
    }
    fn http_method(&self) -> http::Method {
        http::Method::GET
    }
    fn http_path(&self) -> &'static str {
        "/v1/discoveries"
    }

    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let records = ctx
            .storage
            .list_discoveries(&ctx.visibility_ctx.org_id, input.limit.min(100))
            .await
            .map_err(|error| OpError::Storage(error.to_string()))?;
        let endpoint_ids: Vec<_> = records
            .iter()
            .flat_map(|record| [record.from, record.to])
            .collect();
        let endpoints: std::collections::HashSet<_> = ctx
            .storage
            .get_visible_memories(&endpoint_ids, &ctx.visibility_ctx)
            .await
            .map_err(|error| OpError::Storage(error.to_string()))?
            .into_iter()
            .map(|memory| memory.id)
            .collect();
        let mut discoveries = Vec::new();
        for record in records {
            if endpoints.contains(&record.from) && endpoints.contains(&record.to) {
                discoveries.push(DiscoveryJson {
                    discovery_id: record.discovery_id.to_string(),
                    from: hex32(&record.from.0),
                    to: hex32(&record.to.0),
                    discovery_type: record.discovery_type.to_string(),
                    quality: record.quality,
                    via_types: record.via_types,
                });
            }
        }
        Ok(ListDiscoveriesOutput { discoveries })
    }
}

register_operation!(
    ListDiscoveriesOp,
    "list_discoveries",
    "exocortex.list_discoveries",
    GET,
    "/v1/discoveries",
    ListDiscoveriesInput,
    ListDiscoveriesOutput
);

/// `issue_discovery` — bind one durable candidate to this exact caller.
#[derive(Default)]
pub struct IssueDiscoveryOp;

/// Input for `issue_discovery`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct IssueDiscoveryInput {
    /// Durable discovery id.
    pub discovery_id: String,
    /// Relationship kind the caller proposes to assert.
    pub kind: String,
}

/// Immutable proposal returned to the caller for acceptance.
#[derive(Serialize, JsonSchema)]
pub struct IssueDiscoveryOutput {
    /// Discovery id.
    pub discovery_id: String,
    /// Source endpoint.
    pub from: String,
    /// Destination endpoint.
    pub to: String,
    /// Validated kind.
    pub kind: String,
    /// Endpoint-derived visibility.
    pub visibility: String,
}

#[async_trait]
impl Operation for IssueDiscoveryOp {
    type Input = IssueDiscoveryInput;
    type Output = IssueDiscoveryOutput;
    fn name(&self) -> &'static str {
        "issue_discovery"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.issue_discovery"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/issue_discovery"
    }

    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let record = ctx
            .storage
            .get_discovery(&input.discovery_id)
            .await
            .map_err(|error| OpError::Storage(error.to_string()))?
            .ok_or(OpError::NotFound)?;
        if record.region.org != ctx.visibility_ctx.org_id {
            return Err(OpError::NotFound);
        }
        let from = ctx
            .storage
            .get_memory_for(&record.from, &ctx.visibility_ctx)
            .await
            .map_err(|error| match error {
                exocortex_storage::StorageError::PermissionDenied => {
                    OpError::Unauthorized("discovery endpoint is outside caller scope".into())
                }
                other => OpError::Storage(other.to_string()),
            })?
            .ok_or(OpError::NotFound)?;
        let to = ctx
            .storage
            .get_memory_for(&record.to, &ctx.visibility_ctx)
            .await
            .map_err(|error| match error {
                exocortex_storage::StorageError::PermissionDenied => {
                    OpError::Unauthorized("discovery endpoint is outside caller scope".into())
                }
                other => OpError::Storage(other.to_string()),
            })?
            .ok_or(OpError::NotFound)?;
        let ontology = effective_ontology();
        let kind = ontology
            .kind_id(&input.kind)
            .ok_or_else(|| OpError::BadInput(format!("unknown kind `{}`", input.kind)))?;
        if ontology
            .kinds_by_id
            .get(&kind)
            .is_some_and(|meta| meta.computed_only)
        {
            return Err(OpError::BadInput(
                "computed-only kinds cannot be asserted".into(),
            ));
        }
        exocortex_kernel::validator::validate_triple(
            ontology,
            from.memory_type,
            kind,
            to.memory_type,
        )
        .map_err(|error| OpError::BadInput(format!("R-T17: {error}")))?;
        let proposed_visibility =
            exocortex_kernel::visibility::relationship_visibility(from.visibility, to.visibility);
        if proposed_visibility > ctx.visibility_ctx.max_visibility {
            return Err(OpError::Unauthorized(
                "discovery visibility exceeds caller ceiling".into(),
            ));
        }
        let proposal = exocortex_storage::DiscoveryProposal {
            discovery_id: record.discovery_id.clone(),
            region: record.region,
            from: record.from,
            to: record.to,
            kind,
            proposed_visibility,
            caller_scope: ctx.visibility_ctx.clone(),
            // Stable issuance makes retries idempotent without weakening the
            // immutable caller/kind/scope binding.
            issued_at: record.discovered_at,
        };
        ctx.storage
            .create_discovery_proposal(&proposal)
            .await
            .map_err(|error| OpError::Storage(error.to_string()))?;
        Ok(IssueDiscoveryOutput {
            discovery_id: proposal.discovery_id.to_string(),
            from: hex32(&proposal.from.0),
            to: hex32(&proposal.to.0),
            kind: input.kind,
            visibility: format!("{:?}", proposed_visibility).to_ascii_lowercase(),
        })
    }
}

register_operation!(
    IssueDiscoveryOp,
    "issue_discovery",
    "exocortex.issue_discovery",
    POST,
    "/v1/issue_discovery",
    IssueDiscoveryInput,
    IssueDiscoveryOutput
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
        let proposal = ctx
            .storage
            .get_discovery_proposal(&input.discovery_id)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        if proposal.caller_scope != ctx.visibility_ctx {
            return Err(OpError::Unauthorized(
                "discovery proposal was issued to a different caller scope".into(),
            ));
        }
        // IN3 (audit): resolve the CALLER-SUPPLIED kind through the
        // effective ontology — the old code silently wrote RelKindId(0) for
        // every acceptance — and run the same R-T17 triple validation the
        // ingest path uses.
        let ontology = effective_ontology();
        let kind = ontology
            .kind_id(&input.kind)
            .ok_or_else(|| OpError::BadInput(format!("unknown kind `{}`", input.kind)))?;
        if proposal.from != from || proposal.to != to || proposal.kind != kind {
            return Err(OpError::BadInput(
                "acceptance does not match the issued discovery proposal".into(),
            ));
        }
        if input.kind == "SimilarTo" {
            return Err(OpError::BadInput(
                "SimilarTo is computed-only (R-T14); accept a discovery instead".into(),
            ));
        }
        let from_mem = ctx
            .storage
            .get_memory_for(&from, &ctx.visibility_ctx)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        let to_mem = ctx
            .storage
            .get_memory_for(&to, &ctx.visibility_ctx)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        exocortex_kernel::validator::validate_triple(
            ontology,
            from_mem.memory_type,
            kind,
            to_mem.memory_type,
        )
        .map_err(|e| OpError::BadInput(format!("R-T17: {e}")))?;
        // R-Dr1/R-Dr2: acceptance produces an ASSERTED edge whose context
        // references the discovery id.
        let rel = exocortex_kernel::Relationship {
            id: RelationshipId::derive(from, kind, to, Some(&input.discovery_id)),
            kind,
            from,
            to,
            visibility: proposal.proposed_visibility,
            provenance: exocortex_kernel::Provenance::Asserted {
                author: ctx.visibility_ctx.user_id.clone(),
                producer_kind: None,
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
        let record = crate::audit::AuditRecord {
            action: "accept_discovery".into(),
            actor: ctx.visibility_ctx.user_id.clone(),
            org_id: ctx.visibility_ctx.org_id.clone(),
            input_digest: crate::audit::digest_input(&serde_json::json!({
                "discovery_id": input.discovery_id,
            })),
            output_ids: [hex32(&rel.id.0).into()].into_iter().collect(),
            fingerprint: ctx.storage.ontology_fingerprint(),
            lease_epoch: None,
            recorded_at: chrono::Utc::now(),
        };
        let commit = ctx
            .storage
            .accept_discovery(&exocortex_storage::DiscoveryAcceptance {
                discovery_id: proposal.discovery_id,
                region: proposal.region,
                caller_scope: ctx.visibility_ctx.clone(),
                relationship: rel.clone(),
                audit: record,
            })
            .await
            .map_err(|e| match e {
                exocortex_storage::StorageError::ProposalNotFound => OpError::NotFound,
                exocortex_storage::StorageError::ProposalMismatch => {
                    OpError::Unauthorized("proposal scope no longer matches".into())
                }
                other => OpError::Storage(other.to_string()),
            })?;
        let audit_lsn = commit.lsn;
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
        if !ctx.audit_admin {
            return Err(OpError::Unauthorized(
                "audit ledger requires explicit administrator permission".into(),
            ));
        }
        let org = ctx.visibility_ctx.org_id.to_string();
        let rows = crate::audit::audit_range(ctx, &org, input.since_lsn).await?;
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

/// The effective ontology for op-side kind resolution (IN3). Loaded once;
/// the registry server assembles the same pack set at boot.
fn effective_ontology() -> &'static std::sync::Arc<exocortex_kernel::Ontology> {
    static ONTO: std::sync::OnceLock<std::sync::Arc<exocortex_kernel::Ontology>> =
        std::sync::OnceLock::new();
    ONTO.get_or_init(|| {
        std::sync::Arc::new(
            exocortex_kernel::pack::load_registered_packs().expect("registered packs assemble"),
        )
    })
}

/// D2 (agent-instructions PRD §3.2): `preflight_wrapup` — validate a
/// proposed batch without writing. Registered HERE (not in the client)
/// so every surface — the client's MCP dispatch, the backend's HTTP
/// bind, the schema goldens — enumerates the ONE handler running the
/// ONE kernel rulebook (W2).
#[derive(Default)]
pub struct PreflightWrapupOp;

/// Input for `preflight_wrapup`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightWrapupInput {
    /// Project id (future context stamping; unused by validation).
    #[serde(default)]
    pub project_id: String,
    /// Proposed memory drafts.
    pub memories: Vec<crate::preflight::PreflightMemoryDraft>,
    /// Proposed edges.
    #[serde(default)]
    pub edges: Vec<crate::preflight::PreflightEdgeHint>,
}

#[async_trait]
impl Operation for PreflightWrapupOp {
    type Input = PreflightWrapupInput;
    type Output = crate::preflight::PreflightResult;
    fn name(&self) -> &'static str {
        "preflight_wrapup"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.preflight_wrapup"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/preflight_wrapup"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let ontology = ctx.ontology.clone().ok_or_else(|| {
            OpError::Other(
                "preflight requires the effective ontology (surface misconfiguration)".into(),
            )
        })?;
        let cache = ctx.cache.clone();
        let org = ctx.visibility_ctx.org_id.to_string();
        let vc = ctx.visibility_ctx.clone();
        Ok(crate::preflight::validate_batch(
            &ontology,
            &input.memories,
            &input.edges,
            |id| {
                MemoryId::parse_hex(id)
                    .and_then(|id| cache.get_memory(&org, &id, &vc).map(|m| m.memory_type))
            },
        ))
    }
}

register_operation!(
    PreflightWrapupOp,
    "preflight_wrapup",
    "exocortex.preflight_wrapup",
    POST,
    "/v1/preflight_wrapup",
    PreflightWrapupInput,
    crate::preflight::PreflightResult
);

/// D21-b (adapter-contract PRD D2): `preflight_batch` — dry-run the
/// ingest Submit path over a representative sample under a REAL
/// registration, committing nothing. The handler delegates to the
/// backend's ingest service through [`crate::IngestPreflight`], so the
/// verdicts are the ones Submit itself produces — one implementation,
/// one rejection vocabulary (the same RejectCode + correction table
/// `preflight_wrapup` and `preflight_action` report).
#[derive(Default)]
pub struct PreflightBatchOp;

/// One sample memory row (wire `MemoryDraft` shape, JSON-friendly).
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightMemoryRow {
    /// Producer-local id; relationships link via draft keys.
    pub draft_key: String,
    /// MUST resolve to a registered MemoryType.
    pub memory_type: String,
    /// 1..=200 chars (R-T5).
    pub title: String,
    /// Free-text content.
    pub content: String,
    /// Lowercase tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// "private"|"project"|"team"|"org" — the registration's ceiling caps this.
    pub visibility: String,
    /// RFC3339; absent = recorded_at (R-T7).
    #[serde(default)]
    pub valid_from: Option<String>,
    /// RFC3339; absent = open-ended.
    #[serde(default)]
    pub valid_until: Option<String>,
    /// Required when `snapshot` is present (R-T16a).
    #[serde(default)]
    pub external_key: Option<PreflightExternalKeyRow>,
}

/// One sample edge (wire `RelationshipDraft` shape).
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightRelationshipRow {
    /// Source draft key.
    pub from_draft_key: String,
    /// Target draft key within this sample — or empty when `to_memory_id` is set.
    #[serde(default)]
    pub to_draft_key: String,
    /// An EXISTING memory by 32-hex id (cross-batch edge).
    #[serde(default)]
    pub to_memory_id: String,
    /// MUST resolve to a registered kind.
    pub kind: String,
    /// 0.0..1.0; 0 = RelMeta default.
    #[serde(default)]
    pub strength: f32,
    /// 0.0..1.0; 0 = default.
    #[serde(default)]
    pub confidence: f32,
    /// Free-text edge context.
    #[serde(default)]
    pub context: String,
    /// "private"|"project"|"team"|"org"; empty = the registered ceiling.
    #[serde(default)]
    pub visibility: String,
}

/// The external snapshot the sample was observed at (R-T16a).
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightSnapshotRow {
    /// Source snapshot id (rewind detection names it).
    pub snapshot_id: String,
    /// 64-hex (32 bytes) — must match the registered mapping's schema hash.
    pub schema_hash: String,
    /// "iceberg" | "delta" | "parquet-dir" | "custom".
    pub source_flavor: String,
}

/// External identity coordinates (R-T18a): 32-hex table uuid + logical pk.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightExternalKeyRow {
    /// 32-hex (16 bytes).
    pub table_uuid: String,
    /// The row's logical primary key, in the source's own terms.
    pub logical_pk: String,
    /// The mapping version the coordinates were minted under.
    #[serde(default)]
    pub mapping_version: u32,
}

/// Input for `preflight_batch`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightBatchInput {
    /// The REAL registration the dry run runs under (org-scoped; the
    /// authenticated principal must belong to it).
    pub org_id: String,
    /// Registered source URI.
    pub source_uri: String,
    /// Registered producer id.
    pub producer_id: String,
    /// Producer node id for the dry-run identity (cosmetic; nothing commits).
    #[serde(default)]
    pub node_id: String,
    /// Requested project scope for Project-visibility samples — the same
    /// `ClientMetadata` contract a real submission carries; the server
    /// verifies the principal's membership.
    #[serde(default)]
    pub project_id: String,
    /// Requested team scope for Team-visibility samples (same contract).
    #[serde(default)]
    pub team_id: String,
    /// The representative sample.
    pub memories: Vec<PreflightMemoryRow>,
    /// The sample's edges.
    #[serde(default)]
    pub relationships: Vec<PreflightRelationshipRow>,
    /// Present iff the producer is external-source.
    #[serde(default)]
    pub snapshot: Option<PreflightSnapshotRow>,
}

/// One verdict row — the wire `RejectRow` plus its deterministic correction.
#[derive(Serialize, JsonSchema)]
pub struct PreflightRejectRow {
    /// Producer-local key of the offending row.
    pub draft_key: String,
    /// `RejectCode` name (the shared vocabulary).
    pub code: String,
    /// What exactly failed.
    pub detail: String,
    /// Deterministic remediation (the same guidance table Submit reports).
    pub correction: String,
}

/// The dry-run verdict for `preflight_batch`.
#[derive(Serialize, JsonSchema)]
pub struct PreflightBatchOutput {
    /// The dry-run batch id (deterministic over the sample).
    pub batch_id: String,
    /// Rows a real submission would commit.
    pub would_accept: u32,
    /// Rows a real submission would reject.
    pub would_reject: u32,
    /// The rejection rows with corrections.
    pub rejections: Vec<PreflightRejectRow>,
    /// Always false: preflight assigns no LSN, writes no audit row,
    /// moves no cursor.
    pub committed: bool,
}

fn visibility_label_to_wire(label: &str) -> Result<i32, OpError> {
    match label.to_lowercase().as_str() {
        "" | "org" => Ok(3),
        "private" => Ok(0),
        "project" => Ok(1),
        "team" => Ok(2),
        other => Err(OpError::BadInput(format!(
            "unknown visibility `{other}` (expected private|project|team|org)"
        ))),
    }
}

fn parse_ts(s: &Option<String>) -> Result<Option<prost_types::Timestamp>, OpError> {
    s.as_deref()
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            chrono::DateTime::parse_from_rfc3339(raw)
                .map(|dt| prost_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                })
                .map_err(|e| OpError::BadInput(format!("invalid RFC3339 timestamp `{raw}`: {e}")))
        })
        .transpose()
}

fn parse_hex_bytes(s: &str, want: usize, what: &str) -> Result<Vec<u8>, OpError> {
    let bytes = s.as_bytes();
    if bytes.len() != want * 2 {
        return Err(OpError::BadInput(format!(
            "{what} must be {want} bytes ({} hex chars), got {} chars",
            want * 2,
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(want);
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16);
        let lo = (bytes[i + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => out.push((hi * 16 + lo) as u8),
            _ => {
                return Err(OpError::BadInput(format!(
                    "{what} is not valid hex near byte {}",
                    i / 2
                )))
            }
        }
        i += 2;
    }
    Ok(out)
}

#[async_trait]
impl Operation for PreflightBatchOp {
    type Input = PreflightBatchInput;
    type Output = PreflightBatchOutput;
    fn name(&self) -> &'static str {
        "preflight_batch"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.preflight_batch"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/preflight_batch"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let handle = ctx.ingest_preflight.clone().ok_or_else(|| {
            OpError::Unauthorized(
                "preflight_batch requires the backend ingest surface — standalone mode has no Submit path to dry-run"
                    .into(),
            )
        })?;
        if ctx.visibility_ctx.org_id.as_str() != input.org_id {
            return Err(OpError::Unauthorized(
                "authenticated principal cannot preflight another org".into(),
            ));
        }
        let now = chrono::Utc::now();
        let now_ts = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };
        let memories = input
            .memories
            .iter()
            .map(|m| {
                let external_key = m
                    .external_key
                    .as_ref()
                    .map(|k| {
                        Ok::<_, OpError>(exocortex_wire::ingest::v1::ExternalKey {
                            table_uuid: parse_hex_bytes(
                                &k.table_uuid,
                                16,
                                "external_key.table_uuid",
                            )?,
                            logical_pk: k.logical_pk.clone(),
                            mapping_version: k.mapping_version,
                        })
                    })
                    .transpose()?;
                Ok(exocortex_wire::ingest::v1::MemoryDraft {
                    rights: None,
                    draft_key: m.draft_key.clone(),
                    id: String::new(),
                    memory_type: m.memory_type.clone(),
                    title: m.title.clone(),
                    content: m.content.clone(),
                    tags: m.tags.clone(),
                    visibility: visibility_label_to_wire(&m.visibility)?,
                    valid_from: parse_ts(&m.valid_from)?,
                    valid_until: parse_ts(&m.valid_until)?,
                    external_key,
                })
            })
            .collect::<Result<Vec<_>, OpError>>()?;
        let relationships = input
            .relationships
            .iter()
            .map(|r| {
                Ok(exocortex_wire::ingest::v1::RelationshipDraft {
                    from_draft_key: r.from_draft_key.clone(),
                    to_draft_key: r.to_draft_key.clone(),
                    kind: r.kind.clone(),
                    strength: r.strength,
                    confidence: r.confidence,
                    context: r.context.clone(),
                    visibility: visibility_label_to_wire(&r.visibility)?,
                    to_memory_id: r.to_memory_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, OpError>>()?;
        let snapshot = input
            .snapshot
            .as_ref()
            .map(|s| {
                Ok(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
                    snapshot_id: s.snapshot_id.clone(),
                    schema_hash: parse_hex_bytes(&s.schema_hash, 32, "snapshot.schema_hash")?,
                    source_flavor: s.source_flavor.clone(),
                })
            })
            .transpose()?;
        let batch_id = {
            // Deterministic dry-run id: nothing commits, but equal samples
            // name themselves identically in the verdict output.
            let mut hasher = blake3::Hasher::new();
            hasher.update(input.org_id.as_bytes());
            hasher.update(input.source_uri.as_bytes());
            hasher.update(input.producer_id.as_bytes());
            hasher.update(&serde_json::to_vec(&input).map_err(|e| OpError::Other(e.to_string()))?);
            let digest = hasher.finalize();
            let bytes = digest.as_bytes();
            let mut hex = String::with_capacity(8);
            for byte in &bytes[..4] {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
            }
            format!("preflight-{hex}")
        };
        let batch = exocortex_wire::ingest::v1::IngestBatch {
            org_id: input.org_id.clone(),
            source_uri: input.source_uri.clone(),
            producer_id: input.producer_id.clone(),
            batch_id,
            mapping_version: "preflight".into(),
            ontology_fingerprint: vec![],
            ceiling: 0,
            checksum: String::new(),
            observed_at: Some(now_ts),
            recorded_at: Some(now_ts),
            snapshot,
            memories,
            relationships,
            producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
                node_id: if input.node_id.is_empty() {
                    "preflight".into()
                } else {
                    input.node_id.clone()
                },
                agent_id: String::new(),
                adapter_id: input.producer_id.clone(),
                hmac_signature: vec![],
                client_metadata: Some(exocortex_wire::ingest::v1::ClientMetadata {
                    playbook_version: "preflight".into(),
                    client_version: "preflight".into(),
                    harness_hint: String::new(),
                    project_id: input.project_id.clone(),
                    team_id: input.team_id.clone(),
                }),
            }),
        };
        let ack = handle.preflight_signed(&ctx.visibility_ctx, batch).await?;
        let rejections = ack
            .rejections
            .iter()
            .map(|row| {
                let code = exocortex_wire::ingest::v1::RejectCode::try_from(row.code)
                    .unwrap_or(exocortex_wire::ingest::v1::RejectCode::Unknown);
                PreflightRejectRow {
                    draft_key: row.draft_key.clone(),
                    code: format!("{code:?}"),
                    detail: row.detail.clone(),
                    correction: exocortex_wire::corrections::guidance(code)
                        .correction
                        .into(),
                }
            })
            .collect();
        Ok(PreflightBatchOutput {
            batch_id: ack.batch_id,
            would_accept: ack.accepted,
            would_reject: ack.rejected,
            rejections,
            committed: false,
        })
    }
}

register_operation!(
    PreflightBatchOp,
    "preflight_batch",
    "exocortex.preflight_batch",
    POST,
    "/v1/preflight_batch",
    PreflightBatchInput,
    PreflightBatchOutput
);

// ---- PX6: the three kernel-catalogue entries that had no registered
// operation (GetChain, ExplainEdge, RetractEdge). Every kernel
// Action/Function now has exactly one operation-side implementation;
// `kernel_catalogue_is_registered` below pins the bijection.

/// `retract_edge` (§7.11 `RetractEdge`): close `valid_until` on an edge
/// with a reason. The only human path to close an asserted edge; audited
/// atomically with the mutation (R6-B18 pattern).
#[derive(Default)]
pub struct RetractEdgeOp;

/// Input for `retract_edge`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct RetractEdgeInput {
    /// Hex relationship id.
    pub edge_id: String,
    /// Human-readable reason, kept in the audit log.
    pub reason: String,
}

/// Output for `retract_edge`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RetractEdgeOutput {
    /// The closed relationship id.
    pub edge_id: String,
    /// The audit record written in the same commit.
    pub audit_lsn: u64,
}

fn rel_unhex(s: &str) -> Result<RelationshipId, OpError> {
    let bytes: [u8; 16] = {
        let mut out = [0u8; 16];
        if s.len() != 32 {
            return Err(OpError::BadInput("expected 32-char hex id".into()));
        }
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| OpError::BadInput("expected 32-char hex id".into()))?;
        }
        out
    };
    Ok(RelationshipId(bytes))
}

#[async_trait]
impl Operation for RetractEdgeOp {
    type Input = RetractEdgeInput;
    type Output = RetractEdgeOutput;
    fn name(&self) -> &'static str {
        "retract_edge"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.retract_edge"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/retract_edge"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        if input.reason.trim().is_empty() {
            return Err(OpError::BadInput("retraction requires a reason".into()));
        }
        let id = rel_unhex(&input.edge_id)?;
        // Caller must be able to see BOTH endpoints before closing the
        // edge between them (IN2 pattern: scoped reads, never blind
        // mutation of invisible rows).
        let edge = ctx
            .storage
            .get_relationship(&id)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        let endpoints = ctx
            .storage
            .get_visible_memories(&[edge.from, edge.to], &ctx.visibility_ctx)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        if endpoints.len() != 2 {
            return Err(OpError::Unauthorized(
                "caller may not see both endpoints of this edge".into(),
            ));
        }
        // KP5 pattern: the ceiling comes from the kernel's typed Action.
        use exocortex_kernel::actions::Action as _;
        let max = exocortex_kernel::actions::RetractEdge::REQUIRED_VISIBILITY_CEILING;
        if ctx.visibility_ctx.max_visibility > max {
            return Err(OpError::Unauthorized(
                "retraction exceeds the RetractEdge ceiling".into(),
            ));
        }
        let record = crate::audit::AuditRecord {
            action: "retract_edge".into(),
            actor: ctx.visibility_ctx.user_id.clone(),
            org_id: ctx.visibility_ctx.org_id.clone(),
            input_digest: crate::audit::digest_input(&serde_json::json!({
                "edge_id": input.edge_id,
                "reason": input.reason,
            })),
            output_ids: [input.edge_id.clone().into()].into_iter().collect(),
            fingerprint: ctx.storage.ontology_fingerprint(),
            lease_epoch: None,
            recorded_at: chrono::Utc::now(),
        };
        let commit = ctx
            .storage
            .delete_relationship_audited(&id, &record)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        Ok(RetractEdgeOutput {
            edge_id: input.edge_id,
            audit_lsn: commit.lsn,
        })
    }
}

register_operation!(
    RetractEdgeOp,
    "retract_edge",
    "exocortex.retract_edge",
    POST,
    "/v1/retract_edge",
    RetractEdgeInput,
    RetractEdgeOutput
);

/// D7 (§23 #13, round-2 H14): `resolve_contradiction` — record a human
/// resolution over a `Contradicts` edge. The winner stands; the loser is
/// superseded to derived-floor confidence (the same stale-belief
/// semantics the commit path applies to `Replaces`/`Contradicts`
/// targets); the contradiction edge itself closes so it stops firing the
/// detector; and the decision — resolution, note, actor — lands in the
/// audit ledger in the SAME atomic commit (R6-B18 discipline). Every
/// resolution is reversible history, never a destructive edit.
#[derive(Default)]
pub struct ResolveContradictionOp;

/// Input for `resolve_contradiction`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ResolveContradictionInput {
    /// Hex id of the open `Contradicts` edge being resolved.
    pub edge_id: String,
    /// "from" keeps the edge's from-memory and supersedes the to-memory;
    /// "to" the mirror; "neither" supersedes nothing (both stand, the
    /// contradiction is acknowledged and closed).
    pub resolution: String,
    /// The human rationale, kept verbatim in the audit ledger.
    pub note: String,
}

/// Output for `resolve_contradiction`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ResolveContradictionOutput {
    /// The closed contradiction edge.
    pub edge_id: String,
    /// What was recorded.
    pub resolution: String,
    /// The superseded memory (hex), when the resolution named a winner.
    pub superseded: Option<String>,
    /// The audit record written in the same commit.
    pub audit_lsn: u64,
}

#[async_trait]
impl Operation for ResolveContradictionOp {
    type Input = ResolveContradictionInput;
    type Output = ResolveContradictionOutput;
    fn name(&self) -> &'static str {
        "resolve_contradiction"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.resolve_contradiction"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/resolve_contradiction"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        if input.note.trim().is_empty() {
            return Err(OpError::BadInput(
                "contradiction resolution requires a note".into(),
            ));
        }
        let resolution = match input.resolution.as_str() {
            "from" | "to" | "neither" => input.resolution.as_str(),
            other => {
                return Err(OpError::BadInput(format!(
                    "unknown resolution `{other}` (expected from|to|neither)"
                )))
            }
        };
        let ontology = ctx.ontology.clone().ok_or_else(|| {
            OpError::Other(
                "resolve_contradiction requires the effective ontology (surface misconfiguration)"
                    .into(),
            )
        })?;
        let id = rel_unhex(&input.edge_id)?;
        let edge = ctx
            .storage
            .get_relationship(&id)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        if edge.valid_until.is_some() {
            return Err(OpError::BadInput(
                "contradiction edge is already resolved".into(),
            ));
        }
        let kind_name = ontology
            .kinds_by_id
            .get(&edge.kind)
            .map(|kind| kind.display_name.as_str())
            .unwrap_or("");
        if kind_name != "Contradicts" {
            return Err(OpError::BadInput(format!(
                "edge is `{kind_name}`, not Contradicts — resolution is a contradiction-record operation"
            )));
        }
        // IN2 pattern: the caller must see BOTH endpoints before deciding
        // between them.
        let endpoints = ctx
            .storage
            .get_visible_memories(&[edge.from, edge.to], &ctx.visibility_ctx)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        if endpoints.len() != 2 {
            return Err(OpError::Unauthorized(
                "caller may not see both endpoints of this contradiction".into(),
            ));
        }
        let now = chrono::Utc::now();
        // The loser is superseded to the stale-belief floor — the exact
        // treatment materialize_commit_rows gives Replaces/Contradicts
        // targets — never deleted.
        let confidence_floor = exocortex_kernel::memory::derived_confidence(true, 0, 0);
        let mut superseded = None;
        let mut memories = Vec::new();
        if resolution != "neither" {
            let winner = if resolution == "from" {
                edge.from
            } else {
                edge.to
            };
            let loser = if resolution == "from" {
                edge.to
            } else {
                edge.from
            };
            let loser_row = endpoints
                .iter()
                .find(|memory| memory.id == loser)
                .cloned()
                .ok_or(OpError::NotFound)?;
            if loser_row.confidence.partial_cmp_score(&confidence_floor)
                == std::cmp::Ordering::Greater
            {
                let mut stale = loser_row;
                stale.confidence = confidence_floor;
                stale.recorded_at = now;
                memories.push(stale);
            }
            superseded = Some(loser.to_hex());
            let _ = winner;
        }
        // Close the contradiction edge: it is resolved and must stop
        // firing the detector. The row is a new version, not an edit.
        let mut closed = edge;
        closed.valid_until = Some(now);
        closed.recorded_at = now;
        let record = crate::audit::AuditRecord {
            action: "resolve_contradiction".into(),
            actor: ctx.visibility_ctx.user_id.clone(),
            org_id: ctx.visibility_ctx.org_id.clone(),
            input_digest: crate::audit::digest_input(&serde_json::json!({
                "edge_id": input.edge_id,
                "resolution": resolution,
                "note": input.note,
            })),
            output_ids: [input.edge_id.clone().into()]
                .into_iter()
                .chain(superseded.clone().map(smol_str::SmolStr::from))
                .collect(),
            fingerprint: ctx.storage.ontology_fingerprint(),
            lease_epoch: None,
            recorded_at: now,
        };
        let commits = ctx
            .storage
            .upsert_batch_audited(&memories, &[closed], &record)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        let audit_lsn = commits.last().map(|commit| commit.lsn).unwrap_or_default();
        Ok(ResolveContradictionOutput {
            edge_id: input.edge_id,
            resolution: resolution.into(),
            superseded,
            audit_lsn,
        })
    }
}

register_operation!(
    ResolveContradictionOp,
    "resolve_contradiction",
    "exocortex.resolve_contradiction",
    POST,
    "/v1/resolve_contradiction",
    ResolveContradictionInput,
    ResolveContradictionOutput
);

/// `get_chain` (§7.12 `GetChain`): the provenance chain for a memory —
/// the memories an assertion transitively rests on, walked backwards
/// through Derived evidence, caller-visible endpoints only.
#[derive(Default)]
pub struct GetChainOp;

/// Input for `get_chain`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct GetChainInput {
    /// Hex memory id.
    pub memory: String,
    /// Depth bound (hard-capped at 4).
    #[serde(default = "default_chain_depth")]
    pub max_depth: u8,
}

fn default_chain_depth() -> u8 {
    2
}

/// Output for `get_chain`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct GetChainOutput {
    /// Chain of hex memory ids, origin first, requested memory last.
    pub chain: Vec<String>,
}

#[async_trait]
impl Operation for GetChainOp {
    type Input = GetChainInput;
    type Output = GetChainOutput;
    fn name(&self) -> &'static str {
        "get_chain"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.get_chain"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/get_chain"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let target = unhex(&input.memory)?;
        let depth = input.max_depth.min(4);
        // Bounded backward walk over Derived evidence: the memory's own
        // provenance names supporting EDGES; each edge's from-endpoint
        // is the prior belief. One batched read per hop (R6-R191
        // discipline: no per-row point reads on the walk).
        // Levels of the backward walk: level 0 is the target, level i
        // the beliefs level i-1 rests on. Reversed and flattened, the
        // chain reads origin-first, target-last.
        let mut levels: Vec<Vec<String>> = vec![vec![target.to_hex()]];
        let mut frontier = vec![target];
        let mut seen = std::collections::HashSet::from([target]);
        for _ in 0..depth {
            let rows = ctx
                .storage
                .get_visible_memories(&frontier, &ctx.visibility_ctx)
                .await
                .map_err(|e| OpError::Storage(e.to_string()))?;
            let mut evidence: Vec<RelationshipId> = Vec::new();
            for memory in &rows {
                if let exocortex_kernel::Provenance::Derived { evidence: ids, .. } =
                    &memory.provenance
                {
                    evidence.extend(ids.iter().copied());
                }
            }
            if evidence.is_empty() {
                break;
            }
            let edges = ctx
                .storage
                .get_relationships(&evidence)
                .await
                .map_err(|e| OpError::Storage(e.to_string()))?;
            let mut next: Vec<MemoryId> = Vec::new();
            for edge in &edges {
                if edge.from != target && seen.insert(edge.from) {
                    next.push(edge.from);
                }
                if edge.to != target && seen.insert(edge.to) {
                    next.push(edge.to);
                }
            }
            if next.is_empty() {
                break;
            }
            levels.push(next.iter().map(|id| id.to_hex()).collect());
            frontier = next;
        }
        let chain = levels.into_iter().rev().flatten().collect();
        Ok(GetChainOutput { chain })
    }
}

register_operation!(
    GetChainOp,
    "get_chain",
    "exocortex.get_chain",
    POST,
    "/v1/get_chain",
    GetChainInput,
    GetChainOutput
);

/// `explain_edge` (§7.12 `ExplainEdge`): a Steel-rendered explanation
/// tree for a Derived edge, naming every input fact.
#[derive(Default)]
pub struct ExplainEdgeOp;

/// Input for `explain_edge`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ExplainEdgeInput {
    /// Hex relationship id.
    pub edge: String,
}

/// Output for `explain_edge`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ExplainEdgeOutput {
    /// Structured explanation tree (sexp string rendered by Steel).
    pub tree: String,
}

#[async_trait]
impl Operation for ExplainEdgeOp {
    type Input = ExplainEdgeInput;
    type Output = ExplainEdgeOutput;
    fn name(&self) -> &'static str {
        "explain_edge"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.explain_edge"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/explain_edge"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let target_id = rel_unhex(&input.edge)?;
        let target = ctx
            .storage
            .get_relationship(&target_id)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?
            .ok_or(OpError::NotFound)?;
        // Both endpoints must be visible: an explanation is a read of
        // the subgraph it names.
        let endpoints = ctx
            .storage
            .get_visible_memories(&[target.from, target.to], &ctx.visibility_ctx)
            .await
            .map_err(|e| OpError::Storage(e.to_string()))?;
        if endpoints.len() != 2 {
            return Err(OpError::Unauthorized(
                "caller may not see both endpoints of this edge".into(),
            ));
        }
        // Walk the derivation DAG backwards, bounded; every visited
        // edge becomes one ExplainEngine fact.
        let kind_name = |id: exocortex_kernel::RelKindId| -> String {
            ctx.ontology
                .as_ref()
                .and_then(|o| o.kinds_by_id.get(&id))
                .map(|k| k.display_name.to_string())
                .unwrap_or_else(|| format!("{:#x}", id.0))
        };
        let mut facts: Vec<exocortex_reasoning::EdgeFacts> = Vec::new();
        let mut frontier = vec![target_id];
        let mut seen = std::collections::HashSet::from([target_id]);
        let mut budget = 64usize;
        while let Some(id) = frontier.pop() {
            if budget == 0 {
                break;
            }
            budget -= 1;
            let edge = if id == target_id {
                target.clone()
            } else {
                match ctx
                    .storage
                    .get_relationship(&id)
                    .await
                    .map_err(|e| OpError::Storage(e.to_string()))?
                {
                    Some(edge) => edge,
                    None => continue,
                }
            };
            let (rule_id, parents) = match &edge.provenance {
                exocortex_kernel::Provenance::Derived { rule_id, evidence } => {
                    (Some(rule_id.to_string()), evidence.clone())
                }
                _ => (None, Vec::new()),
            };
            for parent in &parents {
                if seen.insert(*parent) {
                    frontier.push(*parent);
                }
            }
            facts.push(exocortex_reasoning::EdgeFacts {
                edge_hex: MemoryId(edge.id.0).to_hex(),
                from_hex: edge.from.to_hex(),
                to_hex: edge.to.to_hex(),
                kind_name: kind_name(edge.kind),
                rule_id,
                parents: parents.iter().map(|p| MemoryId(p.0).to_hex()).collect(),
            });
        }
        if !facts
            .iter()
            .any(|fact| fact.edge_hex == MemoryId(target_id.0).to_hex() && fact.rule_id.is_some())
        {
            return Err(OpError::BadInput(
                "edge is not derived; there is nothing to explain".into(),
            ));
        }
        // Steel's VM is !Send (Rc internals), so the engine is built
        // after the last await and never crosses one.
        let tree = exocortex_reasoning::ExplainEngine::default()
            .explain(facts, &MemoryId(target_id.0).to_hex());
        Ok(ExplainEdgeOutput { tree })
    }
}

register_operation!(
    ExplainEdgeOp,
    "explain_edge",
    "exocortex.explain_edge",
    POST,
    "/v1/explain_edge",
    ExplainEdgeInput,
    ExplainEdgeOutput
);
