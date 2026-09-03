//! PX2: the pack-verb execution framework (palantir-expansion PRD §3.2,
//! §4.3). The kernel holds the typed registrations; THIS module is the
//! framework that runs them — ceiling enforcement, the one-implementation
//! preflight, provenance stamping, the audited batch commit, and the
//! shared registry handlers that put every pack verb on the same
//! MCP/HTTP surface as kernel ops.
//!
//! What a pack Action cannot bypass (P3): the caller-ceiling check, the
//! kernel validator, the computed-only kind rejection, provenance, the
//! audit row, and atomic commit all happen HERE. The body only chooses
//! WHAT to produce.

use exocortex_kernel::verbs::{ActionTarget, PackActionRegistration, PackFunctionRegistration};
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, ProducerKind, Provenance, Relationship, RelationshipId,
    RelationshipProperties,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::audit::digest_input;
use crate::Operation as _;
use crate::{OpContext, OpError, OperationEntry};
// ---------------------------------------------------------------------------
// Registry materialization (called once from `entries()`).
// ---------------------------------------------------------------------------

fn hex16(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn leak(s: String) -> &'static str {
    // Bounded: one allocation per declared verb, once per process.
    Box::leak(s.into_boxed_str())
}

/// Build the `OperationEntry` set for every registered pack verb. Names
/// are `{pack}.{verb}` so the registry's global-name uniqueness holds
/// across packs; HTTP paths ride `/v1/packs/{pack}/{verb}`.
pub(crate) fn registry_entries() -> Vec<OperationEntry> {
    let mut out = Vec::new();
    for reg in exocortex_kernel::verbs::registered_pack_actions() {
        let name = leak(format!("{}.{}", reg.pack_name, reg.verb_name));
        out.push(OperationEntry {
            name,
            mcp_tool_name: leak(format!("exocortex.pack.{}", reg.verb_name)),
            pack: Some(reg.pack_name),
            http_method: || http::Method::POST,
            http_path: leak(format!("/v1/packs/{}/{}", reg.pack_name, reg.verb_name)),
            input_schema: reg.input_schema,
            output_schema: || schemars::schema_for!(PackActionOutput),
            handler: |entry, ctx, v| {
                Box::pin(async move { dispatch_pack_action(entry, ctx, v).await })
            },
        });
    }
    for reg in exocortex_kernel::verbs::registered_pack_functions() {
        let name = leak(format!("{}.{}", reg.pack_name, reg.verb_name));
        out.push(OperationEntry {
            name,
            mcp_tool_name: leak(format!("exocortex.pack.{}", reg.verb_name)),
            pack: Some(reg.pack_name),
            http_method: || http::Method::POST,
            http_path: leak(format!("/v1/packs/{}/{}", reg.pack_name, reg.verb_name)),
            input_schema: reg.input_schema,
            output_schema: reg.output_schema,
            handler: |entry, ctx, v| {
                Box::pin(async move { dispatch_pack_function(entry, ctx, v).await })
            },
        });
    }
    out
}

fn split_entry_name(
    entry: &'static OperationEntry,
) -> Result<(&'static str, &'static str), OpError> {
    let name = entry.name;
    let dot = name
        .find('.')
        .ok_or_else(|| OpError::Other(format!("pack verb entry `{name}` lacks pack identity")))?;
    Ok((&name[..dot], &name[dot + 1..]))
}

fn find_action(entry: &'static OperationEntry) -> Result<&'static PackActionRegistration, OpError> {
    let (pack, verb) = split_entry_name(entry)?;
    exocortex_kernel::verbs::registered_pack_actions()
        .into_iter()
        .find(|r| r.pack_name == pack && r.verb_name == verb)
        .ok_or_else(|| OpError::Other(format!("no pack action registered as `{pack}.{verb}`")))
}

fn find_function(
    entry: &'static OperationEntry,
) -> Result<&'static PackFunctionRegistration, OpError> {
    let (pack, verb) = split_entry_name(entry)?;
    exocortex_kernel::verbs::registered_pack_functions()
        .into_iter()
        .find(|r| r.pack_name == pack && r.verb_name == verb)
        .ok_or_else(|| OpError::Other(format!("no pack function registered as `{pack}.{verb}`")))
}

// ---------------------------------------------------------------------------
// Actions: prepare (validate) + commit + dispatch.
// ---------------------------------------------------------------------------

/// Output of a pack Action: what the framework committed.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PackActionOutput {
    /// The verb that ran, as `{pack}.{verb}`.
    pub verb: String,
    /// Committed memory ids, in product order.
    pub memories: Vec<String>,
    /// Committed relationship ids (authored edges; inverses ride the same
    /// commit through the storage layer's R-T4 materialization).
    pub edges: Vec<String>,
    /// The LSN of the audit row written atomically with the rows.
    pub audit_lsn: u64,
}

/// One row the preflight pass reports for a pack action — the SAME
/// `LocalRejection` vocabulary `preflight_wrapup` emits (one
/// implementation, not three: the kernel validator is the rulebook).
pub type PackPreflightRejection = crate::preflight::LocalRejection;

/// The dry-run verdict over a pack action's product.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PackPreflightResult {
    /// Rows that would commit.
    pub would_accept: u32,
    /// Rows the commit would reject.
    pub would_reject: u32,
    /// The problems, preflight vocabulary.
    pub rejections: Vec<PackPreflightRejection>,
}

/// The validated, commit-ready form of an action product: kernel drafts
/// plus the edge list with in-batch keys resolved against the drafts.
pub struct PreparedAction {
    memories: Vec<Memory>,
    relationships: Vec<Relationship>,
    audit_label: String,
}

/// Run the typed body and validate its product against the effective
/// ontology and the declared ceiling — the ONE preflight rulebook shared
/// with `preflight_wrapup` (W2: the kernel owns the semantics).
/// `resolve_target` maps an existing-memory edge target to its
/// caller-visible row (the commit path resolves through scoped reads;
/// `preflight_action` resolves through the local cache — same rulebook,
/// different reader).
pub fn prepare_pack_action(
    ontology: &exocortex_kernel::Ontology,
    reg: &PackActionRegistration,
    caller: &exocortex_storage::VisibilityContext,
    input: serde_json::Value,
    resolve_target: &dyn Fn(&MemoryId) -> Option<Memory>,
) -> Result<PreparedAction, OpError> {
    // KP5 pattern: the ceiling comes from the typed registration, never
    // from the caller or the body.
    if caller.max_visibility > reg.ceiling {
        return Err(OpError::Unauthorized(format!(
            "caller visibility {:?} exceeds the {:?} ceiling",
            caller.max_visibility, reg.ceiling
        )));
    }
    let ctx = exocortex_kernel::verbs::ActionContext {
        ceiling: reg.ceiling,
    };
    let product = (reg.run)(&ctx, input).map_err(|e| OpError::BadInput(e.to_string()))?;

    // Pack-local type ids remap through the pack's own names (the pack
    // slot's offset is a load-time property; names are the stable surface).
    let pack = ontology
        .packs
        .iter()
        .find(|p| p.name == reg.pack_name)
        .ok_or_else(|| {
            OpError::Other(format!(
                "pack `{}` is not loaded in this ontology",
                reg.pack_name
            ))
        })?;
    let local_name = |local: u8| -> Result<&str, OpError> {
        pack.memory_type_names
            .get(local as usize)
            .map(|n| n.as_str())
            .ok_or_else(|| OpError::BadInput(format!("unknown pack-local memory type {local}")))
    };

    let now = chrono::Utc::now();
    let mut by_key: std::collections::HashMap<SmolStr, Memory> = std::collections::HashMap::new();
    let mut order: Vec<SmolStr> = Vec::new();
    for m in &product.memories {
        let local = local_name(m.memory_type)?;
        let effective = ontology.memory_type_id(local).ok_or_else(|| {
            OpError::BadInput(format!(
                "memory type `{local}` not in the effective ontology"
            ))
        })?;
        // Framework-enforced ceiling: a body stamping wider than the
        // declared verb ceiling is rejected here, no matter what the body
        // did (the acceptance test's "pack author cannot bypass it").
        if !m.visibility.within(reg.ceiling) {
            return Err(OpError::Unauthorized(format!(
                "action product visibility {:?} exceeds the {:?} ceiling",
                m.visibility, reg.ceiling
            )));
        }
        let draft = exocortex_kernel::MemoryDraft {
            memory_type: effective,
            title: m.title.clone(),
            content: m.content.clone(),
            summary: m.summary.clone(),
            visibility: m.visibility,
            context: MemoryContext {
                timestamp: now,
                project_id: caller.project_ids.iter().next().cloned(),
                project_path: None,
                team_id: caller.team_ids.iter().next().cloned(),
                tenant_id: Some(caller.org_id.clone()),
                session_id: None,
                user_id: Some(caller.user_id.clone()),
                created_by: None,
                files_involved: Default::default(),
                languages: Default::default(),
                frameworks: Default::default(),
                technologies: Default::default(),
                git_commit: None,
                git_branch: None,
                working_directory: None,
                entities: Default::default(),
                additional_metadata: serde_json::Value::Null,
            },
            edge_hints: Default::default(),
            external_key: None,
        };
        exocortex_kernel::validator::validate_draft(
            ontology,
            &draft,
            exocortex_kernel::validator::SourceCeiling {
                source: "pack-action",
                ceiling: reg.ceiling,
            },
        )
        .map_err(|e| OpError::BadInput(e.to_string()))?;
        let memory = Memory {
            rights: None,
            id: MemoryId::new_v7(),
            memory_type: effective,
            title: m.title.clone(),
            content: m.content.clone(),
            summary: m.summary.clone(),
            tags: exocortex_kernel::normalize_tags(m.tags.iter().map(|t| t.as_str())),
            visibility: m.visibility,
            provenance: Provenance::Asserted {
                author: format!("{}.{}", reg.pack_name, reg.verb_name).into(),
                producer_kind: Some(ProducerKind::Custom),
            },
            context: draft.context.clone(),
            importance: exocortex_kernel::memory::F01::new(0.5)
                .map_err(|e| OpError::Other(e.to_string()))?,
            confidence: exocortex_kernel::memory::F01::new(0.8)
                .map_err(|e| OpError::Other(e.to_string()))?,
            effectiveness: None,
            usage_count: 0,
            valid_from: now,
            valid_until: None,
            recorded_at: now,
            embedding: None,
            invalidated_by: None,
            lsn: exocortex_kernel::LSN::new_local(0),
        };
        if by_key.insert(m.draft_key.clone(), memory).is_some() {
            return Err(OpError::BadInput(format!(
                "duplicate draft_key `{}` in action product",
                m.draft_key
            )));
        }
        order.push(m.draft_key.clone());
    }

    // Existing-memory targets: the caller must be able to SEE them (IN2
    // discipline — never mutate/blind-link against invisible rows); the
    // visibility check itself happens in the async commit path.
    let mut relationships = Vec::with_capacity(product.edges.len());
    for e in &product.edges {
        let from = by_key.get(&e.from_draft_key).ok_or_else(|| {
            OpError::BadInput(format!(
                "edge from unknown draft_key `{}`",
                e.from_draft_key
            ))
        })?;
        let to = match &e.to {
            ActionTarget::Draft(key) => by_key
                .get(key)
                .ok_or_else(|| OpError::BadInput(format!("edge to unknown draft_key `{key}`")))?,
            ActionTarget::Memory(id) => &resolve_target(id).ok_or_else(|| {
                OpError::Unauthorized(format!(
                    "edge target {} is unknown or not visible to this caller",
                    id.to_hex()
                ))
            })?,
        };
        let kind = ontology.kind_id(e.kind).ok_or_else(|| {
            OpError::BadInput(format!("unknown kind `{}` in action product", e.kind))
        })?;
        if ontology
            .kinds_by_id
            .get(&kind)
            .is_some_and(|meta| meta.computed_only)
        {
            return Err(OpError::Unauthorized(format!(
                "kind `{}` is computed-only (R-T14): Dreams proposes it, actions never assert it",
                e.kind
            )));
        }
        let vis = exocortex_kernel::relationship_visibility(from.visibility, to.visibility);
        if !vis.within(reg.ceiling) {
            return Err(OpError::Unauthorized(format!(
                "edge visibility {vis:?} exceeds the {:?} ceiling",
                reg.ceiling
            )));
        }
        exocortex_kernel::validator::validate_triple(
            ontology,
            from.memory_type,
            kind,
            to.memory_type,
        )
        .map_err(|err| OpError::BadInput(err.to_string()))?;
        let default_strength = ontology.kinds_by_id[&kind].default_strength;
        relationships.push(Relationship {
            id: RelationshipId::derive(from.id, kind, to.id, None),
            kind,
            from: from.id,
            to: to.id,
            visibility: vis,
            provenance: Provenance::Asserted {
                author: format!("{}.{}", reg.pack_name, reg.verb_name).into(),
                producer_kind: Some(ProducerKind::Custom),
            },
            properties: RelationshipProperties {
                strength: e.strength.unwrap_or(default_strength),
                confidence: 0.8,
                context: None,
                evidence_count: 1,
                success_rate: None,
                validation_count: 0,
                counter_evidence_count: 0,
                last_validated: now,
            },
            description: None,
            bidirectional: ontology.kinds_by_id[&kind].bidirectional,
            valid_from: now,
            valid_until: None,
            recorded_at: now,
            invalidated_by: None,
            lsn: exocortex_kernel::LSN::new_local(0),
        });
    }

    let mut memories = Vec::with_capacity(order.len());
    for key in order {
        memories.push(by_key.remove(&key).expect("ordered key"));
    }
    Ok(PreparedAction {
        memories,
        relationships,
        audit_label: format!("{}.{}", reg.pack_name, reg.verb_name),
    })
}

// Existing-memory edge targets are resolved through CALLER-SCOPED reads
// in the async dispatch path and handed to the (sync) prepare pass as a
// lookup closure — `prepare` itself stays storage-free so
// `preflight_action` can run it against the local cache the same way
// `preflight_wrapup` does.

/// Shared registry handler for every pack Action entry: prepare, resolve
/// existing-memory targets through caller-scoped reads, commit rows +
/// audit atomically.
async fn dispatch_pack_action(
    entry: &'static OperationEntry,
    ctx: &OpContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, OpError> {
    ctx.check_deadline()?;
    let reg = find_action(entry)?;
    let ontology = ctx
        .ontology
        .clone()
        .ok_or_else(|| OpError::Other("pack actions require the effective ontology".into()))?;

    // Resolve every existing-memory target referenced by the input JSON
    // (32-hex strings) through CALLER-SCOPED reads before prepare (IN2):
    // a blind batch read keeps invisible rows out of the framework.
    let targets = harvest_visible_targets(ctx, &input).await?;
    let resolve = |id: &MemoryId| targets.get(id).cloned();

    let prepared =
        prepare_pack_action(&ontology, reg, &ctx.visibility_ctx, input.clone(), &resolve)?;

    let mut output_ids: SmallVec<[SmolStr; 8]> = SmallVec::new();
    for m in &prepared.memories {
        output_ids.push(m.id.to_hex().into());
    }
    for r in &prepared.relationships {
        output_ids.push(hex16(&r.id.0).into());
    }
    let audit = crate::audit::AuditRecord {
        action: prepared.audit_label.clone().into(),
        actor: ctx.visibility_ctx.user_id.clone(),
        org_id: ctx.visibility_ctx.org_id.clone(),
        input_digest: digest_input(&serde_json::json!({
            "pack": reg.pack_name,
            "verb": reg.verb_name,
            "caller_visibility": ctx.visibility_ctx.max_visibility as u8,
            "input": input,
        })),
        output_ids,
        fingerprint: ctx.storage.ontology_fingerprint(),
        lease_epoch: None,
        recorded_at: chrono::Utc::now(),
    };
    let records = ctx
        .storage
        .upsert_batch_audited(&prepared.memories, &prepared.relationships, &audit)
        .await
        .map_err(|e| OpError::Storage(e.to_string()))?;
    let audit_lsn = records.last().map(|r| r.lsn).unwrap_or_default();
    serde_json::to_value(PackActionOutput {
        verb: prepared.audit_label,
        memories: prepared.memories.iter().map(|m| m.id.to_hex()).collect(),
        edges: prepared
            .relationships
            .iter()
            .map(|r| hex16(&r.id.0))
            .collect(),
        audit_lsn,
    })
    .map_err(|e| OpError::Other(e.to_string()))
}

/// Find every 32-hex string in the action input and load the
/// caller-visible memory rows it names. Unknown or invisible ids surface
/// as rejections from `prepare` (never as blind links).
async fn harvest_visible_targets(
    ctx: &OpContext,
    value: &serde_json::Value,
) -> Result<std::collections::HashMap<MemoryId, Memory>, OpError> {
    let mut ids: Vec<MemoryId> = Vec::new();
    fn walk(value: &serde_json::Value, ids: &mut Vec<MemoryId>) {
        match value {
            serde_json::Value::String(s) => {
                if s.len() == 32 {
                    if let Some(id) = MemoryId::parse_hex(s) {
                        ids.push(id);
                    }
                }
            }
            serde_json::Value::Array(items) => items.iter().for_each(|v| walk(v, ids)),
            serde_json::Value::Object(map) => map.values().for_each(|v| walk(v, ids)),
            _ => {}
        }
    }
    walk(value, &mut ids);
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let visible = ctx
        .storage
        .get_visible_memories(&ids, &ctx.visibility_ctx)
        .await
        .map_err(|e| OpError::Storage(e.to_string()))?;
    Ok(visible.into_iter().map(|m| (m.id, m)).collect())
}

// ---------------------------------------------------------------------------
// Functions: dispatch through the reasoning crate's Steel interpreter.
// ---------------------------------------------------------------------------

async fn dispatch_pack_function(
    entry: &'static OperationEntry,
    ctx: &OpContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, OpError> {
    ctx.check_deadline()?;
    let reg = find_function(entry)?;
    // v1 pack Functions are pure typed computations over their input
    // (recorded boundary: graph-fed pack functions need a query contract
    // that does not exist yet). Nothing is read, so the caller-visibility
    // filter holds trivially — an authenticated context is still required.
    let _ = &ctx.visibility_ctx;
    let out = eval_pack_function_cached(reg.body, &input).map_err(OpError::BadInput)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// `preflight_action`: the dry-run op for every pack Action (PX2
// acceptance). ONE mechanism: it runs the same `prepare_pack_action` the
// commit path runs, with existing-memory targets resolved from the local
// cache the way `preflight_wrapup` resolves them.
// ---------------------------------------------------------------------------

/// `preflight_action` — dry-run a pack Action without committing.
#[derive(Default)]
pub struct PreflightActionOp;

/// Input for `preflight_action`.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PreflightActionInput {
    /// Owning pack name.
    pub pack: String,
    /// Verb name.
    pub verb: String,
    /// The typed action input, verbatim.
    pub input: serde_json::Value,
}

#[async_trait::async_trait]
impl crate::Operation for PreflightActionOp {
    type Input = PreflightActionInput;
    type Output = PackPreflightResult;
    fn name(&self) -> &'static str {
        "preflight_action"
    }
    fn mcp_tool_name(&self) -> &'static str {
        "exocortex.preflight_action"
    }
    fn http_method(&self) -> http::Method {
        http::Method::POST
    }
    fn http_path(&self) -> &'static str {
        "/v1/preflight_action"
    }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        ctx.check_deadline()?;
        let reg = exocortex_kernel::verbs::registered_pack_actions()
            .into_iter()
            .find(|r| r.pack_name == input.pack && r.verb_name == input.verb)
            .ok_or(OpError::NotFound)?;
        let ontology = ctx
            .ontology
            .clone()
            .ok_or_else(|| OpError::Other("preflight requires the effective ontology".into()))?;
        // Cache-based target resolution: same reader preflight_wrapup uses.
        let cache = ctx.cache.clone();
        let org = ctx.visibility_ctx.org_id.to_string();
        let vc = ctx.visibility_ctx.clone();
        let resolve = |id: &MemoryId| -> Option<Memory> { cache.get_memory(&org, id, &vc) };
        match prepare_pack_action(&ontology, reg, &ctx.visibility_ctx, input.input, &resolve) {
            Ok(prepared) => Ok(PackPreflightResult {
                would_accept: (prepared.memories.len() + prepared.relationships.len()) as u32,
                would_reject: 0,
                rejections: Vec::new(),
            }),
            // Fail-fast is the honest verdict shape for a typed body: the
            // first rulebook violation names itself in the shared
            // correction vocabulary, exactly like a Submit reject row.
            Err(OpError::Unauthorized(detail)) => Ok(PackPreflightResult {
                would_accept: 0,
                would_reject: 1,
                rejections: vec![PackPreflightRejection {
                    draft_key: format!("{}.{}", input.pack, input.verb),
                    code: "VisibilityWidening".into(),
                    detail,
                    correction: exocortex_wire::corrections::guidance(
                        exocortex_wire::ingest::v1::RejectCode::VisibilityWidening,
                    )
                    .correction
                    .into(),
                }],
            }),
            Err(OpError::BadInput(detail)) => Ok(PackPreflightResult {
                would_accept: 0,
                would_reject: 1,
                rejections: vec![PackPreflightRejection {
                    draft_key: format!("{}.{}", input.pack, input.verb),
                    code: "Unknown".into(),
                    detail,
                    correction: "Fix the named field or row; the kernel rulebook rejected it."
                        .into(),
                }],
            }),
            Err(other) => Err(other),
        }
    }
}

crate::register_operation!(
    PreflightActionOp,
    "preflight_action",
    "exocortex.preflight_action",
    POST,
    "/v1/preflight_action",
    PreflightActionInput,
    PackPreflightResult
);

// ---------------------------------------------------------------------------
// PX2: the pack-Function scheme evaluator. Lives in ops (JSON is native
// at this operation boundary) over the same embedded Steel interpreter
// the reasoning crate's explain engine uses — CR-8 keeps the reasoning
// rule path serialization-free, and the interpreter itself is unchanged.
// ---------------------------------------------------------------------------

/// Execute a pack Function's `scheme` body as a pure typed computation
/// over its input. The input object is exposed to the program as
/// `(input "field")` returning the JSON field as a Scheme primitive
/// (bool/int/float/string). Deterministic: same body + same input, same
/// output. The VM is constructed fresh per call — see
/// [`eval_pack_function_cached`] for the budgeted form.
pub fn eval_pack_function(
    body: &str,
    input: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use steel::rvals::SteelVal;
    use steel::steel_vm::engine::Engine;
    use steel::steel_vm::register_fn::RegisterFn;

    let mut vm = Engine::new();
    let input = input.clone();
    vm.register_fn("input", move |field: String| -> SteelVal {
        json_field_to_steel(&input, &field)
    });
    let values = vm.run(body.to_string()).map_err(|e| format!("{e:?}"))?;
    steel_last_to_json(&values)
}

/// The budgeted form: the Steel VM is expensive to construct, so the SLO
/// bench and dispatch reuse a per-thread VM. Determinism is preserved for
/// pure bodies — the program re-evaluates from its own definitions on
/// every `run`, and the one registered FFI (`input`) is re-bound per call.
pub fn eval_pack_function_cached(
    body: &str,
    input: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use steel::rvals::SteelVal;
    use steel::steel_vm::engine::Engine;
    use steel::steel_vm::register_fn::RegisterFn;

    thread_local! {
        static VM: std::cell::RefCell<Option<Engine>> = const { std::cell::RefCell::new(None) };
    }

    struct Output(Option<Result<serde_json::Value, String>>);
    let mut out = Output(None);
    VM.with(|slot| {
        let mut guard = slot.borrow_mut();
        let vm = guard.get_or_insert_with(Engine::new);
        let input = input.clone();
        // Re-registering replaces the previous call's binding.
        vm.register_fn("input", move |field: String| -> SteelVal {
            json_field_to_steel(&input, &field)
        });
        out.0 = Some(match vm.run(body.to_string()) {
            Ok(values) => steel_last_to_json(&values),
            Err(e) => Err(format!("{e:?}")),
        });
    });
    out.0
        .unwrap_or_else(|| Err("pack function evaluation did not run".into()))
}

fn json_field_to_steel(input: &serde_json::Value, field: &str) -> steel::rvals::SteelVal {
    use steel::rvals::SteelVal;
    match input.get(field) {
        None => SteelVal::BoolV(false),
        Some(serde_json::Value::Bool(b)) => SteelVal::BoolV(*b),
        Some(serde_json::Value::Number(n)) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => SteelVal::IntV(i as isize),
            (None, Some(f)) => SteelVal::NumV(f),
            (None, None) => SteelVal::BoolV(false),
        },
        Some(serde_json::Value::String(s)) => SteelVal::StringV(s.as_str().into()),
        Some(other) => SteelVal::StringV(other.to_string().into()),
    }
}

fn steel_last_to_json(values: &[steel::rvals::SteelVal]) -> Result<serde_json::Value, String> {
    use steel::rvals::SteelVal;
    let last = values
        .last()
        .ok_or_else(|| "pack function produced no value".to_string())?;
    match last {
        SteelVal::BoolV(b) => Ok(serde_json::Value::Bool(*b)),
        SteelVal::IntV(i) => Ok(serde_json::Value::from(*i as i64)),
        SteelVal::NumV(f) => Ok(serde_json::Value::from(*f)),
        SteelVal::StringV(s) => Ok(serde_json::Value::String(s.to_string())),
        other => Err(format!(
            "pack function returned an unconvertible value: {other:?}"
        )),
    }
}
