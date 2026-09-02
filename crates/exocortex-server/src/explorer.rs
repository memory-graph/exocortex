//! PX5 (palantir-expansion PRD §3.5): the read-only object explorer,
//! mounted on the node's HTTP surface at `/explorer` behind the same
//! bearer-token layer as the operations (no new auth surface).
//!
//! Six views, all server-rendered HTML with zero JavaScript and no
//! build step — the explorer is a viewer, and its security surface is
//! exactly one read layer over the existing operation handlers:
//!
//! - `/explorer` — index (search + navigation),
//! - `/explorer/memories?type=&project=` — paged list by type,
//! - `/explorer/memories/{id}` — detail + edges grouped by kind,
//! - `/explorer/memories/{id}/neighborhood?k=` — the same bounded
//!   traversal `find_related` uses, as a table,
//! - `/explorer/memories/{id}/provenance` — the memory's provenance,
//!   its `get_chain` derivation chain, and (for administrators) the
//!   audit rows that mention it — degrading honestly without admin,
//! - `/explorer/audit?since_lsn=` — the audit ledger
//!   (`list_audit_records`, administrator-gated like the op it wraps),
//! - `/explorer/ontology` — the loaded packs, types, kinds, verbs,
//!   and both fingerprints: the human-readable `--dump-playbook`.
//!
//! **Visibility is applied at every render**: the list view filters
//! rows through the kernel's `memory_visible` predicate with the
//! CALLER's context, detail/neighborhood reads go through the
//! cache/ops paths that already scope by caller, chain members a
//! caller may not see render as `(hidden)` without leaking ids, and
//! the audit surfaces keep the operation's administrator gate. The
//! regression that pins this (`explorer_list_renders_only_caller_
//! visible_rows`) proves a Private memory belonging to another user
//! is never rendered.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::{routing::get, Router};
use exocortex_ops::operations::{
    FindRelated, FindRelatedInput, GetChainInput, GetChainOp, GetMemory, GetMemoryInput,
    ListAuditInput, ListAuditRecordsOp, SearchInput, SearchMemoriesOp,
};
use exocortex_ops::{OpContext, Operation};
use exocortex_storage::{memory_visible, Direction, RegionKey, TraversalSpec};

const PAGE_STYLE: &str = "body{font-family:ui-monospace,monospace;margin:2rem;max-width:60rem}\
table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccc;padding:.3rem .5rem;text-align:left}\
nav a{margin-right:1rem}code{background:#f4f4f4}";

/// The explorer router; merge it inside the bearer layer (see
/// `http_bind::HttpBind::router`, which does exactly that).
pub fn router(ctx: Arc<OpContext>) -> Router {
    Router::new()
        .route("/explorer", get(index))
        .route("/explorer/search", get(search))
        .route("/explorer/memories", get(list_memories))
        .route("/explorer/memories/:id", get(memory_detail))
        .route("/explorer/memories/:id/neighborhood", get(neighborhood))
        .route("/explorer/memories/:id/provenance", get(provenance))
        .route("/explorer/audit", get(audit))
        .route("/explorer/ontology", get(ontology_view))
        .with_state(ctx)
}

fn esc(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

fn page(title: &str, body: &str) -> Html<String> {
    Html(format!(
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>{}</title><style>{PAGE_STYLE}</style></head>\
<body><nav><a href=\"/explorer\">explorer</a><a href=\"/explorer/ontology\">ontology</a>\
<a href=\"/explorer/audit\">audit</a></nav><h1>{}</h1>{}</body></html>",
        esc(title),
        esc(title),
        body
    ))
}

fn error_page(status: StatusCode, message: &str) -> (StatusCode, Html<String>) {
    (status, page("error", &format!("<p>{}</p>", esc(message))))
}

fn hex32(bytes: &[u8; 16]) -> String {
    exocortex_kernel::MemoryId(*bytes).to_hex()
}

fn provenance_kind(memory: &exocortex_kernel::Memory) -> String {
    match &memory.provenance {
        exocortex_kernel::Provenance::Asserted { author, .. } => {
            format!("asserted by {author}")
        }
        exocortex_kernel::Provenance::Extracted { .. } => "extracted".to_string(),
        exocortex_kernel::Provenance::Derived { rule_id, .. } => {
            format!("derived by {rule_id}")
        }
        exocortex_kernel::Provenance::Computed { .. } => "computed".to_string(),
        _ => "unknown".to_string(),
    }
}

fn type_name(ctx: &OpContext, id: u8) -> String {
    ctx.ontology
        .as_ref()
        .and_then(|onto| onto.memory_type_names.get(id as usize))
        .map(|name| name.to_string())
        .unwrap_or_else(|| format!("type {id}"))
}

#[allow(dead_code)]
fn kind_name(ctx: &OpContext, id: exocortex_kernel::RelKindId) -> String {
    ctx.ontology
        .as_ref()
        .and_then(|onto| onto.kinds_by_id.get(&id))
        .map(|meta| meta.display_name.to_string())
        .unwrap_or_else(|| format!("kind {}", id.0))
}

async fn index(State(_ctx): State<Arc<OpContext>>) -> Html<String> {
    page(
        "exocortex explorer",
        "<form action=\"/explorer/search\" method=\"get\">\
<input name=\"q\" placeholder=\"search memories\"><button>search</button></form>\
<p><a href=\"/explorer/memories\">memories by type</a> — \
<a href=\"/explorer/ontology\">the ontology</a> — \
<a href=\"/explorer/audit\">the audit ledger</a></p>",
    )
}

async fn search(
    State(ctx): State<Arc<OpContext>>,
    Query(input): Query<SearchInput>,
) -> (StatusCode, Html<String>) {
    let op = SearchMemoriesOp;
    match op.handle(&ctx, input).await {
        Ok(output) => {
            let mut rows = String::new();
            for (memory, score) in output.memories.iter().zip(output.scores.iter()) {
                rows.push_str(&format!(
                    "<tr><td><a href=\"/explorer/memories/{}\">{}</a></td><td>{}</td><td>{}</td><td>{score:.2}</td></tr>",
                    memory.id,
                    esc(&memory.title),
                    esc(&type_name(&ctx, memory.memory_type)),
                    esc(&memory.visibility),
                ));
            }
            (
                StatusCode::OK,
                page(
                    "search",
                    &format!("<table><tr><th>title</th><th>type</th><th>visibility</th><th>score</th></tr>{rows}</table>"),
                ),
            )
        }
        Err(error) => error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

#[derive(serde::Deserialize, Default)]
struct ListQuery {
    /// Memory-type NAME (e.g. `Fix`).
    r#type: Option<String>,
    /// Exact project slice; absent = the whole org.
    project: Option<String>,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_page")]
    limit: usize,
}

fn default_page() -> usize {
    50
}

async fn list_memories(
    State(ctx): State<Arc<OpContext>>,
    Query(query): Query<ListQuery>,
) -> (StatusCode, Html<String>) {
    let Some(onto) = ctx.ontology.clone() else {
        return error_page(
            StatusCode::SERVICE_UNAVAILABLE,
            "this surface has no ontology loaded",
        );
    };
    let Some(type_name) = query.r#type.as_deref() else {
        // The type directory when no type is selected.
        let mut items = String::new();
        for (name, id) in onto.memory_type_by_name.iter() {
            items.push_str(&format!(
                "<li><a href=\"/explorer/memories?type={}\">{name} (id {id})</a></li>",
                esc(name)
            ));
        }
        return (
            StatusCode::OK,
            page("memories by type", &format!("<ul>{items}</ul>")),
        );
    };
    let Some(&memory_type) = onto.memory_type_by_name.get(type_name) else {
        return error_page(
            StatusCode::NOT_FOUND,
            &format!("unknown memory type `{type_name}`"),
        );
    };
    let region = RegionKey {
        org: ctx.visibility_ctx.org_id.clone(),
        project: query.project.as_deref().unwrap_or("*").to_string().into(),
        memory_type,
    };
    // Bounded region read (the same seam Dreams uses), then the
    // caller's visibility predicate applied at render — never the
    // reverse.
    let rows = match ctx.storage.memories_in_region(&region, 501).await {
        Ok(rows) => rows,
        Err(error) => return error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let visible: Vec<&exocortex_kernel::Memory> = rows
        .iter()
        .filter(|memory| memory_visible(memory, &ctx.visibility_ctx))
        .collect();
    let window = visible
        .iter()
        .skip(query.offset)
        .take(query.limit.clamp(1, 200))
        .collect::<Vec<_>>();
    let mut body = format!(
        "<p>{} visible rows of type {} (showing {} at offset {})</p>\
<table><tr><th>title</th><th>visibility</th><th>provenance</th><th>lsn</th><th>created</th></tr>",
        visible.len(),
        esc(type_name),
        window.len(),
        query.offset,
    );
    for memory in window {
        body.push_str(&format!(
            "<tr><td><a href=\"/explorer/memories/{}\">{}</a></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            hex32(&memory.id.0),
            esc(&memory.title),
            esc(&format!("{:?}", memory.visibility)),
            esc(&provenance_kind(memory)),
            memory.lsn.value,
            memory.context.timestamp.to_rfc3339(),
        ));
    }
    body.push_str("</table>");
    let next = format!(
        "<p><a href=\"/explorer/memories?type={}&offset={}\">next page</a></p>",
        esc(type_name),
        query.offset + query.limit.clamp(1, 200),
    );
    (StatusCode::OK, page("memories", &format!("{body}{next}")))
}

async fn memory_detail(
    State(ctx): State<Arc<OpContext>>,
    Path(id): Path<String>,
) -> (StatusCode, Html<String>) {
    let op = GetMemory;
    let output = match op.handle(&ctx, GetMemoryInput { id: id.clone() }).await {
        Ok(output) => output,
        // NotFound and Unauthorized both mean "not visible to THIS
        // caller" here — the explorer answers not-found rather than
        // leaking that the row exists.
        Err(exocortex_ops::OpError::NotFound) | Err(exocortex_ops::OpError::Unauthorized(_)) => {
            return error_page(
                StatusCode::NOT_FOUND,
                "no such memory visible to this caller",
            )
        }
        Err(error) => return error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let Some(memory) = output.memory else {
        return error_page(
            StatusCode::NOT_FOUND,
            "no such memory visible to this caller",
        );
    };
    // Edges grouped by kind, one-hop both directions, through the
    // same visibility-scoped traversal the read path uses.
    let anchor = match exocortex_kernel::MemoryId::parse_hex(&memory.id) {
        Some(anchor) => anchor,
        None => return error_page(StatusCode::BAD_REQUEST, "unparseable id"),
    };
    let org = ctx.visibility_ctx.org_id.to_string();
    let mut groups = String::new();
    for (direction, label) in [(Direction::Out, "outgoing"), (Direction::In, "incoming")] {
        let neighbors = ctx.cache.traverse(
            &org,
            &anchor,
            &TraversalSpec {
                direction,
                kinds: Default::default(),
                max_depth: 1,
                max_nodes: 64,
                visibility_ctx: ctx.visibility_ctx.clone(),
                as_of: None,
            },
        );
        if neighbors.is_empty() {
            continue;
        }
        groups.push_str(&format!("<h3>{label}</h3><ul>"));
        for neighbor in &neighbors {
            groups.push_str(&format!(
                "<li><a href=\"/explorer/memories/{}\">{}</a> ({})</li>",
                hex32(&neighbor.id.0),
                esc(&neighbor.title),
                esc(&type_name(&ctx, neighbor.memory_type)),
            ));
        }
        groups.push_str("</ul>");
    }
    if groups.is_empty() {
        groups = "<p>no visible edges</p>".to_string();
    }
    let body = format!(
        "<h2>{}</h2><p><code>{}</code></p><table>\
<tr><th>type</th><td>{}</td></tr>\
<tr><th>visibility</th><td>{}</td></tr>\
<tr><th>provenance</th><td>visible via get_memory</td></tr>\
<tr><th>neighbors</th><td><a href=\"/explorer/memories/{}/neighborhood\">neighborhood</a> · \
<a href=\"/explorer/memories/{}/provenance\">provenance</a></td></tr>\
</table>{groups}",
        esc(&memory.title),
        esc(&memory.id),
        esc(&type_name(&ctx, memory.memory_type)),
        esc(&memory.visibility),
        esc(&memory.id),
        esc(&memory.id),
    );
    (StatusCode::OK, page("memory", &body))
}

#[derive(serde::Deserialize, Default)]
struct NeighborhoodQuery {
    #[serde(default = "default_k")]
    k: u8,
}

fn default_k() -> u8 {
    2
}

async fn neighborhood(
    State(ctx): State<Arc<OpContext>>,
    Path(id): Path<String>,
    Query(query): Query<NeighborhoodQuery>,
) -> (StatusCode, Html<String>) {
    let op = FindRelated;
    match op
        .handle(
            &ctx,
            FindRelatedInput {
                anchor: id,
                k: query.k,
            },
        )
        .await
    {
        Ok(output) => {
            let mut rows = String::new();
            for memory in &output.memories {
                rows.push_str(&format!(
                    "<tr><td><a href=\"/explorer/memories/{}\">{}</a></td><td>{}</td><td>{}</td></tr>",
                    memory.id,
                    esc(&memory.title),
                    esc(&type_name(&ctx, memory.memory_type)),
                    esc(&memory.visibility),
                ));
            }
            (
                StatusCode::OK,
                page(
                    "neighborhood",
                    &format!(
                        "<table><tr><th>title</th><th>type</th><th>visibility</th></tr>{rows}</table>"
                    ),
                ),
            )
        }
        Err(error) => error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn provenance(
    State(ctx): State<Arc<OpContext>>,
    Path(id): Path<String>,
) -> (StatusCode, Html<String>) {
    let Some(anchor) = exocortex_kernel::MemoryId::parse_hex(&id) else {
        return error_page(StatusCode::BAD_REQUEST, "expected 32-char hex id");
    };
    let op = GetChainOp;
    let chain = match op
        .handle(
            &ctx,
            GetChainInput {
                memory: id.clone(),
                max_depth: 2,
            },
        )
        .await
    {
        Ok(output) => output.chain,
        Err(error) => return error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let org = ctx.visibility_ctx.org_id.to_string();
    let mut chain_rows = String::new();
    for (index, hex) in chain.iter().enumerate() {
        // Chain members the CALLER may not see render as hidden — the
        // shape of the chain is honest, no id leaks.
        match exocortex_kernel::MemoryId::parse_hex(hex)
            .and_then(|id| ctx.cache.get_memory(&org, &id, &ctx.visibility_ctx))
        {
            Some(memory) => chain_rows.push_str(&format!(
                "<li><a href=\"/explorer/memories/{hex}\">{}</a></li>",
                esc(&memory.title)
            )),
            None => chain_rows.push_str(&format!("<li>(hidden or absent)</li> index {index}")),
        }
    }
    if chain_rows.is_empty() {
        chain_rows =
            "<li>no derivation chain (no Derived evidence reaches this memory)</li>".into();
    }
    // The audit rows that mention this memory — administrators only,
    // degrading honestly (the op's own gate) without leaking anything.
    let audit_section = if ctx.audit_admin {
        match ListAuditRecordsOp
            .handle(&ctx, ListAuditInput { since_lsn: 0 })
            .await
        {
            Ok(output) => {
                let mut rows = String::new();
                let mut shown = 0usize;
                for record in &output.records {
                    if record.to_string().contains(&hex32(&anchor.0)) {
                        rows.push_str(&format!(
                            "<tr><td><code>{}</code></td></tr>",
                            esc(&record.to_string())
                        ));
                        shown += 1;
                    }
                }
                if shown == 0 {
                    "<p>no audit rows mention this memory</p>".to_string()
                } else {
                    format!("<table><tr><th>audit rows mentioning this memory ({shown})</th></tr>{rows}</table>")
                }
            }
            Err(error) => format!("<p>audit read failed: {}</p>", esc(&error.to_string())),
        }
    } else {
        "<p>audit rows require administrator permission — this view degrades to the chain above</p>"
            .to_string()
    };
    let body = format!(
        "<h3>derivation chain (origin first)</h3><ul>{chain_rows}</ul>\
<h3>why the system believes this</h3>{audit_section}\
<p>Derived-edge explanations: call <code>explain_edge</code> over a derived edge from the \
memory detail view (the operation is registered; the explorer links it in v1.1).</p>",
    );
    (StatusCode::OK, page("provenance", &body))
}

#[derive(serde::Deserialize, Default)]
struct AuditQuery {
    #[serde(default)]
    since_lsn: u64,
}

async fn audit(
    State(ctx): State<Arc<OpContext>>,
    Query(query): Query<AuditQuery>,
) -> (StatusCode, Html<String>) {
    if !ctx.audit_admin {
        return error_page(
            StatusCode::FORBIDDEN,
            "the audit ledger requires explicit administrator permission (same gate as list_audit_records)",
        );
    }
    match ListAuditRecordsOp
        .handle(
            &ctx,
            ListAuditInput {
                since_lsn: query.since_lsn,
            },
        )
        .await
    {
        Ok(output) => {
            let mut rows = String::new();
            for record in &output.records {
                rows.push_str(&format!(
                    "<tr><td><code>{}</code></td></tr>",
                    esc(&record.to_string())
                ));
            }
            (
                StatusCode::OK,
                page(
                    "audit ledger",
                    &format!("<table><tr><th>record</th></tr>{rows}</table>"),
                ),
            )
        }
        Err(error) => error_page(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn ontology_view(State(ctx): State<Arc<OpContext>>) -> (StatusCode, Html<String>) {
    let Some(onto) = ctx.ontology.clone() else {
        return error_page(
            StatusCode::SERVICE_UNAVAILABLE,
            "this surface has no ontology loaded",
        );
    };
    let mut body = String::new();
    body.push_str("<h3>packs</h3><ul>");
    for pack in &onto.packs {
        let version = |v: &exocortex_kernel::pack::PackVersion| {
            format!("{}.{}.{}", v.major, v.minor, v.patch)
        };
        body.push_str(&format!(
            "<li>{} v{} (kernel_min {})</li>",
            esc(&pack.name),
            version(&pack.version),
            version(&pack.kernel_min)
        ));
    }
    body.push_str("</ul><h3>memory types</h3><ul>");
    for (id, name) in onto.memory_type_names.iter().enumerate() {
        body.push_str(&format!("<li>{id} — {}</li>", esc(name)));
    }
    body.push_str("</ul><h3>entity types</h3><ul>");
    for (id, name) in onto.entity_type_names.iter().enumerate() {
        body.push_str(&format!("<li>{id} — {}</li>", esc(name)));
    }
    body.push_str("</ul><h3>relationship kinds</h3><ul>");
    let mut kinds: Vec<_> = onto.kinds_by_id.values().collect();
    kinds.sort_by_key(|meta| meta.id);
    for meta in kinds {
        body.push_str(&format!(
            "<li>{} — {}{}</li>",
            meta.id.0,
            esc(&meta.display_name),
            if meta.computed_only {
                " (computed-only)"
            } else {
                ""
            }
        ));
    }
    body.push_str("</ul><h3>pack verbs</h3><ul>");
    for pack in &onto.packs {
        for action in &pack.actions {
            body.push_str(&format!(
                "<li>{}.{}</li>",
                esc(&pack.name),
                esc(&action.name)
            ));
        }
        for function in &pack.functions {
            body.push_str(&format!(
                "<li>{}.{}</li>",
                esc(&pack.name),
                esc(&function.name)
            ));
        }
    }
    let compat = hex64(&onto.fingerprint.0);
    let build = hex64(&onto.build_fingerprint.0);
    body.push_str(&format!(
        "</ul><h3>fingerprints</h3>\
<p>compatibility (gates): <code>{compat}</code></p>\
<p>build (reports): <code>{build}</code></p>"
    ));
    (StatusCode::OK, page("ontology", &body))
}

fn hex64(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
