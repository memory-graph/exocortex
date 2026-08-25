// crates/exocortex-server/src/http_bind.rs
//! The HTTP parity surface (§21.1, CR-9): every operation registered in
//! `exocortex_ops::entries()` is mounted at its `(method, http_path)` with a
//! shared `Arc<OpContext>`, behind bearer-token auth (R-Sec7). The same
//! type-erased handler serves MCP and HTTP — there is no second
//! implementation of any operation.
//!
//! Also mounts the §19 observability endpoints while the router is open:
//! `/metrics` (R-O2, Prometheus), `/health/ready` (R-O4), `/health/cluster`
//! (R-O5), `/health/sync` (R-O6), and `/health/hydration` (R-M5).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;

use exocortex_ops::{entries, OpContext, OpError, OperationEntry};

/// Shared runtime health snapshot (R-O5/R-O6/R-M5), updated by the node
/// loops and rendered by the health endpoints.
#[derive(Clone, Debug, Default)]
pub struct HealthSnapshot {
    /// This node's id.
    pub node_id: String,
    /// The current Dreams lease holder, when known.
    pub leader_node_id: Option<String>,
    /// The lease fencing epoch in force.
    pub lease_epoch: u64,
    /// Highest backend LSN committed through this node.
    pub backend_lsn: u64,
    /// Highest backend LSN the local sync path applied.
    pub sync_lsn: u64,
    /// True once startup hydration completed.
    pub hydrated: bool,
}

/// The HTTP binding: operation routes + auth + observability.
pub struct HttpBind {
    ctx: Arc<OpContext>,
    bearer: String,
    health: Arc<arc_swap::ArcSwap<HealthSnapshot>>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
}

impl HttpBind {
    /// Build the binding. `bearer` is enforced on every operation route
    /// (R-Sec7); the health and metrics endpoints stay unauthenticated so
    /// load balancers and scrapers can probe them.
    pub fn new(ctx: Arc<OpContext>, bearer: String) -> Self {
        Self {
            ctx,
            bearer,
            health: Arc::new(arc_swap::ArcSwap::from(Arc::new(HealthSnapshot {
                node_id: "exocortex-node".into(),
                ..Default::default()
            }))),
            prometheus: install_prometheus(),
        }
    }

    /// The shared health snapshot handle (node loops update it).
    pub fn health_handle(&self) -> Arc<arc_swap::ArcSwap<HealthSnapshot>> {
        self.health.clone()
    }

    /// Assemble the router: every registered operation at its
    /// `(method, path)`, auth-gated, plus the observability endpoints and
    /// (optionally) the SSE change feed.
    pub fn router(&self, extra: Option<Router>) -> Router {
        let mut ops = Router::new();
        for entry in entries() {
            let handler = op_route(entry, self.ctx.clone());
            ops = if (entry.http_method)() == axum::http::Method::GET {
                ops.route(entry.http_path, get(handler))
            } else {
                ops.route(entry.http_path, post(handler))
            };
        }
        let bearer = self.bearer.clone();
        ops = ops.layer(middleware::from_fn(move |req, next| {
            auth(req, next, bearer.clone())
        }));

        let _health = self.health.clone();
        let health_cluster = self.health.clone();
        let health_sync = self.health.clone();
        let health_hydration = self.health.clone();
        let prom = self.prometheus.clone();
        let ctx = self.ctx.clone();
        let obs = Router::new()
            .route(
                "/metrics",
                get(move || {
                    let text = prom.render();
                    async move {
                        (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                "text/plain; version=0.0.4",
                            )],
                            text,
                        )
                    }
                }),
            )
            .route(
                "/health/ready",
                get(|| async { axum::Json(serde_json::json!({ "status": "ready" })) }),
            )
            .route(
                "/health/cluster",
                get(move || {
                    let h = health_cluster.load_full();
                    async move {
                        axum::Json(serde_json::json!({
                            "node_id": h.node_id,
                            "leader_node_id": h.leader_node_id,
                            "lease_epoch": h.lease_epoch,
                            "backend_lsn": h.backend_lsn,
                        }))
                    }
                }),
            )
            .route(
                "/health/sync",
                get(move || {
                    let h = health_sync.load_full();
                    async move {
                        axum::Json(serde_json::json!({
                            "node_id": h.node_id,
                            "sync_lsn": h.sync_lsn,
                            "backend_lsn": h.backend_lsn,
                            "lag": h.backend_lsn.saturating_sub(h.sync_lsn),
                        }))
                    }
                }),
            )
            .route(
                "/health/hydration",
                get(move || {
                    let h = health_hydration.load_full();
                    let org = ctx.visibility_ctx.org_id.to_string();
                    let version = ctx.cache.version(&org);
                    async move {
                        axum::Json(serde_json::json!({
                            "hydrated": h.hydrated,
                            "resident_orgs": ctx.cache.resident_orgs(),
                            "cache_backend_lsn": version.map(|v| v.backend_lsn).unwrap_or(0),
                            "backend_lsn": h.backend_lsn,
                        }))
                    }
                }),
            );

        let mut router = ops.merge(obs);
        if let Some(extra) = extra {
            router = router.merge(extra);
        }
        router
    }
}

/// Bearer-token check (R-Sec7): `Authorization: Bearer <token>` must match
/// exactly; anything else is a 401 with no detail.
async fn auth(req: Request<Body>, next: Next, bearer: String) -> Response<Body> {
    let ok = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| constant_time_eq(t.as_bytes(), bearer.as_bytes()));
    if !ok {
        metrics::counter!("exocortex_http_auth_failures_total").increment(1);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(req).await
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Build the route handler for one operation entry: JSON body in (POST) or
/// query params in (GET, coerced to typed JSON), `OpError` mapped to
/// status codes, JSON out.
fn op_route(
    entry: &'static OperationEntry,
    ctx: Arc<OpContext>,
) -> impl Clone
       + Fn(
    axum::extract::RawQuery,
    axum::body::Bytes,
) -> futures::future::BoxFuture<'static, Response<Body>>
       + Send
       + 'static {
    move |query: axum::extract::RawQuery, body: axum::body::Bytes| {
        let ctx = ctx.clone();
        Box::pin(async move {
            let input: serde_json::Value = if body.is_empty() {
                query_to_json(query.0.as_deref().unwrap_or(""))
            } else {
                match serde_json::from_slice(&body) {
                    Ok(v) => v,
                    Err(e) => {
                        return err_response(StatusCode::BAD_REQUEST, &e.to_string());
                    }
                }
            };
            metrics::counter!("exocortex_http_requests_total", "op" => entry.name).increment(1);
            match (entry.handler)(&ctx, input).await {
                Ok(out) => axum::Json(out).into_response(),
                Err(e) => match e {
                    OpError::BadInput(m) => err_response(StatusCode::BAD_REQUEST, &m),
                    OpError::Unauthorized(m) => err_response(StatusCode::FORBIDDEN, &m),
                    OpError::NotFound => err_response(StatusCode::NOT_FOUND, "not found"),
                    OpError::DeadlineExceeded => {
                        err_response(StatusCode::REQUEST_TIMEOUT, "deadline exceeded")
                    }
                    OpError::Storage(m) | OpError::Other(m) => {
                        err_response(StatusCode::INTERNAL_SERVER_ERROR, &m)
                    }
                },
            }
        })
    }
}

fn err_response(status: StatusCode, msg: &str) -> Response<Body> {
    (status, axum::Json(serde_json::json!({ "error": msg }))).into_response()
}

/// Query string → JSON object with best-effort scalar typing so
/// `serde_json::from_value` sees numbers where the input schema wants them.
fn query_to_json(q: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = percent_decode(v);
        let typed = if let Ok(n) = v.parse::<i64>() {
            serde_json::Value::from(n)
        } else if let Ok(f) = v.parse::<f64>() {
            serde_json::Value::from(f)
        } else if v == "true" || v == "false" {
            serde_json::Value::from(v == "true")
        } else {
            serde_json::Value::String(v)
        };
        map.insert(percent_decode(k), typed);
    }
    serde_json::Value::Object(map)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(b);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Install the process-wide Prometheus recorder once; subsequent binds
/// reuse the handle (double-install panics).
fn install_prometheus() -> metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
        std::sync::OnceLock::new();
    HANDLE
        .get_or_init(|| {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("install prometheus recorder")
        })
        .clone()
}
