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
use axum::extract::Extension;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;

use exocortex_ops::{entries, OpContext, OpError, OperationEntry};
#[cfg(test)]
use exocortex_storage::VisibilityContext;

use crate::principal::{AuthenticatedPrincipal, PrincipalRegistry};

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
    /// True while the storage backend answers pings (R-O4: storage
    /// reachable). Maintained by a background probe.
    pub storage_ok: bool,
    /// When the lease re-election loop last ticked (R-O4: cluster
    /// membership stable; stale means the owner loop is stuck).
    pub last_lease_tick: Option<chrono::DateTime<chrono::Utc>>,
    /// True while the reasoning worker loop is consuming (R-O4).
    pub reasoning_alive: bool,
    /// True only while the cluster invalidation subscription is live.
    pub cluster_feed_ready: bool,
    /// Monotonic subscription epoch, incremented after each reconnect.
    pub cluster_feed_epoch: u64,
    /// Total observed cluster-feed failures and clean terminations.
    pub cluster_feed_failures: u64,
}

fn hex64(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The HTTP binding: operation routes + auth + observability.
pub struct HttpBind {
    ctx: Arc<OpContext>,
    principals: Arc<PrincipalRegistry>,
    health: Arc<arc_swap::ArcSwap<HealthSnapshot>>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
}

impl HttpBind {
    /// Build a least-privilege embedded single-principal binding. The bearer
    /// protects every operation, SSE, metrics, and detailed health route, but
    /// does not grant audit administration; callers needing that capability
    /// must use [`Self::with_principals`] with an explicit policy.
    pub fn new(ctx: Arc<OpContext>, bearer: String) -> Self {
        let principals = PrincipalRegistry::single(bearer, ctx.visibility_ctx.clone())
            .expect("HttpBind bearer must contain at least 32 bytes");
        Self::with_principals(ctx, Arc::new(principals))
    }

    /// Build a binding backed by an immutable administrator principal policy.
    pub fn with_principals(ctx: Arc<OpContext>, principals: Arc<PrincipalRegistry>) -> Self {
        Self {
            ctx,
            principals,
            // Readiness defaults are optimistic for standalone mounts
            // (embedded/library use); `run_backend_node` installs the
            // maintainers that make these fields observational (R-O4).
            health: Arc::new(arc_swap::ArcSwap::from(Arc::new(HealthSnapshot {
                node_id: "exocortex-node".into(),
                hydrated: true,
                storage_ok: true,
                reasoning_alive: true,
                cluster_feed_ready: true,
                last_lease_tick: Some(chrono::Utc::now()),
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
    /// `(method, path)`, plus any `extra` router (the SSE change feed),
    /// auth-gated together (R-Sec7 / audit CS1: the feed was previously
    /// merged in AFTER the auth layer, leaving `/v1/changes` unauthenticated),
    /// then the observability endpoints.
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
        // The bearer layer covers operations, SSE, the explorer
        // (PX5), metrics, and detailed health. Only a minimal
        // ready/not-ready probe is public.
        let mut protected = ops.merge(crate::explorer::router(self.ctx.clone()));
        if let Some(extra) = extra {
            protected = protected.merge(extra);
        }
        let prom = self.prometheus.clone();
        protected = protected.route(
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
        );
        let health_cluster = self.health.clone();
        let health_sync = self.health.clone();
        let health_hydration = self.health.clone();
        let identity = self.ctx.ontology.clone();
        let ctx = self.ctx.clone();
        protected = protected
            .route(
                "/health/cluster",
                get(move || {
                    let h = health_cluster.load_full();
                    let identity = identity.clone();
                    async move {
                        // OC-PRD D1: the compatibility fingerprint gates;
                        // the build fingerprint reports (diagnostics
                        // surface for "same binary?" questions).
                        let (compat, build) = match identity
                            .as_ref()
                            .map(|o| (hex64(&o.fingerprint.0), hex64(&o.build_fingerprint.0)))
                        {
                            Some((c, b)) => {
                                (serde_json::Value::String(c), serde_json::Value::String(b))
                            }
                            None => (serde_json::Value::Null, serde_json::Value::Null),
                        };
                        axum::Json(serde_json::json!({
                            "node_id": h.node_id,
                            "leader_node_id": h.leader_node_id,
                            "lease_epoch": h.lease_epoch,
                            "backend_lsn": h.backend_lsn,
                            "compatibility_fingerprint": compat,
                            "build_fingerprint": build,
                            "feed_ready": h.cluster_feed_ready,
                            "feed_epoch": h.cluster_feed_epoch,
                            "feed_failures": h.cluster_feed_failures,
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
        let principals = self.principals.clone();
        let protected = protected.layer(middleware::from_fn(move |req, next| {
            auth(req, next, principals.clone())
        }));

        let health_ready = self.health.clone();
        let readiness = Router::new().route(
            "/health/ready",
            get(move || {
                let h = health_ready.load_full();
                async move {
                    // R-O4: public probes learn only ready/not-ready. Detailed
                    // subsystem state remains behind bearer authentication.
                    let lease_fresh = h
                        .last_lease_tick
                        .is_some_and(|t| (chrono::Utc::now() - t).num_seconds() < 15);
                    let ready = h.hydrated
                        && h.storage_ok
                        && h.reasoning_alive
                        && h.cluster_feed_ready
                        && lease_fresh;
                    let status = if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    };
                    (
                        status,
                        axum::Json(serde_json::json!({
                            "status": if ready { "ready" } else { "not-ready" },
                        })),
                    )
                }
            }),
        );

        protected.merge(readiness)
    }
}

/// Bearer-token check (R-Sec7): `Authorization: Bearer <token>` must match
/// exactly; anything else is a 401 with no detail.
async fn auth(
    mut req: Request<Body>,
    next: Next,
    principals: Arc<PrincipalRegistry>,
) -> Response<Body> {
    let principal = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|token| principals.authenticate(token.as_bytes()));
    let Some(principal) = principal else {
        metrics::counter!("exocortex_http_auth_failures_total").increment(1);
        return StatusCode::UNAUTHORIZED.into_response();
    };
    req.extensions_mut().insert(principal.visibility.clone());
    req.extensions_mut().insert(principal);
    next.run(req).await
}

/// Build the route handler for one operation entry: JSON body in (POST) or
/// query params in (GET, coerced to typed JSON), `OpError` mapped to
/// status codes, JSON out.
fn op_route(
    entry: &'static OperationEntry,
    ctx: Arc<OpContext>,
) -> impl Clone
       + Fn(
    Extension<AuthenticatedPrincipal>,
    axum::extract::RawQuery,
    axum::body::Bytes,
) -> futures::future::BoxFuture<'static, Response<Body>>
       + Send
       + 'static {
    move |Extension(principal): Extension<AuthenticatedPrincipal>,
          query: axum::extract::RawQuery,
          body: axum::body::Bytes| {
        // IN11 (audit): every request gets its OWN budget. The shared
        // startup context gave request #2 an already-expired deadline (and
        // nothing read it, so the REQUEST_TIMEOUT mapping was unreachable).
        let ctx = Arc::new(OpContext {
            visibility_ctx: principal.visibility,
            audit_admin: principal.audit_admin,
            deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
            ..(*ctx).clone()
        });
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
            let started = std::time::Instant::now();
            let out = (entry.handler)(entry, &ctx, input).await;
            metrics::histogram!("exocortex_ops_duration_seconds", "op" => entry.name)
                .record(started.elapsed().as_secs_f64());
            match out {
                Ok(out) => axum::Json(out).into_response(),
                Err(e) => match e {
                    OpError::BadInput(m) => err_response(StatusCode::BAD_REQUEST, &m),
                    OpError::Unauthorized(m) => err_response(StatusCode::FORBIDDEN, &m),
                    OpError::NotFound => err_response(StatusCode::NOT_FOUND, "not found"),
                    OpError::DeadlineExceeded => {
                        err_response(StatusCode::REQUEST_TIMEOUT, "deadline exceeded")
                    }
                    OpError::Storage(m) | OpError::Other(m) => internal_error_response(&m),
                },
            }
        })
    }
}

fn internal_error_response(detail: &str) -> Response<Body> {
    // Preserve diagnostics in server-controlled telemetry, but never reflect
    // backend URLs, query text, credentials, or dependency messages to HTTP.
    tracing::error!(error = %detail, "HTTP operation failed");
    err_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
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

/// Install the process-wide metric recorders once (D6): Prometheus
/// always, plus the OTLP fanout leg when compiled in and configured —
/// see `telemetry::install`. Subsequent binds reuse the handle
/// (double-install panics).
fn install_prometheus() -> metrics_exporter_prometheus::PrometheusHandle {
    crate::telemetry::install()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    const HTTP_STORAGE_SENTINEL: &str = "redis://credential-sentinel@example.invalid/private";

    fn sentinel_method() -> http::Method {
        http::Method::POST
    }

    fn sentinel_storage_error(
        _entry: &'static exocortex_ops::OperationEntry,
        _ctx: &OpContext,
        _input: serde_json::Value,
    ) -> futures::future::BoxFuture<'static, Result<serde_json::Value, OpError>> {
        Box::pin(async { Err(OpError::Storage(HTTP_STORAGE_SENTINEL.into())) })
    }

    #[tokio::test]
    async fn embedded_audit_requires_an_explicit_admin_principal() {
        const TOKEN: &str = "test-only-audit-bearer-token-00000000";
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(exocortex_storage::InMemoryStorage::new(ontology));
        let (cache, _writer) = exocortex_cache::LocalCache::new(1024 * 1024);
        let visibility = VisibilityContext {
            user_id: "user".into(),
            org_id: "org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        };
        let ctx = Arc::new(OpContext::per_request(
            visibility.clone(),
            storage,
            Arc::new(cache),
            chrono::Duration::seconds(30),
        ));
        let request = || {
            Request::builder()
                .uri("/v1/audit?since_lsn=0")
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap()
        };

        let ordinary = HttpBind::new(ctx.clone(), TOKEN.into())
            .router(None)
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(ordinary.status(), StatusCode::FORBIDDEN);

        let principals = Arc::new(
            PrincipalRegistry::single_with_audit_admin(TOKEN.into(), visibility, true).unwrap(),
        );
        let admin = HttpBind::with_principals(ctx, principals)
            .router(None)
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(admin.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_http_error_never_reflects_dependency_detail() {
        let response = internal_error_response(HTTP_STORAGE_SENTINEL);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"internal error"}"#);
        assert!(!body
            .windows(HTTP_STORAGE_SENTINEL.len())
            .any(|window| window == HTTP_STORAGE_SENTINEL.as_bytes()));
    }

    #[tokio::test]
    async fn authenticated_http_operation_never_reflects_storage_error_detail() {
        const TOKEN: &str = "test-only-sentinel-bearer-token-00000000";
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(exocortex_storage::InMemoryStorage::new(ontology));
        let (cache, _writer) = exocortex_cache::LocalCache::new(1024 * 1024);
        let visibility = VisibilityContext {
            user_id: "user".into(),
            org_id: "org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        };
        let ctx = Arc::new(OpContext::per_request(
            visibility,
            storage,
            Arc::new(cache),
            chrono::Duration::seconds(30),
        ));
        let schema_source = entries()
            .into_iter()
            .next()
            .expect("operation registry is populated");
        let sentinel_operation = Box::leak(Box::new(OperationEntry {
            name: "sentinel_storage_error",
            mcp_tool_name: "exocortex.test_sentinel_storage_error",
            http_method: sentinel_method,
            http_path: "/v1/test/sentinel-storage-error",
            input_schema: schema_source.input_schema,
            output_schema: schema_source.output_schema,
            pack: None,
            handler: sentinel_storage_error,
        }));
        let extra = Router::new().route(
            sentinel_operation.http_path,
            axum::routing::post(op_route(sentinel_operation, ctx.clone())),
        );
        let response = HttpBind::new(ctx, TOKEN.into())
            .router(Some(extra))
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri(sentinel_operation.http_path)
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"internal error"}"#);
        assert!(!body
            .windows(HTTP_STORAGE_SENTINEL.len())
            .any(|window| window == HTTP_STORAGE_SENTINEL.as_bytes()));
    }

    #[tokio::test]
    async fn bearer_auth_injects_credential_specific_principal() {
        let ontology = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(exocortex_storage::InMemoryStorage::new(ontology));
        let (cache, _writer) = exocortex_cache::LocalCache::new(1024 * 1024);
        let startup = VisibilityContext {
            user_id: "startup-admin".into(),
            org_id: "org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        };
        let ctx = Arc::new(OpContext::per_request(
            startup,
            storage,
            Arc::new(cache),
            chrono::Duration::seconds(30),
        ));
        let bob = VisibilityContext {
            user_id: "bob".into(),
            org_id: "org".into(),
            project_ids: ["project-b".into()].into_iter().collect(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Project,
        };
        let principals = Arc::new(
            PrincipalRegistry::single_with_audit_admin(
                "test-only-bob-bearer-token-00000000".into(),
                bob,
                false,
            )
            .unwrap(),
        );
        let who = Router::new().route(
            "/who",
            get(
                |Extension(principal): Extension<VisibilityContext>| async move {
                    principal.user_id.to_string()
                },
            ),
        );
        let app = HttpBind::with_principals(ctx, principals).router(Some(who));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/who")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer test-only-bob-bearer-token-00000000",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"bob");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/audit?since_lsn=0")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer test-only-bob-bearer-token-00000000",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
