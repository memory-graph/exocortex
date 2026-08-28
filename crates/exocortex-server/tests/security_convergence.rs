use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use exocortex_ops::OpContext;
use exocortex_server::http_bind::HttpBind;
use exocortex_server::principal::PrincipalRegistry;
use exocortex_storage::VisibilityContext;
use tower::ServiceExt as _;

const TOKEN: &str = "test-only-audit-bearer-token-00000000";

fn context() -> (Arc<OpContext>, VisibilityContext) {
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
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
    (
        Arc::new(OpContext::per_request(
            visibility.clone(),
            storage,
            Arc::new(cache),
            chrono::Duration::seconds(30),
        )),
        visibility,
    )
}

fn audit_request() -> Request<Body> {
    Request::builder()
        .uri("/v1/audit?since_lsn=0")
        .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn embedded_audit_is_forbidden_by_default_and_explicit_for_admins() {
    let (ctx, visibility) = context();
    let ordinary = HttpBind::new(ctx.clone(), TOKEN.into())
        .router(None)
        .oneshot(audit_request())
        .await
        .unwrap();
    assert_eq!(ordinary.status(), StatusCode::FORBIDDEN);

    let principals = Arc::new(
        PrincipalRegistry::single_with_audit_admin(TOKEN.into(), visibility, true).unwrap(),
    );
    let admin = HttpBind::with_principals(ctx, principals)
        .router(None)
        .oneshot(audit_request())
        .await
        .unwrap();
    assert_eq!(admin.status(), StatusCode::OK);
}

#[cfg(unix)]
#[test]
fn plaintext_principal_policy_must_be_owner_only_before_parsing() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("principals.json");
    std::fs::write(&path, "not-json").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let error = PrincipalRegistry::load(&path).err().unwrap().to_string();
    assert!(error.contains("owner-only"), "{error}");

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let error = PrincipalRegistry::load(&path).err().unwrap().to_string();
    assert!(error.contains("parse principal policy"), "{error}");
}
