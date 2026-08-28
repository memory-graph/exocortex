use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use exocortex_cluster::ClusterNode;
use exocortex_kernel::Visibility;
use exocortex_storage::{InMemoryStorage, VisibilityContext};
use tower::ServiceExt as _;

fn visibility(user: &str) -> VisibilityContext {
    VisibilityContext {
        user_id: user.into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    }
}

fn request(user: &str) -> Request<Body> {
    let mut request = Request::builder()
        .uri("/v1/changes?since_lsn=0")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer test-only-sse-{user}-token-000000000000"),
        )
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(visibility(user));
    request
}

#[tokio::test]
async fn live_sse_caps_release_global_and_principal_resources_on_cancellation() {
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(ontology.clone()));
    let cluster = Arc::new(ClusterNode::new(
        storage,
        "limit-test".into(),
        ontology.fingerprint,
        [7; 32],
    ));
    let app = exocortex_server::sse::sse_router_with_limits(cluster, 2, 1);

    let alice = app.clone().oneshot(request("alice")).await.unwrap();
    assert_eq!(alice.status(), StatusCode::OK);
    let duplicate = app.clone().oneshot(request("alice")).await.unwrap();
    assert_eq!(duplicate.status(), StatusCode::TOO_MANY_REQUESTS);

    let bob = app.clone().oneshot(request("bob")).await.unwrap();
    assert_eq!(bob.status(), StatusCode::OK);
    let global = app.clone().oneshot(request("carol")).await.unwrap();
    assert_eq!(global.status(), StatusCode::TOO_MANY_REQUESTS);

    drop(alice);
    let replacement = app.oneshot(request("carol")).await.unwrap();
    assert_eq!(replacement.status(), StatusCode::OK);
    drop(bob);
    drop(replacement);
}
