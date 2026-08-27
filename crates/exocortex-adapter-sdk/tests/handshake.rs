//! R7: connect performs `Fingerprint → RegisterSource` before any submit
//! and fails on ceiling mismatch.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::{AdapterSession, SdkError};

#[tokio::test]
async fn empty_auth_token_fails_before_network_access() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(
        "http://127.0.0.1:1",
        dir.path().join("never-created.cursor"),
    );
    cfg.auth_token.clear();
    let error = match AdapterSession::connect(cfg).await {
        Err(error) => error,
        Ok(_) => panic!("empty credentials must fail closed"),
    };
    assert!(
        matches!(error, SdkError::InvalidUnit { ref detail } if detail.contains("auth_token")),
        "empty credentials must fail closed before attempting a backend connection: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_order_is_fingerprint_then_register() {
    let mock = exocortex_adapter_sdk::testing::MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    let session = AdapterSession::connect(cfg).await.unwrap();

    assert_eq!(session.fingerprint(), [7u8; 32]);
    assert_eq!(session.ceiling(), 3);
    assert!(session.cursor().is_none(), "fresh session has no cursor");

    // A submit records the full order.
    let mut session = session;
    session
        .submit_window(vec![common::unit("s1", &["k1", "k2"])], "cursor-1")
        .await
        .unwrap();
    let calls = mock.calls();
    mock.stop();
    assert_eq!(
        calls,
        vec![
            "fingerprint".to_string(),
            "register".to_string(),
            "submit".to_string()
        ],
        "R7: call order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ceiling_mismatch_fails_connect() {
    let mock = exocortex_adapter_sdk::testing::MockServer::start_with(vec![], 1, [7u8; 32]).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    let err = match AdapterSession::connect(cfg).await {
        Err(e) => e,
        Ok(_) => panic!("ceiling mismatch must fail connect"),
    };
    mock.stop();
    match err {
        SdkError::CeilingMismatch {
            configured,
            registered,
        } => {
            assert_eq!(configured, 3);
            assert_eq!(registered, 1);
        }
        other => panic!("expected CeilingMismatch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fingerprint_drift_surfaces_both_values() {
    use exocortex_adapter_sdk::testing::MockSubmit;
    let mock = exocortex_adapter_sdk::testing::MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    let mut session = AdapterSession::connect(cfg).await.unwrap();

    // Drift the backend, then make the server reject with
    // IncompatibleOntology (wire code 1) on the next submit.
    mock.drift_fingerprint([9u8; 32]);
    mock.push_script(vec![MockSubmit::RejectRows(1, "ontology drift (mock)")]);

    let err = session
        .submit_window(vec![common::unit("s1", &["k1"])], "cursor-1")
        .await
        .unwrap_err();
    mock.stop();
    match err {
        SdkError::FingerprintMismatch { expected, got } => {
            assert_eq!(expected, [7u8; 32]);
            assert_eq!(got, [9u8; 32]);
        }
        other => panic!("expected FingerprintMismatch, got {other:?}"),
    }
}
