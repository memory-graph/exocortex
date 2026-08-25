//! R12: the durable cursor advances only after the whole window settles;
//! a transient failure leaves it byte-identical, and a restart replays
//! idempotently.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{instant_sleep, AdapterSession};

#[tokio::test(flavor = "multi_thread")]
async fn transient_failure_leaves_cursor_untouched() {
    // Window of 3 batches; batch 3 fails at transport level, retries
    // exhaust (max_attempts = 2 for speed).
    let cfg_retry = exocortex_adapter_sdk::RetryPolicy {
        max_attempts: 2,
        jitter: false,
        ..Default::default()
    };

    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut cfg = common::config(&mock.url(), cursor.clone());
    cfg.max_batch_bytes = 600; // force ~1 batch per small unit
    cfg.retry = cfg_retry;

    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    // Pre-seed a settled cursor value to observe byte-stability.
    std::fs::write(&cursor, "cursor-0").unwrap();
    let before = std::fs::read(&cursor).unwrap();

    mock.push_script(vec![
        MockSubmit::Accept,
        MockSubmit::Accept,
        MockSubmit::Fail(tonic::Code::Unavailable, "boom"),
        MockSubmit::Fail(tonic::Code::Unavailable, "boom"),
    ]);

    let units = vec![
        common::unit("a", &["k1", "k2", "k3", "k4"]),
        common::unit("b", &["k5", "k6", "k7", "k8"]),
        common::unit("c", &["k9", "k10", "k11", "k12"]),
    ];
    let err = match session.submit_window(units, "cursor-1").await {
        Err(e) => e,
        Ok(_) => panic!("transient failure must abort the window"),
    };
    mock.stop();
    assert!(
        matches!(
            err,
            exocortex_adapter_sdk::SdkError::RetriesExhausted { attempts: 2 }
        ),
        "got {err:?}"
    );
    assert_eq!(
        std::fs::read(&cursor).unwrap(),
        before,
        "R12: transient failure leaves the on-disk cursor byte-identical"
    );

    // Restart: batches 1-2 were accepted; replaying the window from the
    // same cursor returns DUPLICATE_BATCH (success) and settles.
    let mock2 = MockServer::start().await;
    let mut cfg2 = common::config(&mock2.url(), cursor.clone());
    cfg2.max_batch_bytes = 600;
    let mut session2 = AdapterSession::connect_with(cfg2, instant_sleep())
        .await
        .unwrap();
    mock2.push_script(vec![
        MockSubmit::RejectRows(8, "idempotent replay (mock)"),
        MockSubmit::RejectRows(8, "idempotent replay (mock)"),
        MockSubmit::Accept,
    ]);
    let out = session2
        .submit_window(
            vec![
                common::unit("a", &["k1", "k2", "k3", "k4"]),
                common::unit("b", &["k5", "k6", "k7", "k8"]),
                common::unit("c", &["k9", "k10", "k11", "k12"]),
            ],
            "cursor-1",
        )
        .await
        .unwrap();
    mock2.stop();
    assert_eq!(out.duplicates, 2, "replayed batches read as duplicates");
    assert!(out.cursor_advanced);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "cursor-1");
    assert_eq!(session2.cursor(), Some("cursor-1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cursor_advances_exactly_once_on_success() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let cfg = common::config(&mock.url(), cursor.clone());
    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    let out = session
        .submit_window(vec![common::unit("s", &["k1", "k2"])], "w1")
        .await
        .unwrap();
    mock.stop();
    assert_eq!(out.accepted, 2);
    assert!(out.cursor_advanced);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "w1");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_cursor_is_no_cursor_corrupt_is_hard_error() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.cursor");
    let cfg = common::config(&mock.url(), missing.clone());
    let session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    assert!(session.cursor().is_none(), "missing file = no cursor");

    let corrupt = dir.path().join("corrupt.cursor");
    std::fs::write(&corrupt, b"\xff\xfe not utf8").unwrap();
    let cfg2 = common::config(&mock.url(), corrupt);
    let err = match AdapterSession::connect_with(cfg2, instant_sleep()).await {
        Err(e) => e,
        Ok(_) => panic!("corrupt cursor file must fail connect"),
    };
    mock.stop();
    // A non-UTF8 cursor is a hard error (a silent reset would re-ingest
    // the world) — surfaced as CursorCorrupt via io, never Ok(None).
    assert!(
        !matches!(err, exocortex_adapter_sdk::SdkError::TransportConnect(_)),
        "corrupt cursor must not look like a transport failure: {err:?}"
    );
}
