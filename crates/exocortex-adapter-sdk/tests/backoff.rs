//! CUJ-4: RATE_LIMITED triggers exponential backoff; the cursor does not
//! advance while backing off; delays come from the injected sleep (no
//! wall-clock sleeping in tests).

#![cfg(feature = "testing")]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use exocortex_adapter_sdk::testing::{rate_limited, MockServer, MockSubmit};
use exocortex_adapter_sdk::{instant_sleep, AdapterSession, SleepFn};

#[tokio::test(flavor = "multi_thread")]
async fn rate_limit_backs_off_then_succeeds() {
    // Record every delay the session asks to sleep.
    let delays: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = delays.clone();
    let sleep: SleepFn = Arc::new(move |d| {
        seen.lock().unwrap().push(d);
        Box::pin(std::future::ready(()))
    });

    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut cfg = common::config(&mock.url(), cursor.clone());
    cfg.retry.jitter = false; // deterministic delays
    cfg.retry.base = Duration::from_millis(100);

    let mut session = AdapterSession::connect_with(cfg, sleep).await.unwrap();
    mock.push_script(vec![
        rate_limited(),
        rate_limited(),
        rate_limited(),
        MockSubmit::Accept,
    ]);
    let out = session
        .submit_window(vec![common::unit("s", &["k1"])], "w1")
        .await
        .expect("succeeds on the 4th attempt");
    mock.stop();

    assert_eq!(out.accepted, 1);
    assert!(out.cursor_advanced, "cursor advances after success");
    let ds = delays.lock().unwrap().clone();
    assert_eq!(ds.len(), 3, "three backoff sleeps");
    assert!(
        ds.windows(2).all(|w| w[1] > w[0]),
        "strictly increasing delays: {ds:?}"
    );
    assert_eq!(ds[0], Duration::from_millis(100), "base delay first");
}

#[tokio::test(flavor = "multi_thread")]
async fn exhausted_retries_do_not_advance_the_cursor() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let mut cfg = common::config(&mock.url(), cursor.clone());
    cfg.retry.max_attempts = 3;
    cfg.retry.jitter = false;
    std::fs::write(&cursor, "w0").unwrap();

    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    mock.push_script(vec![
        rate_limited(),
        rate_limited(),
        rate_limited(),
        rate_limited(), // 4th: script empty → would accept; attempts already out
    ]);
    let err = session
        .submit_window(vec![common::unit("s", &["k1"])], "w1")
        .await
        .unwrap_err();
    mock.stop();
    assert!(matches!(
        err,
        exocortex_adapter_sdk::SdkError::RetriesExhausted { attempts: 3 }
    ));
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap(),
        "w0",
        "cursor unchanged while backing off / exhausted"
    );
}
