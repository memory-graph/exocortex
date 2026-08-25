//! CUJ-3: a permanent rejection surfaces as a typed outcome naming the
//! draft key — no panic, no infinite retry, no silent drop.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::testing::{unknown_memory_type, MockServer};
use exocortex_adapter_sdk::{instant_sleep, AdapterSession};

#[tokio::test(flavor = "multi_thread")]
async fn unknown_memory_type_is_permanent_and_named() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let cfg = common::config(&mock.url(), cursor.clone());
    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();

    mock.push_script(vec![unknown_memory_type()]);
    let out = session
        .submit_window(vec![common::unit("s", &["k1", "k2"])], "w1")
        .await
        .expect("permanent rejections settle, not error");
    let calls = mock.calls();
    mock.stop();

    assert_eq!(out.permanent_rejections.len(), 2);
    assert!(out.permanent_rejections.iter().all(|r| r.code == 3));
    let names: Vec<&str> = out
        .permanent_rejections
        .iter()
        .map(|r| r.draft_key.as_str())
        .collect();
    assert_eq!(names, vec!["k1", "k2"], "each row names its draft_key");
    // The window still settled (permanent != transient): cursor advanced.
    assert!(out.cursor_advanced);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "w1");
    // And exactly one submit happened — no retry of a permanent code.
    assert_eq!(
        calls.iter().filter(|c| *c == "submit").count(),
        1,
        "permanent rejections are not retried"
    );
}
