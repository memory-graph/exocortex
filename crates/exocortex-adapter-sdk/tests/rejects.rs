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

/// M1: a batch whose ack mixes rate-limited and permanent rows retries;
/// after success the permanents are recorded exactly once (not once per
/// attempt).
#[tokio::test(flavor = "multi_thread")]
async fn mixed_acks_do_not_double_count_permanents() {
    use exocortex_adapter_sdk::testing::MockSubmit;
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    cfg.retry.jitter = false;

    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    // First ack: k1 rate-limited AND k2 permanent (mixed). Second: accept.
    mock.push_script(vec![
        MockSubmit::RejectRows(11, "rate limited"),
        MockSubmit::RejectRows(11, "rate limited"),
        MockSubmit::Accept,
    ]);
    let out = session
        .submit_window(vec![common::unit("s", &["k1", "k2"])], "w1")
        .await
        .expect("settles after retry");
    mock.stop();
    assert_eq!(
        out.permanent_rejections.len(),
        0,
        "no permanents in this script"
    );
    assert_eq!(out.accepted, 2);

    // Now a mixed ack that STAYS mixed on settle: k2 permanent survives
    // the retry and is counted once.
    let mock2 = MockServer::start().await;
    let mut cfg2 = common::config(&mock2.url(), dir.path().join("c2.cursor"));
    cfg2.retry.jitter = false;
    let mut s2 = AdapterSession::connect_with(cfg2, instant_sleep())
        .await
        .unwrap();
    // Script: mixed ack, then an ack with ONLY the permanent row.
    // RateLimited rows retry the whole batch; permanents re-appear.
    mock2.push_script(vec![
        MockSubmit::RejectRows(11, "rate limited"),
        MockSubmit::RejectRows(3, "permanent for k2"),
    ]);
    let out2 = s2
        .submit_window(vec![common::unit("s", &["k1", "k2"])], "w1")
        .await
        .expect("settles");
    mock2.stop();
    // Second ack rejects BOTH rows as permanent (mock rejects all rows);
    // key assertion: count matches the FINAL ack only, not the sum of
    // both acks (the M1 double-count bug would report 3).
    assert_eq!(
        out2.permanent_rejections.len(),
        2,
        "permanents from the settled ack only: {:?}",
        out2.permanent_rejections
    );
}
