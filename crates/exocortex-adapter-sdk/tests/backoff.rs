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

/// C3: the R-I2 budget is verified against SIGNED+STAMPED bytes inside
/// submit_window — a unit that passes the split estimate but overshoots
/// after identity stamping and HMAC must still settle with every
/// emitted batch within the configured limit (the re-split loop), or
/// fail Unsplittable — never submit an over-budget batch.
#[tokio::test(flavor = "multi_thread")]
async fn signed_budget_is_enforced_not_estimated() {
    use exocortex_adapter_sdk::BatchUnit;
    use exocortex_wire::ingest::v1::MemoryDraft;

    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    // A budget that the estimate passes but stamping (~90 bytes of ids,
    // fingerprint, signature) pushes over: single small memory per unit.
    cfg.max_batch_bytes = 420;

    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    let mk = |k: &str| MemoryDraft {
        draft_key: k.into(),
        id: String::new(),
        memory_type: "General".into(),
        title: "t".repeat(120),
        content: "c".repeat(120),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    };
    // Build with sizeable drafts directly:
    let units: Vec<BatchUnit> = (0..4)
        .map(|i| BatchUnit {
            batch_id_seed: format!("u{i}"),
            memories: vec![mk(&format!("k{i}"))],
            relationships: vec![],
            snapshot: None,
            observed_at: std::time::UNIX_EPOCH,
        })
        .collect();

    let out = session.submit_window(units, "w1").await;
    let submitted = mock.submitted();
    mock.stop();
    match out {
        Ok(outcome) => {
            // Settled: every batch the server saw must be within budget.
            for b in &submitted {
                use prost::Message;
                let len = b.encoded_len();
                assert!(
                    len <= 420,
                    "submitted batch {} at {len} bytes exceeds the signed budget",
                    b.batch_id
                );
            }
            assert!(outcome.cursor_advanced);
        }
        Err(exocortex_adapter_sdk::SdkError::Unsplittable { .. }) => {
            // Legitimate: a single memory plus stamping cannot fit.
            for b in &submitted {
                use prost::Message;
                assert!(
                    b.encoded_len() <= 420,
                    "even on failure nothing over-budget was submitted"
                );
            }
        }
        Err(other) => panic!("expected settle or Unsplittable, got {other:?}"),
    }
}

/// C5: `Status::internal` (the server's transient-storage surface)
/// retries with backoff instead of aborting the session.
#[tokio::test(flavor = "multi_thread")]
async fn internal_storage_errors_retry() {
    use exocortex_adapter_sdk::testing::MockSubmit;
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    cfg.retry.max_attempts = 5;
    cfg.retry.jitter = false;

    let mut session = AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    mock.push_script(vec![
        MockSubmit::Fail(tonic::Code::Internal, "storage: transient blip"),
        MockSubmit::Fail(tonic::Code::Internal, "storage: transient blip"),
        MockSubmit::Accept,
    ]);
    let out = session
        .submit_window(vec![common::unit("s", &["k1"])], "w1")
        .await
        .expect("internal errors must not kill the session");
    let submitted = mock.submitted();
    mock.stop();
    assert_eq!(out.accepted, 1);
    assert_eq!(
        submitted.len(),
        3,
        "two internal failures retried, third accepted"
    );
}
