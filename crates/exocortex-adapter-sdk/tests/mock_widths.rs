//! §18.6 width enforcement in the mock (B8/B9): a batch whose snapshot
//! `schema_hash` is not 32 bytes, or whose external keys carry a
//! non-16-byte `table_uuid`, is rejected with INVALID_EXTERNAL_KEY
//! under every scripted action — the same verdict a real backend
//! returns. This is the PX4 defect class (the reference adapter's
//! missing bearer auth) caught in an adapter's OWN tests instead of
//! its first live run.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{instant_sleep, SdkError};
use exocortex_wire::ingest::v1::{ExternalKey, ExternalSnapshotInfo};

fn snap_unit(
    seed: &str,
    schema_hash: Vec<u8>,
    table_uuid: Vec<u8>,
) -> exocortex_adapter_sdk::BatchUnit {
    let mut unit = common::unit(seed, &["k1"]);
    unit.snapshot = Some(ExternalSnapshotInfo {
        snapshot_id: "snap-1".into(),
        schema_hash,
        source_flavor: "custom".into(),
    });
    unit.memories[0].external_key = Some(ExternalKey {
        table_uuid,
        logical_pk: "pk-1".into(),
        mapping_version: 1,
    });
    unit
}

/// A 16-byte snapshot schema_hash (the exact shape the git adapter
/// shipped before its fix) is rejected code 13 even though the script
/// says Accept.
#[tokio::test(flavor = "multi_thread")]
async fn short_schema_hash_is_rejected_under_accept() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(
        common::config(&mock.url(), dir.path().join("c.cursor")),
        instant_sleep(),
    )
    .await
    .unwrap();
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session
        .submit_window(
            vec![snap_unit("a", vec![7u8; 16], vec![1u8; 16])],
            "cursor-1",
        )
        .await
        .unwrap();
    assert_eq!(outcome.accepted, 0);
    assert_eq!(outcome.permanent_rejections.len(), 1);
    assert_eq!(outcome.permanent_rejections[0].code, 13);
    assert!(outcome.permanent_rejections[0]
        .detail
        .contains("schema_hash must be 32 bytes"));
    mock.stop();
}

/// A short external-key table_uuid is the same class of rejection.
#[tokio::test(flavor = "multi_thread")]
async fn short_table_uuid_is_rejected_under_accept() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(
        common::config(&mock.url(), dir.path().join("c.cursor")),
        instant_sleep(),
    )
    .await
    .unwrap();
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session
        .submit_window(
            vec![snap_unit("a", vec![7u8; 32], vec![1u8; 8])],
            "cursor-1",
        )
        .await
        .unwrap();
    assert_eq!(outcome.accepted, 0);
    assert_eq!(outcome.permanent_rejections[0].code, 13);
    assert!(outcome.permanent_rejections[0]
        .detail
        .contains("table_uuid must be 16 bytes"));
    mock.stop();
}

/// Well-formed 32/16-byte coordinates pass the enforcement untouched
/// (the scripted Accept still accepts).
#[tokio::test(flavor = "multi_thread")]
async fn well_formed_coordinates_pass() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(
        common::config(&mock.url(), dir.path().join("c.cursor")),
        instant_sleep(),
    )
    .await
    .unwrap();
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session
        .submit_window(
            vec![snap_unit("a", vec![7u8; 32], vec![1u8; 16])],
            "cursor-1",
        )
        .await
        .unwrap();
    assert_eq!(outcome.accepted, 1);
    assert!(outcome.permanent_rejections.is_empty());
    mock.stop();
}

/// D21-a: registering a table-shaped flavor without a declared
/// projection fails at connect — the mock refuses the registration the
/// same way the real server does.
#[tokio::test(flavor = "multi_thread")]
async fn table_flavor_registration_requires_a_projection() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(&mock.url(), dir.path().join("c.cursor"));
    cfg.source_flavor = "parquet-dir".into();
    let err = match exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep()).await
    {
        Err(err) => err,
        Ok(_) => panic!("registration without a projection must fail at connect"),
    };
    match err {
        SdkError::Transport(status) => {
            let detail = status.message();
            assert!(
                detail.contains("requires a declared projection"),
                "{detail}"
            );
        }
        other => panic!("expected a transport-level registration refusal, got {other:?}"),
    }
    mock.stop();
}
