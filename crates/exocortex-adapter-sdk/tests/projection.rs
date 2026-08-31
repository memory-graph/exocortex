//! D21-a (adapter-contract PRD §3.1, SDK side): the declared projection
//! rides registration, and the declared bounds stop the window — cursor
//! untouched, bound named — instead of letting a table silently dominate
//! the graph. Rewind detection (D21-d) errors before any submission.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{
    instant_sleep, Projection, ProjectionBounds, ProjectionField, SdkError, SourceColumn,
};

fn table_projection(window: u64, run: u64) -> Projection {
    Projection {
        selector: "table:events where kind='fix'".into(),
        fields: vec![ProjectionField {
            source_field: "fix_title".into(),
            memory_type: "Fix".into(),
            kind: String::new(),
        }],
        source_schema: vec![
            SourceColumn {
                name: "fix_title".into(),
                data_type: "string".into(),
            },
            SourceColumn {
                name: "created_at".into(),
                data_type: "timestamp".into(),
            },
        ],
        mapping_version: 1,
        bounds: ProjectionBounds {
            max_rows_per_window: window,
            max_rows_per_run: run,
            max_graph_share_percent: 25,
        },
        last_snapshot_id: "snap-1".into(),
    }
}

fn table_config(
    url: &str,
    cursor: std::path::PathBuf,
    projection: Projection,
) -> exocortex_adapter_sdk::AdapterConfig {
    let mut cfg = common::config(url, cursor);
    cfg.source_flavor = "iceberg".into();
    cfg.projection = Some(projection);
    cfg
}

/// The registration the server receives carries the declared projection
/// verbatim (selector, fields, schema, bounds, mapping version).
#[tokio::test(flavor = "multi_thread")]
async fn projection_rides_registration() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = table_config(
        &mock.url(),
        dir.path().join("c.cursor"),
        table_projection(10, 100),
    );
    let session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    let registrations = mock.registrations();
    drop(session);
    mock.stop();
    let projection = registrations[0]
        .projection
        .as_ref()
        .expect("projection on the wire registration");
    assert_eq!(projection.selector, "table:events where kind='fix'");
    assert_eq!(projection.fields[0].source_field, "fix_title");
    assert_eq!(projection.source_schema.len(), 2);
    assert_eq!(projection.mapping_version, 1);
    assert_eq!(projection.bounds.as_ref().unwrap().max_rows_per_window, 10);
}

/// Exceeding `max_rows_per_window` stops the window BEFORE any wire
/// traffic (A2/A3): no submit reaches the server, the cursor file keeps
/// its prior bytes, and the error names the bound.
#[tokio::test(flavor = "multi_thread")]
async fn window_bound_stops_before_wire_traffic() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    std::fs::write(&cursor, "cursor-0").unwrap();
    let cfg = table_config(&mock.url(), cursor.clone(), table_projection(2, 100));
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();

    let units = vec![common::unit("a", &["k1", "k2", "k3"])];
    let err = session.submit_window(units, "cursor-1").await.unwrap_err();
    let calls = mock.calls();
    match err {
        SdkError::ProjectionBoundExceeded {
            bound,
            value,
            declared,
        } => {
            assert_eq!(bound, "max_rows_per_window");
            assert_eq!((value, declared), (3, 2));
        }
        other => panic!("expected the bound error, got {other:?}"),
    }
    assert!(
        !calls.contains(&"submit".to_string()),
        "no wire traffic for an over-bound window"
    );
    assert_eq!(std::fs::read(&cursor).unwrap(), b"cursor-0");
}

/// The per-RUN bound accumulates across settled windows: the second
/// window that would cross it fails with the same named-bound error,
/// leaving the cursor at the first window's value.
#[tokio::test(flavor = "multi_thread")]
async fn run_bound_accumulates_across_windows() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let cfg = table_config(&mock.url(), cursor.clone(), table_projection(2, 3));
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);

    let first = session
        .submit_window(vec![common::unit("a", &["k1", "k2"])], "cursor-1")
        .await
        .unwrap();
    assert!(first.cursor_advanced);

    let err = session
        .submit_window(vec![common::unit("b", &["k3", "k4"])], "cursor-2")
        .await
        .unwrap_err();
    match err {
        SdkError::ProjectionBoundExceeded {
            bound,
            value,
            declared,
        } => {
            assert_eq!(bound, "max_rows_per_run");
            assert_eq!((value, declared), (4, 3));
        }
        other => panic!("expected the run-bound error, got {other:?}"),
    }
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "cursor-1");
}

/// A window naming an already-superseded snapshot is a REWIND (D21-d):
/// the SDK errors before submitting and the operator-facing error names
/// both snapshots.
#[tokio::test(flavor = "multi_thread")]
async fn rewound_snapshot_errors_before_submission() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cursor = dir.path().join("c.cursor");
    let cfg = table_config(&mock.url(), cursor.clone(), table_projection(10, 100));
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);

    let snap = |id: &str, seed: &str, keys: &[&str]| {
        let mut unit = common::unit(seed, keys);
        unit.snapshot = Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
            snapshot_id: id.into(),
            schema_hash: vec![0u8; 32],
            source_flavor: "iceberg".into(),
        });
        for (i, memory) in unit.memories.iter_mut().enumerate() {
            memory.external_key = Some(exocortex_wire::ingest::v1::ExternalKey {
                table_uuid: vec![1u8; 16],
                logical_pk: format!("{seed}-{i}"),
                mapping_version: 1,
            });
        }
        unit
    };
    session
        .submit_window(vec![snap("snap-1", "a", &["k1"])], "cursor-1")
        .await
        .unwrap();
    session
        .submit_window(vec![snap("snap-2", "b", &["k2"])], "cursor-2")
        .await
        .unwrap();

    let submits_before = mock.submitted().len();
    let err = session
        .submit_window(vec![snap("snap-1", "c", &["k3"])], "cursor-3")
        .await
        .unwrap_err();
    match err {
        SdkError::SourceRewound { observed, last } => {
            assert_eq!((observed.as_str(), last.as_str()), ("snap-1", "snap-2"));
        }
        other => panic!("expected the rewind error, got {other:?}"),
    }
    assert_eq!(mock.submitted().len(), submits_before);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "cursor-2");
}
