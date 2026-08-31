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

/// D21-b: `AdapterSession::preflight` sends the same split/stamped/signed
/// batches as a real submission (through the Preflight RPC), reports the
/// verdicts, and mutates NO session state — the cursor stays absent, and
/// a subsequent `submit_window` still works with the cursor advancing
/// from nothing. Hitting the declared window bound stops the dry run
/// before any wire traffic, naming the bound (A2).
#[tokio::test(flavor = "multi_thread")]
async fn preflight_sends_signed_batches_and_touches_no_state() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("cursor.json"));
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();

    let acks = session
        .preflight(vec![common::unit("dry-1", &["a", "b"])])
        .await
        .unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].accepted, 2);
    assert_eq!(acks[0].assigned_lsn, 0, "dry runs assign no LSN");

    let calls = mock.calls();
    assert!(calls.contains(&"preflight".to_string()), "{calls:?}");
    assert!(!calls.contains(&"submit".to_string()), "{calls:?}");
    // Session state untouched: no cursor yet.
    assert_eq!(session.cursor(), None);
    let preflighted = mock.preflighted();
    assert_eq!(preflighted.len(), 1);
    // The dry-run batch carried a real signature over a real checksum.
    assert!(
        exocortex_wire::signing::verify_signature(&[0u8; 32], &preflighted[0]),
        "preflight batches are signed like submits"
    );

    // The same units still submit cleanly afterwards: the dry run claimed
    // no batch id and advanced nothing.
    let outcome = session
        .submit_window(vec![common::unit("dry-1", &["a", "b"])], "c1")
        .await
        .unwrap();
    assert_eq!(outcome.accepted, 2);
    assert_eq!(session.cursor(), Some("c1"));
    mock.stop();
}

/// D21-b/A2: a dry run over the declared window bound stops before any
/// wire traffic and names the bound — the same pre-wire enforcement a
/// real submission gets.
#[tokio::test(flavor = "multi_thread")]
async fn preflight_over_the_window_bound_stops_before_wire() {
    let mock = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = table_config(
        &mock.url(),
        dir.path().join("cursor.json"),
        table_projection(2, 100),
    );
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();

    let err = session
        .preflight(vec![common::unit("big", &["a", "b", "c"])])
        .await
        .unwrap_err();
    match err {
        SdkError::ProjectionBoundExceeded {
            bound,
            value,
            declared,
        } => {
            assert_eq!(bound, "max_rows_per_window");
            assert_eq!(value, 3);
            assert_eq!(declared, 2);
        }
        other => panic!("expected the named bound, got {other:?}"),
    }
    assert!(
        !mock.calls().contains(&"preflight".to_string()),
        "no wire traffic: {:?}",
        mock.calls()
    );
    mock.stop();
}

/// D21-c: connect pulls the validation manifest; a manifest whose
/// fingerprint does not match the negotiated ontology is refused with a
/// warning and the session degrades to server-side validation (A3).
#[tokio::test(flavor = "multi_thread")]
async fn connect_holds_the_manifest_and_degrades_on_a_stale_one() {
    let mock = MockServer::start().await;
    mock.enable_manifest();
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("cursor.json"));
    let session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    assert!(
        session.manifest().is_some(),
        "the matching manifest is held"
    );
    assert!(mock.calls().contains(&"manifest".to_string()));

    mock.stop();
    let mock = MockServer::start().await;
    mock.serve_stale_manifest_fingerprint();
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("cursor.json"));
    let session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    assert!(
        session.manifest().is_none(),
        "a stale manifest is refused, not trusted"
    );
    mock.stop();
}

/// D21-c: with a manifest held, a mapping error stops the window LOCALLY
/// — before any wire traffic, cursor untouched — with the server's own
/// verdict vocabulary.
#[tokio::test(flavor = "multi_thread")]
async fn local_manifest_rejection_stops_the_window_before_the_wire() {
    let mock = MockServer::start().await;
    mock.enable_manifest();
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(&mock.url(), dir.path().join("cursor.json"));
    let mut session = exocortex_adapter_sdk::AdapterSession::connect_with(cfg, instant_sleep())
        .await
        .unwrap();
    assert!(session.manifest().is_some());

    // The mock's canned rulebook knows only "General"/"RelatedTo"; this
    // unit names a type the rulebook does not carry.
    let mut bad = common::unit("bad-1", &["a"]);
    bad.memories[0].memory_type = "NoSuchType".into();
    let err = session.submit_window(vec![bad], "c1").await.unwrap_err();
    match err {
        SdkError::LocalRejections { rejects } => {
            assert_eq!(rejects.len(), 1);
            assert_eq!(rejects[0].0, "a");
            assert_eq!(rejects[0].1, "UnknownMemoryType");
        }
        other => panic!("expected local rejections, got {other:?}"),
    }
    assert_eq!(session.cursor(), None, "cursor untouched");
    assert!(
        !mock.calls().contains(&"submit".to_string()),
        "no wire traffic: {:?}",
        mock.calls()
    );
    mock.stop();
}
