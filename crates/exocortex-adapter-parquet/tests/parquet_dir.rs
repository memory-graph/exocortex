//! D1 integration: a fixture directory of real Parquet files (written
//! with the parquet crate's own ArrowWriter — offline, deterministic)
//! flows through the mapper and the SDK mock server end to end:
//! registration carries the declared projection under the
//! `parquet-dir` TABLE flavor (the mock refuses it otherwise), every
//! row lands with identity-stable external keys, the snapshot schema
//! hash is the canonical wire digest over the declared columns, an
//! unchanged file set is a no-op, a changed directory is a new
//! snapshot, and a reverted directory is a rewind.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use exocortex_adapter_parquet::{
    declared_columns, map_rows, projection, read_rows, scan_directory, table_uuid_for,
    validate_mapping, with_snapshot_id, Mapping,
};
use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{AdapterSession, SdkError};
use parquet::arrow::ArrowWriter;

fn mapping() -> Mapping {
    serde_json::from_str(
        r#"{
            "memory_type": "Problem",
            "title_column": "title",
            "content_columns": ["detail", "severity"],
            "pk_columns": ["id"],
            "tags_column": "tags",
            "parent_column": "parent_id",
            "parent_kind": "Causes",
            "mapping_version": 1
        }"#,
    )
    .unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("detail", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
    ])
}

/// One fixture row: (id, title?, detail, severity, tags, parent?).
type FixtureRow = (
    i64,
    Option<&'static str>,
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
);

/// One fixture file: rows given as column tuples. Fixed values only —
/// the mapped output is byte-stable across runs.
fn write_file(path: &std::path::Path, rows: &[FixtureRow]) {
    let ids: Int64Array = rows.iter().map(|row| Some(row.0)).collect();
    let titles: StringArray = rows.iter().map(|row| row.1.map(str::to_string)).collect();
    let details: StringArray = rows.iter().map(|row| Some(row.2.to_string())).collect();
    let severities: StringArray = rows.iter().map(|row| Some(row.3.to_string())).collect();
    let tags: StringArray = rows.iter().map(|row| Some(row.4.to_string())).collect();
    let parents: StringArray = rows.iter().map(|row| row.5.map(str::to_string)).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(ids),
            Arc::new(titles),
            Arc::new(details),
            Arc::new(severities),
            Arc::new(tags),
            Arc::new(parents),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Two files: the first carries r-1 (the parent) and a spare row; the
/// second carries r-2 (parented at r-1 across FILES — proving
/// cross-file edges work within one window) and an untitled row
/// (title falls back to the pk). The null-pk case has its own fixture.
fn fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    write_file(
        &dir.path().join("a.parquet"),
        &[
            (
                1,
                Some("first finding"),
                "drain lost entries",
                "high",
                "backend, wal",
                None,
            ),
            (7, Some("spare row"), "identity present", "low", "", None),
        ],
    );
    write_file(
        &dir.path().join("b.parquet"),
        &[
            (
                2,
                Some("second finding"),
                "cursor stalled",
                "medium",
                "ingest",
                Some("1"),
            ),
            (3, None, "untitled row", "low", "", None),
        ],
    );
    dir
}

/// A fixture whose second row carries a NULL id cell: a row without
/// identity cannot join, so it is skipped and counted.
fn fixture_dir_with_null_pk() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let ids: Int64Array = Vec::<Option<i64>>::from([Some(1), None])
        .into_iter()
        .collect();
    let titles: StringArray = Vec::<Option<String>>::from([
        Some("first finding".to_string()),
        Some("null pk row".to_string()),
    ])
    .into_iter()
    .collect();
    let details: StringArray =
        Vec::<Option<String>>::from([Some("d1".to_string()), Some("d2".to_string())])
            .into_iter()
            .collect();
    let severities: StringArray =
        Vec::<Option<String>>::from([Some("high".to_string()), Some("low".to_string())])
            .into_iter()
            .collect();
    let tags: StringArray = Vec::<Option<String>>::from([Some("a".to_string()), None])
        .into_iter()
        .collect();
    let parents: StringArray = Vec::<Option<String>>::from([None::<String>, None::<String>])
        .into_iter()
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(ids),
            Arc::new(titles),
            Arc::new(details),
            Arc::new(severities),
            Arc::new(tags),
            Arc::new(parents),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(dir.path().join("only.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    dir
}

fn config_for(
    url: &str,
    dir: &std::path::Path,
    cursor: std::path::PathBuf,
    max_window: u64,
    map: &Mapping,
    scan: &exocortex_adapter_parquet::DirectoryScan,
) -> exocortex_adapter_sdk::AdapterConfig {
    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        "org",
        "parquet-dir://fixture-table",
        "parquet-adapter",
        url,
    );
    config.source_flavor = "parquet-dir".into();
    config.auth_token = "test-bearer".into();
    config.hmac_key = [7u8; 32];
    config.cursor_path = cursor;
    config.projection = Some(projection(&dir.to_string_lossy(), map, scan, max_window));
    config
}

#[test]
fn scan_is_deterministic_and_schema_faithful() {
    let dir = fixture_dir();
    let a = scan_directory(dir.path()).unwrap();
    let b = scan_directory(dir.path()).unwrap();
    assert_eq!(
        a.files,
        vec!["a.parquet".to_string(), "b.parquet".to_string()]
    );
    assert_eq!(a.file_set_hash, b.file_set_hash);
    let names: Vec<&str> = a.columns.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        ["id", "title", "detail", "severity", "tags", "parent_id"]
    );
    // The arrow types ride the observed schema.
    assert!(a
        .columns
        .iter()
        .any(|(n, t)| n == "id" && t.contains("Int64")));
    validate_mapping(&mapping(), &a).unwrap();
}

#[test]
fn changed_file_set_moves_the_snapshot_id() {
    let dir = fixture_dir();
    let before = scan_directory(dir.path()).unwrap();
    write_file(
        &dir.path().join("c.parquet"),
        &[(9, Some("late finding"), "arrived after", "low", "", None)],
    );
    let after = scan_directory(dir.path()).unwrap();
    assert_ne!(before.file_set_hash, after.file_set_hash);
    assert_eq!(after.files.len(), 3);
}

#[test]
fn missing_mapped_column_is_named_locally() {
    let dir = tempfile::tempdir().unwrap();
    write_file(
        &dir.path().join("thin.parquet"),
        &[(1, Some("t"), "d", "high", "x", None)],
    );
    let scan = scan_directory(dir.path()).unwrap();
    let mut thin = mapping();
    thin.content_columns.push("no_such_column".into());
    let err = validate_mapping(&thin, &scan).unwrap_err().to_string();
    assert!(err.contains("no_such_column"), "{err}");
}

#[test]
fn rows_read_in_order_with_parent_links_across_files() {
    let dir = fixture_dir();
    let (rows, skipped) = read_rows(dir.path(), &mapping()).unwrap();
    assert_eq!(skipped, 0);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].pk, "1");
    assert_eq!(rows[0].tags, vec!["backend", "wal"]);
    // r-2's parent r-1 lives in the other file.
    assert_eq!(rows[2].parent.as_deref(), Some("1"));
    // The untitled row keeps a fallback title at map time.
    let table = table_uuid_for("fixture-table");
    let declared = declared_columns(&mapping(), &scan_directory(dir.path()).unwrap());
    let (unit, skipped_parents) = map_rows(&mapping(), &table, &declared, &rows, "w-0");
    assert_eq!(skipped_parents, 0);
    assert_eq!(unit.memories.len(), 4);
    assert_eq!(unit.relationships.len(), 1);
    assert_eq!(unit.memories[3].title, "3");
}

#[test]
fn null_pk_rows_are_skipped_and_counted() {
    let dir = fixture_dir_with_null_pk();
    let (rows, skipped) = read_rows(dir.path(), &mapping()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(skipped, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_flavor_registration_and_submit_end_to_end() {
    let dir = fixture_dir();
    let scan = scan_directory(dir.path()).unwrap();
    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    let cursor = cursor_dir.path().join("c.cursor");
    // Connect under the TABLE flavor: the mock (like the real server)
    // would refuse this registration without the declared projection.
    let mut session = AdapterSession::connect_with(
        config_for(
            &mock.url(),
            dir.path(),
            cursor.clone(),
            256,
            &mapping(),
            &scan,
        ),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();

    let registrations = mock.registrations();
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].source_flavor, "parquet-dir");
    let wire_projection = registrations[0].projection.as_ref().unwrap();
    assert!(wire_projection.selector.contains("parquet-dir"));
    assert!(wire_projection.selector.contains("v1"));
    assert_eq!(
        wire_projection.bounds.as_ref().unwrap().max_rows_per_window,
        256
    );
    assert!(wire_projection
        .fields
        .iter()
        .any(|f| f.source_field == "parent_id" && f.kind == "Causes"));

    let (rows, _) = read_rows(dir.path(), &mapping()).unwrap();
    let declared = declared_columns(&mapping(), &scan);
    let table = table_uuid_for("fixture-table");
    let (unit, _) = map_rows(&mapping(), &table, &declared, &rows, "window-0");
    let unit = with_snapshot_id(unit, &scan.file_set_hash);
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session
        .submit_window(vec![unit], &scan.file_set_hash)
        .await
        .unwrap();
    assert_eq!(outcome.accepted, 4, "four rows, one null-title fallback");
    assert!(outcome.cursor_advanced);
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap(),
        scan.file_set_hash
    );

    // The submitted batch: canonical 32-byte schema hash (the mock
    // rejects any other width), 16-byte table uuids, the file-set hash
    // as snapshot id, and the cross-file Causes edge.
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), 1);
    let snapshot = submitted[0].snapshot.as_ref().unwrap();
    assert_eq!(snapshot.schema_hash.len(), 32);
    assert_eq!(
        snapshot.schema_hash,
        exocortex_wire::projection::schema_hash(&declared).to_vec()
    );
    assert_eq!(snapshot.snapshot_id, scan.file_set_hash);
    assert!(submitted[0].memories.iter().all(|m| m
        .external_key
        .as_ref()
        .is_some_and(|k| k.table_uuid.len() == 16)));
    assert_eq!(submitted[0].relationships.len(), 1);
    assert_eq!(submitted[0].relationships[0].kind, "Causes");
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_replay_and_rewind_refused() {
    let dir = fixture_dir();
    let scan = scan_directory(dir.path()).unwrap();
    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    let mut session = AdapterSession::connect_with(
        config_for(
            &mock.url(),
            dir.path(),
            cursor_dir.path().join("c.cursor"),
            256,
            &mapping(),
            &scan,
        ),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();

    let build = |seed: &str, snapshot: &str| {
        let (rows, _) = read_rows(dir.path(), &mapping()).unwrap();
        let declared = declared_columns(&mapping(), &scan);
        let (unit, _) = map_rows(
            &mapping(),
            &table_uuid_for("fixture-table"),
            &declared,
            &rows,
            seed,
        );
        with_snapshot_id(unit, snapshot)
    };

    mock.push_script(vec![
        MockSubmit::Accept,
        MockSubmit::Accept,
        MockSubmit::Accept,
    ]);
    session
        .submit_window(
            vec![build("window-0", &scan.file_set_hash)],
            &scan.file_set_hash,
        )
        .await
        .unwrap();
    // Same seed + same snapshot: the batch id is content-bound, so the
    // re-submission carries the SAME id the server's idempotency
    // registry settles (the mock accepts verbatim; the id equality is
    // the replay proof, per the git adapter's suite).
    session
        .submit_window(
            vec![build("window-0", &scan.file_set_hash)],
            &scan.file_set_hash,
        )
        .await
        .unwrap();
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), 2);
    assert_eq!(
        submitted[0].batch_id, submitted[1].batch_id,
        "re-runs derive the same content-bound batch id"
    );

    // A NEW directory state settles; then naming the superseded
    // snapshot again is a rewind the SDK refuses before the wire.
    write_file(
        &dir.path().join("c.parquet"),
        &[(9, Some("late finding"), "arrived after", "low", "", None)],
    );
    let changed = scan_directory(dir.path()).unwrap();
    session
        .submit_window(
            vec![build("window-1", &changed.file_set_hash)],
            &changed.file_set_hash,
        )
        .await
        .unwrap();
    let submits_before = mock.submitted().len();
    let err = match session
        .submit_window(
            vec![build("window-2", &scan.file_set_hash)],
            &scan.file_set_hash,
        )
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("the superseded snapshot must be refused"),
    };
    match err {
        SdkError::SourceRewound { observed, last } => {
            assert_eq!(observed, scan.file_set_hash);
            assert_eq!(last, changed.file_set_hash);
        }
        other => panic!("expected the rewind error, got {other:?}"),
    }
    assert_eq!(mock.submitted().len(), submits_before);
    mock.stop();
}
