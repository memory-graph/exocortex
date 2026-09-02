//! D1 delta-flavor integration: real Delta table layouts built
//! hermetically (JSON commit logs, parquet data files through the
//! arrow writer, single AND multi-part parquet checkpoints with the
//! classic nested action columns) replayed through the scanner, the
//! mapper, and the SDK mock server end to end under the `delta` TABLE
//! flavor.

use std::sync::Arc;

use arrow_array::{
    builder::{ListBuilder, MapBuilder, StringBuilder},
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use exocortex_adapter_delta::{
    declared_columns, map_rows, projection, read_rows, scan_table, table_uuid_for,
    validate_mapping, with_snapshot_id,
};
use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{AdapterSession, SdkError};
use exocortex_adapter_table::Mapping;
use parquet::arrow::ArrowWriter;

const TABLE_ID: &str = "d5f7e2a1-3b4c-4d5e-9f0a-222222222222";

fn mapping() -> Mapping {
    serde_json::from_str(
        r#"{
            "memory_type": "Problem",
            "title_column": "title",
            "content_columns": ["detail", "severity", "region"],
            "pk_columns": ["id"],
            "tags_column": "tags",
            "parent_column": "parent_id",
            "parent_kind": "Causes",
            "mapping_version": 1
        }"#,
    )
    .unwrap()
}

/// Physical data-file schema: everything except the partition column
/// `region` (Delta writers omit partition columns from data files;
/// the `add.partitionValues` carries them).
fn data_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("detail", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
    ])
}

type FixtureRow = (
    i64,
    Option<&'static str>,
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
);

fn write_data_file(path: &std::path::Path, rows: &[FixtureRow]) {
    let ids: Int64Array = rows.iter().map(|row| Some(row.0)).collect();
    let titles: StringArray = rows.iter().map(|row| row.1.map(str::to_string)).collect();
    let details: StringArray = rows.iter().map(|row| Some(row.2.to_string())).collect();
    let severities: StringArray = rows.iter().map(|row| Some(row.3.to_string())).collect();
    let tags: StringArray = rows.iter().map(|row| Some(row.4.to_string())).collect();
    let parents: StringArray = rows.iter().map(|row| row.5.map(str::to_string)).collect();
    let batch = RecordBatch::try_new(
        Arc::new(data_schema()),
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

fn schema_string() -> String {
    serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "title", "type": "string", "nullable": true, "metadata": {}},
            {"name": "detail", "type": "string", "nullable": true, "metadata": {}},
            {"name": "severity", "type": "string", "nullable": true, "metadata": {}},
            {"name": "tags", "type": "string", "nullable": true, "metadata": {}},
            {"name": "parent_id", "type": "string", "nullable": true, "metadata": {}},
            {"name": "region", "type": "string", "nullable": true, "metadata": {}}
        ]
    })
    .to_string()
}

fn meta_action() -> serde_json::Value {
    serde_json::json!({
        "metaData": {
            "id": TABLE_ID,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string(),
            "partitionColumns": ["region"],
            "configuration": {},
            "createdTime": 1700000000000i64
        }
    })
}

fn protocol_action() -> serde_json::Value {
    serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
}

fn add_action(path: &str, region: &str) -> serde_json::Value {
    serde_json::json!({
        "add": {
            "path": path,
            "partitionValues": {"region": region},
            "size": 128,
            "modificationTime": 1700000000000i64,
            "dataChange": true
        }
    })
}

fn remove_action(path: &str) -> serde_json::Value {
    serde_json::json!({"remove": {"path": path, "deletionTimestamp": 1700000001000i64, "dataChange": true}})
}

fn write_commit(dir: &std::path::Path, version: u64, actions: &[serde_json::Value]) {
    let mut text = String::new();
    for action in actions {
        text.push_str(&action.to_string());
        text.push('\n');
    }
    std::fs::write(
        dir.join("_delta_log").join(format!("{version:020}.json")),
        text,
    )
    .unwrap();
}

/// A table whose history is: v0 (meta/protocol + two adds), v1 (one
/// more add), v2 (one remove). Live files after replay: a, b, c minus
/// b = a, c.
fn json_table() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    write_data_file(
        &dir.path().join("part-00000-a.parquet"),
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
    write_data_file(
        &dir.path().join("part-00001-b.parquet"),
        &[(
            2,
            Some("second finding"),
            "cursor stalled",
            "medium",
            "ingest",
            Some("1"),
        )],
    );
    write_data_file(
        &dir.path().join("part-00002-c.parquet"),
        &[(3, None, "untitled row", "low", "", Some("2"))],
    );
    write_commit(
        dir.path(),
        0,
        &[
            protocol_action(),
            meta_action(),
            add_action("part-00000-a.parquet", "us-west"),
            add_action("part-00001-b.parquet", "us-east"),
        ],
    );
    write_commit(
        dir.path(),
        1,
        &[add_action("part-00002-c.parquet", "us-west")],
    );
    write_commit(dir.path(), 2, &[remove_action("part-00001-b.parquet")]);
    dir
}

#[test]
fn json_log_replay_resolves_live_files_and_meta() {
    let dir = json_table();
    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.table_id, TABLE_ID);
    assert_eq!(scan.version, 2);
    assert_eq!(scan.snapshot_id_string(), "v2");
    assert_eq!(scan.files.len(), 2, "b was removed at v2");
    assert!(
        scan.files
            .iter()
            .all(|f| !f.path.to_string_lossy().contains("part-00001-b")),
        "the removed file never reaches the live set"
    );
    assert_eq!(scan.partition_columns, vec!["region".to_string()]);
    assert!(scan.columns.iter().any(|(n, t)| n == "id" && t == "long"));
    assert!(scan
        .columns
        .iter()
        .any(|(n, t)| n == "region" && t == "string"));
    validate_mapping(&mapping(), &scan).unwrap();

    // Rows carry the partition overlay per file.
    let (rows, skipped) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(skipped, 0);
    assert_eq!(rows.len(), 3);
    let region = rows[2]
        .content
        .iter()
        .find(|(c, _)| c == "region")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(region, "us-west");
    // c's row (id 3) parents at id 2, which lived in b — the file v2
    // removed. map_rows counts the link skipped, never dangled.
    let declared = declared_columns(&mapping(), &scan);
    let (unit, skipped_parents) = map_rows(
        &mapping(),
        &table_uuid_for(TABLE_ID),
        &declared,
        &rows,
        "w-0",
    );
    assert_eq!(unit.memories.len(), 3);
    assert_eq!(unit.relationships.len(), 0);
    assert_eq!(
        skipped_parents, 1,
        "the parent that lived in the removed file is counted, never dangled"
    );
    assert_eq!(unit.snapshot.as_ref().unwrap().source_flavor, "delta");
}

#[test]
fn missing_mapped_column_is_named_locally() {
    let dir = json_table();
    let scan = scan_table(dir.path()).unwrap();
    let mut bad = mapping();
    bad.content_columns.push("no_such_column".into());
    let err = validate_mapping(&bad, &scan).unwrap_err().to_string();
    assert!(err.contains("no_such_column"), "{err}");
}

#[test]
fn percent_escaped_paths_decode() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    std::fs::create_dir_all(dir.path().join("date=2026-09-02")).unwrap();
    write_data_file(
        &dir.path().join("date=2026-09-02/part a.parquet"),
        &[(1, Some("escaped"), "url-escaped path", "low", "", None)],
    );
    write_commit(
        dir.path(),
        0,
        &[
            protocol_action(),
            meta_action(),
            add_action("date=2026-09-02/part%20a.parquet", "us-west"),
        ],
    );
    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(
        scan.files.len(),
        1,
        "%20 decodes to the space in the real name"
    );
    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn protocol_shapes_fail_closed() {
    // minReaderVersion 3.
    let dir = json_table();
    write_commit(
        dir.path(),
        3,
        &[serde_json::json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7}})],
    );
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("minReaderVersion 3"), "{err}");

    // Deletion vectors as a table feature.
    let dir = json_table();
    write_commit(
        dir.path(),
        3,
        &[serde_json::json!({"protocol": {
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
        }})],
    );
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("deletionVectors"), "{err}");

    // Column mapping configured.
    let dir = json_table();
    write_commit(
        dir.path(),
        3,
        &[serde_json::json!({"metaData": {
            "id": TABLE_ID,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string(),
            "partitionColumns": ["region"],
            "configuration": {"delta.columnMapping.mode": "name"},
            "createdTime": 1700000000000i64
        }})],
    );
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("columnMapping"), "{err}");
}

#[test]
fn v2_checkpoint_hint_fails_closed() {
    let dir = json_table();
    std::fs::write(
        dir.path().join("_delta_log/_last_checkpoint"),
        r#"{"version": 2, "v2Checkpoint": {"sidecarFiles": [{"path": "a.bin"}]}}"#,
    )
    .unwrap();
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("v2 checkpoint"), "{err}");
}

// --- Checkpoint fixtures (classic nested action columns) -----------

struct CheckpointRow {
    add: Option<(String, String)>, // (path, region)
    remove: Option<String>,
}

/// Write a classic checkpoint parquet: one row per action, the four
/// action struct columns with nulls elsewhere. When `parts` > 1 the
/// rows are distributed across multi-part files
/// `<N>.checkpoint.<i>.<parts>.parquet`.
fn write_checkpoint(dir: &std::path::Path, version: u64, parts: u64, rows: &[CheckpointRow]) {
    let total_rows = rows.len() as u64;
    let per_part = total_rows.div_ceil(parts).max(1);
    for part in 0..parts {
        let start = (part * per_part) as usize;
        let end = start.saturating_add(per_part as usize).min(rows.len());
        let slice: &[CheckpointRow] = &rows[start..end];
        // Meta/protocol ride only part 0.
        let include_meta = part == 0;
        write_checkpoint_part(dir, version, parts, part, slice, include_meta);
    }
    std::fs::write(
        dir.join("_delta_log/_last_checkpoint"),
        format!("{{\"version\": {version}, \"size\": {}}}", rows.len()),
    )
    .unwrap();
}

fn write_checkpoint_part(
    dir: &std::path::Path,
    version: u64,
    parts: u64,
    part: u64,
    rows: &[CheckpointRow],
    include_meta: bool,
) {
    let rows_count = rows.len();

    // add.path / add.partitionValues, one row per action.
    let add_paths: Vec<Option<String>> = rows
        .iter()
        .map(|row| row.add.as_ref().map(|(path, _)| path.clone()))
        .collect();
    let mut add_partition_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        match &row.add {
            Some((_, region)) => {
                add_partition_builder.keys().append_value("region");
                add_partition_builder.values().append_value(region);
                add_partition_builder.append(true).unwrap();
            }
            None => add_partition_builder.append(false).unwrap(),
        }
    }
    let add_partitions = add_partition_builder.finish();
    let add_valid: Vec<bool> = rows.iter().map(|r| r.add.is_some()).collect();
    let add_struct = StructArray::new(
        Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("partitionValues", add_partitions.data_type().clone(), true),
            Field::new("size", DataType::Int64, true),
            Field::new("modificationTime", DataType::Int64, true),
            Field::new("dataChange", DataType::Boolean, true),
        ]),
        vec![
            Arc::new(StringArray::from(add_paths)),
            Arc::new(add_partitions),
            Arc::new(Int64Array::from(vec![Some(128i64); rows_count])),
            Arc::new(Int64Array::from(vec![Some(1700000000000i64); rows_count])),
            Arc::new(BooleanArray::from(vec![Some(true); rows_count])),
        ],
        Some(add_valid.into_iter().collect()),
    );

    // remove.path (+ required sibling fields).
    let remove_paths: Vec<Option<String>> = rows.iter().map(|r| r.remove.clone()).collect();
    let remove_valid: Vec<bool> = rows.iter().map(|r| r.remove.is_some()).collect();
    let remove_struct = StructArray::new(
        Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("deletionTimestamp", DataType::Int64, true),
            Field::new("dataChange", DataType::Boolean, true),
            Field::new("extendedFileMetadata", DataType::Boolean, true),
        ]),
        vec![
            Arc::new(StringArray::from(remove_paths)),
            Arc::new(Int64Array::from(vec![Some(1700000001000i64); rows_count])),
            Arc::new(BooleanArray::from(vec![Some(true); rows_count])),
            Arc::new(BooleanArray::from(vec![Some(true); rows_count])),
        ],
        Some(remove_valid.into_iter().collect()),
    );

    // metaData (only in part 0; one row per action with the same
    // values where valid — the reader takes the last non-null).
    let meta_valid: Vec<bool> = vec![include_meta; rows_count];
    let meta_ids: Vec<Option<String>> = rows
        .iter()
        .map(|_| include_meta.then(|| TABLE_ID.to_string()))
        .collect();
    let meta_schemas: Vec<Option<String>> = rows
        .iter()
        .map(|_| include_meta.then(schema_string))
        .collect();
    let mut partition_list = ListBuilder::new(StringBuilder::new());
    for _ in rows {
        if include_meta {
            partition_list.values().append_value("region");
            partition_list.append(true);
        } else {
            partition_list.append(false);
        }
    }
    let meta_partition_columns = partition_list.finish();
    let mut config_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for _ in rows {
        config_builder.append(include_meta).unwrap();
    }
    let meta_configuration = config_builder.finish();
    let meta_struct = StructArray::new(
        Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("schemaString", DataType::Utf8, true),
            Field::new(
                "partitionColumns",
                meta_partition_columns.data_type().clone(),
                true,
            ),
            Field::new(
                "configuration",
                meta_configuration.data_type().clone(),
                true,
            ),
            Field::new("createdTime", DataType::Int64, true),
        ]),
        vec![
            Arc::new(StringArray::from(meta_ids)),
            Arc::new(StringArray::from(vec![None::<String>; rows_count])),
            Arc::new(StringArray::from(vec![None::<String>; rows_count])),
            Arc::new(StringArray::from(meta_schemas)),
            Arc::new(meta_partition_columns),
            Arc::new(meta_configuration),
            Arc::new(Int64Array::from(vec![Some(1700000000000i64); rows_count])),
        ],
        Some(meta_valid.clone().into_iter().collect()),
    );

    // protocol.minReaderVersion.
    let protocol_struct = StructArray::new(
        Fields::from(vec![
            Field::new("minReaderVersion", DataType::Int32, true),
            Field::new("minWriterVersion", DataType::Int32, true),
        ]),
        vec![
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|_| include_meta.then_some(1i32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|_| include_meta.then_some(2i32))
                    .collect::<Vec<_>>(),
            )),
        ],
        Some(meta_valid.into_iter().collect()),
    );

    let empty_struct = DataType::Struct(Fields::from(vec![Field::new(
        "empty",
        DataType::UInt8,
        true,
    )]));
    let schema = Schema::new(vec![
        Field::new("txn", empty_struct.clone(), true),
        Field::new("add", add_struct.data_type().clone(), true),
        Field::new("remove", remove_struct.data_type().clone(), true),
        Field::new("metaData", meta_struct.data_type().clone(), true),
        Field::new("protocol", protocol_struct.data_type().clone(), true),
        Field::new("commitInfo", empty_struct, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StructArray::new_null(
                Fields::from(vec![Field::new("empty", DataType::UInt8, true)]),
                rows_count,
            )),
            Arc::new(add_struct),
            Arc::new(remove_struct),
            Arc::new(meta_struct),
            Arc::new(protocol_struct),
            Arc::new(StructArray::new_null(
                Fields::from(vec![Field::new("empty", DataType::UInt8, true)]),
                rows_count,
            )),
        ],
    )
    .unwrap();

    let name = if parts == 1 {
        format!("{version:020}.checkpoint.parquet")
    } else {
        format!("{version:020}.checkpoint.{part:05}.{parts:05}.parquet")
    };
    let file = std::fs::File::create(dir.join("_delta_log").join(name)).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn checkpointed_table(parts: u64) -> tempfile::TempDir {
    let dir = json_table();
    // Fold v0..=v2 into the checkpoint: live a (us-west) + c
    // (us-west); b was removed.
    write_checkpoint(
        dir.path(),
        2,
        parts,
        &[
            CheckpointRow {
                add: Some(("part-00000-a.parquet".into(), "us-west".into())),
                remove: None,
            },
            CheckpointRow {
                add: None,
                remove: Some("part-00001-b.parquet".into()),
            },
            CheckpointRow {
                add: Some(("part-00002-c.parquet".into(), "us-west".into())),
                remove: None,
            },
        ],
    );
    // v3: one more file arrives; v4: it is removed again — proving
    // commits AFTER the checkpoint apply on top of it.
    write_data_file(
        &dir.path().join("part-00003-d.parquet"),
        &[(
            4,
            Some("transient"),
            "added and removed after the checkpoint",
            "low",
            "",
            None,
        )],
    );
    write_commit(
        dir.path(),
        3,
        &[add_action("part-00003-d.parquet", "us-east")],
    );
    write_commit(dir.path(), 4, &[remove_action("part-00003-d.parquet")]);
    dir
}

#[test]
fn single_part_checkpoint_plus_commits_replay() {
    let dir = checkpointed_table(1);
    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.version, 4);
    assert_eq!(scan.table_id, TABLE_ID);
    assert_eq!(
        scan.files.len(),
        2,
        "a and c live; d added at v3 and removed at v4"
    );
    assert!(scan
        .files
        .iter()
        .all(|f| !f.path.to_string_lossy().ends_with("part-00001-b.parquet")));
    assert_eq!(scan.partition_columns, vec!["region".to_string()]);
    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn multi_part_checkpoint_replay() {
    let dir = checkpointed_table(3);
    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.version, 4);
    assert_eq!(scan.files.len(), 2);
    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn a_missing_checkpoint_part_fails_closed() {
    let dir = checkpointed_table(2);
    // Delete the second part file.
    let log = dir.path().join("_delta_log");
    let part1 = log.join(format!("{:020}.checkpoint.{:05}.{:05}.parquet", 2, 1, 2));
    std::fs::remove_file(&part1).unwrap();
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("missing parts"), "{err}");
}

fn config_for(
    url: &str,
    cursor: std::path::PathBuf,
    map: &Mapping,
    scan: &exocortex_adapter_delta::DeltaScan,
    max_window: u64,
) -> exocortex_adapter_sdk::AdapterConfig {
    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        "org",
        &format!("delta://{TABLE_ID}"),
        "delta-adapter",
        url,
    );
    config.source_flavor = "delta".into();
    config.auth_token = "test-bearer".into();
    config.hmac_key = [7u8; 32];
    config.cursor_path = cursor;
    config.projection = Some(projection(map, scan, max_window));
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn table_flavor_registration_and_submit_end_to_end() {
    let dir = json_table();
    let scan = scan_table(dir.path()).unwrap();
    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    let mut session = AdapterSession::connect_with(
        config_for(
            &mock.url(),
            cursor_dir.path().join("c.cursor"),
            &mapping(),
            &scan,
            256,
        ),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();

    let registrations = mock.registrations();
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].source_flavor, "delta");
    let wire_projection = registrations[0].projection.as_ref().unwrap();
    assert!(wire_projection.selector.contains(TABLE_ID));
    assert!(wire_projection.selector.contains("v2"));

    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    let declared = declared_columns(&mapping(), &scan);
    let table = table_uuid_for(TABLE_ID);
    let (unit, _) = map_rows(&mapping(), &table, &declared, &rows, "window-0");
    let snapshot = scan.snapshot_id_string();
    let unit = with_snapshot_id(unit, &snapshot);
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session.submit_window(vec![unit], &snapshot).await.unwrap();
    assert_eq!(outcome.accepted, 3);
    assert!(outcome.cursor_advanced);
    assert_eq!(
        std::fs::read_to_string(cursor_dir.path().join("c.cursor")).unwrap(),
        snapshot
    );

    let submitted = mock.submitted();
    let wire_snapshot = submitted[0].snapshot.as_ref().unwrap();
    assert_eq!(wire_snapshot.schema_hash.len(), 32);
    assert_eq!(
        wire_snapshot.schema_hash,
        exocortex_wire::projection::schema_hash(&declared).to_vec()
    );
    assert_eq!(wire_snapshot.snapshot_id, snapshot);
    assert_eq!(wire_snapshot.source_flavor, "delta");
    assert!(submitted[0]
        .memories
        .iter()
        .all(|m| m.tags.contains(&"delta".to_string())));
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn later_version_ingests_and_regression_is_refused() {
    let dir = json_table();
    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    let cursor = cursor_dir.path().join("c.cursor");

    let build = |scan: &exocortex_adapter_delta::DeltaScan, seed: &str| {
        let (rows, _) = read_rows(&mapping(), scan).unwrap();
        let declared = declared_columns(&mapping(), scan);
        let (unit, _) = map_rows(
            &mapping(),
            &table_uuid_for(TABLE_ID),
            &declared,
            &rows,
            seed,
        );
        with_snapshot_id(unit, &scan.snapshot_id_string())
    };

    // Settle v2 first.
    let scan_v2 = scan_table(dir.path()).unwrap();
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), cursor.clone(), &mapping(), &scan_v2, 256),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();
    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);
    session
        .submit_window(vec![build(&scan_v2, "w-0")], &scan_v2.snapshot_id_string())
        .await
        .unwrap();

    // v3 arrives.
    write_data_file(
        &dir.path().join("part-00003-d.parquet"),
        &[(9, Some("late finding"), "arrived later", "low", "", None)],
    );
    write_commit(
        dir.path(),
        3,
        &[add_action("part-00003-d.parquet", "us-west")],
    );
    let scan_v3 = scan_table(dir.path()).unwrap();
    assert_eq!(scan_v3.version, 3);
    session
        .submit_window(vec![build(&scan_v3, "w-1")], &scan_v3.snapshot_id_string())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap(),
        "v3",
        "the cursor advances with the log"
    );

    // The log regresses: someone restores an old _delta_log (v2 files
    // only). Submitting the superseded version is refused BEFORE any
    // wire traffic.
    std::fs::remove_file(dir.path().join("_delta_log/00000000000000000003.json")).unwrap();
    std::fs::remove_file(dir.path().join("part-00003-d.parquet")).unwrap();
    let regressed = scan_table(dir.path()).unwrap();
    assert_eq!(regressed.version, 2);
    let submits_before = mock.submitted().len();
    let err = match session
        .submit_window(
            vec![build(&regressed, "w-2")],
            &regressed.snapshot_id_string(),
        )
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("the superseded version must be refused"),
    };
    match &err {
        SdkError::SourceRewound { observed, last } => {
            assert_eq!(observed, "v2");
            assert_eq!(last, "v3");
        }
        other => panic!("expected SourceRewound, got {other:?}"),
    }
    assert_eq!(mock.submitted().len(), submits_before);
    mock.stop();
}
