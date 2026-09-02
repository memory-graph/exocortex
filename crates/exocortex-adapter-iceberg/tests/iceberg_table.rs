//! D1 iceberg-flavor integration: real Iceberg table layouts built
//! hermetically (Parquet data files via the arrow writer, Avro
//! manifest lists and manifests, `vN.metadata.json` + version hint)
//! flow through the scanner, the mapper, and the SDK mock server end
//! to end under the `iceberg` TABLE flavor.
//!
//! Two manifest-writing paths are covered deliberately:
//!
//! - **v2 manifests are hand-framed.** Iceberg v2 keys partition
//!   struct fields by field-id STRINGS ("1000"), which Avro's name
//!   grammar forbids — apache-avro's own writer cannot produce them
//!   and its parser rejects them ("Invalid field name 1000"). The
//!   fixture writes the object-container framing directly, so the
//!   production sanitize-and-decode path is exercised on exactly the
//!   byte shape real v2 tables carry.
//! - **v1 manifests round-trip through a real Avro writer** (name-keyed
//!   partition fields, one manifest list in the deflate codec), proving
//!   interop with genuine avro-rs output and the inflate path.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use exocortex_adapter_iceberg::{
    declared_columns, map_rows, projection, read_rows, scan_table, table_uuid_for,
    validate_mapping, with_snapshot_id,
};
use exocortex_adapter_sdk::testing::{MockServer, MockSubmit};
use exocortex_adapter_sdk::{AdapterSession, SdkError};
use exocortex_adapter_table::Mapping;
use parquet::arrow::ArrowWriter;

const TABLE_UUID: &str = "9c1a0f8e-2f3d-4a5b-8e7f-111111111111";

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

/// The physical data-file schema: every mapped column EXCEPT the
/// partition column `region` (identity-partitioned columns may be
/// omitted from data files; the manifest carries their values).
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

/// Rows deliberately omit `region` — its value arrives only through
/// the manifest partition overlay, which is what these fixtures prove.
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

// --- Avro binary primitives (for the hand-framed v2 fixtures) ------

fn avro_long(mut n: i64) -> Vec<u8> {
    n = (n << 1) ^ (n >> 63);
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out
}

fn avro_str(s: &str) -> Vec<u8> {
    let mut out = avro_long(s.len() as i64);
    out.extend_from_slice(s.as_bytes());
    out
}

fn avro_bytes(raw: &[u8]) -> Vec<u8> {
    let mut out = avro_long(raw.len() as i64);
    out.extend_from_slice(raw);
    out
}

const V2_MANIFEST_SCHEMA: &str = r#"{
    "type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int", "default": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "default": null},
        {"name": "data_file", "type": {
            "type": "record", "name": "r2", "fields": [
                {"name": "content", "type": "int", "default": 0},
                {"name": "file_path", "type": "string"},
                {"name": "file_format", "type": "string"},
                {"name": "partition", "type": {
                    "type": "record", "name": "r1024", "fields": [
                        {"name": "1000", "type": "string"}
                    ]
                }},
                {"name": "record_count", "type": "long"},
                {"name": "file_size_in_bytes", "type": "long"}
            ]
        }}
    ]
}"#;

/// One v2 manifest entry, binary-encoded by hand against
/// V2_MANIFEST_SCHEMA (field order is the encoding; names never ride
/// the wire).
fn v2_entry(status: i32, file_path: &str, region: &str, record_count: i64) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(avro_long(status as i64));
    out.extend(avro_long(0)); // snapshot_id: null branch of the union
    out.extend(avro_long(0)); // data_file.content: DATA
    out.extend(avro_str(file_path));
    out.extend(avro_str("PARQUET"));
    out.extend(avro_str(region)); // partition."1000"
    out.extend(avro_long(record_count));
    out.extend(avro_long(0)); // file_size_in_bytes
    out
}

fn sync_marker(seed: u8) -> [u8; 16] {
    [seed; 16]
}

/// Write a v2 manifest as a real object-container file whose embedded
/// schema carries the field-id-string partition key ("1000") that
/// apache-avro's parser refuses.
fn write_v2_manifest(path: &std::path::Path, entries: &[Vec<u8>]) {
    let mut out = vec![0x4f, 0x62, 0x6a, 0x01];
    out.extend(avro_long(1));
    out.extend(avro_str("avro.schema"));
    out.extend(avro_bytes(V2_MANIFEST_SCHEMA.as_bytes()));
    out.extend(avro_long(0)); // end of metadata blocks
    let sync = sync_marker(0x5a);
    out.extend_from_slice(&sync);
    let mut data = Vec::new();
    for entry in entries {
        data.extend_from_slice(entry);
    }
    out.extend(avro_long(entries.len() as i64));
    out.extend(avro_long(data.len() as i64));
    out.extend_from_slice(&data);
    out.extend_from_slice(&sync);
    std::fs::write(path, out).unwrap();
}

/// Write a v1 manifest through apache-avro's own writer (name-keyed
/// partition fields are legal Avro names).
fn write_v1_manifest(path: &std::path::Path, entries: &[(i32, &str, &str, i64)]) {
    let schema_json = r#"{
        "type": "record", "name": "manifest_entry", "fields": [
            {"name": "status", "type": "int", "default": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null},
            {"name": "data_file", "type": {
                "type": "record", "name": "r1", "fields": [
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "partition", "type": {
                        "type": "record", "name": "r512", "fields": [
                            {"name": "region", "type": "string"}
                        ]
                    }},
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"}
                ]
            }}
        ]
    }"#;
    let schema = apache_avro::Schema::parse_str(schema_json).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, std::fs::File::create(path).unwrap());
    for (status, file_path, region, count) in entries {
        let entry = apache_avro::types::Value::Record(vec![
            ("status".into(), apache_avro::types::Value::Int(*status)),
            (
                "snapshot_id".into(),
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            ),
            (
                "data_file".into(),
                apache_avro::types::Value::Record(vec![
                    (
                        "file_path".into(),
                        apache_avro::types::Value::String((*file_path).into()),
                    ),
                    (
                        "file_format".into(),
                        apache_avro::types::Value::String("PARQUET".into()),
                    ),
                    (
                        "partition".into(),
                        apache_avro::types::Value::Record(vec![(
                            "region".into(),
                            apache_avro::types::Value::String((*region).into()),
                        )]),
                    ),
                    (
                        "record_count".into(),
                        apache_avro::types::Value::Long(*count),
                    ),
                    (
                        "file_size_in_bytes".into(),
                        apache_avro::types::Value::Long(0),
                    ),
                ]),
            ),
        ]);
        writer.append(entry).unwrap();
    }
    writer.flush().unwrap();
}

/// Write a manifest list through apache-avro's writer; `deflate`
/// exercises the inflate path of the framing reader.
fn write_manifest_list(path: &std::path::Path, manifests: &[String], deflate: bool) {
    let schema_json = r#"{
        "type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "added_snapshot_id", "type": "long"},
            {"name": "added_files_count", "type": "int"},
            {"name": "existing_files_count", "type": "int"},
            {"name": "deleted_files_count", "type": "int"}
        ]
    }"#;
    let schema = apache_avro::Schema::parse_str(schema_json).unwrap();
    let mut writer = apache_avro::Writer::with_codec(
        &schema,
        std::fs::File::create(path).unwrap(),
        if deflate {
            apache_avro::Codec::Deflate(Default::default())
        } else {
            apache_avro::Codec::Null
        },
    );
    for manifest in manifests {
        let entry = apache_avro::types::Value::Record(vec![
            (
                "manifest_path".into(),
                apache_avro::types::Value::String(manifest.clone()),
            ),
            ("manifest_length".into(), apache_avro::types::Value::Long(0)),
            (
                "partition_spec_id".into(),
                apache_avro::types::Value::Int(0),
            ),
            (
                "added_snapshot_id".into(),
                apache_avro::types::Value::Long(9001),
            ),
            (
                "added_files_count".into(),
                apache_avro::types::Value::Int(1),
            ),
            (
                "existing_files_count".into(),
                apache_avro::types::Value::Int(0),
            ),
            (
                "deleted_files_count".into(),
                apache_avro::types::Value::Int(0),
            ),
        ]);
        writer.append(entry).unwrap();
    }
    writer.flush().unwrap();
}

fn schema_fields() -> serde_json::Value {
    serde_json::json!([
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "title", "required": false, "type": "string"},
        {"id": 3, "name": "detail", "required": false, "type": "string"},
        {"id": 4, "name": "severity", "required": false, "type": "string"},
        {"id": 5, "name": "tags", "required": false, "type": "string"},
        {"id": 6, "name": "parent_id", "required": false, "type": "string"},
        {"id": 7, "name": "region", "required": false, "type": "string"}
    ])
}

fn write_metadata_v2(
    dir: &std::path::Path,
    version: u64,
    current_snapshot: i64,
    snapshots: serde_json::Value,
    transform: &str,
) {
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": TABLE_UUID,
        "location": format!("file://{}", dir.display()),
        "last-sequence-number": 1,
        "current-snapshot-id": current_snapshot,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "fields": schema_fields()}],
        "default-spec-id": 0,
        "partition-specs": [{
            "spec-id": 0,
            "fields": [{"name": "region", "transform": transform, "source-id": 7, "field-id": 1000}]
        }],
        "snapshots": snapshots
    });
    std::fs::write(
        dir.join(format!("metadata/v{version}.metadata.json")),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("metadata/version-hint.text"), version.to_string()).unwrap();
}

/// The full v2 fixture: two data files across two manifests (one
/// deleted entry proving status=2 skips), region carried only by the
/// manifest overlay.
fn v2_table() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();

    write_data_file(
        &dir.path().join("data/part-00000-a.parquet"),
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
        &dir.path().join("data/part-00001-b.parquet"),
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

    write_v2_manifest(
        &dir.path().join("metadata/m0.avro"),
        &[
            v2_entry(
                1,
                &format!("file://{}/data/part-00000-a.parquet", dir.path().display()),
                "us-west",
                2,
            ),
            v2_entry(
                2,
                &format!("file://{}/data/obsolete.parquet", dir.path().display()),
                "us-west",
                9,
            ),
        ],
    );
    write_v2_manifest(
        &dir.path().join("metadata/m1.avro"),
        &[v2_entry(
            0,
            &format!("file://{}/data/part-00001-b.parquet", dir.path().display()),
            "us-east",
            2,
        )],
    );
    write_manifest_list(
        &dir.path().join("metadata/snap-9001.avro"),
        &[
            format!("file://{}/metadata/m0.avro", dir.path().display()),
            format!("file://{}/metadata/m1.avro", dir.path().display()),
        ],
        true,
    );
    write_metadata_v2(
        dir.path(),
        1,
        9001,
        serde_json::json!([{
            "snapshot-id": 9001,
            "sequence-number": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "manifest-list": format!("file://{}/metadata/snap-9001.avro", dir.path().display()),
            "summary": {"operation": "append"}
        }]),
        "identity",
    );
    dir
}

#[test]
fn scan_is_deterministic_and_schema_faithful() {
    let dir = v2_table();
    let a = scan_table(dir.path()).unwrap();
    let b = scan_table(dir.path()).unwrap();
    assert_eq!(a.table_uuid, TABLE_UUID);
    assert_eq!(a.format_version, 2);
    assert_eq!(a.snapshot_id, Some(9001));
    assert_eq!(a.snapshot_id_string().as_deref(), Some("9001"));
    assert_eq!(a.files, b.files, "two scans of one table agree");
    // The deleted manifest entry is not live.
    assert_eq!(a.files.len(), 2);
    assert!(
        a.files
            .iter()
            .all(|f| !f.path.ends_with("obsolete.parquet")),
        "status=2 (deleted) entries never reach the live set"
    );
    // Files read in sorted path order.
    assert!(a.files[0].path.to_string_lossy().contains("part-00000"));
    // Table schema renders Iceberg-native types.
    assert!(a.columns.iter().any(|(n, t)| n == "id" && t == "long"));
    assert!(a.columns.iter().any(|(n, t)| n == "title" && t == "string"));
    // Identity partition overlay resolved per file.
    let west = a
        .files
        .iter()
        .find(|f| f.path.to_string_lossy().contains("part-00000"))
        .unwrap();
    assert_eq!(
        west.partition.get("region").cloned().flatten(),
        Some("us-west".to_string())
    );
    validate_mapping(&mapping(), &a).unwrap();
}

#[test]
fn identity_partition_overlay_backs_absent_columns() {
    let dir = v2_table();
    let scan = scan_table(dir.path()).unwrap();
    let (rows, skipped) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(skipped, 0);
    assert_eq!(rows.len(), 4);
    // `region` is in NO data file; its value arrives from the manifest
    // partition overlay, per file.
    let west: Vec<&str> = rows[0]
        .content
        .iter()
        .filter_map(|(c, v)| (c == "region").then_some(v.as_str()))
        .collect();
    assert_eq!(
        west,
        ["us-west"],
        "first file rows carry the manifest's partition value"
    );
    let east: Vec<&str> = rows[2]
        .content
        .iter()
        .filter_map(|(c, v)| (c == "region").then_some(v.as_str()))
        .collect();
    assert_eq!(east, ["us-east"], "second file rows carry their own");
    // The parent link across files survives.
    assert_eq!(rows[2].parent.as_deref(), Some("1"));
}

#[test]
fn non_identity_transform_is_refused_for_mapped_columns() {
    let dir = v2_table();
    write_metadata_v2(
        dir.path(),
        2,
        9001,
        serde_json::json!([{
            "snapshot-id": 9001,
            "sequence-number": 1,
            "manifest-list": format!("file://{}/metadata/snap-9001.avro", dir.path().display())
        }]),
        "bucket[8]",
    );
    let scan = scan_table(dir.path()).unwrap();
    let err = validate_mapping(&mapping(), &scan).unwrap_err().to_string();
    assert!(err.contains("bucket[8]"), "{err}");
    assert!(err.contains("region"), "{err}");
}

#[test]
fn missing_mapped_column_is_named_locally() {
    let dir = v2_table();
    let scan = scan_table(dir.path()).unwrap();
    let mut bad = mapping();
    bad.content_columns.push("no_such_column".into());
    let err = validate_mapping(&bad, &scan).unwrap_err().to_string();
    assert!(err.contains("no_such_column"), "{err}");
}

#[test]
fn delete_files_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    write_data_file(
        &dir.path().join("data/part.parquet"),
        &[(1, Some("t"), "d", "low", "", None)],
    );
    write_v2_manifest(
        &dir.path().join("metadata/m0.avro"),
        &[v2_entry(
            0,
            &format!("file://{}/data/part.parquet", dir.path().display()),
            "us-west",
            1,
        )],
    );
    // A second manifest whose only entry is a position-delete file.
    write_v2_manifest(
        &dir.path().join("metadata/md.avro"),
        &[v2_entry(
            0,
            &format!("file://{}/data/part.parquet", dir.path().display()),
            "us-west",
            1,
        )],
    );
    write_manifest_list(
        &dir.path().join("metadata/snap-1.avro"),
        &[format!("file://{}/metadata/m0.avro", dir.path().display())],
        false,
    );
    write_metadata_v2(
        dir.path(),
        1,
        1,
        serde_json::json!([{
            "snapshot-id": 1,
            "sequence-number": 1,
            "manifest-list": format!("file://{}/metadata/snap-1.avro", dir.path().display())
        }]),
        "identity",
    );
    // Rewrite the manifest entry as content=1 (position deletes): a
    // dedicated entry mutated at the content byte.
    let manifest = dir.path().join("metadata/m0.avro");
    let mut delete_entry = v2_entry(
        0,
        &format!("file://{}/data/part.parquet", dir.path().display()),
        "us-west",
        1,
    );
    // entry layout: status(int) union-null content(int)... content is
    // the 4th encoded field: status(1B) + union(1B) + content(1B).
    delete_entry[2] = 1; // content = 1 (position deletes)
    write_v2_manifest(&manifest, &[delete_entry]);
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("delete-file"), "{err}");
}

#[test]
fn empty_table_has_no_current_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    write_metadata_v2(dir.path(), 1, -1, serde_json::json!([]), "identity");
    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.snapshot_id, None);
    assert_eq!(scan.files.len(), 0);
}

#[test]
fn remote_object_store_paths_fail_closed() {
    let err =
        exocortex_adapter_iceberg::scan_table(std::path::Path::new("/nonexistent")).unwrap_err();
    assert!(err.to_string().contains("no metadata"), "{err}");
    // The scheme refusal is a unit-level behavior of path resolution;
    // exercising it through a metadata file whose snapshot list points
    // at s3://.
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    write_metadata_v2(
        dir.path(),
        1,
        5,
        serde_json::json!([{
            "snapshot-id": 5,
            "sequence-number": 1,
            "manifest-list": "s3://bucket/warehouse/snap-5.avro"
        }]),
        "identity",
    );
    let err = format!("{:#}", scan_table(dir.path()).unwrap_err());
    assert!(err.contains("remote object-store"), "{err}");
}

#[test]
fn v1_name_keyed_manifests_roundtrip_through_a_real_avro_writer() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    write_data_file(
        &dir.path().join("data/part.parquet"),
        &[(1, Some("v1 row"), "name-keyed partitions", "low", "", None)],
    );
    write_v1_manifest(
        &dir.path().join("metadata/m0.avro"),
        &[(
            0,
            &format!("file://{}/data/part.parquet", dir.path().display()),
            "eu-central",
            1,
        )],
    );
    write_manifest_list(
        &dir.path().join("metadata/snap-1.avro"),
        &[format!("file://{}/metadata/m0.avro", dir.path().display())],
        false,
    );
    let metadata = serde_json::json!({
        "format-version": 1,
        "table-uuid": TABLE_UUID,
        "current-snapshot-id": 1,
        "schema": {"type": "struct", "fields": schema_fields()},
        "partition-spec": [{"name": "region", "transform": "identity", "source-id": 7, "field-id": 1000}],
        "snapshots": [{
            "snapshot-id": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "manifest-list": format!("file://{}/metadata/snap-1.avro", dir.path().display())
        }]
    });
    std::fs::write(
        dir.path().join("metadata/v1.metadata.json"),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("metadata/version-hint.text"), "1").unwrap();

    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.format_version, 1);
    assert_eq!(scan.snapshot_id, Some(1));
    assert_eq!(scan.files.len(), 1);
    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    assert_eq!(rows.len(), 1);
    let region = rows[0]
        .content
        .iter()
        .find(|(c, _)| c == "region")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(region, "eu-central");
}

fn config_for(
    url: &str,
    cursor: std::path::PathBuf,
    map: &Mapping,
    scan: &exocortex_adapter_iceberg::IcebergScan,
    max_window: u64,
) -> exocortex_adapter_sdk::AdapterConfig {
    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        "org",
        &format!("iceberg://{TABLE_UUID}"),
        "iceberg-adapter",
        url,
    );
    config.source_flavor = "iceberg".into();
    config.auth_token = "test-bearer".into();
    config.hmac_key = [7u8; 32];
    config.cursor_path = cursor;
    config.projection = Some(projection(map, scan, max_window));
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn table_flavor_registration_and_submit_end_to_end() {
    let dir = v2_table();
    let scan = scan_table(dir.path()).unwrap();
    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    // Connect under the TABLE flavor: the mock (like the real server)
    // refuses this registration without the declared projection.
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
    assert_eq!(registrations[0].source_flavor, "iceberg");
    let wire_projection = registrations[0].projection.as_ref().unwrap();
    assert!(wire_projection.selector.contains(TABLE_UUID));
    assert!(wire_projection.selector.contains("9001"));
    assert_eq!(
        wire_projection.bounds.as_ref().unwrap().max_rows_per_window,
        256
    );
    assert!(wire_projection
        .fields
        .iter()
        .any(|f| f.source_field == "parent_id" && f.kind == "Causes"));

    let (rows, _) = read_rows(&mapping(), &scan).unwrap();
    let declared = declared_columns(&mapping(), &scan);
    let table = table_uuid_for(TABLE_UUID);
    let (unit, _) = map_rows(&mapping(), &table, &declared, &rows, "window-0");
    let snapshot = scan.snapshot_id_string().unwrap();
    let unit = with_snapshot_id(unit, &snapshot);
    mock.push_script(vec![MockSubmit::Accept]);
    let outcome = session.submit_window(vec![unit], &snapshot).await.unwrap();
    assert_eq!(outcome.accepted, 4);
    assert!(outcome.cursor_advanced);
    assert_eq!(
        std::fs::read_to_string(cursor_dir.path().join("c.cursor")).unwrap(),
        snapshot
    );

    // The submitted batch: canonical 32-byte schema hash over the
    // declared columns (typed by the TABLE schema, "long"/"string"),
    // 16-byte table uuids, Iceberg's snapshot id, the `iceberg` flavor
    // tag, and the cross-file Causes edge.
    let submitted = mock.submitted();
    assert_eq!(submitted.len(), 1);
    let wire_snapshot = submitted[0].snapshot.as_ref().unwrap();
    assert_eq!(wire_snapshot.schema_hash.len(), 32);
    assert_eq!(
        wire_snapshot.schema_hash,
        exocortex_wire::projection::schema_hash(&declared).to_vec()
    );
    assert_eq!(wire_snapshot.snapshot_id, snapshot);
    assert_eq!(wire_snapshot.source_flavor, "iceberg");
    assert!(submitted[0].memories.iter().all(|m| m
        .external_key
        .as_ref()
        .is_some_and(|k| k.table_uuid.len() == 16)));
    assert!(submitted[0]
        .memories
        .iter()
        .all(|m| m.tags.contains(&"iceberg".to_string())));
    assert_eq!(submitted[0].relationships.len(), 1);
    assert_eq!(submitted[0].relationships[0].kind, "Causes");
    mock.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn new_snapshot_ingests_and_rewind_is_refused() {
    let dir = v2_table();
    // Snapshot 9001 as shipped: settle it FIRST so the session has
    // seen the table's history in order.
    let scan_9001 = scan_table(dir.path()).unwrap();
    assert_eq!(scan_9001.snapshot_id, Some(9001));
    assert_eq!(scan_9001.files.len(), 2);

    // Commit 2: one more data file under a new snapshot 9002.
    write_data_file(
        &dir.path().join("data/part-00002-c.parquet"),
        &[(9, Some("late finding"), "arrived later", "low", "", None)],
    );
    write_manifest_list(
        &dir.path().join("metadata/snap-9002.avro"),
        &[
            format!("file://{}/metadata/m0.avro", dir.path().display()),
            format!("file://{}/metadata/m1.avro", dir.path().display()),
        ],
        false,
    );
    write_v2_manifest(
        &dir.path().join("metadata/m2.avro"),
        &[v2_entry(
            1,
            &format!("file://{}/data/part-00002-c.parquet", dir.path().display()),
            "us-west",
            1,
        )],
    );
    write_manifest_list(
        &dir.path().join("metadata/snap-9002.avro"),
        &[
            format!("file://{}/metadata/m0.avro", dir.path().display()),
            format!("file://{}/metadata/m1.avro", dir.path().display()),
            format!("file://{}/metadata/m2.avro", dir.path().display()),
        ],
        false,
    );
    write_metadata_v2(
        dir.path(),
        2,
        9002,
        serde_json::json!([
            {"snapshot-id": 9001, "sequence-number": 1,
             "manifest-list": format!("file://{}/metadata/snap-9001.avro", dir.path().display())},
            {"snapshot-id": 9002, "sequence-number": 2,
             "manifest-list": format!("file://{}/metadata/snap-9002.avro", dir.path().display())}
        ]),
        "identity",
    );

    let scan = scan_table(dir.path()).unwrap();
    assert_eq!(scan.snapshot_id, Some(9002));
    assert_eq!(
        scan.files.len(),
        3,
        "the new snapshot carries all live files"
    );

    let mock = MockServer::start().await;
    let cursor_dir = tempfile::tempdir().unwrap();
    let cursor = cursor_dir.path().join("c.cursor");
    let mut session = AdapterSession::connect_with(
        config_for(&mock.url(), cursor.clone(), &mapping(), &scan, 256),
        exocortex_adapter_sdk::instant_sleep(),
    )
    .await
    .unwrap();

    let build = |scan: &exocortex_adapter_iceberg::IcebergScan, seed: &str| {
        let (rows, _) = read_rows(&mapping(), scan).unwrap();
        let declared = declared_columns(&mapping(), scan);
        let (unit, _) = map_rows(
            &mapping(),
            &table_uuid_for(TABLE_UUID),
            &declared,
            &rows,
            seed,
        );
        with_snapshot_id(unit, &scan.snapshot_id_string().unwrap())
    };

    mock.push_script(vec![MockSubmit::Accept, MockSubmit::Accept]);
    // The table's history settles in order: snapshot 9001, then 9002.
    session
        .submit_window(
            vec![build(&scan_9001, "window-0")],
            &scan_9001.snapshot_id_string().unwrap(),
        )
        .await
        .unwrap();
    session
        .submit_window(
            vec![build(&scan, "window-1")],
            &scan.snapshot_id_string().unwrap(),
        )
        .await
        .unwrap();

    // The table regresses: someone restores metadata v1 (snapshot
    // 9001, already superseded). The window refuses BEFORE any wire
    // traffic — a rewound source needs an operator.
    std::fs::write(dir.path().join("metadata/version-hint.text"), "1").unwrap();
    let regressed = scan_table(dir.path()).unwrap();
    assert_eq!(regressed.snapshot_id, Some(9001));
    let submits_before = mock.submitted().len();
    let err = match session
        .submit_window(
            vec![build(&regressed, "window-2")],
            &regressed.snapshot_id_string().unwrap(),
        )
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("the superseded snapshot must be refused"),
    };
    match &err {
        SdkError::SourceRewound { observed, last } => {
            assert_eq!(observed, "9001");
            assert_eq!(last, "9002");
        }
        other => panic!("expected SourceRewound, got {other:?}"),
    }
    assert_eq!(mock.submitted().len(), submits_before);
    assert_eq!(
        std::fs::read_to_string(&cursor).unwrap(),
        "9002",
        "the cursor never regressed with the table"
    );
    mock.stop();
}
