//! D1 (master plan; core PRD §18.4, adapter-contract PRD D21): the
//! Delta-table adapter, `delta` source flavor.
//!
//! A LOCAL Delta table's current state as a BOUNDED, declared-
//! projection import through the signed Ingestion Protocol. The
//! `_delta_log` transaction log IS the table: this adapter replays it
//! directly — JSON commit files, plus parquet checkpoints (single or
//! multi-part) with the JSON commits after the checkpoint — and
//! transcribes the live file set deterministically. No engine, no
//! catalog client, no LLM, no network beyond the backend:
//!
//! - one memory per row of every LIVE data file, typed and titled by
//!   the operator's declared column mapping (never guessed),
//! - identity by `ExternalKey` (`table_uuid` from the log's own
//!   `metaData.id`, `logical_pk` the declared pk columns), so re-runs
//!   are idempotent by construction,
//! - snapshot identity is the Delta LOG VERSION (`v<N>`), which is
//!   monotonic by construction — a table whose log regressed below
//!   the settled cursor is refused locally before any wire traffic,
//!   and the server's rewind history covers what a cold session
//!   cannot see,
//! - partition columns resolve from each `add` action's
//!   `partitionValues` when the writer omitted them from the data
//!   files (Delta partition values ARE the column values — every
//!   Delta partition is identity).
//!
//! Protocol discipline, fail-closed: this reader transcribes CLASSIC
//! Delta layouts. `minReaderVersion` above 2, the deletion-vector /
//! v2-checkpoint table features, and any `delta.columnMapping.*`
//! configuration change what the bytes MEAN (physical column names
//! remapped under column mapping; deleted rows retained inside data
//! files under deletion vectors) — such tables are refused with the
//! protocol named, not misread.
//!
//! The official `deltalake` crate stays deny.toml-banned BY CHOICE
//! (PUBLISHING.md): it drags a datafusion-grade engine and a second
//! arrow line into a leaf binary whose job is a schema-faithful
//! transcription. No new external dependency rides this crate — the
//! log is JSON (serde_json) and parquet (the pinned arrow stack),
//! both already workspace pins.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{Array, StringArray, StructArray};
use exocortex_adapter_sdk::Projection;
use exocortex_adapter_table::Mapping;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

pub use exocortex_adapter_table::{table_uuid_for, Row};

// ---------------------------------------------------------------------------
// The table scan
// ---------------------------------------------------------------------------

/// One live data file of the current table state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaDataFile {
    /// Resolved local path of the Parquet data file (Delta `add.path`
    /// is table-relative with URL-escaped separators; both are
    /// handled at resolution).
    pub path: PathBuf,
    /// Partition values keyed by column name (all Delta partitions
    /// are identity — the value IS the column's value).
    pub partition: BTreeMap<String, Option<String>>,
}

/// The current table state after a full log replay.
#[derive(Clone, Debug)]
pub struct DeltaScan {
    /// The log's own `metaData.id`.
    pub table_id: String,
    /// The latest settled log version (monotonic by construction).
    pub version: u64,
    /// Live data files, sorted by path.
    pub files: Vec<DeltaDataFile>,
    /// The table schema from `metaData.schemaString`:
    /// (column, rendered type) in declaration order.
    pub columns: Vec<(String, String)>,
    /// The partition columns from `metaData.partitionColumns`.
    pub partition_columns: Vec<String>,
}

impl DeltaScan {
    /// The D21 snapshot string (`v<N>`).
    pub fn snapshot_id_string(&self) -> String {
        format!("v{}", self.version)
    }
}

/// Replay the `_delta_log` of a local Delta table to its current
/// state: the latest checkpoint (if any) plus every JSON commit after
/// it, in order.
pub fn scan_table(table_dir: &Path) -> Result<DeltaScan> {
    let log_dir = table_dir.join("_delta_log");
    if !log_dir.is_dir() {
        bail!(
            "{} is not a Delta table (no _delta_log/ directory)",
            table_dir.display()
        );
    }

    // The commit versions present as JSON.
    let mut json_versions: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(&log_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(version) = name
            .strip_suffix(".json")
            .and_then(|digits| digits.parse::<u64>().ok())
        {
            json_versions.push(version);
        }
    }
    json_versions.sort_unstable();
    let last_version = json_versions.last().copied().ok_or_else(|| {
        anyhow!(
            "no JSON commit files under {} — a Delta table always has version 0",
            log_dir.display()
        )
    })?;

    // The latest checkpoint, if one exists: single
    // `<N>.checkpoint.parquet` or multi-part
    // `<N>.checkpoint.<i>.<total>.parquet`.
    let checkpoint = latest_checkpoint(&log_dir, last_version)?;
    // With a checkpoint at N, the checkpoint covers commits 0..=N,
    // so JSON commits N+1.. replay on top; without one, the replay
    // starts at version 0 (where metaData and protocol live).
    let replay_from = checkpoint.as_ref().map(|c| c.version + 1).unwrap_or(0);
    if checkpoint.is_none() && !json_versions.contains(&0) && json_versions[0] != 0 {
        bail!(
            "the log starts at version {} with no checkpoint at or below it — the log is incomplete",
            json_versions[0]
        );
    }

    let mut live: BTreeMap<String, DeltaDataFile> = BTreeMap::new();
    let mut meta: Option<DeltaMeta> = None;
    let mut protocol: Option<serde_json::Value> = None;

    if let Some(checkpoint) = &checkpoint {
        let checkpoint_meta = replay_checkpoint(&log_dir, checkpoint, &mut live)
            .with_context(|| format!("replaying checkpoint at version {}", checkpoint.version))?;
        meta = checkpoint_meta.meta;
        protocol = checkpoint_meta.protocol;
    }
    for version in json_versions.iter().copied().filter(|v| *v >= replay_from) {
        apply_commit(&log_dir, version, &mut live, &mut meta, &mut protocol)
            .with_context(|| format!("applying commit {version}"))?;
    }

    let Some(meta) = meta else {
        bail!("the log carries no metaData action — every table defines one at version 0");
    };
    check_protocol(&protocol)?;
    check_column_mapping(&meta)?;
    let columns = render_schema(&meta.schema_json)?;
    let files: Vec<DeltaDataFile> = live.into_values().collect();
    Ok(DeltaScan {
        table_id: meta.id,
        version: last_version,
        files,
        columns,
        partition_columns: meta.partition_columns,
    })
}

struct DeltaMeta {
    id: String,
    schema_json: serde_json::Value,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
}

struct ReplayMeta {
    meta: Option<DeltaMeta>,
    protocol: Option<serde_json::Value>,
}

struct CheckpointRef {
    version: u64,
    /// The part file names, in part order (one entry for a classic
    /// single-file checkpoint).
    parts: Vec<String>,
}

fn latest_checkpoint(log_dir: &Path, last_version: u64) -> Result<Option<CheckpointRef>> {
    check_last_checkpoint_hint(log_dir)?;
    // The commit versions present as JSON.
    let _ = last_version;
    // Single `<N>.checkpoint.parquet` or multi-part
    // `<N>.checkpoint.<i>.<total>.parquet` — whichever exists on disk
    // is authoritative; `_last_checkpoint` is only a hint.
    let mut single: Option<(u64, String)> = None;
    let mut multi: BTreeMap<(u64, u64), BTreeMap<u64, String>> = BTreeMap::new();
    for entry in std::fs::read_dir(log_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(rest) = name.strip_suffix(".checkpoint.parquet") {
            if let Ok(version) = rest.parse::<u64>() {
                if single.as_ref().is_none_or(|(v, _)| version > *v) {
                    single = Some((version, name.to_string()));
                }
                continue;
            }
        }
        // <N>.checkpoint.<i>.<total>.parquet — the TOTAL rides every
        // part's own name, so a missing part is detectable from the
        // survivors, not assumed from their count.
        if let Some(rest) = name.strip_suffix(".parquet") {
            let segments: Vec<&str> = rest.split('.').collect();
            if segments.len() == 4 && segments[1] == "checkpoint" {
                if let (Some(version), Some(part), Some(total)) = (
                    segments[0].parse::<u64>().ok(),
                    segments[2].parse::<u64>().ok(),
                    segments[3].parse::<u64>().ok(),
                ) {
                    if part < total && total > 0 {
                        multi
                            .entry((version, total))
                            .or_default()
                            .insert(part, name.to_string());
                    }
                }
            }
        }
    }
    if let Some((version, name)) = single {
        if multi.keys().any(|(v, _)| *v == version) {
            bail!(
                "both a single-file and a multi-part checkpoint exist at version {version} — \
                 a Delta writer writes exactly one shape; the log is inconsistent"
            );
        }
        return Ok(Some(CheckpointRef {
            version,
            parts: vec![name],
        }));
    }
    if let Some((&(version, total), parts)) = multi.iter().last() {
        if parts.len() as u64 != total || !parts.keys().copied().eq(0..total) {
            bail!(
                "multi-part checkpoint at version {version} is missing parts (found {}, \
                 expected {total})",
                parts.len()
            );
        }
        let ordered = (0..total)
            .map(|i| {
                parts
                    .get(&i)
                    .cloned()
                    .ok_or_else(|| anyhow!("checkpoint part {i} absent"))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Some(CheckpointRef {
            version,
            parts: ordered,
        }));
    }
    Ok(None)
}

/// The `_last_checkpoint` hint, when present. It is a HINT for
/// readers; the authoritative file set is the directory scan above.
/// Its one fail-closed duty: a v2-checkpoint pointer (sidecar files)
/// names a shape this reader does not support, and silently ignoring
/// it could replay a stale state instead.
fn check_last_checkpoint_hint(log_dir: &Path) -> Result<()> {
    let Ok(text) = std::fs::read_to_string(log_dir.join("_last_checkpoint")) else {
        return Ok(());
    };
    let json: serde_json::Value =
        serde_json::from_str(&text).with_context(|| "parsing _last_checkpoint")?;
    if json
        .get("v2Checkpoint")
        .and_then(|v| v.get("sidecarFiles"))
        .is_some_and(|s| s.as_array().is_some_and(|a| !a.is_empty()))
    {
        bail!(
            "_last_checkpoint points at a v2 checkpoint (sidecar files) — outside this \
             reader's classic checkpoint support; refusing rather than replay a stale state"
        );
    }
    Ok(())
}

/// Resolve a Delta `add.path`: table-relative with URL-escaped `%20`
/// separators and `/` splits, or an absolute/local path from a
/// relocated log. Escapes are decoded conservatively (the standard
/// `%XX` set); any OTHER scheme is refused as outside the local
/// boundary.
fn resolve_path(table_dir: &Path, raw: &str) -> Result<PathBuf> {
    if raw.contains("://") && !raw.starts_with("file://") {
        bail!(
            "data path {raw} is a remote object-store URI — this reader's boundary is the \
             local filesystem; object stores are a deliberate non-goal of this flavor"
        );
    }
    let raw = raw.strip_prefix("file://").unwrap_or(raw);
    let decoded = percent_decode(raw)?;
    let candidate = PathBuf::from(&decoded);
    if candidate.is_absolute() {
        return Ok(candidate);
    }
    Ok(table_dir.join(decoded))
}

fn percent_decode(input: &str) -> Result<String> {
    exocortex_adapter_table::percent_decode(input)
}

fn partition_overlay(
    partition_values: &serde_json::Value,
) -> Result<BTreeMap<String, Option<String>>> {
    let mut overlay = BTreeMap::new();
    if let Some(map) = partition_values.as_object() {
        for (column, value) in map {
            // Delta encodes partition values as strings in the log
            // (null stays JSON null — the column's value was null).
            match value {
                serde_json::Value::Null => {
                    overlay.insert(column.clone(), None);
                }
                serde_json::Value::String(text) => {
                    overlay.insert(column.clone(), Some(text.clone()));
                }
                other => bail!(
                    "partition value for {column} is {other:?} — the Delta log writes \
                     partition values as strings; refusing the malformed shape"
                ),
            }
        }
    }
    Ok(overlay)
}

fn apply_add(
    table_dir: &Path,
    add: &serde_json::Value,
    live: &mut BTreeMap<String, DeltaDataFile>,
) -> Result<()> {
    let Some(raw_path) = add.get("path").and_then(|v| v.as_str()) else {
        bail!("add action carries no path");
    };
    let path = resolve_path(table_dir, raw_path)?;
    let partition = partition_overlay(
        add.get("partitionValues")
            .unwrap_or(&serde_json::Value::Null),
    )?;
    live.insert(
        path.to_string_lossy().into_owned(),
        DeltaDataFile { path, partition },
    );
    Ok(())
}

fn apply_commit(
    log_dir: &Path,
    version: u64,
    live: &mut BTreeMap<String, DeltaDataFile>,
    meta: &mut Option<DeltaMeta>,
    protocol: &mut Option<serde_json::Value>,
) -> Result<()> {
    let path = log_dir.join(format!("{version:020}.json"));
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let action: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("parsing commit {version} action"))?;
        // One action per line; unknown action names are skipped — the
        // Delta protocol requires readers to ignore actions they do
        // not know, and guessing at one would be worse.
        if let Some(add) = action.get("add") {
            apply_add(log_dir.parent().unwrap_or(log_dir), add, live)
                .with_context(|| format!("commit {version} add action"))?;
        } else if let Some(remove) = action.get("remove") {
            if let Some(raw_path) = remove.get("path").and_then(|v| v.as_str()) {
                let resolved = resolve_path(log_dir.parent().unwrap_or(log_dir), raw_path)?;
                live.remove(&resolved.to_string_lossy().into_owned());
            }
        } else if let Some(meta_data) = action.get("metaData") {
            *meta = Some(parse_meta(meta_data)?);
        } else if let Some(protocol_action) = action.get("protocol") {
            *protocol = Some(protocol_action.clone());
        }
    }
    Ok(())
}

fn parse_meta(meta_data: &serde_json::Value) -> Result<DeltaMeta> {
    let id = meta_data
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("metaData carries no id"))?
        .to_string();
    let schema_string = meta_data
        .get("schemaString")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("metaData carries no schemaString"))?;
    let schema_json: serde_json::Value = serde_json::from_str(schema_string)
        .with_context(|| format!("parsing metaData.schemaString of table {id}"))?;
    let partition_columns = meta_data
        .get("partitionColumns")
        .and_then(|v| v.as_array())
        .map(|columns| {
            columns
                .iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let mut configuration = BTreeMap::new();
    if let Some(map) = meta_data.get("configuration").and_then(|v| v.as_object()) {
        for (key, value) in map {
            if let Some(text) = value.as_str() {
                configuration.insert(key.clone(), text.to_string());
            }
        }
    }
    Ok(DeltaMeta {
        id,
        schema_json,
        partition_columns,
        configuration,
    })
}

/// Fail closed on protocol shapes this reader does not transcribe:
/// reader versions above 2, the deletion-vector and v2-checkpoint
/// table features, and column-mapping configuration (they change
/// what the bytes mean: remapped physical names; deleted rows
/// retained inside data files).
fn check_protocol(protocol: &Option<serde_json::Value>) -> Result<()> {
    let Some(protocol) = protocol else {
        return Ok(());
    };
    // Features are checked BEFORE the version number: a v3 table
    // carrying deletionVectors should refuse naming the FEATURE (the
    // operationally meaningful fact), not the version it rides on.
    let refuses_feature = |feature: &str| {
        protocol
            .get("readerFeatures")
            .or_else(|| protocol.get("writerFeatures"))
            .and_then(|v| v.as_array())
            .is_some_and(|features| {
                features
                    .iter()
                    .any(|f| f.as_str().is_some_and(|name| name == feature))
            })
    };
    if refuses_feature("deletionVectors") {
        bail!("table feature deletionVectors is set — deleted rows would remain inside data files; this reader refuses to present them as live");
    }
    if refuses_feature("v2Checkpoint") {
        bail!("table feature v2Checkpoint is set — sidecar-file checkpoints are outside this reader's classic checkpoint support");
    }
    let min_reader = protocol
        .get("minReaderVersion")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    if min_reader > 2 {
        bail!(
            "Delta protocol minReaderVersion {min_reader} exceeds this reader's supported set \
             (<= 2) — newer table features change what the bytes mean; refusing rather than \
             misreading"
        );
    }
    Ok(())
}

fn check_column_mapping(meta: &DeltaMeta) -> Result<()> {
    if meta
        .configuration
        .keys()
        .any(|key| key.starts_with("delta.columnMapping"))
    {
        bail!(
            "delta.columnMapping configuration is set — physical parquet column names are \
             remapped and transcribing them as logical names would lie about the source"
        );
    }
    Ok(())
}

/// Render a Spark-struct schema type as a deterministic string (the
/// Delta analogue of the iceberg adapter's rendering: primitives pass
/// through in the source's own spelling; decimal carries its
/// parameters; struct/array/map render structurally).
fn render_type(ty: &serde_json::Value) -> Result<String> {
    match ty {
        serde_json::Value::String(primitive) => Ok(primitive.clone()),
        serde_json::Value::Object(object) => match object.get("type").and_then(|v| v.as_str()) {
            Some("decimal") => Ok(format!(
                "decimal({},{})",
                object
                    .get("precision")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                object.get("scale").and_then(|v| v.as_u64()).unwrap_or(0)
            )),
            Some("struct") => {
                let mut parts = Vec::new();
                for field in object
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .unwrap_or(&Vec::new())
                {
                    let name = field.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    parts.push(format!(
                        "{name}:{}",
                        render_type(field.get("type").unwrap_or(&serde_json::Value::Null))?
                    ));
                }
                Ok(format!("struct<{}>", parts.join(",")))
            }
            Some("array") => Ok(format!(
                "array<{}>",
                render_type(
                    object
                        .get("elementType")
                        .unwrap_or(&serde_json::Value::Null)
                )?
            )),
            Some("map") => Ok(format!(
                "map<{},{}>",
                render_type(object.get("keyType").unwrap_or(&serde_json::Value::Null))?,
                render_type(object.get("valueType").unwrap_or(&serde_json::Value::Null))?
            )),
            other => bail!("unsupported Delta type object: {other:?}"),
        },
        other => bail!("unsupported Delta type: {other:?}"),
    }
}

fn render_schema(schema: &serde_json::Value) -> Result<Vec<(String, String)>> {
    let fields = schema
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("schemaString carries no fields"))?;
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        let name = field
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("schema field carries no name"))?;
        let ty = field
            .get("type")
            .ok_or_else(|| anyhow!("schema field {name} carries no type"))?;
        out.push((name.to_string(), render_type(ty)?));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Checkpoint replay (parquet; nested action columns)
// ---------------------------------------------------------------------------

fn string_at(array: &dyn Array, row: usize) -> Option<String> {
    let strings = array.as_any().downcast_ref::<StringArray>()?;
    if strings.is_null(row) {
        return None;
    }
    Some(strings.value(row).to_string())
}

fn struct_field<'a>(structure: &'a StructArray, name: &str) -> Option<&'a dyn Array> {
    let index = structure
        .fields()
        .iter()
        .position(|field| field.name() == name)?;
    Some(structure.column(index))
}

/// Read one row's `partitionValues` map out of a MapArray (the
/// checkpoint representation of Delta's partition struct).
fn map_at(array: &dyn Array, row: usize) -> Option<BTreeMap<String, Option<String>>> {
    use arrow_array::MapArray;
    let map = array.as_any().downcast_ref::<MapArray>()?;
    if map.is_null(row) {
        return Some(BTreeMap::new());
    }
    let offsets = map.offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let keys = map.keys();
    let values = map.values();
    let mut out = BTreeMap::new();
    for entry in start..end {
        let key = string_at(keys, entry)?;
        let value = string_at(values, entry);
        out.insert(key, value);
    }
    Some(out)
}

fn replay_checkpoint(
    log_dir: &Path,
    checkpoint: &CheckpointRef,
    live: &mut BTreeMap<String, DeltaDataFile>,
) -> Result<ReplayMeta> {
    let table_dir = log_dir
        .parent()
        .ok_or_else(|| anyhow!("the log directory has no parent"))?;
    let mut replay = ReplayMeta {
        meta: None,
        protocol: None,
    };
    for part in &checkpoint.parts {
        let path = log_dir.join(part);
        let handle =
            std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let reader: ParquetRecordBatchReader = ParquetRecordBatchReaderBuilder::try_new(handle)
            .with_context(|| format!("reading parquet metadata {}", path.display()))?
            .with_batch_size(1024)
            .build()
            .with_context(|| format!("building parquet reader {}", path.display()))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("reading checkpoint {}", path.display()))?;
            let schema = batch.schema();
            for name in ["add", "remove", "metaData", "protocol"] {
                if schema.index_of(name).is_err() {
                    bail!(
                        "checkpoint {} carries no `{name}` column — the classic checkpoint \
                         schema defines all four action columns",
                        path.display()
                    );
                }
            }
            let add = batch
                .column(schema.index_of("add").expect("checked above"))
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("checkpoint add column is not a struct"))?;
            let remove = batch
                .column(schema.index_of("remove").expect("checked above"))
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("checkpoint remove column is not a struct"))?;
            let meta_data = batch
                .column(schema.index_of("metaData").expect("checked above"))
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("checkpoint metaData column is not a struct"))?;
            let add_path = struct_field(add, "path")
                .ok_or_else(|| anyhow!("checkpoint add struct carries no path"))?;
            let add_partitions = struct_field(add, "partitionValues")
                .ok_or_else(|| anyhow!("checkpoint add struct carries no partitionValues"))?;
            let remove_path = struct_field(remove, "path")
                .ok_or_else(|| anyhow!("checkpoint remove struct carries no path"))?;
            let meta_id = struct_field(meta_data, "id")
                .ok_or_else(|| anyhow!("checkpoint metaData struct carries no id"))?;
            let meta_schema = struct_field(meta_data, "schemaString")
                .ok_or_else(|| anyhow!("checkpoint metaData struct carries no schemaString"))?;

            for row in 0..batch.num_rows() {
                if !add.is_null(row) {
                    let raw = string_at(add_path, row)
                        .ok_or_else(|| anyhow!("checkpoint add row {row} carries no path"))?;
                    let path = resolve_path(table_dir, &raw)?;
                    let partition = map_at(add_partitions, row)
                        .ok_or_else(|| anyhow!("checkpoint partitionValues is not a map"))?
                        .into_iter()
                        .collect::<BTreeMap<String, Option<String>>>();
                    live.insert(
                        path.to_string_lossy().into_owned(),
                        DeltaDataFile { path, partition },
                    );
                }
                if !remove.is_null(row) {
                    if let Some(raw) = string_at(remove_path, row) {
                        let resolved = resolve_path(table_dir, &raw)?;
                        live.remove(&resolved.to_string_lossy().into_owned());
                    }
                }
                if !meta_data.is_null(row) {
                    let id = string_at(meta_id, row)
                        .ok_or_else(|| anyhow!("checkpoint metaData row carries no id"))?;
                    let schema_string = string_at(meta_schema, row).ok_or_else(|| {
                        anyhow!("checkpoint metaData row carries no schemaString")
                    })?;
                    let schema_json: serde_json::Value = serde_json::from_str(&schema_string)
                        .with_context(|| format!("parsing checkpoint schemaString of {id}"))?;
                    // Partition columns/configuration ride the JSON
                    // commit path when they exist; a partitioned table
                    // that checkpoints before any JSON replay carries
                    // them in the checkpoint too — read them when
                    // present, else leave the earlier meta standing.
                    let mut partition_columns = replay
                        .meta
                        .as_ref()
                        .map(|m| m.partition_columns.clone())
                        .unwrap_or_default();
                    if let Some(columns) = struct_field(meta_data, "partitionColumns") {
                        if let Some(list) =
                            columns.as_any().downcast_ref::<arrow_array::ListArray>()
                        {
                            if !list.is_null(row) {
                                partition_columns = (list.offsets()[row] as usize
                                    ..list.offsets()[row + 1] as usize)
                                    .filter_map(|i| string_at(list.values(), i))
                                    .collect();
                            }
                        }
                    }
                    let configuration = struct_field(meta_data, "configuration")
                        .and_then(|config| map_at(config, row))
                        .map(|map| {
                            map.into_iter()
                                .filter_map(|(k, v)| v.map(|v| (k, v)))
                                .collect::<BTreeMap<String, String>>()
                        })
                        .unwrap_or_default();
                    replay.meta = Some(DeltaMeta {
                        id,
                        schema_json,
                        partition_columns,
                        configuration,
                    });
                }
            }
            // protocol actions: the checkpoint's protocol column is a
            // struct carrying minReaderVersion; read the first
            // non-null (int32 or int64).
            if replay.protocol.is_none() {
                let protocol = batch
                    .column(schema.index_of("protocol").expect("checked above"))
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| anyhow!("checkpoint protocol column is not a struct"))?;
                if let Some(min_reader) = struct_field(protocol, "minReaderVersion") {
                    let as_i64 = |row: usize| -> Result<Option<i64>> {
                        if let Some(ints) = min_reader
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                        {
                            return Ok((!ints.is_null(row)).then(|| i64::from(ints.value(row))));
                        }
                        if let Some(ints) = min_reader
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                        {
                            return Ok((!ints.is_null(row)).then(|| ints.value(row)));
                        }
                        bail!("checkpoint minReaderVersion is neither int32 nor int64")
                    };
                    for row in 0..batch.num_rows() {
                        if !protocol.is_null(row) {
                            if let Some(value) = as_i64(row)? {
                                replay.protocol =
                                    Some(serde_json::json!({ "minReaderVersion": value }));
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(meta) = &replay.meta {
        check_column_mapping(meta)?;
    }
    Ok(replay)
}

// ---------------------------------------------------------------------------
// Mapping, rows, projection
// ---------------------------------------------------------------------------

/// The declared column set: the mapped columns in declared order,
/// typed by the TABLE schema (D21-d: an unmapped schema addition is
/// accepted; a mapped removal/retype/rename moves the hash and fails
/// closed).
pub fn declared_columns(mapping: &Mapping, scan: &DeltaScan) -> Vec<(String, String)> {
    let observed: BTreeMap<&str, &str> = scan
        .columns
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    mapping
        .referenced_columns()
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                observed.get(name).unwrap_or(&"unobserved").to_string(),
            )
        })
        .collect()
}

/// Fail fast, locally, before any wire traffic: the mapping must
/// reference table columns.
pub fn validate_mapping(mapping: &Mapping, scan: &DeltaScan) -> Result<()> {
    mapping.validate_shape()?;
    let schema_columns: BTreeMap<&str, &str> = scan
        .columns
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    for column in mapping.referenced_columns() {
        if !schema_columns.contains_key(column) {
            bail!(
                "mapped column `{column}` is absent from the table schema (observed: {})",
                scan.columns
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    Ok(())
}

/// Read every row of every live data file, in sorted file order then
/// row order, with each file's partition overlay. Null pk cells skip
/// the row (counted, never guessed).
pub fn read_rows(mapping: &Mapping, scan: &DeltaScan) -> Result<(Vec<Row>, usize)> {
    mapping.validate_shape()?;
    let mut rows = Vec::new();
    let mut skipped = 0usize;
    for file in &scan.files {
        let handle = std::fs::File::open(&file.path)
            .with_context(|| format!("opening {}", file.path.display()))?;
        let reader: ParquetRecordBatchReader = ParquetRecordBatchReaderBuilder::try_new(handle)
            .with_context(|| format!("reading parquet metadata {}", file.path.display()))?
            .with_batch_size(1024)
            .build()
            .with_context(|| format!("building parquet reader {}", file.path.display()))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("reading {}", file.path.display()))?;
            rows.extend(exocortex_adapter_table::read_batch_rows(
                &batch,
                &file.path.display().to_string(),
                mapping,
                &mut skipped,
                &file.partition,
            )?);
        }
    }
    Ok((rows, skipped))
}

/// Map one window of rows to a submission unit (the `delta` flavor
/// tag rides every memory and the snapshot).
pub fn map_rows(
    mapping: &Mapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
) -> (exocortex_adapter_sdk::BatchUnit, usize) {
    exocortex_adapter_table::map_rows(mapping, table_uuid, declared, rows, batch_id_seed, "delta")
}

/// Fill a unit's snapshot id (`v<N>`).
pub fn with_snapshot_id(
    unit: exocortex_adapter_sdk::BatchUnit,
    snapshot_id: &str,
) -> exocortex_adapter_sdk::BatchUnit {
    exocortex_adapter_table::with_snapshot_id(unit, snapshot_id)
}

/// The D21-a projection this adapter declares.
pub fn projection(mapping: &Mapping, scan: &DeltaScan, max_window: u64) -> Projection {
    exocortex_adapter_table::table_projection(
        format!(
            "delta table {} ({} partition columns) at version v{} under the declared column mapping v{}",
            scan.table_id,
            scan.partition_columns.len(),
            scan.version,
            mapping.mapping_version
        ),
        mapping,
        declared_columns(mapping, scan),
        max_window,
        &scan.snapshot_id_string(),
    )
}
