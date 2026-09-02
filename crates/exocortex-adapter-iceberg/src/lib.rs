//! D1 (master plan; core PRD §18.4, adapter-contract PRD D21): the
//! Iceberg-table adapter, `iceberg` source flavor.
//!
//! A LOCAL Iceberg table's current snapshot as a BOUNDED,
//! declared-projection import through the signed Ingestion Protocol.
//! Deterministic transcription only — no catalog client, no compute
//! engine, no LLM, no network beyond the backend:
//!
//! - one memory per row of every LIVE data file of the current
//!   snapshot, typed and titled by the operator's declared column
//!   mapping (never guessed),
//! - identity by `ExternalKey` (`table_uuid` from Iceberg's own
//!   `table-uuid`, `logical_pk` the declared pk columns), so re-runs
//!   are idempotent by construction,
//! - snapshot identity is Iceberg's first-class `current-snapshot-id`
//!   — a later commit is a new snapshot, and a table that presents an
//!   already-superseded snapshot id is a rewind the server refuses,
//! - identity-partitioned columns resolve from the MANIFEST partition
//!   values when the writer omitted them from the data files (the
//!   partition tuple is authoritative); non-identity transforms
//!   (bucket/truncate/month/...) derive values rather than state them
//!   and never back a mapped column — that is stated, fail-closed.
//!
//! The adapter reads the format directly: `metadata.json` (JSON),
//! manifest lists and manifests (Avro object container files), data
//! files (Parquet, through the shared apache-arrow-rs reader). The
//! official `iceberg`/`deltalake`/`duckdb` crates stay
//! deny.toml-banned (the PUBLISHING.md record): they drag a catalog
//! surface and a second arrow line into a leaf binary that needs none
//! of it.
//!
//! The Avro framing is read by this crate, not by apache-avro's OCF
//! `Reader`, for one spec-level reason: Iceberg v2 partition structs
//! key their fields by field-id STRINGS ("1000"), which Avro's name
//! grammar forbids and apache-avro's schema parser rejects ("Invalid
//! field name 1000" — reproduced in this crate's tests). Avro binary
//! is positional, so the framing reader sanitizes the embedded writer
//! schema's names (order-preserving) and decodes with apache-avro
//! against it; well-known entry fields (whose names are always valid)
//! are then read by name, and partition values by position mapped
//! through the table's own partition spec. The `null` and `deflate`
//! codecs (what Iceberg writers emit) are supported; anything else
//! fails closed naming the codec.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use exocortex_adapter_sdk::Projection;
use exocortex_adapter_table::Mapping;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

pub use exocortex_adapter_table::{table_uuid_for, Row};

/// The unit separator joining multi-column logical pks.
pub const PK_SEPARATOR: &str = exocortex_adapter_table::PK_SEPARATOR;

// ---------------------------------------------------------------------------
// The table scan
// ---------------------------------------------------------------------------

/// One live data file of the current snapshot, with its resolved
/// identity-partition overlay (source-column name -> rendered value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDataFile {
    /// Resolved local path of the Parquet data file.
    pub path: PathBuf,
    /// Identity-partition values keyed by SOURCE COLUMN name (the
    /// overlay `read_rows` consults when the file omits the column).
    pub partition: BTreeMap<String, Option<String>>,
    /// The manifest's row count (sanity evidence, not authoritative).
    pub record_count: u64,
}

/// One scan of an Iceberg table's CURRENT state.
#[derive(Clone, Debug)]
pub struct IcebergScan {
    /// The table's own `table-uuid`.
    pub table_uuid: String,
    /// Iceberg format-version (1 or 2; anything else failed closed).
    pub format_version: u32,
    /// The current snapshot id, or `None` when the table has no
    /// current snapshot (a legitimately empty table).
    pub snapshot_id: Option<u64>,
    /// The current snapshot's sequence number (v2), when present.
    pub sequence_number: Option<u64>,
    /// Live data files of the current snapshot, sorted by path.
    pub files: Vec<IcebergDataFile>,
    /// The table schema: (column, rendered type) in schema order.
    pub columns: Vec<(String, String)>,
    /// Every partition field, keyed by SOURCE COLUMN name, with its
    /// transform — `identity` rows back a mapped column; anything else
    /// never does.
    pub partition_fields: BTreeMap<String, String>,
}

impl IcebergScan {
    /// The snapshot id as the D21 snapshot string (`None` on an empty
    /// table — there is nothing to submit, so no identity either).
    pub fn snapshot_id_string(&self) -> Option<String> {
        self.snapshot_id.map(|id| id.to_string())
    }
}

/// Scan a local Iceberg table: the current metadata file, its current
/// snapshot, the snapshot's manifest list and manifests, and the live
/// data-file set with partition overlays.
pub fn scan_table(table_dir: &Path) -> Result<IcebergScan> {
    let metadata_dir = table_dir.join("metadata");
    if !metadata_dir.is_dir() {
        bail!(
            "{} is not an Iceberg table (no metadata/ directory)",
            table_dir.display()
        );
    }
    let metadata_path = current_metadata_file(&metadata_dir)?;
    let metadata_text = std::fs::read_to_string(&metadata_path)
        .with_context(|| format!("reading {}", metadata_path.display()))?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_text)
        .with_context(|| format!("parsing {}", metadata_path.display()))?;

    let format_version = metadata
        .get("format-version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("metadata carries no integer format-version"))?;
    if !matches!(format_version, 1 | 2) {
        bail!(
            "format-version {format_version} is outside this reader's supported set (1, 2) — \
             fail closed rather than misread a newer format"
        );
    }
    let table_uuid = metadata
        .get("table-uuid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("metadata carries no table-uuid"))?
        .to_string();

    // The schema: v2 carries schemas + current-schema-id; v1 carries a
    // single `schema` (accept either spelling on both).
    let schema_json = pick_schema(&metadata)?;
    let columns = render_schema_columns(schema_json)?;
    let column_ids: BTreeMap<i64, String> = schema_json
        .get("fields")
        .and_then(|f| f.as_array())
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| {
                    Some((
                        field.get("id")?.as_i64()?,
                        field.get("name")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();

    // Partition specs: v1 top-level `partition-spec` / v2
    // `partition-specs`. Record each field by SOURCE COLUMN name with
    // its transform, and the field-id -> source-name map for v2
    // partition-struct decoding.
    let mut partition_fields: BTreeMap<String, String> = BTreeMap::new();
    let mut field_id_to_source: BTreeMap<i64, String> = BTreeMap::new();
    let mut field_name_to_source: BTreeMap<String, String> = BTreeMap::new();
    for spec in partition_specs(&metadata)? {
        for field in spec
            .get("fields")
            .and_then(|f| f.as_array())
            .into_iter()
            .flatten()
        {
            let (Some(field_id), Some(source_id), Some(transform), Some(field_name)) = (
                field.get("field-id").and_then(|v| v.as_i64()),
                field.get("source-id").and_then(|v| v.as_i64()),
                field.get("transform").and_then(|v| v.as_str()),
                field.get("name").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            if let Some(source_name) = column_ids.get(&source_id) {
                partition_fields.insert(source_name.clone(), transform.to_string());
                field_id_to_source.insert(field_id, source_name.clone());
                field_name_to_source.insert(field_name.to_string(), source_name.clone());
            }
        }
    }

    // The current snapshot (absent / -1 = empty table).
    let current_snapshot_id = metadata.get("current-snapshot-id").and_then(|v| v.as_i64());
    let (snapshot_id, sequence_number, manifest_list, inline_manifests) = match current_snapshot_id
    {
        Some(id) if id >= 0 => {
            let snapshots = metadata
                .get("snapshots")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("current-snapshot-id {id} but no snapshots list"))?;
            let snapshot = snapshots
                .iter()
                .find(|s| s.get("snapshot-id").and_then(|v| v.as_i64()) == Some(id))
                .ok_or_else(|| {
                    anyhow!("current-snapshot-id {id} names no snapshot in the snapshots list")
                })?;
            let sequence_number = snapshot
                .get("sequence-number")
                .and_then(|v| v.as_u64())
                .filter(|n| *n > 0);
            let manifest_list = snapshot
                .get("manifest-list")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let inline_manifests: Vec<String> = snapshot
                .get("manifests")
                .and_then(|v| v.as_array())
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|e| e.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if manifest_list.is_none() && inline_manifests.is_empty() {
                bail!("snapshot {id} carries neither manifest-list nor manifests");
            }
            (
                Some(id as u64),
                sequence_number,
                manifest_list,
                inline_manifests,
            )
        }
        _ => (None, None, None, Vec::new()),
    };

    let mut files: BTreeMap<String, IcebergDataFile> = BTreeMap::new();
    if let Some(_snapshot_id) = snapshot_id {
        let manifest_paths: Vec<(String, Option<i64>)> = match &manifest_list {
            Some(list_path) => {
                let list_file = resolve_path(table_dir, list_path)
                    .with_context(|| format!("resolving manifest list {list_path}"))?;
                let entries = read_avro_records(&list_file)?;
                entries
                    .iter()
                    .filter_map(|entry| {
                        let manifest_path =
                            record_field(entry, "manifest_path")?.as_str()?.to_string();
                        let spec_id =
                            record_field(entry, "partition_spec_id").and_then(|v| v.as_i64());
                        Some((manifest_path, spec_id))
                    })
                    .collect()
            }
            None => inline_manifests.into_iter().map(|p| (p, None)).collect(),
        };
        for (manifest_path, spec_id) in manifest_paths {
            let manifest_file = resolve_path(table_dir, &manifest_path)
                .with_context(|| format!("resolving manifest {manifest_path}"))?;
            collect_live_files(
                table_dir,
                &manifest_file,
                spec_id,
                &field_id_to_source,
                &field_name_to_source,
                &partition_fields,
                &mut files,
            )
            .with_context(|| format!("reading manifest {manifest_path}"))?;
        }
    }

    Ok(IcebergScan {
        table_uuid,
        format_version: format_version as u32,
        snapshot_id,
        sequence_number,
        files: files.into_values().collect(),
        columns,
        partition_fields,
    })
}

/// The metadata file the table currently points at: `version-hint.text`
/// when present and resolvable, else the highest `v<N>.metadata.json`,
/// else `metadata.json`.
fn current_metadata_file(metadata_dir: &Path) -> Result<PathBuf> {
    if let Ok(hint) = std::fs::read_to_string(metadata_dir.join("version-hint.text")) {
        let hinted = metadata_dir.join(format!("v{}.metadata.json", hint.trim()));
        if hinted.is_file() {
            return Ok(hinted);
        }
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(metadata_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(version) = name
            .strip_prefix('v')
            .and_then(|rest| rest.strip_suffix(".metadata.json"))
            .and_then(|digits| digits.parse::<u64>().ok())
        {
            if best.as_ref().is_none_or(|(v, _)| version > *v) {
                best = Some((version, path));
            }
        }
    }
    if let Some((_, path)) = best {
        return Ok(path);
    }
    let bare = metadata_dir.join("metadata.json");
    if bare.is_file() {
        return Ok(bare);
    }
    bail!(
        "no v<N>.metadata.json or metadata.json under {}",
        metadata_dir.display()
    );
}

fn pick_schema(metadata: &serde_json::Value) -> Result<&serde_json::Value> {
    if let Some(schemas) = metadata.get("schemas").and_then(|v| v.as_array()) {
        let current_id = metadata
            .get("current-schema-id")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("schemas list carries no current-schema-id"))?;
        return schemas
            .iter()
            .find(|s| s.get("schema-id").and_then(|v| v.as_i64()) == Some(current_id))
            .ok_or_else(|| anyhow!("current-schema-id {current_id} names no schema"));
    }
    metadata
        .get("schema")
        .ok_or_else(|| anyhow!("metadata carries neither schemas nor schema"))
}

fn partition_specs(metadata: &serde_json::Value) -> Result<Vec<serde_json::Value>> {
    if let Some(specs) = metadata.get("partition-specs").and_then(|v| v.as_array()) {
        return Ok(specs.clone());
    }
    if let Some(spec) = metadata.get("partition-spec").and_then(|v| v.as_array()) {
        return Ok(vec![serde_json::json!({ "spec-id": 0, "fields": spec })]);
    }
    Ok(Vec::new())
}

/// Render an Iceberg schema type as a deterministic string: primitives
/// pass through; decimal/fixed carry their parameters; struct/list/map
/// render structurally. The declared column set is typed by THIS
/// rendering — registration and batch hash share it, so any
/// deterministic form satisfies D21-d; the source-native spelling keeps
/// it legible against the table's own schema.
fn render_type(ty: &serde_json::Value) -> Result<String> {
    match ty {
        serde_json::Value::String(primitive) => Ok(primitive.clone()),
        serde_json::Value::Object(object) => match object.get("type").and_then(|v| v.as_str()) {
            Some("decimal") => {
                let precision = object.get("precision").and_then(|v| v.as_u64());
                let scale = object.get("scale").and_then(|v| v.as_u64());
                Ok(format!(
                    "decimal({},{})",
                    precision.unwrap_or(0),
                    scale.unwrap_or(0)
                ))
            }
            Some("fixed") => Ok(format!(
                "fixed[{}]",
                object.get("length").and_then(|v| v.as_u64()).unwrap_or(0)
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
            Some("list") => Ok(format!(
                "list<{}>",
                render_type(
                    object
                        .get("element")
                        .and_then(|e| e.get("type"))
                        .unwrap_or(&serde_json::Value::Null)
                )?
            )),
            Some("map") => {
                let key = object
                    .get("key")
                    .and_then(|e| e.get("type"))
                    .unwrap_or(&serde_json::Value::Null);
                let value = object
                    .get("value")
                    .and_then(|e| e.get("type"))
                    .unwrap_or(&serde_json::Value::Null);
                Ok(format!(
                    "map<{},{}>",
                    render_type(key)?,
                    render_type(value)?
                ))
            }
            other => bail!("unsupported Iceberg type object: {other:?}"),
        },
        other => bail!("unsupported Iceberg type: {other:?}"),
    }
}

fn render_schema_columns(schema: &serde_json::Value) -> Result<Vec<(String, String)>> {
    let fields = schema
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("schema carries no fields"))?;
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

/// Resolve an Iceberg path against a local table root: `file://` URIs
/// strip the scheme, absolute paths pass, relative paths resolve against
/// the table root. Any OTHER scheme is a remote object store — outside
/// this adapter's local-table boundary, stated fail-closed.
fn resolve_path(table_dir: &Path, path: &str) -> Result<PathBuf> {
    if let Some(rest) = path.strip_prefix("file://") {
        return Ok(PathBuf::from(rest));
    }
    if path.contains("://") {
        bail!(
            "path {path} is a remote object-store URI — this reader's boundary is the local \
             filesystem (file:// or local paths); object stores are a deliberate non-goal of \
             the D1 iceberg milestone"
        );
    }
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        Ok(candidate)
    } else {
        Ok(table_dir.join(candidate))
    }
}

// ---------------------------------------------------------------------------
// Avro framing (own OCF reader; see the crate docs for why)
// ---------------------------------------------------------------------------

mod avro_framing {
    use super::*;

    pub(super) struct ContainerFile {
        pub(super) schema: serde_json::Value,
        pub(super) records: Vec<serde_json::Value>,
    }

    const MAGIC: [u8; 4] = [0x4f, 0x62, 0x6a, 0x01];

    /// Inflate one Avro "deflate" block. The codec name is one thing
    /// and the container is two: Java Avro (what Iceberg's writers
    /// are built on) emits zlib-WRAPPED deflate (RFC 1950, first byte
    /// 0x78), while the Rust and C implementations emit RAW deflate
    /// (RFC 1951) per the spec text. The first byte discriminates
    /// deterministically; the named container is tried first and the
    /// other only on a decode error, so a block is only ever accepted
    /// by a container that actually decodes it.
    fn inflate_block(block: &[u8]) -> Result<Vec<u8>> {
        fn inflate_with<R: std::io::Read>(mut reader: R) -> Result<Vec<u8>> {
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut out)?;
            Ok(out)
        }
        let zlib_shaped = block.first().is_some_and(|byte| *byte == 0x78);
        let attempts: Vec<Box<dyn std::io::Read>> = if zlib_shaped {
            vec![
                Box::new(flate2::read::ZlibDecoder::new(block)),
                Box::new(flate2::read::DeflateDecoder::new(block)),
            ]
        } else {
            vec![
                Box::new(flate2::read::DeflateDecoder::new(block)),
                Box::new(flate2::read::ZlibDecoder::new(block)),
            ]
        };
        let mut last_error = None;
        for attempt in attempts {
            match inflate_with(attempt) {
                Ok(out) => return Ok(out),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("empty deflate block")))
    }

    fn read_long(bytes: &[u8], at: &mut usize) -> Result<i64> {
        let mut value: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = *bytes
                .get(*at)
                .ok_or_else(|| anyhow!("avro stream truncated inside a varint"))?;
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                bail!("avro varint exceeds 64 bits");
            }
        }
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn read_bytes<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
        let length = read_long(bytes, at)?;
        if length < 0 {
            bail!("avro negative byte length");
        }
        let start = *at;
        let end = start + length as usize;
        if end > bytes.len() {
            bail!("avro stream truncated inside a bytes value");
        }
        *at = end;
        Ok(&bytes[start..end])
    }

    fn read_string(bytes: &[u8], at: &mut usize) -> Result<String> {
        let raw = read_bytes(bytes, at)?;
        String::from_utf8(raw.to_vec()).context("avro metadata string is not UTF-8")
    }

    /// Sanitize an embedded writer schema for apache-avro's parser:
    /// rename every record field / named type whose name violates
    /// Avro's name grammar (Iceberg v2 partition keys are field-id
    /// strings like "1000"). Order-preserving, so binary decoding —
    /// which is positional — is unaffected.
    fn sanitize_schema(value: &serde_json::Value, counter: &mut usize) -> serde_json::Value {
        match value {
            serde_json::Value::Object(object) => {
                let mut out = serde_json::Map::new();
                for (key, inner) in object {
                    if key == "name" {
                        if let Some(name) = inner.as_str() {
                            if !valid_avro_name(name) {
                                *counter += 1;
                                out.insert(
                                    key.clone(),
                                    serde_json::Value::String(format!("sanitized{counter}")),
                                );
                                continue;
                            }
                        }
                    }
                    out.insert(key.clone(), sanitize_schema(inner, counter));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .iter()
                    .map(|item| sanitize_schema(item, counter))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    fn valid_avro_name(name: &str) -> bool {
        let mut chars = name.chars();
        match chars.next() {
            Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Read an Avro object container file into decoded JSON records,
    /// accepting only the codecs Iceberg writers emit.
    pub(super) fn read_container(path: &Path) -> Result<ContainerFile> {
        let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        if raw.len() < MAGIC.len() || raw[..MAGIC.len()] != MAGIC {
            bail!("{} is not an Avro object container file", path.display());
        }
        let mut at = MAGIC.len();
        // Header metadata: a map<string, bytes> — blocks of pairs until
        // a zero (or negative) block count.
        let mut schema_text: Option<String> = None;
        let mut codec = "null".to_string();
        loop {
            let count = read_long(&raw, &mut at)?;
            if count == 0 {
                break;
            }
            // A negative count still carries |count| pairs — it just
            // adds an explicit block-size long first.
            let pairs = count.unsigned_abs();
            if count < 0 {
                read_long(&raw, &mut at)?;
            }
            for _ in 0..pairs {
                let key = read_string(&raw, &mut at)?;
                let value = read_bytes(&raw, &mut at)?;
                if key == "avro.schema" {
                    schema_text =
                        Some(String::from_utf8(value.to_vec()).context("schema is not UTF-8")?);
                } else if key == "avro.codec" {
                    codec = String::from_utf8(value.to_vec()).context("codec is not UTF-8")?;
                }
            }
        }
        let schema_text = schema_text
            .ok_or_else(|| anyhow!("{} carries no avro.schema in its header", path.display()))?;
        let schema: serde_json::Value =
            serde_json::from_str(&schema_text).context("parsing the embedded writer schema")?;
        if codec != "null" && codec != "deflate" {
            bail!(
                "manifest codec {codec} is outside the supported set (null, deflate) — \
                 fail closed rather than guess"
            );
        }
        if raw.len() < at + 16 {
            bail!("{} is truncated before its sync marker", path.display());
        }
        let sync: [u8; 16] = raw[at..at + 16].try_into().expect("16 bytes");
        at += 16;

        // Blocks: count, size, `size` RAW bytes (never length-prefixed
        // again), sync.
        let mut decoded = Vec::new();
        while at < raw.len() {
            let count = read_long(&raw, &mut at)?;
            let size = read_long(&raw, &mut at)?;
            if count <= 0 || size < 0 {
                bail!("{} carries a malformed block header", path.display());
            }
            let block_start = at;
            let block_end = block_start + size as usize;
            if block_end > raw.len() {
                bail!(
                    "{} block of {size} bytes runs past end of file",
                    path.display()
                );
            }
            let block = &raw[block_start..block_end];
            at = block_end;
            if at + 16 > raw.len() {
                bail!(
                    "{} block is truncated before its sync marker",
                    path.display()
                );
            }
            let mut sync_bytes = [0u8; 16];
            sync_bytes.copy_from_slice(&raw[at..at + 16]);
            at += 16;
            if sync_bytes != sync {
                bail!(
                    "{} block sync marker mismatch — corrupt file",
                    path.display()
                );
            }
            let plain: Vec<u8> = if codec == "deflate" {
                inflate_block(block).with_context(|| {
                    format!("inflating a deflate-coded Avro block in {}", path.display())
                })?
            } else {
                block.to_vec()
            };
            let sanitized = sanitize_schema(&schema, &mut 0);
            let avro_schema = apache_avro::Schema::parse_str(&sanitized.to_string())
                .context("parsing the sanitized writer schema")?;
            let mut cursor = std::io::Cursor::new(plain);
            for _ in 0..count {
                let value = apache_avro::from_avro_datum(&avro_schema, &mut cursor, None)
                    .map_err(|e| anyhow!("decoding an Avro value: {e}"))?;
                decoded.push(avro_value_to_json(&value));
            }
        }
        Ok(ContainerFile {
            schema,
            records: decoded,
        })
    }

    /// Decode an apache-avro value to plain JSON. Records become
    /// ARRAYS of `[name, value]` pairs — serde_json's Map does not
    /// preserve insertion order without a workspace-wide feature, and
    /// partition values are decoded POSITIONALLY (Avro binary carries
    /// no names), so order is load-bearing here. Callers read
    /// well-known fields with [`super::record_field`].
    fn avro_value_to_json(value: &apache_avro::types::Value) -> serde_json::Value {
        use apache_avro::types::Value as Avro;
        match value {
            Avro::Null => serde_json::Value::Null,
            Avro::Boolean(b) => serde_json::Value::Bool(*b),
            Avro::Int(i) => serde_json::Value::from(*i),
            Avro::Long(l) => serde_json::Value::from(*l),
            Avro::Float(f) => serde_json::Value::from(*f),
            Avro::Double(d) => serde_json::Value::from(*d),
            Avro::String(s) => serde_json::Value::String(s.clone()),
            Avro::Record(fields) => serde_json::Value::Array(
                fields
                    .iter()
                    .map(|(name, value)| {
                        serde_json::Value::Array(vec![
                            serde_json::Value::String(name.clone()),
                            avro_value_to_json(value),
                        ])
                    })
                    .collect(),
            ),
            Avro::Array(items) => {
                serde_json::Value::Array(items.iter().map(avro_value_to_json).collect())
            }
            other => serde_json::Value::String(format!("{other:?}")),
        }
    }
}

/// Read one field of a decoded record (an array of `[name, value]`
/// pairs) by name.
fn record_field<'a>(record: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    record.as_array()?.iter().find_map(|pair| {
        let parts = pair.as_array()?;
        let key = parts.first()?.as_str()?;
        let value = parts.last()?;
        (key == name).then_some(value)
    })
}

fn read_avro_records(path: &Path) -> Result<Vec<serde_json::Value>> {
    Ok(avro_framing::read_container(path)?.records)
}

/// The partition record's field ORDER inside a manifest schema, from
/// the ORIGINAL (unsanitized) embedded schema: `data_file.partition`
/// is a record whose fields, in order, are the partition spec's
/// fields. Position i in a decoded partition value is spec field i.
fn partition_field_names(manifest_schema: &serde_json::Value) -> Vec<String> {
    manifest_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .into_iter()
        .flatten()
        .filter_map(|field| {
            if field.get("name").and_then(|n| n.as_str()) != Some("data_file") {
                return None;
            }
            field
                .get("type")?
                .get("fields")?
                .as_array()?
                .iter()
                .find(|f| f.get("name").and_then(|n| n.as_str()) == Some("partition"))?
                .get("type")?
                .get("fields")?
                .as_array()
        })
        .flatten()
        .filter_map(|field| {
            field
                .get("name")
                .and_then(|n| n.as_str())
                .map(str::to_string)
        })
        .collect()
}

/// Render a partition VALUE as the string a mapped cell carries:
/// longs/ints/strings/bools/floats render natively; bytes/fixed render
/// as lowercase hex; anything else fails closed naming the kind.
fn render_partition_value(value: &serde_json::Value) -> Result<Option<String>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Bool(b) => Ok(Some(b.to_string())),
        serde_json::Value::Number(number) => Ok(Some(number.to_string())),
        serde_json::Value::String(s) => Ok(Some(s.clone())),
        other => bail!(
            "partition value {other:?} is not a renderable primitive — fail closed rather \
             than guess"
        ),
    }
}

fn collect_live_files(
    table_dir: &Path,
    manifest_file: &Path,
    _partition_spec_id: Option<i64>,
    field_id_to_source: &BTreeMap<i64, String>,
    field_name_to_source: &BTreeMap<String, String>,
    partition_transforms: &BTreeMap<String, String>,
    files: &mut BTreeMap<String, IcebergDataFile>,
) -> Result<()> {
    let container = avro_framing::read_container(manifest_file)?;
    let partition_names = partition_field_names(&container.schema);
    for entry in &container.records {
        let status = record_field(entry, "status")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        // 0 = EXISTING, 1 = ADDED: both are live in the current
        // snapshot's manifest set. 2 = DELETED: superseded, skip.
        if status == 2 {
            continue;
        }
        let Some(data_file) = record_field(entry, "data_file") else {
            continue;
        };
        // v2 content: 0 = data, 1/2 = position/equality deletes. A
        // delete file is not a row source — importing one as data
        // would resurrect deleted rows, so refuse the class.
        if let Some(content) = record_field(data_file, "content").and_then(|v| v.as_i64()) {
            if content != 0 {
                bail!(
                    "manifest carries delete-file content {content} — delete files are not \
                     row sources and this reader refuses to present them as data"
                );
            }
        }
        let Some(file_path) = record_field(data_file, "file_path").and_then(|v| v.as_str()) else {
            continue;
        };
        let format = record_field(data_file, "file_format")
            .and_then(|v| v.as_str())
            .unwrap_or("PARQUET");
        if format != "PARQUET" {
            bail!(
                "data file {file_path} is format {format} — only PARQUET data files are \
                 readable"
            );
        }
        let resolved = resolve_path(table_dir, file_path)?;
        // The partition overlay: decoded values are POSITIONAL over
        // the manifest schema's partition-struct fields; position i
        // maps to the source column through the partition spec — v2
        // keys by field-id string, v1 by field name. Only identity
        // transforms state the column's own value; a derived value
        // (bucket/truncate/month) never backs a cell (mapped columns
        // with such transforms were already refused at validation,
        // and an unmapped one simply carries no overlay).
        let mut partition: BTreeMap<String, Option<String>> = BTreeMap::new();
        if let Some(pairs) = record_field(data_file, "partition").and_then(|v| v.as_array()) {
            for (index, pair) in pairs.iter().enumerate() {
                let Some(original_name) = partition_names.get(index) else {
                    continue;
                };
                let source_column = original_name
                    .parse::<i64>()
                    .ok()
                    .and_then(|field_id| field_id_to_source.get(&field_id))
                    .or_else(|| field_name_to_source.get(original_name));
                let Some(source_column) = source_column else {
                    bail!(
                        "partition field `{original_name}` resolves to no schema column — \
                         the manifest and the metadata partition spec disagree"
                    );
                };
                let identity = partition_transforms
                    .get(source_column)
                    .map(String::as_str)
                    .is_some_and(|transform| transform == "identity");
                if !identity {
                    continue;
                }
                let value = pair.as_array().and_then(|p| p.last());
                partition.insert(
                    source_column.clone(),
                    value.map(render_partition_value).transpose()?.flatten(),
                );
            }
        }
        let record_count = record_field(data_file, "record_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        files.insert(
            resolved.to_string_lossy().into_owned(),
            IcebergDataFile {
                path: resolved,
                partition,
                record_count,
            },
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Mapping, rows, projection
// ---------------------------------------------------------------------------

/// The declared column set: the mapped columns in declared order,
/// typed by the TABLE schema (the authoritative declared schema of the
/// source; files materialize it, manifests partition it). ONE list
/// feeds the projection's `source_schema` AND every batch's snapshot
/// hash (D21-d: an unmapped schema addition is accepted; a mapped
/// removal/retype/rename moves the hash and fails closed).
pub fn declared_columns(mapping: &Mapping, scan: &IcebergScan) -> Vec<(String, String)> {
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
/// reference table columns, and any mapped column whose only
/// representation would be a NON-identity partition transform is
/// refused — a derived value (bucket/truncate/month) is not the
/// column's value and transcribing it would lie about the source.
pub fn validate_mapping(mapping: &Mapping, scan: &IcebergScan) -> Result<()> {
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
        if let Some(transform) = scan.partition_fields.get(column) {
            if transform != "identity" {
                bail!(
                    "mapped column `{column}` is partitioned by transform `{transform}` — a \
                     derived partition value is not the column's value; map a column the \
                     table states directly"
                );
            }
        }
    }
    Ok(())
}

/// Read every row of every live data file, in sorted file order then
/// row order, with each file's identity-partition overlay. Null pk
/// cells skip the row (counted, never guessed).
pub fn read_rows(mapping: &Mapping, scan: &IcebergScan) -> Result<(Vec<Row>, usize)> {
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

/// Map one window of rows to a submission unit (see
/// `exocortex_adapter_table::map_rows`; the `iceberg` flavor tag rides
/// every memory and the snapshot).
pub fn map_rows(
    mapping: &Mapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
) -> (exocortex_adapter_sdk::BatchUnit, usize) {
    exocortex_adapter_table::map_rows(
        mapping,
        table_uuid,
        declared,
        rows,
        batch_id_seed,
        "iceberg",
    )
}

/// Fill a unit's snapshot id (the current `snapshot-id`).
pub fn with_snapshot_id(
    unit: exocortex_adapter_sdk::BatchUnit,
    snapshot_id: &str,
) -> exocortex_adapter_sdk::BatchUnit {
    exocortex_adapter_table::with_snapshot_id(unit, snapshot_id)
}

/// The D21-a projection this adapter declares: the selector names the
/// table by its own uuid and the mapping version, the source schema is
/// the declared columns typed by the table schema, the bounds stop the
/// window rather than truncate it, and the last snapshot id is
/// Iceberg's own — so a table that presents an already-superseded
/// snapshot fails the rewind check.
pub fn projection(mapping: &Mapping, scan: &IcebergScan, max_window: u64) -> Projection {
    exocortex_adapter_table::table_projection(
        format!(
            "iceberg table {} (format v{}) at snapshot {} under the declared column mapping v{}",
            scan.table_uuid,
            scan.format_version,
            scan.snapshot_id_string().unwrap_or_else(|| "none".into()),
            mapping.mapping_version
        ),
        mapping,
        declared_columns(mapping, scan),
        max_window,
        &scan.snapshot_id_string().unwrap_or_default(),
    )
}
