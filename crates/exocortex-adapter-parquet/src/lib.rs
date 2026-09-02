//! D1 (master plan; core PRD §18.4 `parquet-dir-adapter`): the
//! parquet-directory adapter.
//!
//! A directory of Parquet files as a BOUNDED, declared-projection
//! import through the signed Ingestion Protocol. Deterministic
//! transcription only — no inference, no LLM, no network beyond the
//! backend:
//!
//! - one memory per row, typed and titled by the operator's declared
//!   column mapping (never guessed),
//! - identity by `ExternalKey` (`table_uuid` from the pinned table id,
//!   `logical_pk` the declared pk columns, unit-separator joined), so
//!   re-runs are idempotent by construction,
//! - a self-referencing foreign-key column, when declared, becomes the
//!   one relationship a table states factually (an edge per row to the
//!   row its key names, within the same window),
//! - the synthetic snapshot id is the blake3 digest over the sorted
//!   (name, content-hash) file set — a parquet directory has no
//!   snapshot concept, so the file set IS the snapshot (the core PRD's
//!   documented limitation for this flavor: reverting files presents as
//!   a rewind, and rows absent from the directory are never
//!   tombstoned).
//!
//! The adapter registers under the `parquet-dir` source flavor — the
//! FIRST workspace adapter on a table-shaped flavor — so the server
//! enforces the full D21 contract: registration refuses without the
//! declared projection, every batch's snapshot schema hash must equal
//! the canonical wire digest over the declared column set, and a
//! directory state the source already superseded is a rewind.
//!
//! The mapping core itself (mapping shape, row-to-draft transcription,
//! the D21-a projection shape) lives in `exocortex-adapter-table` and
//! is shared with the `iceberg` and `delta` flavor adapters — one
//! implementation, three readers.

use std::path::Path;

use anyhow::{bail, Context, Result};
use exocortex_adapter_sdk::Projection;
pub use exocortex_adapter_table::{map_rows as map_rows_shared, table_uuid_for, with_snapshot_id};
pub use exocortex_adapter_table::{Mapping, Row, PK_SEPARATOR};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

/// One directory scan: the sorted file set, its content digest (the
/// synthetic snapshot id), and the observed schema every file must
/// agree on.
#[derive(Clone, Debug)]
pub struct DirectoryScan {
    /// Sorted `.parquet` file names (relative names, sorted).
    pub files: Vec<String>,
    /// blake3 over the sorted (name, content-hash) list — hex.
    pub file_set_hash: String,
    /// Observed (column, arrow type) pairs, file order.
    pub columns: Vec<(String, String)>,
}

/// Scan a directory of Parquet files: sorted `.parquet` entries, each
/// file's blake3 content digest, and the observed column set. Files
/// whose schemas disagree fail the scan naming both files — never a
/// merged guess.
pub fn scan_directory(dir: &Path) -> Result<DirectoryScan> {
    let mut names: Vec<std::path::PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("scanning {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            names.push(path);
        }
    }
    names.sort();
    if names.is_empty() {
        bail!("no .parquet files under {}", dir.display());
    }

    let mut preimage = String::new();
    let mut columns: Option<Vec<(String, String)>> = None;
    for path in &names {
        let mut hasher = blake3::Hasher::new();
        let mut file =
            std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        std::io::copy(&mut file, &mut hasher)
            .with_context(|| format!("hashing {}", path.display()))?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("non-UTF-8 file name")?;
        preimage.push_str(name);
        preimage.push('\u{0}');
        preimage.push_str(&hasher.finalize().to_hex());
        preimage.push('\u{0}');

        let observed = file_columns(path)?;
        match &columns {
            None => columns = Some(observed),
            Some(known) => {
                if known != &observed {
                    bail!(
                        "schema disagreement: {} and {} declare different columns — one directory is one table",
                        names[0].display(),
                        path.display()
                    );
                }
            }
        }
    }
    Ok(DirectoryScan {
        files: names
            .iter()
            .map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect(),
        file_set_hash: blake3::hash(preimage.as_bytes()).to_hex().to_string(),
        columns: columns.unwrap_or_default(),
    })
}

/// The (column, rendered arrow type) pairs of one Parquet file — the
/// schema the reader actually materializes.
fn file_columns(path: &Path) -> Result<Vec<(String, String)>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("reading parquet metadata")?;
    Ok(builder
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().to_string(), format!("{:?}", field.data_type())))
        .collect())
}

/// Read every row the mapping selects from the scanned files, in
/// sorted file order then row order. Null pk cells skip the row
/// (counted, never guessed — a row without identity cannot join).
pub fn read_rows(dir: &Path, mapping: &Mapping) -> Result<(Vec<Row>, usize)> {
    mapping.validate_shape()?;
    let mut names: Vec<std::path::PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("scanning {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            names.push(path);
        }
    }
    names.sort();

    let mut rows = Vec::new();
    let mut skipped = 0usize;
    let overlay = std::collections::BTreeMap::new();
    for path in &names {
        let file =
            std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let reader: ParquetRecordBatchReader = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("reading parquet metadata")?
            .with_batch_size(1024)
            .build()
            .context("building parquet reader")?;
        for batch in reader {
            let batch = batch.context("reading record batch")?;
            rows.extend(exocortex_adapter_table::read_batch_rows(
                &batch,
                &path.display().to_string(),
                mapping,
                &mut skipped,
                &overlay,
            )?);
        }
    }
    Ok((rows, skipped))
}

/// The declared column set: the mapped columns in declared order,
/// typed by what the scan observed. ONE list feeds the projection's
/// `source_schema` AND the batch snapshot's schema hash — the two can
/// never drift, and unmapped columns in the files deliberately do not
/// participate (D21-d: an unmapped addition is accepted; a mapped
/// removal/retype/rename moves the hash and fails closed).
pub fn declared_columns(mapping: &Mapping, scan: &DirectoryScan) -> Vec<(String, String)> {
    let observed: std::collections::BTreeMap<&str, &str> = scan
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

/// Fail fast when the mapping references a column the scan did not
/// observe — locally, before any wire traffic (PX4's discipline).
pub fn validate_mapping(mapping: &Mapping, scan: &DirectoryScan) -> Result<()> {
    mapping.validate_shape()?;
    let observed: std::collections::BTreeMap<&str, &str> = scan
        .columns
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    for column in mapping.referenced_columns() {
        if !observed.contains_key(column) {
            bail!(
                "mapped column `{column}` is absent from the directory (observed: {})",
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

/// Map one window of rows to a submission unit (see
/// `exocortex_adapter_table::map_rows`; the `parquet` flavor tag rides
/// every memory and the snapshot).
pub fn map_rows(
    mapping: &Mapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
) -> (exocortex_adapter_sdk::BatchUnit, usize) {
    map_rows_shared(
        mapping,
        table_uuid,
        declared,
        rows,
        batch_id_seed,
        "parquet",
    )
}

/// The D21-a projection this adapter declares: the selector names the
/// directory and the mapping version, the field list is the declared
/// column mapping, the source schema is the declared columns typed by
/// the scan (the same list the snapshot hash covers), and the bounds
/// stop the window rather than truncate it. Registering under the
/// `parquet-dir` flavor makes the server enforce all of it.
pub fn projection(
    dir: &str,
    mapping: &Mapping,
    scan: &DirectoryScan,
    max_window: u64,
) -> Projection {
    exocortex_adapter_table::table_projection(
        format!(
            "parquet-dir {dir}/*.parquet under the declared column mapping v{}",
            mapping.mapping_version
        ),
        mapping,
        declared_columns(mapping, scan),
        max_window,
        &scan.file_set_hash,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn scan_of(names: &[(&str, &str)]) -> DirectoryScan {
        DirectoryScan {
            files: vec!["f.parquet".into()],
            file_set_hash: "snapshot-1".into(),
            columns: names
                .iter()
                .map(|(n, t)| (n.to_string(), t.to_string()))
                .collect(),
        }
    }

    #[test]
    fn declared_columns_are_the_mapped_set_typed_by_observation() {
        let scan = scan_of(&[
            ("id", "Int64"),
            ("title", "Utf8"),
            ("detail", "Utf8"),
            ("severity", "Utf8"),
            ("tags", "Utf8"),
            ("parent_id", "Utf8"),
            ("unmapped_extra", "Boolean"),
        ]);
        let cols = declared_columns(&mapping(), &scan);
        let names: Vec<&str> = cols.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            ["title", "detail", "severity", "id", "tags", "parent_id"]
        );
        assert_eq!(cols[3].1, "Int64");
        assert!(
            !names.contains(&"unmapped_extra"),
            "unmapped columns do not participate in the declared schema"
        );
    }

    #[test]
    fn validate_mapping_names_the_missing_column() {
        let scan = scan_of(&[("id", "Int64"), ("title", "Utf8")]);
        let err = validate_mapping(&mapping(), &scan).unwrap_err().to_string();
        assert!(err.contains("`detail`"), "{err}");
    }

    #[test]
    fn snapshot_hash_equals_the_canonical_declared_digest() {
        let scan = scan_of(&[
            ("id", "Int64"),
            ("title", "Utf8"),
            ("detail", "Utf8"),
            ("severity", "Utf8"),
            ("tags", "Utf8"),
            ("parent_id", "Utf8"),
        ]);
        let declared = declared_columns(&mapping(), &scan);
        let rows = vec![Row {
            pk: "r-1".into(),
            title: "first".into(),
            content: vec![],
            tags: vec![],
            parent: None,
        }];
        let (unit, _) = map_rows(&mapping(), &table_uuid_for("t"), &declared, &rows, "w-0");
        assert_eq!(
            unit.snapshot.as_ref().unwrap().schema_hash,
            exocortex_wire::projection::schema_hash(&declared).to_vec(),
            "the batch hash and the registration-derived hash are one value"
        );
        assert_eq!(unit.snapshot.as_ref().unwrap().source_flavor, "parquet");
    }
}
