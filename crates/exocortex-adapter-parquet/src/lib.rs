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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use arrow_cast::display::array_value_to_string;
use exocortex_adapter_sdk::{
    BatchUnit, Projection, ProjectionBounds, ProjectionField, SourceColumn,
};
use exocortex_wire::ingest::v1::{
    ExternalKey, ExternalSnapshotInfo, MemoryDraft, RelationshipDraft,
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

/// The unit separator joining multi-column logical pks (a value
/// containing it would blur the pk boundary; the git adapter's log
/// format uses the same discipline).
pub const PK_SEPARATOR: &str = "\u{1f}";

/// The operator's declared column mapping — what the adapter is
/// entitled to bring in and how it becomes ontology. Authored as JSON
/// next to the invocation; versioned by `mapping_version`.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mapping {
    /// Ontology memory-type name for every row (server-validated).
    pub memory_type: String,
    /// Column carrying the row title.
    pub title_column: String,
    /// Columns rendered into content, in declared order, as
    /// `column: value` lines.
    pub content_columns: Vec<String>,
    /// The primary-key columns (joined, order as declared).
    pub pk_columns: Vec<String>,
    /// Optional column of comma-separated tags.
    pub tags_column: Option<String>,
    /// Optional self-referencing foreign-key column; its value names
    /// the parent row's logical pk (same pk columns).
    pub parent_column: Option<String>,
    /// Relationship kind for `parent_column` edges (required with it).
    pub parent_kind: Option<String>,
    /// Bumped on every deliberate mapping change.
    pub mapping_version: u32,
}

impl Mapping {
    /// Load from a JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading mapping {}", path.display()))?;
        serde_json::from_str(&text).with_context(|| format!("parsing mapping {}", path.display()))
    }

    fn validate_shape(&self) -> Result<()> {
        if self.memory_type.trim().is_empty() {
            bail!("mapping.memory_type is empty");
        }
        if self.title_column.trim().is_empty() {
            bail!("mapping.title_column is empty");
        }
        if self.pk_columns.is_empty() {
            bail!("mapping.pk_columns is empty — rows need identity");
        }
        match (&self.parent_column, &self.parent_kind) {
            (Some(_), None) => bail!("mapping.parent_column requires mapping.parent_kind"),
            (None, Some(_)) => bail!("mapping.parent_kind requires mapping.parent_column"),
            _ => {}
        }
        if self.mapping_version == 0 {
            bail!("mapping.mapping_version starts at 1");
        }
        Ok(())
    }

    /// Every column the mapping references, in declared order.
    pub fn referenced_columns(&self) -> Vec<&str> {
        let mut cols: Vec<&str> = vec![self.title_column.as_str()];
        cols.extend(self.content_columns.iter().map(String::as_str));
        cols.extend(self.pk_columns.iter().map(String::as_str));
        if let Some(tags) = &self.tags_column {
            cols.push(tags.as_str());
        }
        if let Some(parent) = &self.parent_column {
            cols.push(parent.as_str());
        }
        cols
    }
}

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
    let mut names: Vec<PathBuf> = Vec::new();
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

/// One row, fully resolved to strings: exactly what the mapping
/// selects, nothing deduced.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    /// Joined logical pk (`PK_SEPARATOR`).
    pub pk: String,
    /// Title (the mapped title column).
    pub title: String,
    /// Content lines, one per mapped content column, in order.
    pub content: Vec<(String, String)>,
    /// Tags (the mapped tags column, comma-split), if any.
    pub tags: Vec<String>,
    /// Parent logical pk (the mapped parent column), if declared.
    pub parent: Option<String>,
}

/// Read every row the mapping selects from the scanned files, in
/// sorted file order then row order. Null pk cells skip the row
/// (counted, never guessed — a row without identity cannot join).
pub fn read_rows(dir: &Path, mapping: &Mapping) -> Result<(Vec<Row>, usize)> {
    mapping.validate_shape()?;
    let mut names: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("scanning {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            names.push(path);
        }
    }
    names.sort();

    let mut rows = Vec::new();
    let mut skipped = 0usize;
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
            let schema = batch.schema();
            let index_of = |name: &str| -> Option<usize> { schema.index_of(name).ok() };
            for mapped in mapping.referenced_columns() {
                if index_of(mapped).is_none() {
                    bail!(
                        "mapped column `{mapped}` is absent from {} (observed: {})",
                        path.display(),
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            let title_ix = index_of(&mapping.title_column).expect("checked above");
            let content_ix: Vec<usize> = mapping
                .content_columns
                .iter()
                .map(|c| index_of(c).expect("checked above"))
                .collect();
            let pk_ix: Vec<usize> = mapping
                .pk_columns
                .iter()
                .map(|c| index_of(c).expect("checked above"))
                .collect();
            let tags_ix = mapping.tags_column.as_deref().and_then(index_of);
            let parent_ix = mapping.parent_column.as_deref().and_then(index_of);

            for row_index in 0..batch.num_rows() {
                let cell = |ix: usize| -> Option<String> {
                    let column = batch.column(ix);
                    if column.is_null(row_index) {
                        return None;
                    }
                    array_value_to_string(column.as_ref(), row_index).ok()
                };
                let pk_parts: Vec<Option<String>> = pk_ix.iter().map(|ix| cell(*ix)).collect();
                if pk_parts
                    .iter()
                    .any(|p| p.as_deref().unwrap_or("").is_empty())
                {
                    skipped += 1;
                    continue;
                }
                let pk = pk_parts
                    .iter()
                    .map(|p| p.as_deref().unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join(PK_SEPARATOR);
                let title = cell(title_ix).unwrap_or_default();
                let content: Vec<(String, String)> = mapping
                    .content_columns
                    .iter()
                    .zip(content_ix.iter())
                    .map(|(name, ix)| (name.clone(), cell(*ix).unwrap_or_default()))
                    .collect();
                let tags = tags_ix
                    .and_then(cell)
                    .map(|raw| {
                        raw.split(',')
                            .map(str::trim)
                            .filter(|t| !t.is_empty())
                            .map(str::to_ascii_lowercase)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let parent = parent_ix.and_then(cell).filter(|p| !p.is_empty());
                rows.push(Row {
                    pk,
                    title,
                    content,
                    tags,
                    parent,
                });
            }
        }
    }
    Ok((rows, skipped))
}

/// Derive the 16-byte table uuid for a pinned table id (the operator
/// scopes row identity by pinning the same id across runs).
pub fn table_uuid_for(table_id: &str) -> [u8; 16] {
    let digest = blake3::hash(table_id.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

fn truncate_200(s: &str) -> String {
    s.chars().take(200).collect()
}

/// The declared column set: the mapped columns in declared order,
/// typed by what the scan observed. ONE list feeds the projection's
/// `source_schema` AND the batch snapshot's schema hash — the two can
/// never drift, and unmapped columns in the files deliberately do not
/// participate (D21-d: an unmapped addition is accepted; a mapped
/// removal/retype/rename moves the hash and fails closed).
pub fn declared_columns(mapping: &Mapping, scan: &DirectoryScan) -> Vec<(String, String)> {
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

/// Fail fast when the mapping references a column the scan did not
/// observe — locally, before any wire traffic (PX4's discipline).
pub fn validate_mapping(mapping: &Mapping, scan: &DirectoryScan) -> Result<()> {
    mapping.validate_shape()?;
    let observed: BTreeMap<&str, &str> = scan
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

/// Map one window of rows to a submission unit: one memory per row,
/// plus parent edges whose endpoints both fall inside THIS window
/// (cross-window parent links are counted and skipped — the SDK
/// rejects dangling references, and silently splitting them would be a
/// lie about the source).
pub fn map_rows(
    mapping: &Mapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
) -> (BatchUnit, usize) {
    let mut pk_key: BTreeMap<&str, String> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        pk_key.insert(row.pk.as_str(), format!("row-{index}"));
    }
    let mut memories = Vec::with_capacity(rows.len());
    let mut relationships = Vec::new();
    let mut skipped_parents = 0usize;

    for (index, row) in rows.iter().enumerate() {
        let draft_key = format!("row-{index}");
        let mut content = String::new();
        for (column, value) in &row.content {
            content.push_str(&format!("{column}: {value}\n"));
        }
        let mut tags = vec!["parquet".to_string()];
        tags.extend(row.tags.iter().cloned());
        memories.push(MemoryDraft {
            draft_key: draft_key.clone(),
            id: String::new(),
            memory_type: mapping.memory_type.clone(),
            title: truncate_200(if row.title.trim().is_empty() {
                &row.pk
            } else {
                &row.title
            }),
            content,
            tags,
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: table_uuid.to_vec(),
                logical_pk: row.pk.clone(),
                mapping_version: mapping.mapping_version,
            }),
        });
        if let Some(parent_pk) = &row.parent {
            match pk_key.get(parent_pk.as_str()) {
                Some(parent_key) => relationships.push(RelationshipDraft {
                    from_draft_key: draft_key.clone(),
                    to_draft_key: parent_key.clone(),
                    kind: mapping.parent_kind.clone().unwrap_or_default(),
                    strength: 0.0,
                    confidence: 0.9,
                    context: format!(
                        "parent via {}={parent_pk}",
                        mapping.parent_column.as_deref().unwrap_or_default()
                    ),
                    visibility: 3,
                    to_memory_id: String::new(),
                }),
                None => skipped_parents += 1,
            }
        }
    }

    let unit = BatchUnit {
        batch_id_seed: batch_id_seed.into(),
        memories,
        relationships,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: String::new(),
            schema_hash: exocortex_wire::projection::schema_hash(declared).to_vec(),
            source_flavor: "parquet-dir".into(),
        }),
        observed_at: std::time::UNIX_EPOCH,
    };
    (unit, skipped_parents)
}

/// Fill a unit's snapshot id (the directory scan's file-set hash).
pub fn with_snapshot_id(mut unit: BatchUnit, snapshot_id: &str) -> BatchUnit {
    if let Some(snapshot) = &mut unit.snapshot {
        snapshot.snapshot_id = snapshot_id.to_string();
    }
    unit
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
    let mut fields = Vec::new();
    for column in mapping.referenced_columns() {
        fields.push(ProjectionField {
            source_field: column.to_string(),
            memory_type: mapping.memory_type.clone(),
            kind: if Some(column) == mapping.parent_column.as_deref() {
                mapping.parent_kind.clone().unwrap_or_default()
            } else {
                String::new()
            },
        });
    }
    Projection {
        selector: format!(
            "parquet-dir {dir}/*.parquet under the declared column mapping v{}",
            mapping.mapping_version
        ),
        fields,
        source_schema: declared_columns(mapping, scan)
            .into_iter()
            .map(|(name, data_type)| SourceColumn { name, data_type })
            .collect(),
        mapping_version: mapping.mapping_version,
        bounds: ProjectionBounds {
            max_rows_per_window: max_window,
            max_rows_per_run: max_window.saturating_mul(100),
            max_graph_share_percent: 50,
        },
        last_snapshot_id: scan.file_set_hash.clone(),
    }
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

    #[test]
    fn mapping_shape_is_validated() {
        assert!(mapping().validate_shape().is_ok());
        let bad = serde_json::from_str::<Mapping>(
            r#"{"memory_type":"Problem","title_column":"t","content_columns":["c"],"pk_columns":["id"],"parent_column":"p","mapping_version":1}"#,
        )
        .unwrap();
        assert!(bad.validate_shape().is_err(), "parent without kind");
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
    }

    #[test]
    fn rows_map_deterministically_with_stable_identities() {
        let declared = vec![("title".to_string(), "Utf8".to_string())];
        let rows = vec![
            Row {
                pk: "r-1".into(),
                title: "first".into(),
                content: vec![("detail".into(), "d1".into())],
                tags: vec!["alpha".into()],
                parent: None,
            },
            Row {
                pk: "r-2".into(),
                title: "second".into(),
                content: vec![("detail".into(), "d2".into())],
                tags: vec![],
                parent: Some("r-1".into()),
            },
        ];
        let table = table_uuid_for("table-id");
        let (unit, skipped) = map_rows(&mapping(), &table, &declared, &rows, "w-0");
        assert_eq!(skipped, 0);
        assert_eq!(unit.memories.len(), 2);
        assert_eq!(unit.relationships.len(), 1);
        assert_eq!(unit.relationships[0].kind, "Causes");
        assert_eq!(
            unit.memories[0].external_key.as_ref().unwrap().logical_pk,
            "r-1"
        );
        assert!(unit.memories[0].tags.contains(&"parquet".to_string()));
        let (again, _) = map_rows(&mapping(), &table, &declared, &rows, "w-0");
        assert_eq!(unit.memories.len(), again.memories.len());
        assert_eq!(unit.relationships.len(), again.relationships.len());
        // A parent outside the window is skipped and counted, never
        // dangled.
        let lone = vec![rows[1].clone()];
        let (unit, skipped) = map_rows(&mapping(), &table, &declared, &lone, "w-0");
        assert_eq!(unit.relationships.len(), 0);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn empty_title_falls_back_to_the_pk() {
        let declared = vec![];
        let rows = vec![Row {
            pk: "r-9".into(),
            title: "  ".into(),
            content: vec![],
            tags: vec![],
            parent: None,
        }];
        let (unit, _) = map_rows(&mapping(), &table_uuid_for("t"), &declared, &rows, "w-0");
        assert_eq!(unit.memories[0].title, "r-9");
    }
}
