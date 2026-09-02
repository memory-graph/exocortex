//! The shared declared-projection table-mapping core (D1; core PRD
//! §18.4, adapter-contract PRD D21-a): the one implementation of the
//! operator's column mapping, the row-to-draft transcription, and the
//! projection shape every TABLE-flavored adapter declares.
//!
//! This crate owns no source format and no flavor — `parquet-dir`,
//! `iceberg`, and `delta` each read their source into [`Row`]s and
//! their observed schema into declared columns, then hand both here.
//! One implementation because three adapters carrying three copies of
//! "how a table row becomes a typed memory" is exactly the divergence
//! class R6-R286/R6-R287 consolidated away elsewhere: identity,
//! titles, tags, and the D21-a projection must behave identically
//! across flavors or the contract the server enforces is only as
//! strong as the least careful adapter.
//!
//! Not an adapter: it is a library the flavor adapters link (like the
//! SDK is the protocol they speak) and is skipped by the
//! adapter-contract bijection for the same reason `sdk` is — the
//! flavor crates carry the `projection-declared` rows.

use anyhow::{bail, Result};
use exocortex_adapter_sdk::{
    BatchUnit, Projection, ProjectionBounds, ProjectionField, SourceColumn,
};
use exocortex_wire::ingest::v1::{
    ExternalKey, ExternalSnapshotInfo, MemoryDraft, RelationshipDraft,
};

/// The unit separator joining multi-column logical pks (a value
/// containing it would blur the pk boundary; the git adapter's log
/// format uses the same discipline).
pub const PK_SEPARATOR: &str = "\u{1f}";

/// The operator's declared column mapping — what a table-flavored
/// adapter is entitled to bring in and how it becomes ontology.
/// Authored as JSON next to the invocation; versioned by
/// `mapping_version`. Flavor-neutral: the same shape governs a
/// parquet directory, an Iceberg table, and a Delta table.
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
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading mapping {}: {e}", path.display()))?;
        serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing mapping {}: {e}", path.display()))
    }

    /// The shape rules every flavor inherits.
    pub fn validate_shape(&self) -> Result<()> {
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

    /// Every column the mapping references, in declared order. ONE
    /// list feeds the projection's `source_schema` and every batch's
    /// snapshot schema hash — the two can never drift, and unmapped
    /// columns in the source deliberately do not participate (D21-d:
    /// an unmapped addition is accepted; a mapped
    /// removal/retype/rename moves the hash and fails closed).
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

/// One row, fully resolved to strings: exactly what the mapping
/// selects, nothing deduced. Partition-column cells are resolved by
/// the flavor reader (from the source's authoritative partition
/// metadata) before rows reach this layer.
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

fn truncate_200(s: &str) -> String {
    s.chars().take(200).collect()
}

/// The one per-batch row reader every table flavor uses: resolve
/// exactly the mapped columns to strings. A column absent from the
/// record batch resolves through `overlay` — the flavor's
/// authoritative partition values keyed by source-column name
/// (Iceberg/Delta writers may omit identity-partition columns from
/// the data files; the manifest/log carries them). A mapped column
/// in NEITHER place fails naming the source; null pk cells skip the
/// row into `skipped` (counted, never guessed — a row without
/// identity cannot join).
pub fn read_batch_rows(
    batch: &arrow_array::RecordBatch,
    source_name: &str,
    mapping: &Mapping,
    skipped: &mut usize,
    overlay: &std::collections::BTreeMap<String, Option<String>>,
) -> Result<Vec<Row>> {
    use arrow_cast::display::array_value_to_string;

    mapping.validate_shape()?;
    let schema = batch.schema();
    let index_of = |name: &str| -> Option<usize> { schema.index_of(name).ok() };
    for mapped in mapping.referenced_columns() {
        if index_of(mapped).is_none() && !overlay.contains_key(mapped) {
            bail!(
                "mapped column `{mapped}` is absent from {source_name} (observed: {})",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    let title_ix = index_of(&mapping.title_column);
    let content_ix: Vec<Option<usize>> = mapping
        .content_columns
        .iter()
        .map(|c| index_of(c))
        .collect();
    let pk_ix: Vec<Option<usize>> = mapping.pk_columns.iter().map(|c| index_of(c)).collect();
    let tags_ix = mapping.tags_column.as_deref().and_then(index_of);
    let parent_ix = mapping.parent_column.as_deref().and_then(index_of);

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let cell = |ix: usize| -> Option<String> {
            let column = batch.column(ix);
            if column.is_null(row_index) {
                return None;
            }
            array_value_to_string(column.as_ref(), row_index).ok()
        };
        // Overlay first for absent-from-file columns (partition
        // values are constant per file); the file is authoritative
        // for every column it carries.
        let resolve = |ix: Option<usize>, name: &str| -> Option<String> {
            match ix {
                Some(ix) => cell(ix),
                None => overlay.get(name).cloned().flatten(),
            }
        };
        let pk_parts: Vec<Option<String>> = pk_ix
            .iter()
            .zip(mapping.pk_columns.iter())
            .map(|(ix, name)| resolve(*ix, name))
            .collect();
        if pk_parts
            .iter()
            .any(|p| p.as_deref().unwrap_or("").is_empty())
        {
            *skipped += 1;
            continue;
        }
        let pk = pk_parts
            .iter()
            .map(|p| p.as_deref().unwrap_or(""))
            .collect::<Vec<_>>()
            .join(PK_SEPARATOR);
        let title = resolve(title_ix, &mapping.title_column).unwrap_or_default();
        let content: Vec<(String, String)> = mapping
            .content_columns
            .iter()
            .zip(content_ix.iter())
            .map(|(name, ix)| (name.clone(), resolve(*ix, name).unwrap_or_default()))
            .collect();
        let tags = resolve(tags_ix, mapping.tags_column.as_deref().unwrap_or_default())
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(str::to_ascii_lowercase)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let parent = resolve(
            parent_ix,
            mapping.parent_column.as_deref().unwrap_or_default(),
        )
        .filter(|p| !p.is_empty());
        rows.push(Row {
            pk,
            title,
            content,
            tags,
            parent,
        });
    }
    Ok(rows)
}

/// Map one window of rows to a submission unit: one memory per row,
/// plus parent edges whose endpoints both fall inside THIS window
/// (cross-window parent links are counted and skipped — the SDK
/// rejects dangling references, and silently splitting them would be a
/// lie about the source). `flavor_tag` names the source flavor on
/// every memory's tags so rows stay attributable after they land.
pub fn map_rows(
    mapping: &Mapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
    flavor_tag: &str,
) -> (BatchUnit, usize) {
    let mut pk_key: std::collections::BTreeMap<&str, String> = Default::default();
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
        let mut tags = vec![flavor_tag.to_string()];
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
            source_flavor: flavor_tag.into(),
        }),
        observed_at: std::time::UNIX_EPOCH,
    };
    (unit, skipped_parents)
}

/// Fill a unit's snapshot id (the flavor's authoritative snapshot
/// identity — a file-set digest for parquet-dir, the Iceberg
/// snapshot id, the Delta log version).
pub fn with_snapshot_id(mut unit: BatchUnit, snapshot_id: &str) -> BatchUnit {
    if let Some(snapshot) = &mut unit.snapshot {
        snapshot.snapshot_id = snapshot_id.to_string();
    }
    unit
}

/// Derive the 16-byte table uuid for a pinned table id (the operator
/// scopes row identity by pinning the same id across runs; an Iceberg
/// table pins its own `table-uuid`, a parquet directory its declared
/// id, a Delta table its `metaData.id`).
pub fn table_uuid_for(table_id: &str) -> [u8; 16] {
    let digest = blake3::hash(table_id.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

/// The D21-a projection shape every table flavor declares: the
/// caller's selector names the source in its own terms, the field
/// list is the declared column mapping, the source schema is the
/// declared columns typed by what the flavor observed (the same list
/// the snapshot hash covers), and the bounds stop the window rather
/// than truncate it. Registering under a TABLE flavor makes the
/// server enforce all of it.
pub fn table_projection(
    selector: String,
    mapping: &Mapping,
    source_schema: Vec<(String, String)>,
    max_window: u64,
    snapshot_id: &str,
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
        selector,
        fields,
        source_schema: source_schema
            .into_iter()
            .map(|(name, data_type)| SourceColumn { name, data_type })
            .collect(),
        mapping_version: mapping.mapping_version,
        bounds: ProjectionBounds {
            max_rows_per_window: max_window,
            max_rows_per_run: max_window.saturating_mul(100),
            max_graph_share_percent: 50,
        },
        last_snapshot_id: snapshot_id.to_string(),
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
        let (unit, skipped) = map_rows(&mapping(), &table, &declared, &rows, "w-0", "iceberg");
        assert_eq!(skipped, 0);
        assert_eq!(unit.memories.len(), 2);
        assert_eq!(unit.relationships.len(), 1);
        assert_eq!(unit.relationships[0].kind, "Causes");
        assert_eq!(
            unit.memories[0].external_key.as_ref().unwrap().logical_pk,
            "r-1"
        );
        assert!(unit.memories[0].tags.contains(&"iceberg".to_string()));
        assert_eq!(
            unit.snapshot.as_ref().unwrap().source_flavor,
            "iceberg",
            "the flavor tag rides the snapshot so the server's flavor check binds"
        );
        let (again, _) = map_rows(&mapping(), &table, &declared, &rows, "w-0", "iceberg");
        assert_eq!(unit.memories.len(), again.memories.len());
        assert_eq!(unit.relationships.len(), again.relationships.len());
        // A parent outside the window is skipped and counted, never
        // dangled.
        let lone = vec![rows[1].clone()];
        let (unit, skipped) = map_rows(&mapping(), &table, &declared, &lone, "w-0", "iceberg");
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
        let (unit, _) = map_rows(
            &mapping(),
            &table_uuid_for("t"),
            &declared,
            &rows,
            "w-0",
            "delta",
        );
        assert_eq!(unit.memories[0].title, "r-9");
    }

    #[test]
    fn snapshot_hash_equals_the_canonical_declared_digest() {
        let declared = vec![
            ("id".to_string(), "long".to_string()),
            ("title".to_string(), "string".to_string()),
        ];
        let rows = vec![Row {
            pk: "r-1".into(),
            title: "first".into(),
            content: vec![],
            tags: vec![],
            parent: None,
        }];
        let (unit, _) = map_rows(
            &mapping(),
            &table_uuid_for("t"),
            &declared,
            &rows,
            "w-0",
            "delta",
        );
        assert_eq!(
            unit.snapshot.as_ref().unwrap().schema_hash,
            exocortex_wire::projection::schema_hash(&declared).to_vec(),
            "the batch hash and the registration-derived hash are one value"
        );
    }

    #[test]
    fn table_projection_carries_bounds_and_the_parent_kind() {
        let projection = table_projection(
            "iceberg table-x at snapshot 7".into(),
            &mapping(),
            vec![("id".to_string(), "long".to_string())],
            128,
            "7",
        );
        assert_eq!(projection.bounds.max_rows_per_window, 128);
        assert_eq!(projection.bounds.max_rows_per_run, 12800);
        assert_eq!(projection.last_snapshot_id, "7");
        assert!(projection
            .fields
            .iter()
            .any(|f| f.source_field == "parent_id" && f.kind == "Causes"));
    }
}
