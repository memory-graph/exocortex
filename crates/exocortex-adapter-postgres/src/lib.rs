//! D20 (master plan; core PRD §18.4; adapter-contract PRD D21): the
//! Postgres CDC adapter, `cdc-postgres` source flavor — logical
//! replication changes as bounded, declared-projection windows through
//! the signed Ingestion Protocol.
//!
//! The change stream is decoded by the **wal2json** plugin
//! (format-version 2: one JSON change per row event). This crate owns:
//!
//! - the hermetic parse layer — wal2json JSON → [`CdcChange`]
//!   (action, table, column cells) — fully tested without a server;
//! - the mapping layer — the SAME declared column mapping the table
//!   adapters use (`exocortex_adapter_table::Mapping`), resolved cell
//!   by cell through [`exocortex_adapter_table::row_from_cells`], so
//!   INSERT and UPDATE become idempotent upserts keyed by
//!   `ExternalKey` (the row is the same row, whatever the source);
//! - the replication session ([`replication`]) — a first-party
//!   MINIMAL logical-replication client on `postgres-protocol` (the
//!   codecs tokio-postgres itself uses; SCRAM included). The rule-9
//!   record (PUBLISHING.md) is the rejection slip: `pgwire-replication`
//!   needs rustc 1.88 against the pinned 1.85 floor, and
//!   `tokio-postgres` exposes no replication mode — a first-party
//!   client over the standard codecs is the remaining correct option.
//!
//! **Recorded v1 boundary — DELETE events do not propagate.** The
//! Ingestion Protocol carries no producer-side delete surface
//! (Submit/SubmitStream/Register/Fingerprint/Preflight/Manifest:
//! `proto/ingest.proto`), and inventing one is not this row's to do.
//! DELETE events for the mapped table are counted, logged, and
//! surfaced in the run's output; when the protocol grows a governed
//! delete (a natural fit for soft-delete and `valid_until`), this
//! adapter maps it without changing shape. Until then the row records
//! the tension between the PRD's CDC aspiration and the write surface
//! that actually exists.
//!
//! Snapshot identity is the WAL LSN (`lsn-<16 hex>`, monotonic by
//! construction): a stream below the settled cursor is a REGRESSED
//! slot (someone recreated it), refused locally before any wire
//! traffic — the same cold-start gate Delta's versions allow.

pub mod replication;

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
pub use exocortex_adapter_table::{table_uuid_for, Row};

/// One decoded wal2json (format-version 2) change.
#[derive(Clone, Debug, PartialEq)]
pub struct CdcChange {
    /// I / U / D.
    pub action: String,
    /// Fully-qualified table (`public.orders`).
    pub table: String,
    /// Column cells keyed by name, rendered to strings the way
    /// wal2json prints JSON values.
    pub columns: BTreeMap<String, Option<String>>,
    /// For UPDATE/DELETE: the old-identity cells wal2json carries when
    /// REPLICA IDENTITY is FULL (or the replica identity key).
    pub identity: BTreeMap<String, Option<String>>,
}

/// The operator's declared CDC mapping: which table, the shared
/// column mapping, and the declared column types (authored from
/// `\d`; the projection's schema hash is derived from them).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CdcMapping {
    /// Fully-qualified table this adapter watches.
    pub table: String,
    /// The shared declared column mapping.
    #[serde(flatten)]
    pub mapping: exocortex_adapter_table::Mapping,
    /// Declared column types (name -> Postgres type name), for the
    /// declared schema the D21 hash covers. Every mapped column must
    /// carry one.
    pub column_types: BTreeMap<String, String>,
}

impl CdcMapping {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading mapping {}: {e}", path.display()))?;
        let mapping: CdcMapping = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing mapping {}: {e}", path.display()))?;
        mapping.validate()?;
        Ok(mapping)
    }

    /// Every mapped column must carry a declared type (the declared
    /// schema is authored, not observed from one lucky batch).
    pub fn validate(&self) -> Result<()> {
        self.mapping.validate_shape()?;
        if self.table.trim().is_empty() {
            bail!("cdc mapping.table is empty");
        }
        for column in self.mapping.referenced_columns() {
            if !self.column_types.contains_key(column) {
                bail!(
                    "mapped column `{column}` has no declared type in cdc mapping \
                     (author it from `\\d {table}`)",
                    table = self.table
                );
            }
        }
        Ok(())
    }

    /// The declared column set for the D21 projection + snapshot hash.
    pub fn declared_columns(&self) -> Vec<(String, String)> {
        self.mapping
            .referenced_columns()
            .into_iter()
            .map(|column| {
                (
                    column.to_string(),
                    self.column_types.get(column).cloned().unwrap_or_default(),
                )
            })
            .collect()
    }
}

/// Render one wal2json JSON value the way the cell map carries it:
/// null stays None; strings unquote; numbers/bools keep their JSON
/// spelling (deterministic, and the only thing the mapping resolves).
fn render_cell(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

fn cells_of(columns: &serde_json::Value) -> Result<BTreeMap<String, Option<String>>> {
    let mut cells = BTreeMap::new();
    let list = columns.as_array().ok_or_else(|| {
        anyhow::anyhow!("wal2json columns are not an array (format-version 2 required)")
    })?;
    for column in list {
        let name = column
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("wal2json column carries no name"))?;
        cells.insert(name.to_string(), column.get("value").and_then(render_cell));
    }
    Ok(cells)
}

/// Parse one wal2json (format-version 2) change record. Malformed
/// input fails closed with the byte context; guessing a change shape
/// would silently corrupt identity.
pub fn parse_change(payload: &str) -> Result<CdcChange> {
    let value: serde_json::Value = serde_json::from_str(payload)
        .with_context(|| format!("parsing wal2json change `{payload}`"))?;
    let action = value
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("wal2json change carries no action"))?
        .to_string();
    if !matches!(action.as_str(), "I" | "U" | "D" | "T" | "M") {
        bail!("unknown wal2json action `{action}`");
    }
    let table = value
        .get("table")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let columns = match value.get("columns") {
        Some(columns) => cells_of(columns)?,
        None => BTreeMap::new(),
    };
    let identity = match value.get("identity") {
        Some(identity) => cells_of(identity)?,
        None => BTreeMap::new(),
    };
    Ok(CdcChange {
        action,
        table,
        columns,
        identity,
    })
}

/// The outcome of mapping one change.
#[derive(Debug, PartialEq)]
pub enum MappedChange {
    /// A row ready for a window.
    Row(Row),
    /// A DELETE the protocol cannot yet express — counted, never
    /// dropped silently.
    Delete { pk: Option<String> },
    /// A change for another table — skipped by declaration, not by
    /// accident.
    OtherTable,
    /// A row without a usable pk (null pk cell).
    SkippedNoPk,
}

/// Map one change under the declared mapping. Updates resolve the row
/// cells from the change's NEW image when present, falling back to
/// the identity image for columns the new image omits (toast
/// unchanged values can be absent from UPDATE images).
pub fn map_change(mapping: &CdcMapping, change: &CdcChange) -> Result<MappedChange> {
    if change.table != mapping.table {
        return Ok(MappedChange::OtherTable);
    }
    match change.action.as_str() {
        "I" | "U" => {
            let mut cells = change.identity.clone();
            cells.extend(change.columns.clone());
            match exocortex_adapter_table::row_from_cells(
                &mapping.mapping,
                &cells,
                &BTreeMap::new(),
            )? {
                Some(row) => Ok(MappedChange::Row(row)),
                None => Ok(MappedChange::SkippedNoPk),
            }
        }
        "D" => {
            let mut cells = change.identity.clone();
            cells.extend(change.columns.clone());
            let pk = exocortex_adapter_table::row_from_cells(
                &mapping.mapping,
                &cells,
                &BTreeMap::new(),
            )?
            .map(|row| row.pk);
            Ok(MappedChange::Delete { pk })
        }
        // Transaction boundaries and keepalives are not row events.
        _ => Ok(MappedChange::OtherTable),
    }
}

/// Map one window of rows to a submission unit under the
/// `cdc-postgres` flavor tag (see `exocortex_adapter_table::map_rows`).
pub fn map_rows(
    mapping: &CdcMapping,
    table_uuid: &[u8; 16],
    declared: &[(String, String)],
    rows: &[Row],
    batch_id_seed: &str,
) -> (exocortex_adapter_sdk::BatchUnit, usize) {
    exocortex_adapter_table::map_rows(
        &mapping.mapping,
        table_uuid,
        declared,
        rows,
        batch_id_seed,
        "cdc-postgres",
    )
}

/// Fill a unit's snapshot id (`lsn-<16 hex>`).
pub fn with_snapshot_id(
    unit: exocortex_adapter_sdk::BatchUnit,
    snapshot_id: &str,
) -> exocortex_adapter_sdk::BatchUnit {
    exocortex_adapter_table::with_snapshot_id(unit, snapshot_id)
}

/// The D21-a projection this adapter declares: the selector names the
/// slot's table and mapping version; the declared schema is the
/// mapped columns typed by the authored column types; the bounds stop
/// the window rather than truncate it; the last snapshot id is the
/// settled LSN, so a regressed slot fails the rewind checks.
pub fn projection(
    mapping: &CdcMapping,
    max_window: u64,
    last_lsn: &str,
) -> exocortex_adapter_sdk::Projection {
    exocortex_adapter_table::table_projection(
        format!(
            "cdc-postgres table {} via logical replication slot under the declared column mapping v{}",
            mapping.table, mapping.mapping.mapping_version
        ),
        &mapping.mapping,
        mapping.declared_columns(),
        max_window,
        last_lsn,
    )
}

/// The table uuid for a slot (identity scoped by the operator's
/// stable slot/table id).
pub fn table_uuid_for_slot(table_id: &str) -> [u8; 16] {
    table_uuid_for(table_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping() -> CdcMapping {
        serde_json::from_str(
            r#"{
                "table": "public.orders",
                "memory_type": "Problem",
                "title_column": "title",
                "content_columns": ["detail"],
                "pk_columns": ["id"],
                "tags_column": "tags",
                "parent_column": "parent_id",
                "parent_kind": "Causes",
                "mapping_version": 1,
                "column_types": {
                    "id": "int8",
                    "title": "text",
                    "detail": "text",
                    "tags": "text",
                    "parent_id": "int8"
                }
            }"#,
        )
        .unwrap()
    }

    const INSERT: &str = r#"{"action":"I","sequence":"1","table":"public.orders","columns":[
        {"name":"id","type":"int8","value":7},
        {"name":"title","type":"text","value":"late order"},
        {"name":"detail","type":"text","value":"arrived after cutoff"},
        {"name":"tags","type":"text","value":"ops, billing"},
        {"name":"parent_id","type":"int8","value":null}]}"#;

    #[test]
    fn inserts_parse_and_map_with_stable_identity() {
        let change = parse_change(INSERT).unwrap();
        assert_eq!(change.action, "I");
        assert_eq!(change.table, "public.orders");
        let mapped = map_change(&mapping(), &change).unwrap();
        match mapped {
            MappedChange::Row(row) => {
                assert_eq!(row.pk, "7");
                assert_eq!(row.title, "late order");
                assert_eq!(
                    row.content,
                    vec![("detail".to_string(), "arrived after cutoff".to_string())]
                );
                assert_eq!(row.tags, vec!["ops", "billing"]);
                assert_eq!(row.parent, None);
            }
            other => panic!("expected a row, got {other:?}"),
        }
    }

    #[test]
    fn update_without_a_pk_cell_skips_counted() {
        let change = parse_change(
            r#"{"action":"U","table":"public.orders","columns":[{"name":"title","type":"text","value":"x"}],"identity":[]}"#,
        )
        .unwrap();
        assert_eq!(
            map_change(&mapping(), &change).unwrap(),
            MappedChange::SkippedNoPk
        );
    }

    #[test]
    fn update_resolves_new_image_over_identity() {
        let change = parse_change(
            r#"{"action":"U","table":"public.orders",
                "columns":[{"name":"id","type":"int8","value":7},{"name":"detail","type":"text","value":"new"}],
                "identity":[{"name":"id","type":"int8","value":7},{"name":"detail","type":"text","value":"old"},
                            {"name":"title","type":"text","value":"identity title"}]}"#,
        )
        .unwrap();
        match map_change(&mapping(), &change).unwrap() {
            MappedChange::Row(row) => {
                assert_eq!(row.pk, "7");
                assert_eq!(
                    row.content,
                    vec![("detail".to_string(), "new".to_string())],
                    "the new image wins where present"
                );
                assert_eq!(
                    row.title, "identity title",
                    "identity fills toast-omitted columns"
                );
            }
            other => panic!("expected a row, got {other:?}"),
        }
    }

    #[test]
    fn deletes_are_counted_not_dropped() {
        let change = parse_change(
            r#"{"action":"D","table":"public.orders","identity":[{"name":"id","type":"int8","value":7}]}"#,
        )
        .unwrap();
        assert_eq!(
            map_change(&mapping(), &change).unwrap(),
            MappedChange::Delete {
                pk: Some("7".to_string())
            }
        );
    }

    #[test]
    fn other_tables_and_transactions_skip_by_declaration() {
        let other = parse_change(
            r#"{"action":"I","table":"public.users","columns":[{"name":"id","type":"int8","value":1}]}"#,
        )
        .unwrap();
        assert_eq!(
            map_change(&mapping(), &other).unwrap(),
            MappedChange::OtherTable
        );
        let commit = parse_change(r#"{"action":"T","table":""}"#).unwrap();
        assert_eq!(
            map_change(&mapping(), &commit).unwrap(),
            MappedChange::OtherTable
        );
    }

    #[test]
    fn malformed_changes_fail_closed() {
        let err = parse_change("{not json").unwrap_err().to_string();
        assert!(err.contains("wal2json"), "{err}");
        let err = parse_change(r#"{"action":"X"}"#).unwrap_err().to_string();
        assert!(err.contains("action"), "{err}");
    }

    #[test]
    fn mapping_requires_declared_types_for_every_column() {
        let mut bad = mapping();
        bad.column_types.remove("tags");
        let err = bad.validate().unwrap_err().to_string();
        assert!(err.contains("`tags`"), "{err}");
    }

    #[test]
    fn declared_columns_are_typed_by_the_authored_schema() {
        let declared = mapping().declared_columns();
        let names: Vec<&str> = declared.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["title", "detail", "id", "tags", "parent_id"]);
        assert!(declared.iter().any(|(n, t)| n == "id" && t == "int8"));
    }

    #[test]
    fn batch_units_carry_the_cdc_flavor_and_canonical_hash() {
        let rows = vec![Row {
            pk: "7".into(),
            title: "late order".into(),
            content: vec![],
            tags: vec![],
            parent: None,
        }];
        let mapping = mapping();
        let declared = mapping.declared_columns();
        let (unit, _) = map_rows(
            &mapping,
            &table_uuid_for_slot("public.orders"),
            &declared,
            &rows,
            "w-0",
        );
        assert_eq!(
            unit.snapshot.as_ref().unwrap().source_flavor,
            "cdc-postgres"
        );
        assert_eq!(
            unit.snapshot.as_ref().unwrap().schema_hash,
            exocortex_wire::projection::schema_hash(&declared).to_vec()
        );
    }
}
