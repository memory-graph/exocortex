//! D22 (training-corpus direction): materialize the graph as JSONL for
//! training and evaluation, with the two properties that fall out of the
//! existing design rather than needing to be built:
//!
//! - **temporally-clean splits**: `--corpus-as-of T` yields everything
//!   the graph believed as of T and nothing later — `recorded_at <= T`
//!   (we knew it), `valid_from <= T`, and still open (`valid_until`
//!   unset or `> T`). No future leakage by construction: a row corrected
//!   tomorrow is absent from today's cut, and its superseded version is
//!   present exactly as it was believed.
//! - **per-record lineage**: one manifest row per exported memory —
//!   provenance (who/what produced it, asserted or derived), the
//!   external join identity (R-T18a), and the backend LSN — answering
//!   "where did this row come from" without a second graph walk.
//!
//! Boundary (D24, which gates EGRESS): this exporter reads the org's own
//! store; whether a corpus may LEAVE the org is a rights-and-consent
//! question that does not exist in the model yet. The manifest says so
//! on every write.

use anyhow::{Context, Result};
use exocortex_kernel::{Memory, Relationship};
use exocortex_storage::Storage;
use futures::StreamExt;
use std::path::Path;

/// The format discriminator written into every corpus manifest.
pub const FORMAT: &str = "exocortex-corpus";
/// The format version this build writes.
pub const VERSION: u32 = 1;

/// One lineage manifest row.
#[derive(serde::Serialize)]
pub struct LineageRow {
    /// Exported memory id (32-hex).
    pub id: String,
    /// Memory type label at export time.
    pub memory_type: String,
    /// `asserted` | `extracted` | `derived` | `external-snapshot`.
    pub provenance: String,
    /// Author (asserted/extracted) or rule id (derived).
    pub source: String,
    /// Producer kind discriminant, when the row carries one.
    pub producer_kind: Option<String>,
    /// External join identity (`table_uuid-hex:logical_pk`, R-T18a),
    /// straight from the row's snapshot provenance — raw coordinates,
    /// not a digest.
    pub external_key: Option<String>,
    /// The typed entities the memory is about (hex ids, R-T18).
    pub entities: Vec<String>,
    /// The LSN the row was written under.
    pub lsn: u64,
    /// D24: the row's rights verdict — `licensed` (licence + consent
    /// both claimed), `partial` (rights present but incomplete), or
    /// `none` (no rights claimed). Egress decisions read the
    /// manifest's aggregate, computed from exactly these.
    pub rights: String,
}

/// The corpus manifest (one JSON document beside the JSONL files).
#[derive(serde::Serialize)]
pub struct CorpusManifest {
    /// Always [`FORMAT`].
    pub format: String,
    /// The corpus format version this build writes.
    pub version: u32,
    /// 64-hex compatibility fingerprint of the exporting ontology.
    pub compatibility_fingerprint: String,
    /// The cut this corpus represents (RFC3339); "now" when unset.
    pub as_of: Option<String>,
    /// Memories in the cut.
    pub memories: usize,
    /// Edges in the cut (both endpoints in the cut).
    pub edges: usize,
    /// Computed-only kinds (R-T14) present in the edge cut, named — a
    /// consumer can drop Dreams rows by kind instead of trusting dedup.
    pub computed_only_kinds: Vec<String>,
    /// D24 boundary, stated on every export.
    pub egress: String,
}

/// The bi-temporal cut: everything believed as of `as_of` (None = now).
fn memory_believed_at(memory: &Memory, as_of: chrono::DateTime<chrono::Utc>) -> bool {
    memory.recorded_at <= as_of
        && memory.valid_from <= as_of
        && memory.valid_until.is_none_or(|until| until > as_of)
}

fn edge_believed_at(edge: &Relationship, as_of: chrono::DateTime<chrono::Utc>) -> bool {
    edge.recorded_at <= as_of
        && edge.valid_from <= as_of
        && edge.valid_until.is_none_or(|until| until > as_of)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Export the corpus cut into `dir` (created if absent): `memories.jsonl`,
/// `edges.jsonl`, `lineage.jsonl`, `manifest.json`. Returns the manifest.
pub async fn export_corpus<S: Storage>(
    storage: &S,
    ontology: &exocortex_kernel::Ontology,
    as_of: Option<chrono::DateTime<chrono::Utc>>,
    dir: &Path,
) -> Result<CorpusManifest> {
    let cut = as_of.unwrap_or_else(chrono::Utc::now);
    std::fs::create_dir_all(dir)
        .with_context(|| format!("create corpus directory {}", dir.display()))?;

    let mut memories = Vec::new();
    let mut stream = storage.stream_all_memories().await;
    while let Some(row) = stream.next().await {
        let memory = row.context("stream memory")?;
        if memory_believed_at(&memory, cut) {
            memories.push(memory);
        }
    }
    let included: std::collections::HashSet<_> = memories.iter().map(|m| m.id).collect();

    let mut edges = Vec::new();
    let mut stream = storage.stream_all_relationships().await;
    while let Some(row) = stream.next().await {
        let edge = row.context("stream relationship")?;
        // Both endpoints must be in the cut: a training corpus with
        // dangling references is a silent label leak waiting to happen.
        if edge_believed_at(&edge, cut)
            && included.contains(&edge.from)
            && included.contains(&edge.to)
        {
            edges.push(edge);
        }
    }

    let mut computed_only_kinds = std::collections::BTreeSet::new();
    for edge in &edges {
        if let Some(kind) = ontology.kinds_by_id.get(&edge.kind) {
            if kind.computed_only {
                computed_only_kinds.insert(kind.display_name.to_string());
            }
        }
    }

    let mut memories_out = String::new();
    let mut lineage_out = String::new();
    let mut lineage_rows = Vec::with_capacity(memories.len());
    for memory in &memories {
        memories_out.push_str(&serde_json::to_string(memory).context("serialize memory row")?);
        memories_out.push('\n');
        let (provenance, source, producer_kind, external_key) = match &memory.provenance {
            exocortex_kernel::Provenance::Asserted {
                author,
                producer_kind,
            } => (
                "asserted".to_string(),
                author.to_string(),
                producer_kind.map(|kind| format!("{kind:?}")),
                None,
            ),
            exocortex_kernel::Provenance::Extracted { .. } => {
                ("extracted".to_string(), String::new(), None, None)
            }
            exocortex_kernel::Provenance::Derived { rule_id, .. } => {
                ("derived".to_string(), rule_id.to_string(), None, None)
            }
            exocortex_kernel::Provenance::Computed { .. } => {
                ("computed".to_string(), String::new(), None, None)
            }
            // Proposed never persists (rule 6); a stored row carrying it
            // would be a kernel bug worth naming, not hiding.
            exocortex_kernel::Provenance::Proposed { .. } => {
                ("proposed".to_string(), String::new(), None, None)
            }
            exocortex_kernel::Provenance::ExternalSnapshot(snapshot) => {
                let key = if snapshot.external_key.table_uuid.is_empty() {
                    None
                } else {
                    Some(format!(
                        "{}:{}",
                        snapshot.external_key.table_uuid,
                        String::from_utf8_lossy(&snapshot.external_key.logical_pk)
                    ))
                };
                (
                    "external-snapshot".to_string(),
                    snapshot.source_uri.to_string(),
                    Some(snapshot.producer_id.to_string()),
                    key,
                )
            }
        };
        let row = LineageRow {
            id: hex(&memory.id.0),
            memory_type: ontology
                .memory_type_names
                .get(memory.memory_type as usize)
                .map(|name| name.to_string())
                .unwrap_or_default(),
            provenance,
            source,
            producer_kind,
            external_key,
            entities: memory
                .context
                .entities
                .iter()
                .map(|entity| hex(&entity.0))
                .collect(),
            lsn: memory.lsn.value,
            rights: match &memory.rights {
                Some(rights) if rights.egress_permitted() => "licensed".into(),
                Some(_) => "partial".into(),
                None => "none".into(),
            },
        };
        lineage_out.push_str(&serde_json::to_string(&row).context("serialize lineage row")?);
        lineage_out.push('\n');
        lineage_rows.push(row);
    }
    let mut edges_out = String::new();
    for edge in &edges {
        edges_out.push_str(&serde_json::to_string(edge).context("serialize edge row")?);
        edges_out.push('\n');
    }

    // D24: the egress verdict is COMPUTED from the exported rows'
    // own rights claims — every row licensed + consented, or the
    // corpus does not leave under this manifest. Fail closed.
    let licensed = memories
        .iter()
        .filter(|memory| {
            memory
                .rights
                .as_ref()
                .is_some_and(exocortex_kernel::memory::Rights::egress_permitted)
        })
        .count();
    let partial = memories
        .iter()
        .filter(|memory| {
            memory
                .rights
                .as_ref()
                .is_some_and(|rights| !rights.egress_permitted())
        })
        .count();
    let egress = if memories.is_empty() {
        "empty corpus: no rows, no egress claim".to_string()
    } else if licensed == memories.len() {
        format!("permitted: all {licensed} exported rows claim licence + consent basis (D24)")
    } else {
        format!(
            "NOT permitted: {licensed}/{} rows claim licence + consent; {partial} carry              incomplete rights; {} claim none — a corpus leaves the org only when every              row is covered (D24, fail closed)",
            memories.len(),
            memories.len() - licensed - partial
        )
    };

    let manifest = CorpusManifest {
        format: FORMAT.into(),
        version: VERSION,
        compatibility_fingerprint: hex(&ontology.fingerprint.0),
        as_of: as_of.map(|t| t.to_rfc3339()),
        memories: memories.len(),
        edges: edges.len(),
        computed_only_kinds: computed_only_kinds.into_iter().collect(),
        egress,
    };

    let noun = "corpus export";
    exocortex_storage::bounded_io::atomic_write_private(
        &dir.join("memories.jsonl"),
        memories_out.as_bytes(),
        noun,
    )
    .with_context(|| format!("write {}", dir.join("memories.jsonl").display()))?;
    exocortex_storage::bounded_io::atomic_write_private(
        &dir.join("edges.jsonl"),
        edges_out.as_bytes(),
        noun,
    )
    .with_context(|| format!("write {}", dir.join("edges.jsonl").display()))?;
    exocortex_storage::bounded_io::atomic_write_private(
        &dir.join("lineage.jsonl"),
        lineage_out.as_bytes(),
        noun,
    )
    .with_context(|| format!("write {}", dir.join("lineage.jsonl").display()))?;
    exocortex_storage::bounded_io::atomic_write_private(
        &dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)
            .context("serialize manifest")?
            .as_slice(),
        noun,
    )
    .with_context(|| format!("write {}", dir.join("manifest.json").display()))?;
    Ok(manifest)
}
