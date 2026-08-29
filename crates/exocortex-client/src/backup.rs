//! BR-PRD (docs/prd/backup-restore-prd.md): standalone memory backup and
//! restore — the WAL as a single portable file.
//!
//! Export dumps every entry (all states, LSN order) into a versioned,
//! fingerprint-stamped JSON document; two exports of the same WAL are
//! identical modulo `created_at`, so backups diff and review cleanly.
//! Import is all-or-nothing: the fingerprint gate and per-draft
//! revalidation run BEFORE anything is written, entries append re-keyed
//! with their ids/batch ids/states preserved, and the operation is
//! idempotent by construction (ids upsert; batch ids de-duplicate at
//! the drain).

use anyhow::{Context, Result};
use exocortex_storage::bounded_io::{
    atomic_write_private, read_bounded, serialize_json_pretty_bounded,
};

use crate::wal::{Wal, WalEntry};

/// The format discriminator written into every backup.
pub const FORMAT: &str = "exocortex-backup";
/// The format version this build writes and accepts.
pub const VERSION: u32 = 1;
/// Maximum encoded backup size accepted or produced by the standalone tool.
///
/// The WAL itself is capped at 100 MiB. JSON string escaping can expand one
/// input byte to six output bytes, so 640 MiB preserves exportability of every
/// valid WAL while still placing a strict ceiling on serialization and parsing.
pub const MAX_BACKUP_BYTES: u64 = 640 * 1024 * 1024;
const BACKUP_NOUN: &str = "backup";

/// One backup document.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Backup {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`] for files this build reads/writes.
    pub version: u32,
    /// RFC 3339 export time.
    pub created_at: String,
    /// Hex of the effective ontology fingerprint at export time (the
    /// compatibility level since OC-PRD D1).
    pub ontology_fingerprint: String,
    /// The structured ontology summary at export time (OC-PRD D2
    /// backup row): present in every post-OC export, absent in
    /// pre-OC documents (which keep the legacy exact-match gate).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ontology_summary: Option<exocortex_kernel::OntologySummary>,
    /// Every WAL entry, in local-LSN order, in the WAL's own row shape.
    pub entries: Vec<WalEntry>,
}

/// Export every entry to `path` (pretty JSON, LSN order). Returns the
/// entry count.
pub fn export(
    wal: &Wal,
    fingerprint: &str,
    summary: &exocortex_kernel::OntologySummary,
    path: &std::path::Path,
) -> Result<usize> {
    let entries = wal.entries().context("read WAL for backup")?;
    let doc = Backup {
        format: FORMAT.into(),
        version: VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        ontology_fingerprint: fingerprint.into(),
        ontology_summary: Some(summary.clone()),
        entries,
    };
    let n = doc.entries.len();
    let json = serialize_json_pretty_bounded(&doc, MAX_BACKUP_BYTES, BACKUP_NOUN)
        .context("serialize backup")?;
    atomic_write_private(path, &json, BACKUP_NOUN)
        .with_context(|| format!("write backup {}", path.display()))?;
    Ok(n)
}

/// What one import did.
#[derive(Debug, serde::Serialize)]
pub struct ImportReport {
    /// Entries appended.
    pub imported: usize,
    /// The first appended local LSN (0 when nothing was imported).
    pub first_local_lsn: u64,
}

/// Import a backup into `wal`. All-or-nothing: the format/version
/// gates, the fingerprint gate (fail closed — restoring data typed
/// against a different ontology is silent corruption, not a warning),
/// and per-draft revalidation (the same `validate_draft` the offline
/// write path runs, W2's one rulebook) all run before the first
/// append. State rides verbatim: `Synced` never re-drains, `Failed`
/// keeps its history.
pub fn import(
    wal: &Wal,
    ontology: &exocortex_kernel::Ontology,
    path: &std::path::Path,
) -> Result<ImportReport> {
    let raw = read_bounded(path, MAX_BACKUP_BYTES, BACKUP_NOUN)?;
    let doc: Backup = serde_json::from_slice(&raw).context("parse backup")?;
    anyhow::ensure!(
        doc.format == FORMAT,
        "not an exocortex backup (format `{}`)",
        doc.format
    );
    anyhow::ensure!(
        doc.version == VERSION,
        "backup version {} unsupported (this build reads {VERSION})",
        doc.version
    );
    // OC-PRD D2 (backup row): superset accepted, because every draft
    // is revalidated against the current rulebook below before the WAL
    // is touched. Post-OC documents prove their subset structurally;
    // pre-OC documents carry only the v1-scheme hash and keep exact
    // equality against this build's recomputation.
    let verdict = match &doc.ontology_summary {
        Some(summary) => exocortex_kernel::admit_backup(
            exocortex_kernel::BackupOntology::Summarized { summary },
            ontology,
        ),
        None => exocortex_kernel::admit_backup(
            exocortex_kernel::BackupOntology::Legacy {
                fingerprint_hex: &doc.ontology_fingerprint,
            },
            ontology,
        ),
    };
    if let Err(error) = verdict {
        anyhow::bail!(
            "ontology mismatch: {error} — the backup was written against a different pack set"
        );
    }
    // Revalidate every draft against the one rulebook before touching
    // the WAL. Any rejection aborts the whole import.
    for (i, e) in doc.entries.iter().enumerate() {
        anyhow::ensure!(
            e.memories.len() == e.memory_ids.len(),
            "entry {i}: memories/memory_ids length mismatch"
        );
        for (j, d) in e.memories.iter().enumerate() {
            exocortex_kernel::validator::validate_draft(
                ontology,
                d,
                exocortex_kernel::validator::SourceCeiling {
                    source: "backup-import",
                    ceiling: exocortex_kernel::Visibility::Org,
                },
            )
            .with_context(|| format!("entry {i} draft {j} fails current-ontology validation"))?;
        }
    }
    let imported = doc.entries.len();
    let first = wal
        .append_imported_batch(doc.entries)
        .context("atomically append imported backup")?;
    Ok(ImportReport {
        imported,
        first_local_lsn: first,
    })
}

#[cfg(test)]
mod tests {
    use super::BACKUP_NOUN;
    use exocortex_storage::bounded_io::{ensure_size, read_bounded, serialize_json_pretty_bounded};

    #[test]
    fn backup_size_boundary_is_inclusive_and_file_reads_are_bounded() {
        assert!(ensure_size(7, 7, BACKUP_NOUN).is_ok());
        assert!(ensure_size(8, 7, BACKUP_NOUN).is_err());

        let dir =
            std::env::temp_dir().join(format!("exocortex-bounded-backup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("backup.json");
        std::fs::write(&path, b"1234567").unwrap();
        assert_eq!(read_bounded(&path, 7, BACKUP_NOUN).unwrap(), b"1234567");
        assert!(read_bounded(&path, 6, BACKUP_NOUN).is_err());
        assert!(serialize_json_pretty_bounded(&"x", 3, BACKUP_NOUN).is_ok());
        assert!(serialize_json_pretty_bounded(&"x", 2, BACKUP_NOUN).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
