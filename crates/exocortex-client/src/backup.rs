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

use crate::wal::{Wal, WalEntry};

/// The format discriminator written into every backup.
pub const FORMAT: &str = "exocortex-backup";
/// The format version this build writes and accepts.
pub const VERSION: u32 = 1;

/// One backup document.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Backup {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`] for files this build reads/writes.
    pub version: u32,
    /// RFC 3339 export time.
    pub created_at: String,
    /// Hex of the effective ontology fingerprint at export time.
    pub ontology_fingerprint: String,
    /// Every WAL entry, in local-LSN order, in the WAL's own row shape.
    pub entries: Vec<WalEntry>,
}

/// Export every entry to `path` (pretty JSON, LSN order). Returns the
/// entry count.
pub fn export(wal: &Wal, fingerprint: &str, path: &std::path::Path) -> Result<usize> {
    let entries = wal.entries();
    let doc = Backup {
        format: FORMAT.into(),
        version: VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        ontology_fingerprint: fingerprint.into(),
        entries,
    };
    let n = doc.entries.len();
    let json = serde_json::to_string_pretty(&doc).context("serialize backup")?;
    std::fs::write(path, json).with_context(|| format!("write backup {}", path.display()))?;
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
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("read backup {}", path.display()))?;
    let doc: Backup = serde_json::from_str(&raw).context("parse backup")?;
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
    let expected = hex(&ontology.fingerprint.0);
    anyhow::ensure!(
        doc.ontology_fingerprint == expected,
        "ontology fingerprint mismatch: backup {} vs binary {expected} — the backup was written against a different pack set",
        doc.ontology_fingerprint
    );
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
    let mut first = 0u64;
    for (i, e) in doc.entries.into_iter().enumerate() {
        let lsn = wal.append_imported(e).context("append imported entry")?;
        if i == 0 {
            first = lsn;
        }
    }
    Ok(ImportReport {
        imported,
        first_local_lsn: first,
    })
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}
