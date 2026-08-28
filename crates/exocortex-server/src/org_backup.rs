//! BR2 (BR-PRD's deferred backend leg): org-mode backup and restore —
//! the durable store as a file.
//!
//! Export streams every memory and relationship out of any `Storage`
//! backend into a versioned, fingerprint-stamped JSON document (the
//! kernel row shapes are the format — no parallel schema to drift).
//! Restore is a disaster-recovery primitive, not a re-ingest: rows land
//! with their ids, provenance, content, and temporal fields intact
//! through plain upserts, after the fingerprint gate. LSNs are the one
//! sanctioned re-write — storage is the sequence authority (§6.2) and
//! stamps rows at upsert; ordering survives, the old counter does not. The fenced-write rule (R-C3) governs
//! owner writes in a RUNNING cluster; a restore runs as an admin
//! one-shot against quiesced storage, which is the DR model.

use anyhow::{Context, Result};
use exocortex_kernel::{Memory, Ontology, Relationship};
use exocortex_storage::Storage;
use futures::StreamExt;
use std::io::Read as _;
use std::io::Write as _;

/// The format discriminator written into every org backup.
pub const FORMAT: &str = "exocortex-org-backup";
/// The format version this build writes and accepts.
pub const VERSION: u32 = 1;
/// Maximum encoded org backup accepted or produced by the one-shot DR tool.
/// This is deliberately finite because row payloads and graph cardinality are
/// operator-controlled rather than constrained by the ingestion batch limits.
pub const MAX_ORG_BACKUP_BYTES: u64 = 256 * 1024 * 1024;

/// One org backup document.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OrgBackup {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`] for files this build reads/writes.
    pub version: u32,
    /// RFC 3339 export time.
    pub created_at: String,
    /// The org this graph serves (storage-scoped; recorded for the
    /// restore target to assert).
    pub org_id: String,
    /// Hex of the effective ontology fingerprint at export time.
    pub ontology_fingerprint: String,
    /// Every memory row, in the kernel row shape.
    pub memories: Vec<Memory>,
    /// Every relationship row, in the kernel row shape.
    pub relationships: Vec<Relationship>,
}

/// What one import did.
#[derive(Debug, serde::Serialize)]
pub struct ImportReport {
    /// Memory rows restored.
    pub memories: usize,
    /// Relationship rows restored.
    pub relationships: usize,
}

/// Export every row to `path` (pretty JSON, stream order). Returns
/// `(memories, relationships)` counts.
pub async fn export_org<S: Storage>(
    storage: &S,
    org_id: &str,
    fingerprint: &str,
    path: &std::path::Path,
) -> Result<(usize, usize)> {
    let mut memories = Vec::new();
    let mut encoded_row_bytes = 0u64;
    let mut rows = storage.stream_all_memories().await;
    while let Some(row) = rows.next().await {
        let row = row.context("stream memory")?;
        encoded_row_bytes = encoded_row_bytes.saturating_add(
            serde_json::to_vec(&row)
                .context("size memory for backup")?
                .len() as u64,
        );
        ensure_backup_size(encoded_row_bytes, MAX_ORG_BACKUP_BYTES)?;
        memories.push(row);
    }
    let mut relationships = Vec::new();
    let mut rels = storage.stream_all_relationships().await;
    while let Some(r) = rels.next().await {
        let r = r.context("stream relationship")?;
        encoded_row_bytes = encoded_row_bytes.saturating_add(
            serde_json::to_vec(&r)
                .context("size relationship for backup")?
                .len() as u64,
        );
        ensure_backup_size(encoded_row_bytes, MAX_ORG_BACKUP_BYTES)?;
        relationships.push(r);
    }
    let (nm, nr) = (memories.len(), relationships.len());
    let doc = OrgBackup {
        format: FORMAT.into(),
        version: VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        org_id: org_id.into(),
        ontology_fingerprint: fingerprint.into(),
        memories,
        relationships,
    };
    let bytes = serialize_bounded(&doc, MAX_ORG_BACKUP_BYTES).context("serialize backup")?;
    atomic_write_private(path, &bytes)
        .with_context(|| format!("write backup {}", path.display()))?;
    Ok((nm, nr))
}

/// Restore a backup into `storage`. All-or-nothing semantics at the
/// gate level: format/version/org/fingerprint checks all run before
/// the first upsert (upserts themselves are idempotent — a re-run
/// converges on the same rows).
pub async fn import_org<S: Storage>(
    storage: &S,
    ontology: &Ontology,
    org_id: &str,
    path: &std::path::Path,
) -> Result<ImportReport> {
    let raw = read_bounded(path, MAX_ORG_BACKUP_BYTES)?;
    let doc: OrgBackup = serde_json::from_slice(&raw).context("parse backup")?;
    anyhow::ensure!(
        doc.format == FORMAT,
        "not an exocortex org backup (format `{}`)",
        doc.format
    );
    anyhow::ensure!(
        doc.version == VERSION,
        "backup version {} unsupported (this build reads {VERSION})",
        doc.version
    );
    anyhow::ensure!(
        doc.org_id == org_id,
        "org mismatch: backup serves `{}`, target serves `{org_id}`",
        doc.org_id
    );
    let expected = hex(&ontology.fingerprint.0);
    anyhow::ensure!(
        doc.ontology_fingerprint == expected,
        "ontology fingerprint mismatch: backup {} vs binary {expected} — the backup was written against a different pack set",
        doc.ontology_fingerprint
    );
    storage
        .upsert_batch(&doc.memories, &doc.relationships)
        .await
        .context("atomically restore org backup")?;
    Ok(ImportReport {
        memories: doc.memories.len(),
        relationships: doc.relationships.len(),
    })
}

fn ensure_backup_size(size: u64, limit: u64) -> Result<()> {
    anyhow::ensure!(
        size <= limit,
        "org backup is {size} bytes; maximum supported size is {limit} bytes"
    );
    Ok(())
}

fn serialize_bounded<T: serde::Serialize>(value: &T, limit: u64) -> Result<Vec<u8>> {
    let mut output = BoundedOutput::new(limit);
    serde_json::to_writer_pretty(&mut output, value)?;
    Ok(output.bytes)
}

struct BoundedOutput {
    bytes: Vec<u8>,
    limit: u64,
}

impl BoundedOutput {
    fn new(limit: u64) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}

impl std::io::Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.bytes.len() as u64).saturating_add(bytes.len() as u64) > self.limit {
            return Err(std::io::Error::other(format!(
                "org backup exceeds maximum supported size of {} bytes",
                self.limit
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn read_bounded(path: &std::path::Path, limit: u64) -> Result<Vec<u8>> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("read backup {}", path.display()))?;
    ensure_backup_size(
        file.metadata()
            .with_context(|| format!("inspect backup {}", path.display()))?
            .len(),
        limit,
    )?;
    let mut raw = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut raw)
        .with_context(|| format!("read backup {}", path.display()))?;
    ensure_backup_size(raw.len() as u64, limit)?;
    Ok(raw)
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn atomic_write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    atomic_write_private_with(path, bytes, |_| Ok(()))
}

fn atomic_write_private_with(
    path: &std::path::Path,
    bytes: &[u8],
    before_rename: impl FnOnce(&std::path::Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("backup");
    let mut opened = None;
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(".{name}.tmp-{}-{attempt}", std::process::id()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => {
                opened = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary, mut file) = opened.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate backup temporary file",
        )
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        before_rename(&temporary)?;
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{atomic_write_private_with, ensure_backup_size, read_bounded, serialize_bounded};

    #[test]
    fn org_backup_size_boundary_is_inclusive_and_file_reads_are_bounded() {
        assert!(ensure_backup_size(11, 11).is_ok());
        assert!(ensure_backup_size(12, 11).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("org.json");
        std::fs::write(&path, b"12345678901").unwrap();
        assert_eq!(read_bounded(&path, 11).unwrap(), b"12345678901");
        assert!(read_bounded(&path, 10).is_err());
        assert!(serialize_bounded(&"x", 3).is_ok());
        assert!(serialize_bounded(&"x", 2).is_err());
    }

    #[test]
    fn org_backup_atomic_write_preserves_previous_file_on_injected_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("org.json");
        std::fs::write(&path, b"previous").unwrap();
        atomic_write_private_with(&path, b"replacement", |_| {
            Err(std::io::Error::other("injected before rename"))
        })
        .unwrap_err();
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
    }
}
