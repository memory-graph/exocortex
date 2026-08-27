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
use std::io::Write as _;

/// The format discriminator written into every org backup.
pub const FORMAT: &str = "exocortex-org-backup";
/// The format version this build writes and accepts.
pub const VERSION: u32 = 1;

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
    let mut rows = storage.stream_all_memories().await;
    while let Some(row) = rows.next().await {
        memories.push(row.context("stream memory")?);
    }
    let mut relationships = Vec::new();
    let mut rels = storage.stream_all_relationships().await;
    while let Some(r) = rels.next().await {
        relationships.push(r.context("stream relationship")?);
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
    let bytes = serde_json::to_vec_pretty(&doc).context("serialize backup")?;
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
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("read backup {}", path.display()))?;
    let doc: OrgBackup = serde_json::from_str(&raw).context("parse backup")?;
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
    for m in &doc.memories {
        storage.upsert_memory(m).await.context("restore memory")?;
    }
    for r in &doc.relationships {
        storage
            .upsert_relationship(r)
            .await
            .context("restore relationship")?;
    }
    Ok(ImportReport {
        memories: doc.memories.len(),
        relationships: doc.relationships.len(),
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
    use super::atomic_write_private_with;

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
