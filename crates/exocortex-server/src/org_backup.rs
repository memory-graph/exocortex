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
use exocortex_kernel::{
    relationship_visibility, validator::validate_triple, Memory, MemoryId, Ontology, Provenance,
    Relationship, RelationshipId, Visibility,
};
use exocortex_storage::{
    bounded_io::{atomic_write_private, ensure_size, read_bounded, serialize_json_pretty_bounded},
    Storage,
};
use futures::StreamExt;

/// The format discriminator written into every org backup.
pub const FORMAT: &str = "exocortex-org-backup";
/// The format version this build writes and accepts.
pub const VERSION: u32 = 1;
/// Maximum encoded org backup accepted or produced by the one-shot DR tool.
/// This is deliberately finite because row payloads and graph cardinality are
/// operator-controlled rather than constrained by the ingestion batch limits.
pub const MAX_ORG_BACKUP_BYTES: u64 = 256 * 1024 * 1024;
const BACKUP_NOUN: &str = "org backup";

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
        ensure_size(encoded_row_bytes, MAX_ORG_BACKUP_BYTES, BACKUP_NOUN)?;
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
        ensure_size(encoded_row_bytes, MAX_ORG_BACKUP_BYTES, BACKUP_NOUN)?;
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
    let bytes = serialize_json_pretty_bounded(&doc, MAX_ORG_BACKUP_BYTES, BACKUP_NOUN)
        .context("serialize backup")?;
    atomic_write_private(path, &bytes, BACKUP_NOUN)
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
    let raw = read_bounded(path, MAX_ORG_BACKUP_BYTES, BACKUP_NOUN)?;
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
    validate_restore_document(ontology, org_id, &doc)?;
    storage
        .upsert_batch(&doc.memories, &doc.relationships)
        .await
        .context("atomically restore org backup")?;
    Ok(ImportReport {
        memories: doc.memories.len(),
        relationships: doc.relationships.len(),
    })
}

fn validate_restore_document(ontology: &Ontology, org_id: &str, doc: &OrgBackup) -> Result<()> {
    let mut memories = std::collections::HashMap::<MemoryId, &Memory>::new();
    for (index, memory) in doc.memories.iter().enumerate() {
        anyhow::ensure!(
            memories.insert(memory.id, memory).is_none(),
            "memory {index}: duplicate id"
        );
        anyhow::ensure!(
            ontology
                .memory_type_names
                .get(memory.memory_type as usize)
                .is_some(),
            "memory {index}: unknown memory type {}",
            memory.memory_type
        );
        anyhow::ensure!(
            memory.context.tenant_id.as_deref() == Some(org_id),
            "memory {index}: tenant does not exactly match restore org"
        );
        anyhow::ensure!(
            !matches!(&memory.provenance, Provenance::Proposed { .. }),
            "memory {index}: proposed provenance may not persist"
        );
        match memory.visibility {
            Visibility::Private => anyhow::ensure!(
                memory
                    .context
                    .user_id
                    .as_ref()
                    .is_some_and(|id| !id.is_empty()),
                "memory {index}: private visibility requires user scope"
            ),
            Visibility::Project => anyhow::ensure!(
                memory
                    .context
                    .project_id
                    .as_ref()
                    .is_some_and(|id| !id.is_empty()),
                "memory {index}: project visibility requires project scope"
            ),
            Visibility::Team => anyhow::ensure!(
                memory
                    .context
                    .team_id
                    .as_ref()
                    .is_some_and(|id| !id.is_empty()),
                "memory {index}: team visibility requires team scope"
            ),
            Visibility::Org | Visibility::Public => {}
        }
    }

    let mut relationships = std::collections::HashSet::<RelationshipId>::new();
    for (index, relationship) in doc.relationships.iter().enumerate() {
        anyhow::ensure!(
            relationships.insert(relationship.id),
            "relationship {index}: duplicate id"
        );
        let from = memories
            .get(&relationship.from)
            .copied()
            .with_context(|| format!("relationship {index}: missing from endpoint"))?;
        let to = memories
            .get(&relationship.to)
            .copied()
            .with_context(|| format!("relationship {index}: missing to endpoint"))?;
        let metadata = ontology
            .kinds_by_id
            .get(&relationship.kind)
            .with_context(|| format!("relationship {index}: unknown relationship kind"))?;
        if ontology.triples_by_kind.contains_key(&relationship.kind) {
            validate_triple(
                ontology,
                from.memory_type,
                relationship.kind,
                to.memory_type,
            )
            .with_context(|| format!("relationship {index}: invalid ontology triple"))?;
        } else {
            // Inverse companions are materialized durable rows but the
            // catalogue stores type triples on their declared forward kind.
            let forward_kind = ontology
                .kinds_by_id
                .iter()
                .find_map(|(kind, candidate)| {
                    (candidate.inverse == Some(relationship.kind)).then_some(*kind)
                })
                .with_context(|| {
                    format!("relationship {index}: kind has no ontology triple or inverse")
                })?;
            validate_triple(ontology, to.memory_type, forward_kind, from.memory_type)
                .with_context(|| {
                    format!("relationship {index}: invalid inverse ontology triple")
                })?;
        }
        anyhow::ensure!(
            relationship
                .visibility
                .within(relationship_visibility(from.visibility, to.visibility)),
            "relationship {index}: visibility is wider than an endpoint"
        );
        anyhow::ensure!(
            !matches!(&relationship.provenance, Provenance::Proposed { .. }),
            "relationship {index}: proposed provenance may not persist"
        );
        anyhow::ensure!(
            !metadata.computed_only
                || matches!(&relationship.provenance, Provenance::Computed { .. }),
            "relationship {index}: computed-only kind has non-computed provenance"
        );
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::BACKUP_NOUN;
    use exocortex_storage::bounded_io::{
        atomic_write_private_with, ensure_size, read_bounded, serialize_json_pretty_bounded,
    };

    #[test]
    fn org_backup_size_boundary_is_inclusive_and_file_reads_are_bounded() {
        assert!(ensure_size(11, 11, BACKUP_NOUN).is_ok());
        assert!(ensure_size(12, 11, BACKUP_NOUN).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("org.json");
        std::fs::write(&path, b"12345678901").unwrap();
        assert_eq!(
            read_bounded(&path, 11, BACKUP_NOUN).unwrap(),
            b"12345678901"
        );
        assert!(read_bounded(&path, 10, BACKUP_NOUN).is_err());
        assert!(serialize_json_pretty_bounded(&"x", 3, BACKUP_NOUN).is_ok());
        assert!(serialize_json_pretty_bounded(&"x", 2, BACKUP_NOUN).is_err());
    }

    #[test]
    fn org_backup_atomic_write_preserves_previous_file_on_injected_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("org.json");
        std::fs::write(&path, b"previous").unwrap();
        atomic_write_private_with(&path, b"replacement", BACKUP_NOUN, |_| {
            Err(std::io::Error::other("injected before rename"))
        })
        .unwrap_err();
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
    }
}
