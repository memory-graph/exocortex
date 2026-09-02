//! D18 (master plan, adapter roadmap): the git-history adapter.
//!
//! Deterministic transcription of `git log` into the dev-v1 ontology —
//! no auth, no network beyond the backend, no inference, no LLM:
//!
//! - one memory per commit (`fix:` subjects become `Fix`; every other
//!   subject becomes `Command` — the classifier is a prefix test),
//! - one `FileContext` memory per changed path (identity-stable across
//!   runs by `ExternalKey`),
//! - a `Modifies` edge from each commit to every path it changed
//!   (the one relationship git states factually; `fixes #123` and
//!   PR joins belong to the SaaS adapters, D19, not to text guessing),
//! - commit content carries author, date, and the changed-path list, so
//!   the server's own entity extraction converges `File`/`Person`
//!   entities exactly as it does for session wrapups.
//!
//! Re-runs are idempotent by construction: commit and file identities
//! are external keys (`table_uuid` derived from the repo id, `logical_pk`
//! the sha / the path), so the same history maps onto the same rows.

use exocortex_adapter_sdk::{
    BatchUnit, Projection, ProjectionBounds, ProjectionField, SourceColumn,
};
use exocortex_wire::ingest::v1::{
    ExternalKey, ExternalSnapshotInfo, MemoryDraft, RelationshipDraft,
};

/// The source columns this adapter's mapping was authored against —
/// the ONE list shared by the declared projection and the snapshot
/// schema hash, so the observed hash can never drift from the
/// declared one (D21-d; §18.6 pins the width at 32 bytes).
pub const GIT_SOURCE_COLUMNS: &[(&str, &str)] = &[
    ("commit_sha", "sha-hex"),
    ("subject", "string"),
    ("author", "string"),
    ("changed_path", "path"),
];

/// The declared column set as owned `(String, String)` pairs — the
/// shape `exocortex_wire::projection::schema_hash` takes.
pub fn git_source_columns() -> Vec<(String, String)> {
    GIT_SOURCE_COLUMNS
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

/// The `git log --format` this adapter runs: a RECORD separator BEFORE
/// each record (so each record owns its `--name-only` lines), unit
/// separators between fields.
pub const GIT_LOG_FORMAT: &str = "\u{1e}%H\u{1f}%an\u{1f}%ae\u{1f}%aI\u{1f}%s\u{1f}%b";

/// One parsed commit: exactly what `git log` stated, nothing deduced.
#[derive(Clone, Debug, PartialEq)]
pub struct GitCommit {
    /// Full commit sha (identity).
    pub sha: String,
    /// Author name.
    pub author_name: String,
    /// Author email.
    pub author_email: String,
    /// Author timestamp, ISO-8601.
    pub authored_at: String,
    /// Subject line.
    pub subject: String,
    /// Body (may be empty).
    pub body: String,
    /// Changed paths, in git's order.
    pub files: Vec<String>,
}

/// Parse `git log --format=<GIT_LOG_FORMAT> --name-only` output:
/// `\x1e`-separated records whose first line carries six
/// `\x1f`-separated fields and whose remaining lines are the changed
/// paths. Deterministic; malformed records are skipped and counted,
/// never guessed.
pub fn parse_git_log(output: &str) -> (Vec<GitCommit>, usize) {
    let mut commits = Vec::new();
    let mut skipped = 0usize;
    for record in output.split('\u{1e}') {
        let record = record.trim_start_matches('\n');
        if record.trim().is_empty() {
            continue;
        }
        let mut lines = record.splitn(2, '\n');
        let header = lines.next().unwrap_or_default();
        let rest = lines.next().unwrap_or_default();
        let fields: Vec<&str> = header.split('\u{1f}').collect();
        if fields.len() != 6 || fields[0].is_empty() {
            skipped += 1;
            continue;
        }
        let files = rest
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        commits.push(GitCommit {
            sha: fields[0].to_string(),
            author_name: fields[1].to_string(),
            author_email: fields[2].to_string(),
            authored_at: fields[3].to_string(),
            subject: fields[4].to_string(),
            body: fields[5].trim_end().to_string(),
            files,
        });
    }
    (commits, skipped)
}

/// The commit classifier: a prefix test, nothing more. `fix:` commits
/// are `Fix`; everything else is `Command` (a change executed against
/// the tree). Both types may `Modifies` a `FileContext` (R-T17).
pub fn memory_type_for(subject: &str) -> &'static str {
    let subject = subject.trim().to_ascii_lowercase();
    if subject.starts_with("fix:") || subject.starts_with("fix(") {
        "Fix"
    } else {
        "Command"
    }
}

/// Derive the 16-byte table uuid for a repo's commit/file tables: the
/// first 16 bytes of the blake3 digest over the repo id (the configured
/// remote or path — whatever identity the operator pinned).
pub fn table_uuid_for(repo_id: &str) -> [u8; 16] {
    let digest = blake3::hash(repo_id.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

fn truncate_200(s: &str) -> String {
    s.chars().take(200).collect()
}

/// Map parsed commits to one submission unit: commit memories, file
/// memories for every changed path, and `Modifies` edges from each
/// commit to its paths. Deterministic for a given input.
pub fn map_history(repo_id: &str, commits: &[GitCommit], batch_id_seed: &str) -> BatchUnit {
    let table = table_uuid_for(repo_id);
    // Stable draft keys: one per path (sorted), one per commit.
    let mut paths: std::collections::BTreeSet<String> = Default::default();
    for commit in commits {
        paths.extend(commit.files.iter().cloned());
    }
    let file_key: std::collections::HashMap<String, String> = paths
        .into_iter()
        .enumerate()
        .map(|(index, path)| (path, format!("file-{index}")))
        .collect();
    // File memories emit in sorted-path order so the unit is
    // order-deterministic (HashMap iteration is not).
    let sorted_paths: Vec<&String> = {
        let mut names: Vec<&String> = file_key.keys().collect();
        names.sort();
        names
    };

    let mut memories: Vec<MemoryDraft> = Vec::new();
    let mut relationships: Vec<RelationshipDraft> = Vec::new();

    for commit in commits {
        let commit_key = format!("commit-{}", commit.sha);
        let mut content = format!(
            "{}\n\nAuthor: {} <{}>\nDate: {}\nCommit: {}\n",
            commit.subject, commit.author_name, commit.author_email, commit.authored_at, commit.sha
        );
        if !commit.body.is_empty() {
            content.push('\n');
            content.push_str(&commit.body);
            content.push('\n');
        }
        content.push_str("\nChanged paths:");
        for path in &commit.files {
            content.push_str("\n- ");
            content.push_str(path);
        }
        memories.push(MemoryDraft {
            draft_key: commit_key.clone(),
            id: String::new(),
            memory_type: memory_type_for(&commit.subject).into(),
            title: truncate_200(&commit.subject),
            content,
            tags: vec!["git".into(), "commit".into()],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: commit.sha.clone(),
                mapping_version: 1,
            }),
        });
        for path in &commit.files {
            relationships.push(RelationshipDraft {
                from_draft_key: commit_key.clone(),
                to_draft_key: file_key[path].clone(),
                kind: "Modifies".into(),
                strength: 0.0,
                confidence: 0.9,
                context: format!("changed in {}", commit.sha),
                visibility: 3,
                to_memory_id: String::new(),
            });
        }
    }

    for path in sorted_paths {
        let key = &file_key[path];
        memories.push(MemoryDraft {
            draft_key: key.clone(),
            id: String::new(),
            memory_type: "FileContext".into(),
            title: truncate_200(path),
            content: format!("Repository path {path} (repo {repo_id})."),
            tags: vec!["git".into(), "file".into()],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: (*path).clone(),
                mapping_version: 1,
            }),
        });
    }

    BatchUnit {
        batch_id_seed: batch_id_seed.into(),
        memories,
        relationships,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: commits
                .last()
                .map(|commit| commit.sha.clone())
                .unwrap_or_else(|| "empty".into()),
            // The canonical 32-byte digest over the declared column set
            // — the same value the server derives from the registration
            // (a 16-byte table uuid here was rejected by every real
            // backend; the mock now enforces the width).
            schema_hash: exocortex_wire::projection::schema_hash(&git_source_columns()).to_vec(),
            source_flavor: "custom".into(),
        }),
        observed_at: std::time::UNIX_EPOCH,
    }
}

/// The D21-a projection this adapter declares: the selector is the
/// revision range, the mapping is sha -> Fix/Command and path ->
/// FileContext, and the bounds cap the window. The flavor is `custom`
/// (exempt in v1), but the contract is declared anyway — good citizenship
/// is the migration test for every other adapter.
pub fn projection(max_window: u64) -> Projection {
    Projection {
        selector: "refs/HEAD: git log <cursor>..HEAD --reverse".into(),
        fields: vec![
            ProjectionField {
                source_field: "commit_sha".into(),
                memory_type: "Fix".into(),
                kind: String::new(),
            },
            ProjectionField {
                source_field: "changed_path".into(),
                memory_type: "FileContext".into(),
                kind: "Modifies".into(),
            },
        ],
        source_schema: GIT_SOURCE_COLUMNS
            .iter()
            .map(|(name, data_type)| SourceColumn {
                name: (*name).into(),
                data_type: (*data_type).into(),
            })
            .collect(),
        mapping_version: 1,
        bounds: ProjectionBounds {
            max_rows_per_window: max_window,
            max_rows_per_run: max_window.saturating_mul(100),
            max_graph_share_percent: 50,
        },
        last_snapshot_id: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_text() -> String {
        [
            concat!(
                "\u{1e}aaaa\u{1f}Greg Dickson\u{1f}greg@example\u{1f}2026-08-30T10:00:00+00:00\u{1f}",
                "fix: repair WAL drain on restart\u{1f}The drain lost entries when the map was cleared early."
            ),
            "\nsrc/drain.rs\nsrc/wal.rs",
            concat!(
                "\u{1e}bbbb\u{1f}Greg Dickson\u{1f}greg@example\u{1f}2026-08-30T11:00:00+00:00\u{1f}",
                "feat: add projection bounds\u{1f}"
            ),
            "\nsrc/lib.rs",
        ]
        .concat()
    }

    #[test]
    fn parser_reads_records_and_files() {
        let (commits, skipped) = parse_git_log(&log_text());
        assert_eq!(skipped, 0);
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].sha, "aaaa");
        assert_eq!(commits[0].files, vec!["src/drain.rs", "src/wal.rs"]);
        assert_eq!(commits[1].subject, "feat: add projection bounds");
        assert!(commits[1].body.is_empty());
        assert_eq!(commits[1].files, vec!["src/lib.rs"]);
    }

    #[test]
    fn classifier_is_a_prefix_test() {
        assert_eq!(memory_type_for("fix: x"), "Fix");
        assert_eq!(memory_type_for("fix(core): x"), "Fix");
        assert_eq!(memory_type_for("feat: x"), "Command");
        assert_eq!(memory_type_for("Merge branch"), "Command");
    }

    #[test]
    fn mapping_is_deterministic_with_stable_identities() {
        let (commits, _) = parse_git_log(&log_text());
        let a = map_history("repo-id", &commits, "seed");
        let b = map_history("repo-id", &commits, "seed");
        assert_eq!(a.memories.len(), b.memories.len());
        assert_eq!(a.relationships.len(), b.relationships.len());
        // 2 commits + 3 distinct paths.
        assert_eq!(a.memories.len(), 5);
        // 2 + 1 Modifies edges.
        assert_eq!(a.relationships.len(), 3);
        assert!(a.relationships.iter().all(|r| r.kind == "Modifies"));
        let fix = a
            .memories
            .iter()
            .find(|m| m.memory_type == "Fix")
            .expect("fix: commit classifies as Fix");
        assert_eq!(fix.external_key.as_ref().unwrap().logical_pk, "aaaa");
        let file = a
            .memories
            .iter()
            .find(|m| m.memory_type == "FileContext")
            .expect("file memories emitted");
        assert_eq!(file.title, "src/drain.rs");
        // Identity is repo-scoped: a different repo id forks the table.
        let other = map_history("other-repo", &commits, "seed");
        assert_ne!(
            other.memories[0].external_key.as_ref().unwrap().table_uuid,
            fix.external_key.as_ref().unwrap().table_uuid
        );
        // Commit content names the paths so entity extraction converges.
        assert!(fix.content.contains("src/drain.rs"));
        assert!(fix.content.contains("Greg Dickson"));
    }

    #[test]
    fn parser_skips_malformed_records_loudly() {
        let (commits, skipped) = parse_git_log("garbage\nmore garbage\u{1e}");
        assert_eq!((commits.len(), skipped), (0, 1));
    }
}
