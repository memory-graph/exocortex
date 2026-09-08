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

/// One bounded unit plus the revision frontier it completes: the newest
/// commit sha whose rows AND edges have all landed by the end of this
/// unit (empty when the unit ends mid-commit).
pub struct BoundedUnit {
    /// The submission unit.
    pub unit: BatchUnit,
    /// Newest fully-emitted commit sha (monotonic across units; empty
    /// until the first commit completes).
    pub completes_through: String,
}

/// Accumulated slice group: each commit appears at most once (its
/// slices span consecutive groups), so rows = slices + distinct paths.
struct SliceGroup {
    slices: Vec<(usize, Vec<String>)>,
    paths: std::collections::BTreeSet<String>,
    edges: usize,
}

impl SliceGroup {
    fn rows(&self) -> usize {
        self.slices.len() + self.paths.len()
    }
}

fn close_group(
    groups: &mut Vec<SliceGroup>,
    current: &mut SliceGroup,
    frontiers: &mut Vec<String>,
    commits: &[GitCommit],
    pending: &std::collections::BTreeSet<usize>,
    seen: usize,
) {
    let frontier = (0..seen)
        .rev()
        .find(|idx| !pending.contains(idx))
        .map(|idx| commits[idx].sha.clone())
        .unwrap_or_default();
    frontiers.push(frontier);
    groups.push(std::mem::replace(
        current,
        SliceGroup {
            slices: Vec::new(),
            paths: Default::default(),
            edges: 0,
        },
    ));
}

/// Map parsed commits into units bounded by BOTH the declared row
/// ceiling and the protocol's per-batch edge ceiling. Shared paths weld
/// commits into one inseparable component, so a window bounded only by
/// rows can weld more edges than any batch may carry — the server
/// rejects an over-ceiling component permanently and the cursor
/// advances past the lost rows (R12). A single commit with more paths
/// than the edge ceiling is sliced across units: every slice repeats
/// the commit row (idempotent by external key, full changed-path
/// content), and the frontier advances past a commit only once its
/// last slice has landed.
pub fn map_history_bounded(
    repo_id: &str,
    commits: &[GitCommit],
    seed_prefix: &str,
    max_rows: usize,
    max_edges: usize,
) -> Vec<BoundedUnit> {
    let table = table_uuid_for(repo_id);
    let mut groups: Vec<SliceGroup> = Vec::new();
    let mut current = SliceGroup {
        slices: Vec::new(),
        paths: Default::default(),
        edges: 0,
    };
    // Commits whose slices have not all been placed yet.
    let mut pending: std::collections::BTreeSet<usize> = Default::default();
    let mut frontiers: Vec<String> = Vec::new();
    let mut seen = 0usize;
    for (idx, commit) in commits.iter().enumerate() {
        seen = idx + 1;
        pending.insert(idx);
        let mut remaining: Vec<String> = commit.files.clone();
        let mut slices_this_commit = 0usize;
        loop {
            if remaining.is_empty() {
                // Zero-file commits (merges under plain `--name-only`,
                // `--allow-empty`): the commit row still lands — a
                // dropped row with the frontier advancing past it is
                // permanent loss.
                if slices_this_commit == 0 {
                    current.slices.push((idx, Vec::new()));
                }
                break;
            }
            // A commit's slices never share a group: two slices of one
            // commit would emit the same draft_key twice and the SDK
            // rejects the whole unit (InvalidUnit) on every retry.
            if slices_this_commit > 0 && !current.slices.is_empty() {
                close_group(
                    &mut groups,
                    &mut current,
                    &mut frontiers,
                    commits,
                    &pending,
                    seen,
                );
            }
            let new_paths = remaining
                .iter()
                .filter(|path| !current.paths.contains(*path))
                .count();
            let fits = current.slices.is_empty()
                || (current.rows() + 1 + new_paths <= max_rows
                    && current.edges + remaining.len() <= max_edges);
            if !fits {
                close_group(
                    &mut groups,
                    &mut current,
                    &mut frontiers,
                    commits,
                    &pending,
                    seen,
                );
            }
            // This group may take paths bounded by both the remaining
            // edge room and the row room (files are rows too).
            let edge_room = max_edges.saturating_sub(current.edges).max(1);
            let row_room = max_rows
                .saturating_sub(current.rows() + 1)
                .max(1)
                .min(edge_room);
            let take = remaining.len().min(row_room);
            let slice: Vec<String> = remaining.drain(..take).collect();
            for path in &slice {
                current.paths.insert(path.clone());
            }
            current.edges += slice.len();
            current.slices.push((idx, slice));
            slices_this_commit += 1;
        }
        pending.remove(&idx);
    }
    if !current.slices.is_empty() {
        close_group(
            &mut groups,
            &mut current,
            &mut frontiers,
            commits,
            &pending,
            seen,
        );
    }

    groups
        .into_iter()
        .zip(frontiers)
        .enumerate()
        .map(|(index, (group, completes_through))| {
            let mut memories: Vec<MemoryDraft> = Vec::new();
            let mut relationships: Vec<RelationshipDraft> = Vec::new();
            for (commit_idx, slice) in &group.slices {
                let commit = &commits[*commit_idx];
                let commit_key = format!("commit-{}", commit.sha);
                memories.push(commit_memory(&table, commit, &commit_key));
                for path in slice {
                    let file_key = file_draft_key(&group.paths, path);
                    relationships.push(modifies_edge(&commit_key, &file_key, &commit.sha));
                }
            }
            for path in &group.paths {
                memories.push(file_memory(
                    &table,
                    repo_id,
                    path,
                    &file_draft_key(&group.paths, path),
                ));
            }
            let unit = BatchUnit {
                batch_id_seed: format!("{seed_prefix}-{index}"),
                memories,
                relationships,
                snapshot: Some(ExternalSnapshotInfo {
                    snapshot_id: group
                        .slices
                        .last()
                        .map(|(idx, _)| commits[*idx].sha.clone())
                        .unwrap_or_else(|| "empty".into()),
                    schema_hash: exocortex_wire::projection::schema_hash(&git_source_columns())
                        .to_vec(),
                    source_flavor: "custom".into(),
                }),
                observed_at: std::time::UNIX_EPOCH,
            };
            BoundedUnit {
                unit,
                completes_through,
            }
        })
        .collect()
}

/// Draft key for a path within one group's sorted path set.
fn file_draft_key(paths: &std::collections::BTreeSet<String>, path: &str) -> String {
    format!(
        "file-{}",
        paths.iter().position(|p| p == path).expect("path in set")
    )
}

fn commit_memory(table: &[u8; 16], commit: &GitCommit, draft_key: &str) -> MemoryDraft {
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
    MemoryDraft {
        rights: None,
        draft_key: draft_key.into(),
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
    }
}

fn file_memory(table: &[u8; 16], repo_id: &str, path: &str, draft_key: &str) -> MemoryDraft {
    MemoryDraft {
        rights: None,
        draft_key: draft_key.into(),
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
            logical_pk: path.into(),
            mapping_version: 1,
        }),
    }
}

fn modifies_edge(commit_key: &str, file_key: &str, sha: &str) -> RelationshipDraft {
    RelationshipDraft {
        from_draft_key: commit_key.into(),
        to_draft_key: file_key.into(),
        kind: "Modifies".into(),
        strength: 0.0,
        confidence: 0.9,
        context: format!("changed in {sha}"),
        visibility: 3,
        to_memory_id: String::new(),
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
        let a = map_history_bounded("repo-id", &commits, "seed", 256, 64);
        let b = map_history_bounded("repo-id", &commits, "seed", 256, 64);
        assert_eq!(a.len(), b.len());
        assert_eq!(a.len(), 1, "small history packs into one unit");
        let ua = &a[0].unit;
        let ub = &b[0].unit;
        assert_eq!(ua.memories.len(), ub.memories.len());
        assert_eq!(ua.relationships.len(), ub.relationships.len());
        // 2 commits + 3 distinct paths.
        assert_eq!(ua.memories.len(), 5);
        // 2 + 1 Modifies edges.
        assert_eq!(ua.relationships.len(), 3);
        assert!(ua.relationships.iter().all(|r| r.kind == "Modifies"));
        let fix = ua
            .memories
            .iter()
            .find(|m| m.memory_type == "Fix")
            .expect("fix: commit classifies as Fix");
        assert_eq!(fix.external_key.as_ref().unwrap().logical_pk, "aaaa");
        let file = ua
            .memories
            .iter()
            .find(|m| m.memory_type == "FileContext")
            .expect("file memories emitted");
        assert_eq!(file.title, "src/drain.rs");
        // Identity is repo-scoped: a different repo id forks the table.
        let other = map_history_bounded("other-repo", &commits, "seed", 256, 64);
        assert_ne!(
            other[0].unit.memories[0]
                .external_key
                .as_ref()
                .unwrap()
                .table_uuid,
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

    fn commit(sha: &str, files: &[&str]) -> GitCommit {
        GitCommit {
            sha: sha.into(),
            author_name: "A".into(),
            author_email: "a@example".into(),
            authored_at: "2026-09-01T00:00:00+00:00".into(),
            subject: "fix: thing".into(),
            body: String::new(),
            files: files.iter().map(|f| (*f).to_string()).collect(),
        }
    }

    #[test]
    fn bounded_units_respect_the_edge_ceiling() {
        // A merge commit with 70 distinct files: one welded component
        // with 70 Modifies edges. A window bounded only by rows would
        // pack it whole and the server would reject it PERMANENTLY with
        // the cursor advanced past the rows (R12). The bounded mapper
        // slices it; every slice repeats the commit row (full content),
        // and the union still carries every file row and edge.
        let files: Vec<String> = (0..70).map(|i| format!("src/f{i}.rs")).collect();
        let files_ref: Vec<&str> = files.iter().map(String::as_str).collect();
        let commits = vec![commit("aaaa", &files_ref)];
        let units = map_history_bounded("repo", &commits, "w", 256, 64);
        assert!(
            units.len() >= 2,
            "70 edges must slice: {} units",
            units.len()
        );
        for bounded in &units {
            assert!(
                bounded.unit.relationships.len() <= 64,
                "unit carries {} edges",
                bounded.unit.relationships.len()
            );
            let commit_row = bounded
                .unit
                .memories
                .iter()
                .find(|m| m.draft_key == "commit-aaaa")
                .expect("every slice repeats the commit row");
            assert_eq!(
                commit_row.content.matches("src/f").count(),
                70,
                "commit content always lists the FULL changed-path set"
            );
        }
        let all_files: std::collections::BTreeSet<&str> = units
            .iter()
            .flat_map(|b| b.unit.memories.iter())
            .filter_map(|m| m.external_key.as_ref().map(|k| k.logical_pk.as_str()))
            .filter(|pk| pk.starts_with("src/"))
            .collect();
        assert_eq!(all_files.len(), 70, "every file row present exactly once");
        let edges: usize = units.iter().map(|b| b.unit.relationships.len()).sum();
        assert_eq!(edges, 70);
    }

    #[test]
    fn bounded_frontier_advances_only_past_complete_commits() {
        // Commit 1 lands whole; commit 2 is wide enough to slice. The
        // unit that ends mid-commit-2 must not report a frontier past
        // commit 1 — the operator cursor may only advance past commits
        // whose rows AND edges have all settled.
        let files: Vec<String> = (0..70).map(|i| format!("p{i}")).collect();
        let files_ref: Vec<&str> = files.iter().map(String::as_str).collect();
        let commits = vec![commit("b1", &["one"]), commit("b2", &files_ref)];
        let units = map_history_bounded("repo", &commits, "w", 256, 64);
        assert!(units.len() >= 2);
        let mut last = "";
        for bounded in &units {
            if !bounded.completes_through.is_empty() {
                assert!(
                    bounded.completes_through.as_str() >= last,
                    "frontier is monotonic"
                );
                last = bounded.completes_through.as_str();
            }
        }
        assert_eq!(last, "b2", "the final unit completes both commits");
        // Some unit before the end ends mid-commit (frontier still b1 or
        // empty) — the slicing is real, not one unit per commit.
        assert!(
            units.iter().any(|b| b.completes_through.as_str() != "b2"),
            "at least one unit ends before commit b2 completes"
        );
    }

    #[test]
    fn zero_file_commits_still_emit_their_row() {
        // Merge commits list no paths under plain `--name-only` (and
        // `--allow-empty` commits list none either): their row must
        // land — a dropped row with the frontier advancing past it is
        // permanent loss.
        let commits = vec![
            commit("c1", &["a"]),
            commit("c2", &[]),
            commit("c3", &["b"]),
        ];
        let units = map_history_bounded("repo", &commits, "w", 256, 64);
        let keys: Vec<&str> = units
            .iter()
            .flat_map(|b| b.unit.memories.iter().map(|m| m.draft_key.as_str()))
            .collect();
        assert!(
            keys.contains(&"commit-c2"),
            "empty commit row emitted: {keys:?}"
        );
        assert_eq!(units.last().unwrap().completes_through, "c3");
    }

    #[test]
    fn one_commit_never_lands_twice_in_a_unit() {
        // Shared paths tempt the packer to continue a group mid-slice;
        // two slices of one commit in one group emit the same draft_key
        // twice and the SDK rejects the unit on every retry.
        let commits = vec![
            commit("s1", &["a", "b", "c"]),
            commit("s2", &["a", "b", "c"]),
        ];
        let units = map_history_bounded("repo", &commits, "w", 7, 64);
        assert!(!units.is_empty());
        for bounded in &units {
            let mut keys: Vec<&str> = bounded
                .unit
                .memories
                .iter()
                .map(|m| m.draft_key.as_str())
                .collect();
            keys.sort();
            let before = keys.len();
            keys.dedup();
            assert_eq!(
                before,
                keys.len(),
                "duplicate draft keys in one unit: {keys:?}"
            );
        }
        let edges: usize = units.iter().map(|b| b.unit.relationships.len()).sum();
        assert_eq!(edges, 6, "both commits keep all their edges");
    }

    #[test]
    fn bounded_units_share_files_across_commits_without_exceeding_edges() {
        // Two commits sharing paths weld into ONE component; the
        // combined edge count still respects the ceiling.
        let commits = vec![commit("c1", &["a", "b", "c"]), commit("c2", &["a", "d"])];
        let units = map_history_bounded("repo", &commits, "w", 256, 64);
        assert_eq!(units.len(), 1, "small commits pack together");
        assert_eq!(units[0].unit.relationships.len(), 5);
        assert_eq!(units[0].completes_through, "c2");
        assert_eq!(
            units[0].unit.memories.len(),
            6,
            "2 commits + 4 distinct files"
        );
    }
}
