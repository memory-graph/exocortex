//! D19 (master plan, SaaS-API adapter family): the GitHub adapter.
//!
//! Deterministic transcription of GitHub's GraphQL v4 API into the
//! dev-v1 ontology — direct API with an env-only PAT, never MCP. No
//! inference, no LLM: the one cross-type relationship below
//! (`closingIssuesReferences`) is one GitHub states structurally.
//!
//! - one memory per issue (bug-class labels -> `Problem`; everything
//!   else -> `Task`), identity-stable by `ExternalKey`
//!   (`logical_pk` = `issue:<number>`),
//! - one memory per pull request: `fix:`-prefixed titles -> `Fix`
//!   (the D18 classifier); a PR with closing references -> `Solution`;
//!   everything else -> `Command` (a change executed against the tree);
//! - the closing-reference edge: `Fixes` when a Fix PR closes a
//!   Problem, `Solves` when a Solution PR closes one, `RelatedTo`
//!   otherwise (type triples forbid solution kinds onto `Task`) —
//!   closing issues ride the window as their own rows so both edge
//!   endpoints are always in-batch (§18.1),
//! - closed-but-unmerged PRs close (`valid_until` = closedAt:
//!   abandonment retires the belief); merged PRs and closed issues
//!   stay open — completion is a true belief, the Linear adapter's
//!   same rule,
//! - body, author, labels, branches, and urls ride content so the
//!   server's entity extraction converges `Person`/`Url`/`File`
//!   entities with D18's git graph on shared names.
//!
//! Resume shape: issues page ascending under `filterBy: {since}` (the
//! inclusive cursor re-fetches ties, which replay idempotently); PRs
//! have no server-side since filter, so they page NEWEST-first and the
//! walk stops at the first page older than the cursor. Windows emit
//! oldest-first either way.

use exocortex_adapter_sdk::{
    BatchUnit, Projection, ProjectionBounds, ProjectionField, SourceColumn,
};
use exocortex_wire::ingest::v1::{
    ExternalKey, ExternalSnapshotInfo, MemoryDraft, RelationshipDraft,
};

/// The source columns this adapter's mapping was authored against —
/// the ONE list shared by the declared projection and the snapshot
/// schema hash (D21-d).
pub const GITHUB_SOURCE_COLUMNS: &[(&str, &str)] = &[
    ("issue_number", "int"),
    ("pull_number", "int"),
    ("title", "string"),
    ("body", "markdown"),
    ("state", "enum"),
    ("author_login", "string"),
    ("labels", "string[]"),
    ("head_branch", "string"),
    ("base_branch", "string"),
    ("url", "url"),
    ("updated_at", "rfc3339"),
    ("closed_at", "rfc3339?"),
    ("closing_refs", "int[]"),
];

/// The declared column set as owned `(String, String)` pairs — the
/// shape `exocortex_wire::projection::schema_hash` takes.
pub fn github_source_columns() -> Vec<(String, String)> {
    GITHUB_SOURCE_COLUMNS
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

/// The issues page: ascending by updatedAt under the inclusive `since`
/// filter, so the durable cursor re-fetches boundary ties and replays
/// them idempotently.
pub const ISSUES_QUERY: &str = r#"
query IssuesWindow($owner: String!, $repo: String!, $after: String, $since: DateTime, $first: Int!) {
  repository(owner: $owner, name: $repo) {
    issues(first: $first, after: $after, since: $since,
            orderBy: {field: UPDATED_AT, direction: ASC}) {
      nodes { number title body url updatedAt closedAt state
              author { login }
              labels(first: 20) { nodes { name } } }
      pageInfo { hasNextPage endCursor }
    }
  }
}"#;

/// The PRs page: NEWEST first (no server-side since filter exists),
/// walked until a page predates the cursor.
pub const PULLS_QUERY: &str = r#"
query PullsWindow($owner: String!, $repo: String!, $after: String, $first: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequests(first: $first, after: $after,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes { number title body url updatedAt closedAt state mergedAt
              author { login }
              headRefName baseRefName
              closingIssuesReferences(first: 20) {
                nodes { number title body url updatedAt closedAt state
                        author { login }
                        labels(first: 20) { nodes { name } } }
              } }
      pageInfo { hasNextPage endCursor }
    }
  }
}"#;

/// One parsed issue: exactly what GitHub stated, nothing deduced.
#[derive(Clone, Debug, PartialEq)]
pub struct GhIssue {
    /// Issue number (identity within the repo table).
    pub number: u64,
    /// Title.
    pub title: String,
    /// Body (markdown; bounded at map time).
    pub body: String,
    /// URL.
    pub url: String,
    /// updatedAt, RFC3339.
    pub updated_at: String,
    /// closedAt, RFC3339; empty while open.
    pub closed_at: String,
    /// `open` | `closed` (lowercased on parse).
    pub state: String,
    /// Author login; empty for ghost authors.
    pub author: String,
    /// Label names.
    pub labels: Vec<String>,
}

/// One parsed pull request with its structured closing references.
#[derive(Clone, Debug, PartialEq)]
pub struct GhPull {
    /// PR number (identity within the repo table).
    pub number: u64,
    /// Title.
    pub title: String,
    /// Body (markdown; bounded at map time).
    pub body: String,
    /// URL.
    pub url: String,
    /// updatedAt, RFC3339.
    pub updated_at: String,
    /// closedAt, RFC3339; empty while open.
    pub closed_at: String,
    /// `open` | `merged` | `closed` (lowercased on parse).
    pub state: String,
    /// Author login.
    pub author: String,
    /// Head branch.
    pub head_branch: String,
    /// Base branch.
    pub base_branch: String,
    /// Structured closing references (the one cross-type fact GitHub
    /// states; never parsed out of prose).
    pub closing: Vec<GhIssue>,
}

fn str_field(node: &serde_json::Value, key: &str) -> String {
    node.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn login_of(node: &serde_json::Value) -> String {
    node.get("author")
        .filter(|a| !a.is_null())
        .and_then(|a| a.get("login"))
        .and_then(|l| l.as_str())
        .unwrap_or_default()
        .to_string()
}

fn labels_of(node: &serde_json::Value) -> Vec<String> {
    node.get("labels")
        .and_then(|l| l.get("nodes"))
        .and_then(|n| n.as_array())
        .map(|nodes| {
            nodes
                .iter()
                .filter_map(|n| n.get("name").and_then(|v| v.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn parse_issue_node(node: &serde_json::Value) -> Option<GhIssue> {
    let number = node.get("number").and_then(|v| v.as_u64())?;
    Some(GhIssue {
        number,
        title: str_field(node, "title"),
        body: str_field(node, "body"),
        url: str_field(node, "url"),
        updated_at: str_field(node, "updatedAt"),
        closed_at: str_field(node, "closedAt"),
        state: str_field(node, "state").to_ascii_lowercase(),
        author: login_of(node),
        labels: labels_of(node),
    })
}

fn parse_pull_node(node: &serde_json::Value) -> Option<GhPull> {
    let number = node.get("number").and_then(|v| v.as_u64())?;
    let closing = node
        .get("closingIssuesReferences")
        .and_then(|c| c.get("nodes"))
        .and_then(|n| n.as_array())
        .map(|nodes| nodes.iter().filter_map(parse_issue_node).collect())
        .unwrap_or_default();
    Some(GhPull {
        number,
        title: str_field(node, "title"),
        body: str_field(node, "body"),
        url: str_field(node, "url"),
        updated_at: str_field(node, "updatedAt"),
        closed_at: str_field(node, "closedAt"),
        state: str_field(node, "state").to_ascii_lowercase(),
        author: login_of(node),
        head_branch: str_field(node, "headRefName"),
        base_branch: str_field(node, "baseRefName"),
        closing,
    })
}

/// Parse one issues page. Returns `(issues, skipped, has_next,
/// end_cursor)`; a node missing number or updatedAt is skipped and
/// counted, never guessed.
pub fn parse_issues_page(json: &serde_json::Value) -> (Vec<GhIssue>, usize, bool, String) {
    parse_connection(json, "issues", parse_issue_node, |issue| {
        !issue.updated_at.is_empty()
    })
}

/// Parse one PRs page. Same contract as [`parse_issues_page`].
pub fn parse_pulls_page(json: &serde_json::Value) -> (Vec<GhPull>, usize, bool, String) {
    parse_connection(json, "pullRequests", parse_pull_node, |pull| {
        !pull.updated_at.is_empty()
    })
}

fn parse_connection<T>(
    json: &serde_json::Value,
    field: &str,
    parse_node: fn(&serde_json::Value) -> Option<T>,
    valid: impl Fn(&T) -> bool,
) -> (Vec<T>, usize, bool, String) {
    let empty = Vec::new();
    let connection = json
        .get("repository")
        .and_then(|r| r.get(field))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let nodes = connection
        .get("nodes")
        .and_then(|n| n.as_array())
        .unwrap_or(&empty);
    let mut out = Vec::new();
    let mut skipped = 0usize;
    for node in nodes {
        match parse_node(node) {
            Some(value) if valid(&value) => out.push(value),
            _ => skipped += 1,
        }
    }
    let page_info = connection.get("pageInfo");
    let has_next = page_info
        .and_then(|p| p.get("hasNextPage"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let end_cursor = page_info
        .and_then(|p| p.get("endCursor"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    (out, skipped, has_next, end_cursor)
}

/// The issue classifier: bug-class label vocabulary -> `Problem`,
/// everything else -> `Task` (the Linear adapter's same rule).
pub fn issue_memory_type_for(labels: &[String]) -> &'static str {
    let bug_vocabulary = ["bug", "defect", "incident", "regression"];
    if labels.iter().any(|label| {
        let label = label.to_ascii_lowercase();
        bug_vocabulary.iter().any(|word| label.contains(word))
    }) {
        "Problem"
    } else {
        "Task"
    }
}

/// The PR classifier: `fix:` prefix -> `Fix` (the D18 commit
/// classifier); any closing reference -> `Solution`; else `Command`.
pub fn pull_memory_type_for(pull: &GhPull) -> &'static str {
    let title = pull.title.trim().to_ascii_lowercase();
    if title.starts_with("fix:") || title.starts_with("fix(") {
        "Fix"
    } else if !pull.closing.is_empty() {
        "Solution"
    } else {
        "Command"
    }
}

/// Derive the 16-byte table uuid for a repository: the first 16 bytes
/// of the blake3 digest over `github:<owner>/<repo>` (issues and PRs
/// share the table; the logical pk disambiguates).
pub fn table_uuid_for(owner: &str, repo: &str) -> [u8; 16] {
    let digest = blake3::hash(format!("github:{owner}/{repo}").as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

fn truncate_200(s: &str) -> String {
    s.chars().take(200).collect()
}

/// RFC3339 (what GitHub's API states) to the wire's protobuf
/// Timestamp. Unparseable input yields None (the row stays open)
/// rather than a guessed instant.
fn rfc3339_to_timestamp(s: &str) -> Option<prost_types::Timestamp> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        })
}

fn bound_4000(s: &str) -> String {
    match s.chars().count() {
        n if n <= 4000 => s.to_string(),
        _ => {
            let cut: String = s.chars().take(4000).collect();
            format!("{cut}\n\n[body bounded at 4000 chars by the adapter]")
        }
    }
}

fn issue_content(prefix: &str, issue: &GhIssue) -> String {
    let mut content = format!(
        "{prefix}#{}: {}\n\nState: {}\nAuthor: {}\nUpdated: {}\nURL: {}\n",
        issue.number,
        issue.title,
        issue.state,
        if issue.author.is_empty() {
            "unknown"
        } else {
            &issue.author
        },
        issue.updated_at,
        issue.url,
    );
    if !issue.labels.is_empty() {
        content.push_str("Labels: ");
        content.push_str(&issue.labels.join(", "));
        content.push('\n');
    }
    if !issue.body.trim().is_empty() {
        content.push('\n');
        content.push_str(&bound_4000(&issue.body));
        content.push('\n');
    }
    content
}

/// Map one window (issues + PRs, already deduped: closing references
/// not among `issues` are appended as issue rows by the caller) to a
/// submission unit. Deterministic for a given input.
pub fn map_window(
    owner: &str,
    repo: &str,
    issues: &[GhIssue],
    pulls: &[GhPull],
    batch_id_seed: &str,
) -> BatchUnit {
    let table = table_uuid_for(owner, repo);
    let mut memories: Vec<MemoryDraft> = Vec::new();
    let mut relationships: Vec<RelationshipDraft> = Vec::new();

    // Issues first, in given (oldest-first) order.
    for issue in issues {
        memories.push(MemoryDraft {
            rights: None,
            draft_key: format!("issue-{}", issue.number),
            id: String::new(),
            memory_type: issue_memory_type_for(&issue.labels).into(),
            title: truncate_200(&issue.title),
            content: issue_content("Issue", issue),
            tags: vec!["github".into(), "issue".into()],
            visibility: 3,
            // A closed issue is a resolved problem (a true belief), so
            // it stays open; abandonment has no issue-side state here.
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: format!("issue:{}", issue.number),
                mapping_version: 1,
            }),
        });
    }

    for pull in pulls {
        let pull_key = format!("pull-{}", pull.number);
        let mut content = format!(
            "PR #{}: {}\n\nState: {}\nAuthor: {}\nBranches: {} -> {}\nUpdated: {}\nURL: {}\n",
            pull.number,
            pull.title,
            pull.state,
            if pull.author.is_empty() {
                "unknown"
            } else {
                &pull.author
            },
            pull.head_branch,
            pull.base_branch,
            pull.updated_at,
            pull.url,
        );
        if !pull.closing.is_empty() {
            content.push_str("Closes: ");
            content.push_str(
                &pull
                    .closing
                    .iter()
                    .map(|issue| format!("#{}", issue.number))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            content.push('\n');
        }
        if !pull.body.trim().is_empty() {
            content.push('\n');
            content.push_str(&bound_4000(&pull.body));
            content.push('\n');
        }
        memories.push(MemoryDraft {
            rights: None,
            draft_key: pull_key.clone(),
            id: String::new(),
            memory_type: pull_memory_type_for(pull).into(),
            title: truncate_200(&pull.title),
            content,
            tags: vec!["github".into(), "pull-request".into()],
            visibility: 3,
            valid_from: None,
            // A closed-but-unmerged PR is an abandoned change: retired.
            // Merged PRs stay open (the change happened).
            valid_until: if pull.state == "closed" && !pull.closed_at.is_empty() {
                rfc3339_to_timestamp(&pull.closed_at)
            } else {
                None
            },
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: format!("pull:{}", pull.number),
                mapping_version: 1,
            }),
        });

        for issue in &pull.closing {
            let issue_key = format!("issue-{}", issue.number);
            let issue_type = issue_memory_type_for(&issue.labels);
            let kind = match (pull_memory_type_for(pull), issue_type) {
                ("Fix", "Problem") => "Fixes",
                ("Solution", "Problem") => "Solves",
                _ => "RelatedTo",
            };
            relationships.push(RelationshipDraft {
                from_draft_key: pull_key.clone(),
                to_draft_key: issue_key,
                kind: kind.into(),
                strength: 0.0,
                confidence: 0.9,
                context: format!("github closingIssuesReferences ({owner}/{repo})"),
                visibility: 3,
                to_memory_id: String::new(),
            });
        }
    }

    let snapshot_id = issues
        .iter()
        .map(|i| i.updated_at.as_str())
        .chain(pulls.iter().map(|p| p.updated_at.as_str()))
        .max()
        .unwrap_or("empty")
        .to_string();

    BatchUnit {
        batch_id_seed: batch_id_seed.into(),
        memories,
        relationships,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id,
            schema_hash: exocortex_wire::projection::schema_hash(&github_source_columns()).to_vec(),
            source_flavor: "github".into(),
        }),
        observed_at: std::time::UNIX_EPOCH,
    }
}

/// The window cursor: max updatedAt across the window's rows.
pub fn cursor_for(issues: &[GhIssue], pulls: &[GhPull]) -> Option<String> {
    issues
        .iter()
        .map(|i| i.updated_at.as_str())
        .chain(pulls.iter().map(|p| p.updated_at.as_str()))
        .max()
        .map(str::to_string)
}

/// The D21-a projection this adapter declares.
pub fn projection(max_window: u64) -> Projection {
    Projection {
        selector: "issues since cursor (asc) + pulls newer than cursor (desc walk)".into(),
        fields: vec![
            ProjectionField {
                source_field: "issue_number".into(),
                memory_type: "Task".into(),
                kind: String::new(),
            },
            ProjectionField {
                source_field: "pull_number".into(),
                memory_type: "Command".into(),
                kind: String::new(),
            },
            ProjectionField {
                source_field: "closing_refs".into(),
                memory_type: String::new(),
                kind: "Fixes".into(),
            },
        ],
        source_schema: GITHUB_SOURCE_COLUMNS
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

    fn issues_page() -> serde_json::Value {
        serde_json::json!({
            "repository": { "issues": {
                "nodes": [
                    {
                        "number": 101, "title": "Badge shows stale count",
                        "body": "caches forever", "url": "https://github.com/acme/api/issues/101",
                        "updatedAt": "2026-09-01T10:00:00Z", "closedAt": "", "state": "OPEN",
                        "author": { "login": "jorge" },
                        "labels": { "nodes": [ { "name": "Bug" }, { "name": "backend" } ] }
                    },
                    {
                        "number": 102, "title": "Reader for failover",
                        "body": "", "url": "https://github.com/acme/api/issues/102",
                        "updatedAt": "2026-09-02T11:00:00Z", "closedAt": "", "state": "OPEN",
                        "author": { "login": "john" },
                        "labels": { "nodes": [ { "name": "Improvement" } ] }
                    },
                    { "title": "no number node" }
                ],
                "pageInfo": { "hasNextPage": false, "endCursor": "c2" }
            }}
        })
    }

    fn pulls_page() -> serde_json::Value {
        serde_json::json!({
            "repository": { "pullRequests": {
                "nodes": [
                    {
                        "number": 55, "title": "fix: repair badge cache",
                        "body": "", "url": "https://github.com/acme/api/pull/55",
                        "updatedAt": "2026-09-03T10:00:00Z", "closedAt": "", "state": "OPEN",
                        "mergedAt": null, "author": { "login": "greg" },
                        "headRefName": "greg/fix-badge", "baseRefName": "main",
                        "closingIssuesReferences": { "nodes": [
                            {
                                "number": 101, "title": "Badge shows stale count",
                                "body": "", "url": "u101",
                                "updatedAt": "2026-09-01T10:00:00Z", "closedAt": "", "state": "OPEN",
                                "author": { "login": "jorge" },
                                "labels": { "nodes": [ { "name": "Bug" } ] }
                            },
                            {
                                "number": 103, "title": "Also stale somewhere else",
                                "body": "", "url": "u103",
                                "updatedAt": "2026-09-01T09:00:00Z", "closedAt": "", "state": "OPEN",
                                "author": null,
                                "labels": { "nodes": [ { "name": "Bug" } ] }
                            }
                        ]}
                    },
                    {
                        "number": 56, "title": "chore: bump deps",
                        "body": "", "url": "u56",
                        "updatedAt": "2026-09-02T08:00:00Z", "closedAt": "2026-09-02T09:00:00Z",
                        "state": "CLOSED", "mergedAt": null, "author": { "login": "greg" },
                        "headRefName": "chore/deps", "baseRefName": "main",
                        "closingIssuesReferences": { "nodes": [] }
                    }
                ],
                "pageInfo": { "hasNextPage": false, "endCursor": "p2" }
            }}
        })
    }

    #[test]
    fn parsers_read_nodes_and_skip_malformed_loudly() {
        let (issues, skipped, has_next, cursor) = parse_issues_page(&issues_page());
        assert_eq!((issues.len(), skipped), (2, 1));
        assert!(!has_next);
        assert_eq!(cursor, "c2");
        assert_eq!(issues[0].labels, vec!["Bug", "backend"]);
        let (pulls, skipped, has_next, cursor) = parse_pulls_page(&pulls_page());
        assert_eq!((pulls.len(), skipped), (2, 0));
        assert!(has_next || cursor == "p2");
        assert_eq!(pulls[0].closing.len(), 2);
        assert_eq!(pulls[0].closing[1].author, "", "ghost authors stay empty");
    }

    #[test]
    fn classifiers_are_prefix_and_label_tests() {
        assert_eq!(issue_memory_type_for(&["Bug".into()]), "Problem");
        assert_eq!(issue_memory_type_for(&["Improvement".into()]), "Task");
        assert_eq!(
            pull_memory_type_for(&GhPull {
                number: 1,
                title: "fix: x".into(),
                body: String::new(),
                url: String::new(),
                updated_at: String::new(),
                closed_at: String::new(),
                state: String::new(),
                author: String::new(),
                head_branch: String::new(),
                base_branch: String::new(),
                closing: vec![],
            }),
            "Fix"
        );
        let (pulls, _, _, _) = parse_pulls_page(&pulls_page());
        assert_eq!(pull_memory_type_for(&pulls[0]), "Fix");
        assert_eq!(
            pull_memory_type_for(&pulls[1]),
            "Command",
            "no fix prefix, no closing refs"
        );
        let solution = GhPull {
            closing: vec![GhIssue {
                number: 9,
                title: String::new(),
                body: String::new(),
                url: String::new(),
                updated_at: String::new(),
                closed_at: String::new(),
                state: String::new(),
                author: String::new(),
                labels: vec![],
            }],
            ..pulls[1].clone()
        };
        assert_eq!(pull_memory_type_for(&solution), "Solution");
    }

    #[test]
    fn mapping_chooses_type_valid_kinds_and_closes_abandoned_prs() {
        let (issues, _, _, _) = parse_issues_page(&issues_page());
        let (pulls, _, _, _) = parse_pulls_page(&pulls_page());
        // Closing ref 103 rides the window as its own row (the caller's
        // dedupe rule, exercised here by hand).
        let mut window_issues = issues.clone();
        window_issues.push(pulls[0].closing[1].clone());
        let unit = map_window("acme", "api", &window_issues, &pulls, "seed");
        // 3 issues + 2 PRs.
        assert_eq!(unit.memories.len(), 5);
        // PR 55 (Fix) closes 101 (Problem) -> Fixes and 103 (Problem) -> Fixes.
        let fixes: Vec<_> = unit
            .relationships
            .iter()
            .filter(|r| r.kind == "Fixes")
            .collect();
        assert_eq!(fixes.len(), 2, "both closing refs carry type-valid kinds");
        assert!(fixes.iter().all(|r| r.from_draft_key == "pull-55"));
        // Issue 101 is a Problem by its Bug label; identity is repo-scoped.
        let issue101 = unit
            .memories
            .iter()
            .find(|m| m.draft_key == "issue-101")
            .unwrap();
        assert_eq!(issue101.memory_type, "Problem");
        assert_eq!(
            issue101.external_key.as_ref().unwrap().table_uuid,
            table_uuid_for("acme", "api").to_vec()
        );
        assert_eq!(
            issue101.external_key.as_ref().unwrap().logical_pk,
            "issue:101"
        );
        // Closed-unmerged PR retires; merged/open do not.
        let pr56 = unit
            .memories
            .iter()
            .find(|m| m.draft_key == "pull-56")
            .unwrap();
        assert_eq!(pr56.memory_type, "Command");
        assert_eq!(
            pr56.valid_until,
            rfc3339_to_timestamp("2026-09-02T09:00:00Z")
        );
        let pr55 = unit
            .memories
            .iter()
            .find(|m| m.draft_key == "pull-55")
            .unwrap();
        assert_eq!(pr55.valid_until, None);
        assert!(pr55.content.contains("Closes: #101, #103"));
        // Cursor is the window max.
        assert_eq!(
            cursor_for(&window_issues, &pulls).as_deref(),
            Some("2026-09-03T10:00:00Z")
        );
    }

    #[test]
    fn solution_prs_use_solves_and_tasks_fall_back_to_relatedto() {
        let task_issue = GhIssue {
            number: 200,
            title: "Add docs".into(),
            body: String::new(),
            url: String::new(),
            updated_at: "2026-09-01T00:00:00Z".into(),
            closed_at: String::new(),
            state: "open".into(),
            author: String::new(),
            labels: vec![],
        };
        let solution_pull = GhPull {
            number: 90,
            title: "Add the docs page".into(),
            body: String::new(),
            url: String::new(),
            updated_at: "2026-09-02T00:00:00Z".into(),
            closed_at: String::new(),
            state: "open".into(),
            author: String::new(),
            head_branch: "docs".into(),
            base_branch: "main".into(),
            closing: vec![task_issue.clone()],
        };
        let unit = map_window(
            "acme",
            "api",
            &[task_issue.clone()],
            &[solution_pull.clone()],
            "seed",
        );
        assert_eq!(unit.relationships.len(), 1);
        assert_eq!(
            unit.relationships[0].kind, "RelatedTo",
            "Task targets take RelatedTo"
        );
        let problem_issue = GhIssue {
            labels: vec!["Bug".into()],
            ..task_issue.clone()
        };
        let fix_pull = GhPull {
            title: "fix: docs bug".into(),
            closing: vec![problem_issue.clone()],
            ..solution_pull.clone()
        };
        let unit = map_window("acme", "api", &[problem_issue], &[fix_pull], "seed");
        assert_eq!(unit.relationships[0].kind, "Fixes");
    }

    #[test]
    fn mapping_is_deterministic() {
        let (issues, _, _, _) = parse_issues_page(&issues_page());
        let (pulls, _, _, _) = parse_pulls_page(&pulls_page());
        let a = map_window("acme", "api", &issues, &pulls, "seed");
        let b = map_window("acme", "api", &issues, &pulls, "seed");
        assert_eq!(a.memories, b.memories);
        assert_eq!(a.relationships, b.relationships);
    }

    #[test]
    fn projection_declares_the_window_contract() {
        let projection = projection(128);
        assert_eq!(projection.bounds.max_rows_per_window, 128);
        assert_eq!(projection.source_schema.len(), GITHUB_SOURCE_COLUMNS.len());
    }
}
