//! D19 (master plan, SaaS-API adapter family): the Linear adapter.
//!
//! Deterministic transcription of Linear's GraphQL API into the dev-v1
//! ontology — direct API with an env-only key, never MCP (no stable
//! pagination cursors, no rate-limit visibility, a model-facing
//! contract). No inference, no LLM: every relationship below is one
//! Linear states structurally, transcribed rather than inferred.
//!
//! - one memory per issue (`Bug`-class labels -> `Problem`; everything
//!   else -> `Task` — a work item on the board), identity-stable across
//!   runs by `ExternalKey` (`logical_pk` = the issue uuid),
//! - one `Project` memory per referenced project, `InProject` edges
//!   from each issue to it,
//! - `Blocks` from Linear `blocks`/`blocked_by` relations,
//!   `RelatedTo` from `related_to`/`duplicate`,
//!   `Contains` from parent -> sub-issue — only when BOTH endpoints are
//!   in the window (§18.1 forbids cross-batch draft references); a
//!   relation to an issue outside the window waits for that issue's
//!   next update (recorded boundary, not silent loss),
//! - canceled issues close (`valid_until` = canceledAt); completed
//!   issues stay open — completion is a true belief, cancellation
//!   retires one,
//! - description, state, team, assignee, creator, labels, branch, url,
//!   and attachment urls ride content so the server's own entity
//!   extraction converges `Person`/`Url`/`Concept` entities exactly as
//!   it does for session wrapups (the D18 precedent).
//!
//! Re-runs are idempotent by construction: the resume filter is
//! `updatedAt >= cursor` (inclusive), so boundary ties re-fetch and
//! land as idempotent replays, never as duplicate rows.

use exocortex_adapter_sdk::{
    BatchUnit, Projection, ProjectionBounds, ProjectionField, SourceColumn,
};
use exocortex_wire::ingest::v1::{
    ExternalKey, ExternalSnapshotInfo, MemoryDraft, RelationshipDraft,
};

/// The source columns this adapter's mapping was authored against —
/// the ONE list shared by the declared projection and the snapshot
/// schema hash (D21-d; §18.6 pins the hash width at 32 bytes).
pub const LINEAR_SOURCE_COLUMNS: &[(&str, &str)] = &[
    ("issue_uuid", "uuid"),
    ("identifier", "string"),
    ("title", "string"),
    ("description", "markdown"),
    ("state_name", "string"),
    ("state_type", "string"),
    ("team_key", "string"),
    ("project_name", "string"),
    ("assignee_name", "string"),
    ("labels", "string[]"),
    ("branch", "string"),
    ("url", "url"),
    ("updated_at", "rfc3339"),
    ("canceled_at", "rfc3339?"),
    ("relations", "enum[]"),
    ("parent_uuid", "uuid?"),
];

/// The declared column set as owned `(String, String)` pairs — the
/// shape `exocortex_wire::projection::schema_hash` takes.
pub fn linear_source_columns() -> Vec<(String, String)> {
    LINEAR_SOURCE_COLUMNS
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

/// The issues window query: one page, ordered by updatedAt ascending,
/// resumable at `gte` (the durable cursor, inclusive on purpose —
/// boundary ties re-fetch and replay idempotently). Relations and the
/// parent carry only the identifiers the window needs; endpoint rows
/// come from the window itself, never a second lookup.
pub const ISSUES_QUERY: &str = r#"
query IssuesWindow($after: String, $gte: DateTime, $first: Int) {
  issues(after: $after, first: $first, orderBy: updatedAt,
         filter: {updatedAt: {gte: $gte}}) {
    nodes {
      id identifier title description updatedAt canceledAt url branchName
      state { name type }
      team { key name }
      assignee { name }
      project { id name }
      parent { id }
      labels { nodes { name } }
      relations { nodes { type issue { id } } }
      attachments { nodes { title url } }
    }
    pageInfo { hasNextPage endCursor }
  }
}"#;

/// One parsed issue: exactly what Linear stated, nothing deduced.
/// Required fields (id, updatedAt) are non-empty; a node missing one
/// is skipped and counted, never guessed.
#[derive(Clone, Debug, PartialEq)]
pub struct LinearIssue {
    /// Issue uuid (identity).
    pub id: String,
    /// Human identifier, e.g. `LOA-2580`.
    pub identifier: String,
    /// Title.
    pub title: String,
    /// Description (markdown; bounded at map time).
    pub description: String,
    /// updatedAt, RFC3339.
    pub updated_at: String,
    /// canceledAt, RFC3339; empty while open.
    pub canceled_at: String,
    /// URL.
    pub url: String,
    /// Branch name, when Linear's git integration knows it.
    pub branch: String,
    /// State display name.
    pub state_name: String,
    /// State type (`backlog`/`unstarted`/`started`/`completed`/`canceled`).
    pub state_type: String,
    /// Team key, e.g. `LOA`.
    pub team_key: String,
    /// Assignee display name; empty when unassigned.
    pub assignee_name: String,
    /// Referenced project `(uuid, name)`; None when unfiled.
    pub project: Option<(String, String)>,
    /// Parent issue uuid; None for top-level issues.
    pub parent_id: Option<String>,
    /// Label names.
    pub labels: Vec<String>,
    /// Structured relations: `(type, other issue uuid)`.
    pub relations: Vec<(String, String)>,
    /// Attachment `(title, url)` pairs — the structured cross-system
    /// references (GitHub PRs among them) that ride content for entity
    /// convergence. Never parsed out of prose.
    pub attachments: Vec<(String, String)>,
}

fn str_field(node: &serde_json::Value, key: &str) -> String {
    node.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn nested_str(node: &serde_json::Value, path: &[&str]) -> String {
    let mut current = node;
    for key in path {
        current = match current.get(key) {
            Some(value) if !value.is_null() => value,
            _ => return String::new(),
        };
    }
    current.as_str().unwrap_or_default().to_string()
}

/// Parse one GraphQL issues page (`data.issues`). Malformed nodes are
/// skipped and counted, never guessed. Returns `(issues, skipped,
/// has_next, end_cursor)`.
pub fn parse_issues_page(json: &serde_json::Value) -> (Vec<LinearIssue>, usize, bool, String) {
    let empty = Vec::new();
    let nodes = json
        .get("issues")
        .and_then(|issues| issues.get("nodes"))
        .and_then(|nodes| nodes.as_array())
        .unwrap_or(&empty);
    let mut issues = Vec::new();
    let mut skipped = 0usize;
    for node in nodes {
        let id = str_field(node, "id");
        let updated_at = str_field(node, "updatedAt");
        if id.is_empty() || updated_at.is_empty() {
            skipped += 1;
            continue;
        }
        let project = node.get("project").and_then(|p| {
            let id = str_field(p, "id");
            if id.is_empty() {
                None
            } else {
                Some((id, str_field(p, "name")))
            }
        });
        issues.push(LinearIssue {
            id,
            identifier: str_field(node, "identifier"),
            title: str_field(node, "title"),
            description: str_field(node, "description"),
            updated_at,
            canceled_at: str_field(node, "canceledAt"),
            url: str_field(node, "url"),
            branch: str_field(node, "branchName"),
            state_name: nested_str(node, &["state", "name"]),
            state_type: nested_str(node, &["state", "type"]),
            team_key: nested_str(node, &["team", "key"]),
            assignee_name: nested_str(node, &["assignee", "name"]),
            project,
            parent_id: node
                .get("parent")
                .filter(|p| !p.is_null())
                .map(|p| str_field(p, "id"))
                .filter(|id| !id.is_empty()),
            labels: node
                .get("labels")
                .and_then(|l| l.get("nodes"))
                .and_then(|n| n.as_array())
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter_map(|n| n.get("name").and_then(|v| v.as_str()))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            relations: node
                .get("relations")
                .and_then(|r| r.get("nodes"))
                .and_then(|n| n.as_array())
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter_map(|n| {
                            let kind = n.get("type").and_then(|v| v.as_str())?;
                            let other = nested_str(n, &["issue", "id"]);
                            if other.is_empty() {
                                None
                            } else {
                                Some((kind.to_string(), other))
                            }
                        })
                        .collect()
                })
                .unwrap_or_default(),
            attachments: node
                .get("attachments")
                .and_then(|a| a.get("nodes"))
                .and_then(|n| n.as_array())
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter_map(|n| {
                            let url = str_field(n, "url");
                            if url.is_empty() {
                                None
                            } else {
                                Some((str_field(n, "title"), url))
                            }
                        })
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    let page_info = json.get("issues").and_then(|i| i.get("pageInfo"));
    let has_next = page_info
        .and_then(|p| p.get("hasNextPage"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let end_cursor = page_info
        .and_then(|p| p.get("endCursor"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    (issues, skipped, has_next, end_cursor)
}

/// The issue classifier: label names carrying bug-class vocabulary make
/// the row a `Problem`; everything else is a `Task` (work on the
/// board). A substring test, nothing more — the D18 prefix-test
/// discipline.
pub fn memory_type_for(labels: &[String]) -> &'static str {
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

/// Derive the 16-byte table uuid for a workspace: the first 16 bytes of
/// the blake3 digest over `linear:<workspace>` — issues and projects
/// share the table, the logical pk disambiguates (the D18 repo pattern).
pub fn table_uuid_for(workspace: &str) -> [u8; 16] {
    let digest = blake3::hash(format!("linear:{workspace}").as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

fn truncate_200(s: &str) -> String {
    s.chars().take(200).collect()
}

/// RFC3339 (what Linear's API states) to the wire's protobuf
/// Timestamp. Unparseable input yields None (the row stays open)
/// rather than a guessed instant — and the parsers only reach here
/// with fields the API itself emitted.
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
            format!("{cut}\n\n[description bounded at 4000 chars by the adapter]")
        }
    }
}

/// The window cursor: the max updatedAt in the window (ties re-fetch —
/// the gte resume is inclusive and replays idempotently).
pub fn cursor_for(issues: &[LinearIssue]) -> Option<String> {
    issues
        .iter()
        .map(|i| i.updated_at.as_str())
        .max()
        .map(str::to_string)
}

/// Split issues into windows whose MEMORY row count (issues + the
/// projects they introduce) respects the declared per-window bound:
/// the SDK counts memories, so a window of k issues carrying j distinct
/// projects submits k+j rows. A closing chunk reserves one row of
/// headroom for the project the next issue might introduce.
pub fn chunk_windows(issues: Vec<LinearIssue>, max_window: u64) -> Vec<Vec<LinearIssue>> {
    let max_window = max_window.max(2);
    let mut out = Vec::new();
    let mut chunk: Vec<LinearIssue> = Vec::new();
    let mut projects: std::collections::BTreeSet<String> = Default::default();
    for issue in issues {
        let rows = chunk.len() + projects.len();
        if !chunk.is_empty() && rows as u64 + 2 > max_window {
            out.push(std::mem::take(&mut chunk));
            projects.clear();
        }
        if let Some((id, _)) = &issue.project {
            projects.insert(id.clone());
        }
        chunk.push(issue);
    }
    if !chunk.is_empty() {
        out.push(chunk);
    }
    out
}

/// Map one window of parsed issues to a submission unit: issue
/// memories, project memories, and the structured-relation edges whose
/// endpoints are both in the window. Deterministic for a given input.
pub fn map_issues(workspace: &str, issues: &[LinearIssue], batch_id_seed: &str) -> BatchUnit {
    let table = table_uuid_for(workspace);
    let by_id: std::collections::BTreeMap<&str, usize> = issues
        .iter()
        .enumerate()
        .map(|(index, issue)| (issue.id.as_str(), index))
        .collect();

    // Projects, deduped and sorted by id (deterministic emission).
    let projects: std::collections::BTreeMap<&str, &str> = issues
        .iter()
        .filter_map(|issue| {
            issue
                .project
                .as_ref()
                .map(|(id, name)| (id.as_str(), name.as_str()))
        })
        .collect();

    let mut memories: Vec<MemoryDraft> = Vec::new();
    let mut relationships: Vec<RelationshipDraft> = Vec::new();

    for issue in issues {
        let key = format!("issue-{}", issue.id);
        let mut content = format!(
            "{}: {}\n\nState: {} ({})\nTeam: {}\nAssignee: {}\nUpdated: {}\nURL: {}\n",
            if issue.identifier.is_empty() {
                "issue"
            } else {
                &issue.identifier
            },
            issue.title,
            if issue.state_name.is_empty() {
                "unknown"
            } else {
                &issue.state_name
            },
            if issue.state_type.is_empty() {
                "unknown"
            } else {
                &issue.state_type
            },
            if issue.team_key.is_empty() {
                "unfiled"
            } else {
                &issue.team_key
            },
            if issue.assignee_name.is_empty() {
                "unassigned"
            } else {
                &issue.assignee_name
            },
            issue.updated_at,
            issue.url,
        );
        if !issue.labels.is_empty() {
            content.push_str("Labels: ");
            content.push_str(&issue.labels.join(", "));
            content.push('\n');
        }
        if !issue.branch.is_empty() {
            content.push_str("Branch: ");
            content.push_str(&issue.branch);
            content.push('\n');
        }
        if let Some((_, name)) = &issue.project {
            content.push_str("Project: ");
            content.push_str(name);
            content.push('\n');
        }
        if let Some(parent) = &issue.parent_id {
            content.push_str("Parent: ");
            content.push_str(parent);
            content.push('\n');
        }
        for (title, url) in &issue.attachments {
            content.push_str("Attachment: ");
            if !title.is_empty() {
                content.push_str(title);
                content.push_str(": ");
            }
            content.push_str(url);
            content.push('\n');
        }
        if !issue.description.trim().is_empty() {
            content.push('\n');
            content.push_str(&bound_4000(&issue.description));
            content.push('\n');
        }
        memories.push(MemoryDraft {
            rights: None,
            draft_key: key.clone(),
            id: String::new(),
            memory_type: memory_type_for(&issue.labels).into(),
            title: truncate_200(&issue.title),
            content,
            tags: vec!["linear".into(), "issue".into()],
            visibility: 3,
            // Cancellation retires the belief; completion does not.
            valid_from: None,
            valid_until: if issue.canceled_at.is_empty() {
                None
            } else {
                rfc3339_to_timestamp(&issue.canceled_at)
            },
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: issue.id.clone(),
                mapping_version: 1,
            }),
        });

        // Relations: transcribed, both endpoints in-window only.
        for (kind, other) in &issue.relations {
            if !by_id.contains_key(other.as_str()) {
                continue;
            }
            let (from, to) = match kind.as_str() {
                "blocks" => (key.clone(), format!("issue-{other}")),
                "blocked_by" => (format!("issue-{other}"), key.clone()),
                _ => (key.clone(), format!("issue-{other}")), // related_to, duplicate
            };
            relationships.push(RelationshipDraft {
                from_draft_key: from,
                to_draft_key: to,
                kind: if kind == "blocks" || kind == "blocked_by" {
                    "Blocks".into()
                } else {
                    "RelatedTo".into()
                },
                strength: 0.0,
                confidence: 0.9,
                context: format!("linear relation {kind} ({workspace})"),
                visibility: 3,
                to_memory_id: String::new(),
            });
        }

        // Hierarchy: parent contains sub-issue.
        if let Some(parent) = &issue.parent_id {
            if by_id.contains_key(parent.as_str()) {
                relationships.push(RelationshipDraft {
                    from_draft_key: format!("issue-{parent}"),
                    to_draft_key: key.clone(),
                    kind: "Contains".into(),
                    strength: 0.0,
                    confidence: 0.9,
                    context: format!("linear sub-issue ({workspace})"),
                    visibility: 3,
                    to_memory_id: String::new(),
                });
            }
        }

        // Project membership.
        if let Some((project_id, _)) = &issue.project {
            relationships.push(RelationshipDraft {
                from_draft_key: key.clone(),
                to_draft_key: format!("project-{project_id}"),
                kind: "InProject".into(),
                strength: 0.0,
                confidence: 0.9,
                context: format!("linear project membership ({workspace})"),
                visibility: 3,
                to_memory_id: String::new(),
            });
        }
    }

    for (project_id, name) in &projects {
        memories.push(MemoryDraft {
            rights: None,
            draft_key: format!("project-{project_id}"),
            id: String::new(),
            memory_type: "Project".into(),
            title: truncate_200(name),
            content: format!("Linear project {name} (workspace {workspace})."),
            tags: vec!["linear".into(), "project".into()],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: table.to_vec(),
                logical_pk: format!("project:{project_id}"),
                mapping_version: 1,
            }),
        });
    }

    BatchUnit {
        batch_id_seed: batch_id_seed.into(),
        memories,
        relationships,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: cursor_for(issues).unwrap_or_else(|| "empty".into()),
            schema_hash: exocortex_wire::projection::schema_hash(&linear_source_columns()).to_vec(),
            source_flavor: "linear".into(),
        }),
        observed_at: std::time::UNIX_EPOCH,
    }
}

/// The D21-a projection this adapter declares: the selector is the
/// updatedAt window, the mapping is issue -> Task/Problem and project
/// -> Project, and the bounds cap the window (an org can hold 100k
/// issues; never "the whole org" in one window).
pub fn projection(max_window: u64) -> Projection {
    Projection {
        selector: "issues: orderBy updatedAt, filter updatedAt >= cursor".into(),
        fields: vec![
            ProjectionField {
                source_field: "issue_uuid".into(),
                memory_type: "Task".into(),
                kind: String::new(),
            },
            ProjectionField {
                source_field: "project_name".into(),
                memory_type: "Project".into(),
                kind: "InProject".into(),
            },
            ProjectionField {
                source_field: "relations".into(),
                memory_type: String::new(),
                kind: "Blocks".into(),
            },
        ],
        source_schema: LINEAR_SOURCE_COLUMNS
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

    fn page_json() -> serde_json::Value {
        serde_json::json!({
            "issues": {
                "nodes": [
                    {
                        "id": "uuid-a",
                        "identifier": "LOA-1",
                        "title": "Badge shows stale count",
                        "description": "The badge caches forever.",
                        "updatedAt": "2026-09-01T10:00:00.000Z",
                        "canceledAt": "",
                        "url": "https://linear.app/x/issue/LOA-1",
                        "branchName": "gregory/loa-1-badge",
                        "state": { "name": "In Progress", "type": "started" },
                        "team": { "key": "LOA", "name": "Loanlight-eng" },
                        "assignee": { "name": "Jorge" },
                        "project": { "id": "proj-1", "name": "Collateral Review" },
                        "parent": null,
                        "labels": { "nodes": [ { "name": "Bug" }, { "name": "backend" } ] },
                        "relations": { "nodes": [
                            { "type": "blocks", "issue": { "id": "uuid-b" } },
                            { "type": "related_to", "issue": { "id": "uuid-zz-out-of-window" } }
                        ]},
                        "attachments": { "nodes": [
                            { "title": "PR", "url": "https://github.com/acme/api/pull/42" }
                        ]}
                    },
                    {
                        "id": "uuid-b",
                        "identifier": "LOA-2",
                        "title": "Reader instance for failover",
                        "description": "",
                        "updatedAt": "2026-09-02T11:00:00.000Z",
                        "canceledAt": "2026-09-03T09:00:00.000Z",
                        "url": "https://linear.app/x/issue/LOA-2",
                        "branchName": "",
                        "state": { "name": "Canceled", "type": "canceled" },
                        "team": { "key": "LOA", "name": "Loanlight-eng" },
                        "assignee": null,
                        "project": { "id": "proj-1", "name": "Collateral Review" },
                        "parent": { "id": "uuid-a" },
                        "labels": { "nodes": [ { "name": "Improvement" } ] },
                        "relations": { "nodes": [] },
                        "attachments": { "nodes": [] }
                    },
                    { "id": "", "updatedAt": "2026-09-02T12:00:00.000Z" }
                ],
                "pageInfo": { "hasNextPage": true, "endCursor": "cursor-2" }
            }
        })
    }

    #[test]
    fn parser_reads_nodes_and_skips_malformed_loudly() {
        let (issues, skipped, has_next, end_cursor) = parse_issues_page(&page_json());
        assert_eq!(skipped, 1, "empty id is skipped, never guessed");
        assert_eq!(issues.len(), 2);
        assert!(has_next);
        assert_eq!(end_cursor, "cursor-2");
        assert_eq!(issues[0].id, "uuid-a");
        assert_eq!(issues[0].relations.len(), 2);
        assert_eq!(issues[0].attachments.len(), 1);
        assert_eq!(issues[1].parent_id.as_deref(), Some("uuid-a"));
        assert_eq!(issues[1].canceled_at, "2026-09-03T09:00:00.000Z");
    }

    #[test]
    fn classifier_reads_bug_labels() {
        assert_eq!(memory_type_for(&["Bug".into()]), "Problem");
        assert_eq!(memory_type_for(&["Regression risk".into()]), "Problem");
        assert_eq!(memory_type_for(&["Improvement".into()]), "Task");
        assert_eq!(memory_type_for(&[]), "Task");
    }

    #[test]
    fn mapping_transcribes_only_in_window_relations() {
        let (issues, _, _, _) = parse_issues_page(&page_json());
        let unit = map_issues("acme", &issues, "seed");
        // 2 issues + 1 project.
        assert_eq!(unit.memories.len(), 3);
        // blocks (uuid-a -> uuid-b) + contains (uuid-a -> uuid-b) + 2 InProject.
        assert_eq!(unit.relationships.len(), 4);
        assert!(unit.relationships.iter().any(|r| r.kind == "Blocks"
            && r.from_draft_key == "issue-uuid-a"
            && r.to_draft_key == "issue-uuid-b"));
        assert!(unit.relationships.iter().any(|r| r.kind == "Contains"
            && r.from_draft_key == "issue-uuid-a"
            && r.to_draft_key == "issue-uuid-b"));
        assert_eq!(
            unit.relationships
                .iter()
                .filter(|r| r.kind == "InProject")
                .count(),
            2
        );
        // The out-of-window relation produced NO edge.
        assert!(!unit
            .relationships
            .iter()
            .any(|r| r.to_draft_key.contains("uuid-zz")));
        // Bug label -> Problem; canceled issue closes; identity is workspace-scoped.
        let bug = unit
            .memories
            .iter()
            .find(|m| m.draft_key == "issue-uuid-a")
            .unwrap();
        assert_eq!(bug.memory_type, "Problem");
        assert_eq!(bug.valid_until, None);
        assert_eq!(bug.external_key.as_ref().unwrap().logical_pk, "uuid-a");
        assert_eq!(
            bug.external_key.as_ref().unwrap().table_uuid,
            table_uuid_for("acme").to_vec()
        );
        let canceled = unit
            .memories
            .iter()
            .find(|m| m.draft_key == "issue-uuid-b")
            .unwrap();
        assert_eq!(canceled.memory_type, "Task");
        assert_eq!(
            canceled.valid_until,
            rfc3339_to_timestamp("2026-09-03T09:00:00.000Z")
        );
        // Structured attachment rides content for entity convergence.
        assert!(bug.content.contains("https://github.com/acme/api/pull/42"));
        // Projects carry their own identity.
        let project = unit
            .memories
            .iter()
            .find(|m| m.memory_type == "Project")
            .unwrap();
        assert_eq!(
            project.external_key.as_ref().unwrap().logical_pk,
            "project:proj-1"
        );
    }

    #[test]
    fn mapping_is_deterministic_and_cursor_is_max_updated() {
        let (issues, _, _, _) = parse_issues_page(&page_json());
        let a = map_issues("acme", &issues, "seed");
        let b = map_issues("acme", &issues, "seed");
        assert_eq!(a.memories, b.memories);
        assert_eq!(a.relationships, b.relationships);
        assert_eq!(
            cursor_for(&issues).as_deref(),
            Some("2026-09-02T11:00:00.000Z")
        );
        assert_eq!(cursor_for(&[]), None);
    }

    #[test]
    fn long_descriptions_are_bounded_not_truncated_silently() {
        let long = "x".repeat(9000);
        let issue = LinearIssue {
            id: "uuid-long".into(),
            identifier: "LOA-9".into(),
            title: "t".into(),
            description: long,
            updated_at: "2026-09-01T00:00:00.000Z".into(),
            canceled_at: String::new(),
            url: String::new(),
            branch: String::new(),
            state_name: String::new(),
            state_type: String::new(),
            team_key: String::new(),
            assignee_name: String::new(),
            project: None,
            parent_id: None,
            labels: vec![],
            relations: vec![],
            attachments: vec![],
        };
        let unit = map_issues("acme", &[issue], "seed");
        let memory = &unit.memories[0];
        assert!(memory
            .content
            .contains("[description bounded at 4000 chars by the adapter]"));
        assert!(memory.content.chars().count() < 4300);
    }

    #[test]
    fn projection_declares_the_window_contract() {
        let projection = projection(256);
        assert_eq!(projection.bounds.max_rows_per_window, 256);
        assert_eq!(projection.source_schema.len(), LINEAR_SOURCE_COLUMNS.len());
        assert!(projection.selector.contains("updatedAt"));
    }

    fn bare_issue(id: &str, project: Option<&str>) -> LinearIssue {
        LinearIssue {
            id: id.into(),
            identifier: String::new(),
            title: String::new(),
            description: String::new(),
            updated_at: format!("2026-09-01T00:00:{id_len:0>2}.000Z", id_len = id.len()),
            canceled_at: String::new(),
            url: String::new(),
            branch: String::new(),
            state_name: String::new(),
            state_type: String::new(),
            team_key: String::new(),
            assignee_name: String::new(),
            project: project.map(|p| (p.to_string(), p.to_string())),
            parent_id: None,
            labels: vec![],
            relations: vec![],
            attachments: vec![],
        }
    }

    #[test]
    fn chunk_windows_respect_memory_rows_not_issue_counts() {
        // 10 issues, each its own project: 20 memory rows. A bound of 5
        // rows must yield windows of at most 2 issues + 2 projects.
        let issues: Vec<LinearIssue> = (0..10)
            .map(|n| bare_issue(&format!("i{n}"), Some(&format!("p{n}"))))
            .collect();
        let chunks = chunk_windows(issues, 5);
        for chunk in &chunks {
            let projects: std::collections::BTreeSet<&str> = chunk
                .iter()
                .filter_map(|i| i.project.as_ref().map(|(id, _)| id.as_str()))
                .collect();
            let rows = chunk.len() + projects.len();
            assert!(
                rows <= 5,
                "window carried {rows} memory rows ({} issues + {} projects)",
                chunk.len(),
                projects.len()
            );
        }
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 10, "chunking loses no issues");
        // The degenerate bound is clamped: a single issue with a project
        // is two rows, the smallest legal window.
        let single = chunk_windows(vec![bare_issue("solo", Some("p"))], 1);
        assert_eq!(single.len(), 1);
    }
}
