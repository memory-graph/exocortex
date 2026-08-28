//! D6 (agent-instructions PRD §3.6): per-flavor backend write grouping.
//!
//! The turn-level trigger only works if multiple batches from one
//! conversation land in one group, and no harness has a session-end
//! hook to build it — so the COMMIT PATH builds it. A `GroupingRule`
//! keyed on `source_flavor` answers two questions: what is the grouping
//! key for this batch, and what memory type renders it. `session` is
//! the only rule v1 ships (`Conversation` nodes + `InSession` edges);
//! a docs adapter registers its own when it lands, with no change to
//! the commit path.
//!
//! Everything is derived: deterministic ids (same input ⇒ same id, so
//! replays and multi-batch groups converge), `Derived` provenance (the
//! backend asserted it, not an author), and Dreams leaves these rows
//! alone (structural, not content).

use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_wire::ingest::v1::IngestBatch;

/// One grouping rule per registered `source_flavor`. Absent flavor ⇒
/// no grouping (v1 default).
#[derive(Clone, Copy)]
pub struct GroupingRule {
    /// Matches `IngestBatch`'s registered source flavor.
    pub flavor: &'static str,
    /// Extracts the grouping key from the batch (e.g. the id in
    /// `session://<id>`).
    pub key_of: fn(&IngestBatch) -> Option<String>,
    /// The memory type label the grouping node is rendered as, in the
    /// effective ontology.
    pub node_type: &'static str,
    /// The kind linking members to the grouping node.
    pub edge_kind: &'static str,
}

/// The v1 rule set: one rule, the session wrapup.
pub fn grouping_rules() -> &'static [GroupingRule] {
    static RULES: &[GroupingRule] = &[GroupingRule {
        flavor: "session",
        key_of: parse_session_uri,
        node_type: "Conversation",
        edge_kind: "InSession",
    }];
    RULES
}

/// `session://<id>` ⇒ `<id>` (W3's parser, shared).
pub fn parse_session_uri(batch: &IngestBatch) -> Option<String> {
    batch
        .source_uri
        .strip_prefix("session://")
        .filter(|id| !id.is_empty())
        .map(|id| id.to_string())
}

/// Resolve the rule for a batch's source flavor, then its grouping key.
pub fn grouping_key<'a>(
    batch: &IngestBatch,
    source_flavor: &str,
    rules: &'a [GroupingRule],
) -> Option<(&'a GroupingRule, String)> {
    // Registration is the authority. URI parsing extracts a key only after
    // the registered flavor selects the rule, so a custom source cannot gain
    // session semantics merely by choosing a `session://` URI.
    let rule = rules.iter().find(|rule| rule.flavor == source_flavor)?;
    let key = (rule.key_of)(batch)?;
    Some((rule, key))
}

/// Build the grouping node for `(org, flavor, key)` under a
/// deterministic id: same input ⇒ same `MemoryId`, so replays and
/// multi-batch groups converge on ONE node (and `upsert_batch` on an
/// existing id is a version bump, not a duplicate).
pub fn grouping_node(
    ontology: &exocortex_kernel::Ontology,
    org: &str,
    rule: &GroupingRule,
    key: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Memory> {
    let memory_type = ontology.memory_type_id(rule.node_type)?;
    // Deterministic: blake3 over the stable coordinates, first 16 bytes.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"exocortex-grouping-v1");
    hasher_update_str(&mut hasher, org);
    hasher_update_str(&mut hasher, rule.flavor);
    hasher_update_str(&mut hasher, key);
    let hash = hasher.finalize();
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&hash.as_bytes()[..16]);
    let short: String = key.chars().take(8).collect();
    Some(Memory {
        id: MemoryId(id_bytes),
        memory_type,
        title: format!("Session {short}").into(),
        content: format!("Backend grouping node ({}) for {}", rule.flavor, key),
        summary: None,
        tags: Default::default(),
        // Project, within the session-wrapup ceiling of Org: visible to
        // the project the group's members live in.
        visibility: Visibility::Project,
        provenance: Provenance::Derived {
            rule_id: format!("grouping:{}", rule.flavor).into(),
            evidence: vec![],
        },
        context: MemoryContext {
            timestamp: now,
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: Some(key.into()),
            user_id: None,
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: Default::default(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        embedding: None, // structural rows are not embedded (Dreams skips)
        lsn: LSN::new_local(0),
    })
}

/// The member ⇒ node edge for one accepted memory. Deterministic
/// relationship id: re-minting across a restart is a no-op (W7's
/// concern, answered at the id level).
pub fn grouping_edge(
    ontology: &exocortex_kernel::Ontology,
    rule: &GroupingRule,
    member: &Memory,
    node: &Memory,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Relationship> {
    let kind = ontology.kind_id(rule.edge_kind)?;
    Some(Relationship {
        id: RelationshipId::derive(member.id, kind, node.id, None),
        kind,
        from: member.id,
        to: node.id,
        // W5's rule at the source: never more visible than either end.
        visibility: exocortex_kernel::relationship_visibility(member.visibility, node.visibility),
        provenance: Provenance::Derived {
            rule_id: format!("grouping:{}", rule.flavor).into(),
            evidence: vec![],
        },
        properties: RelationshipProperties {
            strength: ontology
                .kinds_by_id
                .get(&kind)
                .map(|m| m.default_strength)
                .unwrap_or(0.8),
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: now,
        },
        description: None,
        bidirectional: false,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        lsn: LSN::new_local(0),
    })
}

fn hasher_update_str(h: &mut blake3::Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}
