//! SR-PRD F1 (docs/bug-prd-standalone-readback.md): the ONE
//! materializer — WAL entry → kernel rows for the served snapshot.
//!
//! Both read-back paths (F2 live write-back, F3 boot seeding) go through
//! `materialize_entry`, so "what the WAL stored" and "what the snapshot
//! serves" cannot drift — the bug class W2 killed at the wire, avoided
//! here by construction.
//!
//! Field parity with the backend commit path (`exocortex-ingest`
//! `service.rs`): `Provenance::Asserted { author: "session-wrapup",
//! CodingAgent }` (the same identity the drain registers), importance
//! 0.5 / confidence 0.8 at ingest, edge ids via
//! `RelationshipId::derive(from, kind, to, None)`, and the W5
//! narrower-endpoint visibility rule. Timestamps come from the stored
//! draft context, so a materialized row is byte-identical on every
//! restart (AC2: ids and rows are stable, not regenerated).
//!
//! The offline WRITE path defers triple validation to drain time (§4.5)
//! — but standalone never drains, so this module runs the kernel's one
//! rulebook (`validate_triple`) on every edge before it is served. An
//! edge whose target is unknown locally, or whose triple is invalid, is
//! dropped and recorded in `dropped_edges` rather than served forever
//! unvalidated.

use std::collections::HashMap;

use exocortex_kernel::{
    Memory, MemoryId, Ontology, ProducerKind, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};

use crate::wal::WalEntry;

/// `MemoryId` → 32-hex (the wire's id rendering, diagnostics only).
fn hex16(b: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(32);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// What one WAL entry materializes into for the served snapshot.
#[derive(Debug, Default)]
pub struct Materialized {
    /// Asserted memories under their WAL-stored ids.
    pub memories: Vec<Memory>,
    /// Edges resolved from the drafts' `edge_hints`, triple-validated.
    pub edges: Vec<Relationship>,
    /// Why a hint was not materialized (surfaced in logs by callers).
    pub dropped_edges: Vec<String>,
}

/// Resolve a cross-batch edge target: its memory type (for the triple
/// check) and visibility (for the W5 narrower-endpoint rule). `None` =
/// no local row with that id — the hint is dropped (AC7).
pub type TargetResolver<'a> = dyn Fn(&MemoryId) -> Option<(u8, Visibility)> + 'a;

/// Materialize one WAL entry into served rows. `resolve_target` answers
/// cross-batch `to_memory_id` lookups; in-batch targets resolve from the
/// batch itself regardless of the resolver. F5: a non-empty session id
/// mints the same Conversation node + InSession edges the backend's
/// commit path would.
pub fn materialize_entry(
    ontology: &Ontology,
    org: &str,
    entry: &WalEntry,
    resolve_target: &TargetResolver<'_>,
) -> Materialized {
    let mut out = Materialized::default();
    if entry.memories.len() != entry.memory_ids.len() {
        // Corrupt length pairing: serve nothing rather than misaligned
        // rows (the WAL remains the source of truth).
        out.dropped_edges.push(format!(
            "entry {}: memories/memory_ids length mismatch ({} vs {})",
            entry.local_lsn,
            entry.memories.len(),
            entry.memory_ids.len()
        ));
        return out;
    }
    for (idx, draft) in entry.memories.iter().enumerate() {
        let ts = draft.context.timestamp;
        out.memories.push(Memory {
            id: entry.memory_ids[idx],
            memory_type: draft.memory_type,
            title: draft.title.clone(),
            content: draft.content.clone(),
            summary: None,
            // §2.6.1: lowercased/trimmed/deduped at draft→memory — the
            // CL1 parallel array carries the harness-supplied tags.
            tags: exocortex_kernel::normalize_tags(
                entry
                    .tags
                    .get(idx)
                    .into_iter()
                    .flatten()
                    .map(|s| s.as_str()),
            ),
            visibility: draft.visibility,
            provenance: Provenance::Asserted {
                author: crate::drain::PRODUCER_ID.into(),
                producer_kind: Some(ProducerKind::CodingAgent),
            },
            context: draft.context.clone(),
            importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
            confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
            effectiveness: None,
            usage_count: 0,
            valid_from: ts,
            valid_until: None,
            recorded_at: ts,
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_local(entry.local_lsn),
        });
    }
    // In-batch targets (id → (type, visibility)) — always authoritative
    // over the resolver: the batch is the freshest truth about itself.
    let mut local: HashMap<MemoryId, (u8, Visibility)> = HashMap::new();
    for m in &out.memories {
        local.insert(m.id, (m.memory_type, m.visibility));
    }
    for (idx, draft) in entry.memories.iter().enumerate() {
        let from = &out.memories[idx];
        for hint in &draft.edge_hints {
            let to_id = hint.to;
            let Some((to_type, to_vis)) = local
                .get(&to_id)
                .copied()
                .map(Some)
                .unwrap_or_else(|| resolve_target(&to_id))
            else {
                out.dropped_edges.push(format!(
                    "entry {}: edge {}→{} dropped: no local row for target",
                    entry.local_lsn,
                    hex16(&from.id.0),
                    hex16(&to_id.0)
                ));
                continue;
            };
            // W2's one rulebook at the read surface: standalone never
            // drains, so this is the only triple check these edges get.
            if let Err(e) = exocortex_kernel::validator::validate_triple(
                ontology,
                from.memory_type,
                hint.kind,
                to_type,
            ) {
                out.dropped_edges.push(format!(
                    "entry {}: edge {} kind {} rejected: {e}",
                    entry.local_lsn,
                    hex16(&from.id.0),
                    hex16(&to_id.0)
                ));
                continue;
            }
            let ts = from.recorded_at;
            out.edges.push(relationship_from_hint(
                ontology,
                from,
                to_id,
                to_vis,
                hint,
                ts,
                entry.local_lsn,
            ));
        }
    }
    // F5: session grouping parity — the backend commit path groups
    // session-flavor batches; the standalone WAL's session id is the
    // same key the drain renders as `session://<id>`. Deterministic ids
    // make multi-batch sessions converge on one node (CR1 upsert).
    if !entry.session_id.is_empty() {
        if let Some(first_ts) = entry.memories.first().map(|d| d.context.timestamp) {
            if let Some(node) = grouping_node_local(ontology, org, &entry.session_id, first_ts) {
                for m in &out.memories {
                    match grouping_edge_local(ontology, m, &node, first_ts) {
                        Some(e) => out.edges.push(e),
                        None => out.dropped_edges.push(format!(
                            "entry {}: kind `{GROUPING_EDGE_KIND}` missing from ontology",
                            entry.local_lsn
                        )),
                    }
                }
                out.memories.push(node);
            } else {
                out.dropped_edges.push(format!(
                    "entry {}: type `{GROUPING_NODE_TYPE}` missing from ontology; no grouping",
                    entry.local_lsn
                ));
            }
        }
    }
    out
}

/// SR-PRD F3: fold ALL WAL entries (every state — standalone never
/// delivers any of them server-side) into one row set for boot seeding.
/// Entries materialize in local-LSN order, so a later entry's
/// cross-batch edge resolves against everything written before it;
/// targets that never existed locally are dropped per the entry rule.
pub fn materialize_all(ontology: &Ontology, org: &str, entries: &[WalEntry]) -> Materialized {
    let mut acc = Materialized::default();
    let mut known: HashMap<MemoryId, (u8, Visibility)> = HashMap::new();
    for entry in entries {
        let rows = materialize_entry(ontology, org, entry, &|id| known.get(id).copied());
        acc.dropped_edges.extend(rows.dropped_edges);
        for m in rows.memories {
            known.insert(m.id, (m.memory_type, m.visibility));
            acc.memories.push(m);
        }
        acc.edges.extend(rows.edges);
    }
    acc
}

// SR-PRD F5: local grouping parity (D6). The backend commit path mints
// a `Conversation` node + `InSession` edges for session-flavor batches
// (`exocortex-ingest/src/grouping.rs`). The client cannot reuse those
// builders — `exocortex-ingest` dev-depends on this crate (a src-dep
// would be a cycle) and would drag `exocortex-dreams` into the
// personal-mode binary — so they are duplicated here as pure kernel
// construction and pinned field-for-field by
// `grouping_parity_with_backend_commit_path` (tests/), the W2
// golden-table discipline applied to builders. Deterministic ids from
// (org, flavor, key) mean an agent that learns "my writes group into
// conversations" in standalone keeps exactly that when a backend
// appears.

/// The session rule the backend registers (grouping.rs): its constants
/// inlined — one source of truth enforced by the parity test.
const GROUPING_FLAVOR: &str = "session";
const GROUPING_NODE_TYPE: &str = "Conversation";
const GROUPING_EDGE_KIND: &str = "InSession";

/// Client-side mirror of `grouping::grouping_node`: deterministic id
/// over (org, flavor, key), `Derived` provenance, Project visibility,
/// 0.8 confidence (the derived base — no evidence events locally).
pub fn grouping_node_local(
    ontology: &Ontology,
    org: &str,
    key: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Memory> {
    let memory_type = ontology.memory_type_id(GROUPING_NODE_TYPE)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"exocortex-grouping-v1");
    hasher_update_str(&mut hasher, org);
    hasher_update_str(&mut hasher, GROUPING_FLAVOR);
    hasher_update_str(&mut hasher, key);
    let hash = hasher.finalize();
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&hash.as_bytes()[..16]);
    let short: String = key.chars().take(8).collect();
    Some(Memory {
        id: MemoryId(id_bytes),
        memory_type,
        title: format!("Session {short}").into(),
        content: format!("Backend grouping node ({GROUPING_FLAVOR}) for {key}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Project,
        provenance: Provenance::Derived {
            rule_id: format!("grouping:{GROUPING_FLAVOR}").into(),
            evidence: vec![],
        },
        context: grouping_context(key, now),
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    })
}

/// Client-side mirror of `grouping::grouping_edge`: member ⇒ node,
/// deterministic relationship id, W5 narrower-endpoint visibility.
pub fn grouping_edge_local(
    ontology: &Ontology,
    member: &Memory,
    node: &Memory,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Relationship> {
    let kind = ontology.kind_id(GROUPING_EDGE_KIND)?;
    Some(Relationship {
        id: RelationshipId::derive(member.id, kind, node.id, None),
        kind,
        from: member.id,
        to: node.id,
        visibility: member.visibility.min(node.visibility),
        provenance: Provenance::Derived {
            rule_id: format!("grouping:{GROUPING_FLAVOR}").into(),
            evidence: vec![],
        },
        properties: exocortex_kernel::RelationshipProperties {
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

fn grouping_context(
    key: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> exocortex_kernel::MemoryContext {
    exocortex_kernel::MemoryContext {
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
    }
}

fn hasher_update_str(h: &mut blake3::Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

/// One hint → one `Relationship`, mirroring the backend mint
/// (`service.rs`): derived id, W5 narrower-endpoint visibility, kind
/// default strength / 0.8 confidence when the hint carries none.
fn relationship_from_hint(
    ontology: &Ontology,
    from: &Memory,
    to: MemoryId,
    to_vis: Visibility,
    hint: &exocortex_kernel::EdgeHint,
    ts: chrono::DateTime<chrono::Utc>,
    local_lsn: u64,
) -> Relationship {
    let default_strength = ontology
        .kinds_by_id
        .get(&hint.kind)
        .map(|m| m.default_strength)
        .unwrap_or(0.5);
    Relationship {
        id: RelationshipId::derive(from.id, hint.kind, to, None),
        kind: hint.kind,
        from: from.id,
        to,
        // W5: never more visible than either endpoint.
        visibility: from.visibility.min(to_vis),
        // Same assertion identity the drained batch would carry.
        provenance: Provenance::Asserted {
            author: crate::drain::PRODUCER_ID.into(),
            producer_kind: Some(ProducerKind::CodingAgent),
        },
        properties: RelationshipProperties {
            strength: hint.strength.unwrap_or(default_strength),
            confidence: hint.confidence.unwrap_or(0.8),
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: ts,
        },
        description: None,
        bidirectional: false,
        valid_from: ts,
        valid_until: None,
        recorded_at: ts,
        invalidated_by: None,
        lsn: LSN::new_local(local_lsn),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exocortex_kernel::{EdgeHint, MemoryContext, MemoryDraft};
    use std::sync::Arc;

    fn ontology() -> Arc<Ontology> {
        let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
        Arc::new(exocortex_kernel::pack::load_registered_packs().unwrap())
    }

    fn ctx(session: &str) -> MemoryContext {
        MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some("p1".into()),
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: Some(session.into()),
            user_id: Some("u1".into()),
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
        }
    }

    fn draft(ontology: &Ontology, type_label: &str, title: &str) -> MemoryDraft {
        MemoryDraft {
            memory_type: ontology.memory_type_id(type_label).unwrap(),
            title: title.into(),
            content: "body".into(),
            summary: None,
            visibility: Visibility::Org,
            context: ctx("s1"),
            edge_hints: Default::default(),
            external_key: None,
        }
    }

    fn entry_with(drafts: Vec<MemoryDraft>) -> WalEntry {
        WalEntry {
            local_lsn: 7,
            session_id: "s1".into(),
            memory_ids: (0..drafts.len()).map(|_| MemoryId::new_v7()).collect(),
            memories: drafts,
            state: crate::wal::WalState::Pending,
            batch_id: "b".into(),
            draft_keys: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// F1: the materializer stamps the backend's field set — Asserted
    /// CodingAgent provenance (the drain's identity), 0.5/0.8 scores,
    /// the stored draft context verbatim, and the local LSN.
    #[test]
    fn materialized_memory_matches_backend_field_set() {
        let onto = ontology();
        let e = entry_with(vec![draft(&onto, "Problem", "Flaky test")]);
        let tags: Vec<Vec<String>> = vec![vec!["CI".into(), " ci ".into()]];
        let e = WalEntry { tags, ..e };
        let m = materialize_entry(&onto, "org", &e, &|_| None);
        // F5: the memory + its session Conversation node.
        assert_eq!(m.memories.len(), 2);
        let mem = m
            .memories
            .iter()
            .find(|x| x.id == e.memory_ids[0])
            .expect("the written memory");
        assert_eq!(mem.id, e.memory_ids[0], "WAL-stored id, not regenerated");
        assert_eq!(
            mem.provenance,
            Provenance::Asserted {
                author: "session-wrapup".into(),
                producer_kind: Some(ProducerKind::CodingAgent)
            }
        );
        assert_eq!(mem.tags.to_vec(), vec!["ci".to_string()], "normalized tags");
        assert_eq!(mem.context.session_id.as_deref(), Some("s1"));
        assert_eq!(mem.lsn, LSN::new_local(7));
        assert_eq!(
            mem.confidence,
            exocortex_kernel::memory::F01::new(0.8).unwrap()
        );
    }

    /// F1/AC7: a hint whose target has no local row is dropped and
    /// recorded — never served unvalidated, never fabricated.
    #[test]
    fn dangling_cross_batch_hint_is_dropped() {
        let onto = ontology();
        let mut d = draft(&onto, "Fix", "Retry once");
        d.edge_hints.push(EdgeHint {
            kind: onto.kind_id("Fixes").unwrap(),
            to: MemoryId::new_v7(), // exists nowhere
            strength: None,
            confidence: None,
        });
        let e = entry_with(vec![d]);
        let m = materialize_entry(&onto, "org", &e, &|_| None);
        assert!(
            m.edges
                .iter()
                .all(|r| r.to != e.memories[0].edge_hints[0].to),
            "dangling edge dropped (only grouping edges remain)"
        );
        assert_eq!(m.dropped_edges.len(), 1);
        assert!(m.dropped_edges[0].contains("no local row"));
    }

    /// F1: in-batch edges resolve WITHOUT the resolver (the batch is its
    /// own truth), validate against the rulebook, and mint the backend's
    /// id derivation + W5 narrower-endpoint visibility.
    #[test]
    fn in_batch_edge_materializes_with_derived_id() {
        let onto = ontology();
        let problem = draft(&onto, "Problem", "Flaky test");
        let mut fix = draft(&onto, "Fix", "Retry once");
        fix.edge_hints.push(EdgeHint {
            kind: onto.kind_id("Fixes").unwrap(),
            to: MemoryId::new_v7(), // patched below once ids exist
            strength: None,
            confidence: None,
        });
        let e = entry_with(vec![problem, fix]);
        let fix_id = e.memory_ids[1];
        let problem_id = e.memory_ids[0];
        let mut drafts = e.memories.clone();
        drafts[1].edge_hints[0].to = problem_id;
        let e = WalEntry {
            memories: drafts,
            ..e
        };
        // Resolver that would FAIL the lookup: in-batch must not need it.
        let m = materialize_entry(&onto, "org", &e, &|_| None);
        // F5 adds the InSession grouping edge; the asserted Fixes edge is
        // found by its derived id.
        let fixes = onto.kind_id("Fixes").unwrap();
        let expected_id = RelationshipId::derive(fix_id, fixes, problem_id, None);
        let rel = m
            .edges
            .iter()
            .find(|r| r.id == expected_id)
            .expect("Fixes edge materialized");
        assert_eq!(rel.from, fix_id);
        assert_eq!(rel.to, problem_id);
        assert_eq!(rel.visibility, Visibility::Org);
        assert!(m
            .edges
            .iter()
            .any(|r| r.kind == onto.kind_id("InSession").unwrap()));
    }

    /// F1: an invalid triple is rejected by the same kernel rulebook the
    /// ingest path runs — a stale hint can never enter the read surface.
    #[test]
    fn invalid_triple_is_rejected() {
        let onto = ontology();
        // Fixes: Fix → Problem only. A Problem→Problem edge is invalid.
        let mut problem = draft(&onto, "Problem", "Flaky test");
        let other = draft(&onto, "Problem", "Other problem");
        let e = entry_with(vec![problem.clone(), other]);
        let to_id = e.memory_ids[1];
        problem.edge_hints.push(EdgeHint {
            kind: onto.kind_id("Fixes").unwrap(),
            to: to_id,
            strength: None,
            confidence: None,
        });
        let mut drafts = e.memories.clone();
        drafts[0].edge_hints = problem.edge_hints.clone();
        let e = WalEntry {
            memories: drafts,
            ..e
        };
        let m = materialize_entry(&onto, "org", &e, &|_| None);
        assert!(
            m.edges.iter().all(|r| r.to != to_id),
            "invalid triple dropped (only grouping edges remain)"
        );
        assert!(m.dropped_edges[0].contains("rejected"));
    }

    /// F5: every non-empty-session entry mints the Conversation node and
    /// one InSession edge per memory — same derivation the backend mints
    /// (pinned field-for-field by the integration parity test).
    #[test]
    fn grouping_rows_minted_per_session() {
        let onto = ontology();
        let e = entry_with(vec![
            draft(&onto, "Problem", "Flaky test"),
            draft(&onto, "Fix", "Retry once"),
        ]);
        let m = materialize_entry(&onto, "the-org", &e, &|_| None);
        let node = grouping_node_local(
            &onto,
            "the-org",
            &e.session_id,
            e.memories[0].context.timestamp,
        )
        .unwrap();
        assert_eq!(
            m.memories.last().unwrap().id,
            node.id,
            "conversation node minted"
        );
        let in_session = onto.kind_id("InSession").unwrap();
        assert_eq!(
            m.edges
                .iter()
                .filter(|r| r.kind == in_session && r.to == node.id)
                .count(),
            2,
            "one InSession edge per memory"
        );
    }
}
