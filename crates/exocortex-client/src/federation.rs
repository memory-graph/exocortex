//! D3 federation, sender side: project a batch into a
//! [`CommonSubsetPlan`] before it crosses to a peer whose ontology
//! knows less — dropping every row the plan's vocabulary cannot carry
//! and naming every drop, never silently. The output batch carries the
//! PLAN's fingerprint as its ontology claim (the honest "I know less"
//! statement the receiver's ingest boundary admits) and is NOT signed:
//! the caller runs [`exocortex_wire::signing::prepare_batch`] after
//! projection, exactly as for any batch it builds itself.

use exocortex_kernel::federation::CommonSubsetPlan;
use exocortex_wire::ingest::v1::IngestBatch;

/// What projection did: the survivors ride the batch; every drop is
/// named so a caller can surface it (an export tool prints this, a
/// submit tool logs it — the never-silent rule).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProjectionReport {
    pub kept_memories: usize,
    pub dropped_memories: Vec<Dropped>,
    pub kept_edges: usize,
    pub dropped_edges: Vec<Dropped>,
    /// The plan's pack names (for the report header).
    pub plan_packs: Vec<String>,
}

/// One named drop: what left the batch and the one-word class of why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dropped {
    /// The draft key (memories) or `from->to` render (edges).
    pub key: String,
    /// `type:Label` / `kind:Label` / `endpoint-dropped`.
    pub reason: String,
}

/// Project `batch` into `plan`: keeps drafts whose `memory_type` label
/// the plan declares and edges whose `kind` it declares; drops an
/// intra-batch edge when either endpoint draft was dropped (no dangling
/// references cross); keeps cross-batch `to_memory_id` edges whose kind
/// survives (the receiver validates the target triple itself). The
/// output's `ontology_fingerprint` becomes the plan's fingerprint.
///
/// Refuses (`Err`) a projection that would keep NOTHING — an empty
/// batch is not a wrapup, and the caller should hear that loudly rather
/// than ship zeroes. `batch_id` and `checksum` ride through untouched:
/// `batch_id` is the producer's identity for this content (a projected
/// replay dedupes correctly), and the checksum is recomputed by the
/// caller's `prepare_batch` (projection invalidates any prior one).
pub fn project_batch(
    batch: &IngestBatch,
    plan: &CommonSubsetPlan,
) -> Result<(IngestBatch, ProjectionReport), String> {
    let mut report = ProjectionReport {
        plan_packs: plan.packs.clone(),
        ..Default::default()
    };
    let mut out = batch.clone();
    out.ontology_fingerprint = plan.fingerprint.to_vec();

    let mut kept_keys = std::collections::BTreeSet::new();
    let mut memories = Vec::with_capacity(batch.memories.len());
    for draft in &batch.memories {
        if plan.memory_types.contains(draft.memory_type.as_str()) {
            kept_keys.insert(draft.draft_key.clone());
            memories.push(draft.clone());
        } else {
            report.dropped_memories.push(Dropped {
                key: draft.draft_key.clone(),
                reason: format!("type:{}", draft.memory_type),
            });
        }
    }
    let mut relationships = Vec::with_capacity(batch.relationships.len());
    for edge in &batch.relationships {
        let kind_ok = plan.kinds.contains(edge.kind.as_str());
        let endpoints_ok = if edge.to_draft_key.is_empty() {
            // Cross-batch: the from-side draft must survive here; the
            // to-side lives on the receiver and is its to validate.
            kept_keys.contains(edge.from_draft_key.as_str())
        } else {
            kept_keys.contains(edge.from_draft_key.as_str())
                && kept_keys.contains(edge.to_draft_key.as_str())
        };
        if kind_ok && endpoints_ok {
            relationships.push(edge.clone());
        } else {
            let reason = if !kind_ok {
                format!("kind:{}", edge.kind)
            } else {
                "endpoint-dropped".to_string()
            };
            report.dropped_edges.push(Dropped {
                key: format!("{}->{}", edge.from_draft_key, {
                    if edge.to_draft_key.is_empty() {
                        edge.to_memory_id.clone()
                    } else {
                        edge.to_draft_key.clone()
                    }
                }),
                reason,
            });
        }
    }
    report.kept_memories = memories.len();
    report.kept_edges = relationships.len();
    if memories.is_empty() {
        return Err(format!(
            "projection into {:?} keeps no memories (dropped {}); nothing to submit",
            plan.packs,
            report.dropped_memories.len()
        ));
    }
    out.memories = memories;
    out.relationships = relationships;
    Ok((out, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use exocortex_kernel::fingerprint::OntologyFingerprint;
    use exocortex_wire::ingest::v1::{MemoryDraft, RelationshipDraft};

    fn plan() -> CommonSubsetPlan {
        CommonSubsetPlan {
            packs: vec!["exocortex-pack-dev-v1".into()],
            fingerprint: [9u8; 32],
            memory_types: ["Fix", "Problem"].into_iter().map(Into::into).collect(),
            kinds: ["Solves", "RelatedTo"]
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }

    fn draft(key: &str, memory_type: &str) -> MemoryDraft {
        MemoryDraft {
            draft_key: key.into(),
            id: String::new(),
            memory_type: memory_type.into(),
            title: format!("{key} title"),
            content: format!("{key} content"),
            visibility: 3,
            tags: vec![],
            valid_from: None,
            valid_until: None,
            external_key: None,
            rights: None,
        }
    }

    fn edge(from: &str, to: &str, kind: &str) -> RelationshipDraft {
        RelationshipDraft {
            from_draft_key: from.into(),
            to_draft_key: to.into(),
            to_memory_id: String::new(),
            kind: kind.into(),
            strength: 0.0,
            confidence: 0.0,
            context: String::new(),
            visibility: 3,
        }
    }

    fn batch() -> IngestBatch {
        IngestBatch {
            org_id: "org".into(),
            source_uri: "session://fed-test".into(),
            producer_id: "session-wrapup".into(),
            batch_id: "batch-1".into(),
            mapping_version: String::new(),
            ontology_fingerprint: vec![1u8; 32],
            ceiling: 3,
            checksum: String::new(),
            observed_at: None,
            recorded_at: None,
            snapshot: None,
            memories: vec![
                draft("fix-1", "Fix"),
                draft("loan-1", "Borrower"),
                draft("prob-1", "Problem"),
            ],
            relationships: vec![
                edge("fix-1", "prob-1", "Solves"),
                edge("loan-1", "prob-1", "Solves"),
                edge("fix-1", "loan-1", "Underwrites"),
                RelationshipDraft {
                    from_draft_key: "fix-1".into(),
                    to_draft_key: String::new(),
                    to_memory_id: "a".repeat(32),
                    kind: "RelatedTo".into(),
                    strength: 0.0,
                    confidence: 0.0,
                    context: String::new(),
                    visibility: 3,
                },
            ],
            producer: None,
        }
    }

    /// The personal↔employer cut: dev rows and dev-kind edges survive
    /// with the plan's fingerprint as the claim; mortgage rows, their
    /// edges, and mortgage-kind edges drop — each drop NAMED.
    #[test]
    fn projection_keeps_the_common_subset_and_names_every_drop() {
        let (out, report) = project_batch(&batch(), &plan()).expect("the cut is non-empty");
        assert_eq!(out.ontology_fingerprint, vec![9u8; 32]);
        let keys: Vec<&str> = out.memories.iter().map(|m| m.draft_key.as_str()).collect();
        assert_eq!(keys, vec!["fix-1", "prob-1"]);
        let edges: Vec<&str> = out
            .relationships
            .iter()
            .map(|e| e.from_draft_key.as_str())
            .collect();
        // Solves(fix->prob) and the cross-batch RelatedTo survive; the
        // loan-anchored Solves and the Underwrites drop.
        assert_eq!(edges, vec!["fix-1", "fix-1"]);
        assert_eq!(report.kept_memories, 2);
        assert_eq!(report.kept_edges, 2);
        assert_eq!(
            report.dropped_memories,
            vec![Dropped {
                key: "loan-1".into(),
                reason: "type:Borrower".into()
            }]
        );
        assert_eq!(
            report.dropped_edges,
            vec![
                Dropped {
                    key: "loan-1->prob-1".into(),
                    reason: "endpoint-dropped".into()
                },
                Dropped {
                    key: "fix-1->loan-1".into(),
                    reason: "kind:Underwrites".into()
                },
            ]
        );
        // batch_id rides through: the producer's identity for this
        // content; the caller re-prepares the checksum.
        assert_eq!(out.batch_id, "batch-1");
    }

    /// A plan that carries none of the batch refuses loudly — shipping
    /// an empty wrapup would be the silent drop this module exists to
    /// prevent.
    #[test]
    fn a_projection_that_keeps_nothing_refuses() {
        let empty_plan = CommonSubsetPlan {
            packs: vec!["unrelated".into()],
            fingerprint: OntologyFingerprint([0u8; 32]).0,
            memory_types: ["Topic"].into_iter().map(Into::into).collect(),
            kinds: ["Mentions"].into_iter().map(Into::into).collect(),
        };
        let err = project_batch(&batch(), &empty_plan).expect_err("nothing survives");
        assert!(
            err.contains("keeps no memories"),
            "the refusal names the empty cut: {err}"
        );
    }
}
