// crates/exocortex-adapter-sdk/src/split.rs
//! Batch splitting (R9/R10/R11): one `BatchUnit` becomes ≥1 signed
//! `IngestBatch`es, each within `max_batch_bytes` (R-I2), each with every
//! relationship co-located with its endpoint drafts (§18.1 forbids
//! cross-batch `draft_key` references), each with a stable id.

use std::collections::BTreeMap;

use exocortex_wire::ingest::v1::{IngestBatch, MemoryDraft, RelationshipDraft};

use crate::{BatchUnit, SdkError};

/// Split one unit into signed batches. Deterministic: components pack in
/// sorted order, memories sort by `draft_key`, and `batch_id` is
/// `"{producer_id}:{seed}:{index}"` (R11) — the same unit always yields
/// byte-identical batches.
pub fn split_unit(
    producer_id: &str,
    unit: &BatchUnit,
    max_batch_bytes: usize,
) -> Result<Vec<IngestBatch>, SdkError> {
    validate_unit(unit)?;

    // Union-find over draft keys: a relationship welds its endpoints
    // into one inseparable component (R10).
    let mut parent: BTreeMap<String, String> = unit
        .memories
        .iter()
        .map(|m| (m.draft_key.clone(), m.draft_key.clone()))
        .collect();
    let find = |k: &str, parent: &mut BTreeMap<String, String>| -> String {
        let mut cur = k.to_string();
        while parent[&cur] != cur {
            let next = parent[&cur].clone();
            cur = next;
        }
        // Path compression.
        let root = cur.clone();
        let mut walk = k.to_string();
        while walk != root {
            let next = parent[&walk].clone();
            parent.insert(walk.clone(), root.clone());
            walk = next;
        }
        root
    };
    for r in &unit.relationships {
        let a = find(&r.from_draft_key, &mut parent);
        let b = find(&r.to_draft_key, &mut parent);
        if a != b {
            parent.insert(a, b);
        }
    }

    // Gather components: root -> (memories, relationships).
    let mut components: BTreeMap<String, (Vec<MemoryDraft>, Vec<RelationshipDraft>)> =
        BTreeMap::new();
    for m in &unit.memories {
        let root = find(&m.draft_key, &mut parent);
        components.entry(root).or_default().0.push(m.clone());
    }
    for r in &unit.relationships {
        let root = find(&r.from_draft_key, &mut parent);
        components.entry(root).or_default().1.push(r.clone());
    }

    // Bin-pack components (sorted by root for determinism) into batches
    // measured by the signed encoded length (R9).
    let mut out: Vec<IngestBatch> = Vec::new();
    let mut pending: Vec<(Vec<MemoryDraft>, Vec<RelationshipDraft>)> = Vec::new();
    let mut pending_bytes = 0usize;
    let mut index: u32 = 0;
    let flush = |out: &mut Vec<IngestBatch>,
                 pending: &mut Vec<(Vec<MemoryDraft>, Vec<RelationshipDraft>)>,
                 index: &mut u32| {
        if pending.is_empty() {
            return;
        }
        let mut memories: Vec<MemoryDraft> = Vec::new();
        let mut relationships: Vec<RelationshipDraft> = Vec::new();
        for (m, r) in pending.drain(..) {
            memories.extend(m);
            relationships.extend(r);
        }
        memories.sort_by(|a, b| a.draft_key.cmp(&b.draft_key));
        relationships.sort_by(|a, b| {
            (&a.from_draft_key, &a.to_draft_key, &a.kind).cmp(&(
                &b.from_draft_key,
                &b.to_draft_key,
                &b.kind,
            ))
        });
        let batch = IngestBatch {
            org_id: String::new(),           // stamped by the session
            source_uri: String::new(),       // stamped by the session
            producer_id: producer_id.into(), // R11 id component
            // CL2 (audit): the id is CONTENT-bound — the same unit split
            // differently (budget change, retry after crash) must never
            // dedupe against a batch with different rows; the canonical
            // checksum covers exactly the rows below, so it is the content
            // identity. The (producer, seed, index) prefix keeps ids
            // distinct across units and re-splits of identical content.
            batch_id: {
                let content = exocortex_wire::signing::canonical_checksum(&IngestBatch {
                    org_id: String::new(),
                    source_uri: String::new(),
                    producer_id: producer_id.into(),
                    batch_id: String::new(),
                    mapping_version: String::new(),
                    ontology_fingerprint: Vec::new(),
                    ceiling: 0,
                    checksum: String::new(),
                    observed_at: None,
                    recorded_at: None,
                    snapshot: unit.snapshot.clone(),
                    memories: memories.clone(),
                    relationships: relationships.clone(),
                    producer: None,
                });
                format!("{producer_id}:{}:{}:{content}", unit.batch_id_seed, *index)
            },
            mapping_version: String::new(), // stamped by the caller pre-submit
            ontology_fingerprint: Vec::new(),
            ceiling: 0,
            checksum: String::new(),
            observed_at: Some(system_ts(unit.observed_at)),
            recorded_at: None,
            snapshot: unit.snapshot.clone(),
            memories,
            relationships,
            producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
                node_id: String::new(),
                agent_id: String::new(),
                adapter_id: String::new(),
                hmac_signature: vec![],
                client_metadata: None,
            }),
        };
        *index += 1;
        out.push(batch);
    };

    for (_root, comp) in components {
        let bytes = component_bytes(producer_id, unit, &comp, index)?;
        if bytes > max_batch_bytes {
            if comp.0.len() == 1 && comp.1.is_empty() && pending.is_empty() && out.is_empty() {
                // A single oversized memory: unsplittable (R10).
                return Err(SdkError::Unsplittable {
                    draft_keys: comp.0.iter().map(|m| m.draft_key.clone()).collect(),
                });
            }
            if !comp.1.is_empty() {
                // A connected component that cannot fit: unsplittable.
                let mut keys: Vec<String> = comp.0.iter().map(|m| m.draft_key.clone()).collect();
                keys.sort();
                return Err(SdkError::Unsplittable { draft_keys: keys });
            }
            // Independent oversized memories flush alone; if even alone
            // they exceed the limit, unsplittable.
            if bytes > max_batch_bytes {
                return Err(SdkError::Unsplittable {
                    draft_keys: comp.0.iter().map(|m| m.draft_key.clone()).collect(),
                });
            }
        }
        if pending_bytes + bytes > max_batch_bytes && !pending.is_empty() {
            flush(&mut out, &mut pending, &mut index);
            pending_bytes = 0;
        }
        pending_bytes += bytes;
        pending.push(comp);
    }
    flush(&mut out, &mut pending, &mut index);

    // Sign every emitted batch, then verify the R-I2 budget against the
    // ACTUAL signed encoded length — the component estimate guides
    // packing, but only the emitted bytes are authoritative.
    use prost::Message;
    for b in &mut out {
        b.checksum = exocortex_wire::signing::canonical_checksum(b);
        if b.encoded_len() > max_batch_bytes {
            let mut keys: Vec<String> = b.memories.iter().map(|m| m.draft_key.clone()).collect();
            keys.sort();
            return Err(SdkError::Unsplittable { draft_keys: keys });
        }
    }
    Ok(out)
}

/// Pre-wire validation (edge cases): dangling relationship endpoints and
/// snapshot rows without external keys never reach the server.
fn validate_unit(unit: &BatchUnit) -> Result<(), SdkError> {
    use std::collections::HashSet;
    let keys: HashSet<&str> = unit.memories.iter().map(|m| m.draft_key.as_str()).collect();
    // M5: a duplicated draft_key would pass endpoint resolution while
    // the server persists two independent rows — reject at the source.
    if keys.len() != unit.memories.len() {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut dups: Vec<&str> = Vec::new();
        for m in &unit.memories {
            if !seen.insert(m.draft_key.as_str()) {
                dups.push(m.draft_key.as_str());
            }
        }
        return Err(SdkError::InvalidUnit {
            detail: format!("duplicate draft_key(s): {dups:?}"),
        });
    }
    for r in &unit.relationships {
        if !keys.contains(r.from_draft_key.as_str()) || !keys.contains(r.to_draft_key.as_str()) {
            return Err(SdkError::InvalidUnit {
                detail: format!(
                    "relationship {}->{} references a draft_key not in this unit",
                    r.from_draft_key, r.to_draft_key
                ),
            });
        }
    }
    if unit.snapshot.is_some() {
        for m in &unit.memories {
            if m.external_key.is_none() {
                return Err(SdkError::InvalidUnit {
                    detail: format!(
                        "snapshot unit memory {} lacks an external_key (MISSING_EXTERNAL_KEY pre-empted)",
                        m.draft_key
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Encoded size of a would-be batch containing exactly this component at
/// this index — the same construction `flush` produces, so the measured
/// size matches what is emitted.
fn component_bytes(
    producer_id: &str,
    unit: &BatchUnit,
    comp: &(Vec<MemoryDraft>, Vec<RelationshipDraft>),
    index: u32,
) -> Result<usize, SdkError> {
    use prost::Message;
    let mut memories = comp.0.clone();
    let relationships = comp.1.clone();
    memories.sort_by(|a, b| a.draft_key.cmp(&b.draft_key));
    let b = IngestBatch {
        org_id: String::new(),
        source_uri: String::new(),
        producer_id: producer_id.into(),
        batch_id: format!("{producer_id}:{}:{}", unit.batch_id_seed, index),
        mapping_version: String::new(),
        ontology_fingerprint: vec![0u8; 32],
        ceiling: 3,
        checksum: "0".repeat(64), // a real checksum's width
        observed_at: Some(system_ts(unit.observed_at)),
        recorded_at: None,
        snapshot: unit.snapshot.clone(),
        memories,
        relationships,
        producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![0u8; 32], // a real signature's width
            client_metadata: None,
        }),
    };
    Ok(b.encoded_len())
}

fn system_ts(t: std::time::SystemTime) -> prost_types::Timestamp {
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exocortex_wire::ingest::v1::MemoryDraft;

    pub(super) fn draft(k: &str, big: bool) -> MemoryDraft {
        MemoryDraft {
            draft_key: k.into(),
            id: String::new(),
            memory_type: "General".into(),
            title: if big { "t".repeat(400) } else { "t".into() },
            content: if big { "c".repeat(400) } else { "c".into() },
            tags: vec![],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: None,
        }
    }

    pub(super) fn unit(
        seed: &str,
        memories: Vec<MemoryDraft>,
        rels: Vec<RelationshipDraft>,
    ) -> BatchUnit {
        BatchUnit {
            batch_id_seed: seed.into(),
            memories,
            relationships: rels,
            snapshot: None,
            observed_at: std::time::UNIX_EPOCH,
        }
    }

    fn rel(a: &str, b: &str) -> RelationshipDraft {
        RelationshipDraft {
            from_draft_key: a.into(),
            to_draft_key: b.into(),
            kind: "Solves".into(),
            strength: 0.5,
            confidence: 0.5,
            context: String::new(),
            visibility: 3,
            to_memory_id: String::new(),
        }
    }

    #[test]
    fn empty_unit_emits_nothing() {
        assert!(split_unit("p", &unit("s", vec![], vec![]), 1024)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn splitting_is_lossless_and_within_budget() {
        let memories: Vec<_> = (0..20).map(|i| draft(&format!("k{i:02}"), true)).collect();
        let batches = split_unit("p", &unit("s", memories.clone(), vec![]), 2048).unwrap();
        assert!(batches.len() > 1);
        use prost::Message;
        for b in &batches {
            assert!(
                b.encoded_len() <= 2048,
                "batch {} exceeds budget: {}",
                b.batch_id,
                b.encoded_len()
            );
        }
        let mut seen: Vec<&str> = batches
            .iter()
            .flat_map(|b| b.memories.iter().map(|m| m.draft_key.as_str()))
            .collect();
        seen.sort_unstable();
        let mut expect: Vec<&str> = memories.iter().map(|m| m.draft_key.as_str()).collect();
        expect.sort_unstable();
        assert_eq!(seen, expect, "union of memories equals input, no loss/dup");
    }

    #[test]
    fn components_stay_together() {
        let memories = vec![draft("a", true), draft("b", true), draft("c", true)];
        let rels = vec![rel("a", "b")];
        let batches = split_unit("p", &unit("s", memories, rels), 2100).unwrap();
        for b in &batches {
            let keys: std::collections::HashSet<&str> =
                b.memories.iter().map(|m| m.draft_key.as_str()).collect();
            for r in &b.relationships {
                assert!(
                    keys.contains(r.from_draft_key.as_str())
                        && keys.contains(r.to_draft_key.as_str()),
                    "relationship severed across batches: {}->{} in {:?}",
                    r.from_draft_key,
                    r.to_draft_key,
                    keys
                );
            }
        }
        // a and b must be in the same batch.
        let ab_together = batches.iter().any(|b| {
            let keys: std::collections::HashSet<&str> =
                b.memories.iter().map(|m| m.draft_key.as_str()).collect();
            keys.contains("a") && keys.contains("b")
        });
        assert!(ab_together, "welded component travels together");
    }

    #[test]
    fn oversized_component_is_unsplittable() {
        let err = split_unit(
            "p",
            &unit(
                "s",
                vec![draft("a", true), draft("b", true)],
                vec![rel("a", "b")],
            ),
            64,
        )
        .unwrap_err();
        match err {
            SdkError::Unsplittable { draft_keys } => {
                assert_eq!(draft_keys, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected Unsplittable, got {other:?}"),
        }
    }

    #[test]
    fn batch_ids_are_deterministic_and_monotonic() {
        let memories = vec![draft("a", true), draft("b", true), draft("c", true)];
        let u = unit("seed-1", memories, vec![]);
        let a = split_unit("p", &u, 1300).unwrap();
        let b = split_unit("p", &u, 1300).unwrap();
        assert_eq!(
            a.iter().map(|x| x.batch_id.clone()).collect::<Vec<_>>(),
            b.iter().map(|x| x.batch_id.clone()).collect::<Vec<_>>(),
            "same unit -> byte-identical ids (R11)"
        );
        // CL2: ids are content-bound — prefix pins (producer, seed, index),
        // suffix is the canonical checksum of the batch's rows.
        assert!(
            a[0].batch_id.starts_with("p:seed-1:0:"),
            "{}",
            a[0].batch_id
        );
        assert!(
            a[1].batch_id.starts_with("p:seed-1:1:"),
            "{}",
            a[1].batch_id
        );
        assert_ne!(a[0].batch_id, a[1].batch_id);

        // Different content under the same seed+index gets a different id:
        // a re-split under a bigger budget packs all rows into batch 0, so
        // its membership (and therefore its id) differs from the tight
        // budget's batch 0 — the old content-free id would have deduped
        // them and silently dropped the extra rows (CL2).
        let c = split_unit("p", &u, 4000).unwrap();
        assert_eq!(c.len(), 1);
        assert_ne!(c[0].batch_id, a[0].batch_id);
    }

    #[test]
    fn dangling_relationship_is_invalid() {
        let err = split_unit(
            "p",
            &unit("s", vec![draft("a", false)], vec![rel("a", "zzz")]),
            2048,
        )
        .unwrap_err();
        assert!(matches!(err, SdkError::InvalidUnit { .. }));
    }

    #[test]
    fn snapshot_without_external_key_is_invalid() {
        let mut m = draft("a", false);
        m.external_key = None;
        let u = BatchUnit {
            snapshot: Some(exocortex_wire::ingest::v1::ExternalSnapshotInfo {
                snapshot_id: "s1".into(),
                schema_hash: vec![0u8; 32],
                source_flavor: "custom".into(),
            }),
            ..unit("s", vec![m], vec![])
        };
        assert!(matches!(
            split_unit("p", &u, 2048).unwrap_err(),
            SdkError::InvalidUnit { .. }
        ));
    }
}

#[cfg(test)]
mod m5_tests {
    use super::tests::{draft, unit};
    use super::*;

    #[test]
    fn duplicate_draft_keys_are_rejected() {
        let err = split_unit(
            "p",
            &unit("s", vec![draft("a", false), draft("a", false)], vec![]),
            2048,
        )
        .unwrap_err();
        match err {
            SdkError::InvalidUnit { detail } => {
                assert!(detail.contains("duplicate"), "{detail}");
            }
            other => panic!("expected InvalidUnit, got {other:?}"),
        }
    }
}
