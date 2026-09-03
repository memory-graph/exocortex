//! M8 acceptance (§12.5 steps 9-10): a 10k dataset with duplicates reduces
//! cardinality >= 20% with non-degrading MCR²; a poison consolidation rolls
//! back within one cycle; the trigger model fires on write counters only;
//! two engines racing for one region settle on exactly one lease holder.

use std::sync::Arc;

use exocortex_dreams::{
    mcr2::{MCR2Engine, MemoryWithEmbedding},
    trigger::{DreamsTrigger, RegionWriteCounters},
    Discovery, DiscoveryKind, DreamsEngine, PruneReason,
};
use exocortex_kernel::{
    Embedding, EmbeddingModel, Memory, MemoryContext, MemoryId, Provenance, RelKindId,
    Relationship, RelationshipId, RelationshipProperties, Visibility, LSN,
};
use exocortex_storage::{InMemoryStorage, LeaseKey, RegionKey, Storage, VisibilityContext};
use futures::StreamExt;

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn mem_with_embedding(i: usize, dup_of: Option<usize>, embedding: Vec<f32>) -> Memory {
    let id = if let Some(src) = dup_of {
        // Deterministic near-duplicate of `src`.
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&(src as u64).to_be_bytes());
        b[8..].copy_from_slice(&(i as u64).to_be_bytes());
        MemoryId(b)
    } else {
        MemoryId::new_v7()
    };
    Memory {
        rights: None,
        id,
        memory_type: 3,
        title: format!("m{i}").into(),
        content: format!("c{i}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "dreams".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some("p".into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("o".into()),
            session_id: None,
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
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: Some(stamped(embedding)),
        lsn: LSN::new_local(0),
    }
}

fn stamped(vector: Vec<f32>) -> Embedding {
    Embedding {
        model: EmbeddingModel {
            name: "bge-small".into(),
            version: "v1".into(),
        },
        vector,
    }
}

fn unit(i: usize) -> Vec<f32> {
    let mut v = vec![0.0; 64];
    v[i % 64] = 1.0;
    v
}

fn relationship(from: MemoryId, to: MemoryId, kind: RelKindId) -> Relationship {
    Relationship {
        id: RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "dreams".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.6,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    }
}

fn kind_named(name: &str) -> RelKindId {
    ontology()
        .kinds_by_id
        .values()
        .find(|kind| kind.display_name == name)
        .map(|kind| kind.id)
        .unwrap()
}

#[tokio::test]
async fn ten_k_dataset_reduces_cardinality_and_keeps_mcr2() {
    let storage = InMemoryStorage::new(ontology());
    // §12.5 step 9's literal dataset: 10,000 memories in storage. The
    // anchor window (top 32 by recency) is filled by 8 duplicate groups
    // x 4 near-duplicates; the remaining 9,968 rows are older fillers
    // (orthogonal embeddings) that stay outside the window.
    let groups = 8usize;
    let dups_per = 4usize;
    let fillers = 10_000usize - groups * dups_per;
    for i in 0..fillers {
        let mut m = mem_with_embedding(i + 100, None, unit(i));
        m.recorded_at = chrono::Utc::now() - chrono::Duration::days(30 + (i % 30) as i64);
        storage.upsert_memory(&m).await.unwrap();
    }
    for g in 0..groups {
        for d in 0..dups_per {
            let mut emb = unit(g);
            emb[(g + d + 1) % 64] = 0.05;
            storage
                .upsert_memory(&mem_with_embedding(50_000 + g * 100 + d, Some(g), emb))
                .await
                .unwrap();
        }
    }

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-1".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let res = engine.try_consolidate(&region).await.expect("cycle");

    // Cardinality reduction among anchors.
    let reduced = res.memories_input.saturating_sub(res.memories_output);
    assert!(
        reduced * 5 >= res.memories_input,
        ">= 20% cardinality reduction: input={} output={}",
        res.memories_input,
        res.memories_output
    );
    // MCR² non-degrading on the surviving set.
    assert!(
        !res.regression,
        "MCR2 must not degrade: before={} after={}",
        res.mcr2_before.delta_r, res.mcr2_after.delta_r
    );
    // Full R-Dr4 stamp.
    assert!(res.lease_epoch >= 1);
    assert_eq!(res.owner_node_id, "dreams-1");
    assert!(!res.session_id.is_empty());
    // R-Dr10: merged ids retained; §12.1 step 4 ABSTRACT stamps the
    // multi-member classes' representatives.
    assert!(!res.merged.is_empty());
    assert!(
        !res.abstracted.is_empty(),
        "abstract records class representatives: input={} merged={}",
        res.memories_input,
        res.merged.len()
    );
}

#[tokio::test]
async fn region_cycle_reuses_one_working_set_across_merge_candidates() {
    let storage = InMemoryStorage::new(ontology());

    // Several interchangeable anchors force the merge loop to process more
    // than one candidate. A larger foreign tenant makes any repeated org-wide
    // scan visible through the storage double's query counters.
    for index in 0..8usize {
        storage
            .upsert_memory(&mem_with_embedding(110_000 + index, Some(77), unit(0)))
            .await
            .unwrap();
    }
    for index in 0..256usize {
        let mut foreign = mem_with_embedding(120_000 + index, None, unit(index + 1));
        foreign.context.tenant_id = Some("other-org".into());
        foreign.context.project_id = Some("other-project".into());
        storage.upsert_memory(&foreign).await.unwrap();
    }

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "bounded-working-set".into(),
    );
    let result = engine
        .try_consolidate(&RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: 3,
        })
        .await
        .expect("cycle");

    assert!(
        result.merged.len() >= 2,
        "the regression must exercise multiple merge candidates"
    );
    let (memory_streams, relationship_streams, frontier_reads, attribute_reads) =
        storage.reasoning_query_counts();
    assert_eq!(
        memory_streams, 0,
        "a bounded region cycle must not fall back to full-store memory streams"
    );
    assert_eq!(
        relationship_streams, 0,
        "a bounded region cycle must not fall back to full-store relationship streams"
    );
    assert_eq!(frontier_reads, 0);
    assert_eq!(attribute_reads, 0);
    assert_eq!(
        storage.region_query_counts(),
        (1, 1),
        "all merge candidates must reuse one bounded regional working set"
    );
}

#[tokio::test]
async fn poison_consolidation_flags_regression_and_rolls_back() {
    let storage = InMemoryStorage::new(ontology());
    // A negative tolerance turns the R-Mcr3 guard into a tripwire: ANY
    // ΔR movement (even the normal post-merge improvement) registers as a
    // regression, so the cycle exercises the REAL rollback path — merged
    // rows are closed back via the fenced delete, not hand arithmetic.
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        -0.5, // poisoned tolerance (R-Mcr3 trips unconditionally)
        0.05,
        true, // rollback_on_regression
        "dreams-1".into(),
    );
    // Two duplicate groups so the cycle genuinely merges rows first.
    for g in 0..2usize {
        for d in 0..3usize {
            let mut emb = unit(g);
            emb[(g + d + 1) % 64] = 0.05;
            storage
                .upsert_memory(&mem_with_embedding(60_000 + g * 100 + d, Some(g), emb))
                .await
                .unwrap();
        }
    }
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let res = engine.try_consolidate(&region).await.expect("cycle");
    assert!(!res.merged.is_empty(), "the cycle merged duplicate rows");
    assert!(
        res.regression,
        "R-Mcr3 guard fired on the poisoned tolerance"
    );
    assert!(!res.hairball_regression, "ΔR guard is independent");
    // Rollback really ran: every merged row is restored to its live preimage.
    for id in &res.merged {
        let row = storage.get_memory(id).await.unwrap().expect("row present");
        assert!(
            row.valid_until.is_none(),
            "rollback restored merged row {id:?}"
        );
    }
}

#[tokio::test]
async fn injected_mid_cycle_failure_restores_exact_mixed_preimage() {
    use futures::StreamExt;
    let storage = InMemoryStorage::new(ontology());
    let target = mem_with_embedding(70_000, None, unit(20));
    let mut duplicate_a = mem_with_embedding(70_001, Some(1), unit(1));
    let mut duplicate_b = mem_with_embedding(70_002, Some(1), unit(1));
    duplicate_a.embedding.as_mut().unwrap().vector[2] = 0.01;
    duplicate_b.embedding.as_mut().unwrap().vector[3] = 0.01;
    let mut preclosed = mem_with_embedding(70_003, None, unit(30));
    preclosed.valid_until = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    let edge_a = relationship(duplicate_a.id, target.id, exocortex_kernel::kinds::SOLVES);
    let edge_b = relationship(duplicate_b.id, target.id, kind_named("Precedes"));
    let mut preclosed_edge = relationship(preclosed.id, target.id, kind_named("RelatedTo"));
    preclosed_edge.valid_until = preclosed.valid_until;
    storage
        .upsert_batch(
            &[target, duplicate_a, duplicate_b, preclosed.clone()],
            &[edge_a, edge_b, preclosed_edge.clone()],
        )
        .await
        .unwrap();
    let mut before_memories = std::collections::HashMap::new();
    let mut memories = storage.stream_all_memories().await;
    while let Some(row) = memories.next().await {
        let mut memory = row.unwrap();
        memory.lsn = LSN::new_local(0);
        before_memories.insert(memory.id, memory);
    }
    drop(memories);
    let mut before_relationships = std::collections::HashMap::new();
    let mut relationships = storage.stream_all_relationships().await;
    while let Some(row) = relationships.next().await {
        let mut relationship = row.unwrap();
        relationship.lsn = LSN::new_local(0);
        before_relationships.insert(relationship.id, relationship);
    }
    drop(relationships);

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "faulted".into(),
    )
    .with_cycle_fault_after(1)
    .with_lease_ttl(std::time::Duration::from_millis(60))
    .with_rollback_pause(std::time::Duration::from_millis(180));
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    assert!(engine.try_consolidate(&region).await.is_err());

    let mut after_memories = std::collections::HashMap::new();
    let mut memories = storage.stream_all_memories().await;
    while let Some(row) = memories.next().await {
        let mut memory = row.unwrap();
        memory.lsn = LSN::new_local(0);
        after_memories.insert(memory.id, memory);
    }
    drop(memories);
    let mut after_relationships = std::collections::HashMap::new();
    let mut relationships = storage.stream_all_relationships().await;
    while let Some(row) = relationships.next().await {
        let mut relationship = row.unwrap();
        relationship.lsn = LSN::new_local(0);
        after_relationships.insert(relationship.id, relationship);
    }
    assert_eq!(after_memories.len(), before_memories.len());
    for (id, before) in &before_memories {
        assert_eq!(
            serde_json::to_value(&after_memories[id]).unwrap(),
            serde_json::to_value(before).unwrap(),
            "memory {id:?} differs from its preimage"
        );
    }
    assert_eq!(after_relationships.len(), before_relationships.len());
    for (id, before) in &before_relationships {
        assert_eq!(
            serde_json::to_value(&after_relationships[id]).unwrap(),
            serde_json::to_value(before).unwrap(),
            "relationship {id:?} differs from its preimage"
        );
    }
    assert_eq!(
        after_memories[&preclosed.id].valid_until,
        preclosed.valid_until
    );
    assert_eq!(
        after_relationships[&preclosed_edge.id].valid_until,
        preclosed_edge.valid_until
    );

    let renewal_engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "renewal-failure".into(),
    )
    .with_lease_ttl(std::time::Duration::from_millis(60))
    .with_cycle_pause_after(1, std::time::Duration::from_millis(180))
    .with_renewal_failure_after(1);
    let error = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        renewal_engine.try_consolidate(&region),
    )
    .await
    .expect("renewal loss aborts before the original lease expires")
    .expect_err("unconfirmed owner renewal aborts the cycle");
    assert!(error.to_string().contains("renewal"));

    let mut restored_memories = std::collections::HashMap::new();
    let mut memories = storage.stream_all_memories().await;
    while let Some(row) = memories.next().await {
        let mut memory = row.unwrap();
        memory.lsn = LSN::new_local(0);
        restored_memories.insert(memory.id, memory);
    }
    let mut restored_relationships = std::collections::HashMap::new();
    let mut relationships = storage.stream_all_relationships().await;
    while let Some(row) = relationships.next().await {
        let mut relationship = row.unwrap();
        relationship.lsn = LSN::new_local(0);
        restored_relationships.insert(relationship.id, relationship);
    }
    assert_eq!(restored_memories.len(), before_memories.len());
    for (id, before) in &before_memories {
        assert_eq!(
            serde_json::to_value(&restored_memories[id]).unwrap(),
            serde_json::to_value(before).unwrap(),
            "memory {id:?} differs after renewal loss"
        );
    }
    assert_eq!(restored_relationships.len(), before_relationships.len());
    for (id, before) in &before_relationships {
        assert_eq!(
            serde_json::to_value(&restored_relationships[id]).unwrap(),
            serde_json::to_value(before).unwrap(),
            "relationship {id:?} differs after renewal loss"
        );
    }
}

#[tokio::test]
async fn similar_edges_derive_the_narrowest_endpoint_visibility() {
    let storage = InMemoryStorage::new(ontology());
    let cases = [
        (Visibility::Org, Visibility::Project, Visibility::Project),
        (Visibility::Team, Visibility::Org, Visibility::Team),
        (Visibility::Private, Visibility::Org, Visibility::Private),
        (Visibility::Team, Visibility::Project, Visibility::Project),
    ];
    let mut memories = Vec::new();
    let mut expected = Vec::new();
    let similar = kind_named("SimilarTo");
    for (case, (from_visibility, to_visibility, expected_visibility)) in
        cases.into_iter().enumerate()
    {
        let axis = case * 2;
        let mut from = mem_with_embedding(90_000 + case * 2, None, unit(axis));
        let mut to_vector = unit(axis);
        to_vector[axis] = 0.9;
        to_vector[axis + 1] = (1.0f32 - 0.9f32.powi(2)).sqrt();
        let mut to = mem_with_embedding(90_001 + case * 2, None, to_vector);
        from.visibility = from_visibility;
        to.visibility = to_visibility;
        expected.push((from.id, to.id, expected_visibility));
        memories.extend([from, to]);
    }
    storage.upsert_batch(&memories, &[]).await.unwrap();
    DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "visibility".into(),
    )
    .try_consolidate(&RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    })
    .await
    .unwrap();
    let mut rows = storage.stream_all_relationships().await;
    let mut relationships = Vec::new();
    while let Some(row) = rows.next().await {
        relationships.push(row.unwrap());
    }
    for (from, to, visibility) in expected {
        let relationship = relationships
            .iter()
            .find(|relationship| {
                relationship.kind == similar
                    && ((relationship.from == from && relationship.to == to)
                        || (relationship.from == to && relationship.to == from))
            })
            .expect("expected SimilarTo edge");
        assert_eq!(
            relationship.visibility, visibility,
            "SimilarTo visibility must be the narrower endpoint"
        );
    }
}

#[tokio::test]
async fn storage_stream_failures_abort_before_any_dreams_mutation() {
    for (memory_fault, relationship_fault) in [(Some(1), None), (None, Some(0))] {
        let storage = InMemoryStorage::new(ontology());
        let from = mem_with_embedding(95_000, None, unit(0));
        let mut vector = unit(0);
        vector[0] = 0.9;
        vector[1] = (1.0f32 - 0.9f32.powi(2)).sqrt();
        let to = mem_with_embedding(95_001, None, vector);
        storage.upsert_batch(&[from, to], &[]).await.unwrap();
        storage.fail_next_stream_after(memory_fault, relationship_fault);

        let engine = DreamsEngine::new(
            Arc::new(storage.clone_dyn()),
            DreamsTrigger::default(),
            0.01,
            0.05,
            true,
            "stream-failure".into(),
        );
        assert!(
            engine
                .try_consolidate(&RegionKey {
                    org: "*".into(),
                    project: "*".into(),
                    memory_type: 3,
                })
                .await
                .is_err(),
            "an authoritative stream error must abort the cycle"
        );
        let mut relationships = storage.stream_all_relationships().await;
        assert!(
            relationships.next().await.is_none(),
            "a truncated scan must not create SimilarTo edges"
        );
    }
}

#[tokio::test]
async fn merge_rewiring_trips_default_hairball_without_delta_r_regression() {
    let ontology = ontology();
    let storage = InMemoryStorage::new(ontology.clone());
    let source_a = mem_with_embedding(80_000, Some(2), unit(2));
    let mut source_b = mem_with_embedding(80_001, Some(2), unit(2));
    source_b.embedding.as_mut().unwrap().vector[3] = 0.01;
    let targets: Vec<_> = (0..4)
        .map(|index| {
            let mut memory = mem_with_embedding(81_000 + index, None, unit(20 + index));
            memory.embedding = None;
            memory
        })
        .collect();
    let kinds: Vec<_> = ontology
        .kinds_by_id
        .keys()
        .copied()
        .filter(|kind| *kind != RelKindId(0x8000_0024))
        .take(34)
        .collect();
    assert_eq!(kinds.len(), 34);
    let mut edges = Vec::new();
    for (index, kind) in kinds.into_iter().enumerate() {
        let from = if index < 17 { source_a.id } else { source_b.id };
        edges.push(relationship(from, targets[index % targets.len()].id, kind));
    }
    let mut memories = vec![source_a, source_b];
    memories.extend(targets);
    storage.upsert_batch(&memories, &edges).await.unwrap();
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "hairball".into(),
    );
    let result = engine
        .try_consolidate(&RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: 3,
        })
        .await
        .unwrap();
    assert!(!result.regression, "hairball guard is independent of ΔR");
    assert!(result.hairball_regression);
    assert!(
        result.sparsity_after.hairball_fraction > result.sparsity_before.hairball_fraction + 0.05
    );
    assert!(!result.rewired.is_empty(), "merge must rewire real edges");
}

#[tokio::test]
async fn lease_race_two_engines_one_region() {
    let storage = InMemoryStorage::new(ontology());
    let a = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "A".into(),
    );
    let region = RegionKey {
        org: "race".into(),
        project: "p".into(),
        memory_type: 3,
    };
    for i in 0..4 {
        let mut memory = mem_with_embedding(i, None, unit(i));
        memory.context.tenant_id = Some("race".into());
        storage.upsert_memory(&memory).await.unwrap();
    }
    // InMemoryStorage grants leases permissively; the LIVE race is asserted
    // in exocortex-cluster's chaos test against FalkorDB. Here: the engine
    // stamps the lease epoch it holds on every result.
    let res = a.try_consolidate(&region).await.expect("cycle under lease");
    assert!(
        res.lease_epoch >= 1,
        "R-C1: cycles run under an active lease"
    );
}

#[tokio::test]
async fn named_project_region_requires_an_active_memory_in_the_same_org() {
    let storage = InMemoryStorage::new(ontology());
    let mut other_org = mem_with_embedding(91, None, unit(1));
    other_org.context.tenant_id = Some("other-org".into());
    other_org.context.project_id = Some("named-project".into());
    storage.upsert_memory(&other_org).await.unwrap();
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-region-check".into(),
    );
    let region = RegionKey {
        org: "org".into(),
        project: "named-project".into(),
        memory_type: 3,
    };
    let error = engine.try_consolidate(&region).await.unwrap_err();
    assert!(error.to_string().contains("unknown project region"));

    let lease_key = exocortex_storage::LeaseKey::Dreams {
        org: "org".into(),
        region: "named-project:3".into(),
    };
    storage
        .acquire_lease(&lease_key, std::time::Duration::from_secs(1))
        .await
        .expect("invalid region was rejected before lease acquisition");
}

#[test]
fn trigger_model_is_write_counter_driven_only() {
    let t = DreamsTrigger::default();
    // Below every threshold: never fires (no clock-only path).
    assert!(!t.should_fire(&RegionWriteCounters {
        memories_since_last_cycle: 0,
        edges_since_last_cycle: 0,
        seconds_since_last_cycle: 0,
    }));
    // Write counters fire once past interval.
    assert!(t.should_fire(&RegionWriteCounters {
        memories_since_last_cycle: t.memory_threshold,
        edges_since_last_cycle: 0,
        seconds_since_last_cycle: t.min_interval_hours as u64 * 3600 + 1,
    }));
}

#[tokio::test]
async fn batched_local_writes_preserve_exact_memory_and_edge_counts() {
    let storage = InMemoryStorage::new(ontology());
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-batch-counter".into(),
    );
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };

    engine.on_writes(region.clone(), 7, 5).await.unwrap();

    let counters = *engine.counters.get(&region).unwrap();
    assert_eq!(counters.memories_since_last_cycle, 7);
    assert_eq!(counters.edges_since_last_cycle, 5);
}

#[test]
fn discovery_is_a_proposal_not_an_edge() {
    let d = Discovery {
        id: uuid::Uuid::new_v4(),
        kind: DiscoveryKind::CrossDomain,
        endpoints: (MemoryId::new_v7(), MemoryId::new_v7()),
        quality: 0.9,
        via_types: (0, 3),
        discovery_cycle_id: "dream:x".into(),
        discovered_at: chrono::Utc::now(),
    };
    assert_eq!(d.rate_quality(), 0.9, "R-Dr6: quality computed once");
    // R-T16/R-Dr1: discovery provenance is Proposed, never a persisted edge.
    let prov = exocortex_dreams::discovery_provenance(d.quality);
    assert!(matches!(prov, Provenance::Proposed { .. }));
}

#[test]
fn mcr2_cross_model_comparison_is_prohibited() {
    let e = MCR2Engine::default();
    let matching = vec![
        MemoryWithEmbedding {
            id: MemoryId::new_v7(),
            class: 1,
            visibility: Visibility::Org,
            embedding: stamped(unit(1)),
        },
        MemoryWithEmbedding {
            id: MemoryId::new_v7(),
            class: 2,
            visibility: Visibility::Org,
            embedding: stamped(unit(2)),
        },
    ];
    let v = e.compute(&matching).expect("compute");
    assert_eq!(
        v.embedding_model,
        exocortex_dreams::mcr2::EmbeddingModelId::bge_small()
    );
    let mut mixed = matching;
    mixed[1].embedding.model.version = "v2".into();
    assert!(matches!(
        e.compute(&mixed),
        Err(exocortex_dreams::mcr2::MCR2Error::CrossModelComparison)
    ));
}

#[test]
fn sparsity_detects_hairballs() {
    use exocortex_dreams::mcr2::compute_sparsity;
    let nodes: Vec<_> = (0..10).map(|i| (MemoryId([i; 16]), 3u8)).collect();
    // One node with 40 edges: hairball fraction 0.1.
    let edges: Vec<_> = (0..40)
        .map(|j| {
            (
                nodes[0].0,
                nodes[1 + (j % 9) as usize].0,
                0u32,
                0u64,
                0.5f32,
            )
        })
        .collect();
    let s = compute_sparsity(&nodes, &edges, 32, None);
    assert!(s.hairball_fraction > 0.0, "hairball detected");
    assert_eq!(s.n_edges, 40);
}

#[test]
fn sparsity_excludes_similarity_edges_from_out_degrees() {
    use exocortex_dreams::mcr2::compute_sparsity;
    let nodes: Vec<_> = (0..2).map(|i| (MemoryId([i; 16]), 3u8)).collect();
    // 40 SimilarTo edges from node 0: excluded entirely (§11.6.1).
    let similar: Vec<_> = (0..40)
        .map(|_| (nodes[0].0, nodes[1].0, 0x8000_0024u32, 0u64, 0.9f32))
        .collect();
    let s = compute_sparsity(
        &nodes,
        &similar,
        32,
        Some(exocortex_kernel::RelKindId(0x8000_0024)),
    );
    assert_eq!(
        s.hairball_fraction, 0.0,
        "similarity edges never make hairballs"
    );
    assert_eq!(s.avg_out_degree, 0.0);
    // Without the exclusion flag the same edges would count.
    let s2 = compute_sparsity(&nodes, &similar, 32, None);
    assert!(s2.hairball_fraction > 0.0);
}

const _UNUSED: Option<PruneReason> = None;

/// §11.2 closed form: R(Z) = ½ log det(I + (d/(n·ε²))·ZZᵀ). With ε=0.5,
/// d=2, and the two orthogonal unit rows e1,e2, ZZᵀ=I so alpha=d/(n ε²)=4
/// and R = ½ log det(5I) = ln 5. The old off-by-1/n bug (alpha=d/(n²ε²)=2)
/// would yield ln 3 instead.
#[test]
fn mcr2_log_det_matches_closed_form() {
    let e = MCR2Engine { epsilon: 0.5 };
    let rows = vec![
        MemoryWithEmbedding {
            id: MemoryId::new_v7(),
            class: 1,
            visibility: Visibility::Org,
            embedding: stamped(vec![1.0, 0.0]),
        },
        MemoryWithEmbedding {
            id: MemoryId::new_v7(),
            class: 2,
            visibility: Visibility::Org,
            embedding: stamped(vec![0.0, 1.0]),
        },
    ];
    let v = e.compute(&rows).expect("compute");
    let ln5 = 5.0f64.ln();
    assert!(
        (v.total_rate as f64 - ln5).abs() < 1e-4,
        "total rate must be ln 5 = {ln5}, got {}",
        v.total_rate
    );
    // Single-row class {1}: ZZᵀ = e1e1ᵀ, alpha = d/(1·ε²) = 8, so
    // R = ½ log det(diag(9,1)) = ½ ln 9 — the per-class collapse.
    let half_ln9 = 0.5 * 9.0f64.ln();
    for (class, rate) in &v.class_rates {
        assert!(
            (*rate as f64 - half_ln9).abs() < 1e-4,
            "class {class} rate must be ½ ln 9 = {half_ln9}, got {rate}"
        );
    }
    // Compact = ½·½ln9 + ½·½ln9 = ½ ln 9; ΔR = ln 5 − ½ ln 9 ≈ 0.5108.
    assert!((v.compact_rate as f64 - half_ln9).abs() < 1e-4);
    let expected_delta = ln5 - half_ln9;
    assert!(
        (v.delta_r as f64 - expected_delta).abs() < 1e-4,
        "delta_r must be {expected_delta}, got {}",
        v.delta_r
    );
    assert!(v.delta_r > 0.0, "orthogonal classes carry positive ΔR");
}

/// §23 #12: the Transitive finder proposes `a->c` for open two-hop paths,
/// excludes pairs with a direct edge, and never proposes over derived
/// path edges (R4/R5 closures, R-Dr7); proposals never write edges.
#[tokio::test]
async fn transitive_finder_proposes_only_open_indirect_pairs() {
    use exocortex_kernel::{RelKindId, Relationship, RelationshipProperties};
    let storage = InMemoryStorage::new(ontology());
    let a = MemoryId([1; 16]);
    let b = MemoryId([2; 16]);
    let c = MemoryId([3; 16]);
    let d = MemoryId([4; 16]);
    for (i, id) in [(1usize, a), (2, b), (3, c), (4, d)] {
        let mut m = mem_with_embedding(i, None, unit(i));
        m.id = id;
        storage.upsert_memory(&m).await.unwrap();
    }
    let kind = RelKindId(5); // any registered kind id; provenance is the axis under test
    let mk = |from, to, derived| Relationship {
        id: exocortex_kernel::RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: Visibility::Org,
        provenance: if derived {
            Provenance::Derived {
                rule_id: "R4".into(),
                evidence: vec![],
            }
        } else {
            Provenance::Asserted {
                author: "t".into(),
                producer_kind: None,
            }
        },
        properties: RelationshipProperties {
            strength: 0.5,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    };
    // a->b->c open asserted path: proposes (a, c).
    storage.upsert_relationship(&mk(a, b, false)).await.unwrap();
    storage.upsert_relationship(&mk(b, c, false)).await.unwrap();
    // Direct edge exists for (a, b) — no proposal for it beyond the path itself.
    storage.upsert_relationship(&mk(a, b, false)).await.unwrap();
    // Derived two-hop path a->b? make d-path derived: b->d derived, d->c? simpler:
    // b->d derived, d... only 2-hop a->b->c qualifies. Add derived path b->d->? skip;
    // assert exclusion via a second path: d->a derived and a->? none.
    storage.upsert_relationship(&mk(b, d, true)).await.unwrap();
    storage.upsert_relationship(&mk(d, c, false)).await.unwrap();

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "disc".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let proposals = engine.run_discovery(&region).await.unwrap();
    let pairs: Vec<_> = proposals.iter().map(|d| d.endpoints).collect();
    assert!(
        pairs.contains(&(a, c)),
        "open two-hop path a->b->c proposes (a,c): {pairs:?}"
    );
    // The derived-first-hop path (b->d derived) never proposes (b,c).
    assert!(
        !pairs.contains(&(b, c)),
        "derived path edges are excluded (R-Dr7): {pairs:?}"
    );
    // §23 #11: quality on the surface equals the finder's stamped value.
    for d in &proposals {
        assert_eq!(d.quality, DiscoveryKind::Transitive.default_quality());
        assert_eq!(d.quality, d.rate_quality());
    }
    // R-Dr1: proposals never became edges.
    let open: Vec<_> = {
        use futures::StreamExt;
        let mut rs = storage.stream_all_relationships().await;
        let mut v = Vec::new();
        while let Some(Ok(r)) = rs.next().await {
            if r.valid_until.is_none() {
                v.push((r.from, r.to));
            }
        }
        v
    };
    assert!(!open.contains(&(a, c)), "proposal did not write an edge");

    let discovery = proposals
        .iter()
        .find(|proposal| proposal.endpoints == (a, c))
        .unwrap();
    engine
        .issue_discovery_proposal(
            discovery,
            &region,
            kind,
            Visibility::Org,
            VisibilityContext {
                user_id: "u".into(),
                org_id: "o".into(),
                project_ids: vec!["p".into()].into(),
                team_ids: Default::default(),
                max_visibility: Visibility::Org,
            },
        )
        .await
        .unwrap();
    assert!(
        !engine
            .pending_discoveries()
            .iter()
            .any(|pending| pending.id == discovery.id),
        "persisted proposals leave the bounded in-memory pending set"
    );
}

#[tokio::test]
async fn pruning_scans_once_and_records_only_closed_rows_in_the_region() {
    let storage = InMemoryStorage::new(ontology());
    for i in 0..2 {
        storage
            .upsert_memory(&mem_with_embedding(300 + i, None, unit(i)))
            .await
            .unwrap();
    }
    let mut closed = mem_with_embedding(310, None, unit(10));
    closed.valid_until = Some(chrono::Utc::now());
    storage.upsert_memory(&closed).await.unwrap();
    let mut foreign = mem_with_embedding(311, None, unit(11));
    foreign.valid_until = Some(chrono::Utc::now());
    foreign.context.project_id = Some("other".into());
    storage.upsert_memory(&foreign).await.unwrap();

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "prune-once".into(),
    );
    let result = engine
        .try_consolidate(&RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: 3,
        })
        .await
        .unwrap();
    assert_eq!(result.pruned, vec![(closed.id, PruneReason::Redundant)]);
}

fn discovery_test_relationship(from: MemoryId, to: MemoryId) -> exocortex_kernel::Relationship {
    use exocortex_kernel::{RelKindId, RelationshipProperties};
    let kind = RelKindId(5);
    exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "production-cycle-test".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.5,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    }
}

#[tokio::test]
async fn discovery_never_crosses_an_out_of_region_intermediate() {
    let storage = InMemoryStorage::new(ontology());
    let a = MemoryId([21; 16]);
    let hidden_b = MemoryId([22; 16]);
    let c = MemoryId([23; 16]);
    for (index, id, project) in [(0, a, "p"), (1, hidden_b, "other"), (2, c, "p")] {
        let mut memory = mem_with_embedding(index, None, unit(index));
        memory.id = id;
        memory.context.project_id = Some(project.into());
        storage.upsert_memory(&memory).await.unwrap();
    }
    storage
        .upsert_relationship(&discovery_test_relationship(a, hidden_b))
        .await
        .unwrap();
    storage
        .upsert_relationship(&discovery_test_relationship(hidden_b, c))
        .await
        .unwrap();
    let engine = DreamsEngine::new(
        Arc::new(storage),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "scoped-discovery".into(),
    );

    let discoveries = engine
        .run_discovery(&RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: 3,
        })
        .await
        .unwrap();

    assert!(
        discoveries.is_empty(),
        "an out-of-region intermediate must not influence a regional proposal"
    );
}

#[tokio::test]
async fn production_cycle_runs_discovery_after_consolidation() {
    let storage = InMemoryStorage::new(ontology());
    let [a, b, c] = [MemoryId([11; 16]), MemoryId([12; 16]), MemoryId([13; 16])];
    for (i, id) in [a, b, c].into_iter().enumerate() {
        let mut memory = mem_with_embedding(i + 200, None, unit(i));
        memory.id = id;
        memory.embedding = None;
        storage.upsert_memory(&memory).await.unwrap();
    }
    storage
        .upsert_relationship(&discovery_test_relationship(a, b))
        .await
        .unwrap();
    storage
        .upsert_relationship(&discovery_test_relationship(b, c))
        .await
        .unwrap();

    let engine = Arc::new(DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "production-discovery".into(),
    ));
    let runner = tokio::spawn(engine.clone().run());
    engine.notify(RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if engine
                .pending_discoveries()
                .iter()
                .any(|discovery| discovery.endpoints == (a, c))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the production run loop must execute discovery after consolidation");
    runner.abort();
}

#[tokio::test]
async fn successful_fire_replay_is_settled_without_repeating_graph_mutations() {
    let storage = InMemoryStorage::new(ontology());
    let [a, b, c] = [MemoryId([21; 16]), MemoryId([22; 16]), MemoryId([23; 16])];
    for (i, id) in [a, b, c].into_iter().enumerate() {
        let mut memory = mem_with_embedding(i + 300, None, unit(i));
        memory.id = id;
        memory.embedding = None;
        storage.upsert_memory(&memory).await.unwrap();
    }
    let first_edge = discovery_test_relationship(a, b);
    storage.upsert_relationship(&first_edge).await.unwrap();
    storage
        .upsert_relationship(&discovery_test_relationship(b, c))
        .await
        .unwrap();
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "success-replay".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };

    assert!(engine
        .try_consolidate_once_for_testing(&region, "success-before-ack")
        .await
        .unwrap()
        .is_some());
    let evidence_after_success = storage
        .get_relationship(&first_edge.id)
        .await
        .unwrap()
        .unwrap()
        .properties
        .evidence_count;
    let discoveries_after_success = storage.list_discoveries("o", 100).await.unwrap();
    assert!(!discoveries_after_success.is_empty());

    assert!(engine
        .try_consolidate_once_for_testing(&region, "newer-success")
        .await
        .unwrap()
        .is_some());
    let evidence_after_newer_success = storage
        .get_relationship(&first_edge.id)
        .await
        .unwrap()
        .unwrap()
        .properties
        .evidence_count;
    let discoveries_after_newer_success = storage.list_discoveries("o", 100).await.unwrap();

    assert!(engine
        .try_consolidate_once_for_testing(&region, "success-before-ack")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        storage
            .get_relationship(&first_edge.id)
            .await
            .unwrap()
            .unwrap()
            .properties
            .evidence_count,
        evidence_after_newer_success,
        "an older successful fire must remain settled after a newer cycle"
    );
    assert_eq!(
        storage.list_discoveries("o", 100).await.unwrap(),
        discoveries_after_newer_success,
        "an older successful fire must preserve discovery identity after a newer cycle"
    );
    assert!(evidence_after_newer_success >= evidence_after_success);
    assert!(discoveries_after_newer_success.len() >= discoveries_after_success.len());
}

#[tokio::test]
async fn discovery_budget_selects_the_same_bounded_pairs() {
    let storage = InMemoryStorage::new(ontology());
    for chain in 0..20u8 {
        let a = MemoryId([chain * 3 + 1; 16]);
        let b = MemoryId([chain * 3 + 2; 16]);
        let c = MemoryId([chain * 3 + 3; 16]);
        for (i, id) in [a, b, c].into_iter().enumerate() {
            let mut memory = mem_with_embedding(chain as usize * 3 + i, None, unit(i));
            memory.id = id;
            storage.upsert_memory(&memory).await.unwrap();
        }
        storage
            .upsert_relationship(&discovery_test_relationship(a, b))
            .await
            .unwrap();
        storage
            .upsert_relationship(&discovery_test_relationship(b, c))
            .await
            .unwrap();
    }
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "bounded-discovery".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let first: Vec<_> = engine
        .run_discovery(&region)
        .await
        .unwrap()
        .into_iter()
        .map(|proposal| proposal.endpoints)
        .collect();
    let second: Vec<_> = engine
        .run_discovery(&region)
        .await
        .unwrap()
        .into_iter()
        .map(|proposal| proposal.endpoints)
        .collect();
    assert_eq!(first.len(), exocortex_dreams::MAX_DISCOVERIES_PER_CYCLE);
    assert_eq!(
        engine.pending_discoveries().len(),
        exocortex_dreams::MAX_DISCOVERIES_PER_CYCLE,
        "a later cycle replaces rather than accumulates pending proposals"
    );
    assert_eq!(
        first, second,
        "the capped proposal set must be reproducible"
    );
}

#[tokio::test]
async fn rollback_preserves_a_concurrent_non_cycle_memory_version() {
    let storage = InMemoryStorage::new(ontology());
    let target = mem_with_embedding(95_000, None, unit(20));
    let mut duplicate_a = mem_with_embedding(95_001, Some(1), unit(1));
    let mut duplicate_b = mem_with_embedding(95_002, Some(1), unit(1));
    duplicate_a.embedding.as_mut().unwrap().vector[2] = 0.01;
    duplicate_b.embedding.as_mut().unwrap().vector[3] = 0.01;
    storage
        .upsert_batch(&[target, duplicate_a.clone(), duplicate_b.clone()], &[])
        .await
        .unwrap();

    duplicate_a.title = "concurrent-a".into();
    duplicate_b.title = "concurrent-b".into();
    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "conditional-rollback".into(),
    )
    .with_cycle_fault_after(1)
    .with_rollback_concurrent_memories(vec![duplicate_a.clone(), duplicate_b.clone()]);
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };

    assert!(engine.try_consolidate(&region).await.is_err());
    assert_eq!(
        storage
            .get_memory(&duplicate_a.id)
            .await
            .unwrap()
            .unwrap()
            .title,
        "concurrent-a"
    );
    assert_eq!(
        storage
            .get_memory(&duplicate_b.id)
            .await
            .unwrap()
            .unwrap()
            .title,
        "concurrent-b"
    );
}

#[tokio::test]
async fn successor_recovers_durable_cycle_journal_without_overwriting_concurrent_version() {
    let storage = InMemoryStorage::new(ontology());
    let target = mem_with_embedding(96_000, None, unit(20));
    let mut duplicate_a = mem_with_embedding(96_001, Some(1), unit(1));
    let mut duplicate_b = mem_with_embedding(96_002, Some(1), unit(1));
    duplicate_a.embedding.as_mut().unwrap().vector[2] = 0.01;
    duplicate_b.embedding.as_mut().unwrap().vector[3] = 0.01;
    storage
        .upsert_batch(&[target, duplicate_a, duplicate_b], &[])
        .await
        .unwrap();
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let lease_key = LeaseKey::Dreams {
        org: region.org.clone(),
        region: "p:3".into(),
    };
    let crashed = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "crashed-owner".into(),
    )
    .with_cycle_crash_after(1);
    assert!(crashed.try_consolidate(&region).await.is_err());

    let journal = storage
        .get_active_cycle_journal(&lease_key)
        .await
        .unwrap()
        .expect("crashed mutation leaves an active durable journal");
    let concurrently_written_id = *journal
        .restore
        .owned_memory_lsns
        .keys()
        .next()
        .expect("the first merge wrote a memory");
    let mut concurrent = storage
        .get_memory(&concurrently_written_id)
        .await
        .unwrap()
        .unwrap();
    concurrent.title = "successor-concurrent-version".into();
    concurrent.valid_until = None;
    storage.upsert_memory(&concurrent).await.unwrap();

    let successor = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        true,
        "successor".into(),
    );
    successor
        .recover_active_cycle_for_test(&region)
        .await
        .unwrap();
    assert!(storage
        .get_active_cycle_journal(&lease_key)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        storage
            .get_memory(&concurrently_written_id)
            .await
            .unwrap()
            .unwrap()
            .title,
        "successor-concurrent-version",
        "recovery compensates only the crashed cycle's owned LSN"
    );
}
