//! `InMemoryStorage` property tests (§6.7 step 3): 10k random memories
//! round-trip, LSN monotonicity, and bi-temporal semantics.

use chrono::{Duration, Utc};
use exocortex_kernel::{
    EntityId, Memory, MemoryContext, MemoryId, Provenance, RelKindId, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{
    memory_visible, AuditEvent, DiscoveryAcceptance, DiscoveryProposal, DiscoveryRecord,
    FencedRestore, InMemoryStorage, IngestBatchKey, IngestCommitOutcome, IngestRegionDelta,
    LeaseKey, MemoryFilter, PostIngestEffect, RegionKey, Storage, StorageError, VisibilityContext,
};
use proptest::prelude::*;
use std::sync::Arc;

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn visibility(vis: u8) -> Visibility {
    match vis % 5 {
        0 => Visibility::Private,
        1 => Visibility::Project,
        2 => Visibility::Team,
        3 => Visibility::Org,
        _ => Visibility::Public,
    }
}

fn base_memory(title: String, content: String, mt: u8, vis: u8) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: mt % 13,
        title: title.into(),
        content,
        summary: None,
        tags: Default::default(),
        visibility: visibility(vis),
        provenance: Provenance::Asserted {
            author: "prop".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
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
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn base_relationship(from: MemoryId, to: MemoryId) -> Relationship {
    let now = Utc::now();
    Relationship {
        id: RelationshipId::derive(from, RelKindId(1), to, None),
        kind: RelKindId(1),
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "prop".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.8,
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
    }
}

#[tokio::test]
async fn stable_operation_key_appends_derived_batch_once() {
    let storage = InMemoryStorage::new(ontology());
    let left = base_memory("left".into(), "left".into(), 1, 3);
    let right = base_memory("right".into(), "right".into(), 1, 3);
    storage
        .upsert_batch(&[left.clone(), right.clone()], &[])
        .await
        .unwrap();
    let relationship = base_relationship(left.id, right.id);
    assert!(storage
        .upsert_batch_once("reasoning:effect-1", &[], &[relationship.clone()])
        .await
        .unwrap());
    assert!(!storage
        .upsert_batch_once("reasoning:effect-1", &[], &[relationship.clone()])
        .await
        .unwrap());
    assert_eq!(storage.relationship_history(&relationship.id).len(), 1);
}

#[test]
fn tenantless_and_foreign_rows_fail_closed() {
    let mut row = base_memory("row".into(), "content".into(), 1, 3);
    row.context.tenant_id = None;
    let vc = VisibilityContext {
        org_id: "org".into(),
        user_id: "user".into(),
        max_visibility: Visibility::Org,
        ..Default::default()
    };
    assert!(!memory_visible(&row, &vc));
    row.context.tenant_id = Some("foreign".into());
    assert!(!memory_visible(&row, &vc));
    row.context.tenant_id = Some("org".into());
    assert!(memory_visible(&row, &vc));
}

#[tokio::test]
async fn fenced_restore_preserves_precycle_history_and_removes_failed_versions() {
    let storage = InMemoryStorage::new(ontology());
    let original = base_memory("original".into(), "content".into(), 3, 3);
    storage.upsert_memory(&original).await.unwrap();
    let preimage = storage.get_memory(&original.id).await.unwrap().unwrap();
    let lease = storage
        .acquire_lease(
            &LeaseKey::Dreams {
                org: "org".into(),
                region: "*:*".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    let mut failed_cycle = preimage.clone();
    failed_cycle.title = "failed-cycle".into();
    let cycle_commit = storage
        .upsert_batch_fenced(&[failed_cycle], &[], &lease)
        .await
        .unwrap();
    storage
        .restore_fenced(
            &FencedRestore {
                memories: vec![preimage.clone()],
                owned_memory_lsns: cycle_commit.memory_lsns,
                ..Default::default()
            },
            &lease,
        )
        .await
        .unwrap();

    let history = storage.memory_history(&preimage.id);
    assert_eq!(
        history.len(),
        1,
        "only the exact pre-cycle assertion remains"
    );
    assert_eq!(history[0].title.as_str(), "original");
}

#[tokio::test]
async fn relationship_assertion_history_is_bitemporal() {
    let storage = InMemoryStorage::new(ontology());
    let from = base_memory("from".into(), "content".into(), 3, 3);
    let to = base_memory("to".into(), "content".into(), 3, 3);
    storage
        .upsert_batch(&[from.clone(), to.clone()], &[])
        .await
        .unwrap();
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = Utc::now() - Duration::hours(1);
    let mut first = base_relationship(from.id, to.id);
    first.valid_from = t0;
    first.valid_until = Some(t1);
    first.recorded_at = t0;
    storage.upsert_relationship(&first).await.unwrap();
    let mut second = first.clone();
    second.valid_from = t1;
    second.valid_until = None;
    second.recorded_at = t1;
    second.properties.evidence_count = 2;
    storage.upsert_relationship(&second).await.unwrap();

    assert_eq!(
        storage.get_state_at(t0).await.unwrap().relationship_count,
        2
    );
    assert_eq!(
        storage.get_state_at(t1).await.unwrap().relationship_count,
        2
    );
    let history = storage.relationship_history(&first.id);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].properties.evidence_count, 1);
    assert_eq!(history[1].properties.evidence_count, 2);
}

fn audit(action: &str) -> AuditEvent {
    AuditEvent {
        action: action.into(),
        actor: "user".into(),
        org_id: "org".into(),
        input_digest: [7; 32],
        output_ids: Default::default(),
        fingerprint: [9; 32],
        lease_epoch: None,
        recorded_at: Utc::now(),
    }
}

fn caller_scope() -> VisibilityContext {
    VisibilityContext {
        org_id: "org".into(),
        user_id: "user".into(),
        project_ids: ["project".into()].into_iter().collect(),
        max_visibility: Visibility::Org,
        ..Default::default()
    }
}

#[tokio::test]
async fn audited_memory_and_discovery_edges_append_history() {
    let storage = InMemoryStorage::new(ontology());
    let mut memory = base_memory("before".into(), "content".into(), 3, 3);
    memory.context.user_id = Some("user".into());
    storage.upsert_memory(&memory).await.unwrap();
    let mut promoted = memory.clone();
    promoted.title = "after".into();
    promoted.recorded_at += Duration::seconds(1);
    storage
        .upsert_memory_audited(&promoted, &audit("promote_visibility"))
        .await
        .unwrap();
    let memory_history = storage.memory_history(&memory.id);
    assert_eq!(memory_history.len(), 2);
    assert_eq!(memory_history[0].title.as_str(), "before");
    assert_eq!(memory_history[1].title.as_str(), "after");

    let mut to = base_memory("to".into(), "content".into(), 3, 3);
    to.context.user_id = Some("user".into());
    storage.upsert_memory(&to).await.unwrap();
    let relationship = base_relationship(memory.id, to.id);
    storage.upsert_relationship(&relationship).await.unwrap();
    let discovery_id = "history-discovery";
    let region = RegionKey {
        org: "org".into(),
        project: "*".into(),
        memory_type: memory.memory_type,
    };
    storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: discovery_id.into(),
            region: region.clone(),
            from: memory.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "cycle".into(),
            discovered_at: Utc::now(),
        })
        .await
        .unwrap();
    let proposal = DiscoveryProposal {
        discovery_id: discovery_id.into(),
        region: region.clone(),
        from: memory.id,
        to: to.id,
        kind: relationship.kind,
        proposed_visibility: relationship.visibility,
        caller_scope: caller_scope(),
        issued_at: Utc::now(),
    };
    storage.create_discovery_proposal(&proposal).await.unwrap();
    let mut accepted = relationship.clone();
    accepted.properties.evidence_count = 2;
    accepted.recorded_at += Duration::seconds(2);
    storage
        .accept_discovery(&DiscoveryAcceptance {
            discovery_id: discovery_id.into(),
            region,
            caller_scope: caller_scope(),
            relationship: accepted,
            audit: audit("accept_discovery"),
        })
        .await
        .unwrap();
    let relationship_history = storage.relationship_history(&relationship.id);
    assert_eq!(relationship_history.len(), 2);
    assert_eq!(relationship_history[0].properties.evidence_count, 1);
    assert_eq!(relationship_history[1].properties.evidence_count, 2);
}

#[tokio::test]
async fn discovery_issue_retires_record_and_consumed_reissue_is_rejected() {
    let storage = InMemoryStorage::new(ontology());
    let from = base_memory("from".into(), "content".into(), 3, 3);
    let to = base_memory("to".into(), "content".into(), 3, 3);
    storage
        .upsert_batch(&[from.clone(), to.clone()], &[])
        .await
        .unwrap();
    let id = "retired-discovery";
    let region = RegionKey {
        org: "org".into(),
        project: "*".into(),
        memory_type: from.memory_type,
    };
    let record = DiscoveryRecord {
        discovery_id: id.into(),
        region: region.clone(),
        from: from.id,
        to: to.id,
        discovery_type: "transitive".into(),
        quality: 0.6,
        via_types: [1, 2],
        discovery_cycle_id: "cycle".into(),
        discovered_at: Utc::now(),
    };
    storage.store_discovery(&record).await.unwrap();
    storage.store_discovery(&record).await.unwrap();
    let mut conflicting = record.clone();
    conflicting.quality = 0.9;
    assert!(matches!(
        storage.store_discovery(&conflicting).await,
        Err(StorageError::ProposalMismatch)
    ));
    let relationship = base_relationship(from.id, to.id);
    let proposal = DiscoveryProposal {
        discovery_id: id.into(),
        region: region.clone(),
        from: from.id,
        to: to.id,
        kind: relationship.kind,
        proposed_visibility: relationship.visibility,
        caller_scope: caller_scope(),
        issued_at: record.discovered_at,
    };
    storage.create_discovery_proposal(&proposal).await.unwrap();
    storage.create_discovery_proposal(&proposal).await.unwrap();
    assert!(matches!(
        storage.store_discovery(&record).await,
        Err(StorageError::ProposalMismatch)
    ));
    assert!(storage.get_discovery(id).await.unwrap().is_none());
    assert!(storage
        .list_discoveries("org", 10)
        .await
        .unwrap()
        .is_empty());
    storage
        .accept_discovery(&DiscoveryAcceptance {
            discovery_id: id.into(),
            region,
            caller_scope: caller_scope(),
            relationship,
            audit: audit("accept_discovery"),
        })
        .await
        .unwrap();
    assert!(matches!(
        storage.create_discovery_proposal(&proposal).await,
        Err(StorageError::ProposalNotFound)
    ));
    assert!(matches!(
        storage.store_discovery(&record).await,
        Err(StorageError::ProposalMismatch)
    ));
}

#[tokio::test]
async fn discovery_persistence_precedes_one_available_invalidation() {
    use futures::StreamExt;
    let storage = InMemoryStorage::new(ontology());
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let mut feed = storage.subscribe_invalidations(&region).await.unwrap();
    let record = DiscoveryRecord {
        discovery_id: "available-discovery".into(),
        region,
        from: MemoryId::new_v7(),
        to: MemoryId::new_v7(),
        discovery_type: "transitive".into(),
        quality: 0.6,
        via_types: [1, 2],
        discovery_cycle_id: "cycle".into(),
        discovered_at: Utc::now(),
    };
    storage.store_discovery(&record).await.unwrap();
    let invalidation = feed.next().await.unwrap().unwrap();
    match invalidation {
        exocortex_storage::Invalidation::DiscoveryAvailable {
            record: published,
            lsn,
        } => {
            assert_eq!(published, record);
            assert!(lsn > 0);
            assert_eq!(
                storage.get_discovery("available-discovery").await.unwrap(),
                Some(record.clone())
            );
        }
        other => panic!("expected DiscoveryAvailable, got {other:?}"),
    }
    storage.store_discovery(&record).await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), feed.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn paused_invalidation_subscriber_observes_overflow_as_an_error() {
    use futures::StreamExt;

    let storage = InMemoryStorage::new(ontology());
    let region = RegionKey {
        org: "*".into(),
        project: "*".into(),
        memory_type: 0,
    };
    let mut feed = storage.subscribe_invalidations(&region).await.unwrap();

    // The in-memory feed holds 4096 entries. Do not poll the subscriber while
    // overflowing it: this models a paused production consumer precisely.
    for sequence in 0..=4096 {
        let mut memory = base_memory(format!("lag-{sequence}"), "payload".into(), 0, 3);
        memory.id = MemoryId::new_v7();
        storage.upsert_batch(&[memory], &[]).await.unwrap();
    }

    let error = feed
        .next()
        .await
        .expect("lagged feed remains open")
        .expect_err("overflow must force a subscriber resync");
    assert!(
        matches!(error, StorageError::Backend(message) if message.contains("lagged")),
        "lag must be surfaced as a storage-feed error"
    );
}

#[tokio::test]
async fn relationships_in_region_is_exact_ordered_and_fail_closed_at_limit() {
    let storage = InMemoryStorage::new(ontology());
    let mut a = base_memory("a".into(), "content".into(), 3, 3);
    let mut b = base_memory("b".into(), "content".into(), 3, 3);
    let mut foreign = base_memory("foreign".into(), "content".into(), 3, 3);
    for memory in [&mut a, &mut b] {
        memory.context.project_id = Some("project".into());
    }
    foreign.context.project_id = Some("other".into());
    storage
        .upsert_batch(&[a.clone(), b.clone(), foreign.clone()], &[])
        .await
        .unwrap();
    let mut lower_id = base_relationship(a.id, b.id);
    lower_id.id = RelationshipId([0x10; 16]);
    let mut higher_id = lower_id.clone();
    higher_id.id = RelationshipId([0x20; 16]);
    let unrelated = base_relationship(a.id, foreign.id);
    storage
        .upsert_batch(&[], &[unrelated, higher_id.clone(), lower_id.clone()])
        .await
        .unwrap();
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let rows = storage.relationships_in_region(&region, 3).await.unwrap();
    assert!(rows.iter().all(|row| {
        (row.from == a.id && row.to == b.id) || (row.from == b.id && row.to == a.id)
    }));
    assert!(rows.windows(2).all(|pair| {
        (pair[0].from, pair[0].to, pair[0].kind, pair[0].id)
            <= (pair[1].from, pair[1].to, pair[1].kind, pair[1].id)
    }));
    assert_eq!(
        rows.iter()
            .filter(|row| {
                row.from == lower_id.from && row.to == lower_id.to && row.kind == lower_id.kind
            })
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![lower_id.id, higher_id.id],
        "the final relationship-id key orders otherwise identical rows"
    );
    assert!(storage.relationships_in_region(&region, 1).await.is_err());
}

/// Strip the storage-assigned LSN so caller-supplied rows compare equal to
/// storage-returned rows (storage stamps its own backend LSN, §6.6).
fn strip_lsn(mut m: Memory) -> Memory {
    m.lsn = LSN::new_local(0);
    m
}

proptest! {
    /// ∀ m: upsert_memory(m); get_memory(&m.id) returns the same row
    /// (modulo the storage-assigned LSN). Storage ops are async; the
    /// property body drives them on a dedicated current-thread runtime.
    #[test]
    fn roundtrip_memory_prop(
        title in "[a-z ]{1,20}",
        content in "[a-z ]{1,60}",
        mt in 0u8..13u8,
        vis in 0u8..5u8,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let store = InMemoryStorage::new(ontology());
        let m = base_memory(title, content, mt, vis);
        rt.block_on(async {
            let commit = store.upsert_memory(&m).await.unwrap();
            prop_assert!(commit.lsn > 0);
            let got = store.get_memory(&m.id).await.unwrap().expect("row present");
            prop_assert_eq!(
                serde_json::to_string(&strip_lsn(got)).unwrap(),
                serde_json::to_string(&strip_lsn(m.clone())).unwrap()
            );
            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?;
    }

    /// LSNs are strictly monotonic across mixed writes (R-S3 / CR-15).
    #[test]
    fn lsn_monotonic_prop(writes in proptest::collection::vec((any::<u8>(), any::<u8>()), 1..64)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let store = InMemoryStorage::new(ontology());
        rt.block_on(async {
            let mut last = 0u64;
            for (i, (mt, vis)) in writes.iter().enumerate() {
                let m = base_memory(format!("m{i}"), "c".into(), *mt, *vis);
                let c = store.upsert_memory(&m).await.unwrap();
                prop_assert!(c.lsn > last);
                last = c.lsn;
            }
            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?;
    }

    /// Re-syncing a stable external identity appends an assertion version
    /// while ordinary streaming continues to expose one current row.
    #[test]
    fn bi_temporal_roundtrip_prop(
        content_a in "[a-z]{1,10}",
        content_b in "[a-z]{1,10}",
        hours_back in 1i64..48i64,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let store = InMemoryStorage::new(ontology());
        rt.block_on(async {
            let t0 = Utc::now() - Duration::hours(hours_back);
            let t1 = Utc::now() - Duration::hours(hours_back.max(1) / 2);
            let mut v1 = base_memory("p".into(), content_a.clone(), 2, 1);
            v1.valid_from = t0;
            v1.valid_until = Some(t1);
            v1.recorded_at = t0;
            store.upsert_memory(&v1).await.unwrap();
            // Current version is readable across its own window.
            let at_t0 = store.valid_at(&v1.id, t0).await.unwrap().unwrap();
            prop_assert_eq!(at_t0.content.as_str(), content_a.as_str());
            // A later snapshot keeps the earlier assertion addressable.
            let mut v2 = v1.clone();
            v2.content = content_b.clone();
            v2.valid_from = t1;
            v2.valid_until = None;
            v2.recorded_at = t1;
            store.upsert_memory(&v2).await.unwrap();
            let superseded = store.valid_at(&v1.id, t0).await.unwrap().unwrap();
            prop_assert_eq!(superseded.content.as_str(), content_a.as_str());
            let at_t1 = store.valid_at(&v1.id, t1).await.unwrap().unwrap();
            prop_assert_eq!(at_t1.content.as_str(), content_b.as_str());
            // A correction may refer to the original valid-time while being
            // learned later; it must not leak into an earlier knowledge cut.
            let t2 = t1 + Duration::seconds(1);
            let mut correction = v2.clone();
            correction.content = "correction".into();
            correction.valid_from = t0;
            correction.recorded_at = t2;
            store.upsert_memory(&correction).await.unwrap();
            let still_v2 = store.valid_at(&v1.id, t1).await.unwrap().unwrap();
            prop_assert_eq!(still_v2.content.as_str(), content_b.as_str());
            let corrected = store.valid_at(&v1.id, t2).await.unwrap().unwrap();
            prop_assert_eq!(corrected.content.as_str(), "correction");
            let before = store.valid_at(&v1.id, t0 - Duration::hours(1)).await.unwrap();
            prop_assert!(before.is_none());
            // Streaming sees exactly one row per id (ST8).
            use futures::StreamExt;
            let mut rows = 0;
            let mut ms = store.stream_all_memories().await;
            while let Some(Ok(m)) = ms.next().await {
                if m.id == v1.id {
                    rows += 1;
                }
            }
            prop_assert_eq!(rows, 1);
            prop_assert_eq!(store.memory_history(&v1.id).len(), 3);
            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?;
    }
}

/// The headline 10k case from the M2 acceptance criteria, run once end-to-end.
#[test]
fn roundtrip_memories_10k() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut rng_state: u64 = 0x853c_49e6_748f_ea9b; // splitmix64
    let store = InMemoryStorage::new(ontology());
    rt.block_on(async {
        for i in 0..10_000u64 {
            rng_state = rng_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = rng_state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            let r = z ^ (z >> 31);
            let m = base_memory(
                format!("memory-{i}"),
                format!("content-{i}"),
                (r % 13) as u8,
                (r % 5) as u8,
            );
            store.upsert_memory(&m).await.unwrap();
            let got = store.get_memory(&m.id).await.unwrap().expect("row present");
            assert_eq!(
                serde_json::to_string(&strip_lsn(got)).unwrap(),
                serde_json::to_string(&strip_lsn(m)).unwrap()
            );
        }
    });
}

#[test]
fn soft_delete_closes_valid_until() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = InMemoryStorage::new(ontology());
    rt.block_on(async {
        let m = base_memory("gone".into(), "x".into(), 5, 3);
        store.upsert_memory(&m).await.unwrap();
        let _ = store.delete_memory(&m.id).await;
        let after = store.get_memory(&m.id).await.unwrap().unwrap();
        assert!(
            after.valid_until.is_some(),
            "soft delete closes valid_until"
        );
    });
}

#[test]
fn get_state_at_counts_versions() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = InMemoryStorage::new(ontology());
    rt.block_on(async {
        let t = Utc::now();
        let mut live = base_memory("live".into(), "x".into(), 1, 3);
        live.valid_from = t - Duration::seconds(1);
        live.recorded_at = t - Duration::seconds(1);
        let mut dead = base_memory("dead".into(), "x".into(), 1, 3);
        dead.valid_from = t - Duration::hours(2);
        dead.valid_until = Some(t - Duration::hours(1));
        dead.recorded_at = t - Duration::hours(2);
        store.upsert_memory(&live).await.unwrap();
        store.upsert_memory(&dead).await.unwrap();
        let snap = store.get_state_at(t).await.unwrap();
        assert_eq!(snap.memory_count, 1);
    });
}

#[tokio::test]
async fn ingest_claim_is_single_winner_and_replays_original_result() {
    let store = Arc::new(InMemoryStorage::new(ontology()));
    let key = IngestBatchKey {
        org_id: "org".into(),
        producer_id: "producer".into(),
        batch_id: "same-batch".into(),
    };
    let left = base_memory("left".into(), "left".into(), 1, 3);
    let right = base_memory("right".into(), "right".into(), 1, 3);
    let left = [left];
    let right = [right];
    let (a, b) = tokio::join!(
        store.commit_ingest_batch(&key, &left, &[], 1),
        store.commit_ingest_batch(&key, &right, &[], 1),
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IngestCommitOutcome::Committed { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IngestCommitOutcome::Duplicate(_)))
            .count(),
        1
    );
    use futures::StreamExt;
    let mut rows = store.stream_all_memories().await;
    let mut count = 0;
    while let Some(row) = rows.next().await {
        row.unwrap();
        count += 1;
    }
    assert_eq!(count, 1, "only the winning batch may mutate storage");
}

#[tokio::test]
async fn failed_ingest_commit_leaves_key_retryable() {
    let store = InMemoryStorage::new(ontology());
    let key = IngestBatchKey {
        org_id: "org".into(),
        producer_id: "producer".into(),
        batch_id: "retryable".into(),
    };
    let from = MemoryId::new_v7();
    let to = MemoryId::new_v7();
    let invalid = Relationship {
        id: RelationshipId::derive(from, RelKindId(0), to, None),
        kind: RelKindId(0),
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "test".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    };
    assert!(store
        .commit_ingest_batch(&key, &[], &[invalid], 1)
        .await
        .is_err());

    let valid = base_memory("valid".into(), "valid".into(), 1, 3);
    assert!(matches!(
        store
            .commit_ingest_batch(&key, &[valid], &[], 1)
            .await
            .unwrap(),
        IngestCommitOutcome::Committed { .. }
    ));
}

#[tokio::test]
async fn ingest_settlement_persists_one_immutable_acknowledgeable_effect() {
    let store = InMemoryStorage::new(ontology());
    let key = IngestBatchKey {
        org_id: "org".into(),
        producer_id: "producer".into(),
        batch_id: "outbox".into(),
    };
    let memory = base_memory("outbox".into(), "payload".into(), 1, 3);
    let effect = PostIngestEffect {
        effect_id: "org/producer/outbox".into(),
        session_memory_ids: vec![memory.id],
        region_deltas: vec![IngestRegionDelta {
            region: RegionKey {
                org: "org".into(),
                project: "*".into(),
                memory_type: memory.memory_type,
            },
            memories: 1,
            relationships: 0,
        }],
    };

    assert!(matches!(
        store
            .commit_ingest_batch_with_effect(&key, &[memory.clone()], &[], 1, &effect)
            .await
            .unwrap(),
        IngestCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        store
            .commit_ingest_batch_with_effect(&key, &[memory], &[], 1, &effect)
            .await
            .unwrap(),
        IngestCommitOutcome::Duplicate(_)
    ));
    assert_eq!(
        store.pending_ingest_effects(10).await.unwrap(),
        [effect.clone()]
    );
    let (claim_a, claim_b) = tokio::join!(
        store.claim_ingest_effect("worker-a", 1_000),
        store.claim_ingest_effect("worker-b", 1_000),
    );
    let (winner, loser) = match (claim_a.unwrap(), claim_b.unwrap()) {
        (Some(claimed), None) => {
            assert_eq!(claimed, effect);
            ("worker-a", "worker-b")
        }
        (None, Some(claimed)) => {
            assert_eq!(claimed, effect);
            ("worker-b", "worker-a")
        }
        claims => panic!("exactly one simultaneous claimant must win: {claims:?}"),
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(store
        .renew_ingest_effect_claim(effect.effect_id.as_str(), winner, 2_000)
        .await
        .unwrap());
    assert!(!store
        .renew_ingest_effect_claim(effect.effect_id.as_str(), loser, 2_000)
        .await
        .unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    assert!(
        store
            .claim_ingest_effect(loser, 30_000)
            .await
            .unwrap()
            .is_none(),
        "renewal must exclude contenders beyond the original lease"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert!(
        !store
            .acknowledge_ingest_effect(effect.effect_id.as_str(), winner)
            .await
            .unwrap(),
        "an expired owner cannot acknowledge before reclaim"
    );
    assert_eq!(
        store.claim_ingest_effect(loser, 30_000).await.unwrap(),
        Some(effect.clone()),
        "an abandoned claim becomes retryable after its deadline"
    );
    assert!(!store
        .acknowledge_ingest_effect(effect.effect_id.as_str(), winner)
        .await
        .unwrap());
    assert!(store
        .acknowledge_ingest_effect(effect.effect_id.as_str(), loser)
        .await
        .unwrap());
    assert!(store.pending_ingest_effects(10).await.unwrap().is_empty());
    assert!(store
        .acknowledge_ingest_effect(effect.effect_id.as_str(), loser)
        .await
        .unwrap());
}

#[tokio::test]
async fn find_by_entity_uses_canonical_memory_context() {
    let store = InMemoryStorage::new(ontology());
    let entity = EntityId([7; 16]);
    let mut matching = base_memory("matching".into(), "matching".into(), 2, 3);
    matching.context.entities.push(entity);
    let other = base_memory("other".into(), "other".into(), 2, 3);
    store
        .upsert_batch(&[matching.clone(), other], &[])
        .await
        .unwrap();

    let rows = store
        .find_by_entity(
            &entity,
            &MemoryFilter {
                limit: 10,
                visibility_ctx: VisibilityContext {
                    org_id: "org".into(),
                    max_visibility: Visibility::Org,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, matching.id);
}

#[tokio::test]
async fn find_by_entity_filters_tenant_before_limit() {
    let store = InMemoryStorage::new(ontology());
    let entity = EntityId([8; 16]);
    let mut matching = base_memory("matching-tenant".into(), "matching".into(), 2, 3);
    matching.context.entities.push(entity);
    let mut tenantless = base_memory("tenantless".into(), "foreign".into(), 2, 3);
    tenantless.context.entities.push(entity);
    tenantless.context.tenant_id = None;
    tenantless.recorded_at = matching.recorded_at + Duration::seconds(1);
    store
        .upsert_batch(&[matching.clone(), tenantless], &[])
        .await
        .unwrap();

    let rows = store
        .find_by_entity(
            &entity,
            &MemoryFilter {
                limit: 1,
                visibility_ctx: VisibilityContext {
                    org_id: "org".into(),
                    max_visibility: Visibility::Org,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|memory| memory.id).collect::<Vec<_>>(),
        [matching.id]
    );
}

#[tokio::test]
async fn bounded_region_seams_filter_before_limit_and_keep_closed_relationships() {
    let store = InMemoryStorage::new(ontology());
    let mut from = base_memory("region-from".into(), "from".into(), 3, 3);
    from.context.project_id = Some("project".into());
    let mut to = base_memory("region-to".into(), "to".into(), 3, 3);
    to.context.project_id = Some("project".into());
    let mut foreign = base_memory("foreign".into(), "foreign".into(), 3, 3);
    foreign.context.tenant_id = Some("other-org".into());
    foreign.context.project_id = Some("project".into());
    let relationship = Relationship {
        id: RelationshipId::derive(from.id, RelKindId(0), to.id, Some("regional")),
        kind: RelKindId(0),
        from: from.id,
        to: to.id,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "test".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: Utc::now(),
        valid_until: Some(Utc::now()),
        recorded_at: Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    };
    store
        .upsert_batch(
            &[from.clone(), to.clone(), foreign],
            &[relationship.clone()],
        )
        .await
        .unwrap();
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let memories = store.memories_in_region(&region, 2).await.unwrap();
    assert_eq!(
        memories.iter().map(|memory| memory.id).collect::<Vec<_>>(),
        {
            let mut ids = vec![from.id, to.id];
            ids.sort();
            ids
        }
    );
    let relationships = store
        .current_relationships_in_region(&region, 10)
        .await
        .unwrap();
    assert!(relationships.iter().any(|row| row.id == relationship.id));
    assert!(store.memories_in_region(&region, 1).await.is_err());
}
