//! `InMemoryStorage` property tests (§6.7 step 3): 10k random memories
//! round-trip, LSN monotonicity, and bi-temporal semantics.

use chrono::{Duration, Utc};
use exocortex_kernel::{
    EntityId, Memory, MemoryContext, MemoryId, Provenance, RelKindId, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{
    InMemoryStorage, IngestBatchKey, IngestCommitOutcome, MemoryFilter, Storage, VisibilityContext,
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
            tenant_id: None,
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
        let _ = rt.block_on(async {
            let commit = store.upsert_memory(&m).await.unwrap();
            prop_assert!(commit.lsn > 0);
            let got = store.get_memory(&m.id).await.unwrap().expect("row present");
            prop_assert_eq!(
                serde_json::to_string(&strip_lsn(got)).unwrap(),
                serde_json::to_string(&strip_lsn(m.clone())).unwrap()
            );
            Ok::<(), proptest::test_runner::TestCaseError>(())
        });
    }

    /// LSNs are strictly monotonic across mixed writes (R-S3 / CR-15).
    #[test]
    fn lsn_monotonic_prop(writes in proptest::collection::vec((any::<u8>(), any::<u8>()), 1..64)) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let store = InMemoryStorage::new(ontology());
        let _ = rt.block_on(async {
            let mut last = 0u64;
            for (i, (mt, vis)) in writes.iter().enumerate() {
                let m = base_memory(format!("m{i}"), "c".into(), *mt, *vis);
                let c = store.upsert_memory(&m).await.unwrap();
                prop_assert!(c.lsn > last);
                last = c.lsn;
            }
            Ok::<(), proptest::test_runner::TestCaseError>(())
        });
    }

    /// Bi-temporal round-trip under the SHARED backend semantics (ST7,
    /// audit): an upsert of the same id overwrites in place — one current
    /// row per id, exactly what the FalkorDB adapter's MERGE serves. The
    /// superseding version's valid window is what `valid_at` answers; the
    /// superseded version is no longer addressable by time (that model was
    /// a double-only fiction the production backend could not serve).
    #[test]
    fn bi_temporal_roundtrip_prop(
        content_a in "[a-z]{1,10}",
        content_b in "[a-z]{1,10}",
        hours_back in 1i64..48i64,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let store = InMemoryStorage::new(ontology());
        let _ = rt.block_on(async {
            let t0 = Utc::now() - Duration::hours(hours_back);
            let t1 = Utc::now() - Duration::hours(hours_back.max(1) / 2);
            let mut v1 = base_memory("p".into(), content_a.clone(), 2, 1);
            v1.valid_from = t0;
            v1.valid_until = Some(t1);
            store.upsert_memory(&v1).await.unwrap();
            // Current version is readable across its own window.
            let at_t0 = store.valid_at(&v1.id, t0).await.unwrap().unwrap();
            prop_assert_eq!(at_t0.content, content_a);
            // Supersede in place: the surviving row's window governs.
            let mut v2 = v1.clone();
            v2.content = content_b.clone();
            v2.valid_from = t1;
            v2.valid_until = None;
            store.upsert_memory(&v2).await.unwrap();
            let superseded = store.valid_at(&v1.id, t0).await.unwrap();
            prop_assert!(
                superseded.is_none(),
                "superseded version no longer addressable (ST7 shared semantics)"
            );
            let at_t1 = store.valid_at(&v1.id, t1).await.unwrap().unwrap();
            prop_assert_eq!(at_t1.content, content_b);
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
            Ok::<(), proptest::test_runner::TestCaseError>(())
        });
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
        let mut dead = base_memory("dead".into(), "x".into(), 1, 3);
        dead.valid_from = t - Duration::hours(2);
        dead.valid_until = Some(t - Duration::hours(1));
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
