//! R6-A04 executable budgets for cache hydration and invalidation updates.

use std::sync::Arc;
use std::time::{Duration, Instant};

use exocortex_cache::{CacheWrite, LocalCache};
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId, Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Invalidation, Storage};

fn memory(i: usize) -> Memory {
    Memory {
        id: MemoryId::from_content_hash("bench-org", &i.to_string()),
        memory_type: 3,
        title: format!("update-{i}").into(),
        content: "bounded update benchmark payload".into(),
        summary: None,
        tags: ["cache".into()].into_iter().collect(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "bench".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: None,
            user_id: Some("bench".into()),
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
        embedding: None,
        lsn: LSN::new_backend(i as u64 + 1),
    }
}

fn relationship(i: usize, from: MemoryId, to: MemoryId) -> Relationship {
    let now = chrono::Utc::now();
    Relationship {
        id: RelationshipId::derive(
            from,
            exocortex_kernel::kinds::FIXES,
            to,
            Some(&i.to_string()),
        ),
        kind: exocortex_kernel::kinds::FIXES,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "bench".into(),
            producer_kind: None,
        },
        properties: exocortex_kernel::RelationshipProperties {
            strength: 0.5,
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

fn main() {
    const RESIDENT: usize = 20_000;
    const DELTAS: usize = 256;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ontology = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let (cache, rx) = LocalCache::new(512 * 1024 * 1024);
    let cache = Arc::new(cache);
    let storage = Arc::new(InMemoryStorage::new(ontology));
    let writer = rt.spawn({
        let cache = cache.clone();
        let storage = storage.clone();
        async move { cache.run(storage, rx).await }
    });

    let rows: Vec<_> = (0..RESIDENT).map(memory).collect();
    let hydration_started = Instant::now();
    rt.block_on(cache.reseed_rows("bench-org".into(), rows, vec![], RESIDENT as u64));
    let hydration = hydration_started.elapsed();

    let baseline_publications = cache.snapshot_publications();
    let update_started = Instant::now();
    rt.block_on(async {
        for i in RESIDENT..RESIDENT + DELTAS {
            cache
                .submit(CacheWrite::Apply(Invalidation::MemorySnapshotUpserted {
                    memory: Box::new(memory(i)),
                    lsn: i as u64 + 1,
                }))
                .await;
        }
        cache.flush().await;
    });
    let update = update_started.elapsed();
    let publications = cache.snapshot_publications() - baseline_publications;

    const RELATIONSHIPS: usize = 10_000;
    const POINT_READS: usize = 1_000;
    let endpoint_a = memory(0);
    let endpoint_b = memory(1);
    let relationships: Vec<_> = (0..RELATIONSHIPS)
        .map(|i| relationship(i, endpoint_a.id, endpoint_b.id))
        .collect();
    rt.block_on(storage.upsert_batch(&[endpoint_a.clone(), endpoint_b.clone()], &relationships))
        .unwrap();
    let target = relationships.last().unwrap().id;
    let point_invalidation_started = Instant::now();
    rt.block_on(async {
        cache
            .submit(CacheWrite::Apply(Invalidation::RelationshipUpserted {
                id: target,
                from: endpoint_a.id,
                to: endpoint_b.id,
                kind: exocortex_kernel::kinds::FIXES,
                lsn: (RESIDENT + DELTAS + RELATIONSHIPS) as u64,
            }))
            .await;
        cache.flush().await;
    });
    let point_invalidation = point_invalidation_started.elapsed();
    assert!(cache
        .graphs_snapshot("bench-org")
        .unwrap()
        .by_rel_id
        .contains_key(&target));
    let edge_fetch_started = Instant::now();
    rt.block_on(async {
        for _ in 0..POINT_READS {
            assert!(storage.get_relationship(&target).await.unwrap().is_some());
        }
    });
    let edge_fetch = edge_fetch_started.elapsed();

    // Generous shared-CI ceilings. The publication assertion is the primary
    // scaling gate: one graph clone/swap for a queued delta burst, not 256.
    let multiplier = std::env::var("SLO_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(1.0, 3.0);
    let hydration_budget = Duration::from_secs(2).mul_f64(multiplier);
    let update_budget = Duration::from_millis(500).mul_f64(multiplier);
    let edge_fetch_budget = Duration::from_millis(100).mul_f64(multiplier);
    let point_invalidation_budget = Duration::from_millis(100).mul_f64(multiplier);
    println!(
        "cache update gate: hydrate {RESIDENT}={hydration:?}; {DELTAS} invalidations={update:?}; publications={publications}"
    );
    println!(
        "indexed relationship gate: point invalidation over {RELATIONSHIPS} rows={point_invalidation:?}; {POINT_READS} reads={edge_fetch:?}"
    );
    let ok = hydration < hydration_budget
        && update < update_budget
        && publications == 1
        && edge_fetch < edge_fetch_budget
        && point_invalidation < point_invalidation_budget;
    println!(
        "cache update/hydration SLO gate: {}",
        if ok { "PASS" } else { "FAIL" }
    );
    writer.abort();
    if !ok {
        std::process::exit(1);
    }
}
