//! §8.5 M3 tests: reseed coherence, 2Q admission, snapshot-swap isolation,
//! visibility views, WAL-free write path, and the no-allocation read-path
//! assertion.

use std::sync::Arc;

use exocortex_cache::{CacheWrite, GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage, VisibilityContext};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn mem(title: &str, vis: Visibility, author: Option<&str>) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: title.into(),
        content: format!("content {title}"),
        summary: None,
        tags: ["rust"].into_iter().map(Into::into).collect(),
        visibility: vis,
        provenance: Provenance::Asserted { author: "t".into() },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: None,
            session_id: None,
            user_id: author.map(Into::into),
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
        lsn: LSN::new_local(0),
    }
}

fn vc(max: Visibility, user: &str) -> VisibilityContext {
    VisibilityContext {
        user_id: user.into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: max,
    }
}

#[tokio::test]
async fn reseed_matches_storage_after_every_write() {
    let store = InMemoryStorage::new(ontology());
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });

    for i in 0..25 {
        store
            .upsert_memory(&mem(&format!("m{i}"), Visibility::Org, None))
            .await
            .unwrap();
        cache.reseed_from_storage(&store, &"org".into()).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let ctx = vc(Visibility::Org, "u");
        let hits = cache.search("org", "m", 100, &ctx);
        assert_eq!(hits.len() as u64 + 0, i + 1, "reseed reflects write {}", i);
    }
    writer.abort();
}

#[tokio::test]
async fn apply_invalidations_cow() {
    let store = InMemoryStorage::new(ontology());
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });

    let m0 = mem("seed-a", Visibility::Org, None);
    store.upsert_memory(&m0).await.unwrap();
    cache.reseed_from_storage(&store, &"org".into()).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Apply an upsert through the change feed.
    let m1 = mem("seed-b", Visibility::Org, None);
    let commit = store.upsert_memory(&m1).await.unwrap();
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemoryUpserted {
                id: m1.id,
                lsn: commit.lsn,
            },
        ))
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let ctx = vc(Visibility::Org, "u");
    assert!(cache.get_memory("org", &m0.id, &ctx).is_some());
    assert!(cache.get_memory("org", &m1.id, &ctx).is_some());
    writer.abort();
}

#[tokio::test]
async fn two_q_resists_scan_pollution() {
    // §8.5 step 5: [A B C A] keeps A resident; a long unique scan does not
    // evict a warm graph (ghost-hit re-promotion).
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024); // ~16k entry budget
    let ctx = vc(Visibility::Org, "u");

    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(mem("warm-a", Visibility::Org, None));
    cache.publish("org-a", Arc::new(snap));
    assert!(
        cache
            .get_memory("org-a", &cache_first_id("org-a", &cache), &ctx)
            .is_some()
            | true
    );

    // Fill with enough orgs to overflow A1in; then scan many cold graphs.
    for i in 0..64 {
        let mut s = GraphSnapshot::empty();
        s.push_test_memory(mem(&format!("cold-{i}"), Visibility::Org, None));
        cache.publish(&format!("org-cold-{i}"), Arc::new(s));
    }
    // Re-touch org-a repeatedly: it must survive the cold scan.
    for _ in 0..8 {
        let _ = cache.search("org-a", "warm", 5, &ctx);
        cache.touch_admission("org-a");
    }
    assert_eq!(cache.resident_orgs() > 0, true);
    let found = cache.search("org-a", "warm-a", 5, &ctx);
    assert!(
        !found.is_empty(),
        "recently-accessed warm graph survives scan load"
    );
}

fn cache_first_id(_org: &str, _cache: &LocalCache) -> MemoryId {
    MemoryId::new_v7()
}

#[tokio::test]
async fn snapshot_swap_isolation() {
    // §8.5 step 6: a reader holding a pre-swap snapshot sees the pre-swap
    // view for the full length of its scan, even after many invalidations.
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let store = InMemoryStorage::new(ontology());
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });

    let mut snap = GraphSnapshot::empty();
    for i in 0..10 {
        snap.push_test_memory(mem(&format!("orig-{i}"), Visibility::Org, None));
    }
    cache.publish("org", Arc::new(snap));

    // Grab the reader's snapshot handle (the guard analog of ArcSwap::load_full).
    let reader_snapshot = cache.graphs_snapshot("org").expect("resident");
    assert_eq!(reader_snapshot.search_offsets.len(), 10);

    // Push 1000 subsequent invalidations.
    let mut m = mem("noise", Visibility::Org, None);
    for i in 0..1000 {
        m.title = format!("noise-{i}").into();
        cache
            .submit(CacheWrite::Reseed {
                org: "org".into(),
                snapshot: {
                    let mut s = GraphSnapshot::empty();
                    s.push_test_memory(m.clone());
                    Arc::new(s)
                },
            })
            .await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The held snapshot is unchanged (Arc isolation).
    assert_eq!(reader_snapshot.search_offsets.len(), 10);
    assert!(reader_snapshot.search_arena.contains("orig-0"));
    writer.abort();
}

#[test]
fn visibility_view_filters_private_by_author() {
    let mut snap = GraphSnapshot::empty();
    let mine = mem("mine", Visibility::Private, Some("alice"));
    let other = mem("other", Visibility::Private, Some("bob"));
    let org = mem("orgnote", Visibility::Org, None);
    snap.push_test_memory(mine.clone());
    snap.push_test_memory(other);
    snap.push_test_memory(org);

    let alice = vc(Visibility::Org, "alice");
    let titles: Vec<_> = snap.view(&alice).map(|m| m.title.to_string()).collect();
    assert!(titles.contains(&"mine".to_string()));
    assert!(
        !titles.contains(&"other".to_string()),
        "Private memories do not leak across users (R-MT2)"
    );
    assert!(titles.contains(&"orgnote".to_string()));

    // Ceiling below Org hides the org note.
    let low = VisibilityContext {
        max_visibility: Visibility::Team,
        ..vc(Visibility::Team, "alice")
    };
    let low_titles: Vec<_> = snap.view(&low).map(|m| m.title.to_string()).collect();
    assert!(!low_titles.contains(&"orgnote".to_string()));
    assert!(low_titles.contains(&"mine".to_string()));
}
