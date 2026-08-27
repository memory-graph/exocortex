//! §8.5 M3 tests: reseed coherence, 2Q admission, snapshot-swap isolation,
//! visibility views, WAL-free write path, and the no-allocation read-path
//! assertion.

use std::sync::Arc;

use exocortex_cache::{CacheWrite, GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{Direction, InMemoryStorage, Storage, TraversalSpec, VisibilityContext};

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
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
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

fn rel(from: MemoryId, to: MemoryId, id_byte: u8) -> exocortex_kernel::Relationship {
    exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId([id_byte; 16]),
        kind: exocortex_kernel::RelKindId(1),
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "test".into(),
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
        let ctx = vc(Visibility::Org, "u");
        let hits = cache.search("org", "m", 100, &ctx);
        assert_eq!(hits.len(), i + 1, "reseed reflects write {}", i);
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
    cache.flush().await;

    let ctx = vc(Visibility::Org, "u");
    assert!(cache.get_memory("org", &m0.id, &ctx).is_some());
    assert!(cache.get_memory("org", &m1.id, &ctx).is_some());
    writer.abort();
}

#[tokio::test]
async fn relationship_fetch_failure_does_not_advance_backend_lsn() {
    let store = InMemoryStorage::new(ontology());
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });
    cache.reseed_from_storage(&store, &"org".into()).await;
    let before = cache.version("org").unwrap().backend_lsn;
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::RelationshipUpserted {
                id: exocortex_kernel::RelationshipId([0xEE; 16]),
                from: MemoryId([1; 16]),
                to: MemoryId([2; 16]),
                kind: exocortex_kernel::RelKindId(1),
                lsn: 99,
            },
        ))
        .await;
    cache.flush().await;
    assert_eq!(
        cache.version("org").unwrap().backend_lsn,
        before,
        "a missing/failing row fetch cannot acknowledge its LSN"
    );
    writer.abort();
}

#[tokio::test]
async fn two_q_resists_scan_pollution() {
    // §8.5 step 5: a long unique scan evicts cold graphs, but a re-referenced
    // warm graph is promoted out of A1in into Am and survives. The budget is
    // small enough that the 65 tiny org graphs (~600B each ≈ 39KB total)
    // overflow it, forcing real evictions.
    let budget = 8 * 1024;
    let (cache, _rx) = LocalCache::new(budget);
    let ctx = vc(Visibility::Org, "u");

    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(mem("warm-a", Visibility::Org, None));
    cache.publish("org-a", Arc::new(snap));
    assert_eq!(cache.a1in_count("org-a"), 1);

    // Re-reference the warm org: 2Q promotes it from A1in to Am.
    cache.touch_admission("org-a");
    assert_eq!(cache.a1in_count("org-a"), 0, "no duplicate A1in entries");
    assert!(cache.am_contains("org-a"), "re-reference promotes to Am");

    // Fill with cold orgs; each publish overflows the byte budget and evicts
    // from A1in, so the cold scan cannot displace the warm Am entry.
    const COLD: usize = 64;
    for i in 0..COLD {
        let mut s = GraphSnapshot::empty();
        s.push_test_memory(mem(&format!("cold-{i}"), Visibility::Org, None));
        cache.publish(&format!("org-cold-{i}"), Arc::new(s));
    }

    // Eviction really happened: far fewer residents than published orgs.
    assert!(
        cache.resident_orgs() < 1 + COLD,
        "budget must force eviction: resident={} published={}",
        cache.resident_orgs(),
        1 + COLD
    );
    // Some cold org was actually evicted.
    let mut evicted = 0;
    for i in 0..COLD {
        if cache.graphs_snapshot(&format!("org-cold-{i}")).is_none() {
            evicted += 1;
        }
    }
    assert!(evicted > 0, "at least one cold org evicted (got {evicted})");

    // The warm org survived the scan load.
    let found = cache.search("org-a", "warm-a", 5, &ctx);
    assert!(
        !found.is_empty(),
        "recently-accessed warm graph survives scan load"
    );
}

#[tokio::test]
async fn repeated_publish_never_duplicates_a1in() {
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    for _ in 0..5 {
        let mut s = GraphSnapshot::empty();
        s.push_test_memory(mem("x", Visibility::Org, None));
        cache.publish("org-x", Arc::new(s));
    }
    assert_eq!(
        cache.a1in_count("org-x"),
        0,
        "re-publish promotes, never duplicates"
    );
    assert!(cache.am_contains("org-x"));
    for i in 0..10 {
        let mut s = GraphSnapshot::empty();
        s.push_test_memory(mem(&format!("y{i}"), Visibility::Org, None));
        cache.publish(&format!("org-y{i}"), Arc::new(s));
    }
    assert_eq!(
        cache.a1in_len(),
        10,
        "A1in holds each distinct org exactly once"
    );
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
                ack: None,
            })
            .await;
    }
    cache.flush().await;

    // The held snapshot is unchanged (Arc isolation).
    assert_eq!(reader_snapshot.search_offsets.len(), 10);
    assert!(reader_snapshot.search_arena.contains("orig-0"));
    writer.abort();
}

#[tokio::test]
async fn memory_upsert_preserves_incident_relationships() {
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let store = Arc::new(InMemoryStorage::new(ontology()));
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone();
        async move { cache.run(store, rx).await }
    });
    let mut first = mem("first", Visibility::Org, None);
    let second = mem("second", Visibility::Org, None);
    let relationship = rel(first.id, second.id, 41);
    cache
        .reseed_rows(
            "org".into(),
            vec![first.clone(), second],
            vec![relationship.clone()],
            1,
        )
        .await;

    first.title = "updated first".into();
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemorySnapshotUpserted {
                memory: Box::new(first),
                lsn: 2,
            },
        ))
        .await;
    cache.flush().await;

    let snapshot = cache.graphs_snapshot("org").expect("resident");
    assert!(snapshot.by_rel_id.contains_key(&relationship.id));
    assert_eq!(snapshot.petgraph.edge_count(), 1);
    writer.abort();
}

#[tokio::test]
async fn node_delete_clears_incident_ids_before_edge_index_reuse() {
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let store = Arc::new(InMemoryStorage::new(ontology()));
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone();
        async move { cache.run(store, rx).await }
    });
    let first = mem("first", Visibility::Org, None);
    let second = mem("second", Visibility::Org, None);
    let third = mem("third", Visibility::Org, None);
    let fourth = mem("fourth", Visibility::Org, None);
    let removed = rel(first.id, second.id, 51);
    let survivor = rel(third.id, fourth.id, 52);
    cache
        .reseed_rows(
            "org".into(),
            vec![first.clone(), second, third, fourth],
            vec![removed.clone()],
            1,
        )
        .await;

    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemoryDeleted {
                id: first.id,
                lsn: 2,
            },
        ))
        .await;
    cache.flush().await;
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::RelationshipSnapshotUpserted {
                relationship: Box::new(survivor.clone()),
                lsn: 3,
            },
        ))
        .await;
    cache.flush().await;
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::RelationshipDeleted {
                id: removed.id,
                lsn: 4,
            },
        ))
        .await;
    cache.flush().await;

    let snapshot = cache.graphs_snapshot("org").expect("resident");
    assert!(!snapshot.by_rel_id.contains_key(&removed.id));
    assert!(snapshot.by_rel_id.contains_key(&survivor.id));
    assert_eq!(snapshot.petgraph.edge_count(), 1);
    writer.abort();
}

#[tokio::test]
async fn released_snapshot_buffer_makes_isolated_update_delta_only() {
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let store = Arc::new(InMemoryStorage::new(ontology()));
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = store.clone();
        async move { cache.run(store, rx).await }
    });
    let residents = (0..2_000)
        .map(|index| mem(&format!("resident-{index}"), Visibility::Org, None))
        .collect();
    cache.reseed_rows("org".into(), residents, vec![], 1).await;

    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemorySnapshotUpserted {
                memory: Box::new(mem("first delta", Visibility::Org, None)),
                lsn: 2,
            },
        ))
        .await;
    cache.flush().await;
    let clones_after_warmup = cache.full_snapshot_clones();
    assert_eq!(clones_after_warmup, 1, "the first delta seeds the RCU pool");

    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemorySnapshotUpserted {
                memory: Box::new(mem("second delta", Visibility::Org, None)),
                lsn: 3,
            },
        ))
        .await;
    cache.flush().await;
    assert_eq!(
        cache.full_snapshot_clones(),
        clones_after_warmup,
        "reader-free steady-state updates reuse a retired graph and apply only journal deltas"
    );
    assert_eq!(cache.version("org").unwrap().backend_lsn, 3);
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

#[test]
fn visibility_view_enforces_project_and_team_membership() {
    let mut project = mem("project", Visibility::Project, None);
    project.context.project_id = Some("p1".into());
    let mut team = mem("team", Visibility::Team, None);
    team.context.team_id = Some("t1".into());
    let mut missing_scope = mem("missing", Visibility::Project, None);
    missing_scope.context.project_id = None;
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(project);
    snap.push_test_memory(team);
    snap.push_test_memory(missing_scope);

    let mut member = vc(Visibility::Org, "alice");
    member.project_ids.push("p1".into());
    member.team_ids.push("t1".into());
    let titles: Vec<_> = snap.view(&member).map(|m| m.title.as_str()).collect();
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"project") && titles.contains(&"team"));

    let outsider = vc(Visibility::Org, "bob");
    assert_eq!(snap.view(&outsider).count(), 0);
}

#[test]
fn traversal_never_crosses_an_invisible_intermediate_node() {
    let a = mem("a", Visibility::Org, None);
    let mut hidden = mem("hidden", Visibility::Project, None);
    hidden.context.project_id = Some("secret".into());
    let c = mem("c", Visibility::Org, None);
    let mut snap = GraphSnapshot::empty();
    for memory in [&a, &hidden, &c] {
        snap.push_test_memory(memory.clone());
    }
    snap.push_test_relationship(rel(a.id, hidden.id, 1));
    snap.push_test_relationship(rel(hidden.id, c.id, 2));

    let cache = LocalCache::new(1024 * 1024).0;
    cache.publish("org", Arc::new(snap));
    let spec = TraversalSpec {
        direction: Direction::Out,
        kinds: Default::default(),
        max_depth: 3,
        max_nodes: 10,
        visibility_ctx: vc(Visibility::Org, "outsider"),
        as_of: None,
    };
    assert!(cache.traverse("org", &a.id, &spec).is_empty());
}

/// CR1 (audit): applying MemoryUpserted for an EXISTING id replaces the
/// node — the stale version stops being searchable and a later delete
/// removes the row for real.
#[tokio::test]
async fn upsert_replaces_stale_version() {
    let onto = ontology();
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let storage = InMemoryStorage::new(onto);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = storage.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });
    let a = mem("alpha-wide", Visibility::Org, None);
    storage.upsert_memory(&a).await.unwrap();
    cache.reseed_from_storage(&storage, &"org".into()).await;

    // Re-upsert the SAME id with a new title (and a narrowed visibility,
    // which search must respect).
    let mut narrowed = a.clone();
    narrowed.title = "alpha-renamed".into();
    narrowed.visibility = Visibility::Private;
    narrowed.context.user_id = Some("other".into());
    storage.upsert_memory(&narrowed).await.unwrap();
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemoryUpserted { id: a.id, lsn: 2 },
        ))
        .await;
    cache.flush().await;

    // The OLD key yields nothing (the stale node is gone, not merely
    // shadowed); the NEW key yields exactly one hit at the new visibility.
    let ctx = vc(Visibility::Org, "alice");
    assert!(
        cache.search("org", "alpha-wide", 10, &ctx).is_empty(),
        "stale version no longer searchable"
    );
    let owner = vc(Visibility::Org, "other");
    let hits = cache.search("org", "alpha-renamed", 10, &owner);
    assert_eq!(hits.len(), 1, "exactly one node for the id: {hits:?}");
    assert_eq!(hits[0].0.title, "alpha-renamed", "new version wins");
    assert_eq!(hits[0].0.visibility, Visibility::Private);

    // And a subsequent delete actually removes it — no orphan copies.
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemoryDeleted { id: a.id, lsn: 3 },
        ))
        .await;
    cache.flush().await;
    let hits = cache.search("org", "alpha-renamed", 10, &owner);
    assert!(hits.is_empty(), "no orphan copies survive the delete");
    writer.abort();
}

/// CR2 (audit): reseed skips soft-deleted rows — a restart cannot
/// resurrect deleted memories.
#[tokio::test]
async fn reseed_skips_deleted_rows() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    let a = mem("alpha", Visibility::Org, None);
    let b = mem("beta", Visibility::Org, None);
    let _ = &onto;
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    storage.delete_memory(&a.id).await.unwrap();

    let snap = GraphSnapshot::from_storage(&storage).await;
    assert!(
        snap.by_id.get(&a.id).is_none(),
        "deleted row not resurrected"
    );
    assert!(snap.by_id.get(&b.id).is_some(), "live row present");
}

/// CR3 (audit): after a delete and fresh inserts (StableGraph reuses node
/// indices), search returns the memory whose key matched — not a neighbor.
#[tokio::test]
async fn search_resolves_correct_node_after_index_reuse() {
    let onto = ontology();
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let storage = InMemoryStorage::new(onto);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = storage.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });
    let mut ids = Vec::new();
    for t in ["w", "x", "y", "z"] {
        let m = mem(t, Visibility::Org, None);
        ids.push(m.id);
        storage.upsert_memory(&m).await.unwrap();
    }
    cache.reseed_from_storage(&storage, &"org".into()).await;

    // Delete the last, then insert two fresh rows (node indices get reused).
    storage.delete_memory(&ids[3]).await.unwrap();
    cache
        .submit(CacheWrite::Apply(
            exocortex_storage::Invalidation::MemoryDeleted { id: ids[3], lsn: 5 },
        ))
        .await;
    for t in ["fresh-one", "fresh-two"] {
        let m = mem(t, Visibility::Org, None);
        storage.upsert_memory(&m).await.unwrap();
        cache
            .submit(CacheWrite::Apply(
                exocortex_storage::Invalidation::MemoryUpserted { id: m.id, lsn: 6 },
            ))
            .await;
    }
    cache.flush().await;

    let ctx = vc(Visibility::Org, "alice");
    let hits = cache.search("org", "fresh-two", 10, &ctx);
    assert_eq!(hits.len(), 1);
    assert!(
        hits[0].0.title.contains("fresh-two"),
        "search returns the memory whose key matched: {:?}",
        hits[0].0.title
    );
    writer.abort();
}

#[test]
fn repeated_replacements_keep_search_index_bounded() {
    let mut snapshot = GraphSnapshot::empty();
    let mut row = mem("version-0000", Visibility::Org, None);
    for version in 0..2_000 {
        row.title = format!("version-{version:04}").into();
        snapshot.push_test_memory(row.clone());
    }

    assert_eq!(snapshot.petgraph.node_count(), 1);
    assert!(
        snapshot.search_arena.len() <= 2 * (row.title.len() + " rust\n".len()) + 1024,
        "replacement history leaked into the search arena: {} bytes",
        snapshot.search_arena.len()
    );
}

#[tokio::test]
async fn queued_invalidations_publish_one_delta_snapshot() {
    let onto = ontology();
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    cache.publish("org", Arc::new(GraphSnapshot::empty()));
    let baseline = cache.snapshot_publications();
    let mut ids = Vec::new();
    for lsn in 1..=128 {
        let row = mem(&format!("delta-{lsn}"), Visibility::Org, None);
        ids.push(row.id);
        cache
            .submit(CacheWrite::Apply(
                exocortex_storage::Invalidation::MemorySnapshotUpserted {
                    memory: Box::new(row),
                    lsn,
                },
            ))
            .await;
    }
    let writer = tokio::spawn({
        let cache = cache.clone();
        let storage = InMemoryStorage::new(onto);
        async move { cache.run(Arc::new(storage), rx).await }
    });
    cache.flush().await;

    assert_eq!(cache.snapshot_publications() - baseline, 1);
    let context = vc(Visibility::Org, "alice");
    assert!(ids
        .iter()
        .all(|id| cache.get_memory("org", id, &context).is_some()));
    writer.abort();
}

/// CR5 (audit): re-upserting the same RelationshipId replaces the edge —
/// no parallel duplicates.
#[tokio::test]
async fn relationship_reupsert_does_not_duplicate() {
    let onto = ontology();
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = std::sync::Arc::new(cache);
    let storage = InMemoryStorage::new(onto.clone());
    let writer = tokio::spawn({
        let cache = cache.clone();
        let store = storage.clone_dyn();
        async move { cache.run(Arc::new(store), rx).await }
    });
    let a = mem("alpha", Visibility::Org, None);
    let b = mem("beta", Visibility::Org, None);
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    let rel = exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId([7; 16]),
        kind: onto.kind_id("RelatedTo").unwrap(),
        from: a.id,
        to: b.id,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        properties: exocortex_kernel::RelationshipProperties {
            strength: 0.5,
            confidence: 0.5,
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
        lsn: LSN::new_backend(1),
    };
    storage.upsert_relationship(&rel).await.unwrap();
    cache.reseed_from_storage(&storage, &"org".into()).await;
    let relationship_streams_before = storage.reasoning_query_counts().1;

    let inv = exocortex_storage::Invalidation::RelationshipUpserted {
        id: rel.id,
        from: rel.from,
        to: rel.to,
        kind: rel.kind,
        lsn: 2,
    };
    cache.submit(CacheWrite::Apply(inv.clone())).await;
    cache.submit(CacheWrite::Apply(inv)).await;
    cache.flush().await;

    let snap = cache.graphs_snapshot("org").expect("resident");
    let count = snap
        .petgraph
        .edge_indices()
        .filter_map(|eid| snap.petgraph.edge_weight(eid))
        .filter(|w| w.id == rel.id)
        .count();
    assert_eq!(count, 1, "CR5: exactly one edge for the RelationshipId");
    assert_eq!(
        storage.reasoning_query_counts().1,
        relationship_streams_before,
        "relationship invalidations use the indexed point read"
    );
    writer.abort();
}

/// BR-PRD regression (found by the idempotent-import test): repeated
/// upserts of one id must leave exactly ONE live search-arena key.
/// StableGraph reuses freed node indices, so `search_nodes` can hold
/// several slots pointing at the same index (prior incarnations);
/// blanking only the first leaked the later incarnations' keys and
/// served the same memory twice in one search.
#[test]
fn repeated_upsert_does_not_leak_arena_keys() {
    let id = MemoryId::new_v7();
    let mut snap = GraphSnapshot::empty();
    let mut m = mem("leak-check", Visibility::Org, None);
    m.id = id;
    for round in 0..5 {
        m.title = format!("leak-check r{round}").into();
        snap.push_test_memory(m.clone()); // push_test_memory == insert_memory (upsert)
    }
    let vc = VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    };
    let hits = snap.view(&vc).filter(|x| x.id == id).count();
    assert_eq!(hits, 1, "one row, not one per upsert");
    // And the arena itself: only the LAST incarnation's key is live.
    for round in 0..5 {
        let needle = format!("leak-check r{round}");
        let n = snap.search_arena.matches(&needle).count();
        let expected = usize::from(round == 4);
        assert_eq!(
            n, expected,
            "round {round} key occurrences (want {expected})"
        );
    }
}
