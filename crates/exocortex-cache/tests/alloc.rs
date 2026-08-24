//! The no-allocation read-path assertion runs in its own test binary because
//! `stats_alloc` counts process-wide allocations; sibling tests would pollute
//! the counters.

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use std::sync::Arc;

#[global_allocator]
static ALLOC: stats_alloc::StatsAlloc<std::alloc::System> = stats_alloc::StatsAlloc::system();

fn mem(title: &str) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: title.into(),
        content: format!("content {title}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted { author: "t".into() },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
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
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

#[test]
fn read_hot_path_snapshot_load_is_allocation_free() {
    // R-Lat3 spirit: the snapshot load + by-id probe on the read hot path
    // performs zero allocations. (Returning a `Memory` necessarily clones
    // once for the value itself; the §8.4 skeleton clones the node payload,
    // so the probe returns the id lookup only.)
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let mut snap = GraphSnapshot::empty();
    let m = mem("alloc-probe");
    let probe_id = m.id;
    snap.push_test_memory(m);
    cache.publish("org", Arc::new(snap));

    for _ in 0..100 {
        let _ = cache.graphs_snapshot("org");
    }

    let before = ALLOC.stats();
    for _ in 0..1000 {
        let snap = cache.graphs_snapshot("org").expect("resident");
        let _ix = snap.by_id.get(&probe_id);
        drop(_ix);
        drop(snap);
    }
    let after = ALLOC.stats();
    assert_eq!(
        after.allocations - before.allocations,
        0,
        "snapshot load + id probe must not allocate (R-Lat3)"
    );
}
