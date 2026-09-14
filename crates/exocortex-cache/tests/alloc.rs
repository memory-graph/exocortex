//! The no-allocation read-path assertion runs in its own test binary, and
//! its counting allocator is thread-scoped (D30): the `stats_alloc`
//! process-wide counter this replaced let a background thread's one-off
//! allocation land inside the measurement window and flake the gate red
//! in CI (run 34565870901). Only allocations made by the calling thread
//! are counted, so the gate measures the read path alone.

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

thread_local! {
    static THREAD_ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

/// Counts allocations made by the CALLING thread only. `dealloc`,
/// `realloc`, and `alloc_zeroed` need no arms of their own: the default
/// `GlobalAlloc` impls route through `alloc`, which is the only number
/// the gate reads.
struct ThreadCountingAlloc;

unsafe impl GlobalAlloc for ThreadCountingAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        THREAD_ALLOCATIONS.try_with(|c| c.set(c.get() + 1)).ok();
        System.alloc(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOC: ThreadCountingAlloc = ThreadCountingAlloc;

fn thread_allocations() -> u64 {
    THREAD_ALLOCATIONS.with(|c| c.get())
}

fn mem(title: &str) -> Memory {
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: title.into(),
        content: format!("content {title}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
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

    let before = thread_allocations();
    for _ in 0..1000 {
        let snap = cache.graphs_snapshot("org").expect("resident");
        let _ix = snap.by_id.get(&probe_id);
        drop(_ix);
        drop(snap);
    }
    let after = thread_allocations();
    assert_eq!(
        after - before,
        0,
        "snapshot load + id probe must not allocate (R-Lat3)"
    );
}

#[test]
fn foreign_thread_allocations_do_not_count_in_the_window() {
    // D30 regression: the process-wide counter flaked this gate red in CI
    // when a background thread allocated inside the window. Sixty-four
    // foreign allocations are placed deterministically between the two
    // reads — signaled by plain atomics, because a channel send from the
    // measuring thread would itself allocate inside the window — and none
    // of them may move the calling thread's count.
    let go = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let go_foreign = Arc::clone(&go);
    let done_foreign = Arc::clone(&done);
    let noisy = std::thread::spawn(move || {
        while !go_foreign.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        for _ in 0..64 {
            let v = vec![0u8; 4096];
            std::hint::black_box(&v);
        }
        done_foreign.store(true, Ordering::Release);
    });

    let before = thread_allocations();
    go.store(true, Ordering::Release);
    while !done.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    let after = thread_allocations();

    noisy.join().expect("noisy thread finishes cleanly");
    assert_eq!(
        after - before,
        0,
        "foreign-thread allocations must stay outside the R-Lat3 window (D30)"
    );
}
