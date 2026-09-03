//! R-Lat1 SLO gate: `search_memories` p50 < 0.5ms, p99 < 3ms on a 100k-memory
//! synthetic dataset (§3 M3, §15). Hand-rolled histogram — no bench framework
//! dependency. Run with `cargo bench -p exocortex-cache` or
//! `cargo xtask bench`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};

fn synth_memory(i: usize) -> Memory {
    let words = [
        "auth", "parser", "cache", "cluster", "storage", "wire", "kernel", "dreams",
    ];
    let title = format!("{} fix number {}", words[i % words.len()], i);
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: (i % 13) as u8,
        title: title.into(),
        content: format!(
            "content {i} with some padding text {}",
            "lorem ipsum ".repeat(4)
        ),
        summary: None,
        tags: std::iter::once(words[i % words.len()])
            .map(Into::into)
            .collect(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "bench".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some("bench".into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("bench-org".into()),
            session_id: None,
            user_id: Some("bench-user".into()),
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

fn vc() -> exocortex_storage::VisibilityContext {
    exocortex_storage::VisibilityContext {
        user_id: "bench-user".into(),
        org_id: "bench-org".into(),
        project_ids: ["bench".into()].into_iter().collect(),
        team_ids: Default::default(),
        max_visibility: Visibility::Org,
    }
}

fn percentile(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    let idx = ((p / 100.0) * (samples.len() - 1) as f64).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn main() {
    const N: usize = 100_000;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Build the 100k snapshot (insert_memory is private; go through a
    // Reseed publish, which is the production build path).
    let (cache, _rx) = LocalCache::new(2 * 1024 * 1024 * 1024);
    let snapshot = {
        // Use the public builder via storage-less insertion path: LocalCache
        // publish requires a CacheWrite::Reseed, which needs a snapshot. The
        // test-support constructor below fills one directly.
        let mut snap = GraphSnapshot::empty();
        for i in 0..N {
            snap.push_test_memory(synth_memory(i));
        }
        Arc::new(snap)
    };
    let _ = rt;
    cache.publish("bench-org", snapshot);
    let ctx = vc();
    let queries = ["auth fix", "parser", "cluster", "number 999", "wire"];

    // Warm up.
    for _ in 0..50 {
        let _ = cache.search("bench-org", queries[0], 20, &ctx);
    }

    let mut samples: Vec<Duration> = Vec::with_capacity(2000);
    for i in 0..2000 {
        let q = queries[i % queries.len()];
        let t0 = Instant::now();
        let hits = cache.search("bench-org", q, 20, &ctx);
        let dt = t0.elapsed();
        assert!(!hits.is_empty(), "query {q} must hit at least once");
        samples.push(dt);
    }

    let p50 = percentile(samples.clone(), 50.0);
    let p99 = percentile(samples.clone(), 99.0);
    println!("search_memories over {N} memories: p50={p50:?} p99={p99:?}");

    // Shared-CI tolerance — see the khop bench for rationale.
    let m = std::env::var("SLO_MULTIPLIER")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(1.0, 3.0);
    // KP5 (audit): the budgets come from the kernel's typed Function
    // surface — the bench cannot silently drift from the declared SLO.
    use exocortex_kernel::functions::{Function, SearchMemories as SearchFn};
    let p50_budget = Duration::from_micros(<SearchFn as Function>::P50_BUDGET_US as u64).mul_f64(m);
    let p99_budget = Duration::from_micros(<SearchFn as Function>::P99_BUDGET_US as u64).mul_f64(m);
    let ok = p50 < p50_budget && p99 < p99_budget;
    if m != 1.0 {
        println!("SLO budgets relaxed x{m} (SLO_MULTIPLIER)");
    }
    println!("SLO gate (R-Lat1): {}", if ok { "PASS" } else { "FAIL" });
    if !ok {
        std::process::exit(1);
    }
}
