//! R-Lat1-adjacent SLO gate: `k_hop_reason(seed, k=2)` over a 128-node
//! neighborhood — p50 < 300µs, p99 < 2ms on a single core (§10.7 step 6).
//! The storage gather dominates in production; this bench measures the
//! Crepe fixpoint + writeback-skip path with facts pre-harvested.

use std::time::{Duration, Instant};

use exocortex_kernel::{EntityId, MemoryId, RelKindId};
use exocortex_reasoning::rules::{self, Edge, EntityFact, TagFact};

fn percentile(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    let idx = ((p / 100.0) * (samples.len() - 1) as f64).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn main() {
    rules::prime(
        &exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );

    // 128-node neighborhood: a chain of 128 memories with mixed edges.
    let ids: Vec<MemoryId> = (0..128).map(|_| MemoryId::new_v7()).collect();
    let mut edges = Vec::new();
    for w in 0..ids.len().saturating_sub(1) {
        let kind = match w % 4 {
            0 => exocortex_kernel::kinds::SOLVES,
            1 => RelKindId(0x8000_0000 | 2), // DependsOn local slot
            2 => exocortex_kernel::kinds::FIXES,
            _ => RelKindId(0x8000_0000 | 1), // Requires local slot
        };
        edges.push(Edge(ids[w], ids[w + 1], kind));
    }
    let entities: Vec<EntityFact> = ids
        .iter()
        .enumerate()
        .map(|(i, m)| EntityFact(*m, EntityId([0u8; 15].concat_saturate(i))))
        .collect();
    let tags: Vec<TagFact> = ids
        .iter()
        .enumerate()
        .map(|(i, m)| TagFact(*m, i as u32 / 16))
        .collect();

    // Warm-up.
    for _ in 0..20 {
        let _ = rules::evaluate(edges.clone(), entities.clone(), tags.clone());
    }

    let mut samples = Vec::with_capacity(500);
    for _ in 0..500 {
        let t0 = Instant::now();
        let derived = rules::evaluate(edges.clone(), entities.clone(), tags.clone());
        let dt = t0.elapsed();
        assert!(derived.total() > 0);
        samples.push(dt);
    }
    let p50 = percentile(samples.clone(), 50.0);
    let p99 = percentile(samples.clone(), 99.0);
    println!("k-hop fixpoint over 128 nodes: p50={p50:?} p99={p99:?}");
    // Shared-CI tolerance (R-Lat1 budgets assume dedicated hardware):
    // `SLO_MULTIPLIER` relaxes BOTH budgets by the same factor so noisy
    // public runners gate regressions without tripping on steal time.
    // Local/dev runs keep the bare budget (multiplier 1.0).
    let m = std::env::var("SLO_MULTIPLIER")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(1.0, 3.0);
    let ok =
        p50 < Duration::from_micros(300).mul_f64(m) && p99 < Duration::from_millis(2).mul_f64(m);
    if m != 1.0 {
        println!("SLO budgets relaxed x{m} (SLO_MULTIPLIER)");
    }
    println!(
        "SLO gate (§10.7 step 6): {}",
        if ok { "PASS" } else { "FAIL" }
    );
    if !ok {
        std::process::exit(1);
    }
}

trait ConcatSaturate {
    fn concat_saturate(self, i: usize) -> [u8; 16];
}

impl ConcatSaturate for [u8; 15] {
    fn concat_saturate(self, i: usize) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..15].copy_from_slice(&self);
        out[15] = (i % 256) as u8;
        out
    }
}
