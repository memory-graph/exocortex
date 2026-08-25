//! R19: golden batches — `(fixture input, config) → IngestBatch` is
//! byte-stable across runs. Regeneration:
//!
//! ```sh
//! cargo test -p exocortex-adapter-sdk --features testing --test golden -- --ignored regenerate
//! ```
//!
//! A deliberate field change must regenerate consciously (visible diff),
//! never drift silently.

#![cfg(feature = "testing")]

mod common;

use exocortex_adapter_sdk::split::split_unit;
use prost::Message;

const GOLDEN_DIR: &str = "tests/golden";

fn units() -> Vec<exocortex_adapter_sdk::BatchUnit> {
    vec![
        common::unit("seed-alpha", &["k1", "k2"]),
        common::unit("seed-beta", &["k1", "k2", "k3", "k4", "k5"]),
    ]
}

fn encode_all() -> Vec<Vec<u8>> {
    units()
        .iter()
        .flat_map(|u| split_unit("test-adapter", u, 1024).unwrap())
        .map(|b| b.encode_to_vec())
        .collect()
}

#[test]
fn batches_match_the_golden_files() {
    let encodings = encode_all();
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_DIR);
    for (i, bytes) in encodings.iter().enumerate() {
        let golden = std::fs::read(dir.join(format!("batch-{i}.binpb"))).unwrap_or_else(|e| {
            panic!(
                "golden batch-{i}.binpb missing ({e}); regenerate with \
                 `cargo test -p exocortex-adapter-sdk --features testing --test golden -- --ignored regenerate`"
            )
        });
        assert_eq!(
            &golden, bytes,
            "batch {i} diverged from the golden bytes — if intentional, regenerate"
        );
    }
    // No stale goldens beyond the emitted count.
    let count = std::fs::read_dir(&dir)
        .map(|d| d.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    assert_eq!(
        count,
        encodings.len(),
        "golden dir has exactly the emitted batches"
    );
}

#[test]
#[ignore = "regenerates the golden .binpb files"]
fn regenerate() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_DIR);
    std::fs::create_dir_all(&dir).unwrap();
    for f in std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
        std::fs::remove_file(f.path()).unwrap();
    }
    for (i, bytes) in encode_all().iter().enumerate() {
        std::fs::write(dir.join(format!("batch-{i}.binpb")), bytes).unwrap();
    }
}
