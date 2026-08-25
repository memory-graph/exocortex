//! Shared fixtures for the SDK integration tests.

use exocortex_adapter_sdk::{AdapterConfig, BatchUnit};
use exocortex_wire::ingest::v1::MemoryDraft;

pub fn draft(k: &str) -> MemoryDraft {
    MemoryDraft {
        draft_key: k.into(),
        id: String::new(),
        memory_type: "General".into(),
        title: format!("title {k}"),
        content: "c".into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

pub fn unit(seed: &str, keys: &[&str]) -> BatchUnit {
    BatchUnit {
        batch_id_seed: seed.into(),
        memories: keys.iter().map(|k| draft(k)).collect(),
        relationships: vec![],
        snapshot: None,
        observed_at: std::time::UNIX_EPOCH,
    }
}

pub fn config(url: &str, cursor_path: std::path::PathBuf) -> AdapterConfig {
    let mut c = AdapterConfig::new("org", "custom://test", "test-adapter", url);
    c.cursor_path = cursor_path;
    c
}
