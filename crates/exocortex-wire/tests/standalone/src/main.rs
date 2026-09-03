//! Constructs a valid, signed IngestBatch using nothing but
//! exocortex-wire — the whole adapter-reachable surface (R2).

use exocortex_wire::ingest::v1::{
    ExternalSnapshotInfo, IngestBatch, MemoryDraft, ProducerIdentity,
};
use exocortex_wire::signing;

fn main() {
    let mut batch = IngestBatch {
        org_id: "org".into(),
        source_uri: "custom://standalone".into(),
        producer_id: "standalone-fixture".into(),
        batch_id: "standalone-fixture:w1:0".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: vec![1u8; 32],
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: "s1".into(),
            schema_hash: vec![2u8; 32],
            source_flavor: "custom".into(),
        }),
        memories: vec![MemoryDraft {
            draft_key: "k1".into(),
            id: String::new(),
            memory_type: "General".into(),
            title: "standalone".into(),
            content: "built outside the workspace".into(),
            tags: vec![],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: None,
            rights: None,
        }],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    signing::prepare_batch(&[3u8; 32], &mut batch);
    assert!(!batch.checksum.is_empty());
    assert!(signing::verify_signature(&[3u8; 32], &batch));
    assert_eq!(
        batch.checksum,
        signing::canonical_checksum(&batch),
        "checksum is stable across recomputation"
    );
    println!("standalone fixture built a valid signed batch");
}
