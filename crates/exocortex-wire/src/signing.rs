// crates/exocortex-wire/src/signing.rs
//! The canonical batch-integrity helpers (§18.1 / §13.6 step 3).
//!
//! One implementation, shared by every producer and by the ingest server:
//! - [`canonical_checksum`]: BLAKE3 hex over an order-independent
//!   canonical projection of the batch's covered fields. Two batches with
//!   the same rows in different orders checksum identically; any change
//!   to a covered field changes the checksum.
//! - [`sign_batch`] / [`verify_signature`]: HMAC-SHA256 over the full
//!   encoded batch (the wire's existing R-I8 envelope), applied after the
//!   checksum is set.
//! - [`prepare_batch`]: checksum-then-sign, the producer-side order.
//!
//! `Hmac<Sha256>` lives here and nowhere else in the workspace — a second
//! copy in a producer crate is how client and server silently diverge.

use hmac::{Hmac, Mac};
use prost::Message;

use crate::ingest::v1::IngestBatch;

type HmacSha256 = Hmac<sha2::Sha256>;

/// Version tag baked into the checksum preimage so a deliberate format
/// change is visible: old and new checksums differ even for identical rows.
const CHECKSUM_VERSION: &str = "exocortex-checksum-v1";

/// Canonical BLAKE3-hex checksum over the batch's covered fields (§18.1):
/// `memories` (all fields incl. tags, external_key, valid_from/until),
/// `relationships`, and `snapshot`. Order-independent within each
/// collection — producers may buffer rows in any order.
pub fn canonical_checksum(b: &IngestBatch) -> String {
    let mut memories: Vec<String> = b.memories.iter().map(canonical_memory).collect();
    memories.sort();
    let mut relationships: Vec<String> =
        b.relationships.iter().map(canonical_relationship).collect();
    relationships.sort();
    let snapshot = b
        .snapshot
        .as_ref()
        .map(canonical_snapshot)
        .unwrap_or_else(|| "none".to_string());

    let mut preimage = String::with_capacity(64 + memories.len() * 128);
    preimage.push_str(CHECKSUM_VERSION);
    preimage.push('\x1e');
    preimage.push_str(&memories.join("\x1e"));
    preimage.push('\x1e');
    preimage.push_str(&relationships.join("\x1e"));
    preimage.push('\x1e');
    preimage.push_str(&snapshot);

    blake3::hash(preimage.as_bytes()).to_hex().to_string()
}

/// Producer-side ordering: set the checksum, then sign the full batch
/// (the signature covers the checksum field itself).
pub fn prepare_batch(key: &[u8; 32], b: &mut IngestBatch) {
    b.checksum = canonical_checksum(b);
    sign_batch(key, b);
}

/// The bytes a signature covers: the fully encoded batch with the
/// signature field itself cleared (the signature cannot cover itself).
/// Signing and verification both hash exactly this form.
fn unsigned_bytes(b: &IngestBatch) -> Vec<u8> {
    let mut unsigned = b.clone();
    if let Some(p) = unsigned.producer.as_mut() {
        p.hmac_signature = vec![];
    }
    unsigned.encode_to_vec()
}

/// HMAC-SHA256 over the full encoded batch, stored on the producer
/// identity (R-I8). Idempotent: re-signing replaces the signature.
pub fn sign_batch(key: &[u8; 32], b: &mut IngestBatch) {
    let bytes = unsigned_bytes(b);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(&bytes);
    if let Some(p) = b.producer.as_mut() {
        p.hmac_signature = mac.finalize().into_bytes().to_vec();
    }
}

/// Constant-time verification of a batch's producer signature.
pub fn verify_signature(key: &[u8; 32], b: &IngestBatch) -> bool {
    let Some(p) = &b.producer else {
        return false;
    };
    if p.hmac_signature.is_empty() {
        return false;
    }
    let bytes = unsigned_bytes(b);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(&bytes);
    let expected = mac.finalize().into_bytes();
    expected.len() == p.hmac_signature.len()
        && bool::from(subtle::ConstantTimeEq::ct_eq(
            expected.as_slice(),
            p.hmac_signature.as_slice(),
        ))
}

fn canonical_memory(m: &crate::ingest::v1::MemoryDraft) -> String {
    let mut tags: Vec<&str> = m.tags.iter().map(|t| t.as_str()).collect();
    tags.sort_unstable();
    let external_key = match &m.external_key {
        Some(k) => format!(
            "{{table_uuid:{},logical_pk:{},mapping_version:{}}}",
            hex(&k.table_uuid),
            hex(k.logical_pk.as_bytes()),
            k.mapping_version
        ),
        None => "none".to_string(),
    };
    format!(
        "{{draft_key:{},id:{},memory_type:{},title:{},content:{},tags:{},visibility:{},valid_from:{:?},valid_until:{:?},external_key:{}}}",
        m.draft_key,
        m.id,
        m.memory_type,
        m.title,
        m.content,
        tags.join(","),
        m.visibility,
        m.valid_from.map(|t| (t.seconds, t.nanos)),
        m.valid_until.map(|t| (t.seconds, t.nanos)),
        external_key,
    )
}

fn canonical_relationship(r: &crate::ingest::v1::RelationshipDraft) -> String {
    format!(
        "{{from:{},to:{},kind:{},strength:{},confidence:{},context:{},visibility:{}}}",
        r.from_draft_key, r.to_draft_key, r.kind, r.strength, r.confidence, r.context, r.visibility
    )
}

fn canonical_snapshot(s: &crate::ingest::v1::ExternalSnapshotInfo) -> String {
    format!(
        "{{snapshot_id:{},schema_hash:{},source_flavor:{}}}",
        s.snapshot_id,
        hex(&s.schema_hash),
        s.source_flavor
    )
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::v1::{ExternalKey, MemoryDraft, ProducerIdentity};

    fn draft(key: &str, title: &str) -> MemoryDraft {
        MemoryDraft {
            draft_key: key.into(),
            id: String::new(),
            memory_type: "General".into(),
            title: title.into(),
            content: format!("content {title}"),
            tags: vec!["b".into(), "a".into()],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: None,
        }
    }

    fn batch(memories: Vec<MemoryDraft>) -> IngestBatch {
        IngestBatch {
            org_id: "org".into(),
            source_uri: "session://t".into(),
            producer_id: "p".into(),
            batch_id: "b1".into(),
            mapping_version: "1".into(),
            ontology_fingerprint: vec![1u8; 32],
            ceiling: 3,
            checksum: String::new(),
            observed_at: None,
            recorded_at: None,
            snapshot: None,
            memories,
            relationships: vec![],
            producer: Some(ProducerIdentity {
                node_id: "n".into(),
                agent_id: "a".into(),
                adapter_id: String::new(),
                hmac_signature: vec![],
            }),
        }
    }

    #[test]
    fn checksum_is_order_independent() {
        let a = batch(vec![draft("k1", "one"), draft("k2", "two")]);
        let b = batch(vec![draft("k2", "two"), draft("k1", "one")]);
        assert_eq!(canonical_checksum(&a), canonical_checksum(&b));
    }

    #[test]
    fn checksum_covers_every_memory_field() {
        let base = || batch(vec![draft("k1", "one")]);
        type Mutation = Box<dyn Fn(&mut MemoryDraft)>;
        let mutations: Vec<(&str, Mutation)> = vec![
            (
                "draft_key",
                Box::new(|m: &mut MemoryDraft| m.draft_key = "k9".into()),
            ),
            (
                "memory_type",
                Box::new(|m: &mut MemoryDraft| m.memory_type = "Task".into()),
            ),
            (
                "title",
                Box::new(|m: &mut MemoryDraft| m.title = "ONE".into()),
            ),
            (
                "content",
                Box::new(|m: &mut MemoryDraft| m.content = "x".into()),
            ),
            (
                "tags",
                Box::new(|m: &mut MemoryDraft| m.tags = vec!["z".into()]),
            ),
            (
                "visibility",
                Box::new(|m: &mut MemoryDraft| m.visibility = 1),
            ),
            (
                "external_key",
                Box::new(|m: &mut MemoryDraft| {
                    m.external_key = Some(ExternalKey {
                        table_uuid: vec![7u8; 16],
                        logical_pk: "pk".into(),
                        mapping_version: 2,
                    })
                }),
            ),
            (
                "valid_from",
                Box::new(|m: &mut MemoryDraft| {
                    m.valid_from = Some(prost_types::Timestamp {
                        seconds: 12,
                        nanos: 0,
                    })
                }),
            ),
            (
                "valid_until",
                Box::new(|m: &mut MemoryDraft| {
                    m.valid_until = Some(prost_types::Timestamp {
                        seconds: 99,
                        nanos: 0,
                    })
                }),
            ),
        ];
        for (name, mutate) in mutations {
            let mut m = batch(vec![draft("k1", "one")]);
            mutate(&mut m.memories[0]);
            assert_ne!(
                canonical_checksum(&base()),
                canonical_checksum(&m),
                "field {name} must be covered"
            );
        }
    }

    #[test]
    fn checksum_covers_relationships_and_snapshot() {
        let base = batch(vec![draft("k1", "one")]);
        let mut with_rel = base.clone();
        with_rel.relationships = vec![crate::ingest::v1::RelationshipDraft {
            from_draft_key: "k1".into(),
            to_draft_key: "k1".into(),
            kind: "Solves".into(),
            strength: 0.5,
            confidence: 0.5,
            context: String::new(),
            visibility: 3,
        }];
        assert_ne!(canonical_checksum(&base), canonical_checksum(&with_rel));

        let mut with_snap = base.clone();
        with_snap.snapshot = Some(crate::ingest::v1::ExternalSnapshotInfo {
            snapshot_id: "s1".into(),
            schema_hash: vec![1u8; 32],
            source_flavor: "custom".into(),
        });
        assert_ne!(canonical_checksum(&base), canonical_checksum(&with_snap));
    }

    #[test]
    fn sign_verify_round_trip() {
        let key = [9u8; 32];
        let mut b = batch(vec![draft("k1", "one")]);
        prepare_batch(&key, &mut b);
        assert!(verify_signature(&key, &b));
        assert!(!b.checksum.is_empty(), "prepare sets the checksum");

        // Flip one signature byte.
        if let Some(p) = b.producer.as_mut() {
            p.hmac_signature[0] ^= 1;
        }
        assert!(!verify_signature(&key, &b));

        // Wrong key.
        let mut c = batch(vec![draft("k1", "one")]);
        prepare_batch(&[9u8; 32], &mut c);
        assert!(!verify_signature(&[8u8; 32], &c));
    }
}
