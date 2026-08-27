//! Conversion between storage `Invalidation` events and the
//! `exocortex.sse.v1` protobuf shape carried inside envelopes.

use exocortex_storage::Invalidation as StorageInv;
use exocortex_wire::sse::v1 as pb;

fn hex(id: &[u8; 16]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(32);
    for b in id {
        out.push(DIGITS[(b >> 4) as usize]);
        out.push(DIGITS[(b & 0xF) as usize]);
    }
    out
}

/// Storage invalidation -> protobuf (§2.6.3 sse.proto).
pub fn invalidation_to_pb(inv: &StorageInv) -> pb::Invalidation {
    match inv {
        StorageInv::MemoryUpserted { id, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::MemoryUpserted(pb::MemoryUpserted {
                id: hex(&id.0),
                snapshot_json: vec![],
            })),
            backend_lsn: *lsn,
        },
        StorageInv::MemoryDeleted { id, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::MemoryDeleted(pb::MemoryDeleted {
                id: hex(&id.0),
            })),
            backend_lsn: *lsn,
        },
        StorageInv::RelationshipUpserted {
            id,
            from,
            to,
            kind,
            lsn,
        } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::RelationshipUpserted(
                pb::RelationshipUpserted {
                    id: hex(&id.0),
                    from: hex(&from.0),
                    to: hex(&to.0),
                    kind: kind.0,
                    snapshot_json: vec![],
                },
            )),
            backend_lsn: *lsn,
        },
        StorageInv::RelationshipDeleted { id, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::RelationshipDeleted(
                pb::RelationshipDeleted { id: hex(&id.0) },
            )),
            backend_lsn: *lsn,
        },
        StorageInv::VisibilityAdvance { lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::VisibilityAdvance(
                pb::VisibilityAdvance {},
            )),
            backend_lsn: *lsn,
        },
        StorageInv::MemorySnapshotUpserted { memory, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::MemoryUpserted(pb::MemoryUpserted {
                id: hex(&memory.id.0),
                snapshot_json: serde_json::to_vec(memory.as_ref())
                    .expect("kernel memory serializes"),
            })),
            backend_lsn: *lsn,
        },
        StorageInv::RelationshipSnapshotUpserted { relationship, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::RelationshipUpserted(
                pb::RelationshipUpserted {
                    id: hex(&relationship.id.0),
                    from: hex(&relationship.from.0),
                    to: hex(&relationship.to.0),
                    kind: relationship.kind.0,
                    snapshot_json: serde_json::to_vec(relationship.as_ref())
                        .expect("kernel relationship serializes"),
                },
            )),
            backend_lsn: *lsn,
        },
        StorageInv::GraphReseed { snapshot_json, lsn } => pb::Invalidation {
            kind: Some(pb::invalidation::Kind::GraphReseed(pb::GraphReseed {
                snapshot_json: snapshot_json.clone(),
            })),
            backend_lsn: *lsn,
        },
    }
}
