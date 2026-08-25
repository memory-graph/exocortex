// ids.rs
use serde::{Deserialize, Serialize};

/// 128-bit deterministic memory identity. See §7.14 R-T18a.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MemoryId(pub [u8; 16]);

impl MemoryId {
    /// Derive a `MemoryId` from external-source coordinates (R-T18a).
    ///
    /// `MemoryId = blake3(org_id || source_uri || table_uuid || logical_pk || mapping_version)[..16]`
    ///
    /// `table_uuid` is raw bytes (§18.6: 16 uniformly random bytes —
    /// overwhelmingly not valid UTF-8). It MUST NOT pass through a lossy
    /// string conversion first: `String::from_utf8_lossy` normalizes
    /// distinct invalid UUIDs onto the same replacement-char string,
    /// silently colliding identities (B8).
    pub fn from_external(
        org_id: &str,
        source_uri: &str,
        table_uuid: &[u8],
        logical_pk: &[u8],
        mapping_version: u32,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(org_id.as_bytes());
        hasher.update(b"\x1e"); // record separator
        hasher.update(source_uri.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(table_uuid);
        hasher.update(b"\x1e");
        hasher.update(logical_pk);
        hasher.update(b"\x1e");
        hasher.update(&mapping_version.to_le_bytes());
        let hash = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&hash.as_bytes()[..16]);
        Self(out)
    }

    /// Fallback for adapters that cannot supply an `ExternalKey` — content hash.
    /// Documented limitation, not a general strategy. See §7.14.
    pub fn from_content_hash(org_id: &str, content: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"content-hash-v1\x1e");
        hasher.update(org_id.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(content.as_bytes());
        let mut out = [0u8; 16];
        out.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self(out)
    }

    /// Random v7 UUID-shaped id for locally-created (Asserted) memories.
    pub fn new_v7() -> Self {
        Self(*uuid::Uuid::now_v7().as_bytes())
    }
}

/// Relationship identity — always derived (never external-keyed).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RelationshipId(pub [u8; 16]);

impl RelationshipId {
    /// `RelationshipId = blake3(from || kind || to || snapshot_id_or_zero)[..16]`.
    /// Deterministic so re-derivation of the same edge in the same snapshot
    /// is idempotent.
    pub fn derive(
        from: MemoryId,
        kind: super::RelKindId,
        to: MemoryId,
        snapshot: Option<&str>,
    ) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(&from.0);
        h.update(&kind.0.to_le_bytes());
        h.update(&to.0);
        h.update(snapshot.unwrap_or("").as_bytes());
        let mut out = [0u8; 16];
        out.copy_from_slice(&h.finalize().as_bytes()[..16]);
        Self(out)
    }
}

/// Entity id — content-hash of `(entity_type, canonical_name)` scoped by org.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EntityId(pub [u8; 16]);

impl EntityId {
    /// Deterministic entity id: `blake3(entity_type || canonical_name)[..16]`
    /// scoped by org (R-T18, §7.2).
    pub fn from_parts(org_id: &str, entity_type: u8, canonical_name: &str) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(b"entity-v1\x1e");
        h.update(org_id.as_bytes());
        h.update(b"\x1e");
        h.update(&entity_type.to_le_bytes());
        h.update(b"\x1e");
        h.update(canonical_name.as_bytes());
        let mut out = [0u8; 16];
        out.copy_from_slice(&h.finalize().as_bytes()[..16]);
        Self(out)
    }
}

/// Pack identity — 16-bit registry id assigned at kernel load time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PackId(pub u16);

/// Local or backend LSN. `LSN::new_local(0)` is reserved for pre-init.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LSN {
    /// Which LSN space this sequence number belongs to.
    pub space: LsnSpace,
    /// Monotonic sequence number within the space.
    pub value: u64,
}

/// The two LSN spaces (§6.2): client-local WAL and backend graph log.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum LsnSpace {
    /// Assigned by the client's local WAL. Monotonic per client.
    Local,
    /// Assigned by the backend's FalkorDB replication log. Monotonic per graph.
    Backend,
}

impl LSN {
    /// Construct a local-space LSN.
    pub fn new_local(v: u64) -> Self {
        Self {
            space: LsnSpace::Local,
            value: v,
        }
    }
    /// Construct a backend-space LSN.
    pub fn new_backend(v: u64) -> Self {
        Self {
            space: LsnSpace::Backend,
            value: v,
        }
    }
}
