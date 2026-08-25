// crates/exocortex-wire/src/lib.rs
//! Cluster-internal wire schemas. This crate never links `exocortex-kernel`,
//! so it can be depended on by `exocortex-worker` without dragging the kernel
//! into out-of-process adapters.
//!
//! Each proto package is nested under its surface module + `v1`, matching the
//! `exocortex_wire::ingest::v1::*` paths used by the §13.5/§18.7 skeletons and
//! letting prost's cross-package references (`super::super::sse::v1::*`) resolve.

/// Wire-schema version for peer admission (R-W2/R-W3). Additive-only
/// evolution; peers with mismatched wire versions refuse to peer.
pub const WIRE_VERSION: u32 = 1;

/// `exocortex.ingest.v1` — the Ingestion Protocol (§18.6).
pub mod ingest {
    /// Protocol buffers for `exocortex.ingest.v1`.
    pub mod v1 {
        tonic::include_proto!("exocortex.ingest.v1");
    }
}
/// `exocortex.cluster.v1` — the cluster invalidation envelope (§2.6.3).
pub mod cluster {
    /// Protocol buffers for `exocortex.cluster.v1`.
    pub mod v1 {
        tonic::include_proto!("exocortex.cluster.v1");
    }
}
/// Canonical batch-integrity helpers (§18.1): checksum + HMAC, the single
/// workspace implementation shared by every producer and the server.
pub mod signing;

/// `exocortex.sse.v1` — SSE change-feed events (§2.6.3).
pub mod sse {
    /// Protocol buffers for `exocortex.sse.v1`.
    pub mod v1 {
        tonic::include_proto!("exocortex.sse.v1");
    }
}
