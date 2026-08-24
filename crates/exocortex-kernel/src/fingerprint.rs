// fingerprint.rs — OntologyFingerprint (§7.17)
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::PackDef;

/// SHA-256 over the kernel definitions plus the sorted set of registered
/// `PackDef`s (R-T21 / R-Pk4).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OntologyFingerprint(pub [u8; 32]);

impl OntologyFingerprint {
    /// Compute the fingerprint over the pack set. Packs are hashed in
    /// name-sorted order, length-prefixed, so the result is independent of
    /// registration order.
    pub fn compute(packs: &[PackDef]) -> Self {
        let mut sorted: Vec<&PackDef> = packs.iter().collect();
        sorted.sort_by(|a, b| a.name.cmp(&b.name));
        let mut h = Sha256::new();
        h.update(b"exocortex-kernel-v1\x1e");
        for p in sorted {
            let bytes = bincode::serialize(p).expect("PackDef must serialize");
            h.update((bytes.len() as u32).to_le_bytes());
            h.update(&bytes);
        }
        let out: [u8; 32] = h.finalize().into();
        Self(out)
    }
}
