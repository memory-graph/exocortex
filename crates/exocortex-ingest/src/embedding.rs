// crates/exocortex-ingest/src/embedding.rs
//! The backend-assigned embedding step (§7.5): `title + content` is embedded
//! after entity extraction, on the ingest commit path only — the client never
//! computes embeddings (§24 q1) and the interactive read path never touches
//! this (R-Lat3). `Memory.embedding` (R-T8) is what Dreams consumes (§12
//! step 1); without it `select_anchors` returns empty on real data.
//!
//! Two implementations ship: a deterministic fake for tests (no model
//! download) and the fastembed-backed default (bge-small, 384-dim) enabled
//! by the `fastembed` cargo feature. The fake is NOT a substitute in
//! production: cross-model comparison is prohibited (R-Mcr1), so the model
//! identity rides every vector via the configured embedder's `model_id`.

use std::sync::Arc;

/// The backend embedder seam.
pub trait Embedder: Send + Sync {
    /// Embed one `title + "\n" + content` document.
    fn embed(&self, text: &str) -> Result<Vec<f32>, String>;
    /// Stable model identity (stamped on MCR² values, R-Mcr1/R-Dr5).
    fn model_id(&self) -> &'static str;
    /// Vector dimensionality.
    fn dim(&self) -> usize;
}

/// Deterministic test double (§24 q1: no model downloads in unit tests).
/// Bag-of-words hashed into a fixed-dim, L2-normalized vector: same input
/// always yields the same vector; different words land in different buckets
/// with random-sign cancellation, so unrelated texts are near-orthogonal
/// while near-duplicate texts stay near-cosine-1.
pub struct FakeEmbedder {
    /// Output dimensionality (default 64).
    pub dim: usize,
}

impl Default for FakeEmbedder {
    fn default() -> Self {
        Self { dim: 64 }
    }
}

impl Embedder for FakeEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let mut v = vec![0.0f32; self.dim];
        for word in text.split_whitespace() {
            let mut h = blake3::Hasher::new();
            h.update(word.as_bytes());
            let hash = h.finalize();
            let bytes = hash.as_bytes();
            // Two words per bucket pass (position + sign) reduce collisions.
            let bucket = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
            let sign = if bytes[4] & 1 == 0 { 1.0 } else { -1.0 };
            v[bucket % self.dim] += sign;
            let bucket2 = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;
            let sign2 = if bytes[9] & 1 == 0 { 1.0 } else { -1.0 };
            v[bucket2 % self.dim] += sign2;
        }
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        Ok(v)
    }

    fn model_id(&self) -> &'static str {
        "fake-deterministic"
    }

    fn dim(&self) -> usize {
        self.dim
    }
}

/// The production embedder: fastembed bge-small (384-dim), the §16 default
/// backend model. Constructed once at server start behind the backend
/// config flag; construction downloads the model on first use.
#[cfg(feature = "fastembed")]
pub struct FastEmbedder {
    model: std::sync::Mutex<fastembed::TextEmbedding>,
}

#[cfg(feature = "fastembed")]
impl FastEmbedder {
    /// Load the default bge-small model.
    pub fn bge_small() -> Result<Self, String> {
        let model = fastembed::TextEmbedding::try_new(
            fastembed::InitOptions::new(fastembed::EmbeddingModel::BGESmallENV15)
                .with_show_download_progress(false),
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            model: std::sync::Mutex::new(model),
        })
    }
}

#[cfg(feature = "fastembed")]
impl Embedder for FastEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let mut model = self.model.lock().map_err(|e| e.to_string())?;
        let out = model
            .embed(vec![text], None)
            .map_err(|e: fastembed::Error| e.to_string())?;
        out.into_iter()
            .next()
            .ok_or_else(|| "empty embedding".to_string())
    }

    fn model_id(&self) -> &'static str {
        "bge-small"
    }

    fn dim(&self) -> usize {
        384
    }
}

/// Convenience alias for the configured embedder handle.
pub type EmbedderRef = Arc<dyn Embedder>;
