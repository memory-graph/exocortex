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
//! identity and revision ride every vector via the configured embedder.

use std::sync::Arc;

/// The backend embedder seam.
pub trait Embedder: Send + Sync {
    /// Embed one `title + "\n" + content` document.
    fn embed(&self, text: &str) -> Result<Vec<f32>, String>;
    /// Embed one admitted ingest batch in a single model invocation. Test
    /// doubles that only implement [`Self::embed`] retain their existing
    /// ergonomics through this deterministic fallback.
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        texts.iter().map(|text| self.embed(text)).collect()
    }
    /// Maximum number of model invocations that may execute concurrently.
    /// Stateful embedders default to one; implementations may explicitly
    /// advertise safe parallelism.
    fn max_concurrency(&self) -> usize {
        1
    }
    /// Stable model name (R-Mcr1/R-Dr5).
    fn model_id(&self) -> &'static str;
    /// Exact model revision stamped beside every stored vector.
    fn model_version(&self) -> &'static str;
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

    fn model_version(&self) -> &'static str {
        "v1"
    }

    fn dim(&self) -> usize {
        self.dim
    }
}

/// Immutable Hugging Face revision supplying the production model.
#[cfg(feature = "fastembed")]
pub const BGE_SMALL_REVISION: &str = "ea104dacec62c0de699686887e3f920caeb4f3e3";
/// Revision-qualified identity stamped beside every production vector.
#[cfg(feature = "fastembed")]
pub const BGE_SMALL_VERSION: &str =
    "hf:Xenova/bge-small-en-v1.5@ea104dacec62c0de699686887e3f920caeb4f3e3";
/// Release-sidecar directory containing the verified model files.
#[cfg(feature = "fastembed")]
pub const BGE_SMALL_DIRECTORY: &str =
    "Xenova_bge-small-en-v1.5-ea104dacec62c0de699686887e3f920caeb4f3e3";

#[cfg(feature = "fastembed")]
const MODEL_FILES: [(&str, usize, &str); 5] = [
    (
        "onnx/model.onnx",
        133_093_490,
        "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
    ),
    (
        "tokenizer.json",
        711_396,
        "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
    ),
    (
        "config.json",
        683,
        "fa73f90bf92c8cace1fbcb709626306f2bdbc9ea3e5b5f94b440df9b6aa56350",
    ),
    (
        "special_tokens_map.json",
        125,
        "b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3",
    ),
    (
        "tokenizer_config.json",
        366,
        "9261e7d79b44c8195c1cada2b453e55b00aeb81e907a6664974b4d7776172ab3",
    ),
];

/// The production embedder: an offline, digest-verified fastembed bge-small
/// model (384-dim), the §16 default backend model.
#[cfg(feature = "fastembed")]
pub struct FastEmbedder {
    model: std::sync::Mutex<fastembed::TextEmbedding>,
}

#[cfg(feature = "fastembed")]
impl FastEmbedder {
    /// Load the pinned bge-small model from its installed release sidecar.
    pub fn bge_small() -> Result<Self, String> {
        let model_dir = resolve_bge_small_directory()?;
        Self::bge_small_from_directory(&model_dir)
    }

    /// Load the pinned model from an explicit directory. All bytes are
    /// verified before they are handed to ONNX Runtime; there is no network
    /// fallback.
    pub fn bge_small_from_directory(model_dir: &std::path::Path) -> Result<Self, String> {
        let mut files = std::collections::HashMap::new();
        for (relative, expected_len, expected_sha256) in MODEL_FILES {
            let path = model_dir.join(relative);
            let bytes = read_verified_artifact(&path, expected_len, expected_sha256)?;
            files.insert(relative, bytes);
        }
        let take = |relative: &'static str,
                    files: &mut std::collections::HashMap<&'static str, Vec<u8>>|
         -> Result<Vec<u8>, String> {
            files
                .remove(relative)
                .ok_or_else(|| format!("verified model artifact disappeared: {relative}"))
        };
        let onnx_file = take("onnx/model.onnx", &mut files)?;
        let tokenizer_files = fastembed::TokenizerFiles {
            tokenizer_file: take("tokenizer.json", &mut files)?,
            config_file: take("config.json", &mut files)?,
            special_tokens_map_file: take("special_tokens_map.json", &mut files)?,
            tokenizer_config_file: take("tokenizer_config.json", &mut files)?,
        };
        let supplied = fastembed::UserDefinedEmbeddingModel::new(onnx_file, tokenizer_files)
            .with_pooling(fastembed::Pooling::Cls);
        let options = fastembed::InitOptionsUserDefined::new().with_max_length(384);
        let model = fastembed::TextEmbedding::try_new_from_user_defined(supplied, options)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            model: std::sync::Mutex::new(model),
        })
    }
}

#[cfg(feature = "fastembed")]
impl Embedder for FastEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        self.embed_batch(&[text.to_owned()])?
            .into_iter()
            .next()
            .ok_or_else(|| "empty embedding".to_string())
    }

    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        let mut model = self.model.lock().map_err(|e| e.to_string())?;
        model
            .embed(texts.to_vec(), None)
            .map_err(|e: fastembed::Error| e.to_string())
    }

    fn model_id(&self) -> &'static str {
        "bge-small"
    }

    fn model_version(&self) -> &'static str {
        BGE_SMALL_VERSION
    }

    fn dim(&self) -> usize {
        384
    }
}

#[cfg(feature = "fastembed")]
fn read_verified_artifact(
    path: &std::path::Path,
    expected_len: usize,
    expected_sha256: &str,
) -> Result<Vec<u8>, String> {
    use std::io::Read as _;

    let file = std::fs::File::open(path)
        .map_err(|error| format!("read model artifact {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("stat model artifact {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("model artifact {} is not a file", path.display()));
    }
    let mut bytes = Vec::with_capacity(expected_len);
    file.take(expected_len as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read model artifact {}: {error}", path.display()))?;
    if bytes.len() != expected_len {
        return Err(format!(
            "model artifact {} has length {}, expected {expected_len}",
            path.display(),
            bytes.len()
        ));
    }
    use sha2::Digest as _;
    let actual_sha256 = format!("{:x}", sha2::Sha256::digest(&bytes));
    if actual_sha256 != expected_sha256 {
        return Err(format!(
            "model artifact {} has sha256 {actual_sha256}, expected {expected_sha256}",
            path.display()
        ));
    }
    Ok(bytes)
}

#[cfg(feature = "fastembed")]
fn resolve_bge_small_directory() -> Result<std::path::PathBuf, String> {
    if let Some(explicit) = std::env::var_os("EXOCORTEX_BGE_SMALL_MODEL_DIR") {
        return Ok(std::path::PathBuf::from(explicit));
    }
    let executable = std::env::current_exe()
        .map_err(|error| format!("resolve current executable for model sidecar: {error}"))?;
    let bin_dir = executable
        .parent()
        .ok_or_else(|| "current executable has no parent directory".to_string())?;
    let candidates = [
        bin_dir.join("models").join(BGE_SMALL_DIRECTORY),
        bin_dir
            .parent()
            .unwrap_or(bin_dir)
            .join("share/exocortex/models")
            .join(BGE_SMALL_DIRECTORY),
        std::path::Path::new("/opt/exocortex/models").join(BGE_SMALL_DIRECTORY),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.is_dir())
        .ok_or_else(|| {
            format!(
                "verified bge-small sidecar {BGE_SMALL_DIRECTORY} not found; set EXOCORTEX_BGE_SMALL_MODEL_DIR"
            )
        })
}

/// Convenience alias for the configured embedder handle.
pub type EmbedderRef = Arc<dyn Embedder>;

#[cfg(all(test, feature = "fastembed"))]
mod fastembed_tests {
    use super::FastEmbedder;

    #[test]
    fn corrupt_or_incomplete_model_sidecar_fails_closed_before_runtime_load() {
        let root =
            std::env::temp_dir().join(format!("exocortex-corrupt-model-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("onnx")).expect("create fixture");
        std::fs::write(root.join("onnx/model.onnx"), b"tampered").expect("write fixture");

        let error = match FastEmbedder::bge_small_from_directory(&root) {
            Ok(_) => panic!("corrupt model was accepted"),
            Err(error) => error,
        };
        assert!(error.contains("has length 8, expected 133093490"));
        std::fs::remove_dir_all(root).expect("remove fixture");
    }
}
