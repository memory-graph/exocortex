//! The MCR² engine (§11.6) and the graph-sparsity diagnostic (§11.6.1).
//! ΔR = R(Z) − R^c(Z|Π) per Yu et al. NeurIPS 2020; computed via
//! log-determinants of covariance matrices (Ma et al. TPAMI 2007 is the
//! coding-rate function origin). Diagnostic only — never a training
//! objective, never on the interactive path (R-Mcr4).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use exocortex_kernel::{Embedding, EmbeddingModel, MemoryId};

/// Embedding model identity for version stamping (R-Mcr1/R-Dr5).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EmbeddingModelId {
    /// Model name (e.g. "bge-small").
    pub name: String,
    /// Model version.
    pub version: String,
}

impl EmbeddingModelId {
    /// The v1 default backend model.
    pub fn bge_small() -> Self {
        Self {
            name: "bge-small".into(),
            version: "v1".into(),
        }
    }
}

impl From<&EmbeddingModel> for EmbeddingModelId {
    fn from(model: &EmbeddingModel) -> Self {
        Self {
            name: model.name.to_string(),
            version: model.version.to_string(),
        }
    }
}

/// One memory with its embedding, the engine's input.
#[derive(Clone, Debug)]
pub struct MemoryWithEmbedding {
    /// The memory id.
    pub id: MemoryId,
    /// Partition class (memory type id).
    pub class: u8,
    /// The model-stamped embedding vector.
    pub embedding: Embedding,
}

/// A computed MCR² value with full provenance (§11.6).
#[derive(Clone, Debug)]
pub struct MCR2Value {
    /// ΔR = total − compact.
    pub delta_r: f32,
    /// R(Z), total coding rate.
    pub total_rate: f32,
    /// Per-class R^c values.
    pub class_rates: HashMap<u8, f32>,
    /// R^c(Z|Π), the weighted compact rate.
    pub compact_rate: f32,
    /// Number of memories scored.
    pub n_memories: usize,
    /// Model identity (R-Mcr1).
    pub embedding_model: EmbeddingModelId,
    /// When this was computed.
    pub computed_at: DateTime<Utc>,
}

/// Errors from the engine.
#[derive(Debug, thiserror::Error)]
pub enum MCR2Error {
    /// Mixing models in one computation (R-Mcr1).
    #[error("cross-model comparison prohibited (R-Mcr1)")]
    CrossModelComparison,
    /// Too few points to score.
    #[error("too few memories to score (need >= {0})")]
    TooFew(usize),
    /// Vectors from one declared model must share one dimensionality.
    #[error("embedding dimensions differ within one model revision")]
    DimensionMismatch,
}

/// The engine (§11.6). `epsilon` default 0.5.
pub struct MCR2Engine {
    /// Distortion parameter ε.
    pub epsilon: f32,
}

impl Default for MCR2Engine {
    fn default() -> Self {
        Self { epsilon: 0.5 }
    }
}

/// Cholesky-based log-det of (I + α·XᵀX) for a row matrix X (d columns).
/// Returns 0.0 for the empty matrix (empty class contributes nothing).
fn log_det_cov(rows: &[&[f32]], epsilon: f32) -> f32 {
    let d = rows.first().map(|r| r.len()).unwrap_or(0);
    if d == 0 || rows.is_empty() {
        return 0.0;
    }
    let n = rows.len() as f32;
    // PRD §11.2: R(Z) = ½ log det(I + (d/(n ε²))·ZZᵀ). Accumulate the Gram
    // matrix XᵀX directly, so alpha = d/(n ε²) — no extra 1/n.
    let alpha = d as f32 / (n * epsilon * epsilon);
    let mut a = vec![vec![0.0f64; d]; d];
    for r in rows {
        for (i, row) in a.iter_mut().enumerate().take(d) {
            for (j, cell) in row.iter_mut().enumerate().take(d) {
                *cell += (r[i] as f64) * (r[j] as f64);
            }
        }
    }
    for row in a.iter_mut().take(d) {
        for cell in row.iter_mut().take(d) {
            *cell *= alpha as f64;
        }
    }
    for (i, row) in a.iter_mut().enumerate().take(d) {
        row[i] += 1.0;
    }
    // Cholesky; on numerical failure (rank deficient), add ridge.
    for attempt in 0..4 {
        let ridge = [0.0, 1e-8, 1e-6, 1e-4][attempt];
        let mut l = vec![vec![0.0f64; d]; d];
        let mut ok = true;
        'outer: for i in 0..d {
            for j in 0..=i {
                let mut sum = a[i][j] + if i == j { ridge } else { 0.0 };
                for k in 0..j {
                    sum -= l[i][k] * l[j][k];
                }
                if i == j {
                    if sum <= 0.0 {
                        ok = false;
                        break 'outer;
                    }
                    l[i][j] = sum.sqrt();
                } else {
                    l[i][j] = sum / l[j][j];
                }
            }
        }
        if ok {
            let mut logdet = 0.0f64;
            for (i, row) in l.iter().enumerate().take(d) {
                logdet += 2.0 * row[i].ln();
            }
            return (0.5 * logdet) as f32;
        }
    }
    0.0
}

impl MCR2Engine {
    /// Compute ΔR over the memory set partitioned by class (memory type).
    pub fn compute(&self, memories: &[MemoryWithEmbedding]) -> Result<MCR2Value, MCR2Error> {
        if memories.len() < 2 {
            return Err(MCR2Error::TooFew(2));
        }
        let model = &memories[0].embedding.model;
        if memories
            .iter()
            .any(|memory| &memory.embedding.model != model)
        {
            return Err(MCR2Error::CrossModelComparison);
        }
        let d = memories[0].embedding.vector.len();
        if memories
            .iter()
            .any(|memory| memory.embedding.vector.len() != d)
        {
            return Err(MCR2Error::DimensionMismatch);
        }
        let all: Vec<&[f32]> = memories
            .iter()
            .map(|m| m.embedding.vector.as_slice())
            .collect();
        let total = log_det_cov(&all, self.epsilon);

        let mut by_class: HashMap<u8, Vec<&[f32]>> = HashMap::new();
        for m in memories {
            by_class
                .entry(m.class)
                .or_default()
                .push(m.embedding.vector.as_slice());
        }
        let mut class_rates = HashMap::new();
        let mut compact = 0.0f32;
        let n = memories.len() as f32;
        for (class, rows) in &by_class {
            let rate = log_det_cov(rows, self.epsilon);
            let weight = rows.len() as f32 / n;
            class_rates.insert(*class, rate);
            compact += weight * rate;
        }
        Ok(MCR2Value {
            delta_r: total - compact,
            total_rate: total,
            class_rates,
            compact_rate: compact,
            n_memories: memories.len(),
            embedding_model: model.into(),
            computed_at: Utc::now(),
        })
    }

    /// Heuristic: intra-class variance high relative to inter-class
    /// distance means consolidation would help (R-Mcr2: a hint).
    pub fn should_consolidate(&self, current: &MCR2Value) -> bool {
        current.delta_r < 0.0 || current.compact_rate / current.total_rate.max(1e-6) > 0.98
    }

    /// Ranked merge candidates: same-class pairs whose merge is predicted to
    /// increase ΔR the most (approximated by cosine similarity; exact
    /// rank-1 updates arrive with the Dreams integration).
    pub fn identify_merge_candidates(
        &self,
        memories: &[MemoryWithEmbedding],
    ) -> Vec<MergeCandidate> {
        let mut out = Vec::new();
        for i in 0..memories.len() {
            for j in (i + 1)..memories.len() {
                let (a, b) = (&memories[i], &memories[j]);
                if a.class != b.class {
                    continue;
                }
                let sim = cosine(&a.embedding.vector, &b.embedding.vector);
                if sim > 0.9 {
                    out.push(MergeCandidate {
                        a: a.id,
                        b: b.id,
                        predicted_delta_r_gain: sim,
                        cosine_similarity: sim,
                    });
                }
            }
        }
        out.sort_by(|x, y| {
            y.predicted_delta_r_gain
                .partial_cmp(&x.predicted_delta_r_gain)
                .unwrap()
        });
        out
    }
}

/// A ranked merge candidate (§11.6).
#[derive(Clone, Debug)]
pub struct MergeCandidate {
    /// First memory.
    pub a: MemoryId,
    /// Second memory.
    pub b: MemoryId,
    /// Predicted ΔR gain from merging.
    pub predicted_delta_r_gain: f32,
    /// Cosine similarity of the pair.
    pub cosine_similarity: f32,
}

/// Cosine similarity; 0.0 for zero vectors.
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

// ---- §11.6.1 graph sparsity ----

/// Cluster identity for density reporting.
pub type ClusterId = u64;

/// The sparsity diagnostic (§11.6.1).
#[derive(Clone, Debug, Default)]
pub struct GraphSparsity {
    /// Average out-degree over asserted+derived edges, excluding SimilarTo.
    pub avg_out_degree: f32,
    /// Per-memory-type median out-degree.
    pub median_out_degree_by_type: HashMap<u8, f32>,
    /// Fraction of memories above the hairball threshold (default 32).
    pub hairball_fraction: f32,
    /// Confidence-weighted density per cluster.
    pub weighted_density_by_cluster: HashMap<ClusterId, f32>,
    /// Memory count.
    pub n_memories: usize,
    /// Edge count.
    pub n_edges: usize,
    /// When computed.
    pub computed_at: DateTime<Utc>,
}

/// Compute sparsity from an edge list: (from, to, kind, similarity-class).
/// `similar_kind` edges are excluded from out-degrees (§11.6.1: the
/// similarity bucket must not inflate hairball detection).
pub fn compute_sparsity(
    nodes: &[(MemoryId, u8)],
    edges: &[(MemoryId, MemoryId, u32, ClusterId, f32)],
    hairball_threshold: u32,
    similar_kind: Option<exocortex_kernel::RelKindId>,
) -> GraphSparsity {
    let mut out_deg: HashMap<MemoryId, u32> = HashMap::new();
    let mut densities: HashMap<ClusterId, (f32, usize)> = HashMap::new();
    let n_counted = 0usize;
    for (from, _to, kind, cluster, confidence) in edges {
        // Similarity edges ride the graph but never count toward
        // out-degrees/hairballs (§11.6.1).
        if similar_kind.is_some_and(|sk| sk.0 == *kind) {
            continue;
        }
        *out_deg.entry(*from).or_default() += 1;
        let e = densities.entry(*cluster).or_default();
        e.0 += *confidence;
        e.1 += 1;
    }
    let n = nodes.len().max(1) as f32;
    let avg = out_deg.values().sum::<u32>() as f32 / n;
    let hairball = out_deg
        .values()
        .filter(|d| **d > hairball_threshold)
        .count() as f32
        / n;

    let mut by_type: HashMap<u8, Vec<u32>> = HashMap::new();
    for (id, mt) in nodes {
        by_type
            .entry(*mt)
            .or_default()
            .push(out_deg.get(id).copied().unwrap_or(0));
    }
    let median_out_degree_by_type = by_type
        .into_iter()
        .map(|(mt, mut ds)| {
            ds.sort();
            let mid = ds.len() / 2;
            let med = if ds.len() % 2 == 1 {
                ds[mid] as f32
            } else {
                (ds[mid - 1] as f32 + ds[mid] as f32) / 2.0
            };
            (mt, med)
        })
        .collect();

    let mut by_node_count: HashMap<ClusterId, usize> = HashMap::new();
    for (_id, _) in nodes {
        *by_node_count.entry(0).or_default() += 1;
    }
    let weighted_density_by_cluster = densities
        .into_iter()
        .map(|(cluster, (conf_sum, edge_n))| {
            let nodes_in = by_node_count
                .get(&cluster)
                .copied()
                .unwrap_or(edge_n)
                .max(1);
            let denom = (nodes_in * (nodes_in - 1)).max(1) as f32;
            (cluster, conf_sum / denom)
        })
        .collect();

    GraphSparsity {
        avg_out_degree: avg,
        median_out_degree_by_type,
        hairball_fraction: hairball,
        weighted_density_by_cluster,
        n_memories: nodes.len(),
        n_edges: edges.len().max(n_counted),
        computed_at: Utc::now(),
    }
}

/// §14.3 effective relationship strength: evidence boost (capped 0.20),
/// success scaling, and age decay (floored 0.5), clamped to [0,1]. This is
/// the single scoring copy — Dreams' strengthen action applies it; there is
/// no second implementation elsewhere.
pub fn effective_strength(base: f32, evidence_count: u32, success_rate: f32, age_days: f32) -> f32 {
    let boost = (0.05 * ((evidence_count as f32 - 1.0).max(0.0)).sqrt()).min(0.20);
    let success = 0.5 + 0.5 * success_rate;
    let decay = (1.0 - 0.01 * age_days).max(0.5);
    ((base + boost) * success * decay).clamp(0.0, 1.0)
}
