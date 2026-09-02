//! D17 (master plan; palantir-expansion PRD §5; core PRD §24): the
//! PROBABILISTIC half of entity resolution — the half D13's
//! deterministic external-key join deliberately excludes.
//!
//! D13 answers "is this row the same row" with a digest; it cannot
//! answer "is *Acme Financial Corp* the same organization as
//! *acme financial corporation*" — two sources that will never agree
//! on an external key. This crate answers that, with the discipline
//! the repo's invariants demand:
//!
//! - **Deterministic statistics, no learning system.** The model is
//!   classical Fellegi-Sunter: comparison features (hand-written
//!   string metrics — no dependency), per-feature m/u probabilities
//!   ESTIMATED from an operator-labelled pair set with Laplace
//!   smoothing, summed log likelihood ratios. Nothing trains, nothing
//!   guesses; every score is reproducible arithmetic over the
//!   labelled set.
//! - **No LLM, no ML crate, no model file** (hard rule 1; the crate
//!   carries no dependency beyond serde/anyhow).
//! - **Matches are proposals, never writes.** Fuzzy ER emits scored
//!   candidate pairs for an operator (or a future Dreams proposal
//!   surface) to accept; it never merges, never mints edges, never
//!   touches a graph. The deterministic join (D13) remains the only
//!   automatic identity.
//! - **The evaluation harness is the deliverable** — precision /
//!   recall / F1 over the labelled set with a confusion table, so
//!   threshold choices are measured, not vibes.
//!
//! Corpus shape: JSONL records `{id, name, attributes: {...}}` (the
//! `--export-corpus` lineage output is a direct source). Labelled
//! set: JSONL `{a, b, label: "match" | "non_match"}`.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

// ---------------------------------------------------------------------------
// String comparators (hand-written; the crate carries no dependency)
// ---------------------------------------------------------------------------

/// Jaro similarity between two strings (case-folded), 0..=1.
pub fn jaro(a: &str, b: &str) -> f64 {
    let a: Vec<char> = a.chars().flat_map(char::to_lowercase).collect();
    let b: Vec<char> = b.chars().flat_map(char::to_lowercase).collect();
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let window = (a.len().max(b.len()) / 2).saturating_sub(1);
    let mut a_matched = vec![false; a.len()];
    let mut b_matched = vec![false; b.len()];
    let mut matches = 0usize;
    for (i, ch) in a.iter().enumerate() {
        let start = i.saturating_sub(window);
        let end = (i + window + 1).min(b.len());
        for j in start..end {
            if !b_matched[j] && *ch == b[j] {
                a_matched[i] = true;
                b_matched[j] = true;
                matches += 1;
                break;
            }
        }
    }
    if matches == 0 {
        return 0.0;
    }
    let mut transpositions = 0usize;
    let mut k = 0usize;
    for i in 0..a.len() {
        if a_matched[i] {
            while !b_matched[k] {
                k += 1;
            }
            if a[i] != b[k] {
                transpositions += 1;
            }
            k += 1;
        }
    }
    let m = matches as f64;
    (m / a.len() as f64 + m / b.len() as f64 + (m - transpositions as f64 / 2.0) / m) / 3.0
}

/// Jaro-Winkler similarity (prefix-weighted), 0..=1.
pub fn jaro_winkler(a: &str, b: &str) -> f64 {
    let base = jaro(a, b);
    if base <= 0.7 {
        return base;
    }
    let a_lower = a.to_lowercase();
    let b_lower = b.to_lowercase();
    let prefix = a_lower
        .chars()
        .zip(b_lower.chars())
        .take(4)
        .take_while(|(x, y)| x == y)
        .count();
    base + 0.1 * prefix as f64 * (1.0 - base)
}

/// Token-set similarity: Jaccard over the whitespace-split,
/// case-folded token sets, with duplicate tokens collapsed. Order
/// differences ("corp financial acme" vs "acme financial corp")
/// should not tank a name comparison.
pub fn token_set(a: &str, b: &str) -> f64 {
    let tokens = |s: &str| -> std::collections::BTreeSet<String> {
        s.split_whitespace()
            .map(|t| {
                t.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_lowercase()
            })
            .filter(|t| !t.is_empty())
            .collect()
    };
    let a = tokens(a);
    let b = tokens(b);
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let intersection = a.intersection(&b).count() as f64;
    let union = a.union(&b).count() as f64;
    intersection / union
}

/// Strip legal-entity suffix noise before comparing ("inc", "llc",
/// "ltd", "corp", "corporation", "co", "company", "gmbh", "s.a.",
/// "plc" and their punctuation forms).
pub fn strip_legal_suffixes(name: &str) -> String {
    const SUFFIXES: &[&str] = &[
        "inc",
        "incorporated",
        "llc",
        "llp",
        "ltd",
        "limited",
        "corp",
        "corporation",
        "co",
        "company",
        "gmbh",
        "ag",
        "sa",
        "s.a.",
        "plc",
        "pty",
        "pvt",
    ];
    name.split_whitespace()
        .map(|token| token.trim_matches('.').to_lowercase())
        .filter(|token| !SUFFIXES.contains(&token.as_str()))
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------
// Corpus and labelled set
// ---------------------------------------------------------------------------

/// One entity record from the corpus.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EntityRecord {
    /// Stable corpus id (a memory id, an external coordinate, or any
    /// operator-chosen key).
    pub id: String,
    /// The entity's display name — the primary comparison surface.
    pub name: String,
    /// Optional attribute strings compared by key (address, city,
    /// domain...). Attributes absent from one side compare as
    /// `missing`, which is NOT evidence either way.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

/// A labelled pair from the operator.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LabelledPair {
    pub a: String,
    pub b: String,
    /// "match" or "non_match".
    pub label: String,
}

/// Load JSONL entity records.
pub fn load_corpus(path: &std::path::Path) -> Result<Vec<EntityRecord>> {
    load_jsonl(path, "corpus")
}

/// Load JSONL labelled pairs, validating labels and pair presence.
pub fn load_labelled(path: &std::path::Path, corpus: &[EntityRecord]) -> Result<Vec<LabelledPair>> {
    let pairs: Vec<LabelledPair> = load_jsonl(path, "labelled set")?;
    let ids: std::collections::BTreeSet<&str> = corpus.iter().map(|r| r.id.as_str()).collect();
    for pair in &pairs {
        if pair.label != "match" && pair.label != "non_match" {
            bail!(
                "labelled pair {} <-> {} carries label `{}` — expected `match` or `non_match`",
                pair.a,
                pair.b,
                pair.label
            );
        }
        for id in [&pair.a, &pair.b] {
            if !ids.contains(id.as_str()) {
                bail!("labelled pair names corpus id {id} that does not exist");
            }
        }
    }
    Ok(pairs)
}

fn load_jsonl<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
    what: &str,
) -> Result<Vec<T>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {what} {}", path.display()))?;
    let mut out = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: T = serde_json::from_str(line).with_context(|| {
            format!("parsing {what} {} line {}", path.display(), line_index + 1)
        })?;
        out.push(value);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Blocking (candidate generation)
// ---------------------------------------------------------------------------

/// Deterministic blocking: two records are candidates when they share
/// a blocking key. Keys: (1) the first three chars of the
/// suffix-stripped name, (2) every significant token of the stripped
/// name, (3) every attribute value token. Candidate generation must
/// never depend on iteration order.
pub fn blocking_keys(record: &EntityRecord) -> Vec<String> {
    let mut keys = Vec::new();
    let stripped = strip_legal_suffixes(&record.name);
    let prefix = stripped.chars().take(3).collect::<String>();
    if !prefix.trim().is_empty() {
        keys.push(format!("p:{prefix}"));
    }
    for token in stripped.split_whitespace() {
        keys.push(format!("t:{token}"));
    }
    for (attribute, value) in &record.attributes {
        for token in value.split_whitespace() {
            keys.push(format!("a:{attribute}:{token}"));
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Generate candidate pairs (i < j by corpus order, deduped) that
/// share at least one blocking key.
pub fn candidates(corpus: &[EntityRecord]) -> Vec<(usize, usize)> {
    let mut by_key: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, record) in corpus.iter().enumerate() {
        for key in blocking_keys(record) {
            by_key.entry(key).or_default().push(index);
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut pairs = Vec::new();
    for members in by_key.values() {
        for i in 0..members.len() {
            for j in i + 1..members.len() {
                if seen.insert((members[i], members[j])) {
                    pairs.push((members[i], members[j]));
                }
            }
        }
    }
    pairs.sort();
    pairs
}

// ---------------------------------------------------------------------------
// Comparison vector and the Fellegi-Sunter model
// ---------------------------------------------------------------------------

/// One comparison feature's outcome between two records. `Missing`
/// is the honest third state: an attribute present on only one side
/// is not evidence in either direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// The values agree exactly (after normalization).
    Exact,
    /// Similar above the feature's similarity band.
    Similar,
    /// Compared and different.
    Different,
    /// The attribute is absent on one or both sides.
    Missing,
}

/// The comparison vector: name similarity (suffix-stripped
/// Jaro-Winkler and token-set bands) plus per-attribute exact/similar
/// bands over the union of both sides' attribute keys.
#[derive(Clone, Debug, PartialEq)]
pub struct ComparisonVector {
    pub name_jw: Comparison,
    pub name_tokens: Comparison,
    pub attributes: BTreeMap<String, Comparison>,
}

/// The band thresholds (similarity >= similar means Similar; exact
/// match of the normalized value means Exact).
pub const NAME_SIMILAR_BAND: f64 = 0.85;
pub const ATTRIBUTE_SIMILAR_BAND: f64 = 0.85;

fn band(a: &str, b: &str, similar_band: f64) -> Comparison {
    let normalize = |s: &str| s.trim().to_lowercase();
    let (a, b) = (normalize(a), normalize(b));
    if a == b {
        return Comparison::Exact;
    }
    if a.is_empty() || b.is_empty() {
        return Comparison::Missing;
    }
    if jaro_winkler(&a, &b) >= similar_band {
        Comparison::Similar
    } else {
        Comparison::Different
    }
}

/// Compare two records into their comparison vector.
pub fn compare(a: &EntityRecord, b: &EntityRecord) -> ComparisonVector {
    let name_a = strip_legal_suffixes(&a.name);
    let name_b = strip_legal_suffixes(&b.name);
    let name_jw = band(&name_a, &name_b, NAME_SIMILAR_BAND);
    let token_outcome = if strip_legal_suffixes(&a.name) == strip_legal_suffixes(&b.name) {
        Comparison::Exact
    } else {
        let similarity = token_set(&a.name, &b.name);
        if similarity >= NAME_SIMILAR_BAND {
            Comparison::Similar
        } else {
            Comparison::Different
        }
    };
    let mut attributes = BTreeMap::new();
    let mut keys: std::collections::BTreeSet<&String> = a.attributes.keys().collect();
    keys.extend(b.attributes.keys());
    for key in keys {
        let outcome = match (a.attributes.get(key), b.attributes.get(key)) {
            (Some(va), Some(vb)) => band(va, vb, ATTRIBUTE_SIMILAR_BAND),
            _ => Comparison::Missing,
        };
        attributes.insert(key.clone(), outcome);
    }
    ComparisonVector {
        name_jw,
        name_tokens: token_outcome,
        attributes,
    }
}

/// The Fellegi-Sunter model: per-feature m (agreement given match)
/// and u (agreement given non-match) probabilities estimated from the
/// labelled set with Laplace smoothing, and the accept/review
/// thresholds over the summed log likelihood ratio.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FsModel {
    /// m probabilities per feature slot: (name_jw, name_tokens) then
    /// attributes in key order. Each entry: [Exact, Similar,
    /// Different, Missing].
    #[serde(rename = "m")]
    pub m: BTreeMap<String, [f64; 4]>,
    /// u probabilities, same shape.
    #[serde(rename = "u")]
    pub u: BTreeMap<String, [f64; 4]>,
    /// Accept threshold on the summed log-likelihood ratio.
    pub accept_threshold: f64,
    /// Review threshold (pairs above this but below accept are
    /// review candidates).
    pub review_threshold: f64,
}

fn slot_index(comparison: Comparison) -> usize {
    match comparison {
        Comparison::Exact => 0,
        Comparison::Similar => 1,
        Comparison::Different => 2,
        Comparison::Missing => 3,
    }
}

/// Estimate the model from the labelled set: for each feature slot,
/// m = P(outcome | match) and u = P(outcome | non_match), each a
/// Laplace-smoothed (add-1 over 4 outcomes) frequency, and the
/// decision thresholds CALIBRATED from the labelled scores — the
/// boundary is the midpoint between the match and non-match mean
/// scores, accept sits 3/4 of the way toward the match mean, review
/// 3/4 of the way toward the non-match mean. Fixed magic thresholds
/// cannot mean the same thing across feature counts and evidence
/// strengths; the labelled set's own separation can.
pub fn estimate(corpus: &[EntityRecord], labelled: &[LabelledPair]) -> Result<FsModel> {
    if labelled.is_empty() {
        bail!("the labelled set is empty — m/u estimation needs labelled pairs");
    }
    let by_id: BTreeMap<&str, &EntityRecord> = corpus
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect();
    let mut m_counts: BTreeMap<String, [u32; 4]> = BTreeMap::new();
    let mut u_counts: BTreeMap<String, [u32; 4]> = BTreeMap::new();
    for pair in labelled {
        let (Some(a), Some(b)) = (by_id.get(pair.a.as_str()), by_id.get(pair.b.as_str())) else {
            bail!("labelled pair {} <-> {} names a missing corpus id (validated earlier — internal error)", pair.a, pair.b);
        };
        let vector = compare(a, b);
        let target = if pair.label == "match" {
            &mut m_counts
        } else {
            &mut u_counts
        };
        let mut record = |slot: &str, outcome: Comparison| {
            let counts = target.entry(slot.to_string()).or_insert([0, 0, 0, 0]);
            counts[slot_index(outcome)] += 1;
        };
        record("name", vector.name_jw);
        record("name_tokens", vector.name_tokens);
        for (attribute, outcome) in &vector.attributes {
            record(&format!("attr:{attribute}"), *outcome);
        }
    }
    let estimate_one = |counts: &BTreeMap<String, [u32; 4]>| -> BTreeMap<String, [f64; 4]> {
        counts
            .iter()
            .map(|(slot, counts)| {
                let total: u32 = counts.iter().sum();
                // Laplace smoothing over the four outcomes.
                let denominator = total + 4;
                let probabilities = counts
                    .iter()
                    .map(|c| (c + 1) as f64 / denominator as f64)
                    .collect::<Vec<_>>()
                    .try_into()
                    .expect("four outcomes");
                (slot.clone(), probabilities)
            })
            .collect()
    };
    let m = estimate_one(&m_counts);
    let u = estimate_one(&u_counts);
    // The u side must see every slot the m side does (a feature that
    // only ever occurs in matches has no non-match evidence and would
    // divide by an unsmoothed zero of the wrong kind); smoothing keeps
    // arithmetic finite, but a slot ABSENT from u entirely is evidence
    // the labelled set is too thin — say so.
    for slot in m.keys() {
        if !u.contains_key(slot) {
            bail!(
                "feature `{slot}` appears only in match pairs — the labelled set has no \
                 non-match evidence for it; label pairs that exercise it or drop the attribute"
            );
        }
    }
    // Calibrate the thresholds from the labelled scores themselves.
    let by_id: BTreeMap<&str, &EntityRecord> = corpus
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect();
    let provisional = FsModel {
        m: m.clone(),
        u: u.clone(),
        accept_threshold: 0.0,
        review_threshold: 0.0,
    };
    let mut match_scores = Vec::new();
    let mut non_match_scores = Vec::new();
    for pair in labelled {
        let (Some(a), Some(b)) = (by_id.get(pair.a.as_str()), by_id.get(pair.b.as_str())) else {
            bail!("internal: labelled pair names a missing corpus id after validation");
        };
        let score = provisional.score(&compare(a, b));
        if pair.label == "match" {
            match_scores.push(score);
        } else {
            non_match_scores.push(score);
        }
    }
    if match_scores.is_empty() || non_match_scores.is_empty() {
        bail!(
            "the labelled set needs BOTH match and non_match pairs to calibrate thresholds \
             ({} match, {} non_match)",
            match_scores.len(),
            non_match_scores.len()
        );
    }
    let mean = |scores: &[f64]| scores.iter().sum::<f64>() / scores.len() as f64;
    let (mu_m, mu_u) = (mean(&match_scores), mean(&non_match_scores));
    let boundary = (mu_m + mu_u) / 2.0;
    let accept_threshold = boundary + 0.75 * (mu_m - boundary);
    let review_threshold = boundary - 0.75 * (boundary - mu_u);
    Ok(FsModel {
        m,
        u,
        accept_threshold,
        review_threshold,
    })
}

impl FsModel {
    /// The summed log-likelihood-ratio score of one comparison
    /// vector: sum over features of ln(m[outcome] / u[outcome]).
    pub fn score(&self, vector: &ComparisonVector) -> f64 {
        let mut score = 0.0;
        let mut add = |slot: &str, outcome: Comparison| {
            if let (Some(m), Some(u)) = (self.m.get(slot), self.u.get(slot)) {
                let index = slot_index(outcome);
                score += (m[index] / u[index]).ln();
            }
        };
        add("name", vector.name_jw);
        add("name_tokens", vector.name_tokens);
        for (attribute, outcome) in &vector.attributes {
            add(&format!("attr:{attribute}"), *outcome);
        }
        score
    }

    /// The decision for a score.
    pub fn decide(&self, score: f64) -> Decision {
        if score >= self.accept_threshold {
            Decision::Match
        } else if score >= self.review_threshold {
            Decision::Review
        } else {
            Decision::NonMatch
        }
    }
}

/// The decision for one pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Match,
    Review,
    NonMatch,
}

/// One scored candidate pair.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ScoredPair {
    pub a: String,
    pub b: String,
    pub score: f64,
    pub decision: String,
}

/// Score every blocked candidate pair under a model.
pub fn score_candidates(corpus: &[EntityRecord], model: &FsModel) -> Vec<ScoredPair> {
    let mut scored = Vec::new();
    for (i, j) in candidates(corpus) {
        let vector = compare(&corpus[i], &corpus[j]);
        let score = model.score(&vector);
        scored.push(ScoredPair {
            a: corpus[i].id.clone(),
            b: corpus[j].id.clone(),
            score,
            decision: match model.decide(score) {
                Decision::Match => "match",
                Decision::Review => "review",
                Decision::NonMatch => "non_match",
            }
            .to_string(),
        });
    }
    scored
}

// ---------------------------------------------------------------------------
// The evaluation harness
// ---------------------------------------------------------------------------

/// Evaluation over the labelled set: every labelled pair scored and
/// decided; precision/recall/F1 for the `Match` decision against the
/// `match` labels, plus the confusion table and the pairs the harness
/// disagreed with.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Evaluation {
    pub labelled_pairs: usize,
    pub true_positives: usize,
    pub false_positives: usize,
    pub false_negatives: usize,
    pub true_negatives: usize,
    pub review: usize,
    pub precision: f64,
    pub recall: f64,
    pub f1: f64,
}

/// Evaluate a model against its labelled set.
pub fn evaluate(
    corpus: &[EntityRecord],
    labelled: &[LabelledPair],
    model: &FsModel,
) -> Result<Evaluation> {
    let by_id: BTreeMap<&str, &EntityRecord> = corpus
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect();
    let mut evaluation = Evaluation {
        labelled_pairs: labelled.len(),
        true_positives: 0,
        false_positives: 0,
        false_negatives: 0,
        true_negatives: 0,
        review: 0,
        precision: 0.0,
        recall: 0.0,
        f1: 0.0,
    };
    for pair in labelled {
        let (Some(a), Some(b)) = (by_id.get(pair.a.as_str()), by_id.get(pair.b.as_str())) else {
            bail!("labelled pair names a missing corpus id");
        };
        let score = model.score(&compare(a, b));
        match (model.decide(score), pair.label.as_str()) {
            (Decision::Match, "match") => evaluation.true_positives += 1,
            (Decision::Match, _) => evaluation.false_positives += 1,
            (Decision::Review, _) => evaluation.review += 1,
            (Decision::NonMatch, "match") => evaluation.false_negatives += 1,
            (Decision::NonMatch, _) => evaluation.true_negatives += 1,
        }
    }
    let tp = evaluation.true_positives as f64;
    let fp = evaluation.false_positives as f64;
    let fn_ = evaluation.false_negatives as f64;
    evaluation.precision = if tp + fp > 0.0 { tp / (tp + fp) } else { 0.0 };
    evaluation.recall = if tp + fn_ > 0.0 { tp / (tp + fn_) } else { 0.0 };
    evaluation.f1 = if evaluation.precision + evaluation.recall > 0.0 {
        2.0 * (evaluation.precision * evaluation.recall)
            / (evaluation.precision + evaluation.recall)
    } else {
        0.0
    };
    Ok(evaluation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, name: &str, attributes: &[(&str, &str)]) -> EntityRecord {
        EntityRecord {
            id: id.into(),
            name: name.into(),
            attributes: attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn jaro_winkler_ranks_typographic_variants_above_unrelated() {
        let similar = jaro_winkler("Acme Financial Corp", "acme financial corporation");
        let unrelated = jaro_winkler("Acme Financial Corp", "Zenith Data Systems");
        assert!(similar > 0.9, "suffix noise should not tank: {similar}");
        assert!(unrelated < 0.6, "{unrelated}");
        assert!(similar > unrelated);
    }

    #[test]
    fn token_set_ignores_word_order() {
        assert!(token_set("corp financial acme", "acme financial corp") > 0.9);
    }

    #[test]
    fn legal_suffixes_strip_deterministically() {
        assert_eq!(
            strip_legal_suffixes("Acme Financial Corp."),
            "acme financial"
        );
        assert_eq!(strip_legal_suffixes("Acme Financial"), "acme financial");
    }

    #[test]
    fn missing_attributes_are_not_evidence() {
        let a = record("a", "Acme", &[("city", "Austin")]);
        let b = record("b", "Acme", &[]);
        let vector = compare(&a, &b);
        assert_eq!(vector.attributes.get("city"), Some(&Comparison::Missing));
        let model = FsModel {
            m: BTreeMap::from([("attr:city".into(), [0.6, 0.2, 0.1, 0.1])]),
            u: BTreeMap::from([("attr:city".into(), [0.05, 0.05, 0.4, 0.5])]),
            accept_threshold: 6.0,
            review_threshold: 2.0,
        };
        let score = model.score(&vector);
        // A missing attribute contributes ln(0.1/0.5) — mild, not the
        // exact-agreement evidence ln(0.6/0.05).
        let exact = model.score(&compare(
            &record("a", "Acme", &[("city", "Austin")]),
            &record("c", "Acme", &[("city", "Austin")]),
        ));
        assert!(
            exact > score,
            "exact agreement scores higher: {exact} vs {score}"
        );
    }

    #[test]
    fn blocking_never_pairs_records_without_shared_keys() {
        let corpus = vec![
            record("1", "Acme Financial Corp", &[]),
            record("2", "acme financial corporation", &[]),
            record("3", "Zenith Data Systems", &[]),
        ];
        let pairs = candidates(&corpus);
        assert!(
            pairs.contains(&(0, 1)),
            "shared prefix/token blocks pair them"
        );
        assert!(
            !pairs.contains(&(0, 2)) && !pairs.contains(&(1, 2)),
            "no shared key, no candidate: {pairs:?}"
        );
    }

    #[test]
    fn estimation_and_evaluation_round_trip_a_clean_labelled_set() {
        let corpus = vec![
            record("1", "Acme Financial Corp", &[("city", "Austin")]),
            record("2", "acme financial corporation", &[("city", "austin")]),
            record("3", "Zenith Data Systems", &[("city", "Reno")]),
            record("4", "zenith data systems", &[("city", "Reno")]),
            record("5", "Harborview Logistics LLC", &[("city", "Miami")]),
        ];
        let labelled = vec![
            LabelledPair {
                a: "1".into(),
                b: "2".into(),
                label: "match".into(),
            },
            LabelledPair {
                a: "3".into(),
                b: "4".into(),
                label: "match".into(),
            },
            LabelledPair {
                a: "1".into(),
                b: "3".into(),
                label: "non_match".into(),
            },
            LabelledPair {
                a: "2".into(),
                b: "4".into(),
                label: "non_match".into(),
            },
            LabelledPair {
                a: "3".into(),
                b: "5".into(),
                label: "non_match".into(),
            },
        ];
        let model = estimate(&corpus, &labelled).unwrap();
        let evaluation = evaluate(&corpus, &labelled, &model).unwrap();
        assert_eq!(evaluation.true_positives, 2, "both matches decided Match");
        assert_eq!(evaluation.false_negatives, 0);
        assert_eq!(evaluation.false_positives, 0);
        assert!((evaluation.f1 - 1.0).abs() < 1e-9, "{}", evaluation.f1);
        // Scored candidates surface the matches above the rest.
        let scored = score_candidates(&corpus, &model);
        let match_scores: Vec<f64> = scored
            .iter()
            .filter(|p| p.decision == "match")
            .map(|p| p.score)
            .collect();
        assert_eq!(match_scores.len(), 2);
    }

    #[test]
    fn a_slot_without_non_match_evidence_is_refused() {
        let corpus = vec![
            record("1", "Acme", &[("domain", "acme.io")]),
            record("2", "Acme", &[("domain", "acme.io")]),
            record("3", "Zenith", &[("city", "Reno")]),
            record("4", "Harborview", &[("city", "Miami")]),
        ];
        let labelled = vec![
            LabelledPair {
                a: "1".into(),
                b: "2".into(),
                label: "match".into(),
            },
            LabelledPair {
                a: "3".into(),
                b: "4".into(),
                label: "non_match".into(),
            },
        ];
        // Neither record of the non-match pair carries `domain`, so
        // attr:domain has match-only evidence — estimation must refuse
        // rather than smooth silently.
        let err = estimate(&corpus, &labelled).unwrap_err().to_string();
        assert!(err.contains("attr:domain"), "{err}");
    }

    #[test]
    fn a_singular_plural_variant_lands_in_review_not_match() {
        // The honest boundary: a near-variant name whose token sets
        // differ (systems vs system) is NOT strong-enough evidence to
        // auto-match — it must land in the review band, not Match,
        // because fuzzy ER proposes rather than merges.
        let corpus = vec![
            record("1", "Acme Financial Corp", &[("city", "Austin")]),
            record("2", "acme financial corporation", &[("city", "austin")]),
            record("3", "Zenith Data Systems", &[("city", "Reno")]),
            record("4", "Zenith Data System", &[("city", "Reno")]),
            record("5", "Harborview Logistics LLC", &[("city", "Miami")]),
        ];
        let labelled = vec![
            LabelledPair {
                a: "1".into(),
                b: "2".into(),
                label: "match".into(),
            },
            LabelledPair {
                a: "3".into(),
                b: "4".into(),
                label: "match".into(),
            },
            LabelledPair {
                a: "1".into(),
                b: "3".into(),
                label: "non_match".into(),
            },
            LabelledPair {
                a: "2".into(),
                b: "4".into(),
                label: "non_match".into(),
            },
            LabelledPair {
                a: "3".into(),
                b: "5".into(),
                label: "non_match".into(),
            },
        ];
        let model = estimate(&corpus, &labelled).unwrap();
        let score = model.score(&compare(&corpus[2], &corpus[3]));
        assert_eq!(
            model.decide(score),
            Decision::Review,
            "score {score}: near-variants are proposals, not automatic matches"
        );
    }

    #[test]
    fn calibration_requires_both_classes() {
        let corpus = vec![record("1", "Acme", &[]), record("2", "Zenith", &[])];
        let labelled = vec![LabelledPair {
            a: "1".into(),
            b: "2".into(),
            label: "non_match".into(),
        }];
        let err = estimate(&corpus, &labelled).unwrap_err().to_string();
        assert!(err.contains("BOTH"), "{err}");
    }

    #[test]
    fn labels_and_ids_are_validated_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let corpus_path = dir.path().join("c.jsonl");
        std::fs::write(
            &corpus_path,
            "{\"id\":\"1\",\"name\":\"Acme\"}\n{\"id\":\"2\",\"name\":\"Zenith\"}\n",
        )
        .unwrap();
        let labelled_path = dir.path().join("l.jsonl");
        std::fs::write(
            &labelled_path,
            "{\"a\":\"1\",\"b\":\"9\",\"label\":\"match\"}\n",
        )
        .unwrap();
        let corpus = load_corpus(&corpus_path).unwrap();
        assert_eq!(corpus.len(), 2);
        let err = load_labelled(&labelled_path, &corpus)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"), "{err}");
        std::fs::write(
            &labelled_path,
            "{\"a\":\"1\",\"b\":\"2\",\"label\":\"maybe\"}\n",
        )
        .unwrap();
        let err = load_labelled(&labelled_path, &corpus)
            .unwrap_err()
            .to_string();
        assert!(err.contains("maybe"), "{err}");
    }
}
