// crates/exocortex-ingest/src/entities.rs
//! The entity-extraction table (§7.2 R-T18): one compiled regex set per
//! EntityType, deterministic, versioned with the pack. Entities are
//! extracted server-side; the harness never supplies them.

use exocortex_kernel::{EntityId, Memory};
use lasso::{Rodeo, Spur};
use once_cell::sync::Lazy;
use regex::RegexSet;
use smol_str::SmolStr;

/// The 12 dev-v1 entity types in declaration order (ids match the
/// effective ontology's assignment).
pub const ENTITY_TYPE_COUNT: usize = 12;

/// Which entity type a pattern belongs to (index == ontology u8 id).
fn pattern_table() -> Vec<(&'static str, Vec<&'static str>)> {
    // Ordered per the dev-v1 pack declaration:
    // File, Function, Class, Error, Technology, Concept,
    // Person, Project, Command, Package, Url, Variable.
    vec![
        (
            "File",
            vec![r"\b[\w\-/\.]+\.(rs|toml|py|ts|js|go|java|c|cpp|h|md)\b"],
        ),
        (
            "Function",
            vec![
                r"\b[a-z_][a-z0-9_]*\(\s*\)", // snake_case call
                r"\bfn\s+([a-z_][a-z0-9_]*)",
            ],
        ),
        (
            "Class",
            vec![r"\b(?:struct|enum|trait|class)\s+([A-Z][A-Za-z0-9_]*)"],
        ),
        (
            "Error",
            vec![r"\b[A-Z][A-Za-z0-9]*Error\b", r"\bpanic![^\]]*\]"],
        ),
        (
            "Technology",
            vec![
                r"\b(rust|tokio|axum|serde|falkordb|redis|postgres|sqlite|python|typescript|kubernetes|docker)\b",
            ],
        ),
        (
            "Concept",
            vec![
                r"\b[A-Z][a-z]+(?:[ -][A-Z][a-z]+)+\b", // Title Case Phrase
            ],
        ),
        (
            "Person",
            vec![
                r"@[a-z][a-z0-9_]{2,}", // @handle
            ],
        ),
        (
            "Project",
            vec![r"\b[a-z0-9]+-[a-z0-9]+(?:-[a-z0-9]+)*-service\b"],
        ),
        (
            "Command",
            vec![r"\b(?:cargo|npm|pnpm|git|docker|kubectl|make|brew)\s+[a-z\-]+"],
        ),
        (
            "Package",
            vec![
                r"\b[a-z0-9_\-]+@[0-9]+\.[0-9]+(?:\.[0-9]+)?\b", // package@version
            ],
        ),
        ("Url", vec![r"https?://[^\s]+"]),
        (
            "Variable",
            vec![
                r"\b[A-Z_][A-Z0-9_]{3,}\b", // SCREAMING_CASE
            ],
        ),
    ]
}

static TABLE: Lazy<Vec<(&'static str, RegexSet)>> = Lazy::new(|| {
    pattern_table()
        .into_iter()
        .map(|(name, pats)| {
            (
                name,
                RegexSet::new(pats).expect("extraction patterns compile"),
            )
        })
        .collect()
});

/// The deterministic extractor. Confidence: ambiguous/overlapping matches
/// carry lower `extraction_confidence` (R-T18); exact matches 0.95,
/// ambiguous 0.6.
#[derive(Clone)]
pub struct EntityExtractor {
    org_id: String,
}

impl EntityExtractor {
    /// Build an extractor scoped to one org (entity ids are org-scoped).
    pub fn new(org_id: &str) -> Self {
        Self {
            org_id: org_id.to_string(),
        }
    }

    /// Extract entities from `content` (+ tags as Concept candidates),
    /// returning `(entity_type, canonical_name, confidence)`. The same
    /// input always yields the same set, in the same order.
    pub fn extract(&self, content: &str, tags: &[SmolStr]) -> Vec<(u8, String, f32)> {
        let mut out: Vec<(u8, String, f32)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let text = format!(
            "{content} {}",
            tags.iter()
                .map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        );
        for (type_idx, (type_name, set)) in TABLE.iter().enumerate() {
            let matches = set.matches(&text);
            let mut hits: Vec<String> = Vec::new();
            for mi in matches.iter() {
                // RegexSet gives set-level indices; recover the concrete
                // match text with the per-pattern regex over the same input
                // (deterministic because pattern order is fixed).
                // IN9 (audit): find_iter collects EVERY occurrence of the
                // pattern — `find` kept only the leftmost match, so a
                // memory mentioning two files yielded one entity and
                // find_by_entity silently missed the memory for the other.
                let pat = pattern_of(type_idx, mi);
                for m in pat.find_iter(&text) {
                    let raw = m.as_str().trim();
                    hits.push(raw.to_string());
                }
            }
            let ambiguous = hits.len() > 1;
            for h in hits {
                let canonical = canonicalize(type_name, &h);
                if seen.insert((type_idx as u8, canonical.clone())) {
                    out.push((
                        type_idx as u8,
                        canonical,
                        if ambiguous { 0.6 } else { 0.95 },
                    ));
                }
            }
        }
        out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        out.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        out
    }

    /// Entity ids for a memory (populates `Memory.context.entities`).
    pub fn entity_ids(&self, content: &str, tags: &[SmolStr]) -> Vec<EntityId> {
        let mut ids: Vec<EntityId> = self
            .extract(content, tags)
            .into_iter()
            .map(|(t, name, _)| EntityId::from_parts(&self.org_id, t, &name))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

fn pattern_of(type_idx: usize, pattern_idx: usize) -> &'static regex::Regex {
    static PER_PATTERN: Lazy<Vec<Vec<regex::Regex>>> = Lazy::new(|| {
        pattern_table()
            .into_iter()
            .map(|(_, pats)| {
                pats.into_iter()
                    .map(|p| regex::Regex::new(p).expect("pattern compiles"))
                    .collect()
            })
            .collect()
    });
    &PER_PATTERN[type_idx][pattern_idx]
}

/// Canonicalize a raw match per its type (lowercase/trim/path-normalize).
fn canonicalize(type_name: &str, raw: &str) -> String {
    match type_name {
        "Technology" | "Concept" | "Project" => raw.to_lowercase(),
        "Url" => raw.trim_end_matches(['.', ',']).to_string(),
        _ => raw.to_string(),
    }
}

/// Entity table coverage check used by tests: the 12 dev-v1 types all have
/// at least one pattern.
pub fn table_is_complete() -> bool {
    TABLE.len() == ENTITY_TYPE_COUNT && TABLE.iter().all(|(_, set)| !set.patterns().is_empty())
}

/// Interner hook (R-M4 parity for extracted names).
pub fn intern_name(interner: &mut Rodeo, name: &str) -> Spur {
    interner.get_or_intern(name)
}

/// Attach extracted entities to a memory (server-side, R-T18).
pub fn attach_entities(m: &mut Memory, extractor: &EntityExtractor) {
    let ids = extractor.entity_ids(&m.content, &m.tags);
    m.context.entities = ids.into_iter().collect();
}
