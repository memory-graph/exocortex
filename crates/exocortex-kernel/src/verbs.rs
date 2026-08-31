// verbs.rs — pack-registered Actions and Functions (PX2, palantir-expansion
// PRD §3.2/§4.1). The `actions!`/`functions!`/`guidance!` sections of the
// `pack!` macro expand into the types collected here.
//
// The split that makes this kernel-pure:
//  - SIGNATURES (name, ceiling, engine, typed input/output names, budgets)
//    land in `PackDef` and therefore in the compatibility fingerprint
//    (OC-PRD D1: meaning-bearing structure). Two components exchanging
//    data must agree on which verbs exist and what they may stamp.
//  - BODIES (Rust action bodies, Scheme function sources) live ONLY in
//    the `inventory` registrations below, never in `PackDef`, so patching
//    a body moves neither fingerprint level (§4.1: "signatures join the
//    compatibility hash, bodies stay out of it").
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{KernelError, MemoryId, Visibility};

/// What a pack action body receives. The ceiling is the framework's
/// enforcement point: every visibility the body produces is clamped (and
/// the commit path rejects rows that would exceed it), so a pack author
/// cannot stamp wider than the declared `min_visibility` no matter what
/// the body does (P3: a compile-time AND framework-enforced property).
#[derive(Clone, Copy, Debug)]
pub struct ActionContext {
    /// The declared visibility ceiling for this verb.
    pub ceiling: Visibility,
}

impl ActionContext {
    /// Clamp a requested visibility to the declared ceiling. Bodies call
    /// this when stamping produced memories.
    pub fn narrow(&self, requested: Visibility) -> Visibility {
        requested.min(self.ceiling)
    }
}

/// Where an action-produced edge points: a draft key within this action's
/// product, or an existing memory id.
#[derive(Clone, Debug)]
pub enum ActionTarget {
    /// A `draft_key` of another memory in the same product.
    Draft(SmolStr),
    /// An existing memory.
    Memory(MemoryId),
}

/// One memory a pack action body produces. `memory_type` is the PACK-LOCAL
/// u8 id (`MemoryType::X.id()` in the pack crate); the framework remaps it
/// to the effective-ontology id through the pack's slot at commit.
#[derive(Clone, Debug)]
pub struct ActionMemory {
    /// Producer-local key for in-batch edge linking.
    pub draft_key: SmolStr,
    /// Pack-local memory type id.
    pub memory_type: u8,
    /// 1..=200 chars (R-T5; enforced by the kernel validator at commit).
    pub title: SmolStr,
    /// Free-text content (R-T5: non-empty).
    pub content: String,
    /// <=500 chars (R-T5).
    pub summary: Option<SmolStr>,
    /// Requested visibility; the framework clamps to the declared ceiling.
    pub visibility: Visibility,
    /// Lowercase tags (normalized at commit).
    pub tags: Vec<SmolStr>,
}

/// One edge a pack action body produces. `kind` is the kind display name
/// (the stable identity surface shared with wire formats and rule
/// sources); the framework resolves it through the effective ontology and
/// rejects unknown or computed-only kinds.
#[derive(Clone, Debug)]
pub struct ActionEdge {
    /// Source draft key within this product.
    pub from_draft_key: SmolStr,
    /// Target: draft key or existing memory.
    pub to: ActionTarget,
    /// Kind display name.
    pub kind: &'static str,
    /// `None` applies the `RelMeta` default strength.
    pub strength: Option<f32>,
}

/// What a pack action body returns: the memories and edges to commit. The
/// framework validates, provenance-stamps, audits, and commits them — the
/// body cannot bypass any of it.
#[derive(Clone, Debug, Default)]
pub struct ActionProduct {
    /// Memories to commit.
    pub memories: Vec<ActionMemory>,
    /// Edges to commit.
    pub edges: Vec<ActionEdge>,
}

impl ActionProduct {
    /// Start an empty product.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one memory (pack-local type id) and return the key handle for
    /// chaining.
    pub fn memory(
        &mut self,
        draft_key: &str,
        memory_type: u8,
        title: &str,
        content: &str,
        visibility: Visibility,
        tags: &[&str],
    ) -> &mut Self {
        self.memories.push(ActionMemory {
            draft_key: SmolStr::new(draft_key),
            memory_type,
            title: SmolStr::new(title),
            content: content.to_owned(),
            summary: None,
            visibility,
            tags: tags.iter().map(|t| SmolStr::new(*t)).collect(),
        });
        self
    }

    /// Attach an optional summary to the most recently added memory.
    pub fn summary(&mut self, summary: &str) -> &mut Self {
        if let Some(last) = self.memories.last_mut() {
            last.summary = Some(SmolStr::new(summary));
        }
        self
    }

    /// Add one edge from a draft key to another draft key.
    pub fn edge_drafts(&mut self, from: &str, to: &str, kind: &'static str) -> &mut Self {
        self.edges.push(ActionEdge {
            from_draft_key: SmolStr::new(from),
            to: ActionTarget::Draft(SmolStr::new(to)),
            kind,
            strength: None,
        });
        self
    }

    /// Add one edge from a draft key to an existing memory.
    pub fn edge_to_memory(&mut self, from: &str, to: MemoryId, kind: &'static str) -> &mut Self {
        self.edges.push(ActionEdge {
            from_draft_key: SmolStr::new(from),
            to: ActionTarget::Memory(to),
            kind,
            strength: None,
        });
        self
    }

    /// Set the strength of the most recently added edge.
    pub fn strength(&mut self, strength: f32) -> &mut Self {
        if let Some(last) = self.edges.last_mut() {
            last.strength = Some(strength);
        }
        self
    }
}

/// Compile-time registration emitted by the `actions!` section (§4.3:
/// macro-generated `inventory::submit!`, one registry — R-P1/R-P2 hold
/// because the operation registry merges these into `entries()`).
#[derive(Clone, Copy)]
pub struct PackActionRegistration {
    /// Owning pack name.
    pub pack_name: &'static str,
    /// Verb name (as declared).
    pub verb_name: &'static str,
    /// Declared visibility ceiling (`min_visibility`).
    pub ceiling: Visibility,
    /// JSON Schema of the typed input.
    pub input_schema: fn() -> schemars::schema::RootSchema,
    /// Deserialize the typed input and run the typed body. The generated
    /// adapter type-checks in the pack crate (PX2-S1 outcome (a)).
    pub run: fn(&ActionContext, serde_json::Value) -> Result<ActionProduct, KernelError>,
}

inventory::collect!(PackActionRegistration);

/// Compile-time registration emitted by the `functions!` section. The body
/// is verbatim source for the declared engine; v1 executes `scheme`
/// bodies through the reasoning crate's embedded Steel interpreter
/// (pure functions over their typed input — the graph-fed contract is
/// recorded as the boundary in the master plan). `datalog` bodies are a
/// pack-compile error: Crepe compiles at build time only, and an
/// unexecutable registration would be a phantom surface.
#[derive(Clone, Copy)]
pub struct PackFunctionRegistration {
    /// Owning pack name.
    pub pack_name: &'static str,
    /// Verb name (as declared).
    pub verb_name: &'static str,
    /// Engine tag; always `scheme` in v1.
    pub engine: &'static str,
    /// Verbatim body source (excluded from both fingerprint levels).
    pub body: &'static str,
    /// p50 latency budget in microseconds (R-Lat1; enforced by the
    /// generated bench harness, not declared-and-forgotten).
    pub p50_budget_us: u32,
    /// p99 latency budget in microseconds.
    pub p99_budget_us: u32,
    /// JSON Schema of the typed input.
    pub input_schema: fn() -> schemars::schema::RootSchema,
    /// JSON Schema of the typed output.
    pub output_schema: fn() -> schemars::schema::RootSchema,
}

inventory::collect!(PackFunctionRegistration);

/// Every registered pack action, sorted by `(pack, verb)` — the
/// deterministic enumeration the operation registry merges.
pub fn registered_pack_actions() -> Vec<&'static PackActionRegistration> {
    let mut all: Vec<&'static PackActionRegistration> = inventory::iter::<PackActionRegistration>
        .into_iter()
        .collect();
    all.sort_by(|a, b| (a.pack_name, a.verb_name).cmp(&(b.pack_name, b.verb_name)));
    all
}

/// Every registered pack function, sorted by `(pack, verb)`.
pub fn registered_pack_functions() -> Vec<&'static PackFunctionRegistration> {
    let mut all: Vec<&'static PackFunctionRegistration> =
        inventory::iter::<PackFunctionRegistration>
            .into_iter()
            .collect();
    all.sort_by(|a, b| (a.pack_name, a.verb_name).cmp(&(b.pack_name, b.verb_name)));
    all
}

/// One structured agent-guidance entry declared in a `guidance!` section
/// (§4.2). Keys and link names resolve against the declaring pack's own
/// type/kind tables at `pack_def()` build time — an entry naming an
/// unknown type or kind fails pack load, exactly as `type_triples!` does.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GuidanceEntry {
    /// The memory type or kind display name this entry advises on.
    pub key: SmolStr,
    /// When the guidance applies (<=160 chars; names nothing checkable).
    pub when: Option<String>,
    /// A caution (<=160 chars; the only other text slot).
    pub caution: Option<String>,
    /// Declared links: `(kind, direction, target-or-source type)` triples
    /// the producer should mint. `=>` is outgoing (key —Kind→ target);
    /// `<=` is incoming (source —Kind→ key).
    pub links: Vec<GuidanceLink>,
}

impl GuidanceEntry {
    /// Human-text budget per §4.2: the escape hatch cannot quietly become
    /// a prose blob.
    pub const MAX_TEXT_CHARS: usize = 160;
}

/// One declared guidance link.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GuidanceLink {
    /// Kind display name.
    pub kind: SmolStr,
    /// `true` for `=>` (outgoing from the key), `false` for `<=`.
    pub outgoing: bool,
    /// The other side's memory type name.
    pub other: SmolStr,
}

/// Signature-level action descriptor carried in `PackDef` (and therefore
/// the compatibility fingerprint). Bodies are deliberately absent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PackActionDef {
    /// Verb name (as declared).
    pub name: SmolStr,
    /// Declared visibility ceiling.
    pub ceiling: Visibility,
    /// Stringified input type name (type-level signature identity).
    pub input_type: SmolStr,
    /// Stringified output type name.
    pub output_type: SmolStr,
}

/// Signature-level function descriptor carried in `PackDef`. Budgets ride
/// the build fingerprint (operational policy, not stored meaning) but not
/// the compatibility summary's verb identity.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PackFunctionDef {
    /// Verb name (as declared).
    pub name: SmolStr,
    /// Engine tag (`scheme`).
    pub engine: SmolStr,
    /// Stringified input type name.
    pub input_type: SmolStr,
    /// Stringified output type name.
    pub output_type: SmolStr,
    /// p50 budget, microseconds.
    pub p50_budget_us: u32,
    /// p99 budget, microseconds.
    pub p99_budget_us: u32,
}

/// Hidden helper the `actions!`/`functions!` munchers use for schema fns
/// (function pointers must be const-constructible; a closure over
/// `schemars::schema_for!` at the call site would capture the type path).
#[doc(hidden)]
pub fn __schema_of<T: schemars::JsonSchema>() -> schemars::schema::RootSchema {
    schemars::schema_for!(T)
}

/// Hidden: decode a pack verb's typed input (the generated `run` adapter
/// calls this so pack crates never need a direct `serde_json` path).
#[doc(hidden)]
pub fn __decode_input<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, KernelError> {
    serde_json::from_value(value).map_err(|e| KernelError::InvalidActionInput(e.to_string()))
}

/// Hidden: one `guidance!` attribute, accumulated by the muncher and
/// folded by [`__guidance_entry`].
#[doc(hidden)]
pub enum __GuidancePiece {
    /// `when: "..."`.
    When(&'static str),
    /// `caution: "..."`.
    Caution(&'static str),
    /// `link: [Kind => Target]` (outgoing) or `[Kind <= Source]`.
    Link(&'static str, bool, &'static str),
}

/// Hidden: fold guidance pieces into one entry, enforcing the §4.2
/// text caps (<=160 chars) at pack-def build time.
#[doc(hidden)]
pub fn __guidance_entry(key: &'static str, pieces: Vec<__GuidancePiece>) -> GuidanceEntry {
    let mut entry = GuidanceEntry {
        key: SmolStr::new_static(key),
        when: None,
        caution: None,
        links: Vec::new(),
    };
    for piece in pieces {
        match piece {
            __GuidancePiece::When(text) => {
                assert!(
                    text.chars().count() <= GuidanceEntry::MAX_TEXT_CHARS,
                    "guidance! `when` for `{key}` exceeds {} chars",
                    GuidanceEntry::MAX_TEXT_CHARS
                );
                entry.when = Some(text.to_owned());
            }
            __GuidancePiece::Caution(text) => {
                assert!(
                    text.chars().count() <= GuidanceEntry::MAX_TEXT_CHARS,
                    "guidance! `caution` for `{key}` exceeds {} chars",
                    GuidanceEntry::MAX_TEXT_CHARS
                );
                entry.caution = Some(text.to_owned());
            }
            __GuidancePiece::Link(kind, outgoing, other) => entry.links.push(GuidanceLink {
                kind: SmolStr::new_static(kind),
                outgoing,
                other: SmolStr::new_static(other),
            }),
        }
    }
    entry
}

/// Kernel error for a pack verb whose typed input could not be decoded —
/// surfaced through the framework as a `BadInput`-class rejection.
impl KernelError {
    /// True when this error is an action-input decode failure.
    pub fn is_invalid_action_input(&self) -> bool {
        matches!(self, KernelError::InvalidActionInput(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guidance_text_caps_are_enforced_at_build_time() {
        let ok = __guidance_entry(
            "K",
            vec![
                __GuidancePiece::When("short"),
                __GuidancePiece::Link("A", true, "B"),
            ],
        );
        assert_eq!(ok.when.as_deref(), Some("short"));
        assert_eq!(ok.links.len(), 1);
        assert!(ok.links[0].outgoing);

        let long: &'static str = Box::leak(
            "x".repeat(GuidanceEntry::MAX_TEXT_CHARS + 1)
                .into_boxed_str(),
        );
        let result = std::panic::catch_unwind(|| {
            __guidance_entry("K", vec![__GuidancePiece::Caution(long)])
        });
        assert!(result.is_err(), "over-length caution must fail pack build");
    }

    #[test]
    fn action_context_narrows_to_the_declared_ceiling() {
        let ctx = ActionContext {
            ceiling: Visibility::Team,
        };
        assert_eq!(ctx.narrow(Visibility::Org), Visibility::Team);
        assert_eq!(ctx.narrow(Visibility::Private), Visibility::Private);
    }
}
