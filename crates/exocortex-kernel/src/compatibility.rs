// compatibility.rs — OC-PRD (docs/prd/ontology-compatibility-prd.md)
//
// The two-level fingerprint (D1) and the per-boundary compatibility
// policy (D2). One hash decides whether two components may talk
// (compatibility); a second, broader hash reports the exact build and
// never gates anything. Superset acceptance (D3) is defined over the
// structured summary below, never over hashes — a hash cannot answer a
// subset question.
use serde::{Deserialize, Serialize};

use crate::kinds::{RelBucket, RelKindId, RelMeta};
use crate::pack::{PackDef, TypeTriple};
use crate::Ontology;

/// The compatibility fingerprint (§7.17 as refined by OC-PRD D1): SHA-256
/// over exactly the ontology's meaning — the ordered name→u8 mappings for
/// memory and entity types, every kind row (ids, names, directions,
/// including R-T4 inverse companions), the type-triple table, and the
/// rule-id list — under a kernel domain separator. Release metadata
/// (`version`, `kernel_min`) is excluded by construction (D5).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityFingerprint(pub [u8; 32]);

/// The build fingerprint (OC-PRD D1): SHA-256 over the full `PackDef`
/// set exactly as scheme v1 computed it, release metadata included. It
/// reports which build produced a component; it never gates. Because it
/// reproduces the v1 algorithm byte-for-byte, it is also the value every
/// pre-OC graph and backup pinned, which is what makes D4's migration
/// check possible.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BuildFingerprint(pub [u8; 32]);

/// One kind row inside an [`OntologySummary`] — the fingerprint-relevant
/// slice of [`RelMeta`], in ontology space (canonical pack ids).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KindSummary {
    /// Canonical kind id.
    pub id: u32,
    /// Stable display name (also the Cypher label, R-T2).
    pub name: String,
    /// Bucket tag (0..=7 canonical, 8 = extension with payload).
    pub bucket: u8,
    /// Extension payload when `bucket == 8`, else 0.
    pub bucket_extension: u16,
    /// Inverse companion id, when any (R-T4).
    pub inverse: Option<u32>,
    /// Symmetric/bidirectional kinds match in both directions.
    pub bidirectional: bool,
    /// Strength applied when an `EdgeHint` omits strength.
    pub default_strength: f32,
    /// R-T14: produced exclusively by backend computation.
    pub computed_only: bool,
}

/// One type-triple rule inside an [`OntologySummary`].
#[derive(Clone, Debug, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct TripleSummary {
    /// The kind the rule governs.
    pub kind: u32,
    /// `None` matches any memory type.
    pub from_types: Option<Vec<u8>>,
    /// `None` matches any memory type.
    pub to_types: Option<Vec<u8>>,
}

/// One pack-registered Action's signature inside an [`OntologySummary`]
/// (PX2 §4.1: signatures join the compatibility hash; bodies never do).
#[derive(Clone, Debug, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct ActionVerbSummary {
    /// Owning pack name.
    pub pack: String,
    /// Verb name.
    pub name: String,
    /// Declared visibility ceiling.
    pub ceiling: u8,
    /// Stringified input type name.
    pub input_type: String,
    /// Stringified output type name.
    pub output_type: String,
}

/// One pack-registered Function's signature inside an [`OntologySummary`]
/// (budgets excluded — operational policy, not stored meaning).
#[derive(Clone, Debug, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct FunctionVerbSummary {
    /// Owning pack name.
    pub pack: String,
    /// Verb name.
    pub name: String,
    /// Engine tag.
    pub engine: String,
    /// Stringified input type name.
    pub input_type: String,
    /// Stringified output type name.
    pub output_type: String,
}

/// The structured inputs of the compatibility fingerprint (OC-PRD D3):
/// everything two components must agree on for stored meaning to be
/// preserved. Serializable so it can ride a graph pin, a backup header,
/// or diagnostics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OntologySummary {
    /// Memory type names in id order (the positional-id hazard lives
    /// here and nowhere else, §1.4 of the OC-PRD).
    pub memory_types: Vec<String>,
    /// Entity type names in id order.
    pub entity_types: Vec<String>,
    /// Every kind row — authored kinds and R-T4 companions — sorted by
    /// canonical id.
    pub kinds: Vec<KindSummary>,
    /// Type-triple rules, deterministically ordered.
    pub type_triples: Vec<TripleSummary>,
    /// Rule ids for fingerprinting, in pack order (name-sorted packs).
    pub rule_ids: Vec<String>,
    /// Pack-registered Action signatures (PX2), sorted by `(pack, name)`.
    /// Adding a verb to a pack is a superset event (OC-PRD D3); changing
    /// an existing verb's signature is a compatibility break.
    pub actions: Vec<ActionVerbSummary>,
    /// Pack-registered Function signatures (PX2), sorted by `(pack, name)`.
    pub functions: Vec<FunctionVerbSummary>,
}

/// The v2 pinned-ontology record persisted in a graph's metadata
/// (OC-PRD D4). Replaces the bare 64-hex v1 value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PinnedOntology {
    /// Record scheme; always 2 for this shape.
    pub scheme: u32,
    /// Hex of the compatibility fingerprint this pin was written under.
    pub compatibility: String,
    /// Hex of the build fingerprint at pin time (reporting only).
    pub build: String,
    /// The structured summary the compatibility fingerprint covers.
    pub summary: OntologySummary,
    /// Compatibility fingerprints this graph still recognizes from
    /// producers (rolling-upgrade window, OC-PRD D3): the prior pin's
    /// value, then its own history, most recent first, bounded.
    pub accepted: Vec<String>,
}

/// Maximum number of retained recognized fingerprints in a pin record.
pub const MAX_ACCEPTED_FINGERPRINTS: usize = 8;

impl PinnedOntology {
    /// Build the record describing `ontology` with the given accepted
    /// history (already ordered most-recent-first, already bounded by
    /// the caller or by [`Self::advance`]).
    pub fn describing(ontology: &Ontology) -> Self {
        Self {
            scheme: 2,
            compatibility: hex(&ontology.fingerprint.0),
            build: hex(&ontology.build_fingerprint.0),
            summary: ontology.summary.clone(),
            accepted: Vec::new(),
        }
    }
}

/// What a persisted pin value decoded to (OC-PRD D4): absent, a legacy
/// scheme-v1 64-hex value, or a v2 record.
#[derive(Clone, Debug, PartialEq)]
pub enum PersistedPin {
    /// No pin row at all (a fresh graph).
    Absent,
    /// A pre-OC 64-hex value — the v1-scheme fingerprint, which equals
    /// the writing build's [`BuildFingerprint`].
    LegacyV1([u8; 32]),
    /// A scheme-2 record.
    V2(Box<PinnedOntology>),
}

/// Parse a persisted pin value. Malformed hex, malformed JSON, or a
/// non-2 scheme fails closed as corruption — never as absence.
pub fn parse_pin(value: &str) -> Result<PersistedPin, CompatibilityError> {
    let trimmed = value.trim();
    if trimmed.starts_with('{') {
        let record: PinnedOntology = serde_json::from_str(trimmed)
            .map_err(|e| CompatibilityError::CorruptRecord(e.to_string()))?;
        if record.scheme != 2 {
            return Err(CompatibilityError::CorruptRecord(format!(
                "unsupported scheme {} (this build reads 2)",
                record.scheme
            )));
        }
        return Ok(PersistedPin::V2(Box::new(record)));
    }
    if trimmed.len() != 64 || !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CompatibilityError::CorruptRecord(format!(
            "expected 64 hex characters or a scheme-2 record, found {} bytes",
            trimmed.len()
        )));
    }
    let mut fp = [0u8; 32];
    for (i, slot) in fp.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16)
            .map_err(|_| CompatibilityError::CorruptRecord("bad hexadecimal byte".into()))?;
    }
    Ok(PersistedPin::LegacyV1(fp))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn bucket_parts(bucket: RelBucket) -> (u8, u16) {
    match bucket {
        RelBucket::Causal => (0, 0),
        RelBucket::Solution => (1, 0),
        RelBucket::Context => (2, 0),
        RelBucket::Learning => (3, 0),
        RelBucket::Similarity => (4, 0),
        RelBucket::Workflow => (5, 0),
        RelBucket::Quality => (6, 0),
        RelBucket::Integration => (7, 0),
        RelBucket::Extension(ext) => (8, ext),
    }
}

fn triple_summary(t: &TypeTriple) -> TripleSummary {
    TripleSummary {
        kind: t.kind.0,
        from_types: t.from_types.clone(),
        to_types: t.to_types.clone(),
    }
}

fn kind_summary(k: &RelMeta) -> KindSummary {
    let (bucket, bucket_extension) = bucket_parts(k.bucket);
    KindSummary {
        id: k.id.0,
        name: k.display_name.to_string(),
        bucket,
        bucket_extension,
        inverse: k.inverse.map(|i| i.0),
        bidirectional: k.bidirectional,
        default_strength: k.default_strength,
        computed_only: k.computed_only,
    }
}

/// Sort packs by name and apply the same canonicalization
/// `Ontology::from_packs` applies (pack slot assignment and triple-side
/// remapping), without the validation. Shared by the summary builder so
/// a fingerprint computed over a raw pack list and one computed over the
/// assembled ontology agree for a single-pack set.
pub(crate) fn canonicalized(packs: &[PackDef]) -> Vec<PackDef> {
    let mut packs: Vec<PackDef> = packs.to_vec();
    packs.sort_by(|a, b| a.name.cmp(&b.name));
    let mt_offsets: Vec<u8> = {
        let mut v = Vec::with_capacity(packs.len());
        let mut acc: u8 = 0;
        for p in &packs {
            v.push(acc);
            acc = acc.saturating_add(p.memory_type_names.len() as u8);
        }
        v
    };
    for (i, p) in packs.iter_mut().enumerate() {
        let slot = (i as u32) << 16;
        let mt_offset = mt_offsets[i];
        for k in p.kinds.iter_mut() {
            if !k.id.is_kernel() {
                k.id = RelKindId(0x8000_0000 | slot | k.id.local_part());
            }
            if let Some(inv) = k.inverse {
                if !inv.is_kernel() {
                    k.inverse = Some(RelKindId(0x8000_0000 | slot | inv.local_part()));
                }
            }
        }
        for t in p.type_triples.iter_mut() {
            if !t.kind.is_kernel() {
                t.kind = RelKindId(0x8000_0000 | slot | t.kind.local_part());
            }
            let remap = |side: &mut Option<Vec<u8>>| {
                if let Some(xs) = side {
                    for x in xs {
                        *x = mt_offset.saturating_add(*x);
                    }
                }
            };
            remap(&mut t.from_types);
            remap(&mut t.to_types);
        }
    }
    packs
}

impl OntologySummary {
    /// Build the summary over canonicalized packs (the same input order
    /// `Ontology::from_packs` establishes).
    pub fn of_canonical_packs(packs: &[PackDef]) -> Self {
        let mut memory_types = Vec::new();
        let mut entity_types = Vec::new();
        let mut kinds = Vec::new();
        let mut type_triples = Vec::new();
        let mut rule_ids = Vec::new();
        let mut actions = Vec::new();
        let mut functions = Vec::new();
        for p in packs {
            memory_types.extend(p.memory_type_names.iter().map(|n| n.to_string()));
            entity_types.extend(p.entity_type_names.iter().map(|n| n.to_string()));
            kinds.extend(p.kinds.iter().map(kind_summary));
            type_triples.extend(p.type_triples.iter().map(triple_summary));
            rule_ids.extend(p.rule_ids.iter().map(|n| n.to_string()));
            actions.extend(p.actions.iter().map(|a| ActionVerbSummary {
                pack: p.name.to_string(),
                name: a.name.to_string(),
                ceiling: a.ceiling as u8,
                input_type: a.input_type.to_string(),
                output_type: a.output_type.to_string(),
            }));
            functions.extend(p.functions.iter().map(|f| FunctionVerbSummary {
                pack: p.name.to_string(),
                name: f.name.to_string(),
                engine: f.engine.to_string(),
                input_type: f.input_type.to_string(),
                output_type: f.output_type.to_string(),
            }));
        }
        kinds.sort_by_key(|k| k.id);
        type_triples.sort_by(|a, b| {
            a.kind
                .cmp(&b.kind)
                .then(a.from_types.cmp(&b.from_types))
                .then(a.to_types.cmp(&b.to_types))
        });
        actions.sort();
        functions.sort();
        Self {
            memory_types,
            entity_types,
            kinds,
            type_triples,
            rule_ids,
            actions,
            functions,
        }
    }

    /// The compatibility fingerprint over this summary (OC-PRD D1).
    /// Every section is length-prefixed under one domain separator so
    /// no field boundary is ambiguous.
    pub fn compatibility_fingerprint(&self) -> CompatibilityFingerprint {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(b"exocortex-compat-v2\x1e");
        let str_section = |h: &mut Sha256, values: &[String]| {
            h.update((values.len() as u32).to_le_bytes());
            for v in values {
                h.update((v.len() as u32).to_le_bytes());
                h.update(v.as_bytes());
            }
        };
        str_section(&mut h, &self.memory_types);
        str_section(&mut h, &self.entity_types);
        h.update((self.kinds.len() as u32).to_le_bytes());
        for k in &self.kinds {
            h.update(k.id.to_le_bytes());
            h.update((k.name.len() as u32).to_le_bytes());
            h.update(k.name.as_bytes());
            h.update([k.bucket]);
            h.update(k.bucket_extension.to_le_bytes());
            h.update([u8::from(k.inverse.is_some())]);
            if let Some(inv) = k.inverse {
                h.update(inv.to_le_bytes());
            }
            h.update([u8::from(k.bidirectional)]);
            h.update(k.default_strength.to_le_bytes());
            h.update([u8::from(k.computed_only)]);
        }
        h.update((self.type_triples.len() as u32).to_le_bytes());
        for t in &self.type_triples {
            h.update(t.kind.to_le_bytes());
            let side = |h: &mut Sha256, side: &Option<Vec<u8>>| match side {
                None => h.update([0u8]),
                Some(xs) => {
                    h.update([1u8]);
                    h.update((xs.len() as u32).to_le_bytes());
                    for x in xs {
                        h.update([*x]);
                    }
                }
            };
            side(&mut h, &t.from_types);
            side(&mut h, &t.to_types);
        }
        str_section(&mut h, &self.rule_ids);
        // PX2 verb signatures: length-prefixed like every other section,
        // budgets excluded (operational policy, not stored meaning).
        let action_verb = |h: &mut Sha256, v: &ActionVerbSummary| {
            h.update((v.pack.len() as u32).to_le_bytes());
            h.update(v.pack.as_bytes());
            h.update((v.name.len() as u32).to_le_bytes());
            h.update(v.name.as_bytes());
            h.update([v.ceiling]);
            h.update((v.input_type.len() as u32).to_le_bytes());
            h.update(v.input_type.as_bytes());
            h.update((v.output_type.len() as u32).to_le_bytes());
            h.update(v.output_type.as_bytes());
        };
        h.update((self.actions.len() as u32).to_le_bytes());
        for v in &self.actions {
            action_verb(&mut h, v);
        }
        let function_verb = |h: &mut Sha256, v: &FunctionVerbSummary| {
            h.update((v.pack.len() as u32).to_le_bytes());
            h.update(v.pack.as_bytes());
            h.update((v.name.len() as u32).to_le_bytes());
            h.update(v.name.as_bytes());
            h.update((v.engine.len() as u32).to_le_bytes());
            h.update(v.engine.as_bytes());
            h.update((v.input_type.len() as u32).to_le_bytes());
            h.update(v.input_type.as_bytes());
            h.update((v.output_type.len() as u32).to_le_bytes());
            h.update(v.output_type.as_bytes());
        };
        h.update((self.functions.len() as u32).to_le_bytes());
        for v in &self.functions {
            function_verb(&mut h, v);
        }
        let out: [u8; 32] = h.finalize().into();
        CompatibilityFingerprint(out)
    }

    /// OC-PRD D3: `self` is a subset of `superset` when every memory
    /// type, entity type, kind, and triple of `self` appears in
    /// `superset` with an identical id, and `superset` adds only new
    /// entries at unused ids. Type-name lists are positional, so a
    /// subset must be a prefix. Kind/triple/rule sets compare by value.
    pub fn is_subset_of(&self, superset: &OntologySummary) -> bool {
        if self.memory_types.len() > superset.memory_types.len()
            || self.entity_types.len() > superset.entity_types.len()
        {
            return false;
        }
        if self.memory_types != superset.memory_types[..self.memory_types.len()] {
            return false;
        }
        if self.entity_types != superset.entity_types[..self.entity_types.len()] {
            return false;
        }
        let merged: std::collections::BTreeMap<_, _> =
            superset.kinds.iter().map(|k| (k.id, k)).collect();
        for row in &self.kinds {
            match merged.get(&row.id) {
                Some(other) if *other == row => {}
                _ => return false,
            }
        }
        let ours: std::collections::BTreeSet<_> = self.type_triples.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.type_triples.iter().collect();
        if !ours.is_subset(&theirs) {
            return false;
        }
        let ours: std::collections::BTreeSet<_> = self.rule_ids.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.rule_ids.iter().collect();
        if !ours.is_subset(&theirs) {
            return false;
        }
        // PX2: an existing verb must be identical; a superset may only ADD
        // verbs (the same discipline OC-PRD D3 applies to kinds/triples).
        let ours: std::collections::BTreeSet<_> = self.actions.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.actions.iter().collect();
        if !ours.is_subset(&theirs) {
            return false;
        }
        let ours: std::collections::BTreeSet<_> = self.functions.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.functions.iter().collect();
        ours.is_subset(&theirs)
    }

    /// A human-legible first divergence between `self` and `superset`
    /// (used by the mismatch errors; not a complete diff).
    pub fn first_divergence(&self, superset: &OntologySummary) -> Option<String> {
        for (i, name) in self.memory_types.iter().enumerate() {
            match superset.memory_types.get(i) {
                Some(other) if other == name => {}
                Some(other) => {
                    return Some(format!(
                        "memory type id {i} is {name:?} here but {other:?} there"
                    ))
                }
                None => return Some(format!("memory type {name:?} (id {i}) is missing")),
            }
        }
        for (i, name) in self.entity_types.iter().enumerate() {
            match superset.entity_types.get(i) {
                Some(other) if other == name => {}
                Some(other) => {
                    return Some(format!(
                        "entity type id {i} is {name:?} here but {other:?} there"
                    ))
                }
                None => return Some(format!("entity type {name:?} (id {i}) is missing")),
            }
        }
        let theirs: std::collections::BTreeMap<_, _> =
            superset.kinds.iter().map(|k| (k.id, k)).collect();
        for row in &self.kinds {
            match theirs.get(&row.id) {
                Some(other) if *other == row => {}
                Some(_) => return Some(format!("kind id {:#x} differs", row.id)),
                None => return Some(format!("kind {} (id {:#x}) is missing", row.name, row.id)),
            }
        }
        let ours: std::collections::BTreeSet<_> = self.type_triples.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.type_triples.iter().collect();
        if let Some(t) = ours.difference(&theirs).next() {
            return Some(format!(
                "type-triple rule for kind {:#x} is missing",
                t.kind
            ));
        }
        let ours: std::collections::BTreeSet<_> = self.rule_ids.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.rule_ids.iter().collect();
        if let Some(r) = ours.difference(&theirs).next() {
            return Some(format!("rule {r:?} is missing"));
        }
        let ours: std::collections::BTreeSet<_> = self.actions.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.actions.iter().collect();
        if let Some(v) = ours.difference(&theirs).next() {
            return Some(format!(
                "pack action {}::{} differs or is missing",
                v.pack, v.name
            ));
        }
        let ours: std::collections::BTreeSet<_> = self.functions.iter().collect();
        let theirs: std::collections::BTreeSet<_> = superset.functions.iter().collect();
        if let Some(v) = ours.difference(&theirs).next() {
            return Some(format!(
                "pack function {}::{} differs or is missing",
                v.pack, v.name
            ));
        }
        None
    }
}

/// Every fingerprint comparison failure, named per boundary (OC-PRD
/// D2). Errors carry the specific divergence whenever structure is
/// available (open question 3).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompatibilityError {
    /// A legacy v1 pin does not match the runtime's v1-scheme
    /// recomputation (its build fingerprint).
    LegacyPinMismatch {
        /// The value persisted in the graph.
        pinned: [u8; 32],
        /// This runtime's v1-scheme recomputation.
        runtime: [u8; 32],
    },
    /// The pinned summary is not a subset of the runtime summary.
    NotASuperset {
        /// First structural divergence, when one was found.
        divergence: Option<String>,
    },
    /// A v2 record's summary does not hash to its recorded
    /// compatibility fingerprint.
    CorruptRecord(String),
    /// A producer offered a fingerprint that is neither current nor
    /// recognized.
    ProducerNotAdmitted {
        /// The offered fingerprint bytes.
        offered: [u8; 32],
        /// The current compatibility fingerprint.
        current: [u8; 32],
    },
    /// A peer (cluster or SSE) offered a different compatibility
    /// fingerprint; this boundary keeps exact equality.
    PeerMismatch {
        /// The peer's fingerprint bytes.
        offered: [u8; 32],
        /// This node's fingerprint bytes.
        ours: [u8; 32],
    },
}

impl std::fmt::Display for CompatibilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompatibilityError::LegacyPinMismatch { pinned, runtime } => write!(
                f,
                "pinned ontology fingerprint {} does not match this build's recomputation {} — the graph was written against a different ontology",
                hex(pinned),
                hex(runtime)
            ),
            CompatibilityError::NotASuperset { divergence } => match divergence {
                Some(d) => write!(
                    f,
                    "runtime ontology is not a superset of the pinned ontology: {d}"
                ),
                None => write!(
                    f,
                    "runtime ontology is not a superset of the pinned ontology"
                ),
            },
            CompatibilityError::CorruptRecord(detail) => {
                write!(f, "corrupt pinned ontology record: {detail}")
            }
            CompatibilityError::ProducerNotAdmitted { offered, current } => write!(
                f,
                "ontology fingerprint {} is not accepted by this server (current {}); producers may know less than the server, never more — re-negotiate with a Fingerprint request",
                hex(offered),
                hex(current)
            ),
            CompatibilityError::PeerMismatch { offered, ours } => write!(
                f,
                "peer ontology fingerprint {} does not match ours {} — cluster and SSE boundaries require exact compatibility-fingerprint equality",
                hex(offered),
                hex(ours)
            ),
        }
    }
}

impl std::error::Error for CompatibilityError {}

impl CompatibilityError {
    /// The two raw hashes involved, when the failure is hash-level.
    pub fn hash_pair(&self) -> Option<([u8; 32], [u8; 32])> {
        match self {
            CompatibilityError::LegacyPinMismatch { pinned, runtime } => Some((*pinned, *runtime)),
            CompatibilityError::ProducerNotAdmitted { offered, current } => {
                Some((*offered, *current))
            }
            CompatibilityError::PeerMismatch { offered, ours } => Some((*offered, *ours)),
            _ => None,
        }
    }
}

/// What the node↔graph boundary decided (OC-PRD D2 row 1).
#[derive(Clone, Debug, PartialEq)]
pub enum NodeGraphDecision {
    /// The pinned record already describes this runtime; nothing to do.
    Satisfied,
    /// The runtime is a strict superset (or a build refresh): persist
    /// this replacement record.
    Advance(PinnedOntology),
    /// A legacy v1 pin matched the runtime's build fingerprint: migrate
    /// the pin to this v2 record.
    Migrate(PinnedOntology),
}

fn bounded_accepted(prior: &[String], newest: String) -> Vec<String> {
    let mut out = vec![newest];
    for fp in prior {
        if out.len() >= MAX_ACCEPTED_FINGERPRINTS {
            break;
        }
        if !out.contains(fp) {
            out.push(fp.clone());
        }
    }
    out
}

/// The node↔its-graph boundary (OC-PRD D2): the runtime ontology must
/// be equal to or a superset of the pinned one. A v1 pin is checked
/// against the v1-scheme recomputation (the build fingerprint) exactly
/// as pre-OC binaries did, then migrates.
pub fn admit_node_graph(
    pin: PersistedPin,
    ours: &Ontology,
) -> Result<NodeGraphDecision, CompatibilityError> {
    match pin {
        PersistedPin::Absent => Ok(NodeGraphDecision::Satisfied),
        PersistedPin::LegacyV1(pinned) => {
            if pinned == ours.build_fingerprint.0 {
                let mut record = PinnedOntology::describing(ours);
                // Pre-OC producers echo the v1-scheme hash; keeping it
                // recognized preserves their rolling window.
                record.accepted = vec![hex(&pinned)];
                Ok(NodeGraphDecision::Migrate(record))
            } else {
                Err(CompatibilityError::LegacyPinMismatch {
                    pinned,
                    runtime: ours.build_fingerprint.0,
                })
            }
        }
        PersistedPin::V2(record) => {
            let stated = record.summary.compatibility_fingerprint();
            if hex(&stated.0) != record.compatibility {
                return Err(CompatibilityError::CorruptRecord(format!(
                    "summary hashes to {} but the record claims {}",
                    hex(&stated.0),
                    record.compatibility
                )));
            }
            if record.summary == ours.summary {
                if record.build == hex(&ours.build_fingerprint.0) {
                    return Ok(NodeGraphDecision::Satisfied);
                }
                let mut next = PinnedOntology::describing(ours);
                next.accepted = record.accepted.clone();
                return Ok(NodeGraphDecision::Advance(next));
            }
            if record.summary.is_subset_of(&ours.summary) {
                let mut next = PinnedOntology::describing(ours);
                next.accepted = bounded_accepted(&record.accepted, record.compatibility.clone());
                Ok(NodeGraphDecision::Advance(next))
            } else {
                Err(CompatibilityError::NotASuperset {
                    divergence: ours.summary.first_divergence(&record.summary),
                })
            }
        }
    }
}

/// The ingest↔producer boundary (OC-PRD D2): the offered fingerprint
/// must be the current compatibility fingerprint or one the graph
/// still recognizes (the rolling-upgrade window).
pub fn admit_producer_batch(
    offered: &[u8],
    ours: &Ontology,
    recognized: &[[u8; 32]],
) -> Result<(), CompatibilityError> {
    let tried: [u8; 32] =
        offered
            .try_into()
            .map_err(|_| CompatibilityError::ProducerNotAdmitted {
                offered: [0; 32],
                current: ours.fingerprint.0,
            })?;
    if tried == ours.fingerprint.0 || recognized.contains(&tried) {
        return Ok(());
    }
    Err(CompatibilityError::ProducerNotAdmitted {
        offered: tried,
        current: ours.fingerprint.0,
    })
}

/// The cluster-peer and SSE-subscriber boundaries (OC-PRD D2): exact
/// compatibility-fingerprint equality. An invalidation cannot be
/// revalidated, so this boundary keeps the strict rule.
pub fn admit_peer(offered: &[u8], ours: &[u8; 32]) -> Result<(), CompatibilityError> {
    let tried: [u8; 32] = offered
        .try_into()
        .map_err(|_| CompatibilityError::PeerMismatch {
            offered: [0; 32],
            ours: *ours,
        })?;
    if tried == *ours {
        Ok(())
    } else {
        Err(CompatibilityError::PeerMismatch {
            offered: tried,
            ours: *ours,
        })
    }
}

/// The backup-restore boundary (OC-PRD D2): superset accepted, because
/// every draft is revalidated against the current rulebook before the
/// WAL is touched. Legacy backups carry no structure and keep the v1
/// behavior (exact build-fingerprint equality).
pub fn admit_backup(backup: BackupOntology<'_>, ours: &Ontology) -> Result<(), CompatibilityError> {
    match backup {
        BackupOntology::Summarized { summary } => {
            if summary.is_subset_of(&ours.summary) {
                Ok(())
            } else {
                Err(CompatibilityError::NotASuperset {
                    divergence: ours.summary.first_divergence(summary),
                })
            }
        }
        BackupOntology::Legacy { fingerprint_hex } => {
            let expected = hex(&ours.build_fingerprint.0);
            if fingerprint_hex == expected {
                Ok(())
            } else {
                Err(CompatibilityError::LegacyPinMismatch {
                    pinned: [0; 32],
                    runtime: ours.build_fingerprint.0,
                })
            }
        }
    }
}

/// What a backup document carries about the ontology it was written
/// under.
#[derive(Clone, Copy, Debug)]
pub enum BackupOntology<'a> {
    /// A post-OC backup: the structured summary.
    Summarized {
        /// The summary the backup was exported under.
        summary: &'a OntologySummary,
    },
    /// A pre-OC backup: only the v1-scheme fingerprint hex.
    Legacy {
        /// Hex of the v1-scheme (build) fingerprint at export time.
        fingerprint_hex: &'a str,
    },
}

impl CompatibilityFingerprint {
    /// Compute over a raw pack list (the §7.17 entry point). Sorts and
    /// canonicalizes exactly as `Ontology::from_packs` does, so a
    /// fingerprint computed over a pack list and the assembled
    /// ontology's own fingerprint agree.
    pub fn compute(packs: &[PackDef]) -> Self {
        OntologySummary::of_canonical_packs(&canonicalized(packs)).compatibility_fingerprint()
    }
}

/// The build fingerprint over a raw pack list (OC-PRD D1). This is the
/// scheme-v1 algorithm, unchanged: packs name-sorted, bincode-encoded,
/// length-prefixed, under the v1 domain separator.
pub fn build_fingerprint(packs: &[PackDef]) -> BuildFingerprint {
    use sha2::{Digest, Sha256};
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
    BuildFingerprint(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PackVersion;

    fn fixture_pack() -> PackDef {
        use crate::kinds::{CAUSES, FIXES, IN_SESSION, SOLVES};
        let kind = |id: crate::RelKindId,
                    name: &str,
                    bucket: crate::RelBucket,
                    inverse: Option<crate::RelKindId>,
                    default_strength: f32| {
            crate::RelMeta {
                id,
                display_name: name.into(),
                bucket,
                inverse,
                bidirectional: false,
                default_strength,
                computed_only: false,
            }
        };
        PackDef {
            name: "compat-fixture".into(),
            version: crate::PackVersion {
                major: 1,
                minor: 0,
                patch: 0,
            },
            kernel_min: crate::PackVersion {
                major: 1,
                minor: 0,
                patch: 0,
            },
            memory_type_names: vec![
                "Problem".into(),
                "Solution".into(),
                "Fix".into(),
                "Error".into(),
            ],
            entity_type_names: vec!["File".into(), "Concept".into()],
            kinds: vec![
                kind(SOLVES, "Solves", crate::RelBucket::Solution, None, 0.85),
                kind(FIXES, "Fixes", crate::RelBucket::Causal, None, 0.90),
                kind(CAUSES, "Causes", crate::RelBucket::Causal, None, 0.85),
                kind(
                    IN_SESSION,
                    "InSession",
                    crate::RelBucket::Context,
                    None,
                    0.80,
                ),
                kind(
                    crate::RelKindId(0x8000_0000),
                    "BuildsOn",
                    crate::RelBucket::Learning,
                    Some(crate::RelKindId(0x8000_0001)),
                    0.75,
                ),
                kind(
                    crate::RelKindId(0x8000_0001),
                    "BuiltOnBy",
                    crate::RelBucket::Learning,
                    Some(crate::RelKindId(0x8000_0000)),
                    0.75,
                ),
            ],
            type_triples: vec![
                crate::pack::TypeTriple {
                    kind: SOLVES,
                    from_types: Some(vec![1, 2]),
                    to_types: Some(vec![0, 3]),
                },
                crate::pack::TypeTriple {
                    kind: crate::RelKindId(0x8000_0000),
                    from_types: None,
                    to_types: None,
                },
            ],
            rule_ids: vec!["compat_rule".into()],
            actions: Vec::new(),
            functions: Vec::new(),
            guidance: Vec::new(),
        }
    }

    #[test]
    fn parse_pin_decodes_legacy_and_v2_fail_closed() {
        assert!(parse_pin("").is_err());
        let legacy = "e1f7d17b90d59b6d09261203b7488c142cdb6b117d72de0b3cb1897483ddc9b2";
        assert!(matches!(parse_pin(legacy), Ok(PersistedPin::LegacyV1(_))));
        let bad_len = &legacy[..63];
        assert!(parse_pin(bad_len).is_err());
        let bad_hex = format!("{}z", &legacy[..63]);
        assert!(parse_pin(&bad_hex).is_err());
        assert!(parse_pin("{\"scheme\":3}").is_err());
        assert!(parse_pin("{not json").is_err());
    }

    #[test]
    fn metadata_only_change_leaves_compatibility_and_moves_build() {
        let base = fixture_pack();
        let mut bumped = base.clone();
        bumped.version = PackVersion {
            major: 9,
            minor: 9,
            patch: 9,
        };
        bumped.kernel_min = PackVersion {
            major: 9,
            minor: 0,
            patch: 0,
        };
        let a = OntologySummary::of_canonical_packs(&canonicalized(std::slice::from_ref(&base)));
        let b = OntologySummary::of_canonical_packs(&canonicalized(std::slice::from_ref(&bumped)));
        assert_eq!(a, b);
        assert_eq!(a.compatibility_fingerprint(), b.compatibility_fingerprint());
        assert_ne!(
            build_fingerprint(&[base.clone()]),
            build_fingerprint(&[bumped])
        );
    }

    #[test]
    fn appended_type_is_a_superset_and_keeps_ids() {
        let mut grown = fixture_pack();
        grown.memory_type_names.push("FutureThing".into());
        let small = OntologySummary::of_canonical_packs(&canonicalized(&[fixture_pack()]));
        let big = OntologySummary::of_canonical_packs(&canonicalized(&[grown]));
        assert!(small.is_subset_of(&big));
        assert!(!big.is_subset_of(&small));
        assert_eq!(
            &big.memory_types[..small.memory_types.len()],
            &small.memory_types
        );
        assert_eq!(small.first_divergence(&big), None);
        assert_eq!(
            big.first_divergence(&small),
            Some("memory type \"FutureThing\" (id 4) is missing".to_string())
        );
    }

    #[test]
    fn inserted_type_is_not_a_superset() {
        let mut inserted = fixture_pack();
        inserted.memory_type_names.insert(0, "AlphaHazard".into());
        let small = OntologySummary::of_canonical_packs(&canonicalized(&[fixture_pack()]));
        let shifted = OntologySummary::of_canonical_packs(&canonicalized(&[inserted]));
        assert!(!small.is_subset_of(&shifted));
    }

    #[test]
    fn removed_rule_or_kind_breaks_subset() {
        let mut shrunk = fixture_pack();
        shrunk.rule_ids.pop();
        let full = OntologySummary::of_canonical_packs(&canonicalized(&[fixture_pack()]));
        let less = OntologySummary::of_canonical_packs(&canonicalized(&[shrunk]));
        assert!(!full.is_subset_of(&less));
        assert_eq!(less.first_divergence(&full), None);
        assert!(full.first_divergence(&less).is_some());
    }

    #[test]
    fn node_graph_policy_migrates_satisfies_and_advances() {
        let onto = Ontology::from_packs(vec![fixture_pack()]).expect("valid");
        // Legacy pin equal to the build fingerprint migrates and keeps
        // the legacy value recognized for pre-OC producers.
        let decision = admit_node_graph(PersistedPin::LegacyV1(onto.build_fingerprint.0), &onto)
            .expect("legacy pin migrates");
        let record = match &decision {
            NodeGraphDecision::Migrate(r) => r,
            other => panic!("expected Migrate, got {other:?}"),
        };
        assert_eq!(record.accepted, vec![hex(&onto.build_fingerprint.0)]);
        // Replaying the migrated record satisfies.
        let replay = admit_node_graph(PersistedPin::V2(Box::new(record.clone())), &onto)
            .expect("v2 record satisfies");
        assert_eq!(replay, NodeGraphDecision::Satisfied);
        // A legacy pin that does not match refuses.
        let err = admit_node_graph(PersistedPin::LegacyV1([7; 32]), &onto);
        assert!(matches!(
            err,
            Err(CompatibilityError::LegacyPinMismatch { .. })
        ));
    }

    #[test]
    fn node_graph_policy_advances_on_superset_and_refuses_on_break() {
        let small_onto = Ontology::from_packs(vec![fixture_pack()]).expect("valid");
        let mut grown = fixture_pack();
        grown.memory_type_names.push("FutureThing".into());
        let big_onto = Ontology::from_packs(vec![grown]).expect("valid");

        let pinned = PinnedOntology::describing(&small_onto);
        let decision = admit_node_graph(PersistedPin::V2(Box::new(pinned.clone())), &big_onto)
            .expect("superset advances the pin");
        match decision {
            NodeGraphDecision::Advance(next) => {
                assert_eq!(next.compatibility, hex(&big_onto.fingerprint.0));
                assert_eq!(next.accepted, vec![pinned.compatibility.clone()]);
            }
            other => panic!("expected Advance, got {other:?}"),
        }

        // The reverse boot (smaller runtime against a bigger pin) refuses.
        let err = admit_node_graph(PersistedPin::V2(Box::new(pinned)), &small_onto)
            .map(|d| format!("{d:?}"));
        let _ = err;
        let pinned_big = PinnedOntology::describing(&big_onto);
        let err = admit_node_graph(PersistedPin::V2(Box::new(pinned_big)), &small_onto);
        assert!(matches!(err, Err(CompatibilityError::NotASuperset { .. })));

        // A record whose summary does not hash to its claimed
        // compatibility fingerprint is corruption.
        let mut corrupt = PinnedOntology::describing(&small_onto);
        corrupt.compatibility = hex(&[9; 32]);
        let err = admit_node_graph(PersistedPin::V2(Box::new(corrupt)), &small_onto);
        assert!(matches!(err, Err(CompatibilityError::CorruptRecord(_))));
    }

    #[test]
    fn producer_policy_accepts_current_and_recognized_only() {
        let onto = Ontology::from_packs(vec![fixture_pack()]).expect("valid");
        admit_producer_batch(&onto.fingerprint.0, &onto, &[]).expect("current accepted");
        admit_producer_batch(&onto.fingerprint.0, &onto, &[[3; 32]])
            .expect("current accepted regardless of history");
        admit_producer_batch(&[3; 32], &onto, &[[3; 32]]).expect("recognized accepted");
        let err = admit_producer_batch(&[4; 32], &onto, &[[3; 32]]);
        assert!(matches!(
            err,
            Err(CompatibilityError::ProducerNotAdmitted { .. })
        ));
    }

    #[test]
    fn peer_policy_is_exact_equality() {
        let onto = Ontology::from_packs(vec![fixture_pack()]).expect("valid");
        admit_peer(&onto.fingerprint.0, &onto.fingerprint.0).expect("equal admitted");
        let err = admit_peer(&[1; 32], &onto.fingerprint.0);
        assert!(matches!(err, Err(CompatibilityError::PeerMismatch { .. })));
    }

    #[test]
    fn backup_policy_accepts_subsets_and_legacy_equals() {
        let small_onto = Ontology::from_packs(vec![fixture_pack()]).expect("valid");
        let mut grown = fixture_pack();
        grown.memory_type_names.push("FutureThing".into());
        let big_onto = Ontology::from_packs(vec![grown]).expect("valid");

        admit_backup(
            BackupOntology::Summarized {
                summary: &small_onto.summary,
            },
            &big_onto,
        )
        .expect("subset backup restores into superset binary");
        let err = admit_backup(
            BackupOntology::Summarized {
                summary: &big_onto.summary,
            },
            &small_onto,
        );
        assert!(matches!(err, Err(CompatibilityError::NotASuperset { .. })));

        admit_backup(
            BackupOntology::Legacy {
                fingerprint_hex: &hex(&small_onto.build_fingerprint.0),
            },
            &small_onto,
        )
        .expect("legacy backup with matching build fingerprint");
        let err = admit_backup(
            BackupOntology::Legacy {
                fingerprint_hex: &hex(&[7; 32]),
            },
            &small_onto,
        );
        assert!(matches!(
            err,
            Err(CompatibilityError::LegacyPinMismatch { .. })
        ));
    }
}
