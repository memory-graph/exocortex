// ontology.rs — the effective ontology assembled from registered packs.
use std::collections::HashMap;

use smol_str::SmolStr;

use crate::pack::TypeTriple;
use crate::{KernelError, KernelResult, PackDef, RelKindId, RelMeta};

/// The effective ontology for this process — kernel + registered packs.
/// Constructed once at startup, then read-only.
#[derive(Debug)]
pub struct Ontology {
    /// Every registered pack, in deterministic (name-sorted) order.
    pub packs: Vec<PackDef>,
    /// Kind id → metadata. Includes inverse companions (R-T4).
    pub kinds_by_id: HashMap<RelKindId, RelMeta>,
    /// Kind id → type-triple rules (R-T17).
    pub triples_by_kind: HashMap<RelKindId, Vec<TypeTriple>>,
    /// Memory type name → u8 id.
    pub memory_type_by_name: HashMap<SmolStr, u8>,
    /// Entity type name → u8 id.
    pub entity_type_by_name: HashMap<SmolStr, u8>,
    /// id → name; the storage layer uses this to mint Cypher labels (§6.5).
    pub memory_type_names: Vec<SmolStr>,
    /// id → name for entity types.
    pub entity_type_names: Vec<SmolStr>,
    /// SHA-256 over the effective ontology's meaning (§7.17, OC-PRD D1
    /// compatibility level). This is the only value that gates.
    pub fingerprint: crate::OntologyFingerprint,
    /// The full-build hash (OC-PRD D1 build level): v1 algorithm over
    /// the complete `PackDef` set, release metadata included. Reports;
    /// never gates.
    pub build_fingerprint: crate::compatibility::BuildFingerprint,
    /// The structured inputs of the compatibility fingerprint
    /// (OC-PRD D3) — what subset/superset verdicts are decided over.
    pub summary: crate::compatibility::OntologySummary,
}

impl Ontology {
    /// Assemble the effective ontology from every pack registered (via
    /// `inventory::submit!`) into the linked binary.
    pub(crate) fn from_registered_packs() -> KernelResult<Self> {
        let packs: Vec<PackDef> = inventory::iter::<crate::pack::PackRegistration>
            .into_iter()
            .map(|r| (r.build)())
            .collect();
        Self::from_packs(packs)
    }

    /// Assemble the effective ontology from an explicit pack list (the
    /// test/xtask entry point; startup uses `load_registered_packs`).
    pub fn from_packs(packs: Vec<PackDef>) -> KernelResult<Self> {
        // R-Pk1: name uniqueness.
        let mut seen = std::collections::HashSet::new();
        for p in &packs {
            if !seen.insert(&p.name) {
                return Err(KernelError::DuplicatePack(p.name.clone()));
            }
        }

        // Deterministic order and pack-id assignment (shared with the
        // fingerprint path so both always agree): packs are sorted by
        // name; pack i receives PackId(i). Pack-space kind ids are
        // canonicalized from their provisional `0x8000_0000 | local`
        // form to `RelKindId::from_pack(PackId(i), local)` (kernel-space
        // ids are untouched). KP2 (audit): triple sides carry pack-local
        // ids and are remapped with the same per-pack running offset the
        // id assignment uses.
        let packs = crate::compatibility::canonicalized(&packs);
        // KP2: duplicate memory-type names (and separately entity-type
        // names) across packs are a registration error, not a silent
        // last-writer-wins. The two namespaces are distinct id spaces —
        // a memory type may share a name with an entity type.
        fn check<'a>(names: impl Iterator<Item = &'a SmolStr>) -> KernelResult<()> {
            let mut seen = std::collections::HashSet::new();
            for n in names {
                if !seen.insert(n.clone()) {
                    return Err(KernelError::DuplicateTypeName(n.clone()));
                }
            }
            Ok(())
        }
        check(packs.iter().flat_map(|p| p.memory_type_names.iter()))?;
        check(packs.iter().flat_map(|p| p.entity_type_names.iter()))?;

        // R-Pk2 groundwork: duplicate kind detection, then kernel-constant
        // coverage.
        let mut kinds_by_id: HashMap<RelKindId, RelMeta> = HashMap::new();
        for p in &packs {
            for k in &p.kinds {
                if kinds_by_id.insert(k.id, k.clone()).is_some() {
                    return Err(KernelError::DuplicateKind(k.id));
                }
            }
        }
        for required in [
            crate::kinds::SOLVES,
            crate::kinds::FIXES,
            crate::kinds::CAUSES,
            crate::kinds::IN_SESSION,
        ] {
            if !kinds_by_id.contains_key(&required) {
                return Err(KernelError::UnboundKernelConstant(required));
            }
        }
        // R-T17: build triple index.
        let mut triples_by_kind: HashMap<RelKindId, Vec<TypeTriple>> = HashMap::new();
        for p in &packs {
            for t in &p.type_triples {
                triples_by_kind.entry(t.kind).or_default().push(t.clone());
            }
        }
        // Name indices. Type ids are assigned in pack order with a running
        // offset so multiple packs can never collide on u8 ids.
        let mut memory_type_by_name = HashMap::new();
        let mut entity_type_by_name = HashMap::new();
        let mut memory_type_names: Vec<SmolStr> = Vec::new();
        let mut entity_type_names: Vec<SmolStr> = Vec::new();
        for p in &packs {
            for name in &p.memory_type_names {
                memory_type_by_name.insert(name.clone(), memory_type_names.len() as u8);
                memory_type_names.push(name.clone());
            }
            for name in &p.entity_type_names {
                entity_type_by_name.insert(name.clone(), entity_type_names.len() as u8);
                entity_type_names.push(name.clone());
            }
        }
        let summary = crate::compatibility::OntologySummary::of_canonical_packs(&packs);
        let fingerprint = summary.compatibility_fingerprint();
        let build_fingerprint = crate::compatibility::build_fingerprint(&packs);
        Ok(Self {
            packs,
            kinds_by_id,
            triples_by_kind,
            memory_type_by_name,
            entity_type_by_name,
            memory_type_names,
            entity_type_names,
            fingerprint,
            build_fingerprint,
            summary,
        })
    }

    /// Look up a relationship kind's id by display name (the stable identity
    /// surface used by wire formats and rule sources).
    pub fn kind_id(&self, name: &str) -> Option<RelKindId> {
        self.kinds_by_id
            .values()
            .find(|k| k.display_name == name)
            .map(|k| k.id)
    }

    /// Look up a memory type's u8 id by name.
    pub fn memory_type_id(&self, name: &str) -> Option<u8> {
        self.memory_type_by_name.get(name).copied()
    }
}
