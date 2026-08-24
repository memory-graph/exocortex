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
    /// SHA-256 over the effective ontology (§7.17).
    pub fingerprint: crate::OntologyFingerprint,
}

impl Ontology {
    /// Assemble the effective ontology from every pack registered (via
    /// `inventory::submit!`) into the linked binary.
    pub(crate) fn from_registered_packs() -> KernelResult<Self> {
        let packs: Vec<PackDef> = inventory::iter::<PackDef>.into_iter().cloned().collect();
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
        // R-Pk2: kernel-constant coverage.
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
        let fingerprint = crate::OntologyFingerprint::compute(&packs);
        Ok(Self {
            packs,
            kinds_by_id,
            triples_by_kind,
            memory_type_by_name,
            entity_type_by_name,
            memory_type_names,
            entity_type_names,
            fingerprint,
        })
    }
}
