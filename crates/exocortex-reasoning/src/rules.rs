// crates/exocortex-reasoning/src/rules.rs
// The crepe! macro generates the relation structs without doc comments;
// documenting the macro's output is not possible, so this module relaxes
// missing_docs for the generated items only.
#![allow(missing_docs)]
//! The Crepe rule program: kernel rules R1-R5, R7-R9 (§10.2/§10.4) plus the
//! dev-v1 pack rules D1-D6 (§7.18) joined into a single compile-time Datalog
//! program (R-Pk3). R6 `reverse_solves` is Steel, not Crepe (§10.4).
//!
//! Kernel-constant ids are compile-time constants; dev-v1 kind ids are
//! pack-space and resolved once through the effective ontology (`prime`) —
//! the only pack↔reasoning coupling (§10.7 step 1).

use exocortex_kernel::{EntityId, MemoryId, RelKindId};

// Kernel-constant ids (kernel space, stable).
const SOLVES: RelKindId = exocortex_kernel::kinds::SOLVES;
const FIXES: RelKindId = exocortex_kernel::kinds::FIXES;
const CAUSES: RelKindId = exocortex_kernel::kinds::CAUSES;
const IN_SESSION: RelKindId = exocortex_kernel::kinds::IN_SESSION;

// Memory-type ids from the linked pack (declaration order == ontology ids).
const SOLUTION_TYPE: u8 = exocortex_pack_dev_v1::MemoryType::Solution.id();
const FIX_TYPE: u8 = exocortex_pack_dev_v1::MemoryType::Fix.id();
const PROBLEM_TYPE: u8 = exocortex_pack_dev_v1::MemoryType::Problem.id();

/// Pack-space kind ids the pack rules filter on, resolved once via `prime`.
pub struct PackKinds {
    /// `DependsOn` (Context bucket).
    pub depends_on: RelKindId,
    /// `Requires` (Context bucket).
    pub requires: RelKindId,
    /// `BuildsOn` (Learning bucket).
    pub builds_on: RelKindId,
    /// `Blocks` (Causal bucket).
    pub blocks: RelKindId,
    /// `Contradicts` (Learning bucket).
    pub contradicts: RelKindId,
    /// `Confirms` (Learning bucket).
    pub confirms: RelKindId,
}

use std::sync::OnceLock;

static PACK_KINDS: OnceLock<PackKinds> = OnceLock::new();

fn pk() -> &'static PackKinds {
    PACK_KINDS
        .get()
        .expect("call Rules::prime(onto) before evaluation")
}

/// Resolve the pack-space kind ids. Call once at engine construction with
/// the effective ontology (idempotent).
pub fn prime(onto: &exocortex_kernel::Ontology) {
    let kind = |name: &str| onto.kind_id(name).expect("pack kind registered");
    let _ = PACK_KINDS.set(PackKinds {
        depends_on: kind("DependsOn"),
        requires: kind("Requires"),
        builds_on: kind("BuildsOn"),
        blocks: kind("Blocks"),
        contradicts: kind("Contradicts"),
        confirms: kind("Confirms"),
    });
}

crepe::crepe! {
    // ---- Inputs: facts harvested from the k-hop bounded neighborhood ----
    @input
    #[derive(Debug)]
    pub struct Edge(pub MemoryId, pub MemoryId, pub RelKindId);

    @input
    #[derive(Debug)]
    pub struct EntityFact(pub MemoryId, pub EntityId);

    @input
    #[derive(Debug)]
    pub struct TagFact(pub MemoryId, pub u32);

    // ---- Outputs: derived facts ----
    @output
    #[derive(Debug)]
    pub struct TypeFromSolves(MemoryId, u8);
    @output
    #[derive(Debug)]
    pub struct TypeFromFixes(MemoryId, u8);
    @output
    #[derive(Debug)]
    pub struct TypeFromCauses(MemoryId, u8);
    @output
    #[derive(Debug)]
    pub struct TransitiveDependsOn(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct TransitiveRequires(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct CoOccurrenceAffinity(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct ProblemSolutionBridge(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct SimilarTagsAffinity(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct ImpliedSolves(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct TransitiveBuildsOn(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct IndirectBlocker(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct ContradictionPropagates(MemoryId, MemoryId);
    @output
    #[derive(Debug)]
    pub struct SessionCohort(MemoryId, MemoryId);

    // R1: a memory that Solves another is a Solution.
    TypeFromSolves(a, SOLUTION_TYPE) <-
        Edge(a, _, k), (k == SOLVES);

    // R2: a memory that Fixes another is a Fix.
    TypeFromFixes(a, FIX_TYPE) <-
        Edge(a, _, k), (k == FIXES);

    // R3: a memory that is Caused by another is a Problem.
    TypeFromCauses(b, PROBLEM_TYPE) <-
        Edge(_, b, k), (k == CAUSES);

    // R4: DependsOn transitivity (k=3 bounded by fact scope).
    TransitiveDependsOn(a, c) <-
        Edge(a, b, k1), Edge(b, c, k2),
        (k1 == pk_ref().depends_on), (k2 == pk_ref().depends_on);

    // R5: Requires transitivity.
    TransitiveRequires(a, c) <-
        Edge(a, b, k1), Edge(b, c, k2),
        (k1 == pk_ref().requires), (k2 == pk_ref().requires);

    // R7: shared entities imply co-occurrence affinity.
    CoOccurrenceAffinity(a, b) <-
        EntityFact(a, e), EntityFact(b, e), (a != b);

    // R8: solutions of the same problem are bridged.
    ProblemSolutionBridge(x, y) <-
        Edge(x, p, kx), Edge(y, p, ky), (kx == SOLVES), (ky == SOLVES), (x != y);

    // R9: shared tags imply affinity.
    SimilarTagsAffinity(a, b) <-
        TagFact(a, t), TagFact(b, t), (a != b);

    // D1: `Fix Fixes Problem` implies `Fix Solves Problem`.
    ImpliedSolves(a, b) <- Edge(a, b, k), (k == FIXES);

    // D2: BuildsOn transitivity.
    TransitiveBuildsOn(a, c) <-
        Edge(a, b, k1), Edge(b, c, k2),
        (k1 == pk_ref().builds_on), (k2 == pk_ref().builds_on);

    // D3: indirect blocker.
    IndirectBlocker(a, c) <-
        Edge(a, b, k1), Edge(b, c, k2),
        (k1 == pk_ref().blocks), (k2 == pk_ref().requires);

    // D4: contradiction propagation.
    ContradictionPropagates(a, c) <-
        Edge(a, b, k1), Edge(b, c, k2),
        (k1 == pk_ref().contradicts), (k2 == pk_ref().confirms);

    // D6: session cohort.
    SessionCohort(m, s) <- Edge(m, s, k), (k == IN_SESSION);
}

fn pk_ref() -> &'static PackKinds {
    pk()
}

/// All derived facts from one fixpoint evaluation over the input facts.
#[derive(Debug, Default)]
pub struct Derived {
    /// R1 type facts.
    pub type_from_solves: Vec<(MemoryId, u8)>,
    /// R2 type facts.
    pub type_from_fixes: Vec<(MemoryId, u8)>,
    /// R3 type facts.
    pub type_from_causes: Vec<(MemoryId, u8)>,
    /// R4 pairs.
    pub transitive_depends_on: Vec<(MemoryId, MemoryId)>,
    /// R5 pairs.
    pub transitive_requires: Vec<(MemoryId, MemoryId)>,
    /// R7 pairs.
    pub co_occurrence_affinity: Vec<(MemoryId, MemoryId)>,
    /// R8 pairs.
    pub problem_solution_bridge: Vec<(MemoryId, MemoryId)>,
    /// R9 pairs.
    pub similar_tags_affinity: Vec<(MemoryId, MemoryId)>,
    /// D1 pairs.
    pub implied_solves: Vec<(MemoryId, MemoryId)>,
    /// D2 pairs.
    pub transitive_builds_on: Vec<(MemoryId, MemoryId)>,
    /// D3 pairs.
    pub indirect_blocker: Vec<(MemoryId, MemoryId)>,
    /// D4 pairs.
    pub contradiction_propagates: Vec<(MemoryId, MemoryId)>,
    /// D6 pairs.
    pub session_cohort: Vec<(MemoryId, MemoryId)>,
}

impl Derived {
    /// Total number of derived facts.
    pub fn total(&self) -> usize {
        self.type_from_solves.len()
            + self.type_from_fixes.len()
            + self.type_from_causes.len()
            + self.transitive_depends_on.len()
            + self.transitive_requires.len()
            + self.co_occurrence_affinity.len()
            + self.problem_solution_bridge.len()
            + self.similar_tags_affinity.len()
            + self.implied_solves.len()
            + self.transitive_builds_on.len()
            + self.indirect_blocker.len()
            + self.contradiction_propagates.len()
            + self.session_cohort.len()
    }
}

/// Run the fixpoint over the given facts. `prime` must have been called.
pub fn evaluate(edges: Vec<Edge>, entities: Vec<EntityFact>, tags: Vec<TagFact>) -> Derived {
    let mut rt = Crepe::new();
    rt.extend(edges);
    rt.extend(entities);
    rt.extend(tags);
    let (
        type_from_solves,
        type_from_fixes,
        type_from_causes,
        transitive_depends_on,
        transitive_requires,
        co_occurrence_affinity,
        problem_solution_bridge,
        similar_tags_affinity,
        implied_solves,
        transitive_builds_on,
        indirect_blocker,
        contradiction_propagates,
        session_cohort,
    ) = rt.run();
    macro_rules! pairs {
        ($s:expr) => {
            $s.into_iter().map(|x| (x.0, x.1)).collect()
        };
    }
    Derived {
        type_from_solves: pairs!(type_from_solves),
        type_from_fixes: pairs!(type_from_fixes),
        type_from_causes: pairs!(type_from_causes),
        transitive_depends_on: pairs!(transitive_depends_on),
        transitive_requires: pairs!(transitive_requires),
        co_occurrence_affinity: pairs!(co_occurrence_affinity),
        problem_solution_bridge: pairs!(problem_solution_bridge),
        similar_tags_affinity: pairs!(similar_tags_affinity),
        implied_solves: pairs!(implied_solves),
        transitive_builds_on: pairs!(transitive_builds_on),
        indirect_blocker: pairs!(indirect_blocker),
        contradiction_propagates: pairs!(contradiction_propagates),
        session_cohort: pairs!(session_cohort),
    }
}
