//! The v1 developer-domain pack (§7.1, §7.2, §7.3, §7.18).
//!
//! 13 memory types × 12 entity types × 48 relationship kinds across 8 buckets.
//! This crate is the sole pack v1 links; adding a second pack in v2 is purely
//! additive.

use exocortex_kernel::pack;

/// Link anchor consumed by production entrypoints through the pack-agnostic
/// kernel contract. Removing this crate from the link leaves the symbol
/// unresolved, so a packless binary cannot be produced accidentally.
#[no_mangle]
pub extern "C" fn exocortex_required_ontology_pack_anchor() {}

pack! {
    name: "exocortex-pack-dev-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        Task, CodePattern, Problem, Solution, Project, Technology,
        Error, Fix, Command, FileContext, Workflow, General, Conversation,
    }

    entity_types! {
        File, Function, Class, Error, Technology, Concept,
        Person, Project, Command, Package, Url, Variable,
    }

    // W6 (audit): R-T14's computed-only kinds — Dreams is the only
    // legitimate producer; the ingest boundary reads THIS marker, not a
    // string literal.
    computed_only_kinds! {
        SimilarTo,
    }

    kinds! {
        // Solution bucket (5) — kernel-const SOLVES is bound to `Solves`.
        Solves        => bucket: Solution,   inverse: SolvedBy,     bi: false, default_strength: 0.85, kernel_const: SOLVES,
        Addresses     => bucket: Solution,   inverse: AddressedBy,  bi: false, default_strength: 0.70,
        AlternativeTo => bucket: Solution,   inverse: Self,         bi: true,  default_strength: 0.60,
        Improves      => bucket: Solution,   inverse: ImprovedBy,   bi: false, default_strength: 0.70,
        Replaces      => bucket: Solution,   inverse: ReplacedBy,   bi: false, default_strength: 0.90,

        // Causal bucket (7) — kernel-const FIXES/CAUSES bound below.
        Causes        => bucket: Causal,     inverse: CausedBy,     bi: false, default_strength: 0.85, kernel_const: CAUSES,
        Prevents      => bucket: Causal,     inverse: PreventedBy,  bi: false, default_strength: 0.80,
        Triggers      => bucket: Causal,     inverse: TriggeredBy,  bi: false, default_strength: 0.75,
        LeadsTo       => bucket: Causal,     inverse: FollowsFrom,  bi: false, default_strength: 0.70,
        Enables       => bucket: Causal,     inverse: EnabledBy,    bi: false, default_strength: 0.65,
        Blocks        => bucket: Causal,     inverse: BlockedBy,    bi: false, default_strength: 0.75,
        Fixes         => bucket: Causal,     inverse: FixedBy,      bi: false, default_strength: 0.90, kernel_const: FIXES,

        // Context bucket (9)
        Uses          => bucket: Context,    inverse: UsedBy,       bi: false, default_strength: 0.70,
        Requires      => bucket: Context,    inverse: RequiredBy,   bi: false, default_strength: 0.85,
        DependsOn     => bucket: Context,    inverse: DependedBy,   bi: false, default_strength: 0.75,
        Contains      => bucket: Context,    inverse: ContainedBy,  bi: false, default_strength: 0.70,
        PartOf        => bucket: Context,    inverse: HasPart,      bi: false, default_strength: 0.70,
        InSession     => bucket: Context,    inverse: HasMember,    bi: false, default_strength: 0.80, kernel_const: IN_SESSION,
        InProject     => bucket: Context,    inverse: ProjectHas,   bi: false, default_strength: 0.80,
        WrittenIn     => bucket: Context,    inverse: Powers,       bi: false, default_strength: 0.65,
        Modifies      => bucket: Context,    inverse: ModifiedBy,   bi: false, default_strength: 0.65,

        // Learning bucket (6)
        Teaches       => bucket: Learning,   inverse: LearnedFrom,  bi: false, default_strength: 0.70,
        Demonstrates  => bucket: Learning,   inverse: Self,         bi: true,  default_strength: 0.65,
        Contradicts   => bucket: Learning,   inverse: Self,         bi: true,  default_strength: 0.80,
        Confirms      => bucket: Learning,   inverse: ConfirmedBy,  bi: false, default_strength: 0.75,
        BuildsOn      => bucket: Learning,   inverse: BuiltOnBy,    bi: false, default_strength: 0.75,
        Specializes   => bucket: Learning,   inverse: Generalizes,  bi: false, default_strength: 0.70,

        // Similarity bucket (4)
        SimilarTo     => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.60,
        DifferentFrom => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.55,
        AnalogousTo   => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.55,
        RelatedTo     => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.30,

        // Workflow bucket (6)
        Precedes      => bucket: Workflow,   inverse: Follows,      bi: false, default_strength: 0.70,
        ParallelTo    => bucket: Workflow,   inverse: Self,         bi: true,  default_strength: 0.50,
        Executes      => bucket: Workflow,   inverse: ExecutedBy,   bi: false, default_strength: 0.75,
        Creates       => bucket: Workflow,   inverse: CreatedBy,    bi: false, default_strength: 0.75,
        Configures    => bucket: Workflow,   inverse: ConfiguredBy, bi: false, default_strength: 0.65,
        Automates     => bucket: Workflow,   inverse: AutomatedBy,  bi: false, default_strength: 0.75,

        // Quality bucket (5)
        Validates     => bucket: Quality,    inverse: ValidatedBy,  bi: false, default_strength: 0.75,
        Tests         => bucket: Quality,    inverse: TestedBy,     bi: false, default_strength: 0.75,
        Measures      => bucket: Quality,    inverse: MeasuredBy,   bi: false, default_strength: 0.65,
        Documents     => bucket: Quality,    inverse: DocumentedBy, bi: false, default_strength: 0.65,
        Verifies      => bucket: Quality,    inverse: VerifiedBy,   bi: false, default_strength: 0.75,

        // Integration bucket (6)
        IntegratesWith=> bucket: Integration,inverse: Self,         bi: true,  default_strength: 0.70,
        Consumes      => bucket: Integration,inverse: ConsumedBy,   bi: false, default_strength: 0.70,
        Produces      => bucket: Integration,inverse: ProducedBy,   bi: false, default_strength: 0.70,
        Exposes       => bucket: Integration,inverse: ExposedBy,    bi: false, default_strength: 0.65,
        Wraps         => bucket: Integration,inverse: WrappedBy,    bi: false, default_strength: 0.70,
        Bridges       => bucket: Integration,inverse: BridgedBy,    bi: false, default_strength: 0.70,
    }

    type_triples! {
        // Solution
        Solves        => (Solution | Fix, Problem | Error),
        Addresses     => (Solution | Fix, Problem | Error),
        AlternativeTo => (Solution | Fix, Solution | Fix),
        Improves      => (Solution | Fix | CodePattern, Solution | Fix | CodePattern | Task),
        Replaces      => (_, _),
        // Causal
        Causes        => (_, Error | Problem),
        Prevents      => (Solution | Fix | CodePattern, Error | Problem),
        Fixes         => (Fix, Error | Problem),
        Triggers      => (_, _), LeadsTo => (_, _), Enables => (_, _), Blocks => (_, _),
        // Context
        // NOTE (PRD §7.18 conflict): `Package` is an EntityType, not a
        // MemoryType; it is dropped from both `Uses` and `Requires` to-sides.
        // Recorded in the M1 report.
        Uses          => (_, Technology | Command),
        Requires      => (_, Technology),
        DependsOn     => (_, _),
        Contains      => (_, _),
        PartOf        => (_, _),
        InSession     => (_, Conversation),
        InProject     => (_, Project),
        WrittenIn     => (CodePattern | FileContext, Technology),
        Modifies      => (Task | Command | Fix, FileContext),
        // Learning
        Teaches       => (_, _), Demonstrates => (_, _), Contradicts => (_, _),
        Confirms      => (_, _), BuildsOn => (_, _), Specializes => (_, _),
        // Similarity
        SimilarTo     => (_, _), DifferentFrom => (_, _), AnalogousTo => (_, _), RelatedTo => (_, _),
        // Workflow
        Precedes      => (_, _), ParallelTo => (_, _), Executes => (Command, _),
        Creates       => (Task | Command | Fix, FileContext),
        Configures    => (_, _), Automates => (Workflow | Command, _),
        // Quality
        Validates     => (_, _), Tests => (_, _), Measures => (_, _),
        Documents     => (_, _), Verifies => (_, _),
        // Integration
        IntegratesWith=> (_, _), Consumes => (_, _), Produces => (_, _),
        Exposes       => (_, _), Wraps => (_, _), Bridges => (_, _),
    }

    // Rules R1-R9 live in the kernel (§10.2). Pack-local rules are D1-D6 and
    // only fire on pack-owned kinds. They MUST NOT bind kernel-const kinds
    // directly by numeric id; they reference them by name so the kernel can
    // inject the interned RelKindId at compile time (see kernel::rules::pack_scope!).
    crepe_rules! {
        // D1: `Fix Fixes Problem` implies `Fix Solves Problem` (subsumption).
        implied_solves(a, b) <- edge(a, b, Fixes), memory(a, MemoryType::Fix, _);
        // D2: `A BuildsOn B` and `B BuildsOn C` implies `A BuildsOn C` (transitivity, k=3 bounded).
        transitive_builds_on(a, c) <- edge(a, b, BuildsOn), edge(b, c, BuildsOn);
        // D3: `A Blocks B` and `B Requires C` implies `A Blocks C` (indirect blocker).
        indirect_blocker(a, c) <- edge(a, b, Blocks), edge(b, c, Requires);
        // D4: contradiction cluster - if A Contradicts B and B Confirms C then A Contradicts C.
        contradiction_propagates(a, c) <- edge(a, b, Contradicts), edge(b, c, Confirms);
        // D5: file-lineage propagation - if a Task Modifies F, and Fix Modifies F later, they share a target.
        shared_target(a, b, f) <- edge(a, f, Modifies), edge(b, f, Modifies), memory(a, _, _), memory(b, _, _), a != b;
        // D6: session cohesion - all memories `InSession S` are candidates for MCR2 grouping.
        session_cohort(m, s) <- edge(m, s, InSession);
    }
}
