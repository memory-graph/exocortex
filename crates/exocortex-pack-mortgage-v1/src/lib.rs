//! The mortgage / consumer-lending pack (PX1, palantir-expansion PRD
//! §3.1; supersedes v2 deferral D2).
//!
//! Authored against `docs/ONTOLOGY_GUIDE.md` as the second pack and the
//! pack-seam proof. The domain test: adverse-action notices legally
//! require a stated reason, rulebooks vary by lender and change over
//! time, so "which rules governed this file" is a bi-temporal question
//! with a compliance consequence.
//!
//! 7 memory types × 5 entity types × 11 relationship kinds.
//!
//! Design, per the guide's methodology:
//!
//! **Retrieval questions.** *"Which rules governed this loan file, and
//! what did each one find?" · "What income sources support this
//! applicant, and which transactions were categorized into them?" ·
//! "Which lender configuration was in force when the decision was
//! made?" · "Has this applicant appeared under another file?"*
//!
//! **Types are category tags.** Each is decidable in one glance at a
//! loan file and stable for years: an `Applicant` is a person, a
//! `Transaction` is a bank row, an `IncomeSource` is a stream the
//! underwriter counts, a `RuleDefinition` is a versioned rule in
//! force, a `RuleFinding` is that rule's verdict on a file, a
//! `LenderConfiguration` is the configuration a rule ran under, and a
//! `LoanApplication` is the file under decision.
//!
//! **Edges serve the inferences.** The load-bearing chains are
//! `RuleFinding → RuleDefinition → (Governs) → LenderConfiguration`
//! (which rules, under whose config) and `Transaction → (SupportsIncome)
//! → IncomeSource → (ForApplicant) → Applicant` (what the income
//! decision rested on). Rule succession (`SupersedesRule`) makes
//! "which version was in force on a given date" traversable.
//!
//! **Computed-only.** `MergeDuplicateApplicant` is a Dreams-computed
//! proposal, never assertable — the same contract as dev-v1's
//! `SimilarTo` (R-T14).

use exocortex_kernel::pack;

pack! {
    name: "exocortex-pack-mortgage-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        Applicant, Transaction, IncomeSource, RuleDefinition,
        RuleFinding, LenderConfiguration, LoanApplication,
    }

    entity_types! {
        Lender, Product, LoanOfficer, Agency, Institution,
    }

    // R-T14: Dreams proposes; producers may never assert.
    computed_only_kinds! {
        MergeDuplicateApplicant,
    }

    kinds! {
        // Authority chains: which rule/config governed what.
        Governs            => bucket: Context,   inverse: GovernedBy,           bi: false, default_strength: 0.90,
        SupersedesRule     => bucket: Learning,  inverse: SupersededByRule,     bi: false, default_strength: 0.90,

        // Classification: transactions into income buckets.
        Categorizes        => bucket: Quality,   inverse: CategorizedBy,        bi: false, default_strength: 0.80,
        SupportsIncome     => bucket: Quality,   inverse: IncomeSupportedBy,    bi: false, default_strength: 0.75,

        // File structure.
        ForApplicant       => bucket: Context,   inverse: ApplicantOn,          bi: false, default_strength: 0.85,
        PartOfApplication  => bucket: Context,   inverse: HasApplicationPart,   bi: false, default_strength: 0.70,
        UnderRule          => bucket: Context,   inverse: FindingsUnder,        bi: false, default_strength: 0.85,
        ConcerningApplication => bucket: Context, inverse: FindingsConcerning,  bi: false, default_strength: 0.85,

        // Workflow and verification.
        PrecedesClosing    => bucket: Workflow,  inverse: FollowsClosing,       bi: false, default_strength: 0.70,
        VerifiedByOfficer  => bucket: Quality,   inverse: OfficerVerified,      bi: false, default_strength: 0.80,
        ReportedBy         => bucket: Integration, inverse: Reports,            bi: false, default_strength: 0.70,

        // Computed-only (R-T14): Dreams proposes applicant merges; no
        // producer may assert them.
        MergeDuplicateApplicant => bucket: Similarity, inverse: Self, bi: true, default_strength: 0.60,
    }

    type_triples! {
        Governs            => (RuleDefinition | LenderConfiguration, LenderConfiguration | LoanApplication | IncomeSource),
        SupersedesRule     => (RuleDefinition, RuleDefinition),
        Categorizes        => (RuleDefinition | LenderConfiguration, Transaction),
        SupportsIncome     => (Transaction, IncomeSource),
        ForApplicant       => (IncomeSource | LoanApplication, Applicant),
        PartOfApplication  => (Transaction | RuleFinding, LoanApplication),
        UnderRule          => (RuleFinding, RuleDefinition),
        ConcerningApplication => (RuleFinding, LoanApplication),
        PrecedesClosing    => (Transaction, Transaction),
        VerifiedByOfficer  => (IncomeSource | RuleFinding, LoanApplication),
        ReportedBy         => (IncomeSource, Applicant),
        MergeDuplicateApplicant => (Applicant, Applicant),
    }

    crepe_rules! {
        // M1: a finding's rule, resolved to the configuration that
        // governed the file — the adverse-action provenance chain.
        finding_under_config(finding, config) <- edge(finding, r, UnderRule), edge(r, config, Governs);
        // M2: the transactions a finding's rule categorized.
        finding_categorized(finding, txn) <- edge(finding, r, UnderRule), edge(r, txn, Categorizes);
        // M3: a finding made under a superseded rule version.
        finding_via_superseded(finding, old) <- edge(finding, r, UnderRule), edge(r, old, SupersedesRule);
    }
}
