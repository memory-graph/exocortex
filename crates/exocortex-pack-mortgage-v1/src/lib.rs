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

use exocortex_kernel::verbs::ActionProduct;
use exocortex_kernel::{pack, KernelError, MemoryId, Visibility};

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

    // PX2: the pack's own typed verbs. The body is a typed transform
    // (type-checked here); the framework provenance-stamps, audits,
    // ceiling-enforces, and commits — the body cannot bypass any of it.
    actions! {
        AttachRuleFinding(
            input: {
                loan: String,
                rule: String,
                finding_title: String,
                finding_content: String,
            },
            min_visibility: Project
        ) = |ctx, input| {
            let loan = MemoryId::parse_hex(&input.loan)
                .ok_or(KernelError::InvalidActionInput("loan must be a 32-hex memory id".into()))?;
            let rule = MemoryId::parse_hex(&input.rule)
                .ok_or(KernelError::InvalidActionInput("rule must be a 32-hex memory id".into()))?;
            let mut product = ActionProduct::new();
            product.memory(
                "finding",
                MemoryType::RuleFinding.id(),
                &input.finding_title,
                &input.finding_content,
                ctx.narrow(Visibility::Project),
                &["rule-finding"],
            );
            product.edge_to_memory("finding", loan, "ConcerningApplication");
            product.edge_to_memory("finding", rule, "UnderRule");
            Ok(product)
        },
    }

    // v1 pack Functions are pure typed computations (scheme). The
    // categorical-eligibility decision from a verified-income row.
    functions! {
        IsCategoricallyEligible(
            input: { income_verified: bool, categorical_kind: String }
        ) -> bool, p50_us: 2000, p99_us: 5000 = scheme {
            (if (input "income_verified")
                (equal? (input "categorical_kind") "categorical")
                #f)
        },
    }

    // PX2 §4.2: structured agent guidance, keyed by this pack's own
    // types/kinds; names resolve at pack-def build time.
    guidance! {
        RuleDefinition {
            when: "authoring or changing a lender rule",
            link: [Governs => LenderConfiguration, SupersedesRule => RuleDefinition],
        },
        RuleFinding {
            when: "recording a rule's verdict on a loan file",
            link: [UnderRule => RuleDefinition, ConcerningApplication => LoanApplication],
        },
        Transaction {
            when: "categorizing a bank transaction into an income source",
            link: [SupportsIncome => IncomeSource],
        },
        MergeDuplicateApplicant {
            caution: "never assert directly - Dreams proposes it (R-T14)",
        },
    }
}
