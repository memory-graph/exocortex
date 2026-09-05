//! The learning-tracking pack (LP1, master plan; owner request
//! 2026-09-04).
//!
//! Authored against `docs/ONTOLOGY_GUIDE.md` as the third pack. The
//! domain test: learning is a bi-temporal act — what was understood,
//! when, from which resource, and how that understanding held up under
//! review — and "what should I study next" is a traversal over
//! prerequisites, not a feeling.
//!
//! 7 memory types × 3 entity types × 11 relationship kinds.
//!
//! Design, per the guide's methodology:
//!
//! **Retrieval questions.** *"What am I learning and where did I leave
//! off?" · "What should I study next for X (prerequisites)?" · "What
//! covered topic X, and how well did it go?" · "Which topics am I
//! still shaky on?" · "Where did this insight come from?" · "What
//! questions did this material raise?"*
//!
//! **Types are category tags.** Each is decidable in one glance at a
//! study session and stable for years: a `LearningGoal` is a
//! commitment ("learn Rust async"), a `Topic` is a subject unit the
//! graph organizes around, a `Resource` is a book/course/article
//! studied, a `StudySession` is first-exposure work, a `ReviewSession`
//! is a deliberate revisit (the spaced-repetition event), an `Insight`
//! is a takeaway gained, and a `Question` is an open confusion. Study
//! vs review stays two types because the agent never agonizes: new
//! material is study, revisited material is review.
//!
//! **Edges serve the inferences.** The load-bearing chains are
//! `LearningGoal → (Pursues) → Topic → (PrerequisiteFor) → Topic`
//! (what to study next, transitively) and `Insight → (About) → Topic
//! ← (Covers) ← Resource` (where knowledge came from). `Answers` rides
//! the Solution bucket so kernel problem-solution bridging semantics
//! apply to a resolved confusion the way they do to a fixed bug;
//! `FoundDifficult`/`Reinforces` ride Learning (belief evolution):
//! mastery is a belief that evidence strengthens or weakens.
//!
//! **Computed-only.** `ClusteredWith` is a Dreams-computed proposal,
//! never assertable — the same contract as dev-v1's `SimilarTo`
//! (R-T14).

use exocortex_kernel::pack;

pack! {
    name: "exocortex-pack-study-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        LearningGoal, Topic, Resource, StudySession,
        ReviewSession, Insight, Question,
    }

    entity_types! {
        Subject, Author, Provider,
    }

    // R-T14: Dreams proposes; producers may never assert.
    computed_only_kinds! {
        ClusteredWith,
    }

    kinds! {
        // Goal structure: what is being learned and in what order.
        Pursues        => bucket: Context,   inverse: PursuedBy,      bi: false, default_strength: 0.80,
        PrerequisiteFor => bucket: Context,  inverse: HasPrerequisite, bi: false, default_strength: 0.85,
        Progresses     => bucket: Workflow,  inverse: ProgressedBy,   bi: false, default_strength: 0.70,

        // Coverage: what touched which topic.
        Covers         => bucket: Context,   inverse: CoveredBy,      bi: false, default_strength: 0.80,
        About          => bucket: Context,   inverse: HasNote,        bi: false, default_strength: 0.75,
        StudiedFrom    => bucket: Context,   inverse: SourceOf,       bi: false, default_strength: 0.80,

        // Confusion and resolution (the problem/solution spine).
        Answers        => bucket: Solution,  inverse: AnsweredBy,     bi: false, default_strength: 0.85,
        Raises         => bucket: Causal,    inverse: RaisedBy,       bi: false, default_strength: 0.75,

        // Mastery as belief evolution: evidence for and against.
        FoundDifficult => bucket: Learning,  inverse: WasDifficultIn, bi: false, default_strength: 0.70,
        Reinforces     => bucket: Learning,  inverse: ReinforcedBy,   bi: false, default_strength: 0.75,

        // Computed-only (R-T14): Dreams clusters related knowledge.
        ClusteredWith  => bucket: Similarity, inverse: Self,          bi: true,  default_strength: 0.60,
    }

    type_triples! {
        Pursues        => (LearningGoal, Topic),
        PrerequisiteFor => (Topic, Topic),
        Progresses     => (StudySession | ReviewSession | Insight, LearningGoal),
        Covers         => (Resource | StudySession | ReviewSession, Topic),
        About          => (Insight | Question, Topic),
        StudiedFrom    => (Insight | StudySession, Resource),
        Answers        => (Insight, Question),
        Raises         => (StudySession | Resource, Question),
        FoundDifficult => (StudySession | ReviewSession, Topic),
        Reinforces     => (ReviewSession, Topic),
        ClusteredWith  => (_, _),
    }

    crepe_rules! {
        // L1: prerequisites chain transitively — "eventually needed
        // for" is a traversal, not a stored fact (the dev-v1 D2 idiom).
        prerequisite_chain(a, c) <- edge(a, b, PrerequisiteFor), edge(b, c, PrerequisiteFor);
        // L2: a resource covering a question's topic is a candidate
        // answer source — "where might the answer live".
        answer_source(r, q) <- edge(q, t, About), edge(r, t, Covers);
        // L3: an insight about a pursued topic progresses the goal
        // without a hand-wired Progresses edge.
        indirect_progress(i, g) <- edge(i, t, About), edge(g, t, Pursues);
    }
}
