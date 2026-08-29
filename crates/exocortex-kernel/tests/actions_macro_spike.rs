//! PX2-S1 (palantir-expansion PRD §4.1): the `actions!`/`functions!`
//! `macro_rules!` spike — a written decision with an executable proof.
//!
//! **The question.** `crates/exocortex-kernel/src/macros.rs:93` records
//! that *"`macro_rules!` cannot tokenize past `;` inside a body, so the
//! whole block is captured as text"* — which is why `CREPE_RULES_SRC`
//! is a `stringify!`. The palantir PRD's `actions!` sketch has
//! semicolon-separated statements inside a body: the same shape.
//!
//! **The decision (outcome (a)).** The recorded constraint is about
//! capturing NON-Rust DSL text for a downstream text compiler (Crepe),
//! where token structure is worthless and only the source string
//! survives. Rust bodies need the opposite treatment, and `macro_rules!`
//! provides it: `$($body:tt)*` matches semicolons, nested braces, and
//! every other token, and re-emitting the capture inside a generated
//! `fn` type-checks it against pack-typed names at pack-compile time.
//! So `actions!` sections are expressible in `macro_rules!` with NO
//! proc-macro and NO new dependencies, and P3's "Actions are typed
//! transforms" plus the visibility-ceiling guarantee remain
//! compile-time properties. `functions!` bodies (`datalog!`/`scheme!`)
//! are text-shaped exactly like `crepe_rules!` and capture the
//! already-proven way.
//!
//! **The proof.** `spike_actions!` below mirrors the muncher shape the
//! `pack!` macro would grow: verb-by-verb, signature group captured as
//! tts (a `:ty` fragment may not be followed by `->`, so the input type
//! is captured as a group and re-emitted), output type as `:ty` (its
//! follow set permits `{`), body as `$($body:tt)*`. The fixture body
//! carries semicolons, nested braces with interior semicolons, and
//! references the pack-emitted enum — it compiles and runs.

/// The muncher a real `actions!` section would use. `[$($acc:tt)*]` is
/// the accumulator of already-generated items.
macro_rules! spike_actions {
    (@verbs [$($acc:tt)*]) => {
        $($acc)*
    };
    (@verbs [$($acc:tt)*]
        $name:ident ($($sig:tt)*) -> $output:ty $body:block
        $($rest:tt)*
    ) => {
        spike_actions!(@verbs [
            $($acc)*
            #[doc = concat!("Pack action `", stringify!($name), "`, body type-checked in the pack crate.")]
            pub fn $name($($sig)*) -> $output $body
        ] $($rest)*);
    };
    ($($verbs:tt)*) => {
        spike_actions!(@verbs [] $($verbs)*);
    };
}

/// The `functions!` shape: names plus text-shaped bodies, exactly the
/// proven `crepe_rules!` capture.
macro_rules! spike_functions {
    (@fns [$($acc:tt)*]) => {
        /// Every function source in declaration order, paired with its
        /// name and engine tag. (`macro_rules!` cannot splice
        /// identifiers, so the registry pairs names with sources
        /// instead of generating per-verb constants.)
        pub static FUNCTION_SOURCES: &[(&str, &str, &str)] = &[$($acc)*];
    };
    (@fns [$($acc:tt)*]
        $name:ident ($($sig:tt)*) -> $output:ty = $engine:ident { $($body:tt)* }
        $($rest:tt)*
    ) => {
        spike_functions!(@fns [
            $($acc)*
            (stringify!($name), stringify!($engine), stringify!($($body)*)),
        ] $($rest)*);
    };
    ($($fns:tt)*) => {
        spike_functions!(@fns [] $($fns)*);
    };
}

/// The pack-shaped fixture: the enums a `pack!` invocation emits, the
/// action bodies that reference them, and the assertions.
mod fixture {
    #![allow(dead_code, non_snake_case, unused_variables)]

    /// Mirrors the `#[repr(u8)]` enum `memory_types!` emits.
    #[repr(u8)]
    pub enum MemoryType {
        /// A lender rule in force.
        RuleDefinition,
        /// A categorical finding against a rule.
        RuleFinding,
    }

    impl MemoryType {
        /// Declaration index == ontology id.
        pub const fn id(self) -> u8 {
            self as u8
        }
    }

    /// Mirrors the draft shape an action receives.
    pub struct FindingDraft {
        /// Target ontology type, set by the action body.
        pub memory_type: u8,
        /// Author-requested ceiling; the action narrows it.
        pub requested_ceiling: u8,
    }

    /// Mirrors the typed ceiling handle the generated body uses.
    pub struct ActionContext {
        /// The maximum visibility the pack's action may stamp.
        pub max_ceiling: u8,
    }

    impl ActionContext {
        /// The compile-time ceiling check the pack! machinery would
        /// inject around every action body.
        pub fn assert_within(&self, requested: u8) -> u8 {
            requested.min(self.max_ceiling)
        }
    }

    spike_actions! {
        AttachRuleFinding(ctx: &ActionContext, input: FindingDraft) -> (u8, u8) {
            let mut draft = input;
            draft.memory_type = MemoryType::RuleFinding.id();
            let ceiling = ctx.assert_within(draft.requested_ceiling);
            (draft.memory_type, ceiling)
        }
        PromoteLoanToUnderwriting(ctx: &ActionContext, input: FindingDraft) -> u8 {
            let mut draft = input;
            {
                // Nested braces with interior semicolons — the exact
                // shape macros.rs:93 recorded as untokenizable.
                draft.memory_type = MemoryType::RuleDefinition.id();
                draft.requested_ceiling = 2;
            }
            draft.memory_type
        }
    }

    spike_functions! {
        ComputeIncomeEligibility(input: u8) -> u8 = datalog {
            eligible(a) <- applicant(a), income(a, verified);
        }
        RenderBasisExplanation(input: u8) -> String = scheme {
            (explain-tree "basis")
        }
    }
}

#[test]
fn actions_bodies_expand_and_type_check() {
    let ctx = fixture::ActionContext { max_ceiling: 3 };
    let draft = fixture::FindingDraft {
        memory_type: 0,
        requested_ceiling: 9,
    };
    let (memory_type, ceiling) = fixture::AttachRuleFinding(&ctx, draft);
    assert_eq!(memory_type, fixture::MemoryType::RuleFinding.id());
    assert_eq!(ceiling, 3, "the injected ceiling check narrows");

    let promoted = fixture::PromoteLoanToUnderwriting(
        &ctx,
        fixture::FindingDraft {
            memory_type: 0,
            requested_ceiling: 0,
        },
    );
    assert_eq!(promoted, fixture::MemoryType::RuleDefinition.id());
}

#[test]
fn function_bodies_capture_as_text_like_crepe_rules() {
    // The proven `stringify!` shape: the reasoning-side engine compiles
    // the source downstream, exactly as `crepe_rules!` does today.
    let sources = fixture::FUNCTION_SOURCES;
    assert_eq!(sources.len(), 2);
    let (name, engine, datalog) = sources[0];
    assert_eq!(name, "ComputeIncomeEligibility");
    assert_eq!(engine, "datalog");
    assert!(
        datalog.contains("eligible(a) <- applicant(a), income(a, verified);"),
        "{datalog}"
    );
    let (_, engine, scheme) = sources[1];
    assert_eq!(engine, "scheme");
    assert!(scheme.contains("explain-tree"), "{scheme}");
}
