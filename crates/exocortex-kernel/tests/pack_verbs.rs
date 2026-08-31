//! PX2: the `actions!` / `functions!` / `guidance!` sections of `pack!`
//! (palantir-expansion PRD §3.2/§4.1/§4.2). One fixture pack declares all
//! three sections; a sibling test in `pack_registration.rs` covers the
//! zero-section case (existing packs compile unchanged).
//!
//! What is pinned here:
//! - registrations enumerate with typed bodies that run through the
//!   generated adapter (`run`), the declared ceiling, and budgets;
//! - verb SIGNATURES join the compatibility fingerprint (OC-PRD D1:
//!   meaning-bearing structure) while BODIES never enter `PackDef`, so
//!   a body patch moves neither fingerprint level;
//! - adding a verb is a superset event (OC-PRD D3 discipline);
//! - guidance entries ride `PackDef` with their text caps enforced.

use exocortex_kernel::verbs::ActionContext;
use exocortex_kernel::{pack, ActionProduct, KernelError, MemoryId, Visibility};

pack! {
    name: "test-pack-verbs",
    version: "0.1.0",
    kernel_min: "1.0.0",

    memory_types! { Zebra, Zed }

    entity_types! { Zzz }

    kinds! {
        Zaps => bucket: Context, inverse: ZappedBy, bi: false, default_strength: 0.70,
    }

    type_triples! {
        Zaps => (Zebra, Zed),
    }

    crepe_rules! {
        zap_chain(a, b) <- edge(a, b, Zaps);
    }

    actions! {
        RecordZap(input: { zed: String, note: String }, min_visibility: Team) = |ctx, input| {
            let mut product = ActionProduct::new();
            let target = MemoryId::parse_hex(&input.zed)
                .ok_or(KernelError::InvalidActionInput("zed must be 32-char hex".into()))?;
            product.memory(
                "zap",
                MemoryType::Zed.id(),
                &input.note,
                &input.note,
                ctx.narrow(Visibility::Project),
                &["zebra"],
            );
            product.edge_to_memory("zap", target, "Zaps");
            Ok(product)
        },
    }

    functions! {
        ZapEligible(input: { flag: bool }) -> bool, p50_us: 50, p99_us: 500 = scheme {
            (if (input "flag") #t #f)
        },
    }

    guidance! {
        Zebra {
            when: "recording a zap",
            link: [Zaps => Zed],
        },
        ZappedBy {
            caution: "companions are materialized, never asserted",
        },
    }
}

#[test]
fn pack_actions_register_with_typed_bodies_and_ceilings() {
    let actions = exocortex_kernel::registered_pack_actions();
    let reg = actions
        .iter()
        .find(|r| r.pack_name == "test-pack-verbs" && r.verb_name == "RecordZap")
        .expect("RecordZap registered");
    assert_eq!(reg.ceiling, Visibility::Team);

    // The typed input decodes through the generated adapter and the body
    // runs — the ceiling handle narrows what the body may stamp.
    let hex_id = "00".repeat(16);
    let product = (reg.run)(
        &ActionContext {
            ceiling: reg.ceiling,
        },
        serde_json::json!({ "zed": hex_id, "note": "zap note" }),
    )
    .expect("typed body runs");
    assert_eq!(product.memories.len(), 1);
    assert_eq!(product.memories[0].memory_type, MemoryType::Zed.id());
    // The body requested Project under a Team ceiling: not narrowed.
    assert_eq!(product.memories[0].visibility, Visibility::Project);
    assert_eq!(product.edges.len(), 1);
    assert_eq!(product.edges[0].kind, "Zaps");

    // A malformed input is a decode rejection, never a panic.
    let bad = (reg.run)(
        &ActionContext {
            ceiling: reg.ceiling,
        },
        serde_json::json!({ "zed": 7 }),
    );
    assert!(bad.is_err());

    // The typed input schema is generated (the --dump-tools surface).
    let schema = (reg.input_schema)();
    assert!(schema.schema.object.is_some(), "{schema:?}");
}

#[test]
fn pack_functions_register_with_budgets_and_scheme_bodies() {
    let functions = exocortex_kernel::registered_pack_functions();
    let reg = functions
        .iter()
        .find(|r| r.pack_name == "test-pack-verbs" && r.verb_name == "ZapEligible")
        .expect("ZapEligible registered");
    assert_eq!(reg.engine, "scheme");
    assert_eq!(reg.p50_budget_us, 50);
    assert_eq!(reg.p99_budget_us, 500);
    assert!(reg.body.contains("(input \"flag\")"), "{}", reg.body);
    let input = (reg.input_schema)();
    assert!(input.schema.object.is_some(), "{input:?}");
    let output = (reg.output_schema)();
    assert!(output.schema.instance_type.is_some(), "{output:?}");
}

#[test]
fn verb_signatures_join_the_compatibility_fingerprint() {
    // Summaries are built directly (the fixture pack deliberately omits
    // the kernel-const kinds, so full Ontology assembly would reject it).
    let base = pack_def();
    let sum = |p: &exocortex_kernel::PackDef| {
        exocortex_kernel::compatibility::OntologySummary::of_canonical_packs(&[p.clone()])
    };
    let base_fp = sum(&base).compatibility_fingerprint();

    // Changing an EXISTING verb's signature is a compatibility break.
    let mut retyped = base.clone();
    retyped.actions[0].ceiling = Visibility::Org;
    assert_ne!(
        base_fp,
        sum(&retyped).compatibility_fingerprint(),
        "a ceiling change must move the compatibility fingerprint"
    );

    // Adding a verb is a superset event (OC-PRD D3): the runtime that
    // knows MORE accepts the pinned summary that knew less.
    let mut extended = base.clone();
    extended
        .actions
        .push(exocortex_kernel::verbs::PackActionDef {
            name: "ExtraVerb".into(),
            ceiling: Visibility::Team,
            input_type: "ExtraVerb::Input".into(),
            output_type: "ActionProduct".into(),
        });
    let extended_sum = sum(&extended);
    assert_ne!(base_fp, extended_sum.compatibility_fingerprint());
    let base_sum = sum(&base);
    assert!(
        base_sum.is_subset_of(&extended_sum),
        "pinned (base) is a subset of the verb-extended runtime"
    );
    assert_eq!(
        base_sum.first_divergence(&extended_sum),
        None,
        "a superset runtime diverges nowhere against the pinned summary"
    );
}

#[test]
fn verb_bodies_never_enter_pack_def() {
    // The registry carries the body; the def carries only the signature.
    let def = pack_def();
    assert_eq!(def.actions.len(), 1);
    assert_eq!(def.actions[0].name, "RecordZap");
    assert_eq!(def.actions[0].input_type, "RecordZap::Input");
    assert_eq!(def.actions[0].output_type, "ActionProduct");
    assert_eq!(def.functions.len(), 1);
    assert_eq!(def.functions[0].p50_budget_us, 50);
    // Bodies are registration-only: nothing in PackDef holds source text.
    let encoded = serde_json::to_string(&def).unwrap();
    assert!(!encoded.contains("input \\\"flag\\\""));
}

#[test]
fn guidance_entries_ride_the_pack_def_with_directional_links() {
    let def = pack_def();
    assert_eq!(def.guidance.len(), 2);
    let zebra = def
        .guidance
        .iter()
        .find(|g| g.key == "Zebra")
        .expect("Zebra guidance");
    assert_eq!(zebra.when.as_deref(), Some("recording a zap"));
    assert_eq!(zebra.links.len(), 1);
    assert_eq!(zebra.links[0].kind, "Zaps");
    assert!(zebra.links[0].outgoing);
    assert_eq!(zebra.links[0].other, "Zed");
    let companion = def
        .guidance
        .iter()
        .find(|g| g.key == "ZappedBy")
        .expect("kind-keyed guidance");
    assert!(companion.caution.is_some());
    assert!(companion.links.is_empty());
}

/// The divergence direction that matters for admission: a pinned summary
/// with a verb must not be admitted against a runtime that LOST it, and
/// the missing verb is named.
#[test]
fn missing_verb_is_a_named_divergence() {
    let base = pack_def();
    let sum = |p: &exocortex_kernel::PackDef| {
        exocortex_kernel::compatibility::OntologySummary::of_canonical_packs(&[p.clone()])
    };
    let mut stripped = base.clone();
    stripped.actions.clear();
    let base_sum = sum(&base);
    let stripped_sum = sum(&stripped);
    assert!(
        !base_sum.is_subset_of(&stripped_sum),
        "a runtime that lost a verb is not a superset"
    );
    let divergence = base_sum.first_divergence(&stripped_sum);
    assert!(
        divergence
            .as_deref()
            .unwrap_or_default()
            .contains("RecordZap"),
        "{divergence:?}"
    );
}
