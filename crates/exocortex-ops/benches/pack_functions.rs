//! PX2: the generated Function-SLO bench harness (palantir-expansion PRD
//! §3.2 support machinery). This bench is GENERATED FROM THE `functions!`
//! BLOCKS in the strongest sense available to a compiled registry: it
//! enumerates `registered_pack_functions()` and gates every declared
//! budget — adding a pack Function costs no hand-written bench, and a
//! verb without budgets cannot exist. Budgets come from the registrations
//! (KP5 discipline: enforced, not declared-and-forgotten).
//!
//! Run with `cargo bench -p exocortex-ops` or `cargo xtask bench`.

use std::time::{Duration, Instant};

fn percentile(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    let idx = ((p / 100.0) * (samples.len() - 1) as f64).round() as usize;
    samples[idx]
}

fn main() {
    // Force-link the pack crates so their inventory registrations run in
    // this binary; an unreferenced dependency is not linked.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let _ = std::hint::black_box(exocortex_pack_mortgage_v1::pack_def().name.clone());

    let functions = exocortex_kernel::verbs::registered_pack_functions();
    assert!(
        !functions.is_empty(),
        "the pack-function registry must not be empty: the mortgage pack declares one"
    );

    // Shared-CI tolerance (search/khop bench rationale).
    let m = std::env::var("SLO_MULTIPLIER")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(1.0, 3.0);
    if m != 1.0 {
        println!("SLO budgets relaxed x{m} (SLO_MULTIPLIER)");
    }

    let mut all_ok = true;
    for reg in functions {
        // A representative input per registered function: the input schema
        // declares one boolean/number/string field shape; the scheme body
        // reads through `(input "field")`. The harness derives the sample
        // input FROM the schema so a new verb is benched without edits.
        let input = sample_input_from_schema(&(reg.input_schema)());
        let run = || exocortex_ops::eval_pack_function_cached(reg.body, &input);

        // Warm up, then sample.
        for _ in 0..50 {
            run().expect("pack function executes");
        }
        let mut samples: Vec<Duration> = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let t0 = Instant::now();
            run().expect("pack function executes");
            samples.push(t0.elapsed());
        }
        let p50 = percentile(samples.clone(), 50.0);
        let p99 = percentile(samples.clone(), 99.0);
        let p50_budget = Duration::from_micros(reg.p50_budget_us as u64).mul_f64(m);
        let p99_budget = Duration::from_micros(reg.p99_budget_us as u64).mul_f64(m);
        let ok = p50 < p50_budget && p99 < p99_budget;
        println!(
            "{}::{}: p50={p50:?} (budget {p50_budget:?}) p99={p99:?} (budget {p99_budget:?}) — {}",
            reg.pack_name,
            reg.verb_name,
            if ok { "PASS" } else { "FAIL" }
        );
        all_ok &= ok;
    }
    println!(
        "pack-function SLO gate: {}",
        if all_ok { "PASS" } else { "FAIL" }
    );
    if !all_ok {
        std::process::exit(1);
    }
}

/// Derive a representative input from the verb's JSON Schema: booleans
/// true, everything else "categorical". A field nothing can fill fails
/// loudly at execution — the harness cannot silently skip a verb.
fn sample_input_from_schema(schema: &schemars::schema::RootSchema) -> serde_json::Value {
    use schemars::schema::{InstanceType, Schema};
    use serde_json::json;
    let mut out = serde_json::Map::new();
    let Some(object) = &schema.schema.object else {
        return json!({});
    };
    for (name, schema) in object.properties.iter() {
        let value = match schema {
            Schema::Object(o) if o.instance_type == Some(InstanceType::Boolean.into()) => {
                json!(true)
            }
            _ => json!("categorical"),
        };
        out.insert(name.clone(), value);
    }
    serde_json::Value::Object(out)
}
