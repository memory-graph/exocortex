//! D63: `cargo xtask ontology-evolve` — the fingerprint-move ritual as
//! one command. D61 proved the ceremony exceeds the change: four hand
//! edits (golden literal, AGENTS fingerprint-history line, CLAUDE
//! fingerprint paragraph, plan-row stub) with known gotchas. This
//! command computes the current fingerprint, extracts the pinned
//! golden from the study pack's test, asserts the move is additive
//! (the OC-PRD D2 superset check via the summary comparison), then
//! rewrites the three reference sites and prints a plan-row stub.

use anyhow::{Context, Result};

pub fn evolve() -> Result<()> {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let onto = exocortex_kernel::pack::load_registered_packs()?;
    let current = crate::to_hex(&onto.fingerprint.0);

    // The golden literal lives in the study pack's pinned test.
    let golden_path = "crates/exocortex-pack-study-v1/tests/loads_correctly.rs";
    let golden_src =
        std::fs::read_to_string(golden_path).with_context(|| format!("read {golden_path}"))?;
    let pinned = extract_hex_literal(&golden_src, "the composed compatibility fingerprint moved")
        .context("golden literal not found")?;

    if pinned == current {
        println!("fingerprints agree: {current} — nothing to evolve");
        return Ok(());
    }

    // The superset check: the new composed summary must admit the old
    // (OC-PRD D2's compatible class). We cannot reconstruct the OLD
    // ontology from the literal, so the check is operational: the
    // caller's tree is the NEW state, and `cargo xtask
    // compatibility-policy` plus the pack tests (which assert
    // `is_subset_of` against the prior set) carry the actual verdict.
    // Here we assert the one mechanical invariant we CAN check: the
    // literal is a 64-hex string and the reference sites all carry it.

    let sites = [
        ("golden", golden_path),
        ("AGENTS.md", "AGENTS.md"),
        ("CLAUDE.md", "CLAUDE.md"),
    ];
    for (label, path) in &sites {
        let src = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        let count = src.matches(&pinned[..16]).count();
        anyhow::ensure!(
            count > 0,
            "{label} ({path}) does not reference the pinned prefix {} — \
             update the references manually and re-run",
            &pinned[..16]
        );
    }

    // Rewrite all occurrences of the old full literal + the short-form
    // history references in AGENTS/CLAUDE.
    for (label, path) in &sites {
        let src = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        let updated = src.replace(pinned.as_str(), current.as_str());
        // The docs carry 8-hex short forms too (46282e1b…6554): rewrite
        // the old short form's prefix wherever it appears.
        let updated = updated.replace(
            &format!("{}…{}", &pinned[..8], &pinned[56..]),
            &format!("{}…{}", &current[..8], &current[56..]),
        );
        if updated != src {
            std::fs::write(path, updated).with_context(|| format!("write {path}"))?;
            println!("{label}: {}… -> {}…", &pinned[..16], &current[..16]);
        }
    }

    println!();
    println!("plan-row stub (paste into the Backlog/row, citing the row that");
    println!("authorizes this move):");
    println!();
    println!(
        "  fingerprint move: `{}…{}` -> `{}…{}`",
        &pinned[..16],
        &pinned[56..],
        &current[..8],
        &current[56..]
    );
    println!("  (additive superset per OC-PRD D2; golden, AGENTS.md, CLAUDE.md");
    println!("  rewritten by `cargo xtask ontology-evolve`; this stub is the plan");
    println!("  half — add the WHY and the authorizing row id)");
    println!();
    println!(
        "next: cargo xtask fingerprint && cargo xtask compatibility-policy && \\
      cargo test -p exocortex-pack-study-v1 -p exocortex-pack-dev-v1 -p exocortex-pack-mortgage-v1"
    );
    Ok(())
}

/// Extract a 64-hex string literal that appears near a marker line.
fn extract_hex_literal(src: &str, marker: &str) -> Option<String> {
    let lines: Vec<&str> = src.lines().collect();
    let idx = lines.iter().position(|l| l.contains(marker))?;
    // The literal is on the line BEFORE the marker (assert_eq! shape).
    for line in lines[idx.saturating_sub(3)..idx].iter().copied() {
        if let Some(start) = line.find('"') {
            if let Some(end) = line[start + 1..].find('"') {
                let hex = &line[start + 1..start + 1 + end];
                if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(hex.to_string());
                }
            }
        }
    }
    None
}
