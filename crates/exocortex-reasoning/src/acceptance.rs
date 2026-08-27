//! Executable PRD §23.3 deployment-mode acceptance probe.

use exocortex_kernel::{EntityId, MemoryId, Ontology};

use crate::rules::{self, Edge, EntityFact, MemoryFact, TagFact};

/// Execute all eight Crepe catalogue rules and the Steel R6 rule.
///
/// Deployment binaries call this probe themselves, so acceptance proves that
/// each shipped mode links and can execute both reasoning runtimes.
pub fn verify_nine_catalogued_rules(ontology: &Ontology) -> Result<(), String> {
    rules::prime(ontology);
    let id = |byte| MemoryId([byte; 16]);
    let [a, b, c, d, e] = [id(1), id(2), id(3), id(4), id(5)];
    let kind = |name: &str| {
        ontology
            .kind_id(name)
            .ok_or_else(|| format!("missing kind {name}"))
    };
    let solution = ontology
        .memory_type_by_name
        .get("Solution")
        .copied()
        .ok_or("missing Solution")?;
    let fix = ontology
        .memory_type_by_name
        .get("Fix")
        .copied()
        .ok_or("missing Fix")?;
    let problem = ontology
        .memory_type_by_name
        .get("Problem")
        .copied()
        .ok_or("missing Problem")?;
    let solves = exocortex_kernel::kinds::SOLVES;
    let fixes = exocortex_kernel::kinds::FIXES;
    let causes = exocortex_kernel::kinds::CAUSES;
    let depends = kind("DependsOn")?;
    let requires = kind("Requires")?;

    let derived = rules::evaluate(
        vec![
            Edge(a, c, solves),
            Edge(b, c, solves),
            Edge(d, c, fixes),
            Edge(e, c, causes),
            Edge(a, b, depends),
            Edge(b, c, depends),
            Edge(a, b, requires),
            Edge(b, c, requires),
        ],
        vec![
            EntityFact(a, EntityId([9; 16])),
            EntityFact(b, EntityId([9; 16])),
        ],
        vec![TagFact(a, 7), TagFact(b, 7)],
        vec![
            MemoryFact(a, solution),
            MemoryFact(d, fix),
            MemoryFact(c, problem),
        ],
    );
    let checks = [
        (derived.type_from_solves.contains(&(a, solution)), "R1"),
        (derived.type_from_fixes.contains(&(d, fix)), "R2"),
        (derived.type_from_causes.contains(&(c, problem)), "R3"),
        (derived.transitive_depends_on.contains(&(a, c)), "R4"),
        (derived.transitive_requires.contains(&(a, c)), "R5"),
        (
            rules::pair_counts(derived.co_occurrence_affinity).contains(&(a, b, 1)),
            "R7",
        ),
        (
            derived
                .problem_solution_bridge
                .iter()
                .any(|pair| *pair == (a, b)),
            "R8",
        ),
        (
            rules::pair_counts(derived.similar_tags_affinity).contains(&(a, b, 1)),
            "R9",
        ),
    ];
    for (passed, rule) in checks {
        if !passed {
            return Err(format!("{rule} did not fire"));
        }
    }
    if crate::explain::reverse_solves(&[(a, c)]) != [(c, a)] {
        return Err("R6 Steel reverse_solves did not fire".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn all_nine_catalogued_rules_execute_in_one_probe() {
        let ontology =
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap();
        super::verify_nine_catalogued_rules(&ontology).unwrap();
    }
}
