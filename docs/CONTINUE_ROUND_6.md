# Round 6 restart checkpoint

Recorded 2026-08-28 because the operator explicitly parked the autonomous
finish-branch run. This file is a restart aid, not closure evidence. The
authoritative scope and status remain `docs/reviews/round-6-review.prd` and
`docs/master-plan.prd`.

## Repository state

- Current committed HEAD at parking time: `1ef4b7f` (`docs: promote
  legacy-marker and closure variants`).
- R6-R182 remains in progress until the final gate matrix and a fresh strict
  whole-project review are clean.
- R6-R275 and R6-R276 remain in progress. Their implementations are present in
  the working tree but are not committed or accepted yet.
- Do not discard or overwrite the unrelated untracked export files:
  `memorygraph-export-memories.json`,
  `memorygraph-export-relationships.json`, and `memorygraph_export.json`.
- No push, publication, release, pull-request mutation, or GitHub review was
  performed.

## Pending implementation work

### R6-R275 — legacy retry-marker replay safety

The working tree distinguishes adopted pre-generation cleanup identities from
current identities, carries that fact through storage and ingest cleanup, and
advances Redis's generation fence without removing a legacy marker that an old
Lua command can still observe. A live Redis regression replays the old Lua
before and after cleanup and expects no duplicate mutation.

Affected files at parking time:

- `crates/exocortex-storage/src/types.rs`
- `crates/exocortex-storage/src/in_memory.rs`
- `crates/exocortex-storage/src/falkor.rs`
- `crates/exocortex-storage/src/cypher.rs`
- `crates/exocortex-storage/tests/in_memory_props.rs`
- `crates/exocortex-storage/tests/integration.rs`
- `crates/exocortex-ingest/src/service.rs`
- `crates/exocortex-dreams/src/lib.rs`
- `crates/exocortex-dreams/src/fire.rs`
- `crates/exocortex-dreams/tests/fire_live.rs`

Before closing it, format and compile the current tree, run the exact live
legacy-replay regression, the full live Redis suite, and the live Falkor outbox
upgrade regression. Then move R6-R275 to Done with concrete evidence in the
same logically scoped commit as its implementation.

Known local service URLs at parking time were:

```sh
FALKOR_URL=falkor://127.0.0.1:16379
REDIS_URL=redis://127.0.0.1:16379
```

Recheck service availability rather than assuming those endpoints remain live.

### R6-R276 — closure evidence syntax coverage

`xtask/src/gates.rs` has an uncommitted implementation that recognizes
expression-bodied closures and closures with explicit return types. It includes
called and uncalled fixtures for dead-enforcement and cited Rust I/O.

The contributing strict-review worker reported these focused checks passing:

```sh
cargo test -p xtask
cargo xtask dead-enforcement
cargo xtask acceptance-coverage
cargo clippy -p xtask --all-targets -- -D warnings
cargo fmt -p xtask -- --check
git diff --check -- xtask/src/gates.rs
```

Rerun them from the parked tree before relying on the result. Then move
R6-R276 to Done with concrete evidence in the same logically scoped commit as
the gate fix.

## Last validation result

The command below exited successfully while the run was being parked:

```sh
cargo test --workspace --features exocortex-adapter-sdk/testing --no-fail-fast
```

It was compiled before the latest R6-R275/R276 edits, so it is baseline
information only and must not be recorded as final evidence.

## Resume sequence

1. Read `AGENTS.md`, `/Users/gregorydickson/.codex/RTK.md`, the complete Round
   6 review, and the complete master plan. Use `.codegraph/` before manual code
   exploration.
2. Inspect the working tree and preserve every unrelated change and export
   file. Do not reset the uncommitted R6-R275/R276 implementations.
3. Format and run focused validation for R6-R275. Fix any failure, close the
   plan row with evidence, and commit it separately.
4. Run focused validation for R6-R276. Fix any failure, close the plan row with
   evidence, and commit it separately.
5. Run a fresh strict `$deep-pr-review --branch` across the entire project and
   remediate every verified actionable finding. Repeat to a clean pass.
6. Run every mandatory gate listed in `AGENTS.md`, plus `metrics-hygiene`,
   `acceptance-coverage`, `deployment-acceptance`, and `ontology-surfaces`.
7. Run live Falkor and Redis validation when available. Run
   `scripts/chaos-leader-kill.sh` when its Docker infrastructure is available;
   otherwise record it as unexecuted, never passed.
8. Run the finish-branch history-backed tech-debt and structural-refactor
   audits and remediate actionable findings.
9. Only after all validation and strict review are clean, close R6-R182 and
   align the Round 6 review and master-plan closure state in the same commit.
10. Perform one final strict review of the closure commit and confirm the
    working tree contains only the three preserved export files.

Do not push, publish, release, modify PR metadata, or post GitHub reviews
without explicit operator authorization.
