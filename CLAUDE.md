# CLAUDE.md

@AGENTS.md

The working guide above is authoritative for this repo — layout, the ten
hard rules, the gate list, and the work lifecycle. Everything below is
Claude-Code-specific and does not restate it.

## Before you search, query the graph

This repo is CodeGraph-indexed (`.codegraph/`). Reach for
`codegraph_explore` before grep/find or reading files — one call returns
verbatim line-numbered source plus callers and blast radius, including
dynamic-dispatch hops grep cannot follow. `codegraph explore "<symbols>"`
works from the shell if the MCP tool is unavailable.

## The lifecycle is not optional

`docs/master-plan.prd` is the single source of truth for accepted work.
Two rules that bite agents specifically:

- **Asked to do something not in the plan? Add it to the plan first.**
  A backlog row with an ID, a one-line description, and a source.
- **Finished something? Close it in the same commit**, with closing
  evidence — commit sha, test file, or gate name. A stale plan is a lie
  about the repo.

Review-round findings (`docs/reviews/round-<n>-review.prd`) flow through
the same lifecycle under their own IDs. Nothing is silently dropped;
deferrals go to the v2 table with a PRD citation.

## Test invocation has a trap

```sh
cargo test --workspace --features exocortex-adapter-sdk/testing
```

The feature is **mandatory**, not optional. Without it the SDK's
mock-driven suites go dark and the run still reports green — a lib canary
fails any run that would leave them dark. Never drop the flag to make a
run faster.

Live-backend suites are feature-gated and **skip loudly** without
`FALKOR_URL`/`REDIS_URL`. A green `cargo xtask storage-conformance`
with either unset has not exercised that backend at all; it prints
`live Falkor suite UNEXECUTED` / `live Redis fire suite UNEXECUTED`.
The Dreams `fire_live` suite is also dark under the plain workspace
run (its `integration` feature is not part of
`--features exocortex-adapter-sdk/testing`) — the conformance gate is
the layer that runs it. Do not report storage coverage you did not
run — read the gate's own output before claiming it passed.

## Fingerprint changes mean you broke something

`cargo xtask fingerprint` prints two lines (OC-PRD D1): line 1 is the
compatibility fingerprint (`d60a2467…4ef52`) — the "if this moved you
broke something" value; line 2 is the build fingerprint
(`e1f7d17b…ddc9b2`, the unchanged v1-scheme value) which only reports.
If the compatibility line moves and you did not deliberately change the
ontology, that is the signal — do not update the golden to make the
gate green. An intended ontology change updates the golden, `AGENTS.md`,
and the master plan in the same commit.

## Iterating vs. claiming done

Fast loop while working — `cargo fmt`, `cargo clippy`, and the tests for
the crates you touched. Before saying a task is complete, run the full
gate list in AGENTS.md; CI runs all of it on push, and "tests pass" is
not the same claim as "gates pass."

Report gate results as they actually came out. If a gate is red, or a
suite was skipped for want of a backend, say so plainly with the output
rather than describing the work as done.

## The PRD is authority

Where code and `docs/prd/exocortex-core-prd.md` disagree, record the
conflict in `docs/MILESTONE_REPORT.md` under deviations. Do not silently
"fix" the PRD to match the code, and do not delete a `§`-reference
comment because it looks stale — those refs are deliberate.

## Persona

If `~/.claude/CLAUDE.md` puts you in a persona, keep it in prose and out
of the artifacts: commit messages follow `area: what and why` under 72
chars, and code comments, PRDs, and plan rows stay plain. Dirty
commentary, clean repo.
