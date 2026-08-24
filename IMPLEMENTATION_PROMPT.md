# Exocortex v1 — Implementation Prompt

Use with your coding agent of choice (Crush, Claude Code, Codex, Cursor). Recommended cadence: **one milestone per session/PR** — run M0, verify the report, merge, then feed the same prompt back for M1. Do not ask the agent to do M0–M8 in one shot; the gates only keep you honest one milestone at a time.

---

## The prompt

You are implementing **Exocortex** from `exocortex-core-prd.md` in this repository. The PRD is the single source of truth. Read the sections a task references before writing code for it — never guess at a shape the PRD already pins.

### Ground rules

1. **Authority.** §2 is the sole authority for crate names, workspace layout, dependency pins, and kernel type definitions (`§2.6.1` defines `Memory`, `Relationship`, `Provenance`, `Visibility`, `RelKindId`/`RelMeta`, `MemoryDraft`, `Ontology`, and the fingerprint — once each). If any other section appears to disagree with §2, §2 wins; record the conflict in your milestone report rather than "fixing" the PRD.
2. **Order.** Implement milestones M0 → M8 strictly in order (§3). Do not start M(n+1) until every acceptance criterion of M(n) is green. Never leave a failing gate for later.
3. **Verbatim means verbatim.** Where the PRD says "copy verbatim from §X", copy exactly — including comments. Only fill what the PRD marks `todo!()` or explicitly instructs.
4. **No scope creep.** Implement exactly the milestone's concrete tasks. Underspecified or missing detail goes in your report as an open question — do not invent significant design silently. Small local choices (test helper names, file organization inside a crate) are yours.
5. **Hard invariants.** No LLM calls anywhere (R-D6/CR-19). No new dependencies beyond §2.2 without recording the reason in the report. No new traits/seams beyond the ones the PRD defines. Cypher stays inside `exocortex-storage` (CR-10).
6. **Commits.** One commit per task-group (or per milestone when small), message format `M<n>: <what and why>`. Never amend a merged milestone.

### Per-milestone loop

1. **Read** the milestone in §3 and every section its tasks reference.
2. **Plan** — restate the milestone's concrete tasks and acceptance criteria as a checklist.
3. **Implement** task by task. `cargo check` between tasks; add the milestone's tests as you go.
4. **Verify** — run every acceptance-criteria command exactly as written, plus the cross-cutting gates from §3 (kernel-purity, `cargo deny check`, `clippy --workspace -- -D warnings`, `fmt --check`, `cargo test --workspace`).
5. **Report** at milestone end:
   - **Gates:** pass/fail per command, with the command lines.
   - **Deviations:** anything that differs from the PRD text, with reason.
   - **Open questions:** anything underspecified, with your recommendation.
   - **Next:** the first task of the following milestone.

### Milestone-specific notes

- **M0:** Task 9 verifies every §2.2 pin resolves (`falkordb`, `steel-core`, `rmcp`, `eventsource-client`, `chitchat`, `fastembed`, `crepe`, `tonic 0.12`…). Adjust a pin minimally if it doesn't, record it, keep going — this is the only milestone where pins may change. Zero kernel unit tests is expected at M0; the test gate just needs a passing harness. Protos: `ingest.proto` from §18.6, `cluster.proto`/`sse.proto` from §2.6.3.
- **M1:** The authoritative ontology is §7.18 (13 memory types, 48 kinds, kernel constants bound there). Fill kinds one bucket at a time with `cargo check` between buckets.
- **M2:** Docker-compose FalkorDB+Redis harness first; `InMemoryStorage` is a v1 deliverable, not a nicety. Fail fast on fingerprint mismatch.
- **M3:** The 100k-memory synthetic dataset and the no-allocation assertion are the point of the milestone — don't ship the cache without the bench.
- **M4:** Kernel rules are R1–R9 (§10.4); pack rules are D1–D6 (§7.18). Derivation writes use `Provenance::Derived { rule_id, evidence }`.
- **M5:** Error names must match §9.7's `ClusterError` exactly. Chaos test is a gate, not a stretch goal.
- **M6:** `proto/ingest.proto` (§18.6) is the only ingest schema. Entities are extracted server-side, never accepted from the harness (R-T18). Session-wrapup sends no `ExternalSnapshotInfo`.
- **M7:** Parity + schema-drift goldens are CI gates; also implement the §21.4 audit log.
- **M8:** Record the clustering crate decision in `crates/exocortex-dreams/CLUSTERING.md` before implementing. Consolidation must stamp the full R-Dr4 `ConsolidationResult`, with both guardrails (R-Mcr3 ΔR, R-Mcr6 hairball).

### Start

Begin with **M0** (§3). Stop after its report. Do not proceed to M1 until the report is reviewed.

---

## After each milestone (for you, the human)

- Skim the report's **Deviations** — anything more than cosmetic deserves a PRD edit so the doc stays ground truth.
- Merge, tag `m<n>`, then re-feed the prompt.
- From M3 on, watch the bench numbers yourself; latency regressions are supposed to fail CI, and the first few will be real.
