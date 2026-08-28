# Agent Instructions & Onboarding — PRD

**Owner:** Gregory Dickson
**Status:** Draft rev 5 — incorporates a second code-review pass, a pre-implementation evaluation, and a prior-art data review. Rev 2 (code-review against `exocortex-kernel`, `exocortex-pack-dev-v1`, `exocortex-ingest`, `exocortex-client`, `exocortex-wire`) changes are flagged **[r2]**; rev 3 changes are flagged **[r3]**; rev 4 changes are flagged **[r4]**; rev 5 changes are flagged **[r5]**. Rev 3 moves all pre-existing-bug remediation out to `docs/bug-prd-codebase-audit.md` and separates producer-agnostic substrate from the coding-agent instruction surface (§1.3). Rev 4 incorporates the pre-implementation evaluation: the client owns session identity (§4.8), `producer_kind` is a closed enum (§3.8), the instruction block drops its inline catalogue and enforces its length bound (§11), `end_session` self-preflights (P6), and every time or calendar estimate is removed from the plan (§7). Rev 5 incorporates evidence from a real deployment of the prior memory system (`memory-graph`; 117 memories / 47 relationships over eight days, §0.1): the write side is already accurate, so the PRD adds the maintenance loop the prior system never closed — derived confidence, staleness surfacing, write-time supersession — and an anti-skew trigger (§3.10, §4.9, §4.10).
**Audience:** Exocortex platform team, coding-agent harness owners (Claude Code, Codex, Cursor, Aider, Continue), first-user integrators
**Parent design docs:**
- `docs/prd/exocortex-core-prd.md` §13 (Session Capture), §7 (Ontology), §18 (Ingestion Protocol), §21 (Operations). **[r2]** This PRD **supersedes §13.2's three-signal trigger design** (directive block, MCP directives resource, lifecycle-hook shims) with the end-of-turn model in P8. The §13.2 artifacts `exocortex.wrapup_schema`, `exocortex://directives/session-wrapup`, and hook shims are dead design; do not build them.
- `README.md` § "Tell your agent to use it" (the current one-paragraph placeholder we are replacing)
- `AGENTS.md` (the *repo's* AGENTS.md, not the user's — different audience)
- **[r3]** `docs/bug-prd-codebase-audit.md` — the shipped-code defect audit. W1–W4 and W6 are hard prerequisites for this PRD; nothing here can ship honestly on top of them. **Note:** that document's rev 2 expanded from 7 defects to 70 across all 14 crates, including five unauthenticated network-surface defects (its Phase A). Those do not block this PRD's *design*, but they precede it in the queue — shipping an onboarding path that invites more producers onto an unauthenticated ingest surface would be the wrong order of work.

## 0. Summary

The Exocortex backend shipped through M8 with all milestone gates green (see `MILESTONE_REPORT.md`) and the MCP surface (`search_memories`, `get_memory`, `find_related`, `end_session`) live. **We have no operable instruction surface for the first user — coding agents — to actually push memories in.** The README's current 5-sentence "drop this in your agent's instructions" is aspirational; a real agent following it will either write nothing (no session-end hook fires) or write garbage (every turn becomes a `General` memory). This PRD ships the missing layer: a **first-party, versioned Agent Playbook** distributed with the client, plus the mechanics that make it operable — a preflight tool, a rejection-driven learning loop, harness-specific integration packs, and small changes to the client and MCP surface that let agents *ask* what to write instead of guessing.

**[r2]** The review found the drafting seed had drifted from the code it describes: the relationship-kind catalogue named two kinds that do not exist and omitted two that do; two of five documented reject codes do not exist; the "backend groups your writes into sessions" behavior was described as existing when nothing in the backend implements it; and the three write-path validators (kernel, ingest, offline) do not share code. Rev 2 fixes these by (a) making the playbook's kind and reject-code tables **generated artifacts** with a CI drift gate, (b) adding a backend session-grouping deliverable (D6), (c) adding a validator-consolidation prerequisite (D2a), and (d) adding cross-batch edge linking (§4.5).

The design invariant: **an agent's instructions should be short, prescriptive, and provable.** Short so a model reads them into every context. Prescriptive so behavior is deterministic across models and harnesses. Provable so the backend rejects deviations and the rejection is legible enough to fix in the same session. This mirrors §18's producer contract — the agent is a producer, and producers get admitted, not trusted.

**[r3]** Rev 3 makes two corrections of kind rather than of fact. First, rev 2 carried its own bug-fix deliverables (D2a validator consolidation, §4.7 ack fidelity) inline, which buried live correctness defects behind a docs PRD's schedule and made this document the blocker for fixes that stand on their own merit. Those move to `docs/bug-prd-codebase-audit.md`, which also records three defects rev 2 missed — the offline WAL is never drained (W1), every relationship is written at `Project` visibility regardless of its endpoints (W5), and R-T14's computed-only kind is a string literal in the ingest crate rather than a property of the ontology (W6, which would have made this PRD's generated kind table instruct agents to use a kind the backend always rejects). Second, rev 2 justified several producer-agnostic mechanisms — cross-batch edges, correction hints, write grouping — purely in terms of coding agents. Coding agents are the *first* consumer, not the only one; §1.3 now marks which deliverables are shared substrate that every future producer inherits, so the second consumer is cheap instead of a fork.

### 0.1 Prior-art evidence: the memory-graph deployment **[r5, new]**

The predecessor system (`memory-graph` — same architecture: an instruction block pasted into the harness's instruction file, agent-authored typed memories, graph links, FalkorDB storage) ran in real daily use, and its export (117 memories, 47 relationships, 8 days, ~15 writes/day — files in the repo root as `memorygraph-export-*.json`) is the only empirical evidence available about what instructed agents actually do. Four findings, each load-bearing for this PRD's remaining design:

1. **Write accuracy is a solved problem.** Sampled content cites file:line ranges, exact constants, and configuration defaults — produced at the event with the code in context. Verbatim claims, no observed confabulation. The instruction-block approach (§11) is validated by the closest thing to a trial run this design will get.
2. **Self-reported numeric scores are constants, not signals.** All 117 memories carry `confidence: 0.8` — every single one, despite the field being optional and the instructions never mentioning it. `importance` clusters at the top of its range (mean 0.86). Agents fill optional numeric fields with their default or their aspiration. Conclusion: revs 1–4's instinct that "the playbook should ask for confidence" would have produced a constant; **confidence must be derived from evidence counters, never asked for** (§4.9).
3. **No maintenance behavior ever fired.** Zero `CONTRADICTS`/`REPLACES` edges in 47 relationships despite both kinds existing and being documented as "supersedes"; `usage_count` is 0 on all 117 memories; one literal duplicate title pair (ADR 009, twice) survived eight days of reads. The system that "worked really well" worked as a *recall* engine — rich documents found by search — while every correction, supersession, and feedback loop the schema made available went unused. Fields and kinds do not close loops; loops close loops. The read path must surface staleness (§4.10) and the write path must prompt supersession at the moment of near-duplication (§4.10), because no amount of playbook prose fired these behaviors in the system where the prose already existed.
4. **Soft rules drift; enforced rules hold.** The instructions said "2–5 tags"; the instance averages 6, max 10. The instructions said link problem→solution; the graph is 49% `solution` memories with 4 `problem` and 3 `SOLVES` edges — agents write what they did, not what it answered. Confirms P4 (only enforced rules shape behavior) and adds a trigger item: the checklist must name "identified a problem" as a write condition in its own right (§3.1, §10), or the graph inherits the solution-skew and the causal rules (R1–R3) starve.

**[r5] What this changes about the PRD's thesis.** Revs 1–4 assumed the risk was "will agents write *accurate* memories" — answer: yes, when writing at the event, which end-of-turn approximates closely enough. The actual risk the evidence exposes is graph *maintenance*: unbounded accumulation of individually-accurate, collectively-stale memories, which is invisible at n=117 over 8 days and existential at org scale. Rev 5 therefore spends its additions on the maintenance loop, not the write path.

## 1. Problem and non-goals

### 1.1 Problem

The current onboarding path assumes an agent will:
1. Read the README instruction paragraph verbatim into its system prompt.
2. Correctly identify "end of session" (a concept that does not exist in Claude Code, Codex, or Cursor — sessions terminate on window-close, model timeout, or user Ctrl-C, none of which fire a hook).
3. Distill a session into 1–5 memory drafts with correct types, titles ≤200 chars, edges between them, and visibility labels.
4. Handle `EndSessionAck.rejections[]` intelligently — currently there is no guidance for what to do when the backend rejects a draft, and no way to recover the rejected content into a corrected batch.
5. Decide when to call `search_memories` vs. `find_related` vs. neither.

**In practice, none of this happens.** We know because:
- We have no telemetry on end_session call rates per agent per session (fixable — separate work item).
- The MCP surface has no "how do I write to you" tool; agents can only discover shape by trying and failing.
- The README treats a 100-page ontology as if a model can learn it from a paragraph.
- The three harnesses we target (Claude Code, Codex, Cursor) have different notions of "instruction file," "MCP config," and "session lifecycle." One paragraph cannot cover all three.

The result: the graph is empty. Every capability we shipped past M3 (reasoning, cluster, Dreams) is dark because there are no memories to reason over.

### 1.2 Non-goals

- **A prompt-engineered LLM that turns arbitrary agent output into memories.** Violates R-D6 (no LLM in Exocortex). The agent is responsible for authoring memories; we make authoring cheap, not automatic.
- **Replacing the MCP protocol.** The tools stay the same; we add ergonomics and instructions around them.
- **Fixing the session-lifecycle gap in every harness.** We accept that harnesses vary and ship guidance per harness (§5). We do not fork Claude Code or Codex.
- **Auto-summarizing conversations.** The agent decides what is memory-worthy. We provide the shape, not the content.
- **Ingesting IDE telemetry (edits, keystrokes, terminal output) as memories.** That's a v2 adapter (`exocortex-adapter-tty` or similar), not this PRD.
- **A visual authoring UI.** Out of scope.
- **[r3] An instruction surface for non-coding-agent producers.** This PRD writes one playbook, for coding agents. Docs adapter (T3 in `mintlify-docs-integration-prd.md`), IDE telemetry, research/planning agents, and analytics adapters need their own instruction surfaces, written when each lands. What this PRD does *not* do is build coding-agent-only mechanics where producer-agnostic ones cost the same — see §1.3 for which deliverables are which, and D8 for the one addition rev 3 makes so the graph can tell producers apart at all.

### 1.3 Substrate vs. surface **[r3]**

Coding agents are the first consumer of the write path, not the only one. Everything in this PRD falls into one of two buckets, and the distinction governs how each thing gets built — substrate is designed for a producer we haven't written yet, surface is designed for Claude Code specifically.

| Deliverable | Bucket | Note |
|---|---|---|
| D1 playbook, D4 harness appendices, D5 install/verify, Appendix B block | **Surface** | Coding-agent-specific by design. A research agent gets its own, later. |
| D3 `playbook_version` | Surface | Reports whichever playbook the binary carries; generalizes for free. |
| D2 `preflight_wrapup` | **Substrate** | Every producer needs pre-submit validation. §3.2 registers it through `exocortex-ops` so MCP and HTTP both get it (CR-9), not as a client-private tool. |
| D6 write grouping | **Substrate** | Rev 2 built it as `Conversation`-per-session. Rev 3 builds a per-flavor grouping resolver; `Conversation` is the dev pack's rendering of the `session` flavor. §3.6. |
| §4.8 client-minted session ids **[r4, new]** | Substrate | Every long-lived producer client benefits; the grouping key stops depending on model behavior. |
| D7 MCP `instructions` | Substrate | One string on a shared server; §3.7 wording is producer-neutral. |
| D8 producer kind **[r3, new]** | Substrate | Today every MCP write is indistinguishable in provenance. §3.8. |
| D9 write telemetry **[r3, new]** | Substrate | S2 and S5 were unmeasurable without it. §3.9. |
| D10 maintenance loop **[r5, new]** | Substrate | Derived confidence, staleness surfacing, supersession prompts — every producer's memories age; §3.10. |
| §4.2 correction hints | Substrate | Merged with the adapter SDK's existing triage table, so adapters get remediation too. |
| §4.5 `to_memory_id` | Substrate | A wire feature. The docs adapter needs it to link a doc to a code memory. |
| W1–W7 (bug PRD) | Substrate | Pre-existing defects; not this PRD's deliverables at all. |

The test applied throughout: *if the second producer would need this, it does not get a coding-agent-shaped name, a coding-agent-shaped config, or a coding-agent-shaped schedule.*

## 2. Design principles

**P1 — Instructions ship with the client, not the docs.** A markdown file that the user has to remember to paste is a broken product. `exocortex-mcp-client` writes the current playbook version to a well-known path on first run and re-runs. Harness config files reference that path.

**P2 — The playbook is versioned and fingerprinted.** Every playbook release has a version string that the client reports via a new `playbook_version` MCP tool. Backend telemetry can distinguish agents on v1.0.0 from v1.2.0, so we know whether behavior changes correlate with instruction changes. **[r2]** The playbook's *facts* — the kind catalogue, the type-triple examples, the reject-code table — are **generated from the code they describe** (`exocortex-pack-dev-v1`, `exocortex-wire`) at build time, not hand-maintained. A CI gate fails if the generated sections drift from their sources. Hand-written prose teaches judgment; generated tables carry the facts. The rev-1 draft proved the converse fails: two phantom kinds (`Extends`, `Reviews`) and two phantom reject codes (`TITLE_BOUNDS`, `INVALID_VISIBILITY`) shipped inside the drafting seed itself.

**P3 — Rejections are the training signal.** `EndSessionAck.rejections[]` is already structured, and each wire `RejectRow` already carries a `detail` string. **[r2]** The client surfaces `detail` verbatim in its ack (today `RejectionSummary` drops it), and `preflight_wrapup` adds deterministic correction hints generated from a static table keyed by the *actual* `RejectCode` enum in `exocortex-wire` (§4.2). The agent sees "kind `Fixes` requires `(Fix, Error | Problem)`; got from-type `Command`; consider `Uses`" and can fix it in the same call.

**P4 — Every playbook rule maps to a kernel invariant.** No aspirational "write good memories" guidance. Every "you MUST" clause in the playbook has a matching validator that rejects violations — client-side via `preflight_wrapup`, server-side via ingest. If nothing will enforce it, it doesn't belong in the playbook. **[r2]** Exception, stated honestly in the playbook: *which* visibility label to choose is advisory policy (the session-wrapup source ceiling is `Org`, so `org` writes are legal); only label *validity* is enforced. **[r3]** Second exception, also stated: preflight enforces what a client can know. Fingerprint drift, source registration, ceiling changes, duplicate batches, and the to-side type of a `to_memory_id` target absent from the local cache are all server-only checks. §3.2 lists the boundary explicitly; a green preflight is a strong signal, not a guarantee, and the playbook says so.

**P5 — Harness-specific integration is a first-party responsibility.** The playbook has three appendices (Claude Code, Codex, Cursor) with the exact config edits, the exact `CLAUDE.md`/`AGENTS.md` block to paste, and the exact per-harness refinement of "what counts as an accepted edit." Not "consult your harness docs."

**P6 — Preflight before write.** A new `preflight_wrapup` tool accepts a proposed batch and returns the *same* rejections the backend would return, without writing. Agents call it before `end_session` and iterate until clean. Costs a round-trip; saves a rejected batch. **[r3]** Registered through `exocortex-ops` so MCP and HTTP expose it identically (CR-9) — the MCP client answers locally because it links the kernel in-process, while every other producer reaches the same validators over HTTP. It is only honest after **W2** (`docs/bug-prd-codebase-audit.md`) makes "the same validators" literally true: today three divergent copies exist, and the offline one validates nothing at all. **[r4]** Rev 1–3 priced this as two tool calls per write (preflight, then `end_session`). It is one: the `end_session` client tool runs the same local validation before any wire dispatch and returns rejections with correction hints in place of a failed write (§3.2). `preflight_wrapup` remains for checking drafts without attempting a write and for non-MCP producers over HTTP.

**P7 — Search is for grounding, not exploration.** The playbook tells agents *when* to search: on session start (for context), on stuck (for prior solutions), on write-conflict (for near-duplicates). Not "whenever you feel like it." Deterministic call sites, no chatter budget.

**P8 — Turn-level triggers, not session-level.** Claude Code, Codex, and Cursor do not expose a session-end hook. Rather than paper over that gap with slash commands or timers, the playbook triggers on **end-of-turn** — a concept every harness has natively. "Session" in Exocortex is a *backend* grouping (D6: one `Conversation` memory per session id, `InSession` edges per accepted batch), not a lifecycle event the agent has to detect. The agent's rule is simple: at end-of-turn, evaluate a five-item checklist; if any item fired, write; otherwise do nothing. **[r4]** The session-level duty rev 2 assigned the agent — mint one id, reuse it — now belongs to the client (§4.8): the client process spans the whole conversation and cannot forget, mis-copy, or re-mint the id under context compaction. An explicitly passed session id is still honored, for deliberate multi-agent sharing (§9.4).

## 3. Deliverables

Seven things ship together. Each is small and independently testable.

### 3.1 D1 — The Agent Playbook (versioned markdown)

**Location:** `crates/exocortex-client/src/playbook/v1_0_0.md` (source of truth). Compiled into the client binary. Written to `~/.exocortex/playbook.md` on first run with a version-tagged filename (`playbook-v1.0.0.md`) plus a `playbook.md` symlink pointing at the current version.

**[r2] Generated sections:** the kind catalogue (§ "How to link"), the type-triple examples, and the reject-code table are emitted into the playbook source by `cargo xtask gen-playbook` from `exocortex-pack-dev-v1` and the `RejectCode` enum. The generated blocks are fenced with `<!-- gen:kinds -->` / `<!-- gen:rejects -->` markers; the CI gate (§7, P2) regenerates and diffs. Prose sections are hand-written.

**[r3] The generator must subtract computed-only kinds.** The pack declares 48 kinds; 47 are assertable by a producer. `SimilarTo` is Dreams-exclusive (R-T14) and the ingest boundary rejects any producer that asserts it. A generator reading the pack naively emits all 48 and the drift gate passes — the generated table would match its source, and the source would be wrong. This is **W6** in the bug PRD: the exclusion currently lives as `const COMPUTED_ONLY_KIND: &str = "SimilarTo"` in `exocortex-ingest/src/service.rs:33`, invisible to the pack. W6 moves it onto the kind definition; `gen-playbook` reads the flag. **Ordering: W6 lands before P1's generated tables, or the playbook ships a rule that guarantees rejections.**

**Shape:** Prescriptive, ≤2000 words, one page in a terminal. Sections:

1. **When to write.** One trigger: end-of-turn. Before sending your final message in a turn, evaluate a five-item checklist (accepted code edit / useful non-obvious command output / "why-or-how" claim about the codebase / decision-against-alternative / explicit user "remember this"). If ANY item fired, write. Otherwise, do nothing. Empty turns are the default. There is no "session-end signal" the agent has to detect — session grouping is a backend concept (D6).
2. **What to write.** A decision tree keyed off session activity:
   - **[r5] Identified a problem, even unsolved** → `Problem` memory. Do not wait for the fix; an unsolved `Problem` is exactly what a future session's `Solves` edge needs. (The prior system's graph ended up 49% `solution` with almost no `problem` nodes and a starving R1–R3 causal chain — §0.1. This trigger item exists to prevent that skew.)
   - Fixed a bug → `Fix` memory + `Fixes` edge to a `Problem` or `Error` memory.
   - Solved a design question → `Solution` memory + `Solves` edge to a `Problem` memory.
   - Learned a technology quirk → `Technology` or `CodePattern` memory + `Uses` / `Demonstrates` edges.
   - Ran a command that worked → `Command` memory, optionally `Uses` edges to `Technology`.
   - **[r5] Observed that an existing memory is now wrong** → write the corrected memory + `Replaces` or `Contradicts` edge to the stale one (see §4.10's write-time prompt; the agent should also do this unprompted when it notices).
   - Nothing concrete happened → **do not write**. Empty sessions are legal and desirable.
3. **How to title.** ≤200 chars (R-T5). Subject-verb-object. "Fixed FalkorDB connection pool exhausting under concurrent writes" not "bug in db."
4. **How to link.** The 8 buckets, one line each, with 1 example. Full catalogue (48 declared, **[r3]** 47 assertable — `SimilarTo` is computed-only) is a generated reference table at the end, not embedded in the flow. **[r2]** Edges may target memories from a *previous* batch via `to_memory_id` (§4.5) — today's `Fix` can link yesterday's `Problem` — in addition to within-batch `draft_key` linking.
5. **Visibility rules.** Default `Project`. Escalate to `Org` only when the memory is not project-scoped (e.g., a `Technology` quirk that applies everywhere). `Private` for user-preference notes. `Team` reserved for cross-project team-owned patterns. **[r2]** Note that all four labels (`private`/`project`/`team`/`org`) are accepted for session-wrapups (source ceiling is `Org`); label choice is policy, label validity is enforced. **[r3]** Edge visibility is *derived*, not chosen — the narrower of the two endpoint visibilities (**W5**; today every edge is hardcoded `Project`, which silently orphans `org` memories at org read scope). The playbook does not ask agents to label edges.
6. **When to search.** Session start (`search_memories` for the project); on stuck (`search_memories` for the error message); before writing a possibly-duplicate memory (`find_related` on the closest existing memory).
7. **When NOT to write.** Chit-chat, single-file edits with no decision, failed experiments where the failure itself is not novel, aesthetic preferences.
8. **How to handle rejections.** `end_session` validates locally before dispatching; fix what the local rejections name and resubmit. If a backend rejection still happens, read the `code` **[r2]** and the `detail` string, look up the fix in the generated table, resubmit in the same turn. **Rejections are never dropped silently** — if the agent cannot self-correct, surface the rejection in its final message so the user sees it.
9. **[r4] Session ids — not the agent's job.** The client mints one session id per conversation and stamps it on every `end_session` call (§4.8). Pass an explicit `session_id` only when deliberately sharing a conversation with another agent.
10. **[r5] Confidence is not yours to report.** Never invent or guess a numeric confidence for a memory. The backend derives it from evidence (§4.9). The only accuracy signal an agent contributes is structural: precise titles, real file paths, exact error strings — the things the prior deployment got right because they were written at the event with the code in context.

Full source in Appendix A of this PRD (§10).

### 3.2 D2 — `preflight_wrapup` MCP tool

**[r3]** Registered once through `exocortex-ops` (CR-9), so it is an operation rather than a client-private tool: the MCP client serves it in-process against the linked kernel with no wire call, and the HTTP surface serves it to every other producer — an adapter author, the worker, a future research agent. Runs the same validators as `IngestService::Submit`. **Prerequisite: W2** (`docs/bug-prd-codebase-audit.md`) — "the same validators" must be one set of functions, not three divergent copies.

```rust
// crates/exocortex-client/src/tools/preflight.rs
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct PreflightWrapupArgs {
    pub session_id: String,
    pub project_id: String,
    pub memories: Vec<MemoryDraftInput>,
    #[serde(default)]
    pub edges: Vec<EdgeHintInput>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct PreflightResult {
    pub would_accept: u32,
    pub would_reject: u32,
    pub rejections: Vec<RejectionSummary>,
    /// Suggested corrections. Populated deterministically — never LLM.
    /// E.g., "kind `Fixes` requires from-type in {Fix}; got `Command`.
    /// Consider `Uses` (Command → Technology | Command is valid)."
    pub corrections: Vec<CorrectionHint>,
}
```

**Correction hints are deterministic and finite.** They are generated by looking up the offending code and consulting static tables (`exocortex-wire/src/corrections.rs` for `RejectCode`, `exocortex-kernel/src/corrections.rs` for `KernelError` — §4.2) that map every rejection kind to its remediation. Both tables are exhaustively matched at compile time. New reject codes → new correction rows in the same PR. Zero LLM.

Wire contract: over MCP, purely client-local — no round-trip, latency equal to the kernel's `validate_draft`, sub-100µs per draft. Over HTTP, one round-trip and no commit.

**[r4] `end_session` self-preflights.** The same local validation runs inside the `end_session` tool before any wire dispatch: a batch with client-detectable problems returns the same `rejections` + `corrections` payload locally instead of a rejected write. One tool call per write; `preflight_wrapup` exists for checking without attempting (and is the HTTP-surface path for non-MCP producers). The playbook and the instruction block recommend `end_session` directly.

**[r3] What preflight cannot check.** P4 claims every playbook rule has a matching validator. Preflight is the client-side half of that claim and its coverage has a hard edge. It validates: field bounds, memory-type resolution, kind resolution, computed-only kinds, full from+to type triples for within-batch edges, visibility-label validity, no-widening against the *last known* ceiling, batch size, and `draft_key` referential integrity. It cannot validate: ontology fingerprint drift since the client started, source registration state, a ceiling changed server-side, `DUPLICATE_BATCH`, or — importantly for §4.5 — the stored type of a `to_memory_id` target that is not in the local cache (that triple is checked server-side only, and preflight reports it as unverified rather than passing it silently). `PreflightResult` therefore carries an `unverified: Vec<UnverifiedCheck>` field naming what it could not reach. The playbook states that a clean preflight means "no client-detectable problem," not "this will commit."

```rust
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct UnverifiedCheck {
    /// Which draft or edge, by producer-local key.
    pub key: String,
    /// What could not be checked, e.g. "to_memory_id target not in local cache".
    pub reason: String,
}
```

### 3.3 D3 — `playbook_version` MCP tool

Returns the compiled-in playbook version, the playbook content hash, **[r2]** and the instruction-block content hash: `{ version: "1.0.0", playbook_hash: "sha256:...", block_hash: "sha256:..." }`. **[r2]** One version string governs the playbook and the `CLAUDE.md` block — they are two views of one versioned artifact; there is no separate `block_version`. Enables:
- Agents can log which playbook they were following (backend telemetry captures this in the `producer_id` metadata).
- CI checks that the compiled playbook matches the `playbook/current.md` source and that the block matches the playbook (prevents drift).
- Users can `exocortex-mcp-client --dump-playbook` and get the exact version their agent is seeing.

Trivial to ship. ~30 LoC.

### 3.4 D4 — Harness integration appendices (three of them)

Living at:
- `docs/agents/claude-code.md`
- `docs/agents/codex.md`
- `docs/agents/cursor.md`

Each answers, exactly, in this order:
1. **Install command** (verbatim shell, one line).
2. **MCP config file location and the block to add** (verbatim JSON/TOML).
3. **The `CLAUDE.md` / `AGENTS.md` block to paste** (verbatim, ≤300 words). This is the load-bearing artifact — it's what's in the agent's context on every turn. Full content in Appendix B (§11).
4. **What "accepted code edit" means in this harness.** The five-item checklist in §3.1 is shape-identical across harnesses; item 1 ("you made a code edit the user accepted") has harness-specific semantics:
   - **Claude Code:** Edits go through an explicit accept/reject prompt; "accepted" = the user pressed accept (or auto-accept is on and the edit landed on disk).
   - **Codex:** Task completion with a non-error exit; edits inside a task are treated as accepted iff the task completed.
   - **Cursor:** Edits appear inline; "accepted" = the user did not immediately revert or reject within the same turn. If the user is silent, treat as accepted.
5. **How to verify it worked.** `exocortex-mcp-client --tail-audit --last 5` shows the most recent writes (defined in D5); if the number went up after a productive turn, wrapup fired.
6. **Common failure modes.** Wrong `--org`, wrong MCP config path, agent silently ignoring `EndSessionAck.rejections`, `CLAUDE.md` block not present or not being loaded by the harness, playbook symlink broken. Each with a diagnostic command.

### 3.5 D5 — Client-side playbook install + verify

**On first run**, `exocortex-mcp-client`:
1. Writes `~/.exocortex/playbook-v1.0.0.md` (or whatever version is compiled in).
2. Creates/updates `~/.exocortex/playbook.md` symlink.
3. Writes `~/.exocortex/version.txt` with the client version + playbook version.
4. Emits a stderr message: `[exocortex] playbook v1.0.0 installed at ~/.exocortex/playbook.md — reference it from your harness instructions.`

**On every run**, if the compiled playbook version is newer than the on-disk version, it re-writes and re-notifies. Users always see the current playbook.

Flags (all defined here; no orphaned references):
- `--dump-playbook` prints the compiled playbook to stdout and exits. Used by CI and by users who want to pipe it into a system prompt template.
- `--dump-block` prints just the `CLAUDE.md`/`AGENTS.md` block (Appendix B) to stdout and exits. Users pipe it into their instruction file: `exocortex-mcp-client --dump-block >> CLAUDE.md`.
- `--verify` sanity-checks the local install: does the MCP config for the detected harness point at the right binary, does the playbook file exist, is the version current, is the backend reachable and the source admitted. Prints a checklist with green/red rows and exits with the number of red rows as its exit code.
- `--tail-audit [--last N]` prints the N most recent local writes (WAL + synced) with timestamps, draft keys, and any rejection codes. Read-only. **[r2]** This defines the command D4 references; rev 1 used it without defining it.

### 3.6 D6 — Backend write grouping **[r2, new; r3, generalized]**

The turn-level trigger only works if multiple batches from one conversation land in one group. Today nothing builds that group: the backend never mints a grouping node, never emits grouping edges, and the online ingest path drops `session_id` from `MemoryContext` — **W3** in the bug PRD (it survives only inside `source_uri = session://<id>`, which nothing parses). Rule D6 (`session_cohort`) consumes edges nothing produces.

**[r3] Built as a general mechanism, not a session mechanism.** Rev 2 specified "one `Conversation` node per session id." That is the right behavior for coding agents and the wrong shape for everything else: a docs adapter has ingest runs, an analytics adapter has snapshot windows, a research agent has investigations. None of them are conversations, and all of them need exactly this grouping. Built narrowly, the second producer forces a second mechanism.

The general form: **a grouping resolver keyed on `source_flavor`.** Registered per flavor, each resolver answers two questions — what is the grouping key for this batch, and what memory type renders it:

```rust
/// One entry per registered source_flavor. Absent flavor → no grouping (v1 default).
pub struct GroupingRule {
    /// Matches IngestBatch.source_flavor / the registered source.
    pub flavor: &'static str,
    /// Extracts the grouping key from the batch (e.g. the id in `session://<id>`).
    pub key_of: fn(&IngestBatch) -> Option<String>,
    /// The memory type the grouping node is rendered as, in the effective ontology.
    pub node_type: &'static str,
    /// The kind linking members to the grouping node.
    pub edge_kind: &'static str,
}
```

`session` registers as `GroupingRule { flavor: "session", key_of: parse_session_uri, node_type: "Conversation", edge_kind: "InSession" }`. That is the only rule v1 ships. The docs adapter registers its own when it lands, with no change to the commit path.

The commit-path behavior, per rule:
1. **Resolve the grouping key** via the rule for the batch's flavor. No rule, or no key → no grouping, commit unchanged.
2. **Upsert one grouping node per (org, flavor, key)** under a deterministic id derived from `(org_id, flavor, key)` via the existing `MemoryId::from_external` shape — same input, same id, so replays and multi-batch groups converge. Title from the rule's renderer (`Session <short-id>`). Visibility: `Project`, within the session-wrapup ceiling of `Org`. Provenance: `Derived` — the backend, not an author, asserted it.
3. **Stamp `MemoryContext`** on every accepted memory in the batch. **[r3]** This is W3's fix and belongs to W2's shared commit-shaping code, not to this deliverable — D6 consumes it, and the online path drops `project_id` and `user_id` today for the same reason it drops `session_id`.
4. **Mint the grouping edges** from each accepted memory to the grouping node. **[r3]** Rev 2 argued that batch idempotency prevents duplicates. It does not, across a restart: `(producer_id, batch_id)` dedup is in-memory only (**W7**, backlog R2). The edge must carry a deterministic relationship id derived from `(from_id, kind, to_id)` so re-minting is a no-op — which is the right answer anyway, since a producer may legitimately re-assert the same edge in a later batch.
5. **Dreams/consolidation leaves these rows alone** — grouping nodes and their edges are structural, not content; excluded from merge/prune candidates in v1.

Acceptance: an e2e test submits two batches with the same `session_id`, different `batch_id`s, and asserts one `Conversation`, two memories stamped with the session id, two `InSession` edges, and identical behavior online and offline. **[r3]** A second test restarts the service between the two batches and asserts the edge count is unchanged (W7). A third registers a synthetic second flavor and asserts it groups under its own node type without touching the session rule. **[r4]** A fourth omits the explicit session id entirely and asserts the client-stamped default (§4.8) groups identically.

### 3.7 D7 — MCP server `instructions` string **[r2, new]**

The client's initialize `instructions` (mcp.rs) today says only *"Exocortex local memory graph. Call exocortex.search_memories to query the org graph."* — it advertises the read path and never mentions writing. Rewrite to one sentence covering both directions:

> "Exocortex typed memory graph. Read with exocortex.search_memories / exocortex.find_related. To write, submit with exocortex.end_session (1-5 typed memories, ≤200-char titles, edges by draft_key or memory id) — it validates locally and explains any rejection. exocortex.preflight_wrapup checks a batch without writing."

**[r3]** Producer-neutral wording. Rev 2's draft said "at end of a productive turn," which bakes coding-agent semantics into a string served by a binary that any MCP consumer may run — a planning agent or a research agent reads the same field. *When* to write belongs in the playbook and the `CLAUDE.md` block, which are coding-agent artifacts by design; the server description states the mechanism only.

This is not prompt injection — it is the protocol-defined server description field, ~40 words, and the only instruction surface we control without user action. Harnesses that surface it (many do) get correct write behavior with zero setup; harnesses that ignore it lose nothing. This is the cheapest adoption lever in the PRD and it was previously conflated with the rejected "inject the playbook into the system prompt" idea (§5).

### 3.8 D8 — Producer kind on the registered source **[r3, new]**

Every MCP write today is `producer_id: "session-wrapup"` with `Provenance::Asserted { author: "session-wrapup" }` (`exocortex-client/src/tools/end_session.rs:158-162`, `exocortex-ingest/src/service.rs:288`). Claude Code, Codex, a planning agent, and a research agent running the same client binary are **indistinguishable in the graph, permanently**. `harness_hint` in §4.4 does not fix this: it is optional telemetry on `ProducerIdentity`, logged and discarded, never stored on the memory.

That is tolerable while coding agents are the only MCP producer and intolerable the moment they aren't. Without it: no per-producer-kind ceilings, no "show me what the research agent asserted," no way to weight or retract one producer's claims, and no way to answer "which producer is generating the rejections" from stored data.

- **`RegisterSourceRequest` gains `producer_kind`** (proto3 additive) — a **[r4] closed enum, not a free string**: `coding_agent | research_agent | docs_adapter | analytics_adapter | custom`. A free string would persist typos into append-only provenance forever — D8's own failure mode reproduced inside D8. Registration rejects an unknown or unspecified value; `custom` is the escape hatch for novel producers, and a genuinely new kind is one additive enum value. Registered once per source, org-admin-visible, alongside the ceiling it already carries.
- **Stored on provenance.** `Provenance::Asserted` gains the producer kind next to `author`. Existing rows read back as `"custom"`; no migration.
- **Ceilings become per-kind-capable.** No behavior change in v1 — every kind keeps the ceiling its source registered — but the policy hook exists before the second producer needs it, rather than after.
- The client sends `coding_agent`. One line.

Cheap now, expensive later: retrofitting producer identity onto a graph that already has a million undifferentiated `session-wrapup` rows means guessing.

### 3.9 D9 — Write telemetry **[r3, new]**

S2 (rejection rate over the first 1000 calls) and S5 (playbook-version distribution across recent writes) are both stated as checkable and neither is measurable. §1.1 concedes the gap — "we have no telemetry on end_session call rates … fixable — separate work item" — and then never assigns the work item. Two of six success criteria therefore depend on something nobody owns, which means this PRD cannot be evaluated against its own bar.

Scope it small enough to be free:
- **Backend counters** on the ingest commit path, labeled by `producer_id`, **[r3]** `producer_kind` (D8), `playbook_version` and `client_version` (§4.4), and `RejectCode`: batches submitted, memories accepted, memories rejected. `metrics::counter!` is already in use at `service.rs` for embeddings; this is the same mechanism.
- **Grouping counters** for S4: grouping nodes created, batches per group at commit.
- **Client-side**, `--tail-audit` (D5) reads the local WAL and audit ledger and answers the same questions for one user without any backend query. This is what a single developer actually uses; the counters are for the aggregate.

No new storage, no new service, no PII beyond what `ProducerIdentity` already carries. If D9 is cut, S2 and S5 must be struck rather than left as unmeasurable criteria.

### 3.10 D10 — The maintenance loop: derived confidence, staleness surfacing, write-time supersession **[r5, new]**

§0.1's evidence: the prior system's schema offered every maintenance behavior this PRD's ontology offers — optional confidence, `CONTRADICTS`/`REPLACES` kinds, a usage counter — and none of it ever fired. 117 memories, 47 edges, zero supersessions, a constant confidence, one live duplicate. The conclusion is not that agents are careless; it is that **optional behaviors invoked by prose do not happen, and fields nobody computes are constants.** D10 is the loop-closing deliverable, in three parts (mechanics in §4.9/§4.10):

1. **Derived confidence (§4.9).** `Memory.confidence` is computed by the backend from evidence — validation history, supporting/contradicting edges, supersession state — never solicited from the producer. The playbook explicitly tells agents not to report it (§3.1 item 10).
2. **Staleness surfacing on read (§4.10a).** `search_memories` and `get_memory` mark any memory that a live `Replaces`/`Contradicts` edge points *at* as superseded, naming the successor. The read path is the only place every future session already looks; a stale belief corrected only in the graph's structure is corrected for queries that traverse edges, and silently wrong for every search hit.
3. **Write-time supersession prompts (§4.10b).** When `end_session`'s local pass finds a draft whose nearest cached neighbor exceeds the similarity threshold, the ack suggests the supersession edge (`Replaces` for same-subject updates, `Contradicts` for refutations) instead of silently adding a near-duplicate. Deterministic — embedding cosine, the same mechanism Dreams already uses — no LLM.

Scope guard: this is not a garbage collector. Old memories are never deleted by D10; they are marked, linked, and ranked. Deletion stays Dreams' consolidation cycle, which already handles the merge case with audit and rollback.

## 4. Kernel, wire, and API changes

Small. Enumerated so no back-fitting.

### 4.1 New MCP tools

- `exocortex.preflight_wrapup` (D2) — validates a batch, returns rejections, correction hints, **[r3]** and the `unverified` list.
- `exocortex.playbook_version` (D3) — returns `{ version, playbook_hash, block_hash }`.
- **[r5]** No new tools for D10 — supersession hints extend `end_session`'s ack and staleness annotations extend the existing read tools (§4.10), keeping the tool surface the playbook has to teach.

Both tools register through `exocortex-ops` per CR-9 (one registration, MCP + HTTP parity). HTTP surface exists automatically. **[r3]** For D2 this is load-bearing rather than incidental: the HTTP surface is how a producer that is not an MCP client validates before submitting.

### 4.2 Correction tables **[r2, revised; r3, merged with the existing triage table]**

Rev 1 placed the table in the kernel keyed on `RejectCode` — wrong owner: `RejectCode` lives in `exocortex-wire` (the proto enum), while the client-local preflight path produces `KernelError`.

**[r3] Half of this already exists and rev 2 proposed to duplicate it.** `crates/exocortex-adapter-sdk/src/classify.rs:29` is an exhaustive, wildcard-free `RejectCode → Disposition` table with exactly the compile-time exhaustiveness property §4.2 asks for — a new variant fails the build there today. Adding a second exhaustive table over the same enum in a different crate produces two tables that agree now and disagree later, and it leaves adapters — which already consult `classify` on every reject — with a disposition and no remediation text.

Merge them. One table over `RejectCode`, in `exocortex-wire`, returning both:

```rust
pub struct RejectGuidance {
    pub disposition: Disposition,
    pub correction: CorrectionTemplate,
}
pub fn guidance(code: RejectCode) -> RejectGuidance;  // exhaustive, no wildcard
```

`exocortex-adapter-sdk::classify` becomes a thin `guidance(code).disposition` — the SDK's R-I4 single-dependency rule is satisfied, since it already depends on `exocortex-wire` and nothing else. Every adapter gains remediation text for free, which is the §1.3 test applied to a table.

Two tables remain, keyed on their own enums (a new variant without an entry fails the build):

- **wire:** `HashMap<RejectCode, CorrectionTemplate>` — batch/ack-level rejections. Templates are static strings or parametric (`InvalidTypeTriple { kind, allowed_from, got } → "Kind '{kind}' requires from-memory-type in {allowed_from}; got '{got}'. RelatedTo is the always-valid fallback."`), populated by the same code that emits the rejection.
- **kernel:** `HashMap<KernelError, CorrectionTemplate>` — local-validation errors (`TitleBounds`, `EmptyContent`, `SummaryBounds`, `MetadataTooLarge`, `InvalidTypeTriple { kind, from }`, `UnknownKind`, `VisibilityWidening { … }`).

Template text is shared by convention (same wording for the same condition on both paths); a unit test asserts the overlap stays in sync for the conditions that can arise on both.

### 4.3 New client startup path

Playbook install on first run (D5). ~50 LoC in `crates/exocortex-client/src/main.rs`. Idempotent; safe on every invocation.

### 4.4 Optional producer telemetry: `client_metadata`

Extend `ProducerIdentity` (in `proto/ingest.proto`) with an optional `client_metadata: ClientMetadata` field containing `{ playbook_version, client_version, harness_hint }`. Backend logs it. Zero behavior change; enables future correlation between playbook versions and rejection rates.

Wire-level: this is a proto3 additive field, non-breaking. `exocortex-wire` regenerates; old clients keep working.

### 4.5 Cross-batch edges: `to_memory_id` **[r2, new]**

Today `EdgeHintInput` links only within-batch `draft_key`s, so a turn-level `Fix` can never `Fixes`-link the `Problem` a previous batch wrote — which quietly breaks the compounding story the whole product rests on. Additive changes, no breakage:

- **Client (`EdgeHintInput`):** optional `to_memory_id: Option<String>` (32-hex). When set, `to_draft_key` must be empty; when `to_draft_key` is set, `to_memory_id` must be empty.
- **Wire (`RelationshipDraft`):** optional `to_memory_id: string` (proto3 additive). Server resolves it to the existing memory, validates existence, and enforces the same R-T17 type triple against the from-draft's type and the target's stored type.
- **Offline path:** the hint is stored on the WAL draft and validated at sync time.
- Unknown or malformed `to_memory_id` → rejection with the existing `InvalidTypeTriple` code and a `detail` naming the id (no new code needed).

Agents already hold hex ids from `search_memories` / `get_memory` / `find_related`, so no new read surface is required.

### 4.6 Validator consolidation — moved to the bug PRD **[r3]**

Rev 2 carried validator consolidation here as deliverable D2a. It is not a deliverable of this PRD: three divergent write-path validators is a live `§4` byte-identical-semantics violation on `main`, independent of whether an agent playbook ever ships, and keeping it here made a correctness fix wait on a docs schedule.

It is now **W2** in `docs/bug-prd-codebase-audit.md`, with the divergence table, the `Fix —Fixes→ Command` reproduction, the shared-function fix, and the golden-fixture parity gate (`cargo xtask write-path-parity`). Two defects rev 2 folded into D2a's scope are separated there because they have different fixes: **W3** (the online path drops `MemoryContext.session_id`, `project_id`, and `user_id`) and **W5** (relationship visibility hardcoded to `Project`). Both land in the shared commit-shaping code W2 creates.

**This PRD's dependency on it is unchanged and hard:** D2 (`preflight_wrapup`) cannot claim to run "the same validators as the backend" until W2 lands. See §7 for ordering.

### 4.7 Ack fidelity — moved to the bug PRD **[r3]**

Also not a deliverable here. The wire `RejectRow` carries `{ draft_key, code, detail }`; the client's `RejectionSummary` (`end_session.rs:73-79`) keeps the first two and drops the third, so the backend's own explanation never reaches the agent. That is **W4** in `docs/bug-prd-codebase-audit.md` — a two-line fix with no dependencies and the highest value-per-byte of anything in either document.

P3 (rejections are the training signal) rests on it entirely: correction hints complement `detail`, they do not replace it, and they only exist on the client-side validation paths (`preflight_wrapup`, and `end_session`'s local pass — §3.2 **[r4]**). `detail` is the only explanation the backend itself sends. **W4 is a hard prerequisite for P3.**

### 4.8 Client-minted session identity **[r4, new]**

Revs 1–3 made the agent responsible for minting one session id per conversation and reusing it — the only mandatory step in the grouping path performed by an LLM. That assignment puts the least deterministic component of the system on the critical path of D6: context compaction, a forked conversation, or a model that simply re-mints per batch silently shreds the grouping, and nothing detects it — every batch is individually valid, the groups just never form, and S4 reads low for a reason the telemetry cannot name.

The fix is ownership, not exhortation. The MCP client process spans the whole harness conversation (stdio servers are launched per session), so the client mints one session id at process start and stamps it on every batch it sends:

- **Default:** no `session_id` argument on `end_session` → the client's per-process id is stamped into the batch context. The agent never thinks about ids; the playbook says so once.
- **Explicit override:** a caller-supplied `session_id` is honored verbatim — this is how two deliberately cooperating agents share one conversation group (§9.4).
- **Offline path:** the id persists with the WAL so drained batches group identically to online ones.
- **Lifetime honesty:** process == conversation in every target harness today. A future harness that forks or resumes conversations across client processes degrades grouping to per-process groups — a recorded limit, not a guessed-at fix.

Acceptance: extend D6's e2e with a variant that omits the session id entirely; two `end_session` calls in one client process with no explicit id MUST produce one grouping node, and a second process MUST group separately.

### 4.9 Derived confidence **[r5, new]**

The prior deployment's `confidence` field was a constant (0.8 on 117/117 memories) because it was optional, unsolicited, and defaulted — the shape of a field nothing computes (§0.1 finding 2). Exocortex already has the counters that make confidence *derivable*: `RelationshipProperties.validation_count` / `counter_evidence_count` / `success_rate`, the R-Dr13/14 outcome channel, and — with §4.10 — supersession state.

- **`Memory.confidence` is backend-owned.** `MemoryDraft` does not carry a producer-settable confidence; ingest stamps it from evidence at commit and re-derives on validation/outcome events. The wire stays unchanged — a draft that somehow arrives with a confidence-looking metadata key is ignored, not honored.
- **Derivation, v1:** start at the F01 default; increment on validation outcomes that exercise the memory's claim (R-Dr13); decrement on counter-evidence; drop to the floor when a live `Replaces`/`Contradicts` edge points at it. Constants live in the pack (`Function` inputs, the KP5 pattern), so tuning is a fingerprint-visible change with golden tests — not a magic number in service code.
- **The playbook and block say one sentence:** agents never report confidence (§3.1 item 10). The only accuracy signals they contribute are the structural ones the prior deployment got right — file paths, error strings, exact values, written at the event.

### 4.10 Staleness surfacing and write-time supersession **[r5, new]**

Two halves of one loop, both deterministic.

**(a) Read path — superseded state is visible where every session already looks.** `search_memories` and `get_memory` results carry a `superseded_by: Option<MemoryId>` resolved from live `Replaces`/`Contradicts` edges, and the default corpus ranks superseded memories below their successors. The soft-delete plumbing (ST1/ST2, CS6 health LSNs) already filters at the property layer; this is the same mechanism applied to a semantic rather than bi-temporal death. An agent that reads a search hit sees the correction in the same breath as the claim — no reliance on edge traversal to discover it, which is the exact reliance the prior deployment's zero-supersession record refutes.

**(b) Write path — the near-duplicate moment is the supersession moment.** The local pass in `end_session` (§3.2 r4) computes each draft's embedding (the client already embeds for search; embeddings exist on the commit path since W3) against the local cache and, for the nearest neighbor above the Dreams merge threshold, appends a non-blocking hint to the ack:

```
similar_to: [{ draft_key, existing_memory_id, existing_title, suggestion: "replaces | contradicts | duplicate | distinct" }]
```

The suggestion is threshold-based, not inferred: near-duplicate + same memory type → `replaces`; near-duplicate + contradictory terms the validator can check (e.g. same `Fixes` target) → `contradicts`; exact-content match → `duplicate` (and the batch proceeds, letting the backend's dedup handle it). The agent decides; the loop fires because the prompt arrives at the moment of duplication, not as standing prose. This is the same cosine mechanism Dreams uses for merge candidates — no new similarity machinery, no LLM.

Both halves register through `exocortex-ops` (CR-9): the read-side annotation rides the existing `search_memories`/`get_memory` handlers, the write-side hint extends the end-of-batch local report. HTTP producers get both identically — a docs adapter re-importing a changed page is exactly case (b), which is why this deliverable is substrate (§1.3).

## 5. Anti-scope

Explicitly rejected during design; recorded so they don't creep back in.

- **A "smart" wrapup tool that reads the last N turns and drafts memories itself.** No LLM in the client (R-D6 applies to the whole distributed system, not just the backend). If the agent needs LLM help, that's the agent's LLM, not ours.
- **Auto-firing `end_session` on a timer or signal.** Sessions must end deliberately. A timer-fired wrapup would generate low-quality memories from mid-work state.
- **A "memory linter" that runs on every turn.** Overhead on the hot path. Preflight is opt-in per call.
- **Custom instruction files per user org.** The playbook is one-size-fits-most-devs. Org-specific guidance goes in the org's own AGENTS.md, referencing the playbook.
- **A prompt-engineering leaderboard.** No.
- **Injecting the playbook into the agent's system prompt from the client.** MCP is a tool protocol, not an instruction protocol; the client cannot reach the harness's system prompt, and we don't try. **[r2]** This is distinct from D7: the MCP `instructions` field at initialize *is* ours to set (protocol-defined server description, ~40 words) and we set it. What stays rejected is pushing the multi-hundred-word playbook, or any per-turn prompt content, from the client. We ship the file *and* the short `CLAUDE.md`/`AGENTS.md` block; the user pastes the block.
- **Slash commands (`/wrapup`, `/remember`, etc.).** Rejected. Slash commands are a symptom of underspecified triggers — a user-facing manual fallback for the case where the agent's own heuristic missed. Fix the trigger, not the workaround. The five-item end-of-turn checklist (§10 / §11) is the trigger; if it's too weak, tighten the checklist, don't add a slash command. This also keeps the surface identical across the three harnesses; slash-command grammars differ.
- **Timer-fired or signal-fired wrapups.** Rejected for the same reason: mid-work state is not memory-worthy, and a timer cannot know whether a turn was productive.
- **[r5] Asking agents for numeric confidence/importance.** The prior deployment asked (the field existed, the schema documented it) and got a constant 0.8 on 117/117 memories — an aspiration, not a signal. Any accuracy score agents report is a constant with noise on it; derived-from-evidence confidence (§4.9) is the only kind worth storing. Same verdict for an `importance` field if anyone proposes one: it clustered at the top of its range in the prior deployment, which makes it a constant with extra steps.

## 6. Success criteria

Numbered so they're checkable.

**S1 — Install-to-first-write:** A developer following the README + harness appendix produces their first non-rejected `end_session` batch without consulting anything beyond those two documents.

**S2 — Rejection rate:** Across the first 1000 real `end_session` calls in the wild, ≤5% of batches are rejected end-to-end (after preflight). Preflight-caught rejections don't count against this; that's the tool working. **[r3] Depends on D9** — this is unmeasurable without write telemetry; if D9 is cut, strike S2 rather than leave it as an uncheckable criterion.

**S3 — Playbook adherence:** Sample 50 accepted `end_session` batches. In ≥90%, the drafts obey playbook rules (title length, type-triple validity, visibility labels sensible, ≤5 memories per batch, edges reference within-batch drafts **[r2]** or valid existing memory ids per §4.5).

**S4 — Coverage: [r2, restated to be measurable; r4, de-clocked]** Across active users, ≥60% of backend session groups (D6 `Conversation` nodes) contain ≥2 wrapup batches — rev 2 scoped this to groups spanning a calendar day; rev 4 drops the clock and takes the stricter population. Turn counts are not visible to the backend, so the original "sessions with ≥5 turns" denominator is unmeasurable without new telemetry; the D6 grouping plus `client_metadata` (§4.4) is the measurable proxy. Corollary: median batches-per-session-group trending >1 is the signal that turn-level triggering works. **Depends on D6.**

**S5 — Playbook versioning works:** Backend telemetry can, on request, produce the distribution of playbook versions across recent writes. Confirms D3/§4.4 telemetry actually flows. **[r3] Depends on D9**, same condition as S2.

**S6 — Verify command is honest: [r2, scoped to what it can know]** `exocortex-mcp-client --verify` exits 0 iff every *client-checkable* precondition of a successful write holds: harness config points at this binary, playbook current, backend reachable, source admitted, ontology fingerprint matching. It must never return green when a known precondition fails (no false greens); it does not attempt to prove a future write succeeds (that would require a polluting probe write, which is rejected).

**S7 — Content accuracy and maintenance [r5, new]:** During dogfood (P10), sample 50 accepted memories and (a) verify each one's checkable claims against the repo (file paths exist, named symbols exist, quoted errors reproduce) — target ≥90% verifiable-true; (b) for each memory superseded by a `Replaces`/`Contradicts` edge, confirm the search path surfaces the supersession — target 100%, it is mechanical; (c) confirm zero unsuperseded near-duplicates (the Dreams merge threshold) survive a full consolidation cycle. Part (a) closes the PRD's biggest unmeasured claim — "agents write accurate memories" — with the same evidence bar the prior deployment's export demonstrated was achievable (§0.1 finding 1). **Depends on D10.**

Failure to meet S1, S4, or S7(a) after the dogfood period (P10) means the playbook itself is wrong (probably too long or too abstract). Iterate on the playbook, not the tooling. Failure of S7(b) or S7(c) is a tooling defect, not a playbook defect.

## 7. Rollout plan

**[r3]** W-items are `docs/bug-prd-codebase-audit.md` deliverables, not this PRD's. They are listed because this PRD's phases depend on them. **[r4]** No time or calendar estimates appear here, by decision (§12): ordering is total where it matters and parallel where it doesn't. The W-item prerequisites are closed (`docs/master-plan.prd`, rounds 4–5), so P1 is unblocked.

| Phase | Deliverable | Owner | Depends on |
|---|---|---|---|
| **P0** **[r3]** | **W4** (ack `detail`) — closed | Client | Nothing |
| **P0.5** **[r3]** | **W2** (one validator) + `xtask write-path-parity` gate, then **W3** (context stamping) and **W5** (derived edge visibility) in the seam it creates — closed | Kernel + ingest + client | Nothing |
| **P0.7** **[r3]** | **W6** (computed-only as a pack property) — closed | Kernel + pack | Nothing |
| P1 | Draft playbook v1.0.0 (D1) with generated tables | Docs + platform | W6 ✓ |
| P2 | `preflight_wrapup` (D2, ops-registered) + **[r4]** `end_session` self-preflight (§3.2) + merged reject-guidance table (§4.2) + playbook-drift CI gate | Client + wire + kernel | P1, W2 ✓ |
| P2b | Write grouping (D6, per-flavor resolver) | Ingest | W2 ✓, W3 ✓, W7 ✓ |
| P3 | `playbook_version` tool (D3) | Client | P1 |
| P4 | Client install + `--verify` + `--dump-playbook` + `--dump-block` + `--tail-audit` (D5) + `instructions` rewrite (D7) + **[r4]** client-minted session ids (§4.8) | Client | P1, W1 ✓ |
| P5 | Draft the `CLAUDE.md`/`AGENTS.md` block (Appendix B, §11) | Docs + platform | P1 |
| P5a | Claude Code appendix (D4) | Platform + first-user | P1–P5 |
| P6 | Codex appendix (D4) | Platform + first-user | P5a (share template) |
| P7 | Cursor appendix (D4) | Platform + first-user | P5a (share template) |
| P8 | `client_metadata` proto field (§4.4) + `to_memory_id` (§4.5) + **[r3]** `producer_kind` (D8) — one additive proto change, one regeneration | Wire + server | P3 |
| **P8b** **[r3]** | Write telemetry (D9) | Server | P8 |
| **P8c** **[r5]** | Maintenance loop (D10): derived confidence (§4.9), superseded-state annotation on reads + write-time supersession hints (§4.10) | Kernel + client + ops | P2 ✓ (embeddings on commit path: W3 ✓) |
| P9 | Ship as `exocortex-client 0.2.0` + README rewrite | Platform | P1–P8c, W1 ✓ |
| P10 | Dogfood on this repo; iterate on the playbook from real usage until S1–S7 are honestly evaluable | Platform | P9 |
| P11 | Publish v1.1.0 playbook incorporating dogfood learnings | Platform | P10 |

**Hard order [r3; r4, de-clocked]:** W4, W2, W6 before anything in this PRD — W4 because P3 is a lie without it, W2 because D2's preflight is dishonest until the validators are one function, W6 because P1's generated kind table teaches a rejected kind until the ontology knows which kinds are computed-only (all three closed). **W1 (WAL drain) blocks P9**: shipping install-and-onboard tooling while offline writes silently never reach the backend would onboard users into data loss (closed). D1 then gates D2/D3/D5/D7, which run in parallel. D6 lands before P9 (S4 and the playbook's grouping claims depend on it). D4 needs D1–D3 to reference. D8 rides P8's single proto regeneration; D9 follows it. **[r5]** D10 lands before P9 too: S7's denominator only accumulates honestly if staleness surfacing and supersession prompts exist from the first dogfood write, and derived confidence (§4.9) must be stamped from the first committed row rather than backfilled.

## 8. Risks

- **The playbook is wrong.** Highest risk. Mitigation: dogfood (P10) before declaring v1.0 stable; iterate from real usage. **[r2]** Reduced, not eliminated, by generated tables: facts can no longer drift, but judgment (the checklist, the decision tree) can still be miscalibrated.
- **Harness lifecycle mismatch (Claude Code has no session-end hook).** Solved by the design, not a residual risk: we don't trigger on session-end at all. Triggers are end-of-turn (§10 checklist), which every harness supports natively. What *is* a residual risk is item-1 semantics ("accepted code edit") varying across harnesses; §3.4 handles this with harness-specific refinements. Mitigation for that: `--verify` spots the "you're not writing" symptom regardless of harness.
- **Preflight and backend validators drift.** **[r2]** The drift already exists — that's why **W2** is a prerequisite, not a follow-up. Post-consolidation, both paths call the same kernel functions, and the golden-fixture parity test (one suite, run against offline-validate and ingest-validate) is a CI gate. **[r3]** The deeper lesson, recorded because it generalizes: every gate in `AGENTS.md` tests a crate, none tests a seam, and all of W2/W3/W5 shipped green because of it. `cargo xtask write-path-parity` is the first seam gate; it should not be the last.
- **Correction hints are unhelpful.** They're finite templates. If a hint is confusing, PR the template. Success criterion S3 catches systematic uselessness.
- **Agents ignore playbook.** Because instructions live in the harness's system prompt, we cannot force adherence. Mitigation: playbook is short (P1 principle); backend rejects non-conforming writes so wrong behavior is loud, not silent; producer telemetry (§4.4) surfaces harnesses whose accept rate is low. **[r2]** D7's `instructions` string gives zero-setup harnesses a correct minimal nudge.
- **Users don't run `--verify`.** Mitigation: playbook step 1 tells them to; harness appendices repeat it; the client's first-run stderr message names it.
- **[r2] D6 minting noise.** Long-lived conversations accumulate one grouping node with hundreds of member edges. Acceptable in v1 (structural rows are excluded from Dreams candidates, §3.6); if graph sparsity metrics flag it, v1.1 can cap or roll groups. Recorded, not solved here.
- **[r3] Preflight is trusted past its coverage.** Agents that treat a green preflight as a commit guarantee will be surprised by server-only rejections (fingerprint drift, ceiling change, remote `to_memory_id` types). Mitigated by the `unverified` list (§3.2) and by the playbook stating the boundary in the same breath as the recommendation. Residual: an agent that ignores `unverified`. Loud, not silent — the rejection still arrives with `detail` (W4).
- **[r3] The second producer arrives before D8.** If a research or planning agent ships against the MCP surface before producer kind is stored, its writes are permanently indistinguishable from coding-agent writes and no retrofit can separate them. This risk expires when D8 lands and grows until it does.
- **[r5] The graph rots anyway.** D10's loop fires at the moments the system controls (near-duplicate writes, search reads) and cannot fire at the ones it doesn't — a claim that quietly stops being true with no successor write and no reader noticing. Mitigation: derived confidence decays with counter-evidence and age-related signals are a v1.1 question; S7 makes the rot rate a measured number instead of a vibe; Dreams consolidation handles the pathological tail. Residual: stale-but-never-contradicted memories remain ranked by their evidence history, which is the honest floor.

## 9. Open questions

1. **Should `end_session` support a "draft" mode where the batch is stored client-side and previewed to the user before write?** Costs a UX flow the CLI can't provide. Recommendation: no in v1; add as `exocortex-mcp-client --review-last-wrapup` in v1.1 if users ask.
2. **Should the playbook be a single MCP resource served by the client rather than a filesystem file?** MCP has a `resources/list` mechanism. Serving it as a resource would let the harness fetch it dynamically and pin it into system prompts. Considered but rejected for v1: adds complexity, no harness currently uses it well. Revisit in v1.2.
3. **Do we want a `learn_from_rejection(rejection_id) → correction_hint` op that's separate from preflight?** Different use case: preflight is "check before write," this would be "help me understand the ack I just got." Overlap is high — **[r2]** and §4.7 now surfaces `detail` on every ack rejection, which covers most of it — punt to v1.1 unless S2 stays high after v1.
4. **Multi-agent same-session writes.** Two agents cooperating on one conversation; both call `end_session` with the same session id. **[r2]** Legal and well-defined under D6 (both batches group under one node), but neither knows the other's drafts, so duplicate memories are likely. **[r3] Rev 2's answer — "document that shared conversations should elect a single writer" — holds only while both writers are coding agents inside one harness.** It is not enforceable across a coding agent and a research agent running as separate MCP clients: neither can see the other, and there is no election mechanism. With D8 the duplicates are at least *attributable* (different producer kinds), and with D6's general grouping they land in one group where a future dedup pass can see them together. Recommendation for v1: leave as-is, drop the single-writer advice from D4 rather than write guidance that cannot be followed, and record cross-producer dedup as a named v2 item rather than an implied one. **[r4]** Client-minted ids (§4.8) close the accidental case entirely: two processes mint two ids, so a shared group now requires both agents to pass the same explicit id — deliberate by construction, and itself the signal that the caller owns dedup. Recommendation unchanged.
5. **Should the playbook include performance guidance ("don't call `find_related` on every turn")?** Yes — shipped: "Cost & etiquette" is §8 of the playbook (Appendix A).
6. **What about non-coding agents (research agents, browsing agents, planning agents)?** Different playbooks needed. Recommendation: v1.0 ships the coding-agent playbook; v1.1 adds a research-agent playbook. Both compile into the same client binary. **[r3]** They identify themselves via `producer_kind` (D8, stored on provenance), not `harness_hint` (§4.4, logged and discarded) — the distinction matters because a playbook selected per producer kind needs the kind to be a first-class registered property, not a telemetry string. `playbook_version` (D3) reports whichever playbook the binary serves for that kind, so the versioning story generalizes without change. **[r3]** The mechanics those playbooks will sit on — preflight over HTTP, per-flavor grouping, cross-batch edges, reject guidance — are substrate this PRD already builds (§1.3); what v1.1 adds is prose and a trigger model, not plumbing.
7. **[r2] Should `InSession`/`Conversation` rows be visible to `search_memories`?** Structural rows pollute search results ("Session a1b2…" matches everything). Recommendation: exclude `Conversation`-type memories from the default search corpus in v1; add a flag later if users want them. Decide at P2b implementation.
8. **[r5] Does D10's write-time hint block the write?** No — `similar_to` is advisory; the batch proceeds regardless, and the agent may add the supersession edge in the same batch (within-batch `draft_key` → the stale memory via `to_memory_id`, §4.5) or a follow-up one. Should a high-similarity duplicate ever *reject* instead of hint? Revisit at P10 with dogfood data; the prior deployment's one literal duplicate caused no observed harm, and a hard reject risks suppressing legitimate "confirmed again" writes that should be `Confirms` edges instead.
9. **[r5] What decays confidence with time alone?** §4.9 derives from evidence events; a memory never re-validated and never contradicted keeps its stamped confidence forever. Recency decay (memory-graph scored `age_days/30`) is a ranking signal, not a truth signal — conflating them makes old-but-true memories rank like lies. Recommendation: v1 derives from evidence only; revisit time-decay at v1.1 with S7 data in hand.

## 10. Appendix A — Draft Playbook v1.0.0

The actual content that ships. Prose is the drafting seed; **[r2]** the kind catalogue and reject-code table below are the *generated* blocks (rendered here from `exocortex-pack-dev-v1` and the `RejectCode` enum as of this rev — regenerating is what keeps them true).

```markdown
# Exocortex Agent Playbook v1.0.0

You have access to `exocortex.*` MCP tools connecting you to a typed,
deterministic memory graph. Sessions compound across time because you
write to it. Follow these rules exactly; the backend enforces them.

## When to write — the end-of-turn checklist

**Trigger:** end of every turn. Before sending your final message,
evaluate the checklist. If ANY item fired during this turn, call
`exocortex.end_session` — it validates locally before writing and tells
you exactly what to fix. If none fired, do nothing — empty turns are
the default and desirable.

**Write if this turn:**
1. Made a code edit the user accepted (see harness appendix for what
   "accepted" means in your harness — Claude Code: user pressed accept
   or auto-accept landed on disk; Codex: task completed non-error;
   Cursor: user did not revert within the same turn).
2. Ran a command whose incantation was non-obvious (unusual flags,
   environment, or ordering) AND it produced the intended result. Your
   routine build/test loop does not count — only commands a future
   session would need to reconstruct.
3. Answered a "why" or "how" question with a claim about the codebase
   (a claim that would benefit a future session to know).
4. Decided against a stated alternative for a stated reason.
5. The user said "remember this" or equivalent.
6. Identified a problem — solved or not. An unsolved `Problem` memory
   is high-value: it is exactly what a future session's `Solves` edge
   needs. Do not wait for the fix to record the problem.

**Sessions are a backend concept.** You do NOT need to detect "session
end," and you do not manage session ids — the client mints one per
conversation and stamps it on every write automatically. Pass your own
`session_id` only when deliberately sharing a conversation with another
agent. Your job is turn-level: check the boxes, write if any fired,
move on.

Do NOT write on:
- Empty acknowledgments ("thanks", "ok")
- Failed experiments where the failure isn't novel
- Mid-work state (a half-done refactor is not a memory — wait for the
  turn where it lands)
- Chit-chat
- Turns where you only read or searched

## Supersession — keeping the graph true

Memory is only useful if it can change its mind.

- When you observe an existing memory is **wrong or outdated**, write
  the corrected memory and link it with `Replaces` (same subject, new
  state) or `Contradicts` (the old claim is false).
- `end_session` flags near-duplicates of existing memories in its
  `similar_to` ack field with a suggested edge (`replaces`,
  `contradicts`, `duplicate`, `distinct`). Act on it: add the suggested
  edge, or write `distinct` reasoning — do not ignore it and do not
  restate what already exists.
- Search results mark superseded memories with `superseded_by` and
  rank them below their successors. Cite the successor, not the
  superseded memory, in your reasoning.
- Do NOT invent numeric confidence scores for memories. The backend
  derives confidence from evidence; your job is verifiable specifics —
  real file paths, exact error strings, precise values.

## What to write

1–5 memories per batch. If you have more, pick the 5 highest-signal.

| Session activity | Memory type | Example title |
|---|---|---|
| Fixed a bug | `Fix` | "Fixed FalkorDB pool exhaustion under concurrent writes" |
| Solved a design question | `Solution` | "Decided to use ArcSwap over RwLock for cache snapshots" |
| Documented a tech quirk | `Technology` | "FalkorDB rejects Cypher CREATE with reserved property names" |
| Extracted a reusable pattern | `CodePattern` | "Idempotent producer submit via (producer_id, batch_id) dedupe" |
| Ran a working command | `Command` | "cargo test -p exocortex-storage --features falkor" |
| Identified a problem (even unsolved) | `Problem` | "Cache reseed misses writes during subscribe gap" |
| Encountered an error | `Error` | "FalkorDB returns 'unknown property' on version-mismatched clients" |

## How to link

Edges are typed. The 8 buckets (48 kinds):

<!-- gen:kinds — do not edit by hand; regenerated from exocortex-pack-dev-v1 -->
- **Solution (5):** `Solves`, `Addresses`, `AlternativeTo`, `Improves`, `Replaces`
- **Causal (7):** `Causes`, `Prevents`, `Triggers`, `LeadsTo`, `Enables`, `Blocks`, `Fixes`
- **Context (9):** `Uses`, `Requires`, `DependsOn`, `Contains`, `PartOf`, `InSession`, `InProject`, `WrittenIn`, `Modifies`
- **Learning (6):** `Teaches`, `Demonstrates`, `Contradicts`, `Confirms`, `BuildsOn`, `Specializes`
- **Similarity (4):** `SimilarTo` †, `DifferentFrom`, `AnalogousTo`, `RelatedTo`
- **Workflow (6):** `Precedes`, `ParallelTo`, `Executes`, `Creates`, `Configures`, `Automates`
- **Quality (5):** `Validates`, `Tests`, `Measures`, `Documents`, `Verifies`
- **Integration (6):** `IntegratesWith`, `Consumes`, `Produces`, `Exposes`, `Wraps`, `Bridges`

† Computed-only (R-T14): the consolidation cycle asserts it, producers may
not. 48 kinds are declared; **47 are yours to use.** Asserting `SimilarTo`
is rejected with `ComputedKindRejected`.
<!-- /gen:kinds -->

Two ways to target an edge:
- **Within the batch:** reference the other draft's `draft_key`.
- **To an existing memory:** set `to_memory_id` to its 32-hex id (you
  have ids from `search_memories` / `get_memory` / `find_related`).
  Today's fix can link yesterday's problem — prefer this over
  re-describing an existing memory.

If in doubt, use `RelatedTo`. It's low-strength but always valid.

**Type triples matter.** `Fixes` requires `(Fix, Error | Problem)`.
`Solves`/`Addresses` require `(Solution | Fix, Problem | Error)`. `Uses`
requires the to-side to be `Technology | Command`. If your edge violates
a triple, the backend rejects it. Call `exocortex.preflight_wrapup`
before `end_session` to catch this.

## Titles

- ≤200 characters, not empty.
- Subject-verb-object.
- Include specifics (function name, error code, tech name).
- Bad: "fixed a bug", "auth stuff"
- Good: "Fixed OAuth token refresh race in RefreshMiddleware.exchange()"

## Visibility

All four labels are accepted: `private`, `project`, `team`, `org`.
Default to `project`. Escalate to `org` for cross-project knowledge (a
technology quirk that applies everywhere). Use `private` for user
preferences ("Gregory prefers 2-space indent for TypeScript"). `team`
is rarely correct — leave it unless you're sure.

You do not label edges. Edge visibility is derived from the two memories
an edge connects (the narrower of the two), so an edge is never more
visible than either end.

## When to read

- **Session start**, once: `search_memories("<project-context-terms>",
  limit=10)`. Grounds you in prior decisions.
- **When stuck**: `search_memories("<exact error message>")` or
  `find_related(<id-of-relevant-memory>)`.
- **Before writing a possibly-duplicate memory**:
  `find_related(<closest-existing-id>)` to check.

Do NOT search on every turn. It's cheap but not free, and the results
inflate your context.

## Rejections

`end_session` returns an ack with `rejections[]`; each row has `code`
and `detail`. Read `detail` first — it names the exact problem.

<!-- gen:rejects — do not edit by hand; regenerated from the RejectCode enum -->
| Code | Meaning | Fix |
|---|---|---|
| `InvalidTypeTriple` | Kind doesn't fit the (from, to) types | Check the buckets above; `RelatedTo` is the safe fallback |
| `UnknownKind` | Kind name typo or not in this pack | Fix the spelling; use only the 48 kinds listed |
| `UnknownMemoryType` | Memory type not in this pack | Fix the type name; use only the 13 listed |
| `Unknown` | Title empty/>200 chars, content empty, or an atomic-batch reject | Read `detail`; usually trim the title |
| `VisibilityWidening` | Visibility above the source ceiling (rare for session wrapups) | Drop to a lower label |
| `ComputedKindRejected` | You asserted `SimilarTo`, which only the consolidation cycle may assert (R-T14) | Use `RelatedTo` or `AnalogousTo` |
| `DuplicateBatch` | Transport replayed the same batch id | Harmless — do NOT resubmit |
| `RateLimited` | Backend is shedding load | Transient; the client retries. Do not resubmit by hand |
| `MissingExternalKey` / `InvalidExternalKey` | External-snapshot coordinates missing or malformed | Cannot occur for session wrapups; if you see it, report it as a bug |
| `Unauthorized` / `BadChecksum` / `IncompatibleOntology` / `UnknownSource` | Batch-level config errors (credentials, fingerprint, org) | Surface to the user; not fixable by you |
<!-- /gen:rejects -->

All 14 `RejectCode` variants appear above. If you meet one that doesn't,
the generator drifted — say so in your final message.

Separately, the client itself refuses (before any wire call) batches
with an unknown visibility label, a memory count outside 1–5, or an edge
referencing a `draft_key` that isn't in the batch. Fix the argument and
retry.

1. If you can self-correct, resubmit within the same turn with fixes.
2. If you can't, tell the user before continuing. Silent rejection is
   worse than no write.

## Preflight

`end_session` runs the same local validation before dispatching: an
invalid batch comes back with rejections and correction hints without
a wire call, so one call per write is enough. Call `preflight_wrapup`
directly when you want to check drafts without attempting a write —
mid-conversation, before you've decided what to keep.

A clean local pass (from either tool) means *no problem the client
can see* — not a guarantee of commit. Some checks only the backend can
run: ontology drift, source registration, ceiling changes, and the
type of a `to_memory_id` target that isn't in your local cache. Both
tools name those in an `unverified` list. Read it; if a cross-batch
edge is listed there, expect that one to be checked server-side.

## Cost & etiquette

- Each `end_session` is one write; keep to ≤5 memories.
- End-of-turn writes are cheap because most turns fail the checklist
  and write nothing. Don't force a wrapup on a turn that didn't earn it.
- Multiple productive turns in one conversation produce multiple wrapup
  batches — that's fine; the backend groups them by session id.
- Don't `search_memories` in a tight loop; the local cache is fast but
  not free.
```

## 11. Appendix B — The `CLAUDE.md` / `AGENTS.md` block

This block is the load-bearing artifact of the whole PRD. It's what lives in the user's repo, in the agent's context, on every turn. The full playbook at `~/.exocortex/playbook.md` is the reference for corner cases — but this block is what shapes behavior because it's always visible.

**Length target:** ≤300 words — **[r4]** enforced by the drift gate, which regenerates the block and fails the build past 300 words. Prescriptive, not aspirational. If a rule can't fit here, it belongs in the full playbook, not the block. **[r4]** Rev 3's block embedded the full 48-kind catalogue inline (~350 words, over its own bound with nothing enforcing it) — violating the block's thesis that only what rides in context on every turn shapes behavior. Rev 4 keeps in-context only what shapes every write: the trigger, the fallback kind, the prohibition, the pointer. The catalogue is reference material and lives in the playbook; the gate cross-checks the block's `RelatedTo`/`SimilarTo` claims against the pack. **[r5]** The block gained two behaviors (the problem trigger item, the supersession paragraph) and paid for them by compression, not by breaching the bound — the full supersession procedure lives in the playbook (§10); the block carries only the two clauses that must fire on every affected turn.

```markdown
## Exocortex — writing to memory

You have `exocortex.*` MCP tools. At the end of every turn, before
your final message, run the checklist below. If ANY item fires, call
`exocortex.end_session` with 1–5 typed memory drafts and any edges —
it validates locally first and tells you exactly what to fix. If none
fire, write nothing.

**Write if this turn:**
- Made a code edit the user accepted
- Ran a non-obvious command that produced the intended result
- Answered a "why/how" question with a claim about the codebase
- Decided against an alternative for a stated reason
- The user said "remember this"
- Identified a problem, solved or not — future `Solves` edges need
  them

**Types you'll usually write:** `Fix`, `Solution`, `Problem`, `Error`,
`CodePattern`, `Command`, `Technology` (full list: playbook).

**Edges:** typed; the full 48-kind catalogue is in
`~/.exocortex/playbook.md`. Link within the batch by `draft_key`, or
to an existing memory by `to_memory_id` (32-hex id from search
results). When in doubt, use `RelatedTo`. Never assert `SimilarTo`
(computed-only).

**Supersession:** if an existing memory is now wrong, write the
corrected memory and link it `Replaces` or `Contradicts`. Act on
`end_session`'s `similar_to` near-duplicate suggestions; never
restate what exists. Cite successors, not `superseded_by`-marked
memories. Never invent confidence scores — the backend derives them
from evidence.

**Titles:** ≤200 chars, subject-verb-object, specific. Good: "Fixed
OAuth token refresh race in exchange()". Bad: "auth fix".

**Visibility:** Default `project`. Escalate to `org` only for
cross-project knowledge. `private` for user preferences.

**Reading:** Once at session start, `search_memories("<project-terms>",
limit=10)`. When stuck, `search_memories("<exact error>")`. Do not
search on every turn.

**Rejections:** read the `code` and `detail`, fix, resubmit in the
same turn. Never drop one silently — surface any unfixable rejection
in your final message.

Session ids are stamped by the client; you don't manage them. Full
reference: `~/.exocortex/playbook.md`.
```

### 11.1 Delivery

- The block is emitted verbatim by `exocortex-mcp-client --dump-block`
  (D5). Users pipe it into their `CLAUDE.md`:
  ```sh
  exocortex-mcp-client --dump-block >> CLAUDE.md
  ```
- **[r2]** The block is versioned identically to the playbook — one version string, one artifact, two renderings. `playbook_version` reports `{ version, playbook_hash, block_hash }`; there is no separate block version.
- The block is treated as a stable API surface: changes bump the
  version, and the changelog for `exocortex-client` calls out any
  edits so users know when to re-paste. Removing a memory type from
  the list is a breaking change; adding one is not.
- The three harness appendices (D4) each say, verbatim: "Run
  `exocortex-mcp-client --dump-block >> CLAUDE.md` (or `AGENTS.md`,
  or your harness's equivalent instruction file). Then verify with
  `exocortex-mcp-client --verify`."

### 11.2 Why the block, not just the playbook

The prior memory solution (pre-Exocortex) shipped rules through
`CLAUDE.md`/`AGENTS.md` directives, and it worked. The pattern that
works is: **short, prescriptive rules in the agent's context on every
turn**. A file at `~/.exocortex/playbook.md` is reference material —
an agent reads it once (if at all) and forgets. A block in CLAUDE.md
is re-read every turn because harnesses re-hydrate their instruction
files into context. That difference is why the block, not the
playbook, is what shapes behavior.

The playbook still exists because (a) it's what `preflight_wrapup`'s
correction hints reference, (b) it's what an agent reads when it needs
the full 48-kind catalogue and type-triple table, and (c) it's a
versioned artifact independent of any user's repo — updates flow
through client releases, not through user PRs to their own `CLAUDE.md`.
But the block is the load-bearing surface.

## 12. Appendix C — Decision log

- **Why a client-installed playbook instead of a hosted URL?** Users often work offline. Files are simpler than URL fetching. A hosted version is fine as a v1.2 addition.
- **Why versioned filenames plus a symlink?** Users grep the file into system prompts. If the file silently changes, prompts silently change. Version-tagged filenames let a user pin a specific version if they want; the symlink is the default.
- **Why not a full LLM-based agent-side "memory scribe"?** Duplication of harness capability. The harness already has an LLM. If we ship one, we're competing with the harness and violating R-D6 for the client.
- **Why not force the harness to inject the playbook?** MCP is a tool protocol, not an instruction protocol. We can't reach into the harness's system prompt. The file-on-disk + appendix-per-harness pattern is the best we can do without upstream changes to each harness. **[r2]** The one exception we do control — the protocol's own `instructions` field — is D7: one sentence, set by us, no user action needed.
- **[r2] Why generated kind/reject tables instead of hand-written?** Because the hand-written ones in this PRD's own first draft were wrong: `Extends` and `Reviews` are not kinds (the pack ships `BuildsOn`; Quality has five kinds, no `Reviews`), and `TITLE_BOUNDS`/`INVALID_VISIBILITY` are not reject codes (title violations reject as `Unknown`; a bad visibility label never reaches the backend — the client refuses it as an invalid argument). If the PRD that specifies the playbook can't keep the facts straight by hand, the artifact can't either. Generation plus a CI drift gate makes fact-drift a build failure instead of a support ticket.
- **[r2] Why correction hints split across wire and kernel?** `RejectCode` lives in `exocortex-wire` (proto enum); `KernelError` lives in the kernel. Each table lives next to its enum and is exhaustively matched, so a new variant without a correction fails the build. Rev 1 put both in the kernel, which would have made the kernel depend on wire-owned vocabulary.
- **[r2] Why backend-minted `Conversation` nodes (D6) instead of asking agents to write them?** The grouping must exist for every batch from every harness, including ones with sloppy instructions. Making it a side effect of the commit path (derived, deterministic id, idempotent under replay) means grouping holds even when the agent ignores the playbook entirely. Agent-authored grouping would be optional grouping, which is no grouping.
- **[r2] Why `to_memory_id` instead of leaving edges within-batch?** Within-batch-only linking caps the graph's value at one turn's horizon: the highest-value edges (today's fix → last week's problem) are exactly the cross-batch ones. The hex ids are already in agent hands from the read tools; the wire change is one additive proto3 field.
- **[r3] Why move D2a and §4.7 out to a bug PRD instead of keeping them here?** Because they are defects on `main`, not features. Rev 2's framing made a `§4` byte-identical-semantics violation, a dropped `detail` string, and (undiscovered until rev 3) a WAL that never drains into deliverables of a documentation PRD — which meant a correctness fix could only ship on a docs schedule, and could be descoped if the docs were descoped. Bug PRDs are the repo's existing shape for this (`docs/bug-prd-external-key-identity.md`). This PRD keeps a hard dependency on them and loses nothing.
- **[r3] Why generalize D6 before a second producer exists?** Normally the right call is to wait for N=2 — `exocortex-adapter-sdk` deliberately omits a `Source` trait for exactly that reason, and that reasoning is sound. The difference here is cost of change. A grouping resolver keyed on `source_flavor` is roughly the same code as `if flavor == "session"`, and the narrow version bakes "conversation" into a commit-path branch, a memory type, and an edge kind. Generalizing later means migrating committed rows; generalizing now costs one struct. Cheap-to-generalize-now, expensive-to-generalize-later inverts the N=2 rule.
- **[r3] Why merge the correction table into the adapter SDK's triage table?** Because `classify.rs` already is that table, with the exact compile-time exhaustiveness property rev 2 proposed to build a second time over the same enum. Two exhaustive tables over one enum in two crates is a drift bug waiting for a new variant. Merging also hands every adapter remediation text it currently lacks — the substrate test from §1.3, applied to forty lines of code.
- **[r3] Why store producer kind (D8) when nothing needs it yet?** Because provenance is append-only in practice. Coding agents are the only MCP producer today, so every write is `session-wrapup`; the moment a second one exists, the history is unattributable and no retrofit can separate it. The cost now is one proto field and one registration argument. The cost later is a guess over a million rows.
- **[r3] Why add D9 rather than drop S2 and S5?** Either is defensible; what is not defensible is keeping success criteria that cannot be evaluated. §1.1 named the telemetry gap and assigned it to nobody. D9 is counters on a commit path that already emits counters. If it is cut, the criteria go with it.
- **Why is the playbook ~2000 words instead of 500?** Because coding agents will follow prescriptive rules and ignore vague ones. Under-specified guidance is the root cause of the "empty graph" problem. Length is fine for the reference playbook because it's read on demand, not on every turn. The `CLAUDE.md` block (§11) is the ≤300-word surface that actually rides in context.
- **Why not ship this before M8?** Because the graph capabilities (Dreams, cluster) had to exist first. Shipping instructions to write into an empty read-side is worse than no instructions. Now that the graph is real, the instructions are the missing rung.
- **Why no slash commands?** A slash command is a user-facing manual fallback for the case where the agent's heuristic missed. That's a symptom of an under-specified trigger, not a solution. The prior memory solution worked without one; we can do at least as well. If the end-of-turn checklist is too weak in practice (S4 dips below 60%), tighten the checklist, don't add a `/wrapup`.
- **Why turn-level triggers instead of session-level?** Because "session" is not a concept any of the three target harnesses expose to the agent. Sessions in Exocortex are a backend grouping (D6) — and, since rev 4, the id is stamped by the client (§4.8), so the agent doesn't need to think about them at all. Every harness has a native notion of "turn," so triggering on end-of-turn is universally implementable without harness changes or user intervention.
- **[r4] Why does the client own session identity?** Because revs 1–3 left the only mandatory LLM-performed step in the grouping path as "mint an id and remember to reuse it," and an LLM is the least reliable keeper of conversation-scoped state in the system — compaction, forks, and simple re-minting all shred grouping silently, with every batch individually valid. The client process already spans the conversation; ownership costs a stamp and deletes a failure class. Explicit override keeps deliberate sharing possible (§9.4).
- **[r4] Why is `producer_kind` a closed enum?** Because an open string field persists typos into append-only provenance forever — D8's own rationale (unattributable history) reproduced inside D8 itself. `custom` is the extension point; a genuinely new kind is one additive enum value.
- **[r4] Why does `end_session` self-preflight?** Revs 1–3 priced preflight as a mandatory second tool call on every write. Folding local validation into `end_session` keeps the entire signal — rejections, corrections, the `unverified` list, zero wire cost on invalid batches — at one call, while `preflight_wrapup` stays for check-without-write and for non-MCP producers over HTTP. Two surfaces, one validator, no per-write tax.
- **[r4] Why did the block shrink?** Because rev 3 embedded all 48 kind names in the one artifact whose defining property is that it is read on every turn. An agent writing an edge needs the fallback (`RelatedTo`), the prohibition (`SimilarTo`), and the pointer — not 48 names it will never reliably recall. Fewer words that are always obeyed beat more words that are sometimes read.
- **[r4] Why no time or calendar estimates?** Because this repository's own history shows they carry no information: the 70-defect audit was opened and fully closed between two consecutive baseline notes in the master plan. Ordering is total where it matters (the hard-order paragraph) and parallel where it doesn't. A week number that is guaranteed wrong on contact is noise that hides the dependency edges that actually govern sequencing.
- **[r5] Why trust a single 117-memory export as evidence?** Because it is the only deployment of this exact architecture (instruction block → agent-authored typed memories → graph) that exists, it ran in real daily use rather than as a demo, and its findings are of two kinds: presence findings (verbatim file:line citations — accuracy achievable) and absence findings (zero supersessions, constant confidence, zero usage counts — optional behaviors don't fire). Absence findings from one deployment are strong evidence because the behaviors had every schema-level opportunity to occur; their failure mode is architectural, not statistical. Where the evidence only suggests (solution-skew), rev 5 spends a trigger item, not a mechanism.
- **[r5] Why derive confidence instead of asking for it?** The prior deployment asked and received a constant — 0.8 on 117/117, unprompted, unvarying. Optional numeric fields filled by LLMs carry the model's default, not a measurement. Evidence counters (`validation_count`, `counter_evidence_count`, `success_rate`, supersession state) are computed by the system from events that actually happened. One of these is a signal; the other is decoration that crowds out real ranking inputs.
- **[r5] Why does supersession need both a read-side mark and a write-side prompt?** Because each covers the other's blind spot. The read-side mark (`superseded_by` on search/get) corrects every future reader even when no agent acts — but it can only mark what has already been superseded. The write-side prompt fires at the moment an agent is *about to create* the near-duplicate, which is the only moment the correction is cheap — but only if the near-duplicate resembles something in the local cache. The prior deployment had neither and closed zero loops; the kinds existed the whole time.
- **[r5] Why an unsolved-problem trigger item?** The prior deployment's graph was 49% `solution` with 4 `problem` memories and 3 `SOLVES` edges — agents document what they did, not what it answered. R1–R3 (the problem→solution causal rules that justify a *typed* graph over a document store) starve without `Problem` nodes. The trigger item is the cheapest possible intervention: one line in the checklist that makes writing the problem a first-class write condition rather than an implied prerequisite of the fix.
