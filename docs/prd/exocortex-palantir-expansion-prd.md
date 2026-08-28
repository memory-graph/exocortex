# Exocortex Palantir-Expansion PRD

**Author:** Gregory Dickson
**Status:** Draft (rev 2)
**Created:** 2026-08-27
**Repo:** [memory-graph/exocortex](https://github.com/memory-graph/exocortex)

---

## 0. Summary

Exocortex v1 shipped the substrate for an OSS Palantir Ontology — kernel + pack seam, typed nodes and edges, provenance, bi-temporal validity, audit ledger, deterministic reasoning, Dreams consolidation with a ΔR gate, HMAC-signed ingest, and a single-binary deployment topology. The claim in the README — *"the Palantir ontology, in-house, as one Rust binary"* — is defensible for the substrate and aspirational for the surface.

This PRD closes the gap between substrate and surface. It ships **five product-scope capabilities** that together make the Palantir framing survive a hostile read of the repo:

1. **A second pack shipped by a second author** — proves the pack seam is a product, not a workspace convenience. Vehicle: `exocortex-pack-mortgage-v1`, our own LoanLight domain, written against `docs/ONTOLOGY_GUIDE.md` and reviewed by someone other than the kernel author.
2. **Pack-registered Actions and Functions** — extends the ontology surface from a fixed kernel operation set to a domain-verb surface every pack declares for itself. Turns *"the ontology is typed"* into *"the operations on the ontology are typed."*
3. **Seam conformance as a first-class CI surface** — the workspace has multiple implementations of the same contract (three `Storage` impls, two operation surfaces, two write paths, three signing pairs, and a kernel Action/Function catalogue that the operation registry is supposed to mirror). Some of those pairings are already asserted; none of them are *enumerated and enforced as a set*. This deliverable completes the coverage and makes the set itself a gate.
4. **The Mintlify adapter, end-to-end, dogfooded on `loanlight-engineering/docs`.** Turns *"documents feed the same graph"* from a diagram into a running demo with a public artifact.
5. **A read-only object explorer.** Ugly is fine. Palantir framing needs a visual surface.

Everything else that came out of the tier-2/3/4 gap analysis — entity resolution beyond regex, ontology branching, org-scale (10M+ memory) benches, granular row-level ACLs — is downstream of these five and explicitly deferred with rationale (§5).

**Non-goals for this PRD:** rewriting v1 to add LLM calls, moving off Rust, adding a hosted SaaS surface, competing with Palantir on their scale (billions of objects) — Exocortex remains the personal-through-org tier, not the enterprise tier. This document also does not schedule, track, or restate remediation work; the codebase audit owns that backlog on its own cadence (§8).

---

## 1. Problem

The v1 delivery closed the *engineering* claim ("the substrate exists, the SLOs hold, the fingerprint is stable") but leaves five *product* claims unverified against reality.

| README claim | v1 state | Gap |
|---|---|---|
| *"A legal, medical, or sales ontology is a Rust crate registering its own types, kinds, and rules through the same seam."* | The pack seam has never been exercised by a second author. `exocortex-pack-dev-v1` is the only pack in the workspace. | The seam is theory. Cross-pack name collisions, cross-pack type-triple composition, cross-pack rule bodies, and pack-load ordering have never been tested. |
| *"Actions are typed writes — each provenance-producing, each audited."* | `exocortex-kernel/src/actions.rs` declares an `Action` trait and four kernel Actions (`CommitWrapup`, `AcceptDiscovery`, `PromoteVisibility`, `RetractEdge`). The `pack!` macro has no `actions!` section, so a pack cannot declare one. | Domain verbs like `PromoteLoanToUnderwriting`, `AttachRuleFinding`, `MergeDuplicateApplicant` cannot exist. The typed-write vocabulary is fixed at kernel-authoring time. |
| *"Functions are typed reads."* | `exocortex-kernel/src/functions.rs` declares a `Function` trait with per-function latency budgets and four kernel Functions (`SearchMemories`, `TraverseRelationships`, `GetChain`, `ExplainEdge`). The `pack!` macro has no `functions!` section. Datalog and Scheme rules run inside Dreams and reasoning; they are not callable by name. | Pack-registered typed functions (`ComputeCategoricalIncomeEligibility(Applicant) -> {Eligible,Ineligible}`) cannot exist. |
| *"Documents feed the same graph through `exocortex-adapter-mintlify`, deterministically, no LLM anywhere in the loop."* | The adapter **shipped** — [memory-graph/exocortex-adapter-mintlify](https://github.com/memory-graph/exocortex-adapter-mintlify) 0.1.0, with signed registrations and batches, identity-stable pages, a durable cursor, a `validate` subcommand for docs CI, 8 golden-batch tests and a mock-server integration test (master plan ML1). `ProducerKind::DocsAdapter` is reserved and exercised in ingest tests. What has never happened is a run against a **real node with real docs**. | The adapter is verified against a mock. The write path from an actual Mintlify site into a queryable typed graph has never been observed end to end. |
| *"The ontology is the compounding asset."* | The only way to see the ontology today is `search_memories` inside an agent, `--tail-audit`, `--dump-playbook`, or raw Cypher. | Users cannot browse the graph. This is the single most consequential missing surface for the Palantir framing rhetorically. |

There is also a **structural** gap, and rev 1 of this PRD overstated it. The accurate statement:

The workspace contains several pairs of components that are required to agree, and the *individual* pairings have been addressed unevenly. Three are already asserted — `crates/exocortex-server/tests/http_parity.rs` byte-compares every registered operation across HTTP and the direct handler; `crates/exocortex-ingest/tests/write_path_parity.rs` runs a golden verdict table through both the kernel validator and the ingest service; `crates/exocortex-client/tests/standalone_readback.rs` covers the online/offline write path. Two are not asserted at all: the `Storage` trait's three implementations have no shared corpus (`crates/exocortex-storage/tests/integration.rs` is Falkor-only, behind `--features integration` and skipped without `FALKOR_URL`), and the signing producer/verifier matrix has no symmetry suite. One more is asserted in the wrong direction: the MCP tool catalogue is checked against the operation registry (`stdio_smoke.rs::mcp_tool_list_matches_registry`), but nothing checks the operation registry against the kernel's own Action/Function catalogue — which is why `RetractEdge`, `GetChain`, and `ExplainEdge` are declared in the kernel with names and latency budgets and have no registered operation behind them.

So the structural problem is not "nothing tests a seam." It is **the set of seams is not enumerated, so coverage drifts silently and a new implementation of an existing contract carries no obligation.** That is what D3 fixes.

---

## 2. Design principles

**P1 — The pack seam is a product surface, not a workspace convenience.** Every capability a pack needs to be first-class — types, entities, kinds, type-triples, rules, actions, functions, agent guidance, tool schemas — is declared *inside* the `pack!` block and generated from it. Nothing in the kernel special-cases `exocortex-pack-dev-v1`. If our own second pack cannot ship without touching the kernel, the seam is broken and the fix is in the kernel, not in the pack.

**P2 — Pack authorship is the acceptance test.** The pack seam is not "done" when the kernel compiles it; it is "done" when a pack author who did not write the kernel writes, ships, and dogfoods a pack using only `docs/ONTOLOGY_GUIDE.md`. Concretely: an engineer at LoanLight who is not the Exocortex primary author writes `exocortex-pack-mortgage-v1`, and every kernel change forced by that authorship is a bug in the seam.

**P3 — Actions and Functions are the ontology's verb surface.** Nodes and edges describe the world; Actions change it and Functions derive from it. A pack that can only declare types and kinds is Foundry's *Objects and Links* — the read-only half. Actions and Functions are what make an ontology governed rather than descriptive. Every pack-registered Action carries provenance, audit, visibility, and preflight in the same shape as the kernel's `CommitWrapup`; every pack-registered Function has a typed input/output signature, a declared latency budget, and a compiled Datalog or Scheme body.

**P4 — The seam set is the CI surface.** Any two components required to agree get a conformance suite, and the *inventory of such pairs* is itself a gate: `xtask seam-inventory` enumerates every declared seam, and adding an implementation of a contract that already has one without extending its suite fails CI. A suite that exists but is unlisted is as much a failure as a seam with no suite.

**P5 — Documents are just another producer.** The Mintlify adapter must ride the *same* `IngestService.Submit` path as `session-wrapup`, sign with the same `exocortex_wire::signing::sign_batch`, be classified by the same `RejectCode` table, and appear in the audit ledger under the same `ProducerKind` enum (variant `DocsAdapter`, already reserved). No document-specific write path.

**P6 — The explorer is read-only in v1.** Writes stay in agents and adapters — the explorer is a viewer, not a Foundry Workshop clone. This keeps the security surface small — one bearer-token read layer over the existing HTTP registry — and keeps v1 small enough to finish.

**P7 — Every success criterion is a row in the acceptance matrix.** `docs/acceptance/section-23.tsv` already carries `criterion / status / requirement / executable_evidence / command / tracking` and is validated by `xtask validate_acceptance_matrix`, which enforces the `verified | partial-deferred | deferred | partial-gap | gap` vocabulary. Every criterion in §6 lands as a row with a runnable command. A criterion with no command is prose, and prose does not gate a release.

---

## 3. Deliverables

Five capabilities, each independently shippable and testable. Ordered so D2 unblocks D1, and everything else parallelizes.

### 3.1 D1 — `exocortex-pack-mortgage-v1` (a second pack, by a second author)

**What ships:** A new crate `crates/exocortex-pack-mortgage-v1` implementing the LoanLight underwriting domain — the same domain RuleOps and the transaction-categorization work operate over — as a pack that composes with `exocortex-pack-dev-v1` at load time.

**Why LoanLight's domain, not a hypothetical legal or medical one:**
- We already know the domain — no research phase.
- We have real prose to test against: `loanlight-engineering/docs` (~70 pages) provides ~50 rules, ~15 integrations, ~10 workflows.
- It is not obviously adjacent to the dev-loop domain, so cross-pack composition gets exercised (a `Fix` in dev-v1 may `Solves` a `RuleDefect` in mortgage-v1).
- We are the first user; if the pack seam is broken for us, it is broken.
- A second real pack matters more to the framing than a third toy pack.

**Ontology sketch (indicative, final list lives in the pack crate):**

*Memory types (9):* `LoanApplication`, `Applicant`, `Property`, `RuleDefinition`, `RuleFinding`, `Underwriter`, `IncomeSource`, `Deposit`, `LenderConfiguration`.

*Entity types (7):* `LenderId`, `LoanNumber`, `RuleId`, `BundleVersion`, `NAICSCode`, `TransactionCode`, `ApplicantExternalId`.

*Relationship kinds (a small first cut — final catalogue in the crate; kinds are additive):* `Owns` (Applicant→LoanApplication), `Secures` (Property→LoanApplication), `Governs` (RuleDefinition→LenderConfiguration), `Findings` (RuleFinding→LoanApplication), `Categorizes` (TransactionCode→Deposit), `SupportsIncome` (Deposit→IncomeSource), `AppliesTo` (LenderConfiguration→LoanApplication), `Supersedes` (bundle version → bundle version), `Contradicts` (RuleFinding→RuleFinding, cross-lender), `DerivedFrom` (RuleFinding→RuleDefinition, provenance-carrying).

*Computed-only kinds:* `NearDuplicateApplicant` (Dreams-only; identity resolution is a Dreams job — see §5 for why entity resolution proper is deferred to v2). The `pack!` macro already accepts a `computed_only_kinds!` section, so this needs no kernel change.

*Type triples:* declared in the pack; cross-pack triples (e.g. `Fixes` in dev-v1 taking a `RuleDefect` in mortgage-v1 as the target) are the acceptance test for cross-pack composition — if this doesn't work, the kernel changes, not the pack.

*Rules (small — the point is to prove the seam):*
- `Findings ∧ DerivedFrom → GovernedByBundle` (transitive closure over rule provenance).
- `Contradicts ∧ same-loan-different-lenders → RegulatoryRisk` (a computed edge Dreams can surface as a proposal, never asserted directly — R-T14 discipline).

**Agent guidance:** The pack declares its own `guidance!` block (§4.2) — structured entries keyed by the pack's own memory types and kinds, rendered into the composed CLAUDE.md/AGENTS.md block by the playbook generator. For mortgage-v1 that is roughly: tag `RuleDefinition` and link it `Governs` → `LenderConfiguration`; when categorizing deposits, link `Categorizes` from `TransactionCode` and `SupportsIncome` to `IncomeSource`; never assert `Contradicts` directly, because Dreams proposes it. The pack author writes declarations, not prose.

**Authorship rule:** the pack is written by an engineer who is not the Exocortex primary author, working from `docs/ONTOLOGY_GUIDE.md`. Every kernel or macro change the pack author needs is filed as a `pack-seam` issue; those issues are D2's deliverable, not this pack's.

**Acceptance:**
- `cargo build -p exocortex-pack-mortgage-v1` succeeds against an unmodified kernel.
- Loading dev-v1 and mortgage-v1 into one node produces one composed ontology, one fingerprint, and no kind-id or triple collisions.
- A cross-pack `Fixes` triple (`Fix` in dev-v1 → `RuleDefect` in mortgage-v1) writes and reads correctly.
- Fingerprint of `{dev-v1, mortgage-v1}` is byte-stable across two clean builds, in the shape `crates/exocortex-pack-dev-v1/tests/dev_v1_fingerprint.txt` already establishes for one pack.
- Fewer than 3 open `pack-seam` kernel issues at ship time (§6 S1 is the authoritative bar; there is no separate "empty list" criterion).

### 3.2 D2 — Pack-registered Actions and Functions

**What ships:** Two additive `pack!` block sections — `actions!` and `functions!` — plus kernel machinery to compile, register, validate, dispatch, audit, and expose them over MCP and HTTP.

**Actions.** A pack-registered Action is a typed write with the same shape as the kernel's `CommitWrapup`: input struct, preflight, kernel validator, provenance stamp (`Provenance::Asserted{producer_kind}`), audit ledger entry, visibility check, LSN assignment, cache invalidation, SSE emission. Actions cannot bypass any of these — the framework runs them. What the pack provides is (a) the typed input struct, (b) the semantic body ("what memories and edges does this Action produce?"), (c) the visibility ceiling this Action requires from its caller (the same `REQUIRED_VISIBILITY_CEILING` associated const the kernel `Action` trait already declares), and (d) the audit label. The kernel does the rest.

Sketch, from the mortgage pack:

```rust
actions! {
    PromoteToUnderwriting(input: LoanApplicationId, min_visibility: Project) {
        // Body is a typed transform: produces new memories/edges to commit.
        // The framework provenance-stamps, audits, and emits.
        produce_memory! { type: LoanApplication, ... };
        produce_edge!   { kind: Precedes, from: ..., to: ... };
    },
    AttachRuleFinding(input: {loan: LoanApplicationId, finding: RuleFindingDraft}, min_visibility: Project) {
        produce_memory! { type: RuleFinding, ... };
        produce_edge!   { kind: Findings, from: ..., to: input.loan };
        produce_edge!   { kind: DerivedFrom, from: ..., to: input.finding.rule_id };
    },
}
```

The kernel-generated MCP tool for `AttachRuleFinding` gets a typed schema, a preflight tool, and the same rejection-code discipline as the wrapup path. Every Action produces an audit row keyed by its name.

**Functions.** A pack-registered Function is a typed read with a compiled body (Datalog fragment or Scheme program), a signature, and a latency budget in the same `P50_BUDGET_US` / `P99_BUDGET_US` shape the kernel `Function` trait already declares. The kernel dispatches it, enforces the visibility filter of the caller, and returns typed results. No LLM.

```rust
functions! {
    IsCategoricallyEligible(input: ApplicantId) -> Bool {
        body: datalog! {
            eligible(A) :- has_income(A, I), income_kind(I, K),
                           categorical_kind(K), verified(I).
        },
        slo: p50: 500us, p99: 3ms,
    },
    ExplainRuleFinding(input: RuleFindingId) -> ExplanationTree {
        body: scheme! { ... },
        slo: p50: 1ms, p99: 5ms,
    },
}
```

Callers hit these over MCP by name — `exocortex.pack.mortgage.is_categorically_eligible(applicant_id=...)`. The kernel enforces the same read-visibility filter as `search_memories`. Datalog bodies participate in the existing reasoning engine's derivation; Scheme bodies run in the existing embedded interpreter.

**Three pieces of supporting machinery this deliverable also owns**, each currently absent and each load-bearing for the acceptance criteria below:

1. **A generic Function-SLO bench harness.** Today exactly one bench consumes the kernel's declared budgets (`crates/exocortex-cache/benches/search.rs`, hand-written for one function). "Every Function's declared p99 is verified in CI" requires a harness generated from the `functions!` block, not a per-function bench written by hand.
2. **`exocortex-mcp-client --dump-tools`.** The client today has `--dump-playbook`, `--dump-block`, `--tail-audit`, and `--verify`. The tool-catalogue dump that D2's acceptance and §6 S2 both depend on does not exist and must be built.
3. **`--dump-fingerprint`.** Needed by D1's byte-stability criterion and by D5's ontology view (§9 Q1).

**Acceptance:**
- Every pack Action produces one audit row per call, keyed by `pack_name`, `verb_name`, `caller_visibility`.
- Every pack Function's declared p99 is enforced by the generated bench harness; exceeding it fails CI.
- The MCP↔HTTP parity suite (D3-S2) covers pack Actions and Functions in the same shape it covers kernel ops.
- The `visibility` ceiling declared per Action is enforced by the framework, not by pack code (kernel test: pack author cannot bypass it).
- `exocortex-mcp-client --dump-tools` lists every pack Action and Function with its typed schema.
- Preflight exists for every pack Action, generalized from the existing `PreflightWrapupOp`.

### 3.3 D3 — Seam conformance suite and inventory

**What ships:** A complete, enumerated set of seam conformance suites under `crates/exocortex-conformance/*`, plus `xtask seam-inventory` and the CI gate that makes the *set* enforceable. Three of the six seams have substantial coverage today and are extended rather than written from scratch; three are new.

**S1 — `Storage` conformance. NEW.** A shared corpus (~50 scenarios covering upsert, fenced-upsert, soft-delete, k-hop, cypher templates, stream, LSN assignment) run against **all three** implementations of the trait: `FalkorStorage`, `InMemoryStorage`, and `NoBackendStorage` (`crates/exocortex-client/src/no_backend.rs`). The third is not optional — it is the offline path, it is a full `impl Storage`, and a two-way Falkor↔InMemory corpus would not exercise it at all. Falkor scenarios stay gated on `FALKOR_URL`; the other two run everywhere.

**S2 — Operation surface parity (MCP ↔ HTTP). EXTEND.** `crates/exocortex-server/tests/http_parity.rs` already issues every registered operation over HTTP and byte-compares against the direct handler, with bearer auth asserted; `crates/exocortex-ops/tests/parity.rs` covers registry parity, schema goldens, and audit rows. The extension is pack verbs: every pack Action and Function from D2 must be enumerated by the same `entries()` walk and parity-checked identically, including rejection code, audit-row content, and visibility filtering.

**S3 — Validator agreement (kernel ↔ ingest). EXTEND.** `crates/exocortex-ingest/tests/write_path_parity.rs` is this suite: a golden verdict table run through both the kernel validator and the ingest service, asserting identical verdicts row for row. The extension is coverage of the draft shapes pack Actions can produce, so a pack cannot introduce a fourth write-path rulebook.

**S4 — Write-path agreement (online ↔ offline). EXTEND.** `crates/exocortex-client/tests/standalone_readback.rs` covers the shape. The extension is folding it into the inventory and asserting the read surface is byte-identical after drain and after boot re-seed, over the same corpus S1 uses, so the online, offline, and no-backend paths are checked against one another rather than each in isolation.

**S5 — Signing symmetry. NEW.** `exocortex-wire::signing` exposes three sign/verify pairs — `sign_batch`/`verify_signature`, `sign_registration`/`verify_registration`, `sign_invalidation_envelope`/`verify_invalidation_envelope`. Producers live in the adapter SDK, the worker, the client (`end_session`, `drain`, `sync`), and the cluster node. S5 is the full matrix: every producer signs a corpus, every verifier accepts it, and a producer added without a matrix row fails the gate.

**S6 — Kernel catalogue ↔ operation registry. NEW.** The kernel declares Actions and Functions as traits with stable `NAME` constants; `exocortex-ops` registers `OperationEntry` values via `inventory`. Nothing asserts these two catalogues agree, and they currently do not: `RetractEdge`, `GetChain`, and `ExplainEdge` are declared in the kernel with names and budgets and have no registered operation. S6 asserts a bijection — every kernel-declared verb has a registered operation and vice versa — and all three orphans are **implemented**, not removed, as part of this deliverable. `GetChain` and `ExplainEdge` because D5's provenance view depends on them (§3.5). `RetractEdge` because a store that never deletes is only trustworthy if there is a governed way to say *that was wrong*: without a registered operation, the real retraction path is a Cypher console with no provenance and no audit row, which is precisely the failure the bi-temporal design exists to prevent. The storage half already exists (`soft_delete_relationship` closes `valid_until`); what is missing is the operation between them — permission check, provenance stamp, audit row, cache invalidation. Note that `exocortex-ops/src/operations.rs:3` already documents `retract_edge` as living in that module; the comment describes an intent nobody finished.

**CI wiring:** `xtask seam-inventory` enumerates the six seams and their suites. Two failure conditions: a listed seam with no green suite, and an implementation of an already-seamed contract (a fourth `Storage`, a third operation surface, a fourth signing producer) that does not appear in the corresponding suite. This second condition is the point of the deliverable — it converts seam coverage from a thing someone remembers into a thing CI requires.

**Acceptance:**
- Six suites enumerated by `xtask seam-inventory`, all green on the runner.
- Adding a fourth `Storage` implementation without adding it to S1's corpus fails the gate. Demonstrated with a throwaway impl in a test.
- S6 reports a bijection: zero kernel-declared verbs without an operation, zero operations without a kernel declaration.
- Each of the six seams lands as a row in `docs/acceptance/section-23.tsv` with a runnable command (P7).

### 3.4 D4 — Mintlify adapter, end-to-end, on `loanlight-engineering/docs`

**What ships:** Not the adapter — that shipped at 0.1.0 and is tested against a mock server (master plan ML1). What ships is the **first real run**: the adapter deployed against the LoanLight docs repo, ingesting every page carrying an `exocortex:` frontmatter block into a running Exocortex node — visible in the object explorer (D5), searchable from an agent, and provenance-stamped as `Provenance::Extracted{producer: DocsAdapter}`.

**Concrete pipeline:**
1. Adapter reads Mintlify pages from a checkout of `loanlight-engineering/docs`.
2. Pages carrying `exocortex:` frontmatter are parsed into typed memory drafts + edges — deterministically, no LLM, per the ingestion-protocol PRD.
3. Adapter submits through the same `IngestService.Submit` path as `session-wrapup`, signs with `exocortex_wire::signing::sign_batch`, and registers as `ProducerKind::DocsAdapter`.
4. Adapter maintains a durable cursor (SDK obligation, already exercised by `crates/exocortex-adapter-sdk/tests/cursor.rs`); rerun is idempotent — same input produces same memory ids.
5. The pack it writes against is our own: kernel + `exocortex-pack-dev-v1` + `exocortex-pack-mortgage-v1` from D1. Domain types like `RuleDefinition`, `Integration`, `Workflow` come from mortgage-v1; generic types like `Task`, `Command` come from dev-v1. This is the cross-pack composition test running on real data.

**Concrete corpus (starting subset of `loanlight-engineering/docs`):**
- `portal/api-rules-authoring.mdx` and adjacent pages — bundle-identity and RuleOps mechanics → `RuleDefinition` memories with `Governs` edges to `LenderConfiguration`.
- `ops/datadog.mdx` — monitoring integration → `Technology` (dev-v1) with `Uses` edges to specific `Workflow` memories.
- Runbook pages → `Workflow` memories with `Precedes` chains.

The bar is not "ingest all 70 pages" — it is "ingest a real, non-toy subset that exercises cross-pack triples and produces something a LoanLight engineer would search for and get a useful result from."

**Acceptance:**
- The adapter runs against a real checkout and commits ≥30 memories + ≥50 edges through the live ingest path (`ProducerKind::DocsAdapter`).
- Every committed memory has `Provenance::Extracted` and an `ExternalKey` pointing back to the source page.
- Rerunning the adapter produces zero new memories (idempotency).
- A LoanLight engineer, given only the Exocortex client, can answer a real question from the graph — "what rule bundles govern `first-lender-services`?" — using only `search_memories` and `find_related`. Screenshot in the PRD closeout.
- A page tagged with a type not present in the loaded packs is rejected with a legible error, not silently accepted — the negative test that finally exercises the SDK's `RejectCode::UnknownType` class against real input.

### 3.5 D5 — Read-only object explorer

**What ships:** A minimal web UI mounted on the backend node at `/explorer`, served by the `exocortex-node` binary, behind the existing bearer-token auth. Rust + `axum` for the server side (already in the workspace), `htmx` + server-rendered HTML for the client. No SPA framework. No JS build step.

**Views:**
- **List by type.** `GET /explorer/memories?type=Fix&project=foo` → paged table of memories: title, visibility, provenance kind, LSN, created.
- **Memory detail.** `GET /explorer/memories/{id}` → the memory's fields plus its edges (outgoing + incoming, grouped by kind), each edge a link to the neighbor.
- **k-hop neighborhood.** `GET /explorer/memories/{id}/neighborhood?k=2` → the same bounded traversal `find_related` uses, rendered as a table (v1) or a small SVG (v1.1). v1 is a table.
- **Provenance trace.** `GET /explorer/memories/{id}/provenance` → the audit rows and derived-edge derivation trees that produced the memory. This is the *why does the system believe this?* surface, and it is the one view with a hard upstream dependency: it needs `GetChain` and `ExplainEdge` to be registered operations, which is D3-S6's job. If S6 lands without them, this view degrades to audit-rows-only and the loom in §6 S5 loses its best 10 seconds.
- **Audit ledger.** `GET /explorer/audit?since=...` → paged view of every action, filterable by pack, verb, actor. Wraps the existing `ListAuditRecordsOp`.
- **Ontology view.** `GET /explorer/ontology` → the loaded packs, their kinds, their type-triples, their Actions and Functions, and the composed fingerprint. This is the human-readable `--dump-playbook`. This one view alone changes the framing: users can see the ontology.

**Non-features (v1):**
- No writes. The explorer is a viewer.
- No accept/reject of Dreams proposals from the UI — v1.1, once the base has proven itself.
- No graph visualization — k-hop is a table. SVG is a v1.1 nice-to-have.
- No custom-query UI. `search_memories`, `find_related`, `get_memory`, and typed Function calls are the query surface; the explorer wraps them.

**Auth:** identical bearer-token layer to the existing HTTP registry. No new auth surface. Visibility filtering is applied at every render — the explorer sees exactly what an authenticated caller would see over MCP.

**Acceptance:**
- All six views work against a real running node with the D4 corpus ingested.
- Every render passes through the same `VisibilityContext` filter as MCP reads (regression test: a `Private` memory belonging to another user is never rendered).
- Served by the `exocortex-node` binary — no separate process, no separate deploy.
- Total incremental binary size ≤2 MB.
- Recorded loom (30s) of "type in the URL, see the LoanLight docs graph, click through provenance."

---

## 4. Kernel and API changes

The three additive changes to the kernel that D1–D5 force.

### 4.1 `pack!` block gains `actions!` and `functions!`

Both sections are optional; existing packs compile unchanged. The macro today accepts `name`, `version`, `kernel_min`, `memory_types!`, `entity_types!`, optional `computed_only_kinds!`, `kinds!`, `type_triples!`, and `crepe_rules!`; the two new sections slot in after `crepe_rules!`. Each is code-generated into a per-pack `ActionRegistry` and `FunctionRegistry`, and `Ontology::from_packs` merges them into a single per-node registry keyed by `(pack_name, verb_name)`. Fingerprint handling is **not** free here, and an earlier draft of this PRD got it wrong. It claimed signatures would be hashed and bodies would not, so bodies could be patched without a fingerprint bump. That is not achievable as the kernel stands: `OntologyFingerprint::compute` hashes `bincode::serialize(PackDef)` wholesale, so anything that lands in `PackDef` is hashed, and **adding the fields at all changes the serialization for every pack — including dev-v1 with zero declared actions**.

That is why this deliverable depends on Wave 0 (`docs/prd/ontology-compatibility-prd.md`). Once the compatibility hash covers only meaning-bearing structure, the signature-versus-body distinction becomes expressible: signatures join the compatibility hash, bodies stay out of it. Landing D2 before Wave 0 means moving the fleet's fingerprint as an unmanaged side effect of a macro change.

### 4.2 Packs declare structured agent guidance

Each pack declares an optional `guidance!` section inside its `pack!` block. Entries are keyed by the pack's own memory types and relationship kinds:

```rust
guidance! {
    RuleDefinition {
        when: "authoring or changing a lender rule",
        link: [ Governs => LenderConfiguration ],
    },
    Deposit {
        when: "categorizing a bank deposit",
        link: [ Categorizes <= TransactionCode, SupportsIncome => IncomeSource ],
    },
    Contradicts {
        caution: "never assert directly — Dreams proposes it",
    },
}
```

**This is not free text, and that is the whole point.** Every bare identifier above is a macro token resolved against the pack's own generated `MemoryType` / `EntityType` enums and kind table, exactly as the existing `type_triples!` section already resolves names at pack-def build time. A mortgage-v1 entry referencing `Fix` — a dev-v1 type — does not need a validator to catch it: it fails to compile, because `Fix` is not a variant of mortgage-v1's `MemoryType`. The composition rule P1 wants is a property of the declaration, not a check bolted onto prose.

Only `when:` and `caution:` are human text, and they deliberately name nothing checkable. Both are capped at 160 characters so the escape hatch cannot quietly become the prose blob this design exists to avoid. Guidance that is genuinely cross-cutting rather than type-keyed is out of scope for v1; if a pack needs it, that is a signal to extend the schema, not to add a paragraph.

**Rendering.** `guidance!` compiles to a per-pack `GuidanceTable`; the playbook generator renders the composed block from the kernel's guidance plus every loaded pack's table, in a stable order (alpha by pack name). This follows the pattern `agent-instructions-prd.md` §3.1 already established — the kind catalogue, type-triple examples, and reject-code table are emitted by `cargo xtask gen-playbook` between `<!-- gen:kinds -->` / `<!-- gen:rejects -->` markers with a CI gate that regenerates and diffs, while genuinely situational prose stays hand-written. Pack guidance is a new generated section under the same mechanism and the same drift gate. Note that `--dump-block` today prints a static const (`exocortex_client::playbook::BLOCK`); per-pack composition does not exist yet, so there is no legacy format to preserve.

**One inherited hazard.** That PRD's `[r3]` note records a generator reading the pack naively and emitting all 48 kinds when only 47 are assertable — the drift gate passed because the generated table matched its source and the source was wrong. Guidance generation has the same exposure: it must consult the same computed-only flag, or mortgage-v1's `NearDuplicateApplicant` will be rendered as authoring advice for a kind no producer may assert. The generator subtracts computed-only kinds; a test asserts it.

**Open risk, stated rather than hidden:** the consumer is an LLM, and hand-written prose may steer it better than rendered structure. Nobody has measured this. If the rendered block underperforms during dogfooding (step 3c), the fix is in the renderer — one change, all packs — which is precisely the flexibility prose blobs would not have given us.

### 4.3 Operation registration for pack verbs

**The registry is a trait plus `inventory`, not an enum.** `exocortex-ops` defines an `Operation` trait and an `OperationEntry` struct, collected with `inventory::collect!(OperationEntry)` and iterated via `entries()`. Both the MCP tool catalogue and the OpenAPI surface enumerate that one registry — R-P1/R-P2, "no operation exists on only one surface" — and `stdio_smoke.rs::mcp_tool_list_matches_registry` holds the MCP side to it.

`inventory` is **compile-time** registration, which forces a choice this PRD has to make rather than defer:

**Chosen: macro-generated `inventory::submit!` per pack verb.** The `actions!`/`functions!` sections expand to one `inventory::submit!` per declared verb inside the pack crate, exactly as the existing `pack!` macro already emits an `inventory::submit!` registration hook for the pack itself. Consequences, stated plainly:

- Pack verbs are known at link time. Adding an Action to a pack is a recompile and a redeploy of the node binary. This is consistent with everything else about the single-binary topology — the ontology is already compiled in — and it is not a regression.
- **One registry, one enumeration.** R-P1/R-P2 hold unchanged: MCP, OpenAPI, and D3-S2's parity walk all keep using `entries()` and pick up pack verbs for free.
- No runtime registry, no dynamic dispatch table, no second code path for "pack ops" versus "kernel ops."

**Rejected: a parallel runtime registry keyed by `(pack_name, verb_name)`,** consulted after `inventory` misses. It would allow loading packs without recompiling, and it would break the single-enumeration invariant — every surface would need to consult two registries, and the parity suite would need to know which one a given verb lives in. The invariant is worth more than the dynamism at this scale.

`OperationEntry` gains a `pack: Option<&'static str>` field so pack verbs are distinguishable for audit keying and for the explorer's ontology view. Additive; existing entries default to `None`.

**Wire.** One field added to `Ack` — `pack_verb_rejection: Option<PackVerbRejection>` — carrying pack-specific reject reasons. Additive, wire-compatible with existing clients. Full spec in the ingest proto delta.

---

## 5. Out of scope

Two different kinds of no, and the distinction matters: this project does
not defer work into a v2 that never arrives. Anything genuinely worth
doing goes into `docs/master-plan.prd` with a wave and an ID.

### 5.1 Not in this PRD — sequenced in the master plan

Each of these is accepted work with a wave, not a deferral.

- **Structured-source adapters (Iceberg / Delta / parquet).** Master plan **D1, Wave 2** — *undeferred*. The largest real gap between the README's claim and what the repo ingests. Every producer today is a coding session, a docs page, or the analytics worker.
- **Deterministic entity resolution.** Master plan **D13, Wave 2**. Resolving one real-world entity across producers by shared external identity — `ExternalKey`, typed entities as convergent join points — is what turns a collection of documents into a model of a world. It is also mostly a join, not a model. `NearDuplicateApplicant` stays a Dreams-computed proposal in mortgage-v1 until D13 lands.
- **Probabilistic entity resolution.** Master plan **D17, Wave 2**, after D13. This is the half that genuinely needs a corpus, a labelled set, and an evaluation harness — and splitting it from D13 is the point: the earlier draft deferred both together and lost the cheap half with the expensive one.
- **Org-scale (10M+) benchmarks.** Master plan **D14, Wave 4**. The 100k suite passes; 10M forces an embedding-store decision that should follow a deployment demanding it, not lead one.
- **Row-level ACLs beyond the five visibility labels.** Master plan **D15, Wave 5**. Needs an authorization DSL; five labels hold for one org.
- **Replacing positional `u8` ontology ids** with explicit or name-derived ids. Named and deliberately deferred by `docs/prd/ontology-compatibility-prd.md` §5.1 — it is the root cause of the fingerprint's strictness, and it would be the largest single change ever made to the kernel.
- **Ontology branching / PRs against the ontology.** Master plan **D16, Wave 5**. Meaningful once a third pack is proposed from outside the org; pack-version + fingerprint already gives "provably the same ontology across a cluster."

### 5.2 Not doing — design choices, not sequencing

- **A hosted SaaS.** AGPL, single-binary, self-hosted. Full stop.
- **A Foundry Workshop clone (write UI).** The explorer is read-only by P6. Writes stay in agents and adapters — that is the point of the framing, not a limitation of it.
- **Semantic search over memory content.** `search_memories` is title/tag-ranked; `SimilarTo` is Dreams-only; the `no LLM inside` invariant holds and `cargo xtask no-llm` enforces it. Anyone needing semantic search integrates an embedding store downstream of the change feed.
- **Multi-pack conflict resolution.** Two packs declaring the same memory type name is a compile error. No runtime disambiguation, ever — pack authors coordinate.
- **A user-authored transform / pipeline layer.** Data arrives through adapters and Dreams consolidates it. There is no Pipeline Builder equivalent and none is planned; transforms belong upstream in the producer.
- **Remediation work of any kind.** The codebase audit and the review rounds own defects on their own cadence (§8).

## 6. Success criteria

Numbered, checkable, and each one a row in `docs/acceptance/section-23.tsv` with a runnable command (P7).

**S1 — The pack seam is a product.** `exocortex-pack-mortgage-v1` is authored by an engineer who is not the Exocortex primary author, ships with zero kernel changes forced by the author *after* D2 lands, and composes with dev-v1 into one byte-stable fingerprint. Fewer than 3 open `pack-seam` kernel issues at ship time.
*Command:* `cargo xtask pack-acceptance --packs dev-v1,mortgage-v1`

**S2 — Actions and Functions are ontology surface.** ≥5 pack Actions and ≥3 pack Functions ship in mortgage-v1. Every one has an audit row per call, a budget enforced by the generated bench harness, an MCP↔HTTP parity assertion (D3-S2), and an entry in `--dump-tools`. A pack Action missing any of the four fails at compile time.
*Command:* `cargo xtask pack-verb-acceptance`

**S3 — The seam set is enforced.** Six conformance suites green on the runner; `xtask seam-inventory` reports exactly six; a throwaway fourth `Storage` impl added without a corpus row fails the gate; S6 reports a kernel-catalogue ↔ registry bijection with zero orphans on either side.
*Command:* `cargo xtask seam-inventory --verify`

**S4 — Documents feed the graph.** D4's adapter commits ≥30 memories + ≥50 edges from a real LoanLight docs checkout into a real node, idempotently, and a LoanLight engineer (Gregory or one other person) answers ≥2 real questions from the resulting graph using only the client tools. Recorded in the D4 closeout note.
*Command:* `cargo xtask docs-adapter-acceptance --checkout <path>`

**S5 — Users can see the ontology.** D5 renders all six views against the D4 corpus without a JS build step, behind the existing bearer-token auth, enforcing visibility filtering at every render. A 30s loom walks from `--verify` install through `/explorer/ontology` → memory detail → provenance trace → audit ledger, on real data.
*Command:* `cargo test -p exocortex-server --test explorer`

**S6 — Framing survives a hostile read.** The README's five product claims (§1 table) each have a demonstrable artifact: a second pack in the workspace, an `actions!` section in that pack, a running Mintlify adapter with commits in the audit ledger, a browseable explorer at a URL, and a fingerprint that changes when a pack is added and stays byte-stable across builds. The tier-2/3/4 gap analysis is repeated at closeout; every tier-2 item is closed or explicitly and defensibly deferred.
*Command:* manual review at closeout; the artifact list is the checklist.

---

## 7. Sequence

Ordering is by dependency and value, **not by schedule**. There are no
dates, durations, or estimates here by deliberate choice: the repo is
release-blocked on Round 6, this work does not start until that closes,
and a number in this table would be a guess dressed as a commitment. Waves
match `docs/master-plan.prd`'s sequenced backlog, where these items live
as PX1-PX6 alongside the rest of the plan.

**Wave 0 — ontology compatibility.** Owned by `docs/prd/ontology-compatibility-prd.md`, not by this PRD, and listed here because step 1a cannot start until it lands: adding `actions!` / `functions!` / `guidance!` to `PackDef` moves the fingerprint for every pack, and Wave 0 is what makes that a managed change rather than a fleet-wide surprise.

**Wave 1 — ontology verbs and modularity.** Nothing downstream reads well until this lands.

| Step | Deliverable | Owner | Depends on |
|---|---|---|---|
| **1a** | `actions!` / `functions!` / `guidance!` on the `pack!` macro; dispatch via macro-generated `inventory::submit!`; audit, MCP+HTTP mount, preflight, visibility-ceiling enforcement (D2 kernel side) | Platform | **Wave 0** |
| **1b** | D2 supporting machinery: generated Function-SLO bench harness, `--dump-tools`, `--dump-fingerprint` | Platform | 1a |
| **1c** | D3-S6 catalogue bijection — `GetChain`, `ExplainEdge`, `RetractEdge` implemented (smallest item in the PRD; `ExplainEdge` gates the explorer's provenance view) | Platform | — |
| **1d** | D3-S1 three-way `Storage` corpus + `xtask seam-inventory` | Platform | — |
| **1e** | D3-S2 extended to pack verbs; D3-S5 signing symmetry | Platform | 1a |
| **1f** | Draft `exocortex-pack-mortgage-v1` (D1) — non-primary author, from `ONTOLOGY_GUIDE.md` alone | LoanLight eng (not Gregory) | 1a, 1b |
| **1g** | D3-S3 / D3-S4 extended to pack-produced drafts; seam-inventory gate on in CI | Platform | 1d, 1e |
| **1h** | Resolve every `pack-seam` issue from 1f; count below 3 | Platform | 1f |
| **1i** | Ship mortgage-v1 v0.1.0 + composed fingerprint stability gate | Platform + LoanLight eng | 1f, 1h |

**Wave 2 — data breadth.** D4 is one producer here, not the headline; it sits beside the structured-source adapters (master plan D1) and deterministic entity resolution (D13).

| Step | Deliverable | Owner | Depends on |
|---|---|---|---|
| **2a** | Add `exocortex:` frontmatter to ~10 pages of `loanlight-engineering/docs`; reconcile the shipped adapter's v1 deviations (references→content, deprecates→tags) against mortgage-v1's types | Platform + docs owner | 1i |
| **2b** | Run D4 end-to-end against a real checkout; corpus lands in an ingest audit trail | Platform | 2a |

**Wave 3 — surface.**

| Step | Deliverable | Owner | Depends on |
|---|---|---|---|
| **3a** | Ship D5 explorer (six views, bearer auth, visibility-filtered at every render) | Platform | 1c (`ExplainEdge`), 2b (a real corpus to demo against) |
| **3b** | Demo recording; publish with the composed pack set; README rewrite | Platform | 3a |
| **3c** | Dogfood on real usage; iterate on the pack and the explorer | Platform | 3b |

The chain that actually constrains everything is Wave 0 → 1a → 1f → 1i → 2b → 3a. Every other step parallelizes against it.

## 8. Relationship to the codebase audit

The audit (`docs/bug-prd-codebase-audit.md`) is a living remediation document with its own severity ordering and its own fix cadence. Two rules, and no more:

1. **This PRD does not schedule audit work and does not restate its findings.** Defect identifiers go stale between revisions of that document; a copy of them here is guaranteed to be wrong and gates nothing. The audit's fix order is authoritative for the audit.
2. **Shipping to a real user requires the release gate of record to be green** — checked at release time, not tracked as a step here. That gate is whatever the master plan currently names: as of this writing the repo is RELEASE BLOCKED on `docs/reviews/round-6-review.prd`, and the codebase audit's own phases were closed in sweep rounds 4-5. Naming a specific document or defect set in this PRD is exactly the staleness §8 exists to prevent — read the master plan's status header instead. If the gate is red when step 3b comes due, 3b waits.

Where this PRD's work overlaps the audit's — the storage conformance suite in particular is something the audit also wants, for its own reasons — that is convergence, not a dependency. Build it once, list it in the seam inventory, and both documents are satisfied.

---

## 9. Open questions

Recorded, not blocking.

1. **Does the explorer's ontology view fingerprint the pack set inline?** Yes if it's cheap — a single `--dump-fingerprint` call under the hood, which step 1b is building anyway. The PRD assumes yes; confirm during D5.
2. **Do pack Functions need parametric visibility, or is caller-visibility sufficient?** v1 assumes caller-visibility. Revisit once a Function needs to elevate temporarily (e.g. a governance function reading across projects).
3. **Fingerprint bump policy for an Action body change vs. a signature change.** Assumption: body changes bump pack minor version but not fingerprint; signature changes bump fingerprint. Validate with the first Action body iteration.
4. **How much of the audit ledger is visible in the explorer to non-admins?** Assumption: users see their own actions plus actions on memories they can read; admins see everything. Confirm at D5.
5. **Should we ship a second Mintlify site's adapter run in v1 as further seam evidence?** Nice-to-have; blocked on a second tagged site existing.

---

## 10. Appendix A — What this PRD does NOT do to the existing PRDs

- Does **not** amend `docs/prd/exocortex-core-prd.md` — additive product scope, not a kernel rewrite.
- Does **not** replace `agent-instructions-prd.md` — the CLAUDE.md/AGENTS.md block from that PRD is kept, and pack guidance composes into it through that PRD's own `gen-playbook` mechanism (§4.2).
- Does **not** replace `docs/prd/mintlify-docs-integration-prd.md` — D4 is the *first end-to-end run* of what that PRD specified; the mechanics stay unchanged.
- Does **not** own the fingerprint. `docs/prd/ontology-compatibility-prd.md` does, and this PRD depends on it (§4.1, §7 Wave 0).
- Does **not** contradict `docs/master-plan.prd` — it feeds it. These deliverables live there as PX1-PX6, and the master plan's sequenced backlog is authoritative for ordering. Note that the structured-source adapters (iceberg/delta/parquet, master plan D1) are **no longer deferred**: they are Wave 2 work alongside this PRD's docs producer, not behind it.
- Does **not** own, restate, or schedule anything in `docs/bug-prd-codebase-audit.md` (§8).

---

## 11. Appendix B — Decision log

- **Why LoanLight's own domain for D1, not a hypothetical legal or medical pack?** Because we are the first user, we know the domain, and a second pack we do not use is a fake acceptance test. Fake acceptance is the failure mode P2 exists to prevent.
- **Why a non-primary-author authorship rule for D1?** Because "the seam works" is a claim about surface, not substrate — and surface is only proven by a second author. If Gregory writes the mortgage pack, we prove nothing beyond "Gregory can write two packs."
- **Why Actions and Functions as one deliverable, not two?** Because they share dispatch, audit, visibility, and parity machinery. Splitting them means shipping half the ontology-verb surface, which is worse than shipping none.
- **Why is pack guidance structured rather than a prose fragment?** Because the rule that a pack may only describe its own types is trivially enforceable when the names are macro tokens and nearly unenforceable when they are English. The prose version required building a text analyzer with false positives on ordinary sentences — and it would have aimed that analyzer at the one external pack author whose experience D1 exists to measure. `agent-instructions-prd.md` had already drawn this line: ontology-naming content is generated, situational prose is hand-written. Pack guidance is the former, and rev 2's first draft put it in the wrong bucket.
- **Why compile-time pack-verb registration instead of a runtime registry?** Because one registry enumerated by every surface is the invariant that makes MCP/HTTP parity checkable at all. Dynamism that costs the invariant is a bad trade at single-binary scale (§4.3).
- **Why six seams, not five?** Because the kernel's own Action/Function catalogue is a contract with the operation registry, and it is currently violated by three verbs. A seam set that omits the one seam known to be broken is decorative.
- **Why implement `RetractEdge` rather than delete it?** Because it is the only governed way to retract a belief, and an append-only store without one does not stop having retractions — it just stops recording them. The alternative retraction path is manual and invisible. It is also cheap: storage already closes `valid_until`, so this is an operation wrapper, not a feature.
- **Why extend three existing suites instead of writing six fresh ones?** Because `http_parity.rs`, `write_path_parity.rs`, and `standalone_readback.rs` already encode the right designs. Rewriting them under a new directory would discard working assertions to make an org chart tidier. The deliverable is the *inventory and the gate*, not the file layout.
- **Why an object explorer in v1, not v2?** Because the Palantir framing is *rhetorically* damaged more by "you can't see the ontology" than by any technical gap. Cheapest, highest-leverage claim-defender in the PRD.
- **Why AGPL and single-binary in the anti-scope?** Because framing drift starts here. "Personal Palantir" means self-hosted; "hosted platform" is a different product with different governance obligations.
- **Why not ship org-scale benches as a deliverable?** Because 10M-memory benches force a storage-architecture decision that shouldn't be made before a real production deployment demands it.
- **Why strip the defect lists from this document?** Because they went stale within one revision — phase membership drifted, identifiers moved, and several cited defects were fixed while the PRD still gated work on them. A capability PRD that carries a copy of a remediation backlog is wrong on a schedule. §8 replaces five paragraphs of stale detail with one release-gate condition that cannot go stale.
