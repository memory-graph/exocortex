# Exocortex — Adapter Contract PRD

**Author:** Gregory Dickson
**Status:** Draft
**Created:** 2026-08-27
**Repo:** [memory-graph/exocortex](https://github.com/memory-graph/exocortex)
**Blocks:** master plan D1 (Iceberg/Delta/parquet), D19 (SaaS APIs), D20 (CDC)
**Depends on:** `docs/prd/ontology-compatibility-prd.md` (Wave 0) for D3

---

## 0. Summary

`exocortex-adapter-sdk` already carries a real contract: registration and
identity (R-I3), HMAC signing, connected-component splitting under a byte
ceiling (R-I2), stable and monotonic batch ids, exponential backoff with
jitter, a durable cursor that advances only after a fully settled window, and
reject triage. One adapter has shipped against it and works.

That contract was written for, and proven by, a source with roughly seventy
documents. Every adapter the plan now wants — Iceberg tables, Postgres CDC,
Linear and GitHub — is one to six orders of magnitude larger, and three things
the current contract leaves undefined stop being theoretical at that size:

1. **Nothing bounds what an adapter may ingest.** There is no declared
   projection, so "ingest the table" is a legal instruction against a benchmark
   that stops at 100k memories.
2. **Ontological validation only happens at the server.** The SDK links
   `exocortex-wire` and never the kernel (R-I1/R-I4), so it catches structural
   errors locally and discovers type, triple, and ceiling errors only after
   submitting. At seventy pages that is a triage inbox; at a million rows it is
   a submit-wait-fail loop with no local answer.
3. **Source schema change has no policy.** `ExternalSnapshot.schema_hash` and
   `IngestBatch.mapping_version` model it on the wire and nothing acts on it.

This PRD closes the three, without weakening the boundary that makes adapters
safe: the SDK still never links the kernel.

---

## 1. Problem

### 1.1 What the contract already covers

Worth stating precisely, because this PRD extends rather than replaces it.
`AdapterConfig` (`crates/exocortex-adapter-sdk/src/lib.rs:56`) carries the org,
source URI, producer identity, `source_flavor` (already
`"iceberg" | "delta" | "parquet-dir" | "custom"`, §18.6), producer kind,
ceiling, credentials, `max_batch_bytes` (4 MiB default, R-I2), the durable
cursor path, and a retry policy. `BatchUnit` is one logical change unit that the
SDK splits into one or more `IngestBatch`es. `WindowOutcome` reports accepted
rows, idempotent duplicates, permanent rejections, and whether the cursor
advanced. `SdkError` enumerates every protocol-level failure, including
`Unsplittable`, `CeilingMismatch`, and `FingerprintMismatch`.

The design anticipated external table sources. What it did not anticipate is
their size.

### 1.2 Gap A — no projection contract

Nothing in registration or configuration says *what subset of the source* an
adapter is entitled to bring in. The Mintlify adapter's answer is implicit and
adequate: pages carrying `exocortex:` frontmatter, of which there are dozens. An
Iceberg adapter has no comparable natural bound. "Every row of the table" is
what a naive implementation does, and the graph is benchmarked at 100k memories
with 10M explicitly deferred (master plan D14).

The failure is not a crash. It is a graph that silently becomes mostly one
table, in which search ranking, Dreams consolidation, and k-hop traversal all
degrade — and which no operator asked for, because nobody was required to state
what they were asking for.

**A projection is not a performance tuning knob. It is part of what the adapter
is** — as much its identity as its source URI — and it belongs in registration,
in the audit ledger, and in the mapping version.

### 1.3 Gap B — validation asymmetry

`split.rs::validate_unit` already rejects malformed units before the wire:
dangling draft references, snapshot rows missing an external key. That is
**structural** validation and it is correct.

What it cannot do is **ontological** validation — is `memory_type: 7` a type the
loaded packs declare, is `(Fix, Fixes, RuleDefect)` a permitted triple, does this
draft's visibility exceed the registered ceiling. Those answers live in the
kernel, and R-I1/R-I4 forbid the SDK and the worker from linking it. So they are
discovered server-side, per batch, after submission.

At seventy documents that is a fine trade: a handful of `RejectCode::UnknownType`
rows land in `WindowOutcome.permanent_rejections` and an operator fixes the
mapping. At a million rows it is a different experience — submit, wait, learn the
mapping was wrong, correct, resubmit — with the added insult that the adapter
had every fact it needed to know locally and was structurally forbidden from
knowing it.

**The boundary is right and should not move.** What is missing is a way for the
rulebook to reach the adapter *as data* rather than as a dependency.

### 1.4 Gap C — source schema evolution

`ExternalSnapshot` carries `source_uri`, `snapshot_id`, and a 32-byte
`schema_hash`; `IngestBatch` carries `mapping_version`. The wire records the
source's schema and the mapping's version, and nothing anywhere decides what to
do when either changes.

The concrete cases, none of which have a defined answer today:

- A column is **added** to the source table, unmapped. Harmless — but nothing
  says so, so each adapter author guesses.
- A **mapped** column is removed. Every subsequent row loses a field that the
  ontology may require. Should fail closed; nothing makes it.
- A mapped column is **retyped** under the same name — `int` becomes `string`,
  or a nullable becomes non-null. The most dangerous case: rows keep arriving,
  parse "successfully," and mean something different.
- The source is **rewound** — Iceberg snapshot rollback, a CDC slot reset. The
  cursor is monotonic; the source is not.

This is the ontology-compatibility problem restated on the far side of the
boundary, and it deserves the same treatment: gate on meaning, fail closed on
anything unproven.

---

## 2. Design principles

**A1 — An adapter declares what it takes.** The projection is registered,
audited, and versioned. An adapter that has not declared one cannot submit. This
is the principle the other deliverables hang from.

**A2 — Bounded by construction, not by convention.** Every projection carries
explicit bounds. An adapter reaching a bound stops and reports; it does not
continue quietly, and it does not decide for itself that the bound was probably
fine to exceed.

**A3 — Fail as early as you can honestly fail.** Structural errors fail in the
adapter. Ontological errors fail against a rulebook the adapter can hold. Only
what genuinely depends on server state — idempotency, LSN assignment, lease
ownership — fails at the server. "Honestly" is doing work here: an adapter must
never *guess* a verdict it cannot compute, because a wrong local accept is worse
than a slow server reject.

**A4 — The kernel never enters the SDK.** R-I1/R-I4 hold unchanged. The rulebook
travels to adapters as versioned, fingerprinted **data**, produced by the server
and interpreted by the SDK. A data interpreter is not a dependency.

**A5 — Source schema change is an event, not a surprise.** Every submission
carries the source schema hash; the server compares it to the one the mapping
was registered against and applies a declared policy. Unmapped additions pass;
anything touching a mapped field fails closed until the mapping version is
raised deliberately.

**A6 — The cursor remains the only truth about progress.** Inherited from the
existing contract and restated because everything here must preserve it: a
window that did not fully settle does not advance the cursor, and nothing in
this PRD introduces partial progress.

---

## 3. Deliverables

### 3.1 D1 — Projection declaration

**What ships:** `RegisterSource` gains a projection descriptor, and
`AdapterConfig` gains the matching field. It declares:

- **Selector** — what subset of the source is in scope, in the source's own
  terms: a table plus predicate for Iceberg, a set of replication slots and
  tables for CDC, a query or label filter for a SaaS API, a frontmatter key for
  docs.
- **Field mapping** — source field to ontology type, entity, and edge kind. This
  is the artifact `mapping_version` versions.
- **Bounds** — maximum rows per window, maximum total rows per run, and a
  maximum share of the graph the adapter may come to occupy. The third is the
  one that prevents the silent-domination failure of §1.2.

The projection is stored server-side against the registered source, appears in
the audit ledger, and is part of the mapping version. Changing it is a mapping
version bump, which makes it visible and reviewable rather than a config edit
nobody sees.

**Acceptance:**
- An adapter that registers without a projection cannot submit; the rejection
  names the missing declaration.
- Exceeding a declared bound stops the window, leaves the cursor unmoved, and
  reports the bound that was hit — it does not truncate silently.
- The projection appears verbatim in the audit ledger and in
  `/explorer/ontology`'s producer view.
- A projection change without a `mapping_version` bump is rejected.

### 3.2 D2 — Adapter preflight (server-side dry run)

**What ships:** a `PreflightBatch` operation: submit a representative sample
under a real registration and receive the verdicts that a real submission would
produce, **without committing anything** — no LSN, no audit row for the data, no
cursor movement.

This generalizes the existing `PreflightWrapupOp` the same way the
palantir-expansion PRD generalizes it to `preflight_action`; the three should
share one implementation and one rejection vocabulary rather than growing three
near-identical preflight paths.

Preflight is the answer for **mapping development** — an adapter author iterates
against real verdicts in seconds. It is deliberately *not* the answer for
per-row validation at volume, because that is a round trip per row. D3 is.

**Acceptance:**
- Preflighting a sample returns byte-identical verdicts to submitting it, proven
  by a test that runs both paths over one corpus.
- Preflight commits nothing: no memory, no edge, no LSN, no cursor advance.
- Preflight is covered by the MCP↔HTTP parity suite like every other operation.

### 3.3 D3 — The rulebook as data

**What ships:** the server publishes a **validation manifest** — a compiled,
versioned, fingerprinted document containing the type-name→id maps, the kind
table, the type triples, computed-only markers, and the registered ceiling. The
SDK gains an interpreter for it and validates every draft locally before the
wire.

The manifest is stamped with the **compatibility fingerprint** from
`docs/prd/ontology-compatibility-prd.md`. That is what makes it safe: an adapter
can tell whether the manifest it holds still describes the server it is talking
to, and a stale manifest is detected rather than silently trusted. This is the
dependency that puts D3 behind Wave 0.

The SDK links no kernel code. It reads a document. If the manifest is absent,
stale, or unparseable, the adapter falls back to today's behaviour — submit and
let the server judge — rather than guessing (A3).

**Acceptance:**
- The SDK's dependency tree still shows `exocortex-wire` only; `xtask kernel-purity`'s
  SDK single-dep assertion passes unchanged.
- For a corpus containing every `RejectCode` class, local manifest validation
  and server validation return identical verdicts row for row — the same
  golden-table shape `write-path-parity` already uses for the kernel and ingest
  validators.
- A manifest whose compatibility fingerprint does not match the server's is
  refused, and the adapter degrades to server-side validation with a warning
  rather than failing the run.
- No new crate depends on `exocortex-kernel`.

### 3.4 D4 — Source schema evolution policy

**What ships:** a declared policy, enforced server-side against the
`schema_hash` carried on every batch and the schema the mapping was registered
against:

| Source change | Policy |
|---|---|
| Unmapped column added | **Accept.** Recorded in the audit ledger; no mapping bump |
| Mapped column removed | **Fail closed.** Every batch rejected until the mapping version is raised |
| Mapped column retyped | **Fail closed.** The dangerous case: rows would keep parsing and mean something else |
| Mapped column renamed | **Fail closed** — indistinguishable from removal plus addition, and guessing is how silent corruption starts |
| Source rewound (snapshot rollback, slot reset) | **Fail closed**, with a distinct reject code. The cursor is monotonic and the source is not; this needs an operator, not a retry |

**Acceptance:**
- Each row of that table has a test asserting the verdict and the reject code.
- The rewind case is distinguishable from ordinary rejection by code alone, so
  an operator can act on it without reading logs.
- Accepting an unmapped addition writes exactly one audit row.

### 3.5 D5 — Contract conformance gate

**What ships:** `cargo xtask adapter-contract`, asserting that every adapter in
the workspace, and the SDK's own test adapters, declare a projection, honour
their bounds, carry a manifest or explicitly opt out, and have a schema-policy
verdict for each row of D4's table. A new adapter that skips any of it fails
CI — the same shape as the seam inventory in the palantir-expansion PRD, and it
should be listed in that inventory rather than standing alone.

---

## 4. Out of scope

### 4.1 Not in this PRD — sequenced in the master plan

- **LLM-assisted extraction adapters.** Master plan **D23**. If that decision
  lands as "external producer," such an adapter is bound by exactly this
  contract — a projection, a manifest, a schema policy — plus a provenance
  requirement that its output be distinguishable and filterable. Nothing here
  needs to change to accommodate it, which is a point in that option's favour.
- **An adapter registry or marketplace.** Distribution is a later problem than
  correctness.
- **Per-producer HMAC keys.** A recorded deviation in the SDK today, and not
  this PRD's concern.

### 4.2 Not doing — design choices

- **Linking the kernel into the SDK or the worker.** R-I1/R-I4 are not
  negotiable; D3 exists specifically to deliver the benefit without the
  dependency.
- **Client-side verdict guessing.** An adapter with no manifest submits and lets
  the server judge. It never approximates a verdict it cannot compute (A3).
- **Partial window commits.** No adapter gets to advance a cursor over a window
  that did not fully settle, however large the window.
- **Unbounded ingest, under any flag.** There is no `--all` and no
  `--ignore-bounds`. A bound that can be waived at the command line is not a
  bound.

---

## 5. Success criteria

Each lands as a row in `docs/acceptance/section-23.tsv` with a runnable command.

**S1 — No adapter ingests undeclared.** Registration without a projection is
refused; submission beyond a declared bound halts the window with the cursor
unmoved and names the bound.
*Command:* `cargo test -p exocortex-adapter-sdk --test projection`

**S2 — Preflight and submission agree.** One corpus through both paths yields
byte-identical verdicts, and preflight commits nothing.
*Command:* `cargo test -p exocortex-ops --test preflight_parity`

**S3 — Local and server validation agree.** A corpus spanning every `RejectCode`
class validates identically against the manifest in-process and against the
server, and the SDK's dependency tree is unchanged.
*Command:* `cargo xtask adapter-contract && cargo xtask kernel-purity`

**S4 — Schema drift fails closed.** Every row of D4's table has a test; the
rewind case carries its own reject code.
*Command:* `cargo test -p exocortex-ingest --test schema_evolution`

**S5 — The contract is enforced on newcomers.** An adapter added without a
projection, bounds, or a schema policy fails CI, demonstrated with a throwaway
adapter in a test.
*Command:* `cargo xtask adapter-contract`

---

## 6. Sequence

Ordering is by dependency, not schedule. This is **Wave 2** work that gates the
adapters around it: D1, D19, and D20 in the master plan should not ship before
at least steps a and d land, or each will invent its own answer and the three
will not agree.

| Step | Deliverable | Depends on |
|---|---|---|
| **a** | D1 projection declaration: wire field, config field, server-side storage, audit, bound enforcement | — |
| **b** | D2 adapter preflight, sharing one implementation with `preflight_wrapup` and `preflight_action` | — |
| **c** | D3 rulebook-as-data manifest, published, fingerprinted, interpreted in the SDK | **OC-PRD Wave 0** (the compatibility fingerprint is what makes a manifest verifiable) |
| **d** | D4 schema evolution policy and its verdict table | a |
| **e** | D5 `xtask adapter-contract`, listed in the seam inventory | a-d |

The constraining chain is a → d → e. Step c is the highest-value item for
adapter authors and the only one with a cross-PRD dependency.

---

## 7. Open questions

1. **Where does the projection's "share of graph" bound get evaluated?** Cheapest
   at ingest against a running count; most accurate in Dreams, which already
   walks the graph. Assumption: ingest, approximate, with Dreams correcting.
2. **Should the manifest be pushed at handshake or pulled and cached?** Pull with
   a cached copy keyed by compatibility fingerprint is the assumption; handshake
   push is simpler but costs bytes on every reconnect.
3. **Does CDC need a distinct cursor shape?** A replication slot's LSN is the
   source's own progress marker and duplicating it in a file may be redundant or
   may be exactly the durable record we want. Resolve during D20, not here.
4. **Is `source_flavor` the right place to hang the schema policy**, or does the
   policy belong entirely in the projection? Assumption: the projection, with
   `source_flavor` selecting defaults.

---

## 8. Relationship to other documents

- **`docs/prd/ontology-compatibility-prd.md`** — D3's manifest is stamped with
  that PRD's compatibility fingerprint; step c waits for Wave 0.
- **`docs/prd/exocortex-palantir-expansion-prd.md`** — D2 preflight and that
  PRD's `preflight_action` are one mechanism, and D5's gate belongs in its seam
  inventory rather than beside it.
- **`docs/prd/mintlify-docs-integration-prd.md`** — the shipped adapter is the
  reference implementation and predates this contract. Bringing it into
  compliance (a declared projection over `exocortex:` frontmatter, a schema
  policy) is the migration test for everything here.
- **`docs/prd/exocortex-core-prd.md`** — §18.1, §18.2, and §18.6 define the
  adapter protocol this extends; R-I1/R-I4 define the boundary it must not
  cross.
