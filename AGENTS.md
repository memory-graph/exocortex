# AGENTS.md

Working guide for coding agents (and humans) in this repository. Read
this before making changes; it encodes the project's rules and where
truth lives. It is deliberately short — the referenced documents carry
the depth.

## What this is

Exocortex is a typed, provenance-stamped memory graph for coding
harnesses, spoken to over MCP. Deterministic (no LLM anywhere in this
codebase), sub-millisecond local reads, durable at org scale. The
authoritative spec is `docs/prd/exocortex-core-prd.md` (§-references
throughout the code point into it); `docs/master-plan.prd` is the
standing worklist — accepted work items live there or they don't live.

## How work is organized

The master plan (`docs/master-plan.prd`) is the single source of truth
for what is being done. Every unit of work follows this lifecycle:

1. **Proposed** — an item enters the plan's Backlog with an ID, a
   one-line description, and a source (PRD §, review round, bug PRD).
2. **In progress** — moved to the In-progress table with an owner.
3. **Done** — moved to Done WITH closing evidence (commit sha, test
   file, or gate name) in the same commit that closes it.
4. **Deferred** — moved to the v2 table with a PRD citation; nothing is
   silently dropped.

Review rounds (`docs/reviews/round-<n>-review.prd`) audit completed
work; their findings flow back through the same lifecycle with their own
IDs. If you are asked to do something not in the plan, add it to the
plan first. If you finish something, close it in the plan in the same
commit — a stale plan is a lie about the repo.

## Layout

- `crates/exocortex-kernel` — ontology core (types, provenance,
  visibility, validators, fingerprint). **Zero I/O, no internal deps.**
- `crates/exocortex-pack-dev-v1` — the dev-domain ontology pack
  (13/12/48 + Datalog rules D1–D6). Depends only on the kernel.
- `crates/exocortex-wire` — protobuf schemas + `signing` (the ONE
  canonical checksum/HMAC implementation) + SSE/cluster envelope types.
- `crates/exocortex-storage` — the storage seam: `Storage` trait,
  registered Cypher catalogue, FalkorDB adapter, in-memory double.
  **All Cypher lives here (CR-10).**
- `crates/exocortex-cache` — lock-free local read path (ArcSwap
  snapshots, 2Q admission).
- `crates/exocortex-reasoning` — Crepe (Datalog R1–R9) + Steel (Scheme)
  engines, derived-edge writeback.
- `crates/exocortex-cluster` — leases with epoch fencing, HMAC-signed
  invalidation envelopes, the SSE change feed + replay ring.
- `crates/exocortex-ingest` — the Ingestion Protocol server.
- `crates/exocortex-ops` — the operation registry; one registration
  serves MCP and HTTP identically (CR-9).
- `crates/exocortex-dreams` — consolidation cycles (MCR², sparsity
  guardrails, rollback) and discovery proposals.
- `crates/exocortex-adapter-sdk` — the §18.2 adapter protocol core
  (handshake, signing, R-I2 splitting, backoff, durable cursor, reject
  triage). Depends on `exocortex-wire` ONLY.
- `crates/exocortex-server` / `-client` / `-worker` — the node binary,
  the MCP client binary, and the adapter host.
- `proto/` — authoritative protobuf sources. `exocortex-wire` vendors
  copies for publishable tarballs; `cargo xtask proto-sync` fails if
  they drift.
- `xtask/` — quality gates. `docs/` — spec, plan, reviews, publishing.

## Hard rules (violations are review-blockers)

1. **No LLM calls, no LLM crates, no LLM endpoints** — anywhere
   (R-D6/CR-19). `cargo xtask no-llm` greps for it.
2. **The kernel stays pure** — no internal deps, no I/O, no adapter/HTTP
   crates in its tree. `cargo xtask kernel-purity`.
3. **`exocortex-worker` and `exocortex-adapter-sdk` never link the
   kernel** (R-I1/R-I4). Wire only.
4. **Cypher never leaves `exocortex-storage`** (CR-10).
5. **One checksum/HMAC implementation** — `exocortex_wire::signing`. Do
   not hand-roll another; clients and the server must never diverge.
6. **Every write carries typed provenance; `Proposed` never persists.**
   Computed-only kinds (SimilarTo) are Dreams-exclusive — the ingest
   boundary rejects them (R-T14).
7. **External identity is raw bytes** — never `from_utf8_lossy` on a
   `table_uuid` (B8). §18.6 widths (16B uuid, 32B schema hash) are
   validated, not coerced.
8. **The PRD is authority.** Where code and PRD disagree, record the
   conflict in `docs/MILESTONE_REPORT.md` deviations rather than
   silently "fixing" the PRD (§-refs in code comments are deliberate).
9. **No new dependencies** without recording the reason where the
   workspace can see it (PUBLISHING.md keeps the list current).
10. **Owner-only writes go through fenced storage calls**
    (`upsert_batch_fenced` / `delete_memory_fenced`) — a stale lease
    must never commit (R-C3).

## Gates — run before claiming done

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --features exocortex-adapter-sdk/testing
cargo deny check
cargo xtask kernel-purity   # kernel purity + SDK single-dep + worker kernel-ban
cargo xtask fingerprint     # must be byte-stable: e1f7d17b…ddc9b2 (W6: pack now carries the R-T14 computed_only marker)
cargo xtask gen-schemas     # schema drift vs goldens
cargo xtask gen-playbook    # playbook facts drift vs pack + RejectCode; block <= 300 words
cargo xtask no-llm
cargo xtask proto-sync      # vendored wire protos match proto/
cargo xtask signing-hygiene # one batch-signing impl; no unsigned submitters
cargo xtask metrics-hygiene # authenticated metrics; bounded literal labels only
cargo xtask wire-standalone # packaged wire builds standalone
cargo xtask bench           # SLO gates (search, k-hop)
cargo xtask storage-conformance   # umbrella: in-memory targets + distinct live Falkor targets (FALKOR_URL)
cargo xtask write-path-parity     # W2  offline + ingest validators agree (golden table)
cargo xtask dead-enforcement      # §2.2 invariant/security fns have live callers
cargo xtask auth-coverage         # §2.3 detailed endpoints reject unauthenticated calls; readiness is minimal/public
cargo xtask artifact-equivalence  # §2.4 pack rules == engine; MCP result == registry
cargo xtask acceptance-coverage   # §23 requirement-to-test/gate matrix is complete and current
cargo xtask deployment-acceptance # §23 one entrypoint / every mode / all nine rules
cargo xtask ontology-surfaces     # §23 non-Rust ontology catalogues derive from the Rust pack
```

CI (`.github/workflows/ci.yml`) runs all of these on push; the toolchain
is pinned 1.85.0. Live-backend suites (`--features integration` with
`FALKOR_URL`) and the compose chaos harness
(`scripts/chaos-leader-kill.sh`) run manually/Docker.

## Conventions

- Commit messages: `area: what and why` (one line under 72 chars);
  explain the *why* in the body when non-obvious.
- One PR-sized change per commit; never amend merged history.
- Every accepted fix carries a test that fails without it. Review
  rounds (`docs/reviews/`) verify fixes AND their verify-clauses.
- Fingerprint changes mean an ontology change — if you didn't intend
  one, you broke something.
- Tests needing a backend use the InMemory double; live-Falkor tests
  are feature-gated and skip (loudly) without `FALKOR_URL`.
- Secrets live in `.env.local` (gitignored). Never commit tokens.

## Where things are documented

| Question | Answer |
|---|---|
| What should this feature do? | `docs/prd/exocortex-core-prd.md` (the §-refs) |
| How do I write an ontology pack? | `docs/ONTOLOGY_GUIDE.md` |
| What's accepted work, and in what order? | `docs/master-plan.prd` — including its PRD index |
| What was delivered/deviated? | `docs/MILESTONE_REPORT.md` |
| What did reviews find? | `docs/reviews/round-<n>-review.prd` (1, 2, 3, 6) |
| How do we publish? | `docs/PUBLISHING.md` (`scripts/publish.sh`) |
| Known bugs? | `docs/bug-prd-*.md` — these keep their `docs/` paths because code comments cite them |
| What is planned but not built? | `docs/prd/` — every feature PRD lives here; the master plan's index says which wave each is in |

**Doc conventions.** Feature PRDs live in `docs/prd/`. Bug PRDs stay at
`docs/bug-prd-*.md` because source comments reference those paths. Review
rounds are `docs/reviews/round-<n>-review.prd`. The master plan is the only
`.prd` outside `docs/reviews/`, and it indexes everything else.

The closing rule of every task: the master plan reflects reality at
every commit (see "How work is organized" above).
