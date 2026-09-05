# Exocortex

![The Exocortex knowledge graph — a typed memory graph rendered as a blueprint schematic](images/hero.jpeg)

**An agent memory and knowledge system.** By
[MemoryGraph](https://github.com/memory-graph).

[![CI](https://github.com/memory-graph/exocortex/actions/workflows/ci.yml/badge.svg)](https://github.com/memory-graph/exocortex/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/exocortex-kernel)](https://crates.io/crates/exocortex-kernel)
[![Rust](https://img.shields.io/badge/rust-1.85-orange)](Cargo.toml)
[![LLMs inside](https://img.shields.io/badge/LLMs_inside-zero-deterministic)](xtask/src/main.rs)

> Your agents and tools write down what they learn. Every fact is typed,
> carries its source, and is stamped with when it was true. Nothing is ever
> deleted, so you can ask what the system knew on any past day. Rules decide
> things, not a model — the same input always produces the same answer. One
> binary, your servers, nothing leaves.
>
> **Memory is what agents write. Knowledge is what it becomes.**

```text
            session ends
                │
                ▼
        exocortex.end_session
                │
                ▼
   ┌─────────────────────────────┐        ┌─────────────────────────────┐
   │ Problem ── Fixes ──▶ Error  │        │ "What have we learned about  │
   │    ▲                       │  ◀────  │  payment-service?"           │
   │    └── Solves ── Solution  │  search │ "What did we try last time   │
   │    └── Modifies ─ File     │         │  that didn't work?"          │
   └─────────────────────────────┘        └─────────────────────────────┘
     typed nodes, typed edges,             answered from the graph your
     provenance + bi-temporal stamps       team built — not transcripts
```

## Why not just a vector store?

Every serious agent stack grows a memory layer, and the default answer
is prose in a vector store — embed the notes, recall by similarity. It
demos well and degrades from there: near-duplicates pile up, nothing in
the pile knows what anything *means*, no fact can say why it is
believed, and the more you store the worse retrieval gets. Memory that
has to compound for years cannot be a pile of prose. It has to be
structured.

Exocortex stores what your agents learn as a typed graph. Every captured
fact is a node with a type — a `Problem`, a `Fix`, a `Solution` — and every
connection is a typed, weighted edge that records who asserted it and when.
The catalogue of types, edges, and rules is called an *ontology*, and in
Exocortex it is ordinary Rust code: versioned, diffable, reviewed in a pull
request like anything else. Every read is a traversal with a microsecond
budget. That is the substrate context engineering actually needs: the
prompt gets assembled from what your org demonstrably knows — the fix, the
decision and its reason, the edge that says *this solution solves that
problem* — not from whatever a similarity search surfaced first.

Claude Code, Codex, Cursor — any MCP client writes to it and reads from
it, so what your org learns in one session is what it knows in the next.
Documents feed the same graph through
[`exocortex-adapter-mintlify`](https://github.com/memory-graph/exocortex-adapter-mintlify),
the reference ingestion adapter: pages tagged with `exocortex:` frontmatter
land as identity-stable typed memories, deterministically, with no LLM
anywhere in the loop. Git history already ingests through the same seam
(`exocortex-adapter-git`: `fix:` commits become `Fix` memories, changed
paths become identity-stable `FileContext` nodes); issue trackers and
analytics tables follow the same contract.

## Three hard properties

1. **Deterministic.** No LLM runs inside Exocortex, ever — enforced by a
   CI gate, not a promise. All intelligence stays in your agent;
   Exocortex produces structured data, and the same input always yields
   the same output.
2. **Fast.** Reads are served from a local in-memory graph — measured
   p50 of 29µs searching 100k memories, with the budget enforced in CI.
3. **Local-first.** Without `--backend`, nothing leaves your machine.
   With one, the network carries only background sync and consolidation.

And one compounding one: **the ontology is the asset.** Every write is an
audited addition to it; every read draws from it; every consolidation
cycle refines it.

## Quickstart (local MCP server)

### 1. Install

**macOS (arm64/Intel) or Linux (x64):**

```sh
curl -LsSf https://github.com/memory-graph/exocortex/releases/latest/download/install.sh | sh
```

Installs the `exocortex` entrypoint and its client/node/worker binaries into
`~/.cargo`. Linux x64 and Apple Silicon archives also include the verified
standalone Falkor runtime; macOS Intel supports client and team/backend modes
because upstream publishes no Intel embedded runtime. The bundled runtime
sets the standalone-mode floors: macOS 15+ (Apple Silicon) or glibc >= 2.38
(Linux x64); client and backend modes run on older systems. No Rust toolchain
or protoc is needed.

**Client only from Cargo** (any platform; needs Rust 1.85+ and `protoc`):

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-mcp-client
```

That command installs only `exocortex-mcp-client`; it does not install the
`exocortex` standalone wrapper or its Falkor runtime. For standalone from a
checkout:

```sh
git clone https://github.com/memory-graph/exocortex
cd exocortex
cargo build --release -p exocortex-client -p exocortex-server
EXOCORTEX_REDIS_SERVER=/path/to/redis-server \
EXOCORTEX_FALKORDB_MODULE=/path/to/falkordb.so \
  scripts/exocortex --mode mcp-standalone --org my-org --user me
```

### 2. Register with your agent

The server speaks MCP over stdio. Add it to your agent's MCP config:

**Claude Code**

```sh
claude mcp add exocortex -- exocortex --mode mcp-standalone --org my-org --user me
```

**Codex / Cursor / any MCP client** — point the stdio server config at
the binary:

```json
{
  "mcpServers": {
    "exocortex": {
      "command": "exocortex",
      "args": ["--mode", "mcp-standalone", "--org", "my-org", "--user", "me"]
    }
  }
}
```

If you installed from a checkout, use the full path to `scripts/exocortex`
and configure the source-build Falkor runtime paths described above.

### 3. Tell your agent to use it

The client ships the instruction block — the load-bearing artifact
that rides in your agent's context on every turn:

```sh
exocortex-mcp-client --dump-block >> CLAUDE.md   # or AGENTS.md / .cursorrules
exocortex-mcp-client --verify                    # green/red install checklist
```

On first run the client also installs the full Agent Playbook under the
OS data home; `exocortex-mcp-client --verify` prints its exact path. It
is the reference for the 58 assertable edge
kinds across the shipped packs (computed-only kinds like `SimilarTo` are
produced exclusively by Dreams and can never be written), the
reject-code table, and supersession rules. Harness-specific setup (what
"accepted edit" means, where the config lives, failure modes) is in
`docs/agents/claude-code.md`, `docs/agents/codex.md`, and
`docs/agents/cursor.md`.

The short version of what the block teaches: at end of every turn, run
a six-item checklist (accepted edit / non-obvious command / why-how
claim / decision-against / "remember this" / identified problem); if
anything fired, call `exocortex.end_session` with 1-5 typed drafts —
it validates locally and tells you exactly what to fix. Sessions now
compound: the backend groups every write in a conversation, near-
duplicates come back as supersession suggestions, and stale memories
are marked with their successors.

### The Functions and Actions your agent gets

| Tool | Kind | What it does |
|---|---|---|
| `exocortex.search_memories` | Function | Ranked free-text search over titles/tags of your org's graph. Superseded memories are marked with `superseded_by` and rank below their successors. |
| `exocortex.get_memory` | Function | Fetch one memory by hex id (carries `superseded_by` when superseded). |
| `exocortex.find_related` | Function | Bounded k-hop neighborhood of a memory. |
| `exocortex.end_session` | Action | Submit the wrapup (1-5 memory drafts, edges by `draft_key` or `to_memory_id`). Self-preflights locally with correction hints; the ack carries advisory `similar_to` near-duplicate suggestions. Offline, it buffers to a local WAL and syncs later. |
| `exocortex.preflight_wrapup` | Function | Validate a proposed batch without writing — same rules `end_session` and the backend enforce, with an `unverified` list of server-only checks. |
| `exocortex.playbook_version` | Function | The compiled playbook version + content hashes. |

The backend registry adds governance operations over authenticated
HTTP — `exocortex.accept_discovery`, `exocortex.promote_visibility`,
`exocortex.retract_edge`, `exocortex.resolve_contradiction`,
`exocortex.preflight_batch` (dry-run an adapter's Submit with zero
commit), `exocortex.list_audit_records`.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--org` | `personal` | Your org id (personal use = any string). |
| `--user` | `dev` | Your user id — drives Private-memory visibility. |
| `--backend` | none | Client-only backend URL for shared-org mode (see below). Omitted on the raw client: writes buffer in the offline WAL until a backend is available. The installed personal entrypoint supplies its supervised loopback backend automatically. |
| `--data-dir` | OS data home | Where the client offline WAL and playbook live. In `mcp-standalone`, `--standalone-data-dir` owns the durable Falkor graph across restarts. |
| `--dump-playbook` | — | Print the compiled playbook and exit. |
| `--dump-block` | — | Print the CLAUDE.md/AGENTS.md instruction block and exit. |
| `--verify` | — | Green/red checklist of every client-checkable write precondition; exit code = red count. |
| `--tail-audit [--last N]` | 5 | Print the N most recent local writes (WAL), newest first. |
| `--export <file>` | — | One-shot backup: dump every WAL entry (all states, LSN order) to a versioned, fingerprint-stamped JSON file. |
| `--import <file>` | — | One-shot restore: import a backup into this data-dir's WAL — all-or-nothing, fingerprint-gated, idempotent (same ids upsert; `Synced` entries never re-drain). |

`exocortex --mode mcp-standalone` is the personal persistent topology: the
wrapper supervises one loopback FalkorDB process and one node, then connects
the foreground MCP client to that node for both writes and reads. It uses an
in-process coordinator—no gossip or cluster election. Dreams counters and fire
retries use the same supervised local Redis so acknowledged writes survive a
process restart; this is durable local transport, not an inter-node service.
The client WAL is a bounded offline buffer, not the standalone database.

## The ontology

This is the product. Everything else exists to serve it.

- **Memories are the nodes.** 13 memory types in the dev pack covering work
  state (Task, Problem, Solution, Fix, Error), code substance (CodePattern,
  Command, FileContext, Workflow), environment (Project, Technology),
  session material (Conversation), and an escape hatch (General). Each type
  is a category tag, not a payload schema — the harness writes free text,
  the envelope stays strictly typed.
- **Relationships are the reasoning surface.** 48 kinds in 8 buckets —
  Solution (Solves, Improves, Replaces), Causal (Causes, Prevents,
  Blocks, Enables), Context (Uses, Requires, Contains), and five more.
  Every edge carries strength, confidence, evidence counts, and
  bi-temporal bounds. The intelligence lives in the edges, not in the
  node payloads.
- **Entities link neighborhoods.** 12 entity types (File, Function,
  Technology, Person, Project, …) are extracted at ingest, so a Solution
  mentioning three files joins three neighborhoods without anyone
  hand-wiring edges. External sources join the same way: the same table
  row arriving through two different producers converges on one identity,
  deterministically, no fuzzy matching.
- **Provenance on every fact.** Asserted / Derived / Computed / Extracted /
  ExternalSnapshot. "Why does the system believe this?" is always
  answerable, structurally.
- **Visibility is mandatory.** Private / Project / Team / Org / Public —
  no default, enforced by the type system. An org shares one graph;
  visibility is how a `Private` memory stays private inside it.
- **Deterministic reasoning.** Rules that derive facts (transitive
  closure, affinity, problem-solution bridging) are compiled Datalog;
  programs that evolve beliefs (reinforce on evidence, decay, detect
  contradictions) are embedded Scheme. No LLM, no model calls, ever.
  Explanations are structured trees; if you want prose, your agent's LLM
  renders the tree.
- **Dreams: consolidation with guardrails.** The backend periodically
  clusters regions of the graph, merges near-duplicates, prunes stale
  material, and proposes — never writes — cross-domain connections a
  single developer wouldn't spot. Accepting a proposal is the audited
  `accept_discovery` action. Every cycle is scored before and after by
  two independent metrics — **MCR²** and graph sparsity — and a cycle
  that regresses either is flagged and rolled back.

  MCR² (*rate reduction*, from Yu & Ma's group, NeurIPS 2020) answers
  one question with a number: *how many bits do the type labels save
  you?* Each memory type should occupy a tight region of embedding
  space, and the regions should stay far apart — so merging true
  duplicates tightens a type's region (score up), while over-merging
  or blur between types collapses them together (score down).

  ```
    ΔR falling — the graph collapses     ΔR rising — the graph holds

     x o . o x . o x o                    o o o    x x x    . . .
     o . x o . x o . x                    o o o    x x x    . . .
     x o . o x . o x o                    o o o    x x x    . . .

        o = Problem    x = Fix    . = Solution
  ```

  It is a diagnostic, never a training objective: no model inside
  Exocortex is optimized against it — it exists so "last night's
  consolidation left the graph better organized" is a measured claim,
  not a vibes claim. (Theory, lineage, and limits: PRD §11.)
- **Kernel + extension packs.** The ontology kernel (types, provenance,
  visibility, actions, functions, rules) is load-bearing and universal.
  Domains ship as packs: v1 ships `exocortex-pack-dev-v1` for the dev
  loop and `exocortex-pack-mortgage-v1` + `exocortex-pack-study-v1` as the
  proof a second and third domain are
  just another crate — and a legal, medical, or sales ontology is the
  same seam. Packs are code, not config — the
  [ontology development guide](docs/ONTOLOGY_GUIDE.md) covers how to
  design and write one, and pack composition carries a
  **compatibility fingerprint** so two components can prove, cheaply,
  that they agree on what the words mean.

### What Exocortex is not

- **Not a RAG system.** It stores typed, provenance-stamped structured
  data your agent reasons over — not text chunks stuffed into a prompt.
- **Not a chat log.** Capture is the distilled session outcome, not
  per-turn transcripts.
- **Not an LLM host.** No models run inside Exocortex. This is a hard
  invariant, not a scoping decision.
- **Not a black box.** Every read can be traced to the facts and rules
  that produced it, and every write is audited.
- **Not a general-purpose knowledge graph (v1).** v1 ships the
  dev-domain pack; a legal, medical, or sales ontology is a pack —
  a crate registering against the same kernel, not a schema
  migration.

## Feeding it your data (adapters)

Every external source enters through one contract — a declared
projection (what subset it may bring in, and the bounds that stop a
table from silently dominating the graph), signed registrations, a
durable cursor that only advances after a window fully settles, and a
schema-evolution policy that fails closed when a mapped column changes
underneath the mapping. Adapters validate their drafts **locally**
against the server's published, fingerprint-stamped rulebook — the same
verdicts Submit would produce, discovered before any wire traffic — and
can dry-run a whole sample with zero commit through Preflight.

- **Git history** — `exocortex-adapter-git`: runs against any local
  checkout, no auth, no network. `fix:` commits become `Fix` memories,
  changed paths become `FileContext` nodes, `Modifies` edges come from
  the one relationship git states factually. Re-runs are idempotent.
- **Docs sites** —
  [`exocortex-adapter-mintlify`](https://github.com/memory-graph/exocortex-adapter-mintlify):
  MDX + `exocortex:` frontmatter → typed memories, with a `validate`
  subcommand for docs CI.
- **Your own** — the `exocortex-adapter-sdk` crate is the whole contract:
  HMAC signing, batch splitting, backoff, cursors, reject triage, and
  the manifest interpreter, linking wire-only (no kernel).

Training pipelines get a first-class cut too: `--export-corpus` writes a
temporally-clean JSONL snapshot (everything the graph believed as of time
T — no future leakage by construction) plus a per-record lineage
manifest. The one who prepares the data is the one who cannot
hallucinate.

## Not our first memory system

Exocortex is the complete evolution of **memory-graph**, our first
memory solution — a production deployment that ran the write→read loop
daily (embedded storage, zero setup, on the order of a dozen writes a
day at its heaviest) and validated the shape everything here is built
on: type-tagged memories plus a typed edge vocabulary, no per-type
payload schemas, proven across five storage backends.

Running it also taught us what to change — and Exocortex is that change,
end to end:

- **One ontology definition; every surface generated from it.** The
  original's taxonomy drifted between its own surfaces — its session
  model lived in one API and not the other. Exocortex's kernel + packs
  generate the MCP schemas, the storage layer, and the SDK from one
  source, and drift is a CI gate instead of a lesson.
- **Governance from day one.** Provenance on every fact, mandatory
  visibility, bi-temporal validity, an audit ledger behind every
  action — enforced by the type system, not a policy layer bolted on
  after the fact.
- **Consolidation that is measured, not hoped for.** The original
  accumulated. Exocortex's Dreams cycles merge and prune under MCR² and
  sparsity guardrails, scored before and after every cycle, rolled back
  on regression.
- **MCP-native and agent-fed.** Claude Code, Codex, Cursor, any harness
  writes and reads directly — the loop that made memory-graph valuable
  is the centerline of a product any agent can use.

## Team mode (optional backend)

The quickstart installer already includes `exocortex-node` — if you ran
it, skip the install line. From source:

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-node
```

Run the node:

```sh
exocortex-node --mode backend-node --storage falkor://falkordb:6379 --allow-private-network-plaintext-data-plane \
  --bind 0.0.0.0:8080 --tls-cert /run/secrets/exocortex/tls.crt \
  --tls-key /run/secrets/exocortex/tls.key \
  --principal-policy /run/secrets/exocortex/principals.json \
  --source-policy /run/secrets/exocortex/sources.json
```

Shared/LAN binds require an operator-provided TLS certificate and key.
Remote Falkor and Dreams Redis endpoints likewise require `falkors://` and
`rediss://`. The `--allow-private-network-plaintext-data-plane` flag shown
above is only for an isolated container/private network whose transport is
secured outside Exocortex; it is not a public-network exception.
For local development only, plaintext must be explicitly enabled with
`--bind 127.0.0.1:8080 --allow-plaintext-loopback`; the node rejects that
mode for every non-loopback address.

`principals.json` is administrator-owned and fail-closed. Each row maps one
non-empty `bearer_token` to `org_id`, `user_id`, explicit `project_ids` and
`team_ids`, plus `max_visibility` (`0` private through `4` public). The same
principal protects HTTP operations, SSE, metrics, and every gRPC method; a
caller-supplied org or project/team scope never overrides this policy.

`sources.json` is also administrator-owned. Each row contains `org_id`,
`source_uri`, `producer_id`, `ceiling`, `producer_kind`, and a 64-hex
`hmac_key`. `producer_kind` is the protobuf numeric enum value (1 coding
agent, 2 research agent, 3 docs adapter, 4 analytics adapter, 5 custom,
6 extraction producer — zero and unknown values fail closed). The key is
unique producer authentication material for that exact identity; it is not
the cluster secret and cannot authenticate a different source or producer.

Back up and restore the org's durable graph (disaster recovery —
byte-faithful rows modulo storage-assigned LSNs, fingerprint-gated):

```sh
exocortex-node --mode backend-node --storage falkor://falkordb:6379 --allow-private-network-plaintext-data-plane --graph-name my-org --export-org org-backup.json
exocortex-node --mode backend-node --storage falkor://falkordb:6379 --allow-private-network-plaintext-data-plane --graph-name my-org --import-org org-backup.json
```

Set `EXOCORTEX_AUTH_TOKEN`, the exact source's `EXOCORTEX_HMAC_KEY`, and its
pre-provisioned per-client `EXOCORTEX_SSE_KEY` in the client process
environment, then run clients with `--backend https://host:8080`. Backend
nodes similarly read `EXOCORTEX_CLUSTER_SECRET`; credentials are never CLI
arguments visible in process listings.
Remote plaintext backend URLs are rejected before credentials are read or
attached; `http` is reserved for literal loopback addresses and `localhost`.
Every operation is also available over authenticated HTTP; the SSE
change feed keeps each client's local cache current. See the
[PRD](docs/prd/exocortex-core-prd.md) (§4 deployment, §17 tenancy) and
[Dockerfile](Dockerfile) / [compose harness](crates/exocortex-cluster/tests/docker-compose-cluster.yml)
for cluster topologies.

## Where this goes

Palantir Foundry's core product is not a data lake or a set of dashboards.
It is the *Ontology* — a governed, typed, versioned semantic layer over an
organization's knowledge, with provenance stamped on every fact. That is
the artifact that compounds: a Foundry deployment is valuable at year three
because of three years of governed Objects and Links, not any single query.

**Exocortex is built from the same primitives, and that part exists
today** — typed nodes and edges, provenance on every write, bi-temporal
validity, an audit ledger, deterministic reasoning, and domain models that
ship as versioned Rust crates rather than clicks in a console.

What Foundry has that Exocortex does not is breadth: hundreds of data
connectors, petabyte scale, and a surface for building applications on top.
That is a roadmap, not a rewrite, and it is written down —
[the plan](docs/master-plan.prd) sequences it wave by wave, with the
reasoning for every step and every deferral. Exocortex is the personal-
through-org tier, not the enterprise one, and the shape of the graph
(type-tagged memories plus a typed edge vocabulary, rather than per-type
payload schemas) was validated by our prior memory-graph deployment before
any of this was written.

| Foundry | Exocortex |
|---|---|
| Objects | **Memories** — typed nodes (13 types in the dev pack: Task, Problem, Solution, Fix, CodePattern, Command, …) |
| Links | **Typed relationships** — 48 kinds in 8 buckets (Solves, Causes, Prevents, Uses, Requires, …) |
| Actions | **Typed writes** — `end_session`, `accept_discovery`, `resolve_contradiction` — each provenance-producing, each audited |
| Functions | **Typed reads** — `search_memories`, `get_memory`, `find_related` — each with a latency SLO |

Every write carries **provenance** (who asserted it, or which rule derived
it) and an explicit **visibility** label. Every fact carries
**bi-temporal validity** — you can always ask "why does the system believe
this?" and "when was this true?". Governance lives in the ontology's type
system, not in a policy layer bolted on top.

**Three ways Exocortex differs from Foundry, one way it is the same:**

- **No data team required.** Foundry's ontology is expensive because a
  data team writes pipelines for months before the first Object exists.
  Exocortex's ontology starts populated the first time a harness calls
  `end_session`. The harness is the pipeline; the session wrapup is the
  transform; the ontology grows from real work.
- **Session-fed, not warehouse-fed.** Exocortex integrates with what a
  coding harness produces at session end — a small, typed, LLM-distilled
  outcome — and treats every other data source as external state behind a
  governed ingestion boundary.
- **One binary, personal and org scale.** Foundry is a hosted platform.
  Exocortex runs equally as a solo developer's local MCP server or as an
  org's networked backend cluster. Same code, same ontology, same rules;
  deployment topology is a runtime flag, never a fork.
- **Same: the ontology is the compounding asset.** Every action is an
  audited addition to it; every read draws from it; every consolidation
  cycle refines it.

## Project layout

`exocortex-kernel` (ontology core, zero I/O) · `exocortex-pack-dev-v1`,
`exocortex-pack-mortgage-v1`, and `exocortex-pack-study-v1` (domain packs) · `exocortex-wire`
(protocols + signing) · `exocortex-storage` (FalkorDB + in-memory) ·
`exocortex-cache` (lock-free read path) · `exocortex-reasoning`
(Datalog + Scheme rules) · `exocortex-cluster` (leases, SSE feed) ·
`exocortex-ingest` · `exocortex-ops` (MCP/HTTP registry) ·
`exocortex-dreams` (consolidation) · `exocortex-adapter-sdk` and
`exocortex-adapter-git` (external-source adapters) ·
`exocortex-server` / `-client` / `-worker` (binaries). All crates are on
crates.io.

## Contributing & docs

Start with [AGENTS.md](AGENTS.md) — the repo's rules, gates, and where
everything is documented: [PRD](docs/prd/exocortex-core-prd.md) (the
spec), [master plan](docs/master-plan.prd) (state),
[MILESTONE_REPORT](docs/MILESTONE_REPORT.md) (what shipped/deviated),
[reviews](docs/reviews/) (audit trails), [PUBLISHING](docs/PUBLISHING.md).

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE).
