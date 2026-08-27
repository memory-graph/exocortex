# Exocortex

```
              ____________________________________________________
           .-"                                                    "-.
         .'                                                        '.
        /                                                            \
       |                                                              |
       |     o------o       o------o       o------o       o------o    |
       |                                                              |
       |    .---------. --Fixes-> .---------. <-Solves- .---------.   |
       |    |   FIX   |           | PROBLEM |           | SOLUTION |   |
       |    '---------'           '---------'           '---------'   |
       |                                                              |
       |     o------o       o------o       o------o       o------o    |
       |                                                              |
        \                                                            /
         '.                                                        .'
           '-.                                                    .-'
              '--______________________________________________--'
                              ^       ||||       |
               end_session    |       ||||       |   search_memories
                              |       ||||       v
                           [ MCP over stdio ]
                Claude Code  ·  Codex  ·  Cursor  ·  any agent
```

**The open-source Palantir: the ontology, in-house.** By
[MemoryGraph](https://github.com/memory-graph).

Your coding agent starts every session from nothing. What you taught it
yesterday — the fix that worked, the decision and why, the command that
unblocked the build — is gone today, for you and for everyone else on
the team. Exocortex is where that knowledge lives instead: your agents
write the distilled outcome of every session into a typed,
provenance-stamped graph, and every later session reads it back in
microseconds.

Palantir Foundry's core product is not a data lake or a set of dashboards.
It is the *Ontology* — a governed, typed, versioned semantic layer over an
organization's knowledge, with provenance stamped on every fact. That is
the artifact that compounds: a Foundry deployment is valuable at year
three because of three years of governed Objects and Links, not any
single query.

Exocortex is that ontology as a single Rust binary, under an OSS license,
fed by your coding agents instead of a data team. Claude Code, Codex,
Cursor, any MCP client writes to it and reads from it — so what your org
learns in one session is what it knows in the next. The ontology's shape
(type-tagged memories + a typed edge vocabulary, not per-type payload
schemas) was validated by our prior memory-graph deployment; Exocortex
is that idea as one fast, governed binary.

| Foundry | Exocortex |
|---|---|
| Objects | **Memories** — typed nodes (13 types: Task, Problem, Solution, Fix, CodePattern, Command, …) |
| Links | **Typed relationships** — 48 kinds in 8 buckets (Solves, Causes, Prevents, Uses, Requires, …) |
| Actions | **Typed writes** — `end_session`, `accept_discovery`, `promote_visibility` — each provenance-producing, each audited |
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

```
session ends ──▶ agent calls end_session ──▶ typed nodes + edges stored
                                                    │
next session ◀── agent queries search/find_related ◀┘
```

The questions this answers concretely: *"What have we learned about
`payment-service`?"* · *"What's blocking `feature-X`?"* · *"What did we
try last time that didn't work?"* — answered from the graph your team
built, not from transcripts stuffed into context.

Three hard properties:

1. **Deterministic.** No LLM runs inside Exocortex, ever. All
   intelligence stays in your agent; Exocortex produces structured data
   and the same input always yields the same output.
2. **Fast.** Reads are served from a local in-memory graph — measured
   p50 of 29µs searching 100k memories, with the budget enforced in CI.
3. **Local-first.** Without `--backend`, nothing leaves your machine.
   With one, the network carries only background sync and consolidation.

## Quickstart (local MCP server)

### 1. Install

**macOS (arm64/Intel) or Linux (x64):**

```sh
curl -LsSf https://github.com/memory-graph/exocortex/releases/latest/download/install.sh | sh
```

Installs `exocortex-mcp-client` (plus `exocortex-node` and
`exocortex-worker` for team mode) into `~/.cargo/bin`. No Rust toolchain
or protoc needed.

**From source** (any platform; needs Rust 1.85+ and `protoc`):

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-mcp-client
```

or from a checkout:

```sh
git clone https://github.com/memory-graph/exocortex
cd exocortex
cargo build --release -p exocortex-client
# binary at target/release/exocortex-mcp-client
```

### 2. Register with your agent

The server speaks MCP over stdio. Add it to your agent's MCP config:

**Claude Code**

```sh
claude mcp add exocortex -- exocortex-mcp-client --org my-org --user me
```

**Codex / Cursor / any MCP client** — point the stdio server config at
the binary:

```json
{
  "mcpServers": {
    "exocortex": {
      "command": "exocortex-mcp-client",
      "args": ["--org", "my-org", "--user", "me"]
    }
  }
}
```

If you installed from a checkout, use the full path to
`target/release/exocortex-mcp-client`.

### 3. Tell your agent to use it

The client ships the instruction block — the load-bearing artifact
that rides in your agent's context on every turn:

```sh
exocortex-mcp-client --dump-block >> CLAUDE.md   # or AGENTS.md / .cursorrules
exocortex-mcp-client --verify                    # green/red install checklist
```

On first run the client also installs the full Agent Playbook at
`~/.exocortex/playbook.md` — the reference for the 47 assertable edge
kinds (the 48th, `SimilarTo`, is computed by Dreams and can never be
written), the reject-code table, and supersession rules. Harness-specific
setup (what "accepted edit" means, where the config lives, failure
modes) is in `docs/agents/claude-code.md`, `docs/agents/codex.md`, and
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

The backend registry adds governance actions — `exocortex.accept_discovery`,
`exocortex.promote_visibility`, `exocortex.list_audit_records` — over
authenticated HTTP.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--org` | `personal` | Your org id (personal use = any string). |
| `--user` | `dev` | Your user id — drives Private-memory visibility. |
| `--backend` | none | Backend URL for shared-org mode (see below). Omitted: standalone personal mode — writes land in the local WAL and are searchable immediately and across restarts (the WAL is the embedded store). |
| `--auth-token` | none | Bearer token for the backend. |
| `--hmac-key` | none | 64-hex-char producer key for backend submits. |
| `--data-dir` | OS data home | Where the offline WAL and the playbook live. |
| `--dump-playbook` | — | Print the compiled playbook and exit. |
| `--dump-block` | — | Print the CLAUDE.md/AGENTS.md instruction block and exit. |
| `--verify` | — | Green/red checklist of every client-checkable write precondition; exit code = red count. |
| `--tail-audit [--last N]` | 5 | Print the N most recent local writes (WAL), newest first. |

## The ontology

This is the product. Everything else exists to serve it.

- **Memories are Objects.** 13 memory types covering work state (Task,
  Problem, Solution, Fix, Error), code substance (CodePattern, Command,
  FileContext, Workflow), environment (Project, Technology), session
  material (Conversation), and an escape hatch (General). Each type is a
  category tag, not a payload schema — the harness writes free text, the
  envelope stays strictly typed.
- **Relationships are the reasoning surface.** 48 kinds in 8 buckets —
  Solution (Solves, Improves, Replaces), Causal (Causes, Prevents,
  Blocks, Enables), Context (Uses, Requires, Contains), and five more.
  Every edge carries strength, confidence, evidence counts, and
  bi-temporal bounds. The intelligence lives in the edges, not in the
  node payloads.
- **Entities link neighborhoods.** 12 entity types (File, Function,
  Technology, Person, Project, …) are extracted at ingest, so a Solution
  mentioning three files joins three neighborhoods without anyone
  hand-wiring edges.
- **Provenance on every fact.** Asserted / Derived / Computed /
  Extracted / Proposed / ExternalSnapshot. "Why does the system believe
  this?" is always answerable, structurally.
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
  `accept_discovery` action. Every cycle is measured against two
  independent metrics (MCR² embedding separation and graph sparsity);
  regressions are flagged and rolled back.
- **Kernel + extension packs.** The ontology kernel (types, provenance,
  visibility, actions, functions, rules) is load-bearing and universal.
  Domains ship as packs: v1 includes `exocortex-pack-dev-v1` for the
  dev loop, and a legal, medical, or sales ontology is a Rust crate
  registering its own types, kinds, and rules through the same seam.
  Packs are code, not config.

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
  dev-domain pack; a legal, medical, or sales ontology is a v2 pack —
  a crate registering against the same kernel, not a schema
  migration.

## Team mode (optional backend)

The quickstart installer already includes `exocortex-node` — if you ran
it, skip the install line. From source:

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-node
```

Run the node:

```sh
exocortex-node --mode backend-node --storage falkor://falkordb:6379 \
  --bind 0.0.0.0:8080 --bearer-token <token>
```

Then run clients with `--backend http://host:8080 --auth-token <token>`.
Every operation is also available over authenticated HTTP; the SSE
change feed keeps each client's local cache current. See the
[PRD](docs/prd/exocortex-core-prd.md) (§4 deployment, §17 tenancy) and
[Dockerfile](Dockerfile) / [compose harness](crates/exocortex-cluster/tests/docker-compose-cluster.yml)
for cluster topologies.

## Project layout

`exocortex-kernel` (ontology core, zero I/O) · `exocortex-pack-dev-v1`
(dev-domain pack) · `exocortex-wire` (protocols + signing) ·
`exocortex-storage` (FalkorDB + in-memory) · `exocortex-cache`
(lock-free read path) · `exocortex-reasoning` (Datalog + Scheme rules) ·
`exocortex-cluster` (leases, SSE feed) · `exocortex-ingest` ·
`exocortex-ops` (MCP/HTTP registry) · `exocortex-dreams` (consolidation)
· `exocortex-adapter-sdk` (external-source adapters) ·
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
