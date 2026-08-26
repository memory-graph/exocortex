# Exocortex

**A memory graph for your coding agent.** Your harness (Claude Code,
Codex, Cursor, any MCP client) forgets everything between sessions.
Exocortex is where the useful parts accumulate: what got fixed, what was
decided, what worked, what didn't — typed, searchable, and provably
deterministic. No LLM inside; all intelligence stays in your agent.

```
session ends ──▶ agent calls end_session ──▶ typed nodes + edges stored
                                                    │
next session ◀── agent queries search/find_related ◀┘
```

## Quickstart (local MCP server)

**Prerequisites:** Rust 1.85+ ([rustup](https://rustup.rs)) and `protoc`
(`brew install protobuf` / `apt install protobuf-compiler`).

### 1. Install

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

Drop this in your agent's instructions (CLAUDE.md / AGENTS.md / system
prompt):

> At the end of each session, call `exocortex.end_session` with a
> structured wrapup: memories for what was fixed, decided, blocked, or
> learned (types: Task, Problem, Solution, Fix, Error, CodePattern,
> Command, Project, Technology, …), plus optional edges between them
> (Solves, Causes, Uses, Requires, …).
>
> At the start of each session and whenever context would help, call
> `exocortex.search_memories` for what we already know (e.g. "payment
> service auth", "feature-X blockers") and `exocortex.find_related` to
> pull the neighborhood around a past memory.

That's it. Sessions now compound.

### The tools your agent gets

| Tool | What it does |
|---|---|
| `exocortex.search_memories` | Ranked free-text search over titles/tags of your org's graph. |
| `exocortex.get_memory` | Fetch one memory by hex id. |
| `exocortex.find_related` | Bounded k-hop neighborhood of a memory. |
| `exocortex.end_session` | Submit the session wrapup (1+ memory drafts, optional edges). Offline, it buffers to a local WAL and syncs later. |

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--org` | `personal` | Your org id (personal use = any string). |
| `--user` | `dev` | Your user id — drives Private-memory visibility. |
| `--backend` | none | Backend URL for shared-org mode (see below). Omitted: standalone with synthetic seed data. |
| `--auth-token` | none | Bearer token for the backend. |
| `--hmac-key` | none | 64-hex-char producer key for backend submits. |
| `--data-dir` | OS data home | Where the offline WAL lives. |

## How memory works

- **Sessions end with a wrapup.** Your agent distills the session into
  typed memories (a handful per call keeps quality high) and typed
  edges. That write is the only required habit; everything else is
  automatic.
- **Everything is typed and provenance-stamped.** Memories are nodes;
  48 relationship kinds (Solves, Causes, Prevents, Uses, …) are the
  reasoning surface. Every row carries provenance (who/what asserted
  it, or which rule derived it) and bi-temporal validity — you can
  always ask "why does the system believe this?" and "when was it
  true?"
- **Deterministic, private by default.** No LLM runs inside Exocortex.
  Reads come from the local in-memory graph (sub-millisecond p50).
  Without `--backend`, nothing leaves your machine.
- **Org memory.** Point multiple developers' clients at one backend and
  their wrapups merge into a shared graph: `Visibility` labels
  (private/project/team/org) do the isolation, and the backend
  periodically runs a consolidation pass ("Dreams") that merges
  near-duplicates and proposes — never writes — cross-domain
  connections.

## Team mode (optional backend)

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-node
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
