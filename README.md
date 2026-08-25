# Exocortex

**The Open-Source Palantir — the Ontology, in-house. A memory graph for coding agents.**

Coding harnesses (Claude Code, Codex, Cursor, custom agents) have short memories: every session starts cold, and what you taught the harness yesterday is gone today. Stuffing transcripts into context is expensive and lossy — the useful memory isn't the transcript, it's the *distilled outcome*: what got fixed, what was decided, what worked, what didn't.

Exocortex is the substrate where those outcomes accumulate. It is a typed, provenance-stamped memory graph that harnesses read and write over [MCP](https://modelcontextprotocol.io) — sub-millisecond locally, durable at org scale on a backend, consolidated over time by an event-driven dreaming cycle.

```
session ends ──▶ end_session(wrapup) ──▶ typed nodes + edges ──▶ WAL ──▶ sync to backend
                                                                    │
next session ◀── MCP query (sub-ms local cache) ◀── SSE change feed ◀┘
                                                                    │
                              Dreams cycle (merge · abstract · prune · discover)
```

## Why it's different

- **No LLM inside. Ever.** The harness has the LLM; Exocortex only produces structured data. That is what makes it deterministic, testable, and fast enough for the interactive path. Enforced by a CI gate that greps the workspace for LLM crates and endpoints.
- **The ontology is the product.** Memories are typed nodes; the reasoning power lives in 48 typed relationship kinds (8 buckets) with strength, confidence, evidence counts, and bi-temporal validity on every edge. Everything carries provenance (`Asserted` / `Derived` / `Computed` / `Extracted` / `Proposed` / `ExternalSnapshot`) — you always know *why the system believes this* and *when it was true*.
- **Personal and org scale, same binary.** A solo developer runs `mcp-standalone` (embedded storage, no cluster). An org runs `backend-node` (3+ nodes, one graph per org, `Visibility` labels do the isolation). Deployment topology is a runtime flag, never a fork.
- **Kernel + packs.** The ontology kernel is universal; domains ship as Rust extension packs. v1 includes `exocortex-pack-dev-v1` (13 memory types, 12 entity types, 48 relationship kinds). A legal or medical pack is a crate that registers against the same seam — packs are code, not config.

## Repository layout

| Crate | Role |
|---|---|
| `exocortex-kernel` | Ontology core: types, provenance, visibility, validators, fingerprint. Zero I/O. |
| `exocortex-pack-dev-v1` | The dev-domain pack (13/12/48 + Crepe rules D1–D6). |
| `exocortex-wire` | Protobuf schemas (ingest, cluster, SSE) + envelope signing types. |
| `exocortex-storage` | The one storage seam: `Storage` trait, Cypher catalogue, FalkorDB adapter, in-memory double. |
| `exocortex-cache` | Lock-free local read path: ArcSwap snapshots, 2Q admission, zero-alloc hot reads. |
| `exocortex-reasoning` | Datalog (Crepe R1–R9) + Scheme (Steel) rule engines; derived-edge writeback. |
| `exocortex-cluster` | Chubby-style leases with epoch fencing, HMAC-signed envelopes, SSE change feed. |
| `exocortex-ingest` | The Ingestion Protocol server: HMAC → fingerprint → ceiling → triples → embeddings. |
| `exocortex-ops` | Operation registry: every action registered once, served over MCP *and* HTTP identically. |
| `exocortex-dreams` | Event-driven consolidation: MCR² rate reduction, sparsity guardrails, discovery proposals. |
| `exocortex-server` | The node binary (`exocortex-node`): MCP standalone or backend cluster mode. |
| `exocortex-client` | The MCP client binary (`exocortex-mcp-client`): local cache, WAL, sync. |
| `exocortex-worker` | The adapter host: runs out-of-process adapters against `IngestService`. |

`xtask` carries the quality gates (`kernel-purity`, `fingerprint`, `gen-schemas`, `no-llm`, `bench`).

## Quick start

Requires Rust 1.85 (pinned in `rust-toolchain.toml`) and `protoc`.

```sh
cargo build --release
cargo test --workspace        # no backend needed, all green
cargo xtask bench             # SLO gates: search p50<500µs/p99<3ms, k-hop p50<300µs/p99<2ms
cargo fmt --check             # enforced in CI
cargo clippy --workspace      # enforced in CI
```

Or install the binaries straight from git (needs `protoc` on PATH):

```sh
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-node
cargo install --git https://github.com/memory-graph/exocortex --bin exocortex-mcp-client
```

### Personal mode (solo developer)

```sh
cargo run --release -p exocortex-server --bin exocortex-node -- --mode mcp-standalone
```

Boots a local node with supervised embedded storage. Point your MCP harness at the client binary, or run the MCP client directly:

```sh
cargo run --release -p exocortex-client --bin exocortex-mcp-client -- --org my-org --user me
```

The harness calls `exocortex.search_memories`, `exocortex.get_memory`, `exocortex.find_related`, `exocortex.end_session` (the session-wrapup write; offline it buffers to a local WAL and reports `{local_lsns, sync_pending}`), `exocortex.promote_visibility`, `exocortex.accept_discovery`, and `exocortex.list_audit_records`.

### Org mode (backend cluster)

```sh
docker build -t exocortex-node:local .
docker compose -f crates/exocortex-cluster/tests/docker-compose-cluster.yml up -d --build
```

Three nodes + one FalkorDB. Every operation is available over authenticated HTTP with identical outputs to the MCP surface; the SSE change feed (`/v1/changes?since_lsn=N&token=…`) keeps client caches current, with a bounded replay window and `409 Resync Required` past it. Release builds refuse to start without `--bearer-token`.

Chaos-check leader re-election (PRD bound: < 2s):

```sh
./scripts/chaos-leader-kill.sh
```

## The guarantees

| Property | How it's enforced |
|---|---|
| Deterministic, no LLM | CI gate `cargo xtask no-llm`; zero LLM crates or endpoints in the workspace. |
| Interactive latency | Bench-asserted SLOs that fail CI on regression (shared-runner tolerance ×2, clamped). |
| Ontology coherence | 32-byte fingerprint pinned in storage and verified on peer admission and every client envelope. |
| Stale-leader safety | Chubby-style leases with monotonic epochs; owner-only writes go through fenced storage calls that reject stale tokens before any row commits. |
| Bi-temporality | Two time axes on every row; a re-synced external source appends assertions, never overwrites. |
| Tenant isolation | One graph per org; visibility enforced at the storage read (`PermissionDenied`, never a silent empty result) and on the per-org audit ledger. |
| Auditability | Every action appends an immutable audit record (action, actor, org, input digest, LSN). |
| Kernel purity | CI gate: the kernel's dependency tree may not contain adapter, HTTP, or LLM crates. |

## Observability

`/metrics` (Prometheus), `/health/ready` (200 only when hydrated ∧ storage reachable ∧ reasoning alive ∧ lease fresh — failed checks are named in the 503 body), `/health/cluster`, `/health/sync`, `/health/hydration`.

## Status

v1 complete: milestones M0–M8 shipped, two review rounds closed, all gates green in CI, live FalkorDB suites and the multi-node chaos harness verified against Docker. The [PRD](docs/prd/exocortex-core-prd.md) is the source of truth; [master plan](docs/master-plan.prd) tracks state and v2 deferrals (Iceberg/Delta adapters, federation, second pack, OTel). Agents and contributors: start with [AGENTS.md](AGENTS.md); specs, reviews, and publishing live in [docs/](docs/).

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
