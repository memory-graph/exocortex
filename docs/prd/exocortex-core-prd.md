# Exocortex (by MemoryGraph) — Product Requirements Document

**Status:** Draft (reconciled — §2 is the sole authority for crate names, workspace layout, dependency pins, and kernel type definitions)
**Codename:** Exocortex (by MemoryGraph)
**Positioning:** Open-Source Palantir — the Ontology, in-house.

**Scope:** A deployable Rust binary + library providing the Exocortex ontology, memory model, linking, chaining, inference, and consolidation capabilities — usable as a local MCP server (stdio, per-developer, in-process cache) or as a networked backend cluster node (HTTP + FalkorDB + Redis coherence), with byte-identical semantics across both modes. What v1 ships is the **ontology kernel** plus one co-compiled **extension pack** (`exocortex-pack-dev-v1`, the current 13/12/48 dev-domain ontology). The kernel is the load-bearing core of a memory ecosystem; extension packs are how the ontology grows without forking the kernel.

**Non-goals:** Any LLM invocation from the Exocortex backend. Reporting or email rendering. Per-turn capture (deferred to v2). Reimplementing embedding models (pluggable, backend-side default). Migrating existing production data (separate plan). **Reading external tables (Iceberg / Delta / Parquet / S3 Tables) inside the kernel or on any interactive read path** — v1 defines the ingestion protocol; a first-party out-of-process table adapter ships in v2 (§18).

---

## 0. Introduction for a new reader

### 0.1 The problem

Coding harnesses (Claude Code, Codex, Cursor, custom agents) have short memories. Each session starts cold; what a developer taught the harness yesterday is gone today. The obvious fix — stuffing prior transcripts into context — is expensive, lossy, and gets worse as an organization scales, because a developer's useful memory isn't the transcript, it's the *distilled outcome*: what got fixed, what was decided, what worked, what didn't. There is no shared substrate today where those distilled outcomes accumulate across sessions, across developers, and across time — with the guarantees a production system needs (determinism, latency, auditability, tenancy).

Exocortex is that substrate. It is a memory graph the harness reads and writes through MCP, that runs sub-millisecond locally and durably at org scale on a backend, that captures the compact structured outcome of every session, and that periodically consolidates the graph into a smaller, denser core of durable knowledge.

### 0.2 The use case, end-to-end

A developer works with a coding harness for a session. At session end, the harness emits a **session-wrapup** — a structured JSON payload of what was learned, decided, fixed, or blocked. Exocortex stores that as a set of typed nodes and edges in the local graph. The write hits the local WAL immediately, then syncs to the backend, then fans out via a change feed to every other client subscribed to that org's graph.

On every subsequent session, the harness queries Exocortex through MCP: "what have we learned about `payment-service`? what's blocking `feature-X`? what did we try last time that didn't work?" Sub-millisecond reads from the local cache return typed, provenance-stamped memories with their inferred relationships. The harness renders those into prompts. The developer never waits on memory.

When a region has accumulated enough new material to be worth consolidating, an event-driven **Dreams cycle** runs on the backend: cluster the region by embedding similarity, merge near-duplicates, abstract common patterns into generalizations, prune stale material, and propose (but never write) cross-domain discoveries a single developer wouldn't have spotted. Triggers are write-counter-based, not clock-based (§12.2). Every consolidation is measured against a rate-reduction metric (§0.4) and a graph-sparsity metric (§11.6.1); if either regresses, the cycle is flagged and optionally rolled back.

**Personal and org scale, same product.** A solo developer runs `mcp-standalone` and gets a personal exocortex — a single-user org, embedded storage, no cluster. An organization deploys `backend-node` in cluster mode and gets a shared graph — one graph per org, `Visibility` labels do the isolation, Dreams finds patterns no single developer would see alone. The tenancy model is uniform (§17); what changes is the population count. The same Rust artifact powers both. The point of an org deployment is knowledge sharing; siloing per user defeats it. The point of a personal deployment is fast local reasoning; adding a cluster defeats it. The design serves both without forking.

**The critical property:** the harness has an LLM; at this point Exocortex does not use an LLM. All text generation happens in the harness. Exocortex only produces structured data. This is what makes it deterministic, testable, and fast enough to sit on the interactive path.

### 0.3 The ontology — what the graph actually contains

Everything Exocortex stores is typed. The **ontology** is defined once in Rust (§7) — as a **kernel** (universal types, provenance, visibility, actions, functions, storage) plus one or more **extension packs** (domain-specific memory types, entity types, relationship kinds, and Crepe rules) — and every other surface (Cypher labels, MCP schemas, SDK types) is generated from the effective ontology at compile time. **Memories are lightly structured at the content layer and heavily structured at the envelope and edge layers.** The envelope is typed and mandatory (id, type, entities, visibility, provenance, temporal bounds, importance, confidence, session/git context). The content is a free-text payload the harness produces and consumes. The reasoning power lives in the typed relationship kinds that connect memories, not in typed payload fields inside them. This shape is inherited from the prior MemoryGraph implementation, which validated it across five storage backends: the type-tag-plus-edge-ontology model is expressive enough for deterministic reasoning without imposing per-type payload schemas that the harness LLM must fill correctly.

v1 ships the kernel plus **exactly one extension pack**: `exocortex-pack-dev-v1`, the 13/12/48 dev-domain ontology. Every count below (memory types, entity types, relationship kinds) refers to that pack — the kernel itself defines the type-tag machinery, not the concrete types.

A new reader should understand three layers:

**Memory types (13 in v1).** Nodes in the graph. Each is a *category tag*, not a schema:

- *Work state:* `Task`, `Problem`, `Solution`, `Fix`, `Error` — what's being worked on, what's broken, what fixed it.
- *Code substance:* `CodePattern`, `Command`, `FileContext`, `Workflow` — the artifacts and procedures that make up the work.
- *Environment:* `Project`, `Technology` — what codebase and what stack.
- *Session material:* `Conversation` — wrap-ups and dialog fragments.
- *Escape hatch:* `General` — anything that doesn't fit; still participates in the graph via entities and edges.

**Entity types (12 in v1).** What memories are *about* — `File`, `Function`, `Class`, `Error`, `Technology`, `Concept`, `Person`, `Project`, `Command`, `Package`, `Url`, `Variable`. Entities are cross-cutting and **extracted at ingest** by the backend from `content` + `context`; a `Solution` memory can mention several entities and thereby link into different neighborhoods of the graph without the harness having to hand-wire every edge.

**Relationship types (48 kinds, 8 buckets).** This is the reasoning surface. Solution (`Solves`, `Addresses`, `AlternativeTo`, `Improves`, `Replaces`), Causal (`Causes`, `Prevents`, `Triggers`, `LeadsTo`, `Enables`, `Blocks`, `Fixes`), Context (`Uses`, `Requires`, `DependsOn`, `Contains`, `PartOf`, `InSession`, `InProject`, `WrittenIn`, `Modifies`), Learning (`Teaches`, `Demonstrates`, `Contradicts`, `Confirms`, `BuildsOn`, `Specializes`), Similarity (`SimilarTo`, `DifferentFrom`, `AnalogousTo`, `RelatedTo`), Workflow (`Precedes`, `ParallelTo`, `Executes`, `Creates`, `Configures`, `Automates`), Quality (`Validates`, `Tests`, `Measures`, `Documents`, `Verifies`), Integration (`IntegratesWith`, `Consumes`, `Produces`, `Exposes`, `Wraps`, `Bridges`). The authoritative table — buckets, inverses, bidirectionality, default strengths, kernel-constant bindings — is the `kinds!` block in §7.18. Inverse labels (`SolvedBy`, `Generalizes`, `HasPart`, …) are auto-registered companions (R-T4), not counted in the 48. Every edge carries `strength`, `confidence`, `evidence_count`, `validation_count`, `counter_evidence_count`, and bi-temporal fields — the intelligence lives in the edge properties, not in the node payload.

Every memory and every relationship carries **provenance** (`Asserted` / `Derived` / `Computed` / `Extracted` / `Proposed` / `ExternalSnapshot`) and **bi-temporal fields** (`valid_from`, `valid_to`/`valid_until`, `recorded_at`, `invalidated_by`). Provenance tells you *why the system believes this*; bi-temporal fields tell you *when it was true and when the system learned it*. Discovery proposals are `Provenance::Proposed` — they are structured suggestions and cannot be persisted as edges until explicitly accepted. `ExternalSnapshot` (§7.9) tags every memory that entered through the Ingestion Protocol from an external source — an Iceberg / S3 Tables / Delta adapter or a custom feed — with the source snapshot coordinates so bi-temporality survives source mutation.

The v1 ontology (dev-v1 pack) is deliberately dev-domain-shaped. Because packs are the seam, a legal or sales or medical pack is a Rust crate that registers its own types, kinds, and rules against the same kernel — not a general-purpose two-tier core rewrite of the kernel. See §7.16 for the roadmap.

**Session semantics in the unified graph.** The prior taxonomy model treated `conversation` as an MCP-only type that did not exist in the API model (its Drift D-3). The unified v1 pack resolves this: a **session is a `Conversation` memory** — one per `end_session` call, carrying the session's context — that groups the wrapup's other memories. The kernel-constant `IN_SESSION` binds this: `InSession(_, Conversation)` is the grouping edge, and pack rule D6 (`session_cohort`) rides it for MCR² session cohorts. There is no separate session object type.

### 0.4 The techniques

Six load-bearing techniques a new reader should recognize before diving into requirements.

**1. Deterministic reasoning in two languages.** No LLM in the backend. Rules that derive facts from other facts (transitive closure, type inference, affinity, problem-solution bridging) are written in **Crepe** — a compile-time Datalog embedded in Rust. Programs that evolve beliefs over time (reinforce on repeated evidence, decay unused edges, detect contradictions, emit explanation traces) are written in **Steel** — an embedded Scheme. Cypher is used only as a storage query language, never for reasoning. Explanations are structured trees; if the harness wants prose, its LLM renders the tree (§10).

**2. Two-tier topology with byte-identical semantics.** A single Rust artifact runs in two modes: as a **local MCP server** (stdio, per-developer, in-memory ArcSwap cache, WAL for offline writes, SSE subscriber to the backend) and as a **backend cluster node** (HTTP, cluster coordination, FalkorDB-backed durable storage). The local mode is not a subset of the backend — it's a replica. The same rules, the same ontology, the same operations, the same results. Topology decides *where owner-only work runs*, never *what capabilities exist* (§4).

**3. Latency invariants as hard constraints.** Interactive reads hit sub-millisecond p50 and single-digit-millisecond p99. This is enforced in CI, not aspirational. The design pays for that with a local cache (ArcSwap for lock-free reads), Crepe compile-time rule evaluation with k=3 fact scoping on the interactive path, no network I/O on the hot path, and pushing every expensive operation (MCR² computation, HDBSCAN clustering, consolidation) into the Dreams cycle on the owner backend node (§15).

**4. Consolidation with two mathematical guardrails.** The Dreams cycle merges, abstracts, prunes, and strengthens. Because those operations can silently degrade the graph, every cycle is measured before and after with two independent metrics:

- **MCR² (Maximal Coding Rate Reduction)** — `ΔR = R(Z) − R^c(Z|Π)` from Yu et al. (NeurIPS 2020). Measures whether the embedding space is class-separated: are `Decision` memories geometrically distinct from `Problem` memories in embedding space? A cycle that drops ΔR by more than tolerance is flagged as a regression (R-Mcr3). See §11 for full theory; §11.9 discusses the field's evolution since 2020 and why v1 uses MCR² as a diagnostic rather than a training objective.

- **Graph sparsity** — hairball fraction, average out-degree, per-type median out-degree, confidence-weighted density per cluster. Measures whether the graph is *dense* in a bad way (every memory linked to every other in its cluster). A cycle that drives the hairball fraction up by more than tolerance is flagged as a regression (R-Mcr6). See §11.6.1.

Both metrics run inside the Dreams cycle, both stamp `ConsolidationResult`, both obey the same rollback flag. Together they catch failure modes that either would miss alone.

**5. Org-scoped tenancy in one graph.** An organization gets exactly one graph. Every memory and every relationship carries an explicit **Visibility** (`Private` / `Project` / `Team` / `Org` / `Public`); the type system requires the field, there is no default, and violations return `PermissionDenied` rather than silently filtering. Consolidation and Dreams cycles operate on **regions** keyed by `(project_id, memory_type)` — each region has its own owner lease, its own MCR² score, and its own sparsity score. Cross-region reconciliation is a separate bounded pass. The point is deliberate: siloing per user defeats the value of an org deployment, but leaking a `Private` memory across users defeats the trust model. Visibility is how both are true at once (§17).

**6. One deliberate seam.** Exocortex has exactly one architectural pluggability point: the database adapter (§6). Everything else — coherence transport, coordination store, embedding runtime, rule authoring, retrieval mechanism — is direct, single-implementation, and shipped with the binary. Traits exist for **testability**, not for hypothetical portability. Storage is FalkorDB. Coherence is Redis pub-sub (same instance as FalkorDB). Leases live in Redis. The embedding runtime is `fastembed` + `bge-small` on the backend. Rules are Crepe, compile-time. Retrieval is graph traversal; embeddings and any ANN operations are offline-only inside Dreams and ingest enrichment. When a real reason to change any of these dependencies appears, we port then; we do not pay design cost now for changes that may never come. Portability of the storage layer is a free side effect of the adapter, not the reason it exists.

### 0.5 What Exocortex is not

To save the new reader time:

- **Not a RAG system.** RAG retrieves text chunks and stuffs them into a prompt. Exocortex stores typed, deterministic, provenance-stamped structured data that a harness *reasons over*. The harness may of course use Exocortex output inside a RAG pipeline; that's the harness's job.
- **Not a chat log.** Session capture is the wrap-up outcome, not per-turn transcripts. Per-turn capture is deferred to v2.
- **Not an LLM host.** No models run in the Exocortex backend. Ever. This is a hard invariant, not a scoping decision (R-D6, CR-19).
- **Not a general-purpose knowledge graph in v1.** The v1 ontology ships the dev-v1 pack. A legal, medical, or sales ontology is a v2 pack — a Rust crate registered against the same kernel — not a schema-migration of the dev-v1 types.
- **Not a training pipeline.** Embeddings are pluggable and frozen in v1. Fine-tuning embeddings on the org graph (via CgMCR² or similar) is the v2 north-star (§24 open question 13) but not shipping now.

### 0.6 How to read the rest of this document

- **§1** — what the product is. Read first.
- **§2–§3** — the implementation ground truth: the Cargo workspace an implementer starts from, and the layered milestones (M0–M8) that a coding agent walks in order. Every later section is the *content* of one or more milestones. Read this pair before opening an editor.
- **§4–§5** — deployment topology and the interactive read/write architecture. Read if you're integrating or operating.
- **§6** — the `Storage` trait and FalkorDB adapter. Read for M2.
- **§7** — the ontology: kernel, extension packs, Actions, Functions, Provenance, Visibility, type-triple rules, ingestion protocol, compounding-asset thesis. Read for M1; re-read whenever you author a pack or a rule.
- **§8–§9** — cache and cluster coordination. Read for M3 and M5.
- **§10** — the reasoning layer (Crepe + Steel). Read for M4.
- **§11–§12** — MCR² theory and the Dreams consolidation cycle. Read for M8.
- **§13–§14** — session capture and scoring. Read for M6.
- **§15–§17** — latency budget, resource envelope, personal + org tenancy. Read while operating a backend or writing perf CI.
- **§18** — external data sources: the ingestion protocol wire format, the adapter contract, and the v2 first-party Iceberg / S3 Tables worker. Read for M6 and every v2 adapter (M9+).
- **§19–§24** — observability, security, operation registry (M7), correctness invariants, success criteria, open questions. Read as the acceptance-criteria layer.

---

## 1. Product Vision

Exocortex is the **Open-Source Palantir** — the Ontology, in-house.

Palantir Foundry's core product is not a data lake or a warehouse or a set of dashboards. It is the *Ontology*: a governed, typed, versioned semantic layer over an organization's knowledge, with Actions that write into it, Functions that read from it, and Provenance stamped on every fact. Analysts don't reason over rows and tables — they reason over Objects, Links, Actions, and Functions. That is the shape of the product an org licenses when it buys Foundry, and it is the shape of the artifact that compounds in value over years of use.

Exocortex is that ontology, deployable as a Rust binary, running under an OSS license, feeding the same kinds of Actions and Functions to coding harnesses that Foundry feeds to analysts. **Objects are Memories.** **Links are typed Relationships (48 kinds, 8 buckets).** **Actions are typed writes** — `commit_wrapup`, `accept_discovery`, `promote_visibility`, `retract_edge` — each with a schema, a Provenance-producing effect, and a visibility contract. **Functions are typed reads** — `search_memories`, `traverse_relationships`, `get_chain`, `explain_edge` — each with a latency SLO and a `snapshot_version` stamp. Every write carries typed **Provenance**; every memory and every relationship carries typed **Visibility**; every fact carries **bi-temporal validity**. Governance is not a wrapper library, it lives inside the ontology.

Three ways Exocortex is different from Foundry, and one way it is the same:

- **Different: no data team required.** Foundry's Ontology is expensive to build because a data team writes pipelines and mapping rules for months before the first Object exists. Exocortex's ontology starts populated the first time a coding harness calls `end_session`. The harness is the pipeline. Session-wrapups are the transform. The ontology grows from real work, not from a project plan.
- **Different: session-fed, not warehouse-fed.** Foundry integrates with warehouses, files, and databases first-class through Data Connection. Exocortex integrates with what a coding harness produces at session end — a small, typed, LLM-distilled outcome — and treats every other data source as *external state on the far side of a governed ingestion boundary* (§18). This is a scope choice, not a limitation.
- **Different: single Rust binary, personal and org scale.** Foundry is a hosted platform. Exocortex is a Rust artifact that runs equally as a solo developer's local MCP server (`mcp-standalone`) or as an org's networked backend cluster. Same code, same ontology, same rules. Deployment topology is a runtime flag, never a codebase fork.
- **Same: the ontology is the compounding asset.** The value of an Exocortex deployment at year three comes from three years of typed, provenance-stamped, bi-temporally-valid Objects, Links, Actions, and Discoveries — not from any single query, cycle, or feature. Every Action is an audited addition to the asset. Every Function reads from the asset. Every Dreams cycle refines it. This is the Foundry thesis, transported to open source and to the developer's inner loop.

Three hard properties we hold ourselves to:

1. **Deterministic.** No LLM in the Exocortex backend, ever. Same input → same output, always. Fully testable with fixtures. Governance and provenance are enforced by Rust's type system and by the storage adapter, not by a policy layer bolted on top.
2. **Fast.** Interactive reads clear in sub-millisecond p50 and single-digit-millisecond p99. Enforced in CI. The developer never waits on the memory system.
3. **Local-first for reads.** The graph is in memory on the developer's machine. Network is only for background sync and cross-machine consolidation.

**Kernel + extension packs.** v1 ships two things: the **ontology kernel** (types, provenance, visibility, actions, functions, rule graph, storage seam, ingestion protocol) and one co-compiled **extension pack** (`exocortex-pack-dev-v1`, the current 13 memory types / 12 entity types / 48 relationship kinds). Future packs — legal, medical, sales, security — are Rust crates that register additional types, relationship kinds, and Crepe rules through the same seam that dev-v1 uses. Packs are code, not config. Details in §7.

**Guiding invariant:** every capability works identically on the local MCP client and on the backend cluster. The client is not a subset — it's a replica. Deployment topology determines where owner-only work runs, never what capabilities exist.

---

## 2. Workspace and Crate Layout

This section pins the ground-truth Cargo workspace an implementer starts from. Everything else in the PRD assumes these crate names, dependency directions, and feature flags. A coding agent should be able to `cargo new` this workspace and have it compile empty before writing any real logic.

**Authority rule:** if any other section of this PRD disagrees with §2 on crate names, workspace layout, dependency pins, or kernel type definitions, **§2 wins**. Later sections own semantics and behavior — never identity. In particular, the kernel source files in §2.6.1 are the single definition of `Memory`, `Relationship`, `Provenance`, `Visibility`, `RelKindId`/`RelMeta`, `MemoryDraft`, the `Ontology` assembly, and the fingerprint; §7 explains what they mean and does not restate them.

The invariant that shapes this layout: **`exocortex-kernel` must not link `iceberg-rust`, `delta-rs`, `duckdb`, `aws-sdk-*`, or any LLM client** (R-I1, R-I5, CR-19, CR-26). Adapters and workers live in separate crates that link the kernel one-way.

### 2.1 Workspace tree

```
exocortex/
├── Cargo.toml                      # workspace root
├── rust-toolchain.toml             # pinned toolchain
├── deny.toml                       # cargo-deny — enforces R-I1/R-I5/CR-19
├── crates/
│   ├── exocortex-kernel/           # (M0) types, macros, validators, no I/O
│   ├── exocortex-pack-dev-v1/      # (M1) the v1 pack: 13 MT × 12 ET × 48 kinds
│   ├── exocortex-wire/             # (M0) protobuf + tonic stubs
│   ├── exocortex-storage/          # (M2) Storage trait + FalkorDB adapter
│   ├── exocortex-cache/            # (M3) ArcSwap graph, 2Q, per-user views
│   ├── exocortex-reasoning/        # (M4) Crepe rules + Steel embedding
│   ├── exocortex-cluster/          # (M5) leases, gossip, SSE, HMAC
│   ├── exocortex-ingest/           # (M6) IngestService impl (server-side)
│   ├── exocortex-worker/           # (M6+) adapter host process
│   ├── exocortex-server/           # (M5) `exocortex-node` binary
│   ├── exocortex-client/           # (M3) `exocortex-mcp-client` binary
│   ├── exocortex-ops/              # (M7) operation registry, MCP+HTTP codegen
│   └── exocortex-dreams/           # (M8) consolidation loop, MCR² engine
├── packs/                          # v2+ packs live here as tenants copy the layout
├── proto/                          # .proto files consumed by exocortex-wire build.rs
│   ├── ingest.proto
│   ├── cluster.proto
│   └── sse.proto
└── xtask/                          # cargo xtask commands: fingerprint, gen-schemas, bench
```

### 2.2 Workspace `Cargo.toml`

```toml
# Cargo.toml (workspace root)
[workspace]
resolver = "2"
members = [
    "crates/exocortex-kernel",
    "crates/exocortex-pack-dev-v1",
    "crates/exocortex-wire",
    "crates/exocortex-storage",
    "crates/exocortex-cache",
    "crates/exocortex-reasoning",
    "crates/exocortex-cluster",
    "crates/exocortex-ingest",
    "crates/exocortex-worker",
    "crates/exocortex-server",
    "crates/exocortex-client",
    "crates/exocortex-ops",
    "crates/exocortex-dreams",
    "xtask",
]

[workspace.package]
version      = "0.1.0"
edition      = "2021"
rust-version = "1.83"
license      = "Apache-2.0"
repository   = "https://github.com/memorygraph/exocortex"

[workspace.dependencies]
# --- runtime & primitives ---
tokio          = { version = "1.40", features = ["full"] }
tokio-stream   = { version = "0.1" }
futures        = "0.3"
async-trait    = "0.1"
bytes          = "1"
thiserror      = "1"
anyhow         = "1"
tracing        = "0.1"
tracing-subscriber        = { version = "0.3", features = ["env-filter", "json"] }
tracing-opentelemetry     = "0.24"
opentelemetry             = "0.23"
opentelemetry-otlp        = { version = "0.16", features = ["tonic"] }

# --- data structures ---
arc-swap  = "1.7"
dashmap   = "6"
petgraph  = "0.6"
smallvec  = "1"
smol_str  = "0.3"
lasso     = { version = "0.7", features = ["multi-threaded"] }
roaring   = "0.10"
indexmap  = "2"

# --- serde & wire ---
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"
schemars    = { version = "0.8", features = ["derive"] }
bincode     = "1.3"
prost       = "0.13"
prost-types = "0.13"
tonic       = { version = "0.12", features = ["tls", "gzip"] }
tonic-build = "0.12"

# --- hashing / crypto ---
blake3      = "1.5"
sha2        = "0.10"
hmac        = "0.12"

# --- storage / http ---
falkordb    = "0.1"          # official FalkorDB Rust client
redis       = { version = "0.27", features = ["tokio-comp", "aio"] }
sled        = "0.34"
axum        = { version = "0.7", features = ["macros", "http2"] }
tower       = "0.5"
tower-http  = { version = "0.6", features = ["trace", "cors", "compression-full"] }
eventsource-client = "0.13"

# --- reasoning ---
crepe       = "0.1"
steel-core  = "0.6"

# --- MCP / ops ---
rmcp        = "0.1"
inventory   = "0.3"

# --- embedding runtime (backend default) ---
fastembed   = "3"

# --- time / rand ---
chrono      = { version = "0.4", features = ["serde"] }
chrono-tz   = "0.9"
uuid        = { version = "1", features = ["v4", "v7", "serde"] }
rand        = "0.8"

# --- cluster / gossip ---
chitchat    = "0.9"

# --- misc runtime ---
once_cell   = "1"
parking_lot = "0.12"
lru         = "0.12"
async-stream = "0.3"
http        = "1"
subtle      = "2"

# --- observability ---
metrics                     = "0.23"
metrics-exporter-prometheus = "0.15"

# --- testing ---
proptest    = "1"

# --- internal ---
exocortex-kernel      = { path = "crates/exocortex-kernel" }
exocortex-pack-dev-v1 = { path = "crates/exocortex-pack-dev-v1" }
exocortex-wire        = { path = "crates/exocortex-wire" }
exocortex-storage     = { path = "crates/exocortex-storage" }
exocortex-cache       = { path = "crates/exocortex-cache" }
exocortex-reasoning   = { path = "crates/exocortex-reasoning" }
exocortex-cluster     = { path = "crates/exocortex-cluster" }
exocortex-ingest      = { path = "crates/exocortex-ingest" }
exocortex-ops         = { path = "crates/exocortex-ops" }
exocortex-dreams      = { path = "crates/exocortex-dreams" }
exocortex-worker      = { path = "crates/exocortex-worker" }
exocortex-server      = { path = "crates/exocortex-server" }
exocortex-client      = { path = "crates/exocortex-client" }

[profile.release]
lto            = "fat"
codegen-units  = 1
panic          = "abort"
debug          = 1

[profile.bench]
inherits = "release"
debug    = 2
```

### 2.3 Toolchain

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.83.0"
components = ["rustfmt", "clippy", "rust-src"]
profile = "minimal"
```

### 2.4 `cargo-deny` policy — enforces the kernel-purity invariants

```toml
# deny.toml — the CI barrier that keeps R-I1 / R-I5 / CR-19 / CR-26 real.
[bans]
multiple-versions = "warn"
wildcards         = "deny"

# The kernel must not transitively depend on adapter, LLM, or aws crates.
# Enforced by a xtask that runs `cargo tree -p exocortex-kernel -e no-dev`
# and greps the output. `deny.toml` is the secondary defence.
[[bans.deny]]
name = "duckdb"
[[bans.deny]]
name = "iceberg"
[[bans.deny]]
name = "delta_kernel"
[[bans.deny]]
name = "deltalake"
[[bans.deny]]
name = "aws-sdk-s3"
[[bans.deny]]
name = "aws-sdk-glue"
[[bans.deny]]
name = "async-openai"
[[bans.deny]]
name = "anthropic-sdk"
# `reqwest` is NOT banned globally — HTTP clients are legitimate in server and
# client crates. Keeping it out of `exocortex-kernel` specifically is enforced
# by `cargo xtask kernel-purity` (§2.7).

[licenses]
unlicensed  = "deny"
allow       = ["Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Zlib", "Unicode-DFS-2016"]
```

### 2.5 Dependency graph (allowed edges only)

```
                        exocortex-kernel
                              ▲
     ┌────────────┬───────────┼──────────┬──────────────┐
     │            │           │          │              │
 pack-dev-v1   wire        storage     cache        reasoning
                              ▲          ▲              ▲
                              └────┬─────┘              │
                                   │                    │
                                cluster ────────────────┤
                                   ▲                    │
                                   │                    │
                                  ops ──────────────────┤
                                   ▲                    │
                          ┌────────┴─────────┐          │
                          │                  │          │
                        client            server        │
                                            ▲           │
                                            └──── dreams┘

  ingest depends on: kernel, wire, storage, cache, reasoning
  worker depends on: kernel, wire (uses ingest via tonic — never links kernel from an adapter)
```

Rules for edges:

- **Every edge points up.** No crate depends on `exocortex-client`, `-server`, `-worker`, or `-dreams`.
- **`exocortex-kernel` has no reverse edges.** Only leaf crates depend on it.
- **`exocortex-worker` does not link `exocortex-kernel`.** It links `exocortex-wire` only (protobuf types) and talks to `exocortex-ingest` over the network. This is what R-I1 protects.
- **`exocortex-pack-dev-v1` depends only on `exocortex-kernel`.** Packs never depend on storage, cache, cluster, or ops.

### 2.6 Per-crate `Cargo.toml` and `lib.rs` skeletons

The rest of this section gives every crate's Cargo manifest and a compilable `lib.rs` stub. Function bodies are `todo!()` where later sections specify them; the workspace should compile end-to-end after this scaffolding.

#### 2.6.1 `exocortex-kernel`

```toml
# crates/exocortex-kernel/Cargo.toml
[package]
name    = "exocortex-kernel"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde       = { workspace = true }
serde_json  = { workspace = true }
bincode     = { workspace = true }
schemars    = { workspace = true }
smol_str    = { workspace = true }
smallvec    = { workspace = true }
thiserror   = { workspace = true }
tracing     = { workspace = true }
chrono      = { workspace = true }
uuid        = { workspace = true }
blake3      = { workspace = true }
sha2        = { workspace = true }
inventory   = { workspace = true }
lasso       = { workspace = true }
crepe       = { workspace = true }

[features]
default = []
# no default features; the kernel does zero I/O
```

```rust
// crates/exocortex-kernel/src/lib.rs
//! Exocortex kernel — universal ontology machinery.
//!
//! The kernel defines the shape of a `Memory`, a `Relationship`, and the rules
//! by which one may enter the graph. It defines *no* concrete `MemoryType`,
//! `EntityType`, or named relationship kind — those come from packs
//! (see `exocortex-pack-dev-v1`). See PRD §7.
//!
//! # Invariants enforced here
//! - R-Pk1..R-Pk5 pack constraints (see `pack::registry`)
//! - R-T1..R-T18a memory/relationship rules
//! - R-I1..R-I7 ingestion protocol invariants (validator half — the wire half
//!   lives in `exocortex-wire`)
//! - CR-19: no LLM. Depend on this crate transitively at your peril if you
//!   want to add one.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod ids;
pub mod visibility;
pub mod provenance;
pub mod kinds;
pub mod memory;
pub mod relationship;
pub mod entity;
pub mod draft;
pub mod pack;
pub mod ontology;
pub mod validator;
pub mod actions;
pub mod functions;
pub mod fingerprint;
pub mod error;

pub use error::{KernelError, KernelResult};
pub use fingerprint::OntologyFingerprint;
pub use ids::{MemoryId, RelationshipId, EntityId, PackId, LSN};
pub use kinds::{RelKindId, RelBucket, RelMeta};
pub use memory::{Memory, MemoryContext};
pub use relationship::{Relationship, RelationshipProperties};
pub use provenance::{Provenance, ExternalSnapshot, ExternalKey};
pub use visibility::Visibility;
pub use draft::{MemoryDraft, EdgeHint};
pub use pack::{PackDef, PackVersion};
pub use ontology::Ontology;
```

The submodule stubs — every file below compiles as-is inside `crates/exocortex-kernel/src/`:

```rust
// ids.rs
use serde::{Deserialize, Serialize};

/// 128-bit deterministic memory identity. See §7.14 R-T18a.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MemoryId(pub [u8; 16]);

impl MemoryId {
    /// Derive a `MemoryId` from external-source coordinates (R-T18a).
    ///
    /// `MemoryId = blake3(org_id || source_uri || table_uuid || logical_pk || mapping_version)[..16]`
    pub fn from_external(
        org_id: &str,
        source_uri: &str,
        table_uuid: &str,
        logical_pk: &[u8],
        mapping_version: u32,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(org_id.as_bytes());
        hasher.update(b"\x1e"); // record separator
        hasher.update(source_uri.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(table_uuid.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(logical_pk);
        hasher.update(b"\x1e");
        hasher.update(&mapping_version.to_le_bytes());
        let hash = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&hash.as_bytes()[..16]);
        Self(out)
    }

    /// Fallback for adapters that cannot supply an `ExternalKey` — content hash.
    /// Documented limitation, not a general strategy. See §7.14.
    pub fn from_content_hash(org_id: &str, content: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"content-hash-v1\x1e");
        hasher.update(org_id.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(content.as_bytes());
        let mut out = [0u8; 16];
        out.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self(out)
    }

    /// Random v7 UUID-shaped id for locally-created (Asserted) memories.
    pub fn new_v7() -> Self { Self(*uuid::Uuid::now_v7().as_bytes()) }
}

/// Relationship identity — always derived (never external-keyed).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RelationshipId(pub [u8; 16]);

impl RelationshipId {
    /// `RelationshipId = blake3(from || kind || to || snapshot_id_or_zero)[..16]`.
    /// Deterministic so re-derivation of the same edge in the same snapshot
    /// is idempotent.
    pub fn derive(from: MemoryId, kind: super::RelKindId, to: MemoryId, snapshot: Option<&str>) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(&from.0);
        h.update(&kind.0.to_le_bytes());
        h.update(&to.0);
        h.update(snapshot.unwrap_or("").as_bytes());
        let mut out = [0u8; 16];
        out.copy_from_slice(&h.finalize().as_bytes()[..16]);
        Self(out)
    }
}

/// Entity id — content-hash of `(entity_type, canonical_name)` scoped by org.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EntityId(pub [u8; 16]);

/// Pack identity — 16-bit registry id assigned at kernel load time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PackId(pub u16);

/// Local or backend LSN. `LSN::new_local(0)` is reserved for pre-init.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LSN {
    pub space: LsnSpace,
    pub value: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LsnSpace { Local, Backend }

impl LSN {
    pub fn new_local(v: u64) -> Self { Self { space: LsnSpace::Local, value: v } }
    pub fn new_backend(v: u64) -> Self { Self { space: LsnSpace::Backend, value: v } }
}
```

```rust
// visibility.rs
use serde::{Deserialize, Serialize};

/// Every memory and relationship carries an explicit `Visibility`. No default
/// (R-T6, CR-22).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum Visibility {
    Private = 0,
    Project = 1,
    Team    = 2,
    Org     = 3,
    Public  = 4,   // reserved for v2; v1 read paths treat as Org (R-T11)
}

impl Visibility {
    /// True iff `self` is not wider than `ceiling`. Used by the ingest
    /// validator to enforce R-T11a (no-widening rule).
    pub fn within(self, ceiling: Visibility) -> bool { (self as u8) <= (ceiling as u8) }
}
```

```rust
// provenance.rs — the canonical six-variant provenance (semantics: §7.9).
// This is the ONLY definition; packs cannot add variants (R-Pk5).
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Six-variant provenance (§7.9). `Proposed` never persists (R-T16).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Provenance {
    Asserted { author: SmolStr },
    Derived { rule_id: SmolStr, evidence: Vec<crate::RelationshipId> },
    Computed { producer: ComputedProducer, threshold: f32 },
    Extracted { extractor: SmolStr, extraction_confidence: f32 },
    Proposed { discovery_id: uuid::Uuid, score: f32 },
    ExternalSnapshot(ExternalSnapshot),
}

/// Internal computed producers (§7.9).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ComputedProducer {
    SimilarityCosine,    // ingest-time embedding compare (deferred; §24 q12)
    SimilarityHnsw,      // Dreams-time ANN
    EntityCoOccurrence,  // two memories share >=N typed entities
    SessionCoOccurrence, // two memories from the same session_id
}

/// External-system coordinates carried on every ingested assertion (R-T16a).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalSnapshot {
    pub source_uri: SmolStr,       // e.g. "iceberg://catalog/db/table"
    pub snapshot_id: SmolStr,      // source-specific snapshot handle
    pub schema_hash: [u8; 32],     // source column schema at snapshot
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub external_key: ExternalKey,
    pub producer_id: SmolStr,      // adapter id
}

/// Stable coordinates for identity derivation (R-T18a).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalKey {
    pub table_uuid: SmolStr,       // logical table identity, path-independent
    pub logical_pk: Vec<u8>,       // primary key bytes
    pub mapping_version: u32,      // bumped when the adapter changes column mapping
}
```

```rust
// kinds.rs
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Interned handle for a relationship kind (§7.3).
///
/// - High bit clear: kernel space (constants below).
/// - High bit set:   pack space (assigned by pack at register time,
///                   `RelKindId((pack_id << 16) | local_id | 0x8000_0000)`).
///
/// `RelMeta.display_name` doubles as the stable ASCII Cypher label (R-T2).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RelKindId(pub u32);

impl RelKindId {
    pub const fn from_kernel(local: u16) -> Self { Self(local as u32) }
    pub fn from_pack(pack: crate::PackId, local: u16) -> Self {
        Self(0x8000_0000 | ((pack.0 as u32) << 16) | (local as u32))
    }
    pub fn is_kernel(self) -> bool { self.0 & 0x8000_0000 == 0 }
}

// Kernel constants — the closed list referenced by kernel rules R1–R9
// and the ingest validator. Additive-only across kernel major versions.
pub const SOLVES:     RelKindId = RelKindId::from_kernel(0);
pub const FIXES:      RelKindId = RelKindId::from_kernel(1);
pub const CAUSES:     RelKindId = RelKindId::from_kernel(2);
pub const IN_SESSION: RelKindId = RelKindId::from_kernel(3);

/// Eight buckets (§0.3). `Extension` is reserved for pack-defined buckets that
/// don't map into one of the seven kernel-canonical buckets.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RelBucket {
    Causal, Solution, Context, Learning, Similarity, Workflow, Quality,
    Integration,
    Extension(u16), // pack-defined
}

/// Metadata a pack attaches to every registered kind.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelMeta {
    pub id:               RelKindId,
    pub display_name:     SmolStr,
    pub bucket:           RelBucket,
    pub inverse:          Option<RelKindId>,
    pub bidirectional:    bool,
    pub default_strength: f32,
}
```

```rust
// memory.rs — the canonical Memory and MemoryContext (semantics: §7.5, §7.6).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{EntityId, MemoryId, Provenance, Visibility, LSN};

/// A score clamped to [0.0, 1.0] at construction (used by §14).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct F01(f32);
impl F01 {
    pub fn new(v: f32) -> Result<Self, crate::KernelError> {
        if (0.0..=1.0).contains(&v) { Ok(Self(v)) }
        else { Err(crate::KernelError::ScoreOutOfRange(v)) }
    }
    pub fn get(self) -> f32 { self.0 }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Memory {
    pub id: MemoryId,
    pub memory_type: u8,              // resolved via effective ontology
    pub title: SmolStr,               // 1..=200 chars (R-T5)
    pub content: String,              // >=1 char, harness-produced (R-T5)
    pub summary: Option<SmolStr>,     // <=500 chars (R-T5)
    pub tags: SmallVec<[SmolStr; 4]>, // lowercased, trimmed, deduped
    pub visibility: Visibility,       // R-T6: required, no default
    pub provenance: Provenance,       // §7.9
    pub context: MemoryContext,
    pub importance: F01,              // defaults 0.5 at ingest
    pub confidence: F01,              // defaults 0.8 at ingest
    pub effectiveness: Option<F01>,   // set by Dreams or explicit outcome
    pub usage_count: u32,             // incremented on read
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub invalidated_by: Option<MemoryId>,
    pub embedding: Option<Vec<f32>>,  // R-T8: stripped before cache/SSE
    pub lsn: LSN,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryContext {
    pub timestamp: DateTime<Utc>,               // mandatory; defaults to now() at ingest (R-T9)
    pub project_id: Option<SmolStr>,
    pub project_path: Option<SmolStr>,
    pub team_id: Option<SmolStr>,
    pub tenant_id: Option<SmolStr>,
    pub session_id: Option<SmolStr>,
    pub user_id: Option<SmolStr>,               // author
    pub created_by: Option<SmolStr>,            // may differ from user_id for agent authorship
    pub files_involved: SmallVec<[SmolStr; 4]>,
    pub languages: SmallVec<[SmolStr; 2]>,
    pub frameworks: SmallVec<[SmolStr; 2]>,
    pub technologies: SmallVec<[SmolStr; 2]>,
    pub git_commit: Option<SmolStr>,
    pub git_branch: Option<SmolStr>,
    pub working_directory: Option<SmolStr>,
    pub entities: SmallVec<[EntityId; 8]>,      // extracted by backend (R-T18)
    pub additional_metadata: serde_json::Value, // <= 8 KiB serialized (R-T10)
}
```

```rust
// relationship.rs — the canonical Relationship (semantics: §7.8).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{MemoryId, Provenance, RelKindId, RelationshipId, Visibility, LSN};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Relationship {
    pub id: RelationshipId,
    pub kind: RelKindId,
    pub from: MemoryId,
    pub to:   MemoryId,
    pub visibility: Visibility,
    pub provenance: Provenance,
    pub properties: RelationshipProperties,
    pub description: Option<SmolStr>,  // human-readable, optional
    pub bidirectional: bool,           // derived from RelMeta at ingest (R-T4)
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub invalidated_by: Option<RelationshipId>,
    pub lsn: LSN,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationshipProperties {
    pub strength: f32,
    pub confidence: f32,
    pub context: Option<SmolStr>,
    pub evidence_count: u32,
    pub success_rate: Option<f32>,      // Solution-bucket edges: how often it worked
    pub validation_count: u32,
    pub counter_evidence_count: u32,
    pub last_validated: DateTime<Utc>,
}
```

```rust
// entity.rs
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use crate::EntityId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub entity_type: u8,   // resolved via effective ontology
    pub canonical_name: SmolStr,
    pub aliases: Vec<SmolStr>,
}
```

```rust
// draft.rs — the write-path input (§7.14)
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::{MemoryContext, MemoryId, RelKindId, Visibility};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryDraft {
    pub memory_type: u8,       // resolved via effective ontology
    pub title: SmolStr,
    pub content: String,
    pub summary: Option<SmolStr>,
    pub visibility: Visibility,
    pub context: MemoryContext,
    pub edge_hints: SmallVec<[EdgeHint; 4]>,
    /// If present, the draft carries the source-derived identity coordinates.
    /// Absence forces content-hash fallback (see `MemoryId::from_content_hash`).
    pub external_key: Option<crate::ExternalKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeHint {
    pub kind: RelKindId,
    pub to: MemoryId,
    pub strength: Option<f32>,
    pub confidence: Option<f32>,
}
```

```rust
// pack.rs — registration and the `pack!` macro (§7.0)
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{RelKindId, RelMeta};

/// Compiled result of a `pack!` invocation. Registered with `inventory::submit!`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackDef {
    pub name: SmolStr,
    pub version: PackVersion,
    pub kernel_min: PackVersion,
    pub memory_type_names: Vec<SmolStr>,
    pub entity_type_names: Vec<SmolStr>,
    pub kinds: Vec<RelMeta>,
    pub type_triples: Vec<TypeTriple>,
    // Rules are compiled into the reasoning crate at build time, not shipped
    // in PackDef. PackDef only carries the rule-id list for fingerprinting.
    pub rule_ids: Vec<SmolStr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackVersion { pub major: u16, pub minor: u16, pub patch: u16 }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypeTriple {
    pub kind: RelKindId,
    /// `None` matches any memory type. Otherwise matches any listed type.
    pub from_types: Option<Vec<u8>>,
    pub to_types: Option<Vec<u8>>,
}

inventory::collect!(PackDef);

/// Called once at process startup. Consumes every `inventory::submit!` in the
/// linked binary and produces the effective ontology. Fails if:
/// - two packs share a name (R-Pk1)
/// - some kernel-constant `RelKindId` has no concrete kind bound (R-Pk2)
pub fn load_registered_packs() -> Result<crate::Ontology, crate::KernelError> {
    // Implementation in ontology.rs — this fn is the entry point.
    crate::ontology::Ontology::from_registered_packs()
}

/// The `pack!` macro. Emits:
///   - A `pub const PACK_DEF: PackDef = ...;` inside the pack crate.
///   - An `inventory::submit!` block registering it.
///   - Zero-sized marker types for `MemoryType`/`EntityType` variants that
///     packs can name in their Rust code.
///
/// The macro body is straight `macro_rules!`; the coding agent should copy the
/// implementation from `crates/exocortex-kernel/src/macros.rs` verbatim.
#[macro_export]
macro_rules! pack {
    (
        name: $name:literal,
        version: $version:literal,
        kernel_min: $kernel_min:literal,
        memory_types! { $($mt:ident),* $(,)? }
        entity_types! { $($et:ident),* $(,)? }
        kinds! { $($kind:ident => bucket: $bucket:ident, inverse: $inv:tt, bi: $bi:literal, default_strength: $ds:literal),* $(,)? }
        type_triples! { $($tk:ident => ($from:tt, $to:tt)),* $(,)? }
        crepe_rules! { $($rule:tt)* }
    ) => {
        // Emitted skeleton — full expansion in macros.rs. This shape lets
        // callers write the ergonomic DSL shown in PRD §7.0.
        pub const PACK_DEF: $crate::PackDef = $crate::PackDef {
            name: ::smol_str::SmolStr::new_static($name),
            version: $crate::__parse_version!($version),
            kernel_min: $crate::__parse_version!($kernel_min),
            memory_type_names: ::std::vec![],   // populated by proc-macro pass in v1.1
            entity_type_names: ::std::vec![],
            kinds: ::std::vec![],
            type_triples: ::std::vec![],
            rule_ids: ::std::vec![],
        };
        ::inventory::submit! { PACK_DEF.clone() }
    };
}
```

```rust
// ontology.rs — the effective ontology assembled from registered packs.
use std::collections::HashMap;

use smol_str::SmolStr;

use crate::{KernelError, KernelResult, PackDef, RelKindId, RelMeta, TypeTriple};

/// The effective ontology for this process — kernel + registered packs.
/// Constructed once at startup, then read-only.
#[derive(Debug)]
pub struct Ontology {
    pub packs: Vec<PackDef>,
    pub kinds_by_id: HashMap<RelKindId, RelMeta>,
    pub triples_by_kind: HashMap<RelKindId, Vec<TypeTriple>>,
    pub memory_type_by_name: HashMap<SmolStr, u8>,
    pub entity_type_by_name: HashMap<SmolStr, u8>,
    /// id → name; the storage layer uses this to mint Cypher labels (§6.5).
    pub memory_type_names: Vec<SmolStr>,
    pub entity_type_names: Vec<SmolStr>,
    pub fingerprint: crate::OntologyFingerprint,
}

impl Ontology {
    pub(crate) fn from_registered_packs() -> KernelResult<Self> {
        let packs: Vec<PackDef> = inventory::iter::<PackDef>.into_iter().cloned().collect();
        Self::from_packs(packs)
    }

    pub fn from_packs(packs: Vec<PackDef>) -> KernelResult<Self> {
        // R-Pk1: name uniqueness.
        let mut seen = std::collections::HashSet::new();
        for p in &packs {
            if !seen.insert(&p.name) {
                return Err(KernelError::DuplicatePack(p.name.clone()));
            }
        }
        // R-Pk2: kernel-constant coverage.
        let mut kinds_by_id: HashMap<RelKindId, RelMeta> = HashMap::new();
        for p in &packs {
            for k in &p.kinds {
                if kinds_by_id.insert(k.id, k.clone()).is_some() {
                    return Err(KernelError::DuplicateKind(k.id));
                }
            }
        }
        for required in [crate::kinds::SOLVES, crate::kinds::FIXES,
                         crate::kinds::CAUSES, crate::kinds::IN_SESSION] {
            if !kinds_by_id.contains_key(&required) {
                return Err(KernelError::UnboundKernelConstant(required));
            }
        }
        // R-T17: build triple index.
        let mut triples_by_kind: HashMap<RelKindId, Vec<TypeTriple>> = HashMap::new();
        for p in &packs {
            for t in &p.type_triples {
                triples_by_kind.entry(t.kind).or_default().push(t.clone());
            }
        }
        // Name indices. Type ids are assigned in pack order with a running
        // offset so multiple packs can never collide on u8 ids.
        let mut memory_type_by_name = HashMap::new();
        let mut entity_type_by_name = HashMap::new();
        let mut memory_type_names: Vec<SmolStr> = Vec::new();
        let mut entity_type_names: Vec<SmolStr> = Vec::new();
        for p in &packs {
            for name in &p.memory_type_names {
                memory_type_by_name.insert(name.clone(), memory_type_names.len() as u8);
                memory_type_names.push(name.clone());
            }
            for name in &p.entity_type_names {
                entity_type_by_name.insert(name.clone(), entity_type_names.len() as u8);
                entity_type_names.push(name.clone());
            }
        }
        let fingerprint = crate::OntologyFingerprint::compute(&packs);
        Ok(Self { packs, kinds_by_id, triples_by_kind,
                  memory_type_by_name, entity_type_by_name,
                  memory_type_names, entity_type_names, fingerprint })
    }
}
```

```rust
// validator.rs — the type-triple validator and no-widening enforcer.
use crate::{EdgeHint, KernelError, KernelResult, MemoryDraft, Ontology, Visibility};

/// The per-source visibility ceiling registered at admission time (R-T11a).
#[derive(Clone, Copy, Debug)]
pub struct SourceCeiling {
    pub source: &'static str,
    pub ceiling: Visibility,
}

/// Validate a single draft against the effective ontology.
///
/// Enforces:
///  - R-T5..R-T10 field bounds
///  - R-T11a no-widening
///  - R-T17 type-triple rules
pub fn validate_draft(
    onto: &Ontology,
    draft: &MemoryDraft,
    ceiling: SourceCeiling,
) -> KernelResult<()> {
    // R-T5
    if draft.title.is_empty() || draft.title.len() > 200 {
        return Err(KernelError::TitleBounds);
    }
    if draft.content.is_empty() {
        return Err(KernelError::EmptyContent);
    }
    if let Some(s) = &draft.summary {
        if s.len() > 500 { return Err(KernelError::SummaryBounds); }
    }
    if draft.context.additional_metadata.to_string().len() > 8 * 1024 {
        return Err(KernelError::MetadataTooLarge);
    }
    // R-T11a
    if !draft.visibility.within(ceiling.ceiling) {
        return Err(KernelError::VisibilityWidening {
            source: ceiling.source,
            ceiling: ceiling.ceiling,
            attempted: draft.visibility,
        });
    }
    // R-T17
    for hint in &draft.edge_hints {
        let triples = onto.triples_by_kind.get(&hint.kind)
            .ok_or(KernelError::UnknownKind(hint.kind))?;
        if !triples.iter().any(|t| matches_triple(t, draft.memory_type, /* to_type */ None)) {
            return Err(KernelError::InvalidTypeTriple {
                kind: hint.kind, from: draft.memory_type,
            });
        }
    }
    Ok(())
}

fn matches_triple(t: &crate::TypeTriple, from: u8, to: Option<u8>) -> bool {
    let from_ok = t.from_types.as_deref().map_or(true, |xs| xs.contains(&from));
    let to_ok = match (t.to_types.as_deref(), to) {
        (None, _) => true,
        (Some(_), None) => true, // to-side deferred to the peer draft
        (Some(xs), Some(v)) => xs.contains(&v),
    };
    from_ok && to_ok
}
```

```rust
// actions.rs — typed writes (§7.11). Handler bodies live in exocortex-ops.
use serde::{Deserialize, Serialize};
use crate::{MemoryDraft, MemoryId, RelationshipId, Visibility};

pub trait Action: Send + Sync + 'static {
    type Input:  Serialize + for<'de> Deserialize<'de>;
    type Output: Serialize + for<'de> Deserialize<'de>;
    const NAME: &'static str;                      // stable, human-readable
    const REQUIRED_VISIBILITY_CEILING: Visibility; // author must be within source ceiling
}

pub struct CommitWrapup;
pub struct AcceptDiscovery;
pub struct PromoteVisibility;
pub struct RetractEdge;

impl Action for CommitWrapup {
    type Input  = Vec<MemoryDraft>;
    type Output = Vec<MemoryId>;
    const NAME: &'static str = "commit_wrapup";
    const REQUIRED_VISIBILITY_CEILING: Visibility = Visibility::Org;
}

// (identical shape for the others — bodies in exocortex-ops)
```

`RelationshipId` is re-exported for the `AcceptDiscovery`/`RetractEdge` outputs.

```rust
// functions.rs — typed reads (§7.12). Handler bodies live in exocortex-ops.
use serde::{Deserialize, Serialize};
use crate::{MemoryId, RelKindId, RelationshipId};

pub trait Function: Send + Sync + 'static {
    type Input:  Serialize + for<'de> Deserialize<'de>;
    type Output: Serialize + for<'de> Deserialize<'de>;
    const NAME: &'static str;
    const P50_BUDGET_US: u32;   // perf CI (§15, R-Lat1)
    const P99_BUDGET_US: u32;
}

pub struct SearchMemories;        // 500us / 3ms
pub struct TraverseRelationships; // 2ms / 10ms
pub struct GetChain;              // 1ms / 5ms
pub struct ExplainEdge;           // 1ms / 5ms
```

```rust
// fingerprint.rs — OntologyFingerprint (§7.17)
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::PackDef;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OntologyFingerprint(pub [u8; 32]);

impl OntologyFingerprint {
    pub fn compute(packs: &[PackDef]) -> Self {
        let mut sorted: Vec<&PackDef> = packs.iter().collect();
        sorted.sort_by(|a, b| a.name.cmp(&b.name));
        let mut h = Sha256::new();
        h.update(b"exocortex-kernel-v1\x1e");
        for p in sorted {
            let bytes = bincode::serialize(p).expect("PackDef must serialize");
            h.update(&(bytes.len() as u32).to_le_bytes());
            h.update(&bytes);
        }
        let out: [u8; 32] = h.finalize().into();
        Self(out)
    }
}
```

```rust
// error.rs — the kernel error surface.
use thiserror::Error;
use crate::{RelKindId, Visibility};

pub type KernelResult<T> = Result<T, KernelError>;

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("two packs share the name `{0}` (R-Pk1)")]
    DuplicatePack(smol_str::SmolStr),
    #[error("duplicate RelKindId {0:?} across packs")]
    DuplicateKind(RelKindId),
    #[error("kernel constant {0:?} is not bound by any registered pack (R-Pk2)")]
    UnboundKernelConstant(RelKindId),
    #[error("unknown RelKindId {0:?} in effective ontology")]
    UnknownKind(RelKindId),
    #[error("invalid type triple: kind {kind:?} on memory_type {from}")]
    InvalidTypeTriple { kind: RelKindId, from: u8 },
    #[error("title must be 1..=200 chars (R-T5)")]
    TitleBounds,
    #[error("content must be non-empty (R-T5)")]
    EmptyContent,
    #[error("visibility widening rejected: source={source} ceiling={ceiling:?} attempted={attempted:?}")]
    VisibilityWidening { source: &'static str, ceiling: Visibility, attempted: Visibility },
    #[error("summary must be <=500 chars (R-T5)")]
    SummaryBounds,
    #[error("additional_metadata exceeds 8 KiB serialized (R-T10)")]
    MetadataTooLarge,
    #[error("score {0} out of [0.0, 1.0]")]
    ScoreOutOfRange(f32),
}
```

#### 2.6.2 `exocortex-pack-dev-v1`

```toml
# crates/exocortex-pack-dev-v1/Cargo.toml
[package]
name    = "exocortex-pack-dev-v1"
version.workspace = true
edition.workspace = true

[dependencies]
exocortex-kernel = { workspace = true }
inventory        = { workspace = true }
smol_str         = { workspace = true }
```

```rust
// crates/exocortex-pack-dev-v1/src/lib.rs
//! The v1 developer-domain pack (§7.1, §7.2, §7.3).
//!
//! 13 memory types × 12 entity types × 48 relationship kinds across 8 buckets.
//! This crate is the sole pack v1 links; adding a second pack in v2 is purely
//! additive.

use exocortex_kernel::pack;

pack! {
    name: "exocortex-pack-dev-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        Task, CodePattern, Problem, Solution, Project, Technology,
        Error, Fix, Command, FileContext, Workflow, General, Conversation,
    }

    entity_types! {
        File, Function, Class, Error, Technology, Concept, Person, Project,
        Command, Package, Url, Variable,
    }

    kinds! {
        // Solution bucket (5)
        Solves       => bucket: Solution,  inverse: SolvedBy,     bi: false, default_strength: 0.85,
        Addresses    => bucket: Solution,  inverse: AddressedBy,  bi: false, default_strength: 0.70,
        AlternativeTo=> bucket: Solution,  inverse: Self,         bi: true,  default_strength: 0.60,
        Improves     => bucket: Solution,  inverse: ImprovedBy,   bi: false, default_strength: 0.70,
        Replaces     => bucket: Solution,  inverse: ReplacedBy,   bi: false, default_strength: 0.90,
        // ... (43 more kinds — see PRD §7.18 for the full list. A coding agent
        // fills these in one bucket at a time, running `cargo check` between
        // buckets to catch typos.)
    }

    type_triples! {
        Solves    => (Solution | Fix, Problem | Error),
        Addresses => (Solution | Fix, Problem | Error),
        Executes  => (Command, _),
        Modifies  => (Task | Command | Fix, FileContext),
        Creates   => (Task | Command | Fix, FileContext),
        InSession => (_, Conversation),
        // ...
    }

    crepe_rules! {
        // R1: if a memory Solves another, source is Solution
        type_from_solves(a, MemoryType::Solution) <- edge(a, b, Solves), memory(a, _, _);
        // R2..R6 — see §10.2
    }
}
```

#### 2.6.3 `exocortex-wire`

```toml
# crates/exocortex-wire/Cargo.toml
[package]
name    = "exocortex-wire"
version.workspace = true
edition.workspace = true
build   = "build.rs"

[dependencies]
prost       = { workspace = true }
prost-types = { workspace = true }
tonic       = { workspace = true }
bytes       = { workspace = true }
serde       = { workspace = true }

[build-dependencies]
tonic-build = { workspace = true }
prost-build = "0.13"
```

```rust
// crates/exocortex-wire/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["../../proto/ingest.proto",
              "../../proto/cluster.proto",
              "../../proto/sse.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
```

```rust
// crates/exocortex-wire/src/lib.rs
//! Cluster-internal wire schemas. This crate never links `exocortex-kernel`,
//! so it can be depended on by `exocortex-worker` without dragging the kernel
//! into out-of-process adapters.

pub mod ingest { tonic::include_proto!("exocortex.ingest.v1"); }
pub mod cluster { tonic::include_proto!("exocortex.cluster.v1"); }
pub mod sse { tonic::include_proto!("exocortex.sse.v1"); }
```

The `proto/ingest.proto` file is fully specified in §18.6 — copy it verbatim into `proto/`. `proto/cluster.proto` and `proto/sse.proto` are the cluster-internal envelopes used by §9; their minimal v1 contents:

```protobuf
// proto/cluster.proto
syntax = "proto3";
package exocortex.cluster.v1;
import "sse.proto";
message InvalidationEnvelope {
  uint32 wire_version         = 1;  // WIRE_VERSION const (R-W2)
  bytes  ontology_fingerprint = 2;  // 32 bytes; peer admission gate (R-W3)
  string emitter_node_id      = 3;
  exocortex.sse.v1.Invalidation inv = 4;
  bytes  hmac                 = 5;  // HMAC-SHA256 over fields 1..4 (R-W4)
}
```

```protobuf
// proto/sse.proto
syntax = "proto3";
package exocortex.sse.v1;
message Invalidation {
  oneof kind {
    MemoryUpserted      memory_upserted      = 1;
    MemoryDeleted       memory_deleted       = 2;
    RelationshipUpserted relationship_upserted = 3;
    RelationshipDeleted  relationship_deleted  = 4;
  }
  uint64 backend_lsn = 10;
}
message MemoryUpserted      { bytes id = 1; bytes snapshot_json = 2; }
message MemoryDeleted       { bytes id = 1; }
message RelationshipUpserted { bytes id = 1; bytes from = 2; bytes to = 3; uint32 kind = 4; }
message RelationshipDeleted  { bytes id = 1; }
```

#### 2.6.4 `exocortex-storage`

```toml
[package]
name    = "exocortex-storage"
version.workspace = true
edition.workspace = true

[dependencies]
exocortex-kernel = { workspace = true }
async-trait      = { workspace = true }
falkordb         = { workspace = true }
tokio            = { workspace = true }
tracing          = { workspace = true }
thiserror        = { workspace = true }
serde            = { workspace = true }
serde_json       = { workspace = true }
smol_str         = { workspace = true }

[features]
integration = []
```

```rust
// crates/exocortex-storage/src/lib.rs
pub mod trait_;
pub mod types;
pub mod cypher;
pub mod falkor;
pub mod in_memory;

pub use trait_::{Storage, StorageError};
pub use falkor::FalkorStorage;
pub use in_memory::InMemoryStorage;
```

```rust
// trait_.rs — the storage seam. Full signature in §6.1; support types in §6.3.
use async_trait::async_trait;
use exocortex_kernel::{Memory, MemoryId, Relationship, RelationshipId};

use crate::types::CommitRecord;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError>;
    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError>;
    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError>;
    // ...full method list in §6.1.
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("ontology fingerprint mismatch: storage={storage:?} runtime={runtime:?}")]
    FingerprintMismatch { storage: [u8; 32], runtime: [u8; 32] },
}
```

```rust
// falkor.rs — FalkorDB implementation, expanded in §6.
use async_trait::async_trait;
use exocortex_kernel::{Memory, MemoryId, Relationship, RelationshipId};

use crate::{Storage, StorageError};

pub struct FalkorStorage {
    // client: falkordb::Client,
    // graph_name: String,
    // pinned_fingerprint: [u8; 32],
}

#[async_trait]
impl Storage for FalkorStorage {
    async fn upsert_memory(&self, _m: &Memory) -> Result<crate::types::CommitRecord, StorageError> { todo!() }
    async fn get_memory(&self, _id: &MemoryId) -> Result<Option<Memory>, StorageError> { todo!() }
    async fn upsert_relationship(&self, _r: &Relationship) -> Result<crate::types::CommitRecord, StorageError> { todo!() }
}
```

#### 2.6.5 — 2.6.12 Remaining crates (stub Cargo + lib.rs)

The remaining crates follow the same pattern. For each, the manifest lists workspace deps, and the `lib.rs` starts with a module map and re-exports. The full bodies are specified in later PRD sections:

| Crate | Depends on | lib.rs stub returns |
|---|---|---|
| `exocortex-cache`     | kernel, arc-swap, dashmap, petgraph, roaring, lasso | see §8 |
| `exocortex-reasoning` | kernel, pack-dev-v1, crepe, steel-core | see §10 |
| `exocortex-cluster`   | kernel, wire, storage, redis, chitchat, hmac, sha2, axum, eventsource-client | see §9 |
| `exocortex-ingest`    | kernel, wire, storage, cache, reasoning, tonic | see §18 |
| `exocortex-worker`    | wire, tonic, tokio (deliberately NOT kernel) | see §18 |
| `exocortex-server`    | kernel, storage, cache, reasoning, cluster, ingest, ops, dreams, axum, rmcp | see §21 |
| `exocortex-client`    | kernel, wire, tonic, cache, reasoning, ops, rmcp, sled, eventsource-client | see §21 |
| `exocortex-ops`       | kernel, storage, cache, reasoning, schemars, inventory, rmcp | see §21 |
| `exocortex-dreams`    | kernel, storage, cache, reasoning, cluster | see §12 |

For every crate above, the initial `src/lib.rs` should be:

```rust
//! (crate purpose in one paragraph — see the referenced PRD section)
#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

// Modules populated by the milestone that owns this crate.
// Empty compilable scaffold at M0:
pub fn __placeholder() {}
```

### 2.7 `xtask` commands

```toml
# xtask/Cargo.toml
[package]
name    = "xtask"
version.workspace = true
edition.workspace = true

[dependencies]
anyhow      = { workspace = true }
clap        = { version = "4", features = ["derive"] }
serde_json  = { workspace = true }
```

```rust
// xtask/src/main.rs
use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Cli { #[command(subcommand)] cmd: Cmd }

#[derive(Subcommand)]
enum Cmd {
    /// Compute + print the effective OntologyFingerprint of the pack set
    /// that would be linked into `exocortex-server`. Used in CI to detect
    /// unintended ontology drift between commits.
    Fingerprint,
    /// Generate MCP + OpenAPI schemas from the operation registry.
    /// Fails if generated schemas are out of date (CI gate).
    GenSchemas,
    /// Run the R-I1/R-I5 kernel purity check: shells out to
    /// `cargo tree -p exocortex-kernel -e no-dev` and greps for banned crates.
    KernelPurity,
    /// Run the interactive-read latency benchmark (R-Lat1 SLO gate).
    Bench,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fingerprint   => todo!(),
        Cmd::GenSchemas    => todo!(),
        Cmd::KernelPurity  => todo!(),
        Cmd::Bench         => todo!(),
    }
}
```

### 2.8 What must compile after M0

A coding agent that completes only the scaffolding in this section should have:

- `cargo check --workspace` passing.
- `cargo test -p exocortex-kernel` passing (unit tests inside kernel modules).
- `cargo xtask kernel-purity` passing.
- No file in `crates/exocortex-worker/` referencing `exocortex_kernel`.
- `proto/ingest.proto` present, copied verbatim from §18.6; `cluster.proto`/`sse.proto` from §2.6.3.

Everything after M0 (packs, storage, reasoning, cluster, ingest, dreams) is scheduled in §3.

---

## 3. Milestones by Layer

Milestones are ordered by architectural layer, not by wall-clock. A coding agent walks them in order. Each milestone is one PR-worth of work with concrete acceptance criteria that either compile-pass, test-pass, or benchmark-pass. Do not start milestone Mn+1 until Mn's acceptance criteria are green — the criteria are what make the layer safe to build on. Before starting any milestone, read the sections its tasks reference; §2 is always authoritative for names, layout, and types.

### M0 — Workspace scaffolding (kernel + wire)

**Owner crate(s):** `exocortex-kernel`, `exocortex-wire`, `xtask`.

**Objective.** Produce the empty-but-typed skeleton described in §2. The kernel compiles with every trait/struct/enum defined. The wire crate compiles with generated protobuf stubs. `xtask kernel-purity` is a working CI gate.

**Concrete tasks.**

1. Create the workspace tree from §2.1.
2. Write the workspace `Cargo.toml` from §2.2 verbatim.
3. Pin the toolchain (§2.3).
4. Write `deny.toml` from §2.4.
5. Populate `crates/exocortex-kernel/` with the fifteen source files from §2.6.1 verbatim. Every `todo!()` inside a function body is expected; every type must exist and compile.
6. Populate `crates/exocortex-wire/` with `build.rs` and `lib.rs` from §2.6.3. Copy `proto/ingest.proto` verbatim from §18.6 and `cluster.proto`/`sse.proto` from §2.6.3.
7. Populate `xtask` from §2.7. Implement only `Cmd::KernelPurity`; the others may stay `todo!()`.
8. Populate every other `crates/*/lib.rs` with the placeholder stub from §2.6.5.
9. Verify every pinned dependency in §2.2 resolves on crates.io at the pinned major (e.g. `falkordb`, `steel-core`, `rmcp`, `eventsource-client`, `chitchat`). Record any missing crates and adjust pins in a single commit — this is the only milestone where dependency pins may change.

**Acceptance criteria (CI must pass all).**

- `cargo check --workspace` succeeds.
- `cargo test -p exocortex-kernel --lib` succeeds (zero tests at M0; the tests arrive in M1).
- `cargo xtask kernel-purity` succeeds — no banned crate appears in `cargo tree -p exocortex-kernel -e no-dev`.
- `cargo deny check` succeeds.
- `cargo clippy --workspace -- -D warnings` succeeds.
- `rustfmt` idempotent.

### M1 — Ontology (kernel validators + dev-v1 pack)

**Owner crate(s):** `exocortex-kernel`, `exocortex-pack-dev-v1`.

**Objective.** The effective ontology loads at process startup, computes an `OntologyFingerprint`, and rejects malformed drafts. The full dev-v1 pack — 13 memory types, 12 entity types, 48 relationship kinds, type triples, and pack-local rules D1–D6 (§7.18) — is a linkable crate.

**Concrete tasks.**

1. Implement the `pack!` macro body in `exocortex-kernel/src/macros.rs`. Expand `memory_types!`, `entity_types!`, `kinds!`, `type_triples!`, and `crepe_rules!` into the `PackDef` shape from §2.6.1. Emit `#[derive(...)]` Rust enums for `MemoryType` and `EntityType` inside the pack crate.
2. Complete `Ontology::from_packs` and `OntologyFingerprint::compute` (already in §2.6.1 — verify they compile with the macro output).
3. Implement `validate_draft` per the body in §2.6.1 `validator.rs`. Add unit tests:
   - `title` bounds (R-T5).
   - Widening rejected (R-T11a).
   - Unknown kind rejected.
   - Invalid triple rejected.
   - Valid `Solves(Solution, Problem)` accepted.
4. Fill `exocortex-pack-dev-v1` with all 48 kinds. Group by bucket; `cargo check` between buckets.
5. Add an integration test `crates/exocortex-pack-dev-v1/tests/loads_correctly.rs` that calls `load_registered_packs()` and asserts the 48 kinds are present, all four kernel constants are bound, and the fingerprint is deterministic (same across two calls).
6. Add validator tests for the R-T5/R-T10 bounds: empty title, 201-char title, 501-char summary, 9 KiB `additional_metadata` — all rejected.

**Acceptance criteria.**

- `cargo test -p exocortex-kernel` — all validator tests pass.
- `cargo test -p exocortex-pack-dev-v1` — the loads_correctly test passes.
- `cargo xtask fingerprint` prints a stable 64-hex-char digest across two runs on the same commit.
- Removing any single kernel constant from the pack causes `load_registered_packs()` to return `KernelError::UnboundKernelConstant` — asserted by an ignored-by-default test.
- The dev-v1 pack crate does not depend on any crate other than `exocortex-kernel` and `inventory`.

### M2 — Storage (`Storage` trait + FalkorDB adapter)

**Owner crate(s):** `exocortex-storage`.

**Objective.** A working `Storage` implementation over FalkorDB that round-trips memories and relationships, enforces the pinned `OntologyFingerprint`, and supports bi-temporal reads.

**Concrete tasks (specified in full in §6).**

1. Finalize the `Storage` trait with all methods (writes, reads, bi-temporal, LSN ops).
2. Implement `FalkorStorage`. Include: Cypher label generation from the effective ontology at startup (R-T2); write path that stamps `lsn`, `recorded_at`; read path with `visibility` filter injected as a Cypher WHERE clause; bi-temporal `valid_at(t)` helper.
3. Persist the pinned `OntologyFingerprint` in a `_exocortex_meta` node; reject any process whose runtime fingerprint differs (R-D5).
4. Add a docker-compose harness that runs FalkorDB for integration tests.
5. Write property tests using `proptest` for LSN monotonicity and bi-temporal round-tripping.

**Acceptance criteria.**

- `cargo test -p exocortex-storage --features integration` passes against a live FalkorDB.
- Fingerprint mismatch aborts startup with a clear error.
- Property test: `∀ m, upsert_memory(m); get_memory(&m.id) == Some(m)` holds for 10k random memories.
- No method in the trait leaks a Cypher string to the caller (Cypher is an implementation detail per CR-10).

### M3 — Cache and MCP client (interactive-read path online)

**Owner crate(s):** `exocortex-cache`, `exocortex-client`, `exocortex-reasoning` (partial).

**Objective.** The `exocortex-mcp-client` binary starts, subscribes to a placeholder SSE feed (or file-backed feed for now), maintains an in-memory graph via ArcSwap, and answers a `search_memories` MCP call in sub-millisecond p50 against a 100k-memory synthetic dataset.

**Concrete tasks (specified in full in §8).**

1. Implement `exocortex-cache::Graph` using `petgraph::StableGraph<MemoryNode, RelEdge>` behind `ArcSwap<Arc<Graph>>`. Lock-free reads, single-writer publish.
2. Implement per-user filtered views: `Graph::view(user_ctx)` returns an iterator that lazily filters by `Visibility` (never a materialised copy).
3. Implement 2Q admission + eviction with a ghost queue sized 5% of active.
4. Wire an MCP server in `exocortex-client` using `rmcp`. Expose `search_memories` (Function `SearchMemories`) and stub the other Functions with `todo!()`.
5. Local WAL over `sled` for offline writes — buffer only, no reconciliation yet.
6. **Implement `--mode mcp-standalone` supervision** in `exocortex-node`: spawn and supervise a bundled `redis-server` with the FalkorDB module loaded, on a random localhost port (`--storage=falkordb-embedded`, §4.3), data dir under the user's data home; docker-compose path for CI.

**Acceptance criteria.**

- `exocortex-mcp-client` binary runs, accepts MCP over stdio, and returns synthetic data.
- Micro-benchmark `search_memories` p50 < 0.5 ms, p99 < 3 ms on 100k memories (§15, latency budget).
- No allocation on the read hot path (asserted via `dhat-heap` or `stats_alloc`).
- Cache eviction under scan load does not evict recently-accessed items (asserted with a scan-pollution test).

### M4 — Reasoning (kernel rules R1–R9, Steel embedding)

**Owner crate(s):** `exocortex-reasoning`.

**Objective.** Type inference, transitive closure, and problem-solution bridging derivations run deterministically in-process. Explanation traces render via Steel.

**Concrete tasks (specified in full in §10).**

1. Set up Crepe compile-time programs, one per rule R1–R9. Each rule imports the ontology's `RelKindId` constants — this is the only pack↔reasoning coupling.
2. Wire the reasoning engine to the cache: when `Graph` publishes a new snapshot, an incremental Crepe evaluation runs on the delta only (k=3 fact scoping on the interactive path).
3. Embed Steel via `steel-core`. Expose a `Storage`-shaped read interface and the `Function` set. Load `.scm` files from `exocortex-server/scripts/`.
4. Implement `ExplainEdge` Function: walks derivation provenance, produces a Steel `sexp` tree, callers render prose.

**Acceptance criteria.**

- `cargo test -p exocortex-reasoning`: rules R1–R9 produce expected derivations against fixtures.
- Adding a `Solves(A,B)` edge causes A's memory type to be re-derived as `Solution` (R1) within the same commit.
- `ExplainEdge` on a rule-derived edge returns a Steel tree that names every input fact.
- No hot-path serialization: `serde_json` is not on the read path (CI grep gate).

### M5 — Cluster (leases, gossip, SSE)

**Owner crate(s):** `exocortex-cluster`, `exocortex-server`.

**Objective.** A 3-node backend cluster elects a Dreams owner via Chubby-style Redis leases, gossips membership via `chitchat`, and pushes change-feed deltas to subscribed clients over SSE. HMAC-signed at every hop. `OntologyFingerprint` + wire version gate peer admission (§9.1).

**Concrete tasks (specified in full in §9).**

1. Implement `Lease::acquire(kind, node_id)` using `SET NX EX` + fencing epoch. Owner-elected work (Dreams, backfill, cleanup) refuses to run without an active lease with the current epoch.
2. Implement `Cluster::join()` — HMAC handshake, fingerprint check, wire-version check. Reject mismatches with the exact error names in §9.7 (`ClusterError::{WireMismatch, OntologyMismatch, HmacFailed}`).
3. Implement SSE server in `exocortex-server` using `axum` + `tokio-stream::wrappers::BroadcastStream`. Emit protobuf-encoded `ChangeFeedDelta` over `text/event-stream`.
4. Implement SSE client in `exocortex-client` using `eventsource-client`. Reconnecting subscriber; applies deltas in backend-LSN order; buffers ahead-of-order deltas.
5. Chaos test: kill the leader mid-Dreams, verify a new lease is acquired within 2s (the acceptance criterion below) and no zombie writes land (fenced by epoch).

**Acceptance criteria.**

- 3-node docker-compose harness: leader election converges within 2s of leader-kill.
- SSE latency: p95 delta arrival < 200ms end-to-end.
- Peer admission rejects a node with a different fingerprint — the error message names the mismatch.
- `chaos::split_brain` test: a partitioned old leader cannot commit after epoch fencing.

### M6 — Ingestion (session-wrapup + adapter host)

**Owner crate(s):** `exocortex-ingest`, `exocortex-worker`.

**Objective.** The session-wrapup MCP tool `end_session` produces a valid `IngestBatch`, sends it via tonic to the backend, and the backend admits it after ontology + ceiling + type-triple validation. An `exocortex-worker` process (linking `exocortex-wire` only, never the kernel) is a runnable no-op that will host adapters in v2.

**Concrete tasks (specified in full in §18).**

1. Implement `IngestService` in `exocortex-ingest` behind tonic. Method `Submit(IngestBatch) -> IngestAck` runs the §7.13 validation pipeline for every batch; batches are atomic (R-T17) — the ack names the first offending draft_key and its `RejectCode`.
2. Wire the session-wrapup adapter inside `exocortex-client`: `end_session` builds a batch with `source_uri = "session://<session_id>"`, `producer_id = "session-wrapup"`, and **no** `ExternalSnapshotInfo` (§18.3 — session-wrapup is an origin, not a re-sync).
3. Implement `exocortex-worker` main: parses `--adapter <name>` and `--config <path>`; loads a shared-object adapter (v2 will be `iceberg-adapter.so`); pumps `IngestBatch` frames to the server. Ships as a no-op ("hello world") for v1.
4. Implement the entity-extraction table (§7.2): one compiled regex set per `EntityType` (file paths, function/class/variable identifiers, error signatures, URLs, `package@version`, shell commands, technology names); deterministic; ambiguous matches recorded with `Provenance::Extracted { extraction_confidence }` (R-T18).
5. Persist per-source `SourceCeiling` in the server config (via `RegisterSource`, §18.6); the validator reads it on every batch.

**Acceptance criteria.**

- End-to-end test: harness sends `end_session` → client emits `IngestBatch` → server validates, writes memories, publishes SSE deltas → a second client observes them.
- A batch with `visibility=Team` under a `Project`-ceiling source is rejected with `RejectCode::VISIBILITY_WIDENING`.
- A batch with an unknown `RelKindId` is rejected with `RejectCode::UNKNOWN_KIND`.
- `exocortex-worker` compiles and runs `--adapter noop` cleanly. `cargo tree -p exocortex-worker | grep exocortex-kernel` returns nothing.

### M7 — Operation Registry (MCP + HTTP parity)

**Owner crate(s):** `exocortex-ops`, `exocortex-server`, `exocortex-client`.

**Objective.** Every operation the system exposes is declared once in `exocortex-ops` and generates:

- an MCP tool schema surfaced to the harness,
- an OpenAPI path in the backend HTTP server,
- a typed Rust handler with a shared implementation body.

**Concrete tasks (specified in full in §21).**

1. Define the `Operation` trait: `name`, `Input: JsonSchema`, `Output: JsonSchema`, `mode: OperationMode { ClientOnly, ServerOnly, Both }`, `async fn handle(&self, ctx: &Ctx, input: Input) -> Result<Output>`.
2. Use `inventory::submit!` to register every operation. Kernel Actions and Functions register their operations here.
3. `exocortex-server::openapi()` walks the registry, emits `openapi.json`. `exocortex-client::mcp_tools()` walks the same registry, emits the tool schema list to the MCP host.
4. `cargo xtask gen-schemas` verifies checked-in `openapi.json` and `mcp-tools.json` match the registry (CI gate).

**Acceptance criteria.**

- Every operation the client can call is also callable via HTTP against the server (CR-9).
- Schema drift is caught in CI: modifying an operation without regenerating schemas fails `cargo xtask gen-schemas`.

### M8 — Dreams (consolidation + MCR²)

**Owner crate(s):** `exocortex-dreams`.

**Objective.** The Dreams cycle runs on the owner-elected server node, triggered by write-count thresholds. It computes MCR² before and after each consolidation, aborts if the score degrades, and emits an audit trail.

**Concrete tasks (specified in full in §12).**

1. Implement the trigger loop over the per-region write counters from §12.2.
2. Implement `MCR2Engine::score(region)`: partition by `MemoryType`, compute the ΔR score from §11.
3. **Pick the clustering implementation**: evaluate `linfa-clustering` (DBSCAN) versus a vendored HDBSCAN over the region's embedding matrix; record the decision in `crates/exocortex-dreams/CLUSTERING.md`. Whichever is chosen must assign every anchor to a cluster id (or noise) deterministically.
4. Implement the four consolidation actions: `merge`, `abstract`, `prune`, `strengthen`.
5. Every consolidation session is wrapped in a lease acquisition (R-C1, R-C3) and stamped with a fencing epoch.
6. If `mcr2_after < mcr2_before - tolerance` (default tolerance 0.01, R-Mcr3) — abort, rollback the transaction, log a diagnostic.

**Acceptance criteria.**

- On a 10k-memory synthetic dataset with duplicates, running Dreams reduces cardinality by ≥ 20% and improves MCR² score.
- Injecting a poison consolidation (a hand-crafted merge that provably degrades MCR²) rolls back within one cycle.
- Consolidation never runs without an active lease — asserted by killing Redis and observing the loop stall gracefully.

### M9+ — v2 adapters and second pack

Not v1 scope. Placeholder for `iceberg-adapter` (Iceberg / S3 Tables), `delta-adapter`, `parquet-dir-adapter`, and a second pack demonstrating the pack seam. Each of these is one milestone. See §18.4 and §7.16.

### Cross-cutting quality gates (every milestone)

These run on every PR, independent of the milestone being merged:

- `cargo xtask kernel-purity` — R-I1 / R-I5 defence in depth.
- `cargo deny check` — license and banned-crate policy.
- `cargo clippy -- -D warnings` — zero-warning policy.
- `cargo fmt -- --check` — style.
- `cargo test --workspace` — every crate's unit tests.
- `cargo bench` on the interactive-read benchmark — R-Lat1 SLO gate; regressions fail CI.
- `cargo xtask gen-schemas` — MCP + OpenAPI drift detector (after M7).

Any of these failing blocks merge. This is what makes the milestone acceptance criteria real rather than aspirational.

---

## 4. Deployment Model

### 4.1 Two-tier topology

```
┌──────────────────────────────────────────┐
│  Developer's laptop                      │
│  ┌────────────────────────────────────┐  │
│  │ Coding harness (Claude Code,       │  │
│  │ Codex, Cursor, custom agent)       │  │
│  │  — has its own LLM                 │  │
│  └──────────────┬─────────────────────┘  │
│                 │ MCP stdio               │
│                 ▼                        │
│  ┌────────────────────────────────────┐  │
│  │ exocortex (mode: mcp-client)       │  │
│  │  — local ArcSwap cache             │  │
│  │  — deterministic reasoning         │  │
│  │  — WAL for offline writes          │  │
│  │  — SSE subscriber                  │  │
│  └──────────────┬─────────────────────┘  │
└─────────────────┼────────────────────────┘
                  │ HTTPS + SSE change feed
                  ▼
┌────────────────────────────────────────────┐
│  Exocortex backend cluster                 │
│  ┌──────────────┐  ┌──────────────┐        │
│  │ exocortex    │  │ exocortex    │  ...   │
│  │ (backend-    │  │ (backend-    │        │
│  │  node)       │  │  node)       │        │
│  └──────┬───────┘  └──────┬───────┘        │
│         │                 │                 │
│    gossip + pub-sub between backend nodes  │
│         │                 │                 │
│         ▼                 ▼                 │
│  ┌──────────────────────────────────┐      │
│  │  FalkorDB (persistent storage)   │      │
│  └──────────────────────────────────┘      │
└────────────────────────────────────────────┘
```

### 4.2 One artifact, two modes

A single Rust cargo workspace produces:

| Artifact | Role |
|---|---|
| `exocortex-kernel` (lib) | Types, provenance, visibility, ontology machinery, validators — no I/O |
| `exocortex-pack-dev-v1` (lib) | The v1 dev-domain ontology pack (§7.18) |
| `exocortex-wire` (lib) | Protobuf schemas + `prost` types + `tonic` stubs (ingest, cluster, sse) |
| `exocortex-storage` (lib) | `Storage` trait, FalkorDB adapter, `InMemoryStorage` test double |
| `exocortex-cache` (lib) | ArcSwap graph cache, 2Q admission, visibility-filtered views |
| `exocortex-reasoning` (lib) | Crepe rule catalogue + Steel runtime: belief evolution, explanation |
| `exocortex-cluster` (lib) | Backend-only: gossip, coherence, leases, invalidation transport |
| `exocortex-ingest` (lib) | Ingestion Protocol server (`IngestService`) |
| `exocortex-dreams` (lib) | Consolidation + discovery cycle, MCR² engine — deterministic, no LLM |
| `exocortex-ops` (lib) | Operation registry; generates MCP tool + OpenAPI schemas |
| `exocortex-node` (bin, from `exocortex-server`) | `--mode backend-node \| mcp-standalone \| embedded` |
| `exocortex-mcp-client` (bin, from `exocortex-client`) | Local MCP server: cache, WAL, SSE subscriber, sync |
| `exocortex-worker` (bin) | Out-of-process adapter host (no-op in v1; v2 adapters live here) |
| `mcp-tools.json` / `openapi.json` | Generated from the operation registry; CI-checked for drift (M7) |

The embedding runtime (`fastembed` + `bge-small`) is a backend dependency of `exocortex-server`/`exocortex-dreams`, not a separate crate. Client-side sync (WAL, SSE subscriber, LSN reconciliation) lives inside `exocortex-client`.

Modes:

```
exocortex-mcp-client \
    --backend=https://exocortex.example.com \
    --auth-token=$EXOCORTEX_TOKEN

exocortex-node --mode=mcp-standalone \    # local, no backend; process-local FalkorDB
    --storage=falkordb-embedded

exocortex-node --mode=backend-node \      # cluster peer
    --bind=:8080 \
    --storage=falkordb://falkor.svc:6379/exocortex \
    --cluster-endpoints=node1:9090,node2:9090

exocortex-node --mode=embedded            # in-process library, tests/benchmarks
```

### 4.3 Deployment scenarios

| Scenario | Mode | Storage | Cluster |
|---|---|---|---|
| Developer with team backend | `mcp-client` | Backend | Backend has N nodes |
| Solo developer, no backend | `mcp-standalone` | `falkordb-embedded` | None |
| Team self-hosted backend | `backend-node` × N | Networked FalkorDB | Yes |
| Hosted cloud service | `backend-node` × N | FalkorDB Cloud | Yes |
| CI / integration test | `embedded` | `falkordb-embedded` | None |

**`falkordb-embedded` means process-local, not in-process.** FalkorDB is a Redis module; it has no in-process mode. `--storage=falkordb-embedded` makes `exocortex-node` spawn and supervise a bundled `redis-server` with the FalkorDB module loaded, on a random localhost port, data directory under the user's data home. CI runs the same topology via docker-compose. The supervisor ships in M3 (§3).

**R-D1.** The same kernel/cache/reasoning code paths execute in every scenario. The cluster coordinator is a no-op in single-node and mcp-* modes.

**R-D2.** Storage backend selection is a trait boundary (§6), not a compile-time feature.

**R-D3.** MCP-client, MCP-standalone, and backend-node modes expose the same operations. MCP tool schema and HTTP OpenAPI schema are both generated from a shared operation registry (§21).

**R-D4.** Adding or removing a backend cluster node MUST NOT require a client-visible change. Cluster membership is a runtime property.

**R-D5.** An `mcp-client` and its backend MUST agree on `OntologyFingerprint` before the client is admitted. On mismatch the client refuses to start and logs the diff.

**R-D6.** Exocortex MUST NOT call an LLM directly. All LLM inference happens in the coding harness. The `LLMProvider` trait, `LLMService`, prompt caches, and provider configuration are removed from the design. This is a deployment-level invariant enforced by codebase grep and mocked-provider audits in CI (CR-19).

### 4.4 What the backend does that the client doesn't

The backend runs the **owner-only** work:

- **Consolidation cycles** (MCR² merge/abstract/prune/strengthen) — one owner per **region** (§17.3)
- **Discovery generation** (cross-domain, temporal-echo, orphan, transitive finders) — one owner per region
- **Cross-region reconciliation** — one owner per graph, bounded edge budget
- **Cleanup cycles** (delete derived edges below confidence floor) — one owner per graph
- **Retroactive backfill** (apply new rules to existing memories) — one owner per graph
- **Persistent storage** (FalkorDB writes with monotonic LSN emission)
- **Cross-machine and cross-user consolidation** (memories from all users in an org flow into the same graph and consolidate together)

The client runs the **hot-path** work:

- **Fast local reads** over an in-memory ArcSwap snapshot
- **Local reasoning** — Crepe derivation and Steel belief evolution on the snapshot
- **Local session-wrapup capture** — writes flow through the WAL, batch to backend
- **Local search / chain / traverse** operations

Both run the same rule catalogue against the same ontology. The client's inference results are always locally recomputable, so a client that has fallen behind still returns correct answers relative to its snapshot — it just may not see the backend's most-recent derivations until sync catches up.

---

## 5. System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Transport Adapters                            │
│  MCP stdio (rmcp) │ HTTP (axum) │ Embedded library API                │
├──────────────────────────────────────────────────────────────────────┤
│                       Operation Registry                             │
│  Single set of typed operations; each surface is an adapter.         │
├──────────────────────────────────────────────────────────────────────┤
│                          Service Layer                               │
│  Memory │ Relationship │ Search │ Chain │ Inference │ Dreams │ Session│
├──────────────────────────────────────────────────────────────────────┤
│                Reasoning Layer — TWO LANGUAGES                       │
│  ┌───────────────────────────┐  ┌──────────────────────────────┐     │
│  │ Crepe (Datalog)           │  │ Steel (Scheme)               │     │
│  │  — derivation             │  │  — belief evolution          │     │
│  │  — transitive closure     │  │  — explanation traces        │     │
│  │  — idempotent by design   │  │  — custom rules              │     │
│  └───────────────────────────┘  └──────────────────────────────┘     │
│                    Both engines read the same snapshot               │
├──────────────────────────────────────────────────────────────────────┤
│                   In-Memory Cache (Local, Snapshot-Coherent)         │
│  StableGraph + DashMap + lasso + ArcSwap                             │
│  + version { local_lsn, backend_lsn } + admission (2Q)               │
├──────────────────────────────────────────────────────────────────────┤
│    Client-side sync (exocortex-client)  │  Backend-side cluster       │
│    ─────────────────────────────────    │  (exocortex-cluster)        │
│    WAL │ SSE subscriber │ batch sync   │  gossip │ invalidation      │
│                                         │  leases │ owner election    │
├──────────────────────────────────────────────────────────────────────┤
│                       Storage Trait (§6)                             │
│      FalkorDB (net, backend)  │  FalkorDB (embedded, standalone)     │
│                    Cypher = STORAGE only                             │
├──────────────────────────────────────────────────────────────────────┤
│                        Pluggable Services                            │
│  Embeddings (backend default; local optional)                        │
│  Entity extraction (regex + NLP, deterministic)                      │
│  NO LLM. Anywhere. Ever.                                             │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.1 Interactive read path (mcp-client)

```
Harness → MCP tool → Operation → Service::search_memories
  → LocalCache::snapshot (ArcSwap load, ~10ns)
  → base match + chain expansion + Crepe augmentation
  → rank via scoring algebra (§11)
  → Return { result, snapshot_version: { local_lsn, backend_lsn, age_ms } }

Target p50: 500µs.  Target p99: 3ms.
```

No network. No LLM. No disk. Serialization happens only at the MCP boundary (JSON to the harness).

### 5.2 Session-wrapup write path (mcp-client)

```
Harness detects session end (directive, hook, or explicit)
  → Harness's LLM extracts structured wrapup JSON (1-5 memories)
  → MCP tool exocortex.end_session(wrapup_batch)
  → Service::end_session
    → LocalCache::apply_batch (ArcSwap update)
    → WAL::append_batch (async fsync)
    → Sync::enqueue(batch)                 [background]
    → Return { local_lsns: [...], sync_pending: true }
      ↓
  Background sync worker (independent task):
    gRPC IngestService.Submit(batch) → backend (§18)
      → backend Storage::upsert_batch → backend_lsn
      → SSE change feed publishes updates to all subscribed clients
      → this client receives its own updates on SSE, reconciles
         local_lsn ↔ backend_lsn, marks batch synced in WAL
```

Interactive latency is bounded by local cache update + WAL append. Network happens off the critical path.

### 5.3 Backend write path (backend-node)

```
Client POST → Operation → Service::end_session_batch
  → Storage::upsert_batch (durable write, Cypher)
      ↓ returns [CommitRecord { lsn }]
  → LocalCache::apply_batch (this backend node)
  → Cluster::publish_invalidation(batch, max_lsn)  [to peer backend nodes]
  → SSE::publish(batch)                             [to subscribed clients]
  → Return [{ id, backend_lsn }]
```

### 5.4 Dreams cycle (backend-only, owner-elected, event-triggered)

```
TriggerWatcher on backend node                       ← pub-sub: write-counter increments
  → evaluate per-region trigger predicates (§12.2)
  → if fired: RPUSH exocortex:dreams:queue { region }

DreamsWorker on backend node (drains queue)
  → BLPOP exocortex:dreams:queue
  → Coordinator::acquire_lease(region, OwnerRole::DreamsCycle, 60s)
  → ConsolidationService.run(region)
    → fetch memories + embeddings for region
    → MCR² before  |  Sparsity before
    → cluster via HDBSCAN
    → MERGE / ABSTRACT / PRUNE / STRENGTHEN via Storage
    → create SimilarTo edges (Computed { SimilarityHnsw }, threshold 0.85)
    → MCR² after   |  Sparsity after
    → Storage::write ConsolidationResult (audit record, region-tagged)
  → DiscoveryEngine.run(region)
    → cross_domain | temporal_echo | orphan | transitive
    → Storage::write Discovery records (Proposed provenance; NOT edges)
  → Coordinator::release_lease
  → reset region write-counters
  → SSE::publish invalidations + new discoveries
```

No LLM call anywhere in this cycle. Discoveries are structured proposals with typed endpoints. If the harness wants prose about a discovery, it fetches the discovery and its endpoints and narrates in its own LLM.

---

## 6. Database Adapter Layer

### 6.0 The one deliberate seam

Exocortex has one architectural pluggability point: the `Storage` trait. It is implemented **twice** in v1:

- **`FalkorDBStorage`** — the real one. Handles both networked (backend) and embedded (standalone / dev) modes internally. All Cypher, all Redis interactions, all connection management, all lease coordination, and all change-feed subscription live inside this impl.
- **`InMemoryStorage`** — the mock for tests. Rust `HashMap`s and `Vec`s, no Cypher, no I/O, deterministic. **Ship as a v1 deliverable, not a stretch goal.** Unit tests depend on it existing.

Every subsystem above `Storage` — cache, reasoning, Dreams, MCP surface, cluster coordination — depends on the trait, not on FalkorDB. Cypher does not leak out of `FalkorDBStorage`; Redis wire calls do not leak out of `FalkorDBStorage`. When someone asks "why is this a trait," the answer is *so we can test it*, never *so we can swap it*. Alternate backends (Memgraph, Kùzu, Neptune, Neo4j) are not designed for; they become **possible** as a free side effect of the adapter, not a goal.

Leases and change-feed subscription live **on `Storage`**, not on separate traits. For FalkorDB they are all Redis; pretending otherwise would add seams that do not earn their keep. If a second backend ever ships, whether these should split is that backend's problem, not v1's.

### 6.1 The `Storage` trait

```rust
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    // ---- Memory + relationship writes ----
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord>;
    async fn upsert_batch(&self, ms: &[Memory], rs: &[Relationship])
        -> Result<Vec<CommitRecord>>;
    async fn delete_memory(&self, id: &MemoryId) -> Result<CommitRecord>;

    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord>;
    async fn delete_relationship(&self, id: &RelationshipId) -> Result<CommitRecord>;

    // ---- Reads (interactive path) ----
    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>>;
    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>>;
    async fn traverse(&self, from: &MemoryId, spec: &TraversalSpec)
        -> Result<Vec<Memory>>;
    async fn find_by_entity(&self, entity: &EntityId, filter: &MemoryFilter)
        -> Result<Vec<Memory>>;

    // ---- Bi-temporal ----
    async fn get_state_at(&self, t: DateTime<Utc>) -> Result<GraphSnapshot>;
    async fn valid_at(&self, id: &MemoryId, at: DateTime<Utc>) -> Result<Option<Memory>>;

    // ---- Bulk / streaming (Dreams, backfill) ----
    async fn query_cypher(&self, q: &CypherQuery) -> Result<ResultSet>;
    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory>>;
    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship>>;

    // ---- Offline similarity (Dreams / ingest enrichment ONLY) ----
    // Never called on the interactive path. See principle 6 (§0.4).
    async fn find_similar_offline(
        &self,
        query: &Embedding,
        k: usize,
        filter: &MemoryFilter,
    ) -> Result<Vec<(MemoryId, f32)>>;

    // ---- Leases + fencing (called from cluster code; §9.2) ----
    async fn acquire_lease(&self, key: &LeaseKey, ttl: Duration) -> Result<OwnerLease>;
    async fn renew_lease(&self, lease: &OwnerLease) -> Result<OwnerLease>;
    async fn release_lease(&self, lease: OwnerLease) -> Result<()>;

    // ---- Change feed (backs SSE clients; §9.1, §9.6) ----
    async fn subscribe_invalidations(
        &self,
        region: &RegionKey,
    ) -> Result<BoxStream<'_, Invalidation>>;

    // ---- Metadata ----
    fn capabilities(&self) -> StorageCapabilities;
    fn backend_id(&self) -> StorageBackendId;      // "falkordb" | "in-memory"
    fn ontology_fingerprint(&self) -> [u8; 32];
}

pub struct CommitRecord {
    pub lsn: u64,               // monotonic per-graph LSN
    pub committed_at: DateTime<Utc>,
    pub node_id: Option<u64>,
    pub edge_id: Option<u64>,
}
```

**Cypher is used for four things only:**

1. Persist memory nodes and edges via parameterized templates.
2. Read a bounded subgraph for hydration or point-in-time reconstruction.
3. Return the raw fact set that Crepe materializes into Datalog inputs.
4. Backfill and cleanup operations.

Cypher does not derive, does not reason, does not evaluate rules.

### 6.2 Two LSN spaces

The design has **two monotonic LSN spaces**:

- **Local LSN** — assigned by the client's local WAL on write. Monotonic per client.
- **Backend LSN** — assigned by the backend's FalkorDB replication log. Monotonic per graph.

Reconciliation happens on sync ack: the client receives the backend LSN(s) for its batch and stores a `(local_lsn, backend_lsn)` mapping. Subsequent SSE deltas arrive with backend LSNs; the client applies them in backend-LSN order.

**R-S1.** The backend FalkorDB backend MUST use the official `falkordb` Rust crate.

**R-S2.** All persistence Cypher templates use `$timestamp` parameters and typed edge labels drawn from the enum-derived allowlist. No `MERGE` on relationships.

**R-S3.** Every mutation returns a `CommitRecord` with a monotonic per-graph LSN. Local WAL emits local LSNs; backend Storage emits backend LSNs.

**R-S4.** The local cache tracks both `last_applied_local_lsn` and `last_applied_backend_lsn` per graph. Every read response includes both.

### 6.3 Support types — full definitions

Every type the trait method signatures reference. These live in `exocortex-storage/src/types.rs`:

```rust
// crates/exocortex-storage/src/types.rs
use chrono::{DateTime, Utc};
use exocortex_kernel::{EntityId, MemoryId, RelKindId, RelationshipId, Visibility};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRecord {
    pub lsn: u64,
    pub committed_at: DateTime<Utc>,
    pub node_id: Option<u64>,
    pub edge_id: Option<u64>,
}

/// Bounded traversal descriptor. Every field carries a hard cap enforced
/// server-side (CR-6: no unbounded traversal).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraversalSpec {
    pub direction: Direction,
    pub kinds: SmallVec<[RelKindId; 8]>,           // empty = all kinds
    pub max_depth: u8,                              // hard-capped at 4 by validator
    pub max_nodes: u32,                             // hard-capped at 2048
    pub visibility_ctx: VisibilityContext,
    pub as_of: Option<DateTime<Utc>>,               // bi-temporal snapshot
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Direction { Out, In, Both }

/// The per-request identity + visibility scope. Every read is filtered by this
/// at the storage boundary; there is no "unfiltered read" surface (CR-22).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisibilityContext {
    pub user_id: SmolStr,
    pub org_id: SmolStr,
    pub project_ids: SmallVec<[SmolStr; 4]>,
    pub team_ids: SmallVec<[SmolStr; 4]>,
    pub max_visibility: Visibility,                 // effective ceiling
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryFilter {
    pub memory_types: SmallVec<[u8; 8]>,           // empty = all types
    pub entity_any_of: SmallVec<[EntityId; 8]>,
    pub project_id: Option<SmolStr>,
    pub session_id: Option<SmolStr>,
    pub valid_at: Option<DateTime<Utc>>,
    pub limit: u32,                                 // hard-capped at 500
    pub visibility_ctx: VisibilityContext,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub as_of: DateTime<Utc>,
    pub backend_lsn: u64,
    pub memory_count: u64,
    pub relationship_count: u64,
}

#[derive(Clone, Debug)]
pub struct CypherQuery {
    pub template_id: &'static str,      // must match a registered template
    pub params: serde_json::Value,
    pub read_only: bool,
    pub deadline: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ResultSet {
    pub rows: Vec<serde_json::Value>,
    pub scanned_rows: u64,
}

pub type Embedding = SmallVec<[f32; 384]>;

/// Chubby-style lease key. Every owner-only operation names its lease with one
/// of these; a lease holder never runs work outside the key it holds.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LeaseKey {
    Dreams { org: SmolStr, region: SmolStr },
    Backfill { org: SmolStr },
    Cleanup { org: SmolStr },
    Consolidation { org: SmolStr, region: SmolStr },
}

#[derive(Clone, Debug)]
pub struct OwnerLease {
    pub key: LeaseKey,
    pub owner_node_id: SmolStr,
    pub epoch: u64,                                 // monotonic per key
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub grace_period: chrono::Duration,             // Chubby-style grace window (§9.2)
    pub fencing_token: SmolStr,                     // opaque to caller; echoed on writes
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RegionKey {
    pub org: SmolStr,
    pub project: SmolStr,
    pub memory_type: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Invalidation {
    MemoryUpserted { id: MemoryId, lsn: u64 },
    MemoryDeleted  { id: MemoryId, lsn: u64 },
    RelationshipUpserted { id: RelationshipId, from: MemoryId, to: MemoryId, kind: RelKindId, lsn: u64 },
    RelationshipDeleted  { id: RelationshipId, lsn: u64 },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct StorageCapabilities {
    pub bi_temporal: bool,
    pub streaming: bool,
    pub leases: bool,
    pub change_feed: bool,
    pub max_traversal_depth: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageBackendId { FalkorDB, InMemory }
```

### 6.4 Cypher template catalogue

**R-S5.** All Cypher lives in a single module `exocortex-storage/src/cypher.rs`. No other module in the workspace emits Cypher (CR-10). Templates are registered at compile time and referenced by `template_id`.

```rust
// crates/exocortex-storage/src/cypher.rs
//! Every Cypher template the FalkorDB adapter can execute. Registered at
//! compile time; the trait method `query_cypher` refuses templates not listed
//! here. This keeps Cypher confined to one file (CR-10).

use std::collections::HashMap;
use once_cell::sync::Lazy;

pub struct Template {
    pub id: &'static str,
    pub read_only: bool,
    pub cypher: &'static str,
    pub required_params: &'static [&'static str],
}

pub static TEMPLATES: Lazy<HashMap<&'static str, Template>> = Lazy::new(|| {
    let mut m = HashMap::new();
    macro_rules! reg { ($t:expr) => { m.insert($t.id, $t); } }

    reg!(Template {
        id: "upsert_memory",
        read_only: false,
        required_params: &["id", "memory_type_label", "props_json",
                           "visibility", "valid_from", "valid_until",
                           "invalidated_by", "recorded_at", "lsn"],
        cypher: r#"
            MERGE (m:Memory {id: $id})
            SET m.memory_type_label = $memory_type_label,
                m.visibility        = $visibility,
                m.valid_from        = $valid_from,
                m.valid_until       = $valid_until,
                m.recorded_at       = $recorded_at,
                m.invalidated_by    = $invalidated_by,
                m.props_json        = $props_json,
                m.lsn               = $lsn
            RETURN id(m) AS node_id, m.lsn AS lsn
        "#,
    });

    reg!(Template {
        id: "upsert_relationship",
        read_only: false,
        // Note: no MERGE on the relationship (R-S2). We DELETE-then-CREATE so
        // each write is a new relationship row in FalkorDB, giving us stable
        // bi-temporal history.
        required_params: &["rel_id", "from", "to", "kind_label", "props_json",
                           "visibility", "valid_from", "valid_until",
                           "invalidated_by", "recorded_at", "lsn"],
        cypher: r#"
            MATCH (a:Memory {id: $from}), (b:Memory {id: $to})
            OPTIONAL MATCH (a)-[old]->(b) WHERE old.id = $rel_id
            DELETE old
            WITH a, b
            CREATE (a)-[r:RELATES {id: $rel_id,
                                   kind_label: $kind_label,
                                   visibility: $visibility,
                                   valid_from: $valid_from,
                                   valid_until: $valid_until,
                                   recorded_at: $recorded_at,
                                   invalidated_by: $invalidated_by,
                                   props_json: $props_json,
                                   lsn: $lsn}]->(b)
            RETURN id(r) AS edge_id
        "#,
    });

    reg!(Template {
        id: "get_memory_by_id",
        read_only: true,
        required_params: &["id", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.visibility <= $max_visibility
            RETURN m LIMIT 1
        "#,
    });

    reg!(Template {
        id: "traverse_bounded",
        read_only: true,
        required_params: &["from", "kind_labels", "max_depth", "max_nodes", "max_visibility"],
        cypher: r#"
            MATCH (a:Memory {id: $from})
            CALL {
              WITH a
              MATCH path = (a)-[rels:RELATES*1..$max_depth]->(b:Memory)
              WHERE ALL(r IN rels WHERE r.kind_label IN $kind_labels
                                    AND r.visibility <= $max_visibility)
                AND b.visibility <= $max_visibility
              RETURN b, rels LIMIT $max_nodes
            }
            RETURN b, rels
        "#,
    });

    reg!(Template {
        id: "valid_at",
        read_only: true,
        required_params: &["id", "at", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.valid_from <= $at
              AND (m.valid_until IS NULL OR m.valid_until > $at)
              AND m.visibility <= $max_visibility
            RETURN m ORDER BY m.recorded_at DESC LIMIT 1
        "#,
    });

    reg!(Template {
        id: "find_by_entity",
        read_only: true,
        required_params: &["entity_id", "limit", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory)-[:MENTIONS]->(e:Entity {id: $entity_id})
            WHERE m.visibility <= $max_visibility
            RETURN m ORDER BY m.recorded_at DESC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_memories",
        read_only: true,
        required_params: &["after_lsn", "limit"],
        cypher: r#"
            MATCH (m:Memory) WHERE m.lsn > $after_lsn
            RETURN m ORDER BY m.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_relationships",
        read_only: true,
        required_params: &["after_lsn", "limit"],
        cypher: r#"
            MATCH ()-[r:RELATES]->() WHERE r.lsn > $after_lsn
            RETURN r ORDER BY r.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "read_fingerprint",
        read_only: true,
        required_params: &[],
        cypher: r#"
            MATCH (m:_ExocortexMeta {key: 'ontology_fingerprint'})
            RETURN m.value AS fp LIMIT 1
        "#,
    });

    reg!(Template {
        id: "write_fingerprint",
        read_only: false,
        required_params: &["fp"],
        cypher: r#"
            MERGE (m:_ExocortexMeta {key: 'ontology_fingerprint'})
            SET m.value = $fp
        "#,
    });

    m
});

/// Validates a `CypherQuery` before it hits the driver:
///   - `template_id` MUST be registered here
///   - every `required_param` MUST be present
///   - if `read_only` on the template is true, `q.read_only` must also be true
pub fn validate(q: &crate::CypherQuery) -> Result<&'static Template, crate::StorageError> {
    let t = TEMPLATES.get(q.template_id)
        .ok_or_else(|| crate::StorageError::Backend(format!(
            "unregistered cypher template: {}", q.template_id)))?;
    for p in t.required_params {
        if q.params.get(p).is_none() {
            return Err(crate::StorageError::Backend(format!(
                "template `{}` missing param `{p}`", t.id)));
        }
    }
    if t.read_only && !q.read_only {
        return Err(crate::StorageError::Backend(format!(
            "template `{}` is read-only", t.id)));
    }
    Ok(t)
}
```

### 6.5 `FalkorStorage` — implementation skeleton

```rust
// crates/exocortex-storage/src/falkor.rs
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use falkordb::Client as FalkorClient;
use futures::stream::BoxStream;
use redis::AsyncCommands;
use tracing::instrument;

use exocortex_kernel::{
    Memory, MemoryId, Ontology, Relationship, RelationshipId, Visibility,
};

use crate::cypher;
use crate::types::*;
use crate::{Storage, StorageError};

pub struct FalkorStorage {
    client:    FalkorClient,
    graph:     String,                      // "exocortex:{org_id}"
    redis:     redis::aio::MultiplexedConnection,
    node_id:   smol_str::SmolStr,
    ontology:  Arc<Ontology>,
    lsn_key:   String,
}

pub struct FalkorConfig {
    pub falkor_url: String,
    pub redis_url:  String,
    pub graph_name: String,
    pub org_id:     smol_str::SmolStr,
    pub node_id:    smol_str::SmolStr,
}

impl FalkorStorage {
    pub async fn connect(cfg: FalkorConfig, ontology: Arc<Ontology>)
        -> Result<Self, StorageError>
    {
        let client = FalkorClient::open(&cfg.falkor_url).await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let redis_client = redis::Client::open(cfg.redis_url.as_str())
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let redis = redis_client.get_multiplexed_async_connection().await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let this = Self {
            client, graph: cfg.graph_name, redis,
            node_id: cfg.node_id, ontology,
            lsn_key: format!("exocortex:{}:lsn", cfg.org_id),
        };
        this.pin_fingerprint().await?;
        Ok(this)
    }

    /// Read the persisted fingerprint. If empty, write ours. If present and
    /// different, refuse to start (R-D5).
    async fn pin_fingerprint(&self) -> Result<(), StorageError> {
        // pseudocode; fill from cypher templates `read_fingerprint`/`write_fingerprint`.
        Ok(())
    }

    /// Assign the next monotonic backend LSN via Redis INCR (R-S3).
    async fn next_lsn(&self) -> Result<u64, StorageError> {
        let n: u64 = self.redis.clone().incr(&self.lsn_key, 1_u64).await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(n)
    }

    /// Convert a runtime `RelKindId` into the Cypher label used in FalkorDB.
    /// Labels are drawn from the ontology at startup (R-T2 / R-S2).
    fn kind_label(&self, k: exocortex_kernel::RelKindId) -> Result<&str, StorageError> {
        self.ontology.kinds_by_id.get(&k)
            .map(|m| m.display_name.as_str())
            .ok_or_else(|| StorageError::Backend(format!("unknown RelKindId {:?}", k)))
    }
}

#[async_trait]
impl Storage for FalkorStorage {
    #[instrument(skip(self, m))]
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let mt_label = self.ontology.memory_type_names.get(m.memory_type as usize)
            .ok_or_else(|| StorageError::Backend(format!("bad memory_type {}", m.memory_type)))?;
        let q = CypherQuery {
            template_id: "upsert_memory", read_only: false,
            params: serde_json::json!({
                "id": m.id, "memory_type_label": mt_label,
                "props_json": serde_json::to_string(m).unwrap(),
                "visibility": m.visibility as u8,
                "valid_from": m.valid_from, "valid_until": m.valid_until,
                "recorded_at": m.recorded_at, "invalidated_by": m.invalidated_by,
                "lsn": lsn,
            }),
            deadline: now + chrono::Duration::seconds(5),
        };
        let _t = cypher::validate(&q)?;
        // pseudo: self.client.graph(&self.graph).query_with_params(t.cypher, &q.params).await?;
        Ok(CommitRecord { lsn, committed_at: now, node_id: None, edge_id: None })
    }

    async fn upsert_batch(&self, ms: &[Memory], rs: &[Relationship])
        -> Result<Vec<CommitRecord>, StorageError>
    {
        // Single FalkorDB transaction: MULTI/EXEC over the FalkorDB Redis
        // connection. On any per-row failure, the whole batch rolls back.
        let mut out = Vec::with_capacity(ms.len() + rs.len());
        for m in ms { out.push(self.upsert_memory(m).await?); }
        for r in rs { out.push(self.upsert_relationship(r).await?); }
        Ok(out)
    }

    async fn delete_memory(&self, _id: &MemoryId) -> Result<CommitRecord, StorageError> {
        // Soft delete: set `valid_until = now()`; do not remove the node.
        todo!("soft-delete template + LSN emit")
    }

    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let kind_label = self.kind_label(r.kind)?;
        let q = CypherQuery {
            template_id: "upsert_relationship", read_only: false,
            params: serde_json::json!({
                "rel_id": r.id, "from": r.from, "to": r.to,
                "kind_label": kind_label,
                "props_json": serde_json::to_string(r).unwrap(),
                "visibility": r.visibility as u8,
                "valid_from": r.valid_from, "valid_until": r.valid_until,
                "recorded_at": r.recorded_at, "invalidated_by": r.invalidated_by,
                "lsn": lsn,
            }),
            deadline: now + chrono::Duration::seconds(5),
        };
        let _t = cypher::validate(&q)?;
        Ok(CommitRecord { lsn, committed_at: now, node_id: None, edge_id: None })
    }

    async fn delete_relationship(&self, _id: &RelationshipId) -> Result<CommitRecord, StorageError> { todo!() }

    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        let q = CypherQuery {
            template_id: "get_memory_by_id", read_only: true,
            params: serde_json::json!({ "id": id, "max_visibility": Visibility::Org as u8 }),
            deadline: Utc::now() + chrono::Duration::milliseconds(50),
        };
        let _t = cypher::validate(&q)?;
        Ok(None)
    }

    async fn get_memories(&self, _ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> { todo!() }

    async fn traverse(&self, from: &MemoryId, spec: &TraversalSpec) -> Result<Vec<Memory>, StorageError> {
        // CR-6 hard caps before touching Cypher.
        if spec.max_depth > 4 { return Err(StorageError::Backend("max_depth > 4".into())); }
        if spec.max_nodes > 2048 { return Err(StorageError::Backend("max_nodes > 2048".into())); }
        let kinds: Vec<&str> = spec.kinds.iter()
            .map(|k| self.kind_label(*k)).collect::<Result<_, _>>()?;
        let q = CypherQuery {
            template_id: "traverse_bounded", read_only: true,
            params: serde_json::json!({
                "from": from, "kind_labels": kinds,
                "max_depth": spec.max_depth, "max_nodes": spec.max_nodes,
                "max_visibility": spec.visibility_ctx.max_visibility as u8,
            }),
            deadline: Utc::now() + chrono::Duration::milliseconds(50),
        };
        let _t = cypher::validate(&q)?;
        Ok(vec![])
    }

    async fn find_by_entity(&self, entity: &exocortex_kernel::EntityId, filter: &MemoryFilter)
        -> Result<Vec<Memory>, StorageError>
    {
        if filter.limit > 500 { return Err(StorageError::Backend("limit > 500".into())); }
        let q = CypherQuery {
            template_id: "find_by_entity", read_only: true,
            params: serde_json::json!({
                "entity_id": entity, "limit": filter.limit,
                "max_visibility": filter.visibility_ctx.max_visibility as u8,
            }),
            deadline: Utc::now() + chrono::Duration::milliseconds(50),
        };
        let _t = cypher::validate(&q)?;
        Ok(vec![])
    }

    async fn get_state_at(&self, _t: DateTime<Utc>) -> Result<GraphSnapshot, StorageError> { todo!() }

    async fn valid_at(&self, id: &MemoryId, at: DateTime<Utc>) -> Result<Option<Memory>, StorageError> {
        let q = CypherQuery {
            template_id: "valid_at", read_only: true,
            params: serde_json::json!({
                "id": id, "at": at, "max_visibility": Visibility::Org as u8,
            }),
            deadline: Utc::now() + chrono::Duration::milliseconds(50),
        };
        let _t = cypher::validate(&q)?;
        Ok(None)
    }

    async fn query_cypher(&self, q: &CypherQuery) -> Result<ResultSet, StorageError> {
        let _t = cypher::validate(q)?;
        Ok(ResultSet { rows: vec![], scanned_rows: 0 })
    }

    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        Box::pin(futures::stream::empty())
    }

    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        Box::pin(futures::stream::empty())
    }

    async fn find_similar_offline(
        &self, _query: &Embedding, _k: usize, _filter: &MemoryFilter,
    ) -> Result<Vec<(MemoryId, f32)>, StorageError> { todo!() }

    async fn acquire_lease(&self, key: &LeaseKey, ttl: Duration) -> Result<OwnerLease, StorageError> {
        // Redis: SET NX EX with (epoch = INCR of epoch key) as the token.
        let key_str = serde_json::to_string(key).unwrap();
        let redis_key = format!("exocortex:lease:{key_str}");
        let epoch_key = format!("{redis_key}:epoch");
        let epoch: u64 = self.redis.clone().incr(&epoch_key, 1_u64).await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let token = format!("{}:{}", self.node_id, epoch);
        let ok: bool = self.redis.clone().set_nx(&redis_key, &token).await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        if !ok { return Err(StorageError::Backend("lease held by another node".into())); }
        let _: () = self.redis.clone().expire(&redis_key, ttl.as_secs() as i64).await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let now = Utc::now();
        Ok(OwnerLease {
            key: key.clone(), owner_node_id: self.node_id.clone(), epoch,
            acquired_at: now,
            expires_at: now + chrono::Duration::from_std(ttl).unwrap(),
            fencing_token: token.into(),
        })
    }

    async fn renew_lease(&self, _lease: &OwnerLease) -> Result<OwnerLease, StorageError> { todo!() }
    async fn release_lease(&self, _lease: OwnerLease) -> Result<(), StorageError> { Ok(()) }

    async fn subscribe_invalidations(&self, _region: &RegionKey)
        -> Result<BoxStream<'_, Invalidation>, StorageError>
    {
        Ok(Box::pin(futures::stream::empty()))
    }

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            bi_temporal: true, streaming: true, leases: true, change_feed: true,
            max_traversal_depth: 4,
        }
    }
    fn backend_id(&self) -> StorageBackendId { StorageBackendId::FalkorDB }
    fn ontology_fingerprint(&self) -> [u8; 32] { self.ontology.fingerprint.0 }
}
```

### 6.6 `InMemoryStorage` — the test double (v1 deliverable)

Every unit test above the storage seam uses this. **Ship it in v1 or the workspace has no testable subsystems above §6.**

```rust
// crates/exocortex-storage/src/in_memory.rs
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;

use exocortex_kernel::{Memory, MemoryId, Ontology, Relationship, RelationshipId};
use crate::types::*;
use crate::{Storage, StorageError};

pub struct InMemoryStorage {
    memories: Mutex<HashMap<MemoryId, Vec<Memory>>>,             // history stack per id
    rels:     Mutex<HashMap<RelationshipId, Vec<Relationship>>>,
    lsn:      AtomicU64,
    ontology: std::sync::Arc<Ontology>,
}

impl InMemoryStorage {
    pub fn new(ontology: std::sync::Arc<Ontology>) -> Self {
        Self {
            memories: Default::default(),
            rels: Default::default(),
            lsn: AtomicU64::new(0),
            ontology,
        }
    }
    fn next_lsn(&self) -> u64 { self.lsn.fetch_add(1, Ordering::SeqCst) + 1 }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.memories.lock().unwrap();
        let mut m = m.clone();
        m.lsn = exocortex_kernel::LSN::new_backend(lsn);
        store.entry(m.id).or_default().push(m);
        Ok(CommitRecord { lsn, committed_at: Utc::now(), node_id: None, edge_id: None })
    }
    async fn upsert_batch(&self, ms: &[Memory], rs: &[Relationship])
        -> Result<Vec<CommitRecord>, StorageError>
    {
        let mut out = Vec::new();
        for m in ms { out.push(self.upsert_memory(m).await?); }
        for r in rs { out.push(self.upsert_relationship(r).await?); }
        Ok(out)
    }
    async fn delete_memory(&self, _id: &MemoryId) -> Result<CommitRecord, StorageError> { todo!() }
    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn();
        let mut store = self.rels.lock().unwrap();
        let mut r = r.clone();
        r.lsn = exocortex_kernel::LSN::new_backend(lsn);
        store.entry(r.id).or_default().push(r);
        Ok(CommitRecord { lsn, committed_at: Utc::now(), node_id: None, edge_id: None })
    }
    async fn delete_relationship(&self, _id: &RelationshipId) -> Result<CommitRecord, StorageError> { todo!() }
    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        Ok(self.memories.lock().unwrap().get(id).and_then(|h| h.last().cloned()))
    }
    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> {
        let store = self.memories.lock().unwrap();
        Ok(ids.iter().filter_map(|id| store.get(id).and_then(|h| h.last().cloned())).collect())
    }
    async fn traverse(&self, _from: &MemoryId, _spec: &TraversalSpec) -> Result<Vec<Memory>, StorageError> {
        Ok(vec![]) // Cache-backed traversal lives in §8.
    }
    async fn find_by_entity(&self, _e: &exocortex_kernel::EntityId, _f: &MemoryFilter) -> Result<Vec<Memory>, StorageError> { Ok(vec![]) }
    async fn get_state_at(&self, _t: DateTime<Utc>) -> Result<GraphSnapshot, StorageError> { todo!() }
    async fn valid_at(&self, id: &MemoryId, at: DateTime<Utc>) -> Result<Option<Memory>, StorageError> {
        let store = self.memories.lock().unwrap();
        Ok(store.get(id).and_then(|h| h.iter().rev()
            .find(|m| m.valid_from <= at && m.valid_until.map_or(true, |v| v > at))
            .cloned()))
    }
    async fn query_cypher(&self, _q: &CypherQuery) -> Result<ResultSet, StorageError> {
        Err(StorageError::Backend("InMemoryStorage does not implement Cypher".into()))
    }
    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        let all: Vec<_> = self.memories.lock().unwrap().values()
            .flat_map(|h| h.iter().cloned().map(Ok)).collect();
        Box::pin(futures::stream::iter(all))
    }
    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        let all: Vec<_> = self.rels.lock().unwrap().values()
            .flat_map(|h| h.iter().cloned().map(Ok)).collect();
        Box::pin(futures::stream::iter(all))
    }
    async fn find_similar_offline(&self, _q: &Embedding, _k: usize, _f: &MemoryFilter)
        -> Result<Vec<(MemoryId, f32)>, StorageError> { Ok(vec![]) }
    async fn acquire_lease(&self, key: &LeaseKey, ttl: std::time::Duration) -> Result<OwnerLease, StorageError> {
        let now = Utc::now();
        Ok(OwnerLease {
            key: key.clone(), owner_node_id: "in-memory".into(),
            epoch: 1, acquired_at: now,
            expires_at: now + chrono::Duration::from_std(ttl).unwrap(),
            fencing_token: "in-memory:1".into(),
        })
    }
    async fn renew_lease(&self, l: &OwnerLease) -> Result<OwnerLease, StorageError> { Ok(l.clone()) }
    async fn release_lease(&self, _l: OwnerLease) -> Result<(), StorageError> { Ok(()) }
    async fn subscribe_invalidations(&self, _r: &RegionKey) -> Result<BoxStream<'_, Invalidation>, StorageError> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities { bi_temporal: true, streaming: true, leases: false, change_feed: false, max_traversal_depth: 4 }
    }
    fn backend_id(&self) -> StorageBackendId { StorageBackendId::InMemory }
    fn ontology_fingerprint(&self) -> [u8; 32] { self.ontology.fingerprint.0 }
}
```

### 6.7 Implementation steps (M2)

1. **Add support types** (`types.rs`) verbatim from §6.3. `cargo check`.
2. **Add Cypher templates** (`cypher.rs`) verbatim from §6.4. Add unit test `validate_rejects_unknown_template()`.
3. **Add `InMemoryStorage`** (`in_memory.rs`) verbatim from §6.6. Add property test `roundtrip_memories` using `proptest` (10k random memories → `upsert` → `get_memory` returns Some(same)).
4. **Add `FalkorStorage` skeleton** (`falkor.rs`) with `todo!()` bodies. `cargo check`.
5. **Bring up docker-compose** (`crates/exocortex-storage/tests/docker-compose.yml`) with FalkorDB + Redis. Behind `--features integration`.
6. **Fill `pin_fingerprint`** first — fail fast on mismatch. Write an ignored integration test that starts Falkor with an old fingerprint stored, verify the process aborts.
7. **Fill `upsert_memory` and `get_memory`**. Add roundtrip integration test.
8. **Fill `upsert_relationship` and `traverse`**. Add integration test with a 3-hop chain.
9. **Fill `valid_at`**. Add bi-temporal test: insert `Memory` at t0, supersede at t1, verify `valid_at(t0)` returns the original and `valid_at(t1)` returns the successor.
10. **Fill lease methods**. Add chaos test: two workers race for the same lease; only one succeeds; the loser retries after TTL.
11. **Fill `subscribe_invalidations`**. Add end-to-end test: `upsert_memory` on node A results in an `Invalidation::MemoryUpserted` on node B's subscription.

**M2 acceptance criteria** are green when steps 1–11 pass on `cargo test -p exocortex-storage --features integration`.

---

## 7. Ontology — Kernel and Packs

The ontology is a set of Rust enums, structs, macros, and Crepe rules; every surface (MCP tool schemas, OpenAPI, protobuf) generates from the *effective ontology* (kernel plus registered packs) at compile time.

**Shape at a glance.** Heavy envelope, single-string content, typed relationship kinds with rich edge properties. Reasoning power lives in the type tag + typed edges, not in typed payload fields inside memories. This is the key insight from the prior implementation: the harness LLM is better at producing `(type_tag, free_text_content, entities, edge_hints)` than at filling per-type payload schemas correctly. The v1 pack shape is **inherited from the prior MemoryGraph implementation**, validated across five storage backends (FalkorDB, FalkorDBLite, Memgraph, SQLite, Cloud). Three upgrades from prior: entities as a first-class typed field extracted at ingest, bi-temporal on memories (not just relationships), visibility promoted to a required top-level field.

### 7.0 Kernel and Packs

The ontology is split into two layers:

- **Kernel.** Universal machinery: `Memory` and `Relationship` structs; `Visibility`, `Provenance`, and bi-temporal fields; the kind-registration seam (`RelKindId` handles and the `kinds!` macro); the Ingestion Protocol; the type-triple validator; Actions and Functions; the compounding-asset invariants. The kernel does not name any concrete `MemoryType`, `EntityType`, or `RelKindId` — those come from packs.
- **Extension packs.** Rust crates that register concrete types, kinds, metadata, type-triple rules, and Crepe rules against the kernel. Every pack is code, not config: types are Rust enum variants, kinds are `RelKindId` interned handles, rules are Crepe programs compiled alongside the pack crate and hooked into the kernel's stratified rule graph. v1 ships exactly one pack, `exocortex-pack-dev-v1`.

**How a pack registers.** A pack crate uses the kernel's `pack!` macro to declare its ontology surface, then `inventory::submit!` to register itself with the kernel at binary link time:

```rust
pack! {
    name: "exocortex-pack-dev-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        Task, CodePattern, Problem, Solution, Project, Technology,
        Error, Fix, Command, FileContext, Workflow, General, Conversation,
    }

    entity_types! {
        File, Function, Class, Error, Technology, Concept, Person, Project,
        Command, Package, Url, Variable,
    }

    kinds! {
        // Solution (5)
        Solves       => bucket: Solution,   inverse: SolvedBy,   bi: false, default_strength: 0.85,
        Addresses    => bucket: Solution,   inverse: AddressedBy, bi: false, default_strength: 0.70,
        AlternativeTo=> bucket: Solution,   inverse: Self,        bi: true,  default_strength: 0.60,
        Improves     => bucket: Solution,   inverse: ImprovedBy,  bi: false, default_strength: 0.70,
        Replaces     => bucket: Solution,   inverse: ReplacedBy,  bi: false, default_strength: 0.90,
        // ... 43 more kinds across Causal, Context, Learning, Similarity, Workflow, Quality, Integration
    }

    type_triples! {
        Solves    => (Solution | Fix, Problem | Error),
        Addresses => (Solution | Fix, Problem | Error),
        Executes  => (Command, _),
        Modifies  => (Task | Command | Fix, FileContext),
        Creates   => (Task | Command | Fix, FileContext),
        InSession => (_, Conversation),
        // ...
    }

    crepe_rules! {
        // R1: type_from_solves — if a memory Solves another, the source is a Solution
        type_from_solves(a, MemoryType::Solution) <- edge(a, b, Solves), memory(a, _, _);
        // ...
    }
}

inventory::submit! { pack!(exocortex_pack_dev_v1) }
```

**Kernel constants for well-known kinds.** A handful of relationship kinds have kernel-level semantic meaning — the type-inference rules R1–R3 reference them, and the Ingestion Protocol type-triple validator references them. These are declared as kernel constants that a pack MUST bind to at load time:

```rust
// In exocortex-kernel/src/kinds.rs
pub const SOLVES: RelKindId  = RelKindId::from_kernel(0);
pub const FIXES: RelKindId   = RelKindId::from_kernel(1);
pub const CAUSES: RelKindId  = RelKindId::from_kernel(2);
pub const IN_SESSION: RelKindId = RelKindId::from_kernel(3);
// ... a short, closed list; kernel constants are additive-only across kernel major versions.
```

Packs that need to specialize a kernel-constant kind (e.g., a medical pack aliasing `SOLVES` to `TREATS` for a domain-specific display label) do so through the pack's `kinds!` block; the underlying `RelKindId` is stable. Packs that do not reference a kernel constant simply ignore it.

**Non-negotiable pack constraints:**

- **R-Pk1** (pack registration). A pack registers exactly one `PackDef` per crate. Two packs with the same name refuse to link — registration is compile-time exhaustive.
- **R-Pk2** (kernel-constant coverage). Every kernel constant `RelKindId` MUST be bound to a concrete kind by the loaded pack set. Missing bindings refuse to link.
- **R-Pk3** (rule stratification). Every pack's Crepe rules compile alongside the pack crate and are joined into the kernel's stratified rule graph. Packs cannot rebind kernel rules R1–R9; they can add rules that fire on their own kinds.
- **R-Pk4** (fingerprint). The `OntologyFingerprint` (§7.17) is computed over `SHA-256(kernel_defs || sorted(pack_defs))`. Peers with mismatched fingerprints refuse to connect (§9.1).
- **R-Pk5** (what packs cannot do). Packs cannot modify the `Memory` or `Relationship` struct layout; cannot add new `Provenance` variants (those are kernel-only, gated by the compounding-asset invariants of §7.10); cannot bypass the type-triple validator; cannot bypass the Ingestion Protocol; cannot install a hot-path LLM call (there are no LLM calls, R-D6 / CR-19).

The rest of §7 describes the kernel plus the dev-v1 pack in one narrative. Concrete enums (`MemoryType`, `EntityType`, and named kinds) below are dev-v1 pack content, presented here because dev-v1 is what v1 ships. The kernel does not name them.

### 7.1 The `MemoryType` enum

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MemoryType {
    Task,          // A unit of work in progress or completed
    CodePattern,   // A reusable code shape (idiom, refactor, template)
    Problem,       // A described issue, bug, or blocker
    Solution,      // A described fix, workaround, or approach
    Project,       // A named project or codebase
    Technology,    // A tool, framework, library, or service
    Error,         // A specific error message or failure mode
    Fix,           // A specific fix applied to a specific error
    Command,       // A shell command or tool invocation
    FileContext,   // A file's state or role at a point in time
    Workflow,      // A multi-step procedure or process
    General,       // Anything that doesn't fit another type
    Conversation,  // A session wrap-up or dialog fragment
}
```

**Discipline.** `General` is the escape hatch. If a harness cannot decide, it uses `General` rather than misclassifying. `General` memories still participate in graph traversal via their entities and edges; they simply do not participate in type-specific rules.

### 7.2 The `EntityType` enum

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntityType {
    File, Function, Class, Error, Technology, Concept, Person, Project,
    Command, Package, Url, Variable,
}

pub struct EntityRef {
    pub kind: EntityType,
    pub value: SmolStr,       // e.g. "src/auth.rs", "handle_login", "tokio"
    pub normalized: SmolStr,  // canonicalized: lowercase, path-normalized, etc.
}
```

**Upgrade from prior implementation.** Entities are extracted **at ingest** into `Vec<EntityRef>` on the memory, not derived at query time via regex. Extraction runs inside the backend on `commit_wrapup`, uses a compiled regex table per `EntityType`, and yields typed references. Cost is paid once, on write.

**The extraction table (R-T18).** Extraction is a fixed, deterministic table of compiled regex sets — one per `EntityType` — owned by `exocortex-ingest`: file paths, function/class/variable identifiers, error signatures, technology names (from the `Technology` entity dictionary), `package@version` pairs, URLs, and shell-command shapes. Every match yields an `Entity` row (id = content-hash of `(entity_type, normalized_name)`, §2.6.1 `ids.rs`) plus a confidence; ambiguous or overlapping matches carry `Provenance::Extracted { extractor, extraction_confidence }`. The table is data, versioned with the pack; adding a pattern is a pack-version bump and changes the `OntologyFingerprint`.

### 7.3 Relationship kinds — the reasoning surface (`RelKindId`)

Edge labels are not a closed Rust enum. They are **`RelKindId` handles** — interned `u32` identifiers registered by kernel constants (§7.0) and by pack `kinds!` blocks. This is what lets a pack add domain-specific relationship kinds without a kernel fork, while keeping the same compile-time exhaustive-match guarantees a closed enum provides.

The canonical `RelKindId` type, its kernel/pack bit layout, and the kernel constants (`SOLVES`, `FIXES`, `CAUSES`, `IN_SESSION`) are defined once, in §2.6.1 `kinds.rs`. This section is semantics, not a second definition: kernel-space ids (high bit clear) are semantically referenced by kernel rules R1–R9; pack-space ids (high bit set) are assigned by the `pack!` macro at registration.

**Dev-v1 pack: 48 kinds across eight buckets.** The authoritative dev-v1 kind table — every kind with its bucket, inverse, bidirectionality flag, default strength, and kernel-constant bindings — is the `kinds!` block of §7.18. Bucket sizes: Solution 5, Causal 7, Context 9, Learning 6, Similarity 4, Workflow 6, Quality 5, Integration 6. **Inverse labels are implicit:** every `inverse:` target (`SolvedBy`, `Generalizes`, `HasPart`, …) is auto-registered by the `kinds!` macro as a read-only companion kind. Companions are materialized on write (R-T4) but are not among the 48 authored kinds and cannot be authored directly. The `kinds!` macro emits (a) a constant `RelKindId` per named kind, (b) the metadata table entry, (c) an exhaustive-match `kinds_match!` helper the pack's Crepe rules use so that adding a kind without handling it in a rule is a compile error, just like an enum variant. Kinds are interned via `lasso`; the `SmolStr` display name is interned once at load time.

**Deltas from the prior taxonomy, and why.** Five changes, all deliberate: (a) `DependsOn` moved from Workflow to Context — it is a structural dependency, not an execution-order fact, and kernel rule R4 (`transitive_depends_on`) reads it there. (b) `BuildsOn` replaces the prior `Extends` in Learning — evolutionary "A extends B's knowledge over time", distinct from inheritance; pack rule D2 (`transitive_builds_on`) rides it. Kernel rule R6 (`reverse_solves`) does not cover this path; `BuildsOn` is explicit. (c) `ParallelTo` is restored to Workflow — concurrent-branch reasoning matters in a dev pack (parallel work streams, PR fan-out). (d) Effectiveness intelligence — `EFFECTIVE_FOR` / `INEFFECTIVE_FOR` / `PREFERRED_OVER` in the prior taxonomy — now lives in edge `strength`, `success_rate`, and `effectiveness` on `RelationshipProperties`/`Memory` (§7.8, §7.5), not in dedicated kinds. "Was this solution effective?" is answered by reading `success_rate` on `Solves`/`Fixes` edges (§14.3), not by traversing an `EffectiveFor` edge. (e) `Reviews` is folded into `Validates`/`Verifies` — a code review is a validation with a human verifier; the distinction was not load-bearing. Net: 48 kinds preserved.

**R-T1.** Every `RelKindId` in a stored `Relationship` MUST resolve to a registered kind in the effective ontology. The registry is closed at binary link time; new kinds require a new pack version (which changes the `OntologyFingerprint`, §7.17).
**R-T2.** The Cypher label allowlist is generated from the effective ontology at startup; unrecognized labels are rejected at write time.
**R-T3.** `MemoryType::Conversation` (dev-v1 pack) is available in every deployment mode that loads the dev-v1 pack.
**R-T4.** Inverse materialization is symmetric: for any `k` where `k.inverse() == Some(k')`, writing `k(a,b)` writes `k'(b,a)` in the same transaction. Registered by the pack's `kinds!` block.

### 7.4 Metadata for `RelKindId`

`RelMeta` and `RelBucket` are defined once, in §2.6.1 `kinds.rs`. Semantics:

- `display_name` doubles as the **stable ASCII Cypher label** (R-T2); it never changes for a given kind id.
- `inverse` names the auto-registered companion kind (§7.3); `bidirectional` is orthogonal to `inverse` (CR-2).
- `bucket` feeds kernel rule scoping — kernel rules R1–R9 inspect the kernel-canonical buckets and never inspect pack `Extension` buckets.
- `default_strength` applies when an `EdgeHint` omits strength.

The kernel exposes lookups on `Ontology` (`kinds_by_id`), not methods on `RelKindId`, because kinds are interned handles, not enum variants.

### 7.5 The `Memory` struct — heavy envelope, single-string content

The canonical `Memory` is defined once, in §2.6.1 `memory.rs`. Field-group semantics:

- **Identity:** `id` (`MemoryId`), `memory_type` (u8 resolved through the effective ontology — the dev-v1 pack's generated enum is §7.1).
- **Content:** `title` (1..=200), `content` (≥1 char, single free-text payload, harness-produced and harness-consumed), `summary` (optional, ≤500). The reasoning power lives in the envelope and the typed edges — never in typed payload fields inside the memory.
- **Entities:** `context.entities` — extracted at ingest (§7.2), never supplied by the harness.
- **Scoring:** `importance`, `confidence`, `effectiveness` are `F01` ([0,1] enforced at construction); defaults 0.5 / 0.8 at ingest; `usage_count` increments on read.
- **Tenancy:** `visibility` — required, no default (R-T6).
- **Provenance:** §7.9.
- **Bi-temporal:** `valid_from`, `valid_until`, `recorded_at`, `invalidated_by` — on memories, not just edges (upgrade from prior).
- **Backend-only:** `embedding` — stripped before cache/SSE (R-T8).
- **Storage-assigned:** `lsn` (`LSN`, §6.2).

**R-T5.** `title` MUST be 1–200 characters. `content` MUST be ≥1 character. `summary` MUST be ≤500 characters. Violations return `ValidationError` at commit; storage never sees invalid memories.

**R-T6.** `visibility` has no default and no `Option`. The type system forces every writer to specify it. This is the tenancy invariant from §17 lifted into the type system.

**R-T7.** `valid_from` defaults to `recorded_at` if the harness does not supply it. `valid_until` is `None` unless explicitly superseded. Bi-temporal queries (`Storage::valid_at`) respect both fields.

**R-T8.** `embedding` is stripped from every `Memory` before it enters the local cache or the SSE change feed. Client-side `Memory` values always have `embedding: None`.

### 7.6 The `MemoryContext` struct — the entity linkage layer

`MemoryContext` is defined once, in §2.6.1 `memory.rs`. Field groups: project/codebase (`project_id`, `project_path`); code artifacts (`files_involved`, `languages`, `frameworks`, `technologies`); git (`git_commit`, `git_branch`, `working_directory`); session + authorship (`session_id`, `user_id`, `created_by` — `created_by` may differ from `user_id` for agent authorship); tenancy hints (`tenant_id`, `team_id` — visibility itself lives on `Memory`, these are auxiliary); extracted entities; and the escape hatch `additional_metadata`.

**R-T9.** `MemoryContext` fields marked `Option` may be absent; the harness supplies what it knows. Absence is not an error. Only `timestamp` is mandatory and always defaults to `now()` at ingest if unset.

**R-T10.** `additional_metadata` is bounded at 8 KiB serialized. Larger payloads are rejected at commit. This is the pressure valve, not the primary channel.

### 7.7 The `Visibility` enum — required, no default

`Visibility` is defined once, in §2.6.1 `visibility.rs` — a `Copy`, ordered unit enum (`Private` < `Project` < `Team` < `Org` < `Public`). The ordering is load-bearing: storage-level visibility filters are single `visibility <= $max_visibility` comparisons (§6.4), and `Copy` keeps the hot path allocation-free. The label's *subjects* are not carried in the enum — they resolve against `MemoryContext` (`Private` → the `user_id` author; `Project` → members of `project_id`; `Team` → members of `team_id`; §17.2).

**R-T11.** Read paths that see a `Memory` with `Visibility::Public` in v1 log a warning and treat it as `Org`. `Public` is reserved for v2 cross-org sharing; the enum variant exists now so schema evolution is additive.

**R-T11a. No-widening rule for pipeline-derived memories.** Every ingestion source (§7.13) registers a **visibility ceiling** at admission time. Any `Memory` produced by that source MUST have `Visibility` equal to or narrower than the ceiling; the Ingestion Protocol validator rejects the batch otherwise. Widening a source-derived memory's visibility (e.g., promoting from `Team` to `Org`) is only allowed through an explicit `promote_visibility` Action (§7.11) authored by a human and audited — never automatically by a re-ingest. This is a kernel invariant because source authorization at read time does not remain enforceable after materialization; the register-and-ceiling pattern is how Unity Catalog and Foundry both handle the same failure mode.

### 7.8 The `Relationship` struct — first-class typed edges

`Relationship` and `RelationshipProperties` are defined once, in §2.6.1 `relationship.rs`. Semantics beyond the fields: `visibility` defaults to the more restrictive endpoint at ingest; `bidirectional` is derived from `RelMeta` at ingest and stamped for query speed; `description` is optional human-readable context. Edge intelligence — `strength`, `confidence`, `context`, `evidence_count`, `success_rate`, `validation_count`, `counter_evidence_count`, `last_validated` — lives in `RelationshipProperties`; the bi-temporal fields live on the `Relationship` itself and are not duplicated inside properties.

**R-T12.** `evidence_count`, `validation_count`, and `counter_evidence_count` are monotonic non-decreasing within a `(from, to, kind)` tuple; Dreams merges evidence rather than overwriting it.

**R-T13.** For every bucket, `RelationshipProperties::strength` participates in the reasoning-time confidence budget: rule outputs derive from `min(strength_of_evidence_edges)` in Crepe (§10).

### 7.9 Provenance — typed, six variants

Provenance is defined once, in §2.6.1 `provenance.rs` — exactly six variants, closed by R-Pk5:

- `Asserted { author }` — direct assertion by a session harness or an accepted discovery.
- `Derived { rule_id, evidence }` — materialized by a Crepe rule; `evidence` is the supporting `RelationshipId` set.
- `Computed { producer, threshold }` — internal producer, one of the `ComputedProducer` enum: `SimilarityCosine`, `SimilarityHnsw`, `EntityCoOccurrence`, `SessionCoOccurrence`.
- `Extracted { extractor, extraction_confidence }` — extracted from raw text at ingest.
- `Proposed { discovery_id, score }` — discovery proposal; never persists as an edge (§12).
- `ExternalSnapshot(ExternalSnapshot)` — ingested from an external source snapshot (§7.13, §18); carries `source_uri`, `snapshot_id`, `schema_hash`, `observed_at`, `external_key`, `producer_id`. Recorded per assertion, not per source row: the same source row may produce multiple assertions across time, each with its own `snapshot_id` and `observed_at`. This is what preserves bi-temporality when the underlying external table mutates.

**R-T14.** Every `SimilarTo` edge carries `Provenance::Computed`.
**R-T15.** Bi-temporal fields (`valid_from`, `valid_until`, `recorded_at`, `invalidated_by`) survive every round trip.
**R-T16.** `Provenance::Proposed` never appears on a persisted edge. `DiscoveryId` and `RelationshipId` are distinct types.
**R-T16a.** `Provenance::ExternalSnapshot` is the only variant that carries external-system coordinates. Assertions from a re-sync of the same source produce **new** `Memory`/`Relationship` rows with a new `snapshot_id`/`observed_at` and a new `recorded_at`; they do not overwrite prior assertions. This is what preserves the two-time-dimension guarantee across source mutation.

**Motivating example (the two-sync failure mode).** A source row asserts `payments-service → team-payments` at snapshot `s1`, `observed_at = T0`. Three weeks later the same source row now reads `payments-service → team-platform` at snapshot `s2`, `observed_at = T1`. Under a naive assertion model there are two bad options: (a) overwrite the older assertion, which destroys bi-temporality and hides the fact that the org used to believe the payments team owned it; or (b) fabricate a synthetic "retraction" event between `T0` and `T1`, which pollutes bi-temporality with events that never happened. `Provenance::ExternalSnapshot` gives a third option: keep both assertions, both with their own `snapshot_id` and `observed_at`; the org owns the fact that its own external source changed its mind on that date, and Crepe rules that reason over ownership can see the change explicitly.

### 7.10 The ontology as a compounding asset

The thesis of Exocortex: **what compounds is the typed graph, not the machinery that reads bytes.** Palantir's decade-long moat is Foundry's Ontology — typed objects, links, actions, functions — not its warehouse readers, which are replaceable and, in fact, get replaced regularly (Spark → Rubix, Foundry's proprietary compute layer; various parquet readers over the years). The ontology is what an organization pays to build, keep clean, and get right.

Three operational consequences follow, and every kernel invariant in §7 is a consequence of one of them:

**(a) The ontology outlives the backend.** Storage adapters (§6) are pluggable and expected to be replaced (FalkorDB → native Rust store → something else in v3). The kernel's `Memory`, `Relationship`, `Provenance`, `Visibility`, and `RelKindId` shapes do not change under a storage swap. This is why the storage adapter interface is defined against ontology types, not against a Cypher dialect. It is why `OntologyFingerprint` (§7.17) is computed from the ontology, not from the schema of any specific backend.

**(b) The ontology outlives any single ingestion source.** An org that connects Iceberg, then Delta, then a custom REST feed over three years still has one graph. The Ingestion Protocol (§7.13) is the *only* way external data enters, and every entering memory/edge is stamped with `Provenance::ExternalSnapshot` (§7.9). Because provenance is per-assertion, not per-row-in-source, a source's schema evolution or replacement never destroys prior assertions — they remain, `valid_until`-closed, in the graph. This is the difference between "we had a data pipeline" and "we have an ontology."

**(c) The ontology outlives the harness LLM.** No LLM output lands as a stored memory without going through the kernel's `MemoryDraft` shape, the type-triple validator (§7.15), the ingestion protocol (§7.13), and the no-widening visibility rule (§7.7). Model regressions do not corrupt the store; they at most reduce write throughput as invalid drafts are rejected. Model upgrades do not require a graph migration.

Everything a pack CAN do (register kinds, add rules, add types) supports these three properties. Everything a pack CANNOT do (change the `Memory` layout, add `Provenance` variants, bypass validation, install a hot-path LLM call) protects these three properties. When choosing between kernel design options during v1 implementation, the tie-breaker is always: *which option keeps the ontology, and the provenance stamped on it, the compounding asset?*

### 7.11 Actions — typed writes as first-class ontology

Actions are the **only** way the graph is written. Every write path in Exocortex is a named Action with a typed input, a typed output, a permission check, and a provenance stamp. There is no "raw insert" API. This matches the Palantir Foundry model exactly and is what makes the write side auditable.

v1 kernel Actions:

```rust
pub trait Action {
    type Input;
    type Output;
    const NAME: &'static str;                       // stable, human-readable
    const REQUIRED_VISIBILITY_CEILING: Visibility;  // author must be within source ceiling

    fn execute(ctx: &ActionCtx, input: Self::Input) -> Result<Self::Output>;
}

// The four kernel Actions:
pub struct CommitWrapup;      // session-wrapup batch — the first ingestion adapter (§7.13, §13)
pub struct AcceptDiscovery;   // promote a Dreams proposal to a stored edge (§12)
pub struct PromoteVisibility; // human-authored widening; only path around R-T11a
pub struct RetractEdge;       // close valid_until on an edge with reason
```

Every Action call is recorded in the audit log (§21.4) with actor, input, output IDs, and the `OntologyFingerprint` current at execution time. Actions are the transaction boundary; a `CommitWrapup` call is one transaction across the memories, entities, and edges it produces. This is what "typed writes" means in Exocortex: the kernel refuses to store anything not produced by a named Action.

Packs can register additional Actions (e.g., a mortgage pack could add `close_loan`), subject to R-Pk5: they cannot bypass the kernel's `MemoryDraft` shape, type-triple validator, ingestion protocol, or no-widening rule.

### 7.12 Functions — typed reads as first-class ontology

Symmetrically, Functions are the only way the graph is read on the interactive path. Every read is a named Function with a typed input, a typed output, a visibility filter, and a latency budget. There is no ad-hoc Cypher on the hot path; the query pool (§8) only executes registered Functions.

v1 kernel Functions:

```rust
pub trait Function {
    type Input;
    type Output;
    const NAME: &'static str;
    const P50_BUDGET_US: u32;  // for perf CI (§15)
    const P99_BUDGET_US: u32;

    fn execute(ctx: &FunctionCtx, input: Self::Input) -> Result<Self::Output>;
}

// The four kernel Functions:
pub struct SearchMemories;        // text + entity + type filters → ranked Memory IDs
pub struct TraverseRelationships; // k-hop typed traversal with visibility filter
pub struct GetChain;              // provenance chain for a memory (§10)
pub struct ExplainEdge;           // human-readable proof for a Derived edge (Steel; §10)
```

Cross-org and open-web retrieval is **not** a kernel Function. The harness may perform it, may pass the results in as a `MemoryDraft` through a session wrapup, but the interactive Function set is closed and deterministic. This is what makes latency budgets in §15 defensible — no Function reaches for a network, a table reader, or an LLM.

### 7.13 The Ingestion Protocol — how external data enters

Everything that becomes a `Memory` or a `Relationship` enters through the Ingestion Protocol. Session-wrapup is one implementation; the protocol is the general shape.

**The protocol has two parties.** The **producer** is any process that emits proposed memories, entities, and relationships — the session-wrapup client, an external Iceberg/Delta reader worker (v2, §18), a custom domain feed. The **kernel** is the consumer. The producer speaks a versioned typed schema; the kernel validates, stamps, and admits.

**Wire shape (versioned protobuf; canonical schema in §18.6):** an `IngestBatch` carries `org_id`, `source_uri`, `producer_id`, `batch_id` (idempotency key), `ontology_fp`, the registered `ceiling`, `observed_at`, an optional `snapshot` (present iff the source is external), an HMAC over the encoded batch (R-I8), and repeated `ProposedMemory` / `ProposedEntity` / `ProposedRelationship` rows linked by producer-local `draft_key`s. Do not restate the schema anywhere else; §18.6 is authoritative.

**Kernel-side validation (single pipeline; no per-adapter code):**

1. `ontology_fp` matches the kernel's current fingerprint. Reject `INCOMPATIBLE_ONTOLOGY` otherwise.
2. `producer_id` is a registered source with an admission record and a registered `ceiling`. Reject `UNKNOWN_SOURCE` otherwise.
3. Every `ProposedMemory.visibility` ≤ `ceiling`. Reject `VISIBILITY_WIDENING` otherwise (R-T11a).
4. Every `ProposedRelationship` type-triple is permitted by the effective ontology (§7.15). Reject `INVALID_TYPE_TRIPLE` otherwise.
5. `batch_id` has not been previously committed for this `producer_id`. Reject `DUPLICATE_BATCH` otherwise (idempotency).
6. If `snapshot` is present, every produced memory/edge is stamped `Provenance::ExternalSnapshot { source_uri, snapshot.snapshot_id, snapshot.schema_hash, observed_at }`. Otherwise `Provenance::Asserted` (session-wrapup) or `Provenance::Extracted` (entity extraction inside the kernel).
7. Identity derivation (R-T18a) computes stable `MemoryId` values from `(org, source_uri, table_uuid, logical_pk, mapping_version)` when the source supplies them; never from file paths or row offsets.

Session-wrapup (§13) is the reference implementation: the wrapup client produces an `IngestBatch` with `source_uri = "session://…"`, `producer_id = "session-wrapup"`, no `snapshot`, and the `Provenance::Asserted` fallback applies. Every other producer plugs into the same pipeline, which is why session-wrapup is not special-cased in the kernel — it is *the first adapter*.

The protocol is not a read-path concern. Producers write batches; readers use Functions (§7.12). This is the single most important architectural decision in the ontology-vs-machinery split: **the kernel never reads external bytes on an interactive path.** External systems always cross the wire as an `IngestBatch`.

### 7.14 The write-path input shape — what the harness sends

The harness does not construct `Memory` directly. It sends a `MemoryDraft` (which the session-wrapup Action wraps into an `IngestBatch`, §7.13) and the backend produces a `Memory`. This is the seam where entity extraction and defaulting happen.

```rust
pub struct MemoryDraft {
    pub memory_type: MemoryType,
    pub title: SmolStr,
    pub content: String,
    pub summary: Option<SmolStr>,
    pub tags: Vec<SmolStr>,
    pub context: MemoryContextDraft, // subset; missing fields filled from session
    pub importance: Option<F01>,     // defaults 0.5
    pub confidence: Option<F01>,     // defaults 0.8
    pub visibility: Visibility,      // REQUIRED
    pub edge_hints: Vec<EdgeHint>,   // typed relationships to other memories
    pub valid_from: Option<DateTime<Utc>>, // defaults to now
    pub valid_to: Option<DateTime<Utc>>,
    pub external_key: Option<ExternalKey>, // supplied by external-source adapters only
}

pub struct EdgeHint {
    pub to: MemoryId,
    pub kind: RelKindId,              // interned handle (§7.3)
    pub strength: Option<F01>,       // defaults from RelMeta
    pub confidence: Option<F01>,     // defaults 0.8
    pub context: Option<SmolStr>,
}

pub struct ExternalKey {
    pub table_uuid: uuid::Uuid,       // stable per-table id from the source catalog
    pub logical_pk: SmolStr,          // source-declared primary key value
    pub mapping_version: u32,         // increments when the adapter's mapping rules change
}
```

**R-T17.** Every `EdgeHint` is verified at commit: the target memory MUST exist, and the `(from.type, kind, to.type)` triple MUST be permitted by the type-triple table (§7.15). Invalid hints reject the entire draft — no partial commits.

**R-T18.** The backend extracts `entities` from the `MemoryDraft` at commit; the harness does not supply them. Extraction uses regex tables per `EntityType`; ambiguous matches are recorded with `Provenance::Extracted { extraction_confidence }`.

**R-T18a. Identity derivation rule (kernel invariant).** `MemoryId` values assigned to memories from an external source are derived deterministically as `blake3(org_id || source_uri || external_key.table_uuid || external_key.logical_pk || external_key.mapping_version)`. `MemoryId` values MUST NOT be derived from a source's file path, row offset, byte offset, ingest timestamp, or any other quantity that changes when the source is reorganized or re-materialized. This is what makes the graph immune to source layout changes: renaming an Iceberg table's underlying files does not fork every derived memory into a new identity. Adapters that cannot supply an `ExternalKey` (e.g., pure text feeds) fall back to content-hash identity; that fallback is a documented limitation, not a general strategy.

### 7.15 Type-triple rules — what edges are valid

Some relationship kinds only make sense between certain memory types. These are enforced at commit and available to Crepe as facts. The type-triple table lives in the pack (via `type_triples!` in the `pack!` block, §7.0); the kernel provides the validator that consumes it.

```rust
// Kernel-side signature; the pack fills the table.
pub fn is_valid_edge(from: u8, kind: RelKindId, to: u8) -> bool {
    registry::type_triple_check(from, kind, to)
}
```

Example dev-v1 pack entries (from `type_triples!`, §7.18 — the full table):

- `Solves | Addresses → (Solution | Fix, Problem | Error)`
- `Causes → (any, Error | Problem)`; `Triggers → (any, any)` — reasoning constrains, not types
- `Executes → (Command, any)`
- `Modifies | Creates → (Task | Command | Fix, FileContext)`
- `InSession → (any, Conversation)`

**R-T19.** The type-triple table is expressed as a single declarative source (the pack's `type_triples!` block), code-generated into both the runtime validator and the MCP tool schema. Adding a rule requires editing one place; adding a type or a kind is a codegen-driven refactor within the pack.

### 7.16 Scope: dev-v1 pack in v1

**R-T20.** v1 ships the kernel plus `exocortex-pack-dev-v1` — 13 memory types, 12 entity types, 48 relationship kinds. A second-pack v2 target validates the seam (`exocortex-pack-sales-v1` or `exocortex-pack-legal-v1` are the candidate reference domains, decided in v2). The formerly-mentioned "abstract two-tier core" is *not* the v2 target: the v2 target is proving the second pack cleanly co-exists with dev-v1 in one binary. This is an explicit scope choice, not an oversight.

### 7.17 `OntologyFingerprint`

**R-T21.** The kernel computes an `OntologyFingerprint` — SHA-256 over the kernel definitions plus the sorted set of registered `PackDef`s (all types, all `RelKindId` metadata, the type-triple table, and the kernel/pack versions) — and stamps every stored graph, every Action, and every Ingestion Protocol batch. Cluster peers refuse mismatched fingerprints (§9.1). `mcp-client` refuses to connect to a backend with a mismatched fingerprint. Producers with a mismatched fingerprint receive `INCOMPATIBLE_ONTOLOGY` from the Ingestion Protocol. The fingerprint appears on `Storage::ontology_fingerprint()` (§6.1) so mock and real backends can both report it consistently in tests.

### 7.18 `exocortex-pack-dev-v1` — the shipping pack, full skeleton

The dev-v1 pack is a single Rust crate that registers 13 memory types, 12 entity types, 48 relationship kinds, and 6 pack-local Crepe rules (D1–D6) against the kernel.

```rust
// crates/exocortex-pack-dev-v1/src/lib.rs
//! v1's only ontology pack. Adds the 13 MemoryType / 12 EntityType /
//! 48 RelKindId set validated in the prior implementation, plus rules D1..D6.

use exocortex_kernel::{pack, pack_def::*, RelKindId, Visibility};

pack! {
    name: "exocortex-pack-dev-v1",
    version: "1.0.0",
    kernel_min: "1.0.0",

    memory_types! {
        Task, CodePattern, Problem, Solution, Project, Technology,
        Error, Fix, Command, FileContext, Workflow, General, Conversation,
    }

    entity_types! {
        File, Function, Class, Error, Technology, Concept,
        Person, Project, Command, Package, Url, Variable,
    }

    kinds! {
        // Solution bucket (5) — kernel-const SOLVES is bound to `Solves`.
        Solves        => bucket: Solution,   inverse: SolvedBy,     bi: false, default_strength: 0.85, kernel_const: SOLVES,
        Addresses     => bucket: Solution,   inverse: AddressedBy,  bi: false, default_strength: 0.70,
        AlternativeTo => bucket: Solution,   inverse: Self,         bi: true,  default_strength: 0.60,
        Improves      => bucket: Solution,   inverse: ImprovedBy,   bi: false, default_strength: 0.70,
        Replaces      => bucket: Solution,   inverse: ReplacedBy,   bi: false, default_strength: 0.90,

        // Causal bucket (7) — kernel-const FIXES/CAUSES bound below.
        Causes        => bucket: Causal,     inverse: CausedBy,     bi: false, default_strength: 0.85, kernel_const: CAUSES,
        Prevents      => bucket: Causal,     inverse: PreventedBy,  bi: false, default_strength: 0.80,
        Triggers      => bucket: Causal,     inverse: TriggeredBy,  bi: false, default_strength: 0.75,
        LeadsTo       => bucket: Causal,     inverse: FollowsFrom,  bi: false, default_strength: 0.70,
        Enables       => bucket: Causal,     inverse: EnabledBy,    bi: false, default_strength: 0.65,
        Blocks        => bucket: Causal,     inverse: BlockedBy,    bi: false, default_strength: 0.75,
        Fixes         => bucket: Causal,     inverse: FixedBy,      bi: false, default_strength: 0.90, kernel_const: FIXES,

        // Context bucket (9)
        Uses          => bucket: Context,    inverse: UsedBy,       bi: false, default_strength: 0.70,
        Requires      => bucket: Context,    inverse: RequiredBy,   bi: false, default_strength: 0.85,
        DependsOn     => bucket: Context,    inverse: DependedBy,   bi: false, default_strength: 0.75,
        Contains      => bucket: Context,    inverse: ContainedBy,  bi: false, default_strength: 0.70,
        PartOf        => bucket: Context,    inverse: HasPart,      bi: false, default_strength: 0.70,
        InSession     => bucket: Context,    inverse: HasMember,    bi: false, default_strength: 0.80, kernel_const: IN_SESSION,
        InProject     => bucket: Context,    inverse: ProjectHas,   bi: false, default_strength: 0.80,
        WrittenIn     => bucket: Context,    inverse: Powers,       bi: false, default_strength: 0.65,
        Modifies      => bucket: Context,    inverse: ModifiedBy,   bi: false, default_strength: 0.65,

        // Learning bucket (6)
        Teaches       => bucket: Learning,   inverse: LearnedFrom,  bi: false, default_strength: 0.70,
        Demonstrates  => bucket: Learning,   inverse: Self,         bi: true,  default_strength: 0.65,
        Contradicts   => bucket: Learning,   inverse: Self,         bi: true,  default_strength: 0.80,
        Confirms      => bucket: Learning,   inverse: ConfirmedBy,  bi: false, default_strength: 0.75,
        BuildsOn      => bucket: Learning,   inverse: BuiltOnBy,    bi: false, default_strength: 0.75,
        Specializes   => bucket: Learning,   inverse: Generalizes,  bi: false, default_strength: 0.70,

        // Similarity bucket (4)
        SimilarTo     => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.60,
        DifferentFrom => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.55,
        AnalogousTo   => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.55,
        RelatedTo     => bucket: Similarity, inverse: Self,         bi: true,  default_strength: 0.30,

        // Workflow bucket (6)
        Precedes      => bucket: Workflow,   inverse: Follows,      bi: false, default_strength: 0.70,
        ParallelTo    => bucket: Workflow,   inverse: Self,         bi: true,  default_strength: 0.50,
        Executes      => bucket: Workflow,   inverse: ExecutedBy,   bi: false, default_strength: 0.75,
        Creates       => bucket: Workflow,   inverse: CreatedBy,    bi: false, default_strength: 0.75,
        Configures    => bucket: Workflow,   inverse: ConfiguredBy, bi: false, default_strength: 0.65,
        Automates     => bucket: Workflow,   inverse: AutomatedBy,  bi: false, default_strength: 0.75,

        // Quality bucket (5)
        Validates     => bucket: Quality,    inverse: ValidatedBy,  bi: false, default_strength: 0.75,
        Tests         => bucket: Quality,    inverse: TestedBy,     bi: false, default_strength: 0.75,
        Measures      => bucket: Quality,    inverse: MeasuredBy,   bi: false, default_strength: 0.65,
        Documents     => bucket: Quality,    inverse: DocumentedBy, bi: false, default_strength: 0.65,
        Verifies      => bucket: Quality,    inverse: VerifiedBy,   bi: false, default_strength: 0.75,

        // Integration bucket (6)
        IntegratesWith=> bucket: Integration,inverse: Self,         bi: true,  default_strength: 0.70,
        Consumes      => bucket: Integration,inverse: ConsumedBy,   bi: false, default_strength: 0.70,
        Produces      => bucket: Integration,inverse: ProducedBy,   bi: false, default_strength: 0.70,
        Exposes       => bucket: Integration,inverse: ExposedBy,    bi: false, default_strength: 0.65,
        Wraps         => bucket: Integration,inverse: WrappedBy,    bi: false, default_strength: 0.70,
        Bridges       => bucket: Integration,inverse: BridgedBy,    bi: false, default_strength: 0.70,
    }

    type_triples! {
        // Solution
        Solves        => (Solution | Fix, Problem | Error),
        Addresses     => (Solution | Fix, Problem | Error),
        AlternativeTo => (Solution | Fix, Solution | Fix),
        Improves      => (Solution | Fix | CodePattern, Solution | Fix | CodePattern | Task),
        Replaces      => (_, _),
        // Causal
        Causes        => (_, Error | Problem),
        Prevents      => (Solution | Fix | CodePattern, Error | Problem),
        Fixes         => (Fix, Error | Problem),
        Triggers      => (_, _), LeadsTo => (_, _), Enables => (_, _), Blocks => (_, _),
        // Context
        Uses          => (_, Technology | Command | Package),
        Requires      => (_, Technology | Package),
        DependsOn     => (_, _),
        Contains      => (_, _),
        PartOf        => (_, _),
        InSession     => (_, Conversation),
        InProject     => (_, Project),
        WrittenIn     => (CodePattern | FileContext, Technology),
        Modifies      => (Task | Command | Fix, FileContext),
        // Learning
        Teaches       => (_, _), Demonstrates => (_, _), Contradicts => (_, _),
        Confirms      => (_, _), BuildsOn => (_, _), Specializes => (_, _),
        // Similarity
        SimilarTo     => (_, _), DifferentFrom => (_, _), AnalogousTo => (_, _), RelatedTo => (_, _),
        // Workflow
        Precedes      => (_, _), ParallelTo => (_, _), Executes => (Command, _),
        Creates       => (Task | Command | Fix, FileContext),
        Configures    => (_, _), Automates => (Workflow | Command, _),
        // Quality
        Validates     => (_, _), Tests => (_, _), Measures => (_, _),
        Documents     => (_, _), Verifies => (_, _),
        // Integration
        IntegratesWith=> (_, _), Consumes => (_, _), Produces => (_, _),
        Exposes       => (_, _), Wraps => (_, _), Bridges => (_, _),
    }

    // Rules R1-R9 live in the kernel (§10.2). Pack-local rules are D1-D6 and
    // only fire on pack-owned kinds. They MUST NOT bind kernel-const kinds
    // directly by numeric id; they reference them by name so the kernel can
    // inject the interned RelKindId at compile time (see kernel::rules::pack_scope!).
    crepe_rules! {
        // D1: `Fix Fixes Problem` implies `Fix Solves Problem` (subsumption).
        implied_solves(a, b) <- edge(a, b, Fixes), memory(a, MemoryType::Fix, _);
        // D2: `A BuildsOn B` and `B BuildsOn C` implies `A BuildsOn C` (transitivity, k=3 bounded).
        transitive_builds_on(a, c) <- edge(a, b, BuildsOn), edge(b, c, BuildsOn);
        // D3: `A Blocks B` and `B Requires C` implies `A Blocks C` (indirect blocker).
        indirect_blocker(a, c) <- edge(a, b, Blocks), edge(b, c, Requires);
        // D4: contradiction cluster - if A Contradicts B and B Confirms C then A Contradicts C.
        contradiction_propagates(a, c) <- edge(a, b, Contradicts), edge(b, c, Confirms);
        // D5: file-lineage propagation - if a Task Modifies F, and Fix Modifies F later, they share a target.
        shared_target(a, b, f) <- edge(a, f, Modifies), edge(b, f, Modifies), memory(a, _, _), memory(b, _, _), a != b;
        // D6: session cohesion - all memories `InSession S` are candidates for MCR2 grouping.
        session_cohort(m, s) <- edge(m, s, InSession);
    }
}

inventory::submit! { pack!(exocortex_pack_dev_v1) }
```

### 7.19 Implementation steps (M1)

1. **Scaffold `exocortex-kernel/src/ontology.rs`** with the `pack!` macro shell (parse the DSL, generate the `PackDef` value). `cargo check`.
2. **Add `RelKindId`, `MemoryTypeId`, `EntityTypeId`** interning tables. Ids are `NonZeroU16`. Interning happens at pack-load time; ids are stable within a process.
3. **Add kernel constants** (`SOLVES`, `FIXES`, `CAUSES`, `IN_SESSION`) as `RelKindId::from_kernel(N)`. Kernel constant slots 0..15 are reserved; slots 16.. are pack-owned.
4. **Add pack-registration test double**: `exocortex-kernel/tests/pack_registration.rs` registers a fake `TestPack` and asserts (a) kernel constants are bound, (b) `pack!` refuses duplicates, (c) fingerprint changes when a kind is added.
5. **Scaffold `exocortex-pack-dev-v1`** as a new crate. `cargo check`.
6. **Copy the `pack!` block above** verbatim into `exocortex-pack-dev-v1/src/lib.rs`. `cargo check`.
7. **Add `type_triples!` validation**: `Ontology::validate_relationship(from_type, to_type, kind) -> Result<(), KernelError>` (the `InvalidTypeTriple` variant, §2.6.1 `error.rs`) runs the pattern-match table above. Unit-test with 20 legal / 20 illegal triples.
8. **Add `crepe_rules!` compilation**: rules D1–D6 compile with the kernel's rule graph into a single Crepe program per binary. Bench: rule graph compile time <2s for the full v1 pack.
9. **Verify `OntologyFingerprint::compute(&[pack_defs])`** (§2.6.1 `fingerprint.rs`) — SHA-256 over the length-prefixed bincode of each sorted `PackDef`; BLAKE3 is used for `MemoryId` derivation (R-T18a), never for the fingerprint. Stable across process restarts on the same code (property test: 1000 restarts, identical fingerprint).
10. **Add golden-file test** `tests/dev_v1_fingerprint.txt` — fingerprint is pinned; changing the pack updates the golden file, forcing an intentional decision.

**M1 acceptance criteria:** steps 1–10 pass on `cargo test -p exocortex-kernel -p exocortex-pack-dev-v1`.

---

## 8. In-Memory Cache — Local Node

The local cache is authoritative for reads on the node that holds it. Storage is the system of record. The cache reconstructs from storage on cold start and stays coherent through either the SSE change feed (mcp-client) or the cluster invalidation transport (backend-node).

### 8.1 Structure

The full implementation skeleton — `GraphSnapshot`, `LocalCache`, the 2Q state, and the single-writer loop — is §8.4. The published read snapshot carries: the `petgraph::StableGraph`, `by_id` / `by_entity` / `by_type` / `by_tag` indices (roaring bitmaps keyed by interned handles), a `lasso` interner, and the LSN frontier. `CacheVersion` is stamped on every read response (R-M7):

```rust
pub struct CacheVersion {
    pub local_lsn: u64,           // this client's local WAL frontier
    pub backend_lsn: u64,         // backend commits observed so far
    pub node_clocks: HashMap<NodeId, u64>,  // backend-side only
    pub published_at: Instant,
}
```

**R-M1.** Reads MUST NOT block writes and vice versa. Single-writer + `ArcSwap::load` gives lock-free reads.
**R-M2.** External-facing IDs are stable ULID strings. Internal representation is `u64`.
**R-M3.** `StableGraph` gives CSR-adjacent storage. Bidirectional edges stored once with a symmetry flag.
**R-M4.** Hot-path strings use `SmolStr`; labels use `Spur` (lasso handle).
**R-M5.** Hydration is observable: `/health/hydration` returns `{ progress, eta, memories_loaded, relationships_loaded, target_backend_lsn }`.
**R-M6.** Under sustained sync load, applying invalidations MUST NOT starve interactive reads. Backpressure at `sync_apply_queue_max` (default 1000).
**R-M7.** Every read response carries `snapshot_version = { local_lsn, backend_lsn, age_ms }` so callers can detect staleness.
**R-M8.** The cache MUST be reconstructible from storage. Corruption or LSN gap during recovery triggers rebuild. Rebuild is a first-class operation, not an error path.
**R-M9.** The cache holds no information not present in storage OR deterministically recomputable from persisted state.

### 8.2 Local write coordination

Client writes (session-wrapup only in v1):

1. Acquire writer mutex.
2. WAL::append_batch (async fsync scheduled).
3. Assign local LSNs to each item in the batch.
4. Clone `Arc<CacheInner>`, apply batch, set `last_applied_local_lsn`.
5. `ArcSwap::store(new_inner)`.
6. Enqueue batch for background sync to backend.
7. Return { local_lsns } to caller.
8. Release mutex.

Interactive reads never block on step 6 (the network). Reads issued between steps 5 and 8 will see the write via ArcSwap load.

**R-M10.** Local cache MUST be updated after WAL append is durable (fsynced or scheduled with `O_SYNC`), never before.
**R-M11.** WAL entries carry state `{ Pending, Synced { backend_lsn }, Failed }`. On backend ack, entry transitions Pending → Synced. On terminal failure, Failed and surfaced to operator.

### 8.3 The 2Q admission policy

**R-M12.** When cache pressure exceeds `cache_max_bytes`, eviction uses the 2Q algorithm:
- New graphs enter a small **A1in** queue (FIFO, 25% of budget).
- Graphs accessed again before A1in eviction are promoted to a main **Am** queue (LRU, 50% of budget).
- Evicted A1in graphs go to a small **A1out** ghost queue (25% of budget) that remembers recent evictions but stores no data. A hit in A1out re-admits directly to Am on next access.

**Why 2Q:** it's a well-established replacement for LRU that resists scan pollution — a client that briefly needs a huge graph doesn't push its warm graphs out. Same idea as ARC but simpler to implement and tune. Chosen because Exocortex has clear "hot working set + occasional cold graph" access patterns.

**R-M13.** Eviction is graph-atomic — a graph is either fully cached or fully uncached, never partial. This preserves the invariant that in-memory queries always see a coherent per-graph view.

### 8.4 `LocalCache` — full skeleton

```rust
// crates/exocortex-cache/src/lib.rs
//! Lock-free readers, single-writer coordination. The graph itself is a
//! petgraph::StableGraph wrapped in ArcSwap so read-side never blocks.

use std::sync::Arc;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use smol_str::SmolStr;
use tokio::sync::mpsc;

use exocortex_kernel::{
    EntityId, Memory, MemoryId, RelKindId, Relationship, RelationshipId, Visibility,
};
use exocortex_storage::{
    Invalidation, MemoryFilter, RegionKey, Storage, TraversalSpec, VisibilityContext,
};

/// Immutable snapshot of one org's graph. Reads see a consistent view.
pub struct GraphSnapshot {
    pub petgraph: StableGraph<Memory, Relationship>,
    pub by_id:    DashMap<MemoryId, NodeIndex>,
    pub by_entity:DashMap<EntityId, smallvec::SmallVec<[MemoryId; 8]>>,
    pub by_type:  DashMap<u8, roaring::RoaringBitmap>,          // memory_type → ids (§8.1)
    pub by_tag:   DashMap<lasso::Spur, roaring::RoaringBitmap>, // tag → ids
    pub interner: std::sync::Arc<lasso::ThreadedRodeo>,
    pub last_local_lsn:   u64,
    pub last_backend_lsn: u64,
    pub built_at:         chrono::DateTime<chrono::Utc>,
}

pub struct LocalCache {
    graphs:  DashMap<SmolStr, Arc<ArcSwap<GraphSnapshot>>>,   // key = org_id
    tq:      Mutex<TwoQState>,                                // 2Q admission
    writer:  mpsc::Sender<CacheWrite>,                        // single-writer channel
    budget:  usize,                                           // cache_max_bytes
}

struct TwoQState {
    a1in:   std::collections::VecDeque<SmolStr>,   // FIFO, 25% budget
    am:     lru::LruCache<SmolStr, ()>,            // LRU, 50% budget
    a1out:  lru::LruCache<SmolStr, ()>,            // ghost, 25% budget
    bytes:  usize,
}

pub enum CacheWrite {
    Apply(Invalidation),
    Reseed { org: SmolStr, snapshot: Arc<GraphSnapshot> },
    Evict(SmolStr),
}

impl LocalCache {
    pub fn new(budget_bytes: usize) -> (Self, mpsc::Receiver<CacheWrite>) {
        let (tx, rx) = mpsc::channel(1024);
        (Self {
            graphs: DashMap::new(),
            tq: Mutex::new(TwoQState {
                a1in: Default::default(),
                am: lru::LruCache::new(std::num::NonZeroUsize::new(1024).unwrap()),
                a1out: lru::LruCache::new(std::num::NonZeroUsize::new(1024).unwrap()),
                bytes: 0,
            }),
            writer: tx, budget: budget_bytes,
        }, rx)
    }

    /// The single writer loop. Applies invalidations and reseeds serially so
    /// snapshots swap atomically.
    pub async fn run<S: Storage>(&self, storage: Arc<S>, mut rx: mpsc::Receiver<CacheWrite>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                CacheWrite::Apply(inv) => self.apply(inv).await,
                CacheWrite::Reseed { org, snapshot } => {
                    self.graphs.entry(org.clone())
                        .or_insert_with(|| Arc::new(ArcSwap::new(snapshot.clone())))
                        .store(snapshot);
                    self.admit(&org);
                }
                CacheWrite::Evict(org) => { self.graphs.remove(&org); }
            }
        }
    }

    async fn apply(&self, inv: Invalidation) {
        // For any invalidation, copy the current snapshot, mutate the copy,
        // and ArcSwap::store the new Arc. Readers observing the old snapshot
        // continue to see it until they release their Guard.
        //
        // On MemoryUpserted, look up the memory in Storage (single point read)
        // and splice it in. On MemoryDeleted, remove the node. Relationships
        // follow the same pattern.
        //
        // If the org isn't cached, drop the invalidation - future reads will
        // fault it in.
        let _ = inv;
    }

    fn admit(&self, org: &SmolStr) {
        let mut tq = self.tq.lock();
        if tq.am.contains(org) { tq.am.put(org.clone(), ()); return; }
        if tq.a1out.contains(org) { tq.am.put(org.clone(), ()); tq.a1out.pop(org); return; }
        tq.a1in.push_back(org.clone());
        while tq.bytes > self.budget {
            if let Some(evicted) = tq.a1in.pop_front() {
                tq.a1out.put(evicted.clone(), ());
                self.graphs.remove(&evicted);
                // pseudo: subtract sizeof(evicted graph) from tq.bytes
            } else if let Some((evicted, _)) = tq.am.pop_lru() {
                self.graphs.remove(&evicted);
            } else { break; }
        }
    }

    /// Read path — never blocks. Returns None if the org isn't cached; the
    /// caller (mcp-client or backend node) faults it in via `reseed`.
    pub fn get_memory(&self, org: &str, id: &MemoryId, vc: &VisibilityContext) -> Option<Memory> {
        let g = self.graphs.get(org)?;
        let snap = g.load_full();
        let ix = snap.by_id.get(id)?;
        let m = snap.petgraph.node_weight(*ix)?.clone();
        if m.visibility as u8 > vc.max_visibility as u8 { return None; }
        Some(m)
    }

    pub fn traverse(&self, org: &str, from: &MemoryId, spec: &TraversalSpec) -> Vec<Memory> {
        let Some(g) = self.graphs.get(org) else { return vec![]; };
        let snap = g.load_full();
        let Some(start) = snap.by_id.get(from).map(|r| *r) else { return vec![]; };
        // BFS bounded by spec.max_depth and spec.max_nodes.
        let mut out = Vec::new();
        let mut queue = std::collections::VecDeque::from([(start, 0u8)]);
        let mut seen = std::collections::HashSet::from([start]);
        while let Some((n, d)) = queue.pop_front() {
            if out.len() >= spec.max_nodes as usize { break; }
            for e in snap.petgraph.edges(n) {
                let er = e.weight();
                if !spec.kinds.is_empty() && !spec.kinds.contains(&er.kind) { continue; }
                if er.visibility as u8 > spec.visibility_ctx.max_visibility as u8 { continue; }
                let dst = match spec.direction {
                    exocortex_storage::Direction::Out => e.target(),
                    exocortex_storage::Direction::In  => e.source(),
                    exocortex_storage::Direction::Both => e.target(),
                };
                if !seen.insert(dst) { continue; }
                if let Some(m) = snap.petgraph.node_weight(dst) {
                    if m.visibility as u8 <= spec.visibility_ctx.max_visibility as u8 {
                        out.push(m.clone());
                    }
                }
                if d + 1 < spec.max_depth { queue.push_back((dst, d + 1)); }
            }
        }
        out
    }

    pub async fn reseed_from_storage<S: Storage>(&self, storage: &S, org: &SmolStr) {
        // Stream all memories + relationships and rebuild the snapshot.
        let mut g = StableGraph::new();
        let by_id = DashMap::new();
        let by_entity = DashMap::new();
        let mut ms = storage.stream_all_memories().await;
        use futures::StreamExt;
        while let Some(m) = ms.next().await {
            if let Ok(m) = m {
                let ix = g.add_node(m.clone());
                by_id.insert(m.id, ix);
                for e in &m.context.entities { by_entity.entry(*e).or_default().push(m.id); }
            }
        }
        let mut rs = storage.stream_all_relationships().await;
        while let Some(r) = rs.next().await {
            if let Ok(r) = r {
                if let (Some(a), Some(b)) = (by_id.get(&r.from), by_id.get(&r.to)) {
                    g.add_edge(*a, *b, r);
                }
            }
        }
        let snap = Arc::new(GraphSnapshot {
            petgraph: g, by_id, by_entity,
            last_local_lsn: 0, last_backend_lsn: 0,
            built_at: chrono::Utc::now(),
        });
        let _ = self.writer.send(CacheWrite::Reseed { org: org.clone(), snapshot: snap }).await;
    }
}
```

### 8.5 Implementation steps (M3)

1. **Scaffold `exocortex-cache` crate** with `arc-swap`, `dashmap`, `petgraph`, `lru`, `parking_lot`, `smallvec`, `tokio` (all workspace deps, §2.2). Add `dhat` and `stats_alloc` as dev-dependencies for the no-allocation assertion.
2. **Add `GraphSnapshot` and `LocalCache` types** verbatim from §8.4. `cargo check`.
3. **Add `reseed_from_storage`** — property test: reseeding after every write leaves the cache byte-identical to storage.
4. **Add `apply(Invalidation)` bodies** — copy-on-write per op. Bench: 10k invalidations/sec on one core.
5. **Add 2Q admission unit tests**: sequence `[A B C A]` → A survives eviction; sequence `[A B C D E F ... 100 unique]` → A1out ghost hits re-promote hot graphs.
6. **Add snapshot-swap invariant test**: readers holding a `Guard` on the pre-swap snapshot see the pre-swap view for the full length of their scan, even after 1000 subsequent invalidations.
7. **Wire `apply` to `Storage::subscribe_invalidations`** in `backend-node`'s main loop.

**M3 acceptance criteria:** steps 1–7 pass on `cargo test -p exocortex-cache`.

---

## 9. Coherence, Sync, and Cluster

The design has **two coherence protocols** — one inside the backend cluster, one between the backend and its subscribed clients. They share the LSN concept but not the transport.

### 9.1 Backend-internal coherence (cluster nodes)

Backend nodes form a real distributed system.

**Membership: gossip via `chitchat`** (pinned in §2.2) — heartbeats every 1s, failure detection at 5s.

**Invalidation transport: Redis pub-sub with protobuf-encoded payloads** — FalkorDB is already Redis-hosted, so zero net-new infrastructure. Payloads use protobuf (see §9.6); deltas are self-describing (contain the changed memory/relationship snapshot) up to `delta_max_bytes` (default 64KB); larger changes fall back to a `BulkInvalidate` that instructs peers to fetch a range from storage.

**Node-to-node RPC: gRPC over `tonic`** — targeted refetches after a reordering gap, cross-node hydration probes, and cluster-control operations use typed gRPC methods rather than ad-hoc HTTP endpoints. Cluster gossip stays on `chitchat`'s native transport.

**LSN ordering: preserved by the transport OR reordered by the receiver.** Receivers buffer out-of-order deltas up to `reorder_window_ms` (default 200ms); persistent gaps trigger a targeted refetch from storage.

**HMAC-signed deltas.** No cluster admits a peer with a mismatched HMAC key or divergent `OntologyFingerprint`. Peer admission verifies both (a) `OntologyFingerprint` equality — SHA-256 over the effective ontology (kernel + registered packs), §7.17 — and (b) wire-schema version compatibility, §9.6. Mismatch on either rejects the peer before any state exchange.

### 9.2 Chubby-style leases for owner-only operations

Consolidation, Dreams cycles, backfill, and cleanup all require **exactly one owner per graph** to avoid race conditions.

**R-C1.** Owner election uses **Chubby-style leases with a grace period**:

`LeaseKey`, `OwnerLease` (including the Chubby-style `grace_period`), and `RegionKey` are defined once, in §6.3 — leases live on `Storage`, not on a separate coordinator trait (R-C2). Each `LeaseKey` variant names one owner-only role:

```rust
pub enum OwnerRole {   // semantic role carried by each LeaseKey variant (§6.3)
    Consolidation { region: RegionKey },   // per-region (§17.3)
    DreamsCycle   { region: RegionKey },
    CrossRegion,                            // one holder per graph
    Backfill,
    Cleanup,
}
```

**How the grace period works** (borrowed directly from Chubby):

- A node holds a lease for `duration`. It renews before expiry to extend.
- If the node loses contact with the coordinator, it enters a **grace period** during which it MUST NOT commit new owner-only work but MAY finish in-flight work.
- After `duration + grace_period` with no successful renewal, the lease is considered lost. The node aborts any in-flight owner-only work.
- Another node cannot acquire the same lease until `duration + grace_period` has elapsed from the last confirmed renewal. This eliminates the classic split-brain window.

**Why Chubby-style leases:** the pattern is 20+ years old, battle-tested, and specifically designed to prevent two owners on both sides of a network partition. Same idea as `etcd` election, ZooKeeper's ephemeral nodes, and Kubernetes leader-election.

**R-C2.** Owner election runs on **Redis** — the same instance as invalidation pub-sub (principle 6, §0.4). Implementation: `SET NX EX` + `WATCH` for atomic acquisition and renewal. **Single-node `mcp-standalone`** uses an **in-process** coordinator (no Redis dependency in embedded mode); backend cluster deployments always use Redis. `etcd` is not supported; the adapter seam is `Storage`, not `Coordinator` (principle 6).

**R-C3.** Every lease has a monotonic `lease_epoch`. Storage writes made under lease epoch N are tagged with N. A write from a stale lease (epoch N when the current lease is N+1) is rejected by storage — this is a **fencing token**, another well-established distributed-systems technique.

### 9.3 Client-to-backend coherence (SSE change feed)

Local MCP clients are dumb subscribers. No gossip, no leases, no cluster membership.

**Transport: Server-Sent Events over HTTPS.** One long-lived connection per client per graph subscription. Backend pushes `CacheDelta` events as they commit.

**Event stream:**

```
event: delta
data: {
  "backend_lsn": 12345,
  "committed_at": "2025-11-19T…",
  "kind": {
    "MemoryUpserted": { "id": "…", "snapshot": { … } }
  }
}

event: heartbeat
data: { "backend_lsn": 12345 }

event: bulk_invalidate
data: { "affected_graph": "…", "min_lsn": 12000 }
```

**R-C4.** SSE is one-way (backend → client). Clients send writes via `POST /session/wrapup` (batch of memories + relationships). Writes are not part of the SSE channel.

**R-C5.** Every SSE event includes the current `backend_lsn`. Heartbeats fire every 5s so clients detect stalled connections.

**R-C6.** Client reconnect after disconnect passes `?since_lsn=N`. Backend replays deltas after N from its replay buffer (default: last 15 minutes of deltas held in Redis Streams). If N is older than the replay window, backend responds `409 Resync Required` and the client does a targeted rehydration from storage.

**R-C7.** SSE payloads are HMAC-signed with a per-client shared secret. Prevents delta injection.

### 9.4 Consistency semantics

| Read pattern | Guarantee |
|---|---|
| Client reads its own local writes | Strong read-your-writes (local cache always has them) |
| Client reads writes from another client, same graph | Eventual, bounded by SSE lag (typically <500ms) |
| Backend node reads writes from another backend node | Eventual, bounded by cluster invalidation lag (typically <100ms in-datacenter) |
| Client requires cross-client freshness | Passes `?min_backend_lsn=N`; server blocks briefly or returns `503 Cache Stale` |
| Client reads during network partition | Reads succeed from last known snapshot; `snapshot_age_ms` inflates |

**Not offered:** linearizability across clients or across backend nodes. Global serializable transactions. Cross-graph atomicity.

### 9.5 Cache-miss and cache-fill

Not every graph fits in every node's cache. Cache-miss is a first-class case.

**R-C8.** When a query references a graph not currently resident, the node fetches the graph (or its k-hop neighborhood for the query, k=3 default) from storage and hydrates a bounded region into the cache. 2Q admission (R-M12) governs which older graph gets evicted if any.

**R-C9.** Per-graph eviction is graph-atomic — see R-M13.

**R-C10.** Backend cluster: after eviction, cross-node reads for the evicted graph route to a node that holds it (consistent-hash routing at the cluster edge) or trigger rehydration if none does.

### 9.6 Wire protocols

Different boundaries have different constraints. We pick per boundary rather than picking one wire format and forcing it everywhere.

| Boundary | Format | Reason |
|---|---|---|
| MCP stdio (harness ↔ local `mcp-client`) | **JSON-RPC** | MCP spec is JSON-RPC. Same-machine, same-process boundary; parse cost is not the bottleneck. Non-negotiable. |
| Client ↔ backend, SSE change feed | **JSON** (v1) | SSE is text-framed by spec. Binary-over-SSE is a hack; switching to WebSockets or gRPC streaming is a v2 conversation. Current parse cost is not the bottleneck. |
| Client → backend, `POST /session/wrapup` batch | **JSON** | Low-frequency writes. Symmetry with SSE. Same schema surface. |
| Cluster-internal invalidation pub-sub | **Protocol Buffers** | High-throughput fan-out to N peer nodes. Schema evolution matters. Binary size matters when payloads approach the 64KB `delta_max_bytes` limit. |
| Cluster-internal node-to-node RPC | **gRPC over `tonic`** | Typed methods, schema-driven, built on protobuf. Replaces ad-hoc HTTP for targeted refetches and control operations. |
| WAL codec | **`bincode`** (v1); `rkyv` deferred | Local file format, no cross-version negotiation. `rkyv` zero-copy reads deferred until recovery-speed is a measured problem. |

**R-W1.** Cluster-internal protobuf schemas live in a dedicated `exocortex-wire` crate. Generated code compiles into `exocortex-cluster` and any other crate that needs to speak the cluster protocol.

**R-W2.** Protobuf schemas follow additive-only evolution: adding fields is forward-compatible; removing or repurposing field numbers is forbidden. A version-header field on every message identifies the wire schema version. Backend nodes with mismatched wire versions refuse to peer.

**R-W3.** The `OntologyFingerprint` (R-T21, SHA-256 over the effective ontology — kernel + registered packs) and the wire version are distinct. A cluster can upgrade wire protocol without an ontology change and vice versa. Both must match for peer admission.

**R-W4.** Protobuf payloads on Redis pub-sub are HMAC-signed (R-Sec4) with a cluster-shared key before publish. Peers verify HMAC before decoding.

**R-W5.** Client-facing JSON schemas (SSE events, batch-sync bodies) remain the source of truth for the client-backend contract. If a future version moves SSE to a binary format, the JSON schemas serve as the semantic reference for the binary encoding.

**R-W6.** Not chosen and why:

- **Cap'n Proto / FlatBuffers.** Zero-copy reads are attractive but the Rust ecosystem support is thinner than `tonic`/`prost`, and the version-evolution story is worse than protobuf. Not a win at v1 scale.
- **`rkyv` for cluster payloads.** Same reasoning as WAL — the zero-copy benefit only materializes if buffers are held; cluster invalidations are decoded once and applied to the in-memory graph, so we get no benefit.
- **MessagePack.** JSON-shaped binary. If we're breaking wire compat with clients, we get proper schema evolution too (protobuf) rather than shaving bytes off a self-describing format.
- **`borsh`.** Deterministic serialization, no schema evolution. Right for content-addressed payloads like WAL entries; wrong for anything that evolves.

### 9.7 Cluster and SSE skeletons

```rust
// crates/exocortex-cluster/src/node.rs
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

use exocortex_kernel::OntologyFingerprint;
use exocortex_storage::{Storage, Invalidation, LeaseKey, OwnerLease};
use exocortex_wire::{InvalidationEnvelope, WIRE_VERSION};

pub struct ClusterNode<S: Storage> {
    storage:  Arc<S>,
    node_id:  smol_str::SmolStr,
    fp:       OntologyFingerprint,
    hmac_key: [u8; 32],
    tx:       broadcast::Sender<InvalidationEnvelope>,   // local fan-out
}

impl<S: Storage + 'static> ClusterNode<S> {
    pub fn new(storage: Arc<S>, node_id: smol_str::SmolStr,
               fp: OntologyFingerprint, hmac_key: [u8; 32]) -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self { storage, node_id, fp, hmac_key, tx }
    }

    /// Subscribe to storage invalidations, sign them, fan out to peers via
    /// Redis pub-sub and to the local SSE hub.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let region = exocortex_storage::RegionKey {
            org: "*".into(), project: "*".into(), memory_type: 0,
        };
        let mut sub = self.storage.subscribe_invalidations(&region).await?;
        use futures::StreamExt;
        while let Some(inv) = sub.next().await {
            let env = self.envelope(inv);
            let _ = self.tx.send(env.clone());
            // Fan out via Redis pub-sub (pseudo):
            // redis.publish(&format!("exocortex:{}:inv", org), env.encode_to_vec()).await?;
        }
        Ok(())
    }

    fn envelope(&self, inv: Invalidation) -> InvalidationEnvelope {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let mut env = InvalidationEnvelope {
            wire_version: WIRE_VERSION,
            ontology_fingerprint: self.fp.0.to_vec(),
            emitter_node_id: self.node_id.to_string(),
            inv: Some(inv.into()),
            hmac: vec![],
        };
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.hmac_key).unwrap();
        let payload = prost::Message::encode_to_vec(&env);
        mac.update(&payload);
        env.hmac = mac.finalize().into_bytes().to_vec();
        env
    }

    /// Peer admission: verifies wire version, ontology fingerprint, and HMAC
    /// before accepting an inbound invalidation from a chitchat peer.
    pub fn admit(&self, env: &InvalidationEnvelope) -> Result<(), ClusterError> {
        if env.wire_version != WIRE_VERSION { return Err(ClusterError::WireMismatch); }
        if env.ontology_fingerprint.as_slice() != self.fp.0.as_slice() {
            return Err(ClusterError::OntologyMismatch);
        }
        // pseudo: verify HMAC in constant time
        Ok(())
    }

    /// Owner-only lease acquisition (§9.2). Callers pass a fencing token to
    /// every write; storage rejects writes with a stale epoch.
    pub async fn acquire(&self, key: LeaseKey, ttl: std::time::Duration)
        -> Result<OwnerLease, ClusterError>
    {
        self.storage.acquire_lease(&key, ttl).await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }

    pub fn subscribe_local(&self) -> broadcast::Receiver<InvalidationEnvelope> { self.tx.subscribe() }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("wire version mismatch")]    WireMismatch,
    #[error("ontology mismatch")]        OntologyMismatch,
    #[error("hmac verification failed")] HmacFailed,
    #[error("storage: {0}")]             Storage(String),
}
```

SSE surface (client-facing):

```rust
// crates/exocortex-server/src/sse.rs
use axum::{extract::State, response::sse::{Event, KeepAlive, Sse}, routing::get, Router};
use futures::stream::Stream;
use std::{sync::Arc, time::Duration};

pub fn sse_router<S: exocortex_storage::Storage + 'static>(
    cluster: Arc<exocortex_cluster::ClusterNode<S>>,
) -> Router {
    Router::new().route("/v1/changes", get(handler)).with_state(cluster)
}

async fn handler<S: exocortex_storage::Storage + 'static>(
    State(cluster): State<Arc<exocortex_cluster::ClusterNode<S>>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = cluster.subscribe_local();
    let stream = async_stream::stream! {
        let mut rx = tokio_stream::wrappers::BroadcastStream::new(rx);
        use futures::StreamExt;
        while let Some(item) = rx.next().await {
            if let Ok(env) = item {
                let payload = serde_json::to_string(&env).unwrap();
                yield Ok(Event::default().event("inv").data(payload));
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
```

### 9.8 Implementation steps (M5)

1. **Verify `exocortex-wire` compiles** with `proto/cluster.proto` + `proto/sse.proto` from §2.6.3 (`InvalidationEnvelope`, `Invalidation`); export `pub const WIRE_VERSION: u32 = 1;` from the crate root.
2. **Scaffold `exocortex-cluster`** and add `ClusterNode` skeleton verbatim from §9.7. `cargo check`.
3. **Fill `run` loop and `envelope()` HMAC signing**. Bench: 100k envelopes/sec on one core.
4. **Add peer admission tests**: mismatched wire version, mismatched fingerprint, bad HMAC — all rejected.
5. **Wire chitchat peer discovery** (see `chitchat = "0.9"` in §2.2). Two-node in-process integration test using `chitchat::spawn_chitchat` with `SO_REUSEPORT` on 127.0.0.1.
6. **Wire Redis pub-sub**: `redis.publish("exocortex:{org}:inv", envelope)`; local subscriber pipes into `LocalCache::apply`.
7. **Add SSE router** verbatim from §9.7. End-to-end test: upsert on backend → SSE client observes `inv` event within 50ms.
8. **Add lease chaos test**: two `ClusterNode`s race for `LeaseKey::Dreams { org, region }`; only one succeeds; the loser retries after TTL and observes the winner's fencing token change.

**M5 acceptance criteria:** steps 1–8 pass on `cargo test -p exocortex-cluster --features integration`.

---

## 10. Reasoning Layer — Two Languages

No LLM anywhere. Explanation traces are Steel.

### 10.1 Division of labor

| Question | Language | Why |
|---|---|---|
| "What does the graph imply?" | **Crepe / Datalog** | Declarative, stratified, fixpoint-idempotent |
| "How should belief evolve on evidence?" | **Steel / Scheme** | Procedural, zero-copy Rust interop |
| "Why did we conclude X?" | **Steel / Scheme** | Traverses `Provenance::Derived { rule, depth }` chains |
| "What is persisted?" | Storage layer (Cypher) | Storage language, not a reasoner |

**R-L1.** Declarative fact derivation → Crepe. Procedural belief evolution or graph-traversal explanation → Steel. Cypher is never the answer to "how do we reason about this?"
**R-L2.** No rule is written in Cypher. The 9-rule catalogue is 8 Crepe + 1 Steel (R6). Three additional Steel programs handle belief evolution: reinforce, decay, detect_contradiction. One Steel program handles explanation traces: explain_edge.

### 10.2 Crepe rule catalogue

9 rules across type-inference, transitive-closure, affinity, and problem-solution bridge — 8 Crepe and 1 Steel (R6, listed in §10.4 for symmetry). Compile-time Datalog, no runtime interpretation, k=3 fact scoping on the interactive path.

### 10.3 Steel — belief evolution and explanation

Programs: reinforce, decay, detect_contradiction, explain_edge. Explanation walks the derivation chain backwards emitting structured trees. If the harness wants prose from an explanation tree, the harness's LLM renders it — Exocortex only produces the tree.

### 10.4 The rule catalogue

| ID | Rule | Layer | Tier |
|---|---|---|---|
| R1 | `type_from_solves` | **Crepe** | AllTiers |
| R2 | `type_from_fixes` | **Crepe** | AllTiers |
| R3 | `type_from_causes` | **Crepe** | AllTiers |
| R4 | `transitive_depends_on` | **Crepe** | AllTiers |
| R5 | `transitive_requires` | **Crepe** | AllTiers |
| R6 | `reverse_solves` | **Steel** | AllTiers |
| R7 | `co_occurrence_affinity` | **Crepe** | Enrichment |
| R8 | `problem_solution_bridge` | **Crepe** | Enrichment |
| R9 | `similar_tags_affinity` | **Crepe** | Enrichment |
| — | `reinforce` | **Steel** | Always |
| — | `decay` | **Steel** | Always |
| — | `detect_contradiction` | **Steel** | Always |
| — | `explain_edge` | **Steel** | Always |

### 10.5 Execution model

**R-L3.** Reasoning is **asynchronous by default** in v1. Session-wrapup writes enqueue reasoning; results arrive at the client via the SSE change feed as derived edges land in storage.

**R-L4.** Interactive reads augment on-the-fly with a small k=1 or k=2 Crepe pass over the query's neighborhood (`crepe_kfact_hop` = 2 for interactive, 3 for enrichment). This is the read-side reasoning — no writes.

**R-L5.** Backfill and cleanup are owner-only operations on the backend, gated by leases (§9.2).

**R-L6.** Every Crepe rule is idempotent by Datalog semantics. Every Steel rule is tested for idempotency.

**R-L7.** Reasoning queue overflow is observable, never silent — `exocortex_reasoning_dropped_total{graph}` metric + WARN log with memory ID and graph.

### 10.6 Reasoning engine skeleton

```rust
// crates/exocortex-reasoning/src/engine.rs
//! Two-language runtime. Crepe rules run at compile-time-fixed strata; Steel
//! embeds a small Scheme VM per session for explanation traces.

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn, instrument};

use exocortex_kernel::{Memory, MemoryId, RelKindId, Relationship, RelationshipId};
use exocortex_storage::Storage;

pub struct ReasoningEngine<S: Storage> {
    storage:  Arc<S>,
    tx_work:  mpsc::Sender<ReasoningWork>,
    rx_work:  tokio::sync::Mutex<mpsc::Receiver<ReasoningWork>>,
    k_hop:    u8,     // 2 = interactive, 3 = enrichment
}

pub enum ReasoningWork {
    KHopOver { seed: MemoryId, k: u8 },
    SessionWrapup { memories: Vec<MemoryId> },
}

impl<S: Storage + 'static> ReasoningEngine<S> {
    pub fn new(storage: Arc<S>, queue_depth: usize, k_hop: u8) -> Self {
        let (tx, rx) = mpsc::channel(queue_depth);
        Self { storage, tx_work: tx, rx_work: tokio::sync::Mutex::new(rx), k_hop }
    }

    pub async fn enqueue(&self, w: ReasoningWork) {
        if self.tx_work.try_send(w).is_err() {
            metrics::counter!("exocortex_reasoning_dropped_total").increment(1);
            warn!("reasoning queue full; dropping work");
        }
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            let w = { let mut r = self.rx_work.lock().await; r.recv().await };
            let Some(w) = w else { break; };
            match w {
                ReasoningWork::KHopOver { seed, k }         => self.k_hop_reason(seed, k).await,
                ReasoningWork::SessionWrapup { memories }   => self.session_reason(&memories).await,
            }
        }
    }

    #[instrument(skip(self))]
    async fn k_hop_reason(&self, seed: MemoryId, k: u8) {
        // 1. Gather the k-hop neighborhood via storage.traverse (bounded).
        // 2. Load facts into a Crepe program (compile-time-generated struct
        //    per pack — harvested by the kernel's `pack!` macro, §2.6.1).
        // 3. Run the fixed-point; collect derived facts.
        // 4. For each derived Relationship not already present, write it back
        //    with Provenance::Derived { rule_id, evidence: [ids] }.
        let _ = seed; let _ = k;
    }
    async fn session_reason(&self, ms: &[MemoryId]) { let _ = ms; }
}
```

Steel embedding for explanation:

```rust
// crates/exocortex-reasoning/src/explain.rs
// Crate: steel-core (§2.2). Verify its lib name/root path at M0 task 9.
use steel::steel_vm::engine::Engine;
use exocortex_kernel::{ExplainEdge, RelationshipId};

pub struct ExplainEngine { vm: Engine }
impl ExplainEngine {
    pub fn new() -> Self {
        let mut vm = Engine::new();
        // Register FFI: (memory-of id) → alist, (edge-of id) → alist,
        //               (parents-of edge-id) → list of edge ids
        // pseudo: vm.register_fn("memory-of", |id: String| { ... });
        Self { vm }
    }
    pub fn explain(&mut self, edge: RelationshipId) -> ExplainEdge {
        // Walk the derivation chain from the edge back to the input facts
        // using the registered FFI. Return an ExplainEdge tree with
        // rule_id, input_edges, timestamps.
        let _ = edge; ExplainEdge::default()
    }
}
```

### 10.7 Implementation steps (M4)

1. **Scaffold `exocortex-reasoning`** with `crepe` and `steel-core` (workspace pins, §2.2). Rule harvesting lives in the kernel's `pack!` macro (§2.6.1) — there is no separate macros crate.
2. **Compile R1–R9 (kernel, §10.4) + D1–D6 (dev-v1 pack, §7.18)** into a single Crepe program; the kernel's `pack!` macro harvests `crepe_rules!` blocks from every registered pack. `cargo check`; assert compile time <2s.
3. **Add `ReasoningEngine` skeleton** verbatim from §10.6. `cargo check`.
4. **Add `k_hop_reason` bodies**: bounded traversal → load into Crepe → derive → write back with `Provenance::Derived { rule_id, evidence }`.
5. **Idempotency property test**: apply the same input set twice; second run derives zero new relationships.
6. **Latency bench**: `k_hop_reason(seed, k=2)` over a 128-node neighborhood → p50 <300µs, p99 <2ms on a single core.
7. **Add `ExplainEngine`** with FFI-registered `(memory-of id)`, `(edge-of id)`, `(parents-of edge-id)`; walk a derivation chain of depth 5 in <5ms.
8. **Wire into ingest**: on every session-wrapup submit, enqueue `SessionWrapup { memories }` after storage commit.

**M4 acceptance criteria:** steps 1–8 pass on `cargo test -p exocortex-reasoning`.

---

## 11. Background — MCR² Rate Reduction

Consolidation is the load-bearing operation in Dreams, and MCR² is the metric that makes it honest. This section establishes the theory before §12 uses it.

### 11.1 The problem MCR² solves

A memory graph that keeps growing becomes less useful, not more. Two failure modes:

- **Collapse** — everything drifts toward one big blob in embedding space. Types blur, retrieval returns the same few high-connectivity memories for every query, the graph loses discriminative power.
- **Sprawl** — thousands of near-duplicate memories accumulate in every cluster. Storage grows, retrieval gets slow, and the developer sees the same idea rephrased ten different ways when they search.

Aggressive pruning (the design goal in §12) can make either failure mode worse if it's not measured. **What we need is a scalar that says whether the graph is well-structured after a change** — and that goes up when structure improves, down when it degrades. That scalar is MCR².

### 11.2 Maximal Coding Rate Reduction — the theory

MCR² comes out of Yi Ma's group at UC Berkeley. The lineage runs in three steps, and it matters which is being cited for which claim:

**Step 1 — the coding-rate function (2007).** Ma, Derksen, Hong & Wright, *Segmentation of Multivariate Mixed Data via Lossy Data Coding and Compression*, IEEE TPAMI. Rooted in Shannon rate–distortion theory: how many bits does it take to encode a set of vectors to within precision ε? They derived

```
R(Z) = ½ log det( I + (d / (n · ε²)) · Z Zᵀ )
```

and used it to segment mixed data into subspaces. This is the formula the production `_coding_rate()` implementation matches: `½ log det(I + α · Σ)`, computed via `slogdet` for numerical stability. The 2007 paper originates the *measurement*, not the objective.

**Step 2 — MCR² as a learning principle (2020).** Yu, Chan, You, Song & Ma, *Learning Diverse and Discriminative Representations via the Principle of Maximal Coding Rate Reduction*, NeurIPS 2020 (arXiv:2006.08558). This is the actual origin of the name and the objective. It turns the 2007 measurement into something to maximize:

```
ΔR = R(Z) − R^c(Z | Π)

where:
  Z          = the full matrix of embedding vectors, one per memory
  Π          = the partition of Z into classes (memory types, in our case)
  R(Z)       = total coding rate — bits to encode Z as one set at distortion ε
  R^c(Z|Π)   = compact coding rate — bits to encode each class separately
  ΔR         = rate reduction — bits saved by knowing the class assignment
```

The intuition is a tension between two forces: **expand** the whole dataset so it occupies as much space as possible (high total rate), while **compress** each class into as tight a subspace as possible (low per-class rate). Maximize the gap and the classes get pushed into orthogonal, low-dimensional subspaces — what the paper calls a *linear discriminative representation*. It was proposed as a principled alternative to cross-entropy, which optimizes classification accuracy but says nothing about the geometry of what it learns.

**Step 3 — downstream constructions.** ReduNet (Chan, Yu, You, Qi, Wright & Ma, *ReduNet: A White-box Deep Network from the Principle of Maximal Coding Rate Reduction*, JMLR 2022) unrolls gradient ascent on MCR² into a fully white-box network where every layer has a derivation rather than a hyperparameter. CRATE (Yu et al., *White-Box Transformers via Sparse Rate Reduction*, 2023) extends the same construction to transformer architectures. Exocortex uses neither — they're mentioned so a reader familiar with the family knows this is the same objective, not a coincidence of names.

Mechanically, both `R(Z)` and `R^c(Z|Π)` reduce to log-determinants of covariance matrices — `R(Z)` from the covariance of the whole set, `R^c(Z|Π)` from a weighted sum of per-class covariances. Higher ΔR means the classes occupy well-separated, low-volume subspaces. Lower ΔR means the classes overlap or one class has swelled to dominate the space.

### 11.3 Why this fits Exocortex — diagnostic, not objective

The repurposing is the interesting part. In its home context (Yu et al. 2020, ReduNet, CRATE), ΔR is a **training objective** — you differentiate through it and it shapes the representation. Exocortex uses the same quantity as a **diagnostic** — we never optimize against it, we measure it around consolidation sessions.

Why that works:

1. Our ontology is **already a partition**. Every memory has exactly one `MemoryType`, so Π is given for free. In the original setting the partition is either the training label or something the network is learning to produce; here it's a schema fact.
2. The embeddings are **already computed for search**. Computing ΔR adds a matrix operation over already-materialized data, no new pipeline.
3. Consolidation — the operation whose quality we most need to check — changes exactly the right variables: memory count, per-class variance, and cross-class overlap. Merging, abstracting, and pruning all modify Σ in ways that show up in ΔR.

The intuition maps cleanly onto what we care about:

- **ΔR goes up** when a consolidation makes the graph better-structured: merging redundant memories tightens per-class subspaces, abstracting concrete instances into a general pattern moves an outlier into a coherent class, pruning noise reduces intra-class variance.
- **ΔR goes down** when a consolidation degrades the graph: merging two memories that should have stayed separate collapses subspace dimensions, over-abstraction blurs class boundaries, aggressive pruning strips discriminative signal.

**This means every consolidation session is scored.** We compute ΔR before, run the merge/abstract/prune/strengthen actions, compute ΔR after. If `mcr2_after < mcr2_before` the session degraded the graph and the operator has grounds to roll back (or at minimum, to investigate). The same quantity that says "this representation separates its classes well" answers "did last night's consolidation leave the graph better-organized than it found it." That's what makes the claim in Dreams (§12) — that a session lowering ΔR has degraded the graph — actually checkable rather than rhetorical.

### 11.4 Bridge to surprise / novelty

MCR² also connects to the surprise metric used by Dreams' contradiction detector:

- **High surprise on ingest** (a new memory sits far from all existing centroids) tends to **expand the subspace** for its class — genuine new information. ΔR usually goes up when this memory is added.
- **Low surprise on ingest** (the memory sits near a dense existing cluster) tends to **reinforce an existing subspace** — redundancy, a merge candidate. Consolidation is where redundant memories get compressed and ΔR is preserved or improved.

Surprise is the write-time signal. MCR² is the consolidation-time metric. They're two views of the same underlying geometry.

### 11.5 What MCR² does NOT do

Worth being explicit to keep the design honest:

- **MCR² is not a loss function we're training against.** We're not doing MCR²-guided representation learning. We're using an off-the-shelf metric to *score* consolidation decisions made by other logic (HDBSCAN clustering, threshold rules).
- **MCR² is not a merge decision by itself.** It tells us whether a session helped. Which specific memories to merge is a clustering problem answered by HDBSCAN + threshold rules from the ontology.
- **MCR² depends on the embedding model.** A ΔR value computed under `bge-small` is not comparable to a ΔR computed under `bge-large`. Every stored MCR² value is stamped with the embedding model ID + version (R-Mcr1, CR-4). Cross-model comparisons are prohibited at the type level.
- **MCR² needs enough data to be meaningful.** For very small graphs (dozens of memories) ΔR is noisy. The `dreams_min_memories` gate (default 2) is generous but should probably be higher in practice — around 50 — before MCR² deltas are informative. This is a tuning parameter, not a correctness issue.

### 11.6 The `MCR2Engine` API

```rust
pub struct MCR2Engine {
    epsilon: f32,           // distortion parameter; default 0.5
    embedding_model: EmbeddingModelId,
}

pub struct MCR2Value {
    pub delta_r: f32,
    pub total_rate: f32,          // R(Z)
    pub class_rates: HashMap<MemoryType, f32>,   // per-class R^c(Z|Π_k)
    pub compact_rate: f32,        // R^c(Z|Π)
    pub n_memories: usize,
    pub embedding_model: EmbeddingModelId,
    pub computed_at: DateTime<Utc>,
}

impl MCR2Engine {
    pub fn compute(&self, memories: &[MemoryWithEmbedding]) -> MCR2Value;

    /// True when ΔR suggests the graph would benefit from consolidation.
    /// Heuristic: intra-class variance high relative to inter-class distance.
    pub fn should_consolidate(&self, current: &MCR2Value) -> bool;

    /// Ranked merge candidates — pairs of memories within the same class
    /// whose merge is predicted to increase ΔR the most.
    pub fn identify_merge_candidates(&self, memories: &[MemoryWithEmbedding])
        -> Vec<MergeCandidate>;
}

pub struct MergeCandidate {
    pub a: MemoryId,
    pub b: MemoryId,
    pub predicted_delta_r_gain: f32,
    pub cosine_similarity: f32,
}
```

### 11.6.1 Graph-sparsity diagnostic

ΔR measures whether the *embedding* geometry is class-separated. It does not measure whether the *graph* is sparse. Consolidation can improve ΔR while producing a link hairball (every memory linked to every other memory in its cluster), and the metric would not catch that. §11.9 discusses why full SRR does not apply here (embeddings are frozen); the pragmatic answer is a separate sparsity diagnostic computed over the graph adjacency, not over `Z`.

```rust
pub struct GraphSparsity {
    /// Average out-degree over asserted+derived edges, excluding SimilarTo.
    pub avg_out_degree: f32,
    /// Per-memory-type median out-degree. Detects type-local hairballs.
    pub median_out_degree_by_type: HashMap<MemoryType, f32>,
    /// Fraction of memories with out-degree above `hairball_threshold`
    /// (default 32). Hairball detection.
    pub hairball_fraction: f32,
    /// Confidence-weighted density: sum of edge confidences / (n * (n-1)).
    /// Cluster-scoped; reported per HDBSCAN cluster.
    pub weighted_density_by_cluster: HashMap<ClusterId, f32>,
    pub n_memories: usize,
    pub n_edges: usize,
    pub computed_at: DateTime<Utc>,
}

impl MCR2Engine {
    /// Cheap: O(E) pass over the edge set. No embeddings needed.
    pub fn compute_sparsity(&self, storage: &dyn Storage, scope: RegionKey)
        -> GraphSparsity;
}
```

Graph sparsity is reported alongside ΔR and stamped into `ConsolidationResult` as `sparsity_before` and `sparsity_after`. It is a diagnostic, not a training signal — same status as ΔR.

**R-Mcr1.** Every `MCR2Value` MUST carry `embedding_model`. Reads that mix models return an error (`CrossModelComparison`).

**R-Mcr2.** `should_consolidate()` and `identify_merge_candidates()` are hints, not commands. Consolidation still runs its own clustering and threshold logic; the MCR² engine informs but does not decide.

**R-Mcr3.** A consolidation session that produces `mcr2_after < mcr2_before − tolerance` (default tolerance `0.01`) emits a WARN log and records `regression: true` in the `ConsolidationResult`. Optionally the operator can enable auto-rollback via `--consolidation-rollback-on-regression`. Default is warn-only.

**R-Mcr4.** MCR² computation is CPU-bound and MUST NOT run on the interactive path. It runs inside the Dreams cycle, on the owner backend node, off any request-response path.

**R-Mcr5.** Every consolidation session MUST compute `GraphSparsity` before and after the merge/abstract/prune/strengthen phase. Both values are stamped into `ConsolidationResult` (`sparsity_before`, `sparsity_after`) alongside `mcr2_before`, `mcr2_after`. Computation is `O(E)` and runs on the owner node inside the Dreams cycle.

**R-Mcr6.** A consolidation session that produces `hairball_fraction_after > hairball_fraction_before + hairball_tolerance` (default `0.05`) emits a WARN log and records `hairball_regression: true` in `ConsolidationResult`. This is the sparsity equivalent of R-Mcr3. Auto-rollback obeys the same operator flag as ΔR regressions. Rationale: consolidation is not supposed to make the graph denser; if it does, either the merge heuristic is too aggressive or the clustering is producing spurious co-membership.

### 11.7 Complexity and cost

Computing MCR² requires log-determinants of `d × d` covariance matrices where `d` is the embedding dimension (384 for `bge-small`, 1024 for `bge-large`).

- **Total-rate log-det:** `O(d³)` per computation — 57M ops for d=384, 1B ops for d=1024. Sub-second on modern CPUs even for d=1024.
- **Per-class log-dets:** `O(k · d³)` where `k` is the number of classes (13 memory types in v1). Still sub-second.
- **Merge candidate scoring:** each candidate's predicted ΔR gain can be computed via rank-1 updates to the class covariance, avoiding full recomputation — `O(d²)` per candidate rather than `O(d³)`.

**In practice:** Dreams cycles run when a region crosses its trigger predicate (§12.2), typically hours to days apart per region rather than a nightly wall-clock cycle. Even a region with 100k memories has an MCR² computation that finishes in under a minute. The engine is not a bottleneck at v1 scale.

### 11.8 References

Ordered by role in the lineage.

1. **Ma, Derksen, Hong & Wright.** *Segmentation of Multivariate Mixed Data via Lossy Data Coding and Compression.* IEEE TPAMI, 2007.
   Origin of the coding-rate function `R(Z) = ½ log det(I + α Z Zᵀ)`. Rate–distortion foundation. Not yet MCR².
2. **Yu, Chan, You, Song & Ma.** *Learning Diverse and Discriminative Representations via the Principle of Maximal Coding Rate Reduction.* NeurIPS 2020. [arXiv:2006.08558](https://arxiv.org/abs/2006.08558).
   **Primary citation for MCR².** Introduces `ΔR = R(Z) − R^c(Z|Π)` as a learning objective; the origin of the name and the principle. This is what the Exocortex implementation cites in its docstrings.
3. **Chan, Yu, You, Qi, Wright & Ma.** *ReduNet: A White-box Deep Network from the Principle of Maximal Coding Rate Reduction.* JMLR, 2022.
   Unrolls gradient ascent on MCR² into a white-box network. Referenced for context; Exocortex does not use ReduNet layers.
4. **Yu, Buchanan, Pai, Chu, Wu, Tong, Haeffele & Ma.** *White-Box Transformers via Sparse Rate Reduction (CRATE).* 2023. [arXiv:2306.01129](https://arxiv.org/abs/2306.01129); extended version [arXiv:2311.13110](https://arxiv.org/abs/2311.13110).
   Adds an explicit sparsity term to give **Sparse Rate Reduction (SRR)**: `ΔR(Z) − λ‖Z‖₀`. Derives a transformer architecture (CRATE) as unrolled optimization of SRR. Relevant to Exocortex because graph memories are structurally sparse in their linkage; SRR is the natural successor objective, discussed in §11.9.
5. **He, Huang, Meng, Qi, Xiao & Li.** *Graph Cut-guided Maximal Coding Rate Reduction for Learning Image Embedding and Clustering (CgMCR²).* ACCV 2024. [openaccess.thecvf.com](https://openaccess.thecvf.com/content/ACCV2024/html/He_Graph_Cut-guided_Maximal_Coding_Rate_Reduction_for_Learning_Image_Embedding_ACCV_2024_paper.html).
   Jointly optimizes normalized-cut clustering and MCR², so the clustering step and the rate-reduction metric are coherent by construction. Directly parallel to Exocortex’s HDBSCAN + MCR² pairing; see §11.9.
6. **Hu, Zou & Xu.** *An In-depth Investigation of Sparse Rate Reduction in Transformer-like Models.* 2024. [arXiv:2411.17182](https://arxiv.org/abs/2411.17182).
   Empirically evaluates SRR as a complexity measure across model variants; finds a positive correlation with generalization that outperforms path-norm and sharpness-based baselines. Direct empirical support for using rate reduction as a **diagnostic**, not only as a training loss — which is exactly Exocortex’s use.
7. **Buchanan, Pai, Wang & Ma.** *Principles and Practice of Deep Representation Learning* (also subtitled *A Mathematical Theory of Memory*; earlier title *Learning Deep Representations of Data Distributions*). Open-source textbook, 2025–2026. [ma-lab-berkeley.github.io/deep-representation-learning-book](https://ma-lab-berkeley.github.io/deep-representation-learning-book/).
   Consolidates the full lineage — rate distortion, MCR², SRR, CRATE — into a single reference. Authoritative survey citation; the Exocortex codebase’s `dreams/mcr2.py` docstring cites the v1 title of this book as its textbook reference alongside Yu et al. 2020.

**Citation-hygiene note.** Any external-facing docstring should cite Yu et al. NeurIPS 2020 as the origin of the MCR² objective, Ma et al. TPAMI 2007 as the origin of the underlying coding-rate function, and Buchanan et al. as the textbook survey.

### 11.9 Where the field went after 2020

The rate-reduction line of work did not stop at MCR². Three developments matter for Exocortex, though **none of them displace the v1 design** — they clarify what MCR² is doing here and set up what a v2 could adopt.

**Sparse Rate Reduction (SRR).** The 2023 CRATE work extends MCR² by adding an explicit sparsity term, giving `ΔR(Z) − λ‖Z‖₀`. The intuition is that a good representation is not only class-separated (MCR²’s job) but also sparse in what each token uses. This maps unusually well onto a memory graph: consolidated memories should link to a *few* strongly-related neighbors, not densely to everything. Adopting SRR as an additional diagnostic — a per-cycle report of the sparsity of memory–memory link structure, alongside the current ΔR — is the most natural v2 evolution of §11.

**Graph Cut-guided MCR² (CgMCR²).** ACCV 2024 introduces joint optimization of normalized-cut clustering and MCR². Exocortex today does HDBSCAN first, MCR² second, and treats the pair as diagnostic — which is fine, but the two steps are not coherent by construction. CgMCR² shows how to make them coherent: the clustering step provides `Π`, and MCR² shapes the embedding so `Π` becomes cleaner, and the loop iterates. For v1 this is out of scope (we do not train embeddings), but if v2 ever fine-tunes an embedding model on the org graph, CgMCR² is the reference architecture.

**SRR as a generalization measure.** Hu, Zou & Xu (2024) is the cautionary but supportive citation. They ask: is SRR actually optimized in trained CRATE models, and does it correlate with generalization at all? Empirically — yes on both counts, and SRR outperforms path-norm and sharpness-based complexity measures as a predictor of generalization. This matters here because Exocortex’s premise is that ΔR is a **meaningful signal about representation quality**, not just a loss function. That claim is not self-evident. The 2024 investigation gives independent empirical support for treating rate reduction as a legitimate diagnostic, which is exactly what R-Mcr3 (regression flag) relies on.

**What v1 actually adopts.** `ΔR = R(Z) − R^c(Z|Π)` à la Yu et al. 2020 stays the primary metric. On top of that, v1 ships a **graph-sparsity diagnostic** (§11.6.1, R-Mcr5, R-Mcr6) that captures the *spirit* of SRR — sparsity of the consolidated representation — without requiring the frozen-embedding-incompatible `λ‖Z‖₀` term. Consolidation now has two guardrails: ΔR (embedding geometry) and hairball fraction (graph density). Both are stamped into `ConsolidationResult`, both flag regressions, both obey the same rollback flag.

**What v1 defers, and why.**

- **Full SRR** (`ΔR − λ‖Z‖₀`) is defined over the token/embedding matrix in a trainable transformer. Exocortex uses a pluggable frozen embedding model, so `‖Z‖₀` on frozen embeddings is a constant with no lever to pull. Adapting the sparsity term to the graph adjacency matrix instead is a real research question, not a straightforward port. R-Mcr5/R-Mcr6 ship the pragmatic version of the idea now; the formal SRR objective waits until we can justify the math.
- **CgMCR²** requires a training loop that jointly optimizes clustering assignments and embedding parameters. There is no embedding network to fine-tune in v1. Adopting it means picking an embedding architecture we own, building a training pipeline, deciding the fine-tuning corpus, handling model-version-stamping across fine-tunes (R-Mcr1), and accepting that consolidation quality depends on training quality. That is a v2 workstream, not a v1 patch — see open question 13.
- **Hu, Zou, Xu (2024)** does not add a new mechanism; it validates that rate reduction correlates with generalization. That validation is already banked as the empirical justification for R-Mcr3.


---

## 12. Dreams — Event-Driven Consolidation, Zero Reporting

Dreams is not a scheduler. It is an **event-driven worker** that drains a queue of regions whose write activity has crossed a trigger threshold. Quiet hours are a *preference*, not a schedule (§12.2).

### 12.1 What Dreams does

The Dreams cycle runs on a backend node holding an `OwnerRole::DreamsCycle` lease for a graph. It does two things:

**Consolidation (writes to graph).**
1. Fetch memories and embeddings.
2. Compute `mcr2_before` via `MCR2Engine::compute` (§11.6).
3. Cluster via HDBSCAN.
4. MERGE / ABSTRACT / PRUNE / STRENGTHEN via Storage. Merge candidates are informed by `MCR2Engine::identify_merge_candidates`.
5. Create SimilarTo edges @ threshold 0.85 with `Provenance::Computed { producer: SimilarityHnsw, threshold: 0.85 }`. Ingest-time cosine seeding (`SimilarityCosine`) is deferred: writing SimilarTo on the hot path would put embedding compute on the write critical path, which violates R-Lat3. All SimilarTo edges land in Dreams. (See open question 12 for revisiting this once ingest embedding cost is measured.)
6. Compute `mcr2_after`.
7. Write ConsolidationResult audit record. If `mcr2_after < mcr2_before − tolerance`, mark `regression: true` (R-Mcr3).

**Discovery (proposes, never writes edges).**
- Cross-domain finder
- Temporal echo finder
- Orphan finder
- Transitive finder (excludes pairs already derived by R4/R5)

Discovery writes `Discovery` records with `Provenance::Proposed`. These are structured proposals — `{ id, kind, endpoints: (MemoryId, MemoryId), quality, via_types, discovered_at, discovery_cycle_id }`. **No prose. No LLM narration.**


**R-Dr1.** Discovery MUST NOT write to the graph. The type system prevents it: `Storage` has no `write_discovery` method that produces an edge.

**R-Dr2.** A `Discovery` is a proposal. Responses carry NO `RelationshipId`. Acceptance produces an asserted edge whose `context` references the discovery ID.

**R-Dr3.** Consolidation is owner-only, gated by `OwnerRole::Consolidation` lease with epoch fencing (R-C3).

**R-Dr4.** `ConsolidationResult` stamped with `{ session_id, user_id, started_at, completed_at, memories_input, memories_output, mcr2_before, mcr2_after, sparsity_before, sparsity_after, merged, abstracted, pruned, strengthened, owner_node_id, lease_epoch, regression, hairball_regression }`.

**R-Dr5.** `mcr2_before` and `mcr2_after` stamped with embedding model ID and version. Cross-model comparisons prohibited at the type level.

**R-Dr6.** Discovery quality computed once via `Discovery::rate_quality()`. Metrics emit the same value. Test asserts equality.

**R-Dr7.** Transitive discovery excludes already-derived pairs. Records `via_types: (RelKindId, RelKindId)`.

**R-Dr8.** If the harness wants prose about a discovery — a narrative "why is this interesting?" — the harness fetches the discovery + endpoint memories via MCP tools and narrates in its own LLM. Exocortex ships structured data only.

### 12.2 Trigger model — write-counters, not clocks

Dreams runs when a region has accumulated enough new material to be worth consolidating, not on a wall-clock schedule. This keeps compute proportional to churn: an idle region never runs, a hot region runs as soon as it's worth running.

**Trigger predicate.** Per region `(project_id, memory_type)`, the backend maintains three counters incremented by every commit:

```rust
pub struct RegionWriteCounters {
    pub memories_since_last_cycle: u32,
    pub edges_since_last_cycle: u32,
    pub seconds_since_last_cycle: u64, // wall clock, updated on read
}

pub struct DreamsTrigger {
    pub memory_threshold: u32,   // default 1000
    pub edge_threshold: u32,     // default 5000
    pub age_floor_days: u32,     // default 30 — forces a cycle on stale-but-live regions
    pub min_interval_hours: u32, // default 6  — rate limit (R-MT17)
}

impl DreamsTrigger {
    pub fn should_fire(&self, c: &RegionWriteCounters) -> bool {
        let min_interval = (self.min_interval_hours as u64) * 3600;
        if c.seconds_since_last_cycle < min_interval { return false; }
        c.memories_since_last_cycle >= self.memory_threshold
            || c.edges_since_last_cycle    >= self.edge_threshold
            || c.seconds_since_last_cycle  >= (self.age_floor_days as u64) * 86400
    }
}
```

**Transport.** Counter increments publish on the same Redis instance that carries invalidation pub-sub (principle 6, §0.4). The `TriggerWatcher` task on each backend node subscribes to `exocortex:writes:{region}` and evaluates `should_fire` locally. When a region fires, the watcher `RPUSH`es the region key onto a single shared Redis list `exocortex:dreams:queue`. The `DreamsWorker` on the owner-elected node `BLPOP`s from the queue. One queue, one worker per region, exactly-once processing enforced by the region lease (R-C1, R-C3).

**R-Dr12.** Dreams triggers are **event-driven**, not scheduled. There is no cron, no timer thread, no wall-clock scheduler. The only wall-clock element is the `age_floor_days` predicate, which forces a cycle on regions that have been quiet long enough to be worth re-consolidating anyway (drift detection).

**R-Dr13.** Region counters reset atomically on cycle completion via a Lua script that also removes the region from the queue's in-flight set. This prevents double-firing when a cycle finishes near a threshold.

**R-Dr14.** Quiet-hours preference. Each org may declare a preferred consolidation window in its canonical timezone (default UTC, `dreams_prefer_hours = 02:00–06:00`). When the queue has a backlog outside preferred hours, Dreams still runs — the preference does not block progress; it only reorders queue draining when the queue is short. Rationale: **an event-driven system that refuses to work outside preferred hours is a scheduled system with extra steps.**

**R-Dr15.** Queue backpressure. `exocortex:dreams:queue` is bounded by `dreams_queue_max` (default 1000 region entries). If the queue saturates, the watcher drops the newest fire and increments `exocortex_dreams_queue_dropped_total{region}`. The region will re-fire on the next increment past threshold; no data is lost, only the fire signal.

**R-Dr16.** All four `RegionWriteCounters` and `DreamsTrigger` fields are per-region, tunable per org via `dreams_trigger.*` config. Defaults hold unless operators override.

### 12.3 Aggressive pruning is a first-class concern

**R-Dr9.** Consolidation's PRUNE phase is expected to be **aggressive** — the design assumption is that captured memories are pruned to a much smaller durable core over time. Prune decisions cascade through the change feed as `MemoryDeleted` deltas; clients apply and evict.

**R-Dr10.** Every prune decision is auditable via `ConsolidationResult`. A pruned memory's ID and reason are retained even after the memory itself is deleted, so "why is this gone?" is always answerable from the audit log.

**R-Dr11.** Aggressive pruning is governed by MCR². A prune batch that would drop `mcr2_after` below `mcr2_before − tolerance` is flagged (R-Mcr3). The metric prevents "prune-happy" cycles from silently degrading the graph.

### 12.4 Dreams engine skeleton

```rust
// crates/exocortex-dreams/src/lib.rs
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn, instrument};

use exocortex_kernel::{MemoryId, RelationshipId};
use exocortex_storage::{Storage, LeaseKey, RegionKey, OwnerLease};

use crate::mcr2::{GraphSparsity, MCR2Value};   // §11.6, §11.6.1
use crate::trigger::{DreamsTrigger, RegionWriteCounters};  // §12.2

pub struct DreamsEngine<S: Storage> {
    storage: Arc<S>,
    counters: dashmap::DashMap<RegionKey, RegionWriteCounters>,  // §12.2
    dreams_trigger: DreamsTrigger,                               // §12.2
    tolerance: f32,              // R-Mcr3 ΔR regression tolerance (default 0.01)
    hairball_tolerance: f32,     // R-Mcr6 (default 0.05)
    rollback_on_regression: bool,// operator flag (default false)
    tx_fire: mpsc::Sender<RegionKey>,
    rx_fire: tokio::sync::Mutex<mpsc::Receiver<RegionKey>>,
    node_id: smol_str::SmolStr,
}

/// The audit record stamped per cycle — carries every field R-Dr4 mandates.
pub struct ConsolidationResult {
    pub session_id:      smol_str::SmolStr,
    pub user_id:         Option<smol_str::SmolStr>,
    pub started_at:      chrono::DateTime<chrono::Utc>,
    pub completed_at:    chrono::DateTime<chrono::Utc>,
    pub region:          RegionKey,
    pub memories_input:  u32,
    pub memories_output: u32,
    pub mcr2_before:     MCR2Value,       // §11.6 — carries embedding_model (R-Dr5)
    pub mcr2_after:      MCR2Value,
    pub sparsity_before: GraphSparsity,   // §11.6.1 (R-Mcr5)
    pub sparsity_after:  GraphSparsity,
    pub merged:          Vec<MemoryId>,
    pub abstracted:      Vec<MemoryId>,
    pub pruned:          Vec<(MemoryId, PruneReason)>,
    pub strengthened:    Vec<RelationshipId>,
    pub rewired:         Vec<RelationshipId>,
    pub owner_node_id:   smol_str::SmolStr,
    pub lease_epoch:     u64,
    pub regression:         bool,   // R-Mcr3
    pub hairball_regression: bool,  // R-Mcr6
}

#[derive(Clone, Debug)]
pub enum PruneReason {
    Redundant, Superseded, Stale, LowValue,
}

impl<S: Storage + 'static> DreamsEngine<S> {
    pub fn new(storage: Arc<S>, dreams_trigger: DreamsTrigger,
               tolerance: f32, hairball_tolerance: f32,
               rollback_on_regression: bool,
               node_id: smol_str::SmolStr) -> Self {
        let (tx_fire, rx_fire) = mpsc::channel(1024);
        Self { storage, counters: DashMap::new(), dreams_trigger,
               tolerance, hairball_tolerance, rollback_on_regression,
               tx_fire, rx_fire: tokio::sync::Mutex::new(rx_fire), node_id }
    }

    pub async fn on_write(&self, region: RegionKey) {
        let mut e = self.counters.entry(region.clone()).or_default();
        e.memories_since_last_cycle += 1;
        if self.dreams_trigger.should_fire(&e) {
            // Production resets via the R-Dr13 Lua script; in-process reset here.
            *e = RegionWriteCounters::default();
            drop(e);
            let _ = self.tx_fire.try_send(region);
        }
    }

    pub async fn run(self: Arc<Self>) {
        while let Some(region) = { let mut r = self.rx_fire.lock().await; r.recv().await } {
            match self.try_consolidate(&region).await {
                Ok(res) => info!(?res, "consolidation ok"),
                Err(e)  => warn!(?e, "consolidation failed"),
            }
        }
    }

    #[instrument(skip(self))]
    async fn try_consolidate(&self, region: &RegionKey) -> anyhow::Result<ConsolidationResult> {
        // Region-scoped lease: the key names the full region incl. memory_type (R-MT6).
        let lease_key = LeaseKey::Dreams {
            org: region.org.clone(),
            region: format!("{}:{}", region.project, region.memory_type).into(),
        };
        let lease = self.storage.acquire_lease(&lease_key, Duration::from_secs(60)).await?;
        // Scope: only memories in this region; k=3 bounded neighborhood per anchor.
        let anchors = self.select_anchors(region).await?;
        let mcr2_before = self.score_region(region).await?;
        let sparsity_before = self.sparsity(region).await?;

        let mut res = ConsolidationResult {
            session_id: format!("dream:{}", uuid::Uuid::new_v4()).into(),
            user_id: None,
            started_at: chrono::Utc::now(), completed_at: chrono::Utc::now(),
            region: region.clone(),
            memories_input: anchors.len() as u32, memories_output: anchors.len() as u32,
            mcr2_before: mcr2_before.clone(), mcr2_after: mcr2_before,
            sparsity_before: sparsity_before.clone(), sparsity_after: sparsity_before,
            merged: vec![], abstracted: vec![], pruned: vec![],
            strengthened: vec![], rewired: vec![],
            owner_node_id: self.node_id.clone(), lease_epoch: lease.epoch,
            regression: false, hairball_regression: false,
        };

        for anchor in &anchors {
            self.merge(&mut res, anchor, &lease).await?;
            self.rewire(&mut res, anchor, &lease).await?;
            self.strengthen(&mut res, anchor, &lease).await?;
            self.abstract_up(&mut res, anchor, &lease).await?;
            self.prune(&mut res, anchor, &lease).await?;
        }
        res.mcr2_after = self.score_region(region).await?;
        res.sparsity_after = self.sparsity(region).await?;
        res.completed_at = chrono::Utc::now();
        if res.mcr2_after.delta_r < res.mcr2_before.delta_r - self.tolerance {
            res.regression = true;   // R-Mcr3
            if self.rollback_on_regression {
                warn!("MCR2 degraded {} -> {} - rolling back",
                      res.mcr2_before.delta_r, res.mcr2_after.delta_r);
                self.rollback(&res, &lease).await?;
            }
        }
        if res.sparsity_after.hairball_fraction
            > res.sparsity_before.hairball_fraction + self.hairball_tolerance {
            res.hairball_regression = true;   // R-Mcr6
        }
        self.write_audit(&res).await?;
        self.storage.release_lease(lease).await?;
        Ok(res)
    }

    async fn select_anchors(&self, _r: &RegionKey) -> anyhow::Result<Vec<MemoryId>> { Ok(vec![]) }
    async fn score_region(&self, _r: &RegionKey) -> anyhow::Result<MCR2Value> { todo!("§11.6") }
    async fn sparsity(&self, _r: &RegionKey) -> anyhow::Result<GraphSparsity> { todo!("§11.6.1") }
    async fn merge(&self, _res: &mut ConsolidationResult, _a: &MemoryId, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn rewire(&self, _res: &mut ConsolidationResult, _a: &MemoryId, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn strengthen(&self, _res: &mut ConsolidationResult, _a: &MemoryId, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn abstract_up(&self, _res: &mut ConsolidationResult, _a: &MemoryId, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn prune(&self, _res: &mut ConsolidationResult, _a: &MemoryId, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn rollback(&self, _res: &ConsolidationResult, _l: &OwnerLease) -> anyhow::Result<()> { Ok(()) }
    async fn write_audit(&self, _res: &ConsolidationResult) -> anyhow::Result<()> { Ok(()) }
}
```

### 12.5 Implementation steps (M8)

1. **Scaffold `exocortex-dreams`** crate. `cargo check`.
2. **Add `DreamsEngine` skeleton** verbatim from §12.4.
3. **Implement `select_anchors`**: rank regional memories by (in-degree × recency-decay); take top 32. Bench <5ms per region on 10k-memory region.
4. **Implement `score_region` (MCR²)**: pull node embeddings from storage's offline embedding table (populated at ingest per §11), compute MCR² over the anchor cover set.
5. **Implement `merge` and `rewire`**: duplicate detection via cosine similarity ≥0.92 AND same-triple; the merged memory carries `Provenance::Derived { rule_id: "dreams:merge", evidence }` naming the dream cycle, and the absorbed `MemoryId`s are retained in `ConsolidationResult.merged` so "why is this gone?" stays answerable (R-Dr10).
6. **Implement `abstract_up`**: create parent `Memory` when ≥3 siblings share a type + entity cover ≥0.6.
7. **Implement `prune`**: mark `valid_until = now()` for memories flagged Redundant/Superseded/Stale/LowValue with audit trail.
8. **Implement `rollback`**: bi-temporal, never destructive — close `valid_until = now()` on every row the cycle wrote, then write a `ConsolidationResult` with `regression: true` linked to the dream_id; the audit trail makes the rollback reviewable.
9. **End-to-end test**: seed 500-memory region with 40 duplicates → run consolidation → assert ≥35 merged, MCR² non-degrading, dream_id auditable.
10. **Chaos test**: two `DreamsEngine`s race for the same region lease; only one runs; the loser's counter increments continue.

**M8 acceptance criteria:** steps 1–10 pass on `cargo test -p exocortex-dreams --features integration`.

---

## 13. Session Capture Contract

### 13.1 What a session-wrapup is

A `SessionWrapup` is a batch of 1-5 memories produced by the harness at the end of a coding session. Structured JSON. Every field typed against the effective ontology (kernel + registered packs). The wrapup is a thin envelope around `Vec<MemoryDraft>` — `MemoryDraft` is the canonical write-path input defined in §7.14, and `EdgeHint` (§7.14) is how relationships are declared. Wrapup does not define its own draft shape. On the wire, the session-wrapup client wraps this batch into an `IngestBatch` (§18.1) with `producer_id = "session-wrapup"` and submits it to `IngestService.Submit`.

```rust
pub struct SessionWrapup {
    pub session_id: SessionId,
    pub user_id: UserId,
    pub project: Option<ProjectId>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub memories: Vec<MemoryDraft>,        // §7.14; 1-5 typically
    pub context: SessionContext,           // files, technologies, entities — session-level, distinct from Memory.context
}
```

**Relationships travel inside `MemoryDraft.edge_hints`** (§7.14). There is no separate `RelationshipDraft` on the wrapup — every edge is declared as an `EdgeHint` on the memory it originates from. **Entities are extracted by the backend at commit** (R-T18); the harness does not supply them.

**R-Sc1.** The MCP tool `exocortex.end_session` accepts a `SessionWrapup`. The Exocortex server validates against the schema, assigns local LSNs, stores locally, and enqueues sync.

**R-Sc2.** The harness's LLM does the extraction. Exocortex does no LLM inference on the wrapup path.

**R-Sc3.** Wrapup payloads have a soft cap (`session_wrapup_max_memories`, default 10). Batches beyond the cap are truncated with a WARN log. The client additionally hard-rejects batches outside 1..=5 before touching the wire (§13.5); the server cap is defense in depth.

### 13.2 Wrapup triggers

The harness decides when a session ends. Exocortex supports three signals:

**Primary — agent directives in configuration files.** The harness's system prompt or `AGENTS.md` / `CLAUDE.md` / equivalent includes a directive block titled *Memory (Exocortex)*:

> **Memory (Exocortex)**
>
> At the end of any working session, before context compaction, or when the user signals we're wrapping up, call the MCP tool `exocortex.end_session` with a structured wrapup:
>
> - accomplished: what durable outcome was produced
> - decided: what design or architectural choices were made
> - fixed: what problems were solved
> - open: what remains to do
> - context: which files, functions, or technologies mattered
>
> Follow the schema returned by `exocortex.wrapup_schema`.

**Secondary — MCP resource for discoverable directives.** The Exocortex server exposes `exocortex://directives/session-wrapup` as an MCP resource that agents auto-load on connect. Agents that support MCP resources see the directive without any config file edit.

**Tertiary — lifecycle hooks.** For agents with lifecycle hooks (Claude Code's `SessionEnd`, `PreCompact`, etc.), a hook shim calls `exocortex.end_session`. If the hook cannot produce a structured wrapup, it produces a **raw transcript** payload and the client-side `mcp-client` returns an error asking the caller to structure it — the backend does NOT call an LLM.

**R-Sc4.** Exocortex ships example directive snippets for the top three coding harnesses (Claude Code, Codex, Cursor). Adoption is manual — copy into your agent config or point at the MCP resource.

**R-Sc5.** No wrapup is fabricated by Exocortex. If the harness doesn't call `end_session`, no memory is captured. The system is honest about capture cadence.

### 13.3 Offline capture

**R-Sc6.** When `mcp-client` is offline (no backend connectivity), session-wrapup writes complete locally: WAL append, cache update, `local_lsn` assigned. The batch's WAL entry is marked `Pending`.

**R-Sc7.** On reconnect, the sync worker replays Pending entries in local-LSN order. Backend assigns backend LSNs; client reconciles.

**R-Sc8.** WAL is bounded by `wal_max_bytes` (default 100MB). At 90% capacity, `end_session` returns `WAL Near Full` warning. At 100%, `end_session` fails with `WAL Full` — this is a rare edge case (100MB is >100k session-wrapup memories) but bounded is better than unbounded.

### 13.4 Anti-goals

Explicit non-features to prevent scope creep:

- **No per-turn capture in v1.** The harness may call `end_session` frequently if it wants finer-grained capture, but Exocortex does not offer a per-turn ingestion tool. Deferred to v2.
- **No conversation transcript storage.** Wrapup memories reference session IDs; the transcript itself is not stored.
- **No LLM-based extraction on the Exocortex side.** If the harness sends a raw transcript, the server returns an error requiring structured input.
- **No email or push-notification rendering.** All narration lives in the harness.

### 13.5 `end_session` MCP tool skeleton

```rust
// crates/exocortex-client/src/tools/end_session.rs
//! MCP tool the harness calls at the end of a coding session. Wraps 1-5
//! MemoryDraft rows into an IngestBatch (§18.6) and submits over gRPC.
//! Entities are NOT accepted from the harness — the backend extracts them
//! (R-T18). Session-wrapup sends no ExternalSnapshotInfo (§18.3).

use rmcp::{tool, ToolError};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, IngestBatch, MemoryDraft,
    RelationshipDraft, ProducerIdentity, Visibility,
};

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
pub struct EndSessionArgs {
    pub session_id: String,
    pub project_id: String,
    /// 1..=5 memory drafts. Anything else is rejected client-side before the wire.
    pub memories:   Vec<MemoryDraftInput>,
    /// Optional edges between the memories in this batch, linked by draft_key.
    #[serde(default)]
    pub edges:      Vec<EdgeHintInput>,
}

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
pub struct MemoryDraftInput {
    pub draft_key:   String,       // links edges within this batch
    pub memory_type: String,       // MUST match a registered MemoryType label
    pub title:       String,       // 1..=200 chars (R-T5)
    pub content:     String,
    pub visibility:  String,       // "private"|"project"|"team"|"org" (R-T6)
    #[serde(default)]
    pub tags:        Vec<String>,
}

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
pub struct EdgeHintInput {
    pub from_draft_key: String,
    pub to_draft_key:   String,
    pub kind:           String,  // MUST match a registered kind display_name
    #[serde(default)]
    pub strength:       f32,     // 0 = RelMeta default
}

pub struct EndSessionTool {
    client: IngestServiceClient<Channel>,
    org_id:     String,
    fingerprint: [u8; 32],
    hmac_key:    [u8; 32],
    node_id:  String,
    agent_id: String,
}

impl EndSessionTool {
    #[tool(name = "end_session", description = "Persist a coding session's memories and edges into Exocortex.")]
    pub async fn handle(&self, args: EndSessionArgs) -> Result<EndSessionAck, ToolError> {
        if args.memories.is_empty() || args.memories.len() > 5 {
            return Err(ToolError::from(anyhow::anyhow!(
                "memories: expected 1..=5, got {}", args.memories.len())));
        }
        let now = std::time::SystemTime::now();
        let recorded_at = prost_types::Timestamp::from(now);
        let memories: Vec<MemoryDraft> = args.memories.into_iter().map(|m| MemoryDraft {
            draft_key: m.draft_key,
            id: new_ulid(),                       // suggested id; kernel assigns final MemoryId
            memory_type: m.memory_type,
            title: m.title,
            content: m.content,
            tags: m.tags,
            visibility: parse_visibility(&m.visibility)?,
            valid_from: Some(recorded_at.clone()),
            valid_until: None,
            external_key: None,                   // never for session-wrapup (§18.3)
        }).collect();
        let relationships: Vec<RelationshipDraft> = args.edges.into_iter().map(|e| RelationshipDraft {
            from_draft_key: e.from_draft_key,
            to_draft_key:   e.to_draft_key,
            kind: e.kind,
            strength: e.strength,
            confidence: 0.8,
            context: String::new(),
            visibility: Visibility::Project as i32,  // ≤ the registered ceiling
        }).collect();

        let mut batch = IngestBatch {
            org_id: self.org_id.clone(),
            source_uri: format!("session://{}", args.session_id),
            producer_id: "session-wrapup".into(),
            batch_id: new_ulid(),                     // idempotency key (R-I3)
            mapping_version: "session-wrapup:1.0.0".into(),
            ontology_fingerprint: self.fingerprint.to_vec(),
            ceiling: Visibility::Org as i32,          // registered ceiling (§18.2)
            checksum: compute_checksum(&memories, &relationships),
            observed_at: Some(recorded_at.clone()),
            recorded_at: Some(recorded_at),
            snapshot: None,                           // no ExternalSnapshotInfo (§18.3)
            memories,
            relationships,
            producer: Some(ProducerIdentity {
                node_id: self.node_id.clone(), agent_id: self.agent_id.clone(),
                adapter_id: String::new(), hmac_signature: vec![],
            }),
        };
        sign_hmac(&mut batch, &self.hmac_key);
        let ack = self.client.clone().submit(batch).await
            .map_err(|e| ToolError::from(anyhow::anyhow!("ingest: {e}")))?
            .into_inner();
        Ok(EndSessionAck { accepted: ack.accepted, rejected: ack.rejected,
                           assigned_lsn: ack.assigned_lsn })
    }
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct EndSessionAck { pub accepted: u32, pub rejected: u32, pub assigned_lsn: u64 }

fn new_ulid() -> String { /* uuid v7, simple format */ String::new() }
fn parse_visibility(_s: &str) -> Result<i32, ToolError> { Ok(Visibility::Project as i32) }
fn compute_checksum(_m: &[MemoryDraft], _r: &[RelationshipDraft]) -> String {
    // BLAKE3 over canonical (sorted) serialization — same input ⇒ same checksum
    String::new()
}
fn sign_hmac(_b: &mut IngestBatch, _k: &[u8; 32]) { /* hmac-sha256 over prost-encoded batch minus signature */ }
```

### 13.6 Implementation steps (M6, session-capture slice)

1. **Scaffold `end_session.rs`** in `exocortex-client/src/tools/` verbatim from §13.5.
2. **Register the tool** with `rmcp` in `exocortex-client`'s MCP server (wired at M3), then through the operation registry at M7 for HTTP parity.
3. **Fill `compute_checksum`** with BLAKE3 over canonical serialization; unit test: same input → same checksum, different order of edges → same checksum (sort before hash).
4. **Fill `sign_hmac`** with `hmac::Hmac<Sha256>`; unit test: tampered batch fails server-side admission (§18.7).
5. **Integration test**: harness calls `end_session` with 3 memories + 2 edges → `IngestServer` on `InMemoryStorage` → `IngestAck { accepted: 5, rejected: 0 }`.
6. **Rejection tests**: 0 memories (client-side reject), 6 memories (client-side reject), unknown memory_type (server-side reject with `UNKNOWN_MEMORY_TYPE`).

**M6 acceptance criteria (session-capture slice):** steps 1–6 pass on `cargo test -p exocortex-client`.

---

## 14. Scoring Algebra


### 14.1 Search relevance

```
final_score = base_match
            + (explicit_relationship_count × 0.30)
            + (Σ inferred_edge_confidence × 0.15)
            + (importance × 0.50)
            + (recency_boost × 0.10 if recent)
```

### 14.2 Derived-edge confidence

| Rule class | Formula |
|---|---|
| Transitive (R4, R5) | `1.0 / depth` |
| Co-occurrence affinity (R7) | `shared_count / 5.0` |
| Tag affinity (R9) | `shared_tag_count / 5.0` |
| Problem-solution bridge (R8) | fixed `0.8` |

### 14.3 Effective relationship strength

```rust
fn effective_strength(base: f32, ev: u32, sr: f32, age_days: f32) -> f32 {
    let boost = (0.05 * ((ev as f32 - 1.0).max(0.0)).sqrt()).min(0.20);
    let success = 0.5 + 0.5 * sr;
    let decay = (1.0 - 0.01 * age_days).max(0.5);
    ((base + boost) * success * decay).clamp(0.0, 1.0)
}
```

### 14.4 Context relevance and surprise

The formulas above are the complete scoring algebra; there is no second copy elsewhere in the ontology. Every stored MCR² and surprise value is stamped with the embedding model ID and version; cross-model comparisons are prohibited (R-Mcr1, R-Dr5). See §11 for the theoretical treatment of MCR².

---

## 15. Latency Budget

**R-Lat1.** Interactive operations MUST meet the following SLOs. Regressions break CI.

| Operation | Target p50 | Target p99 |
|---|---|---|
| `store_memory` (unlikely in v1; via `end_session` only) | 200µs | 2ms |
| `end_session` (batch of 5, local WAL + cache, sync async) | 500µs | 3ms |
| `search_memories` (base match + k=2 Crepe augmentation) | 500µs | 3ms |
| `get_chain` (depth ≤ 5) | 1ms | 5ms |
| `traverse_relationships` (k=3) | 2ms | 10ms |
| `explain_edge` (depth ≤ 5) | 1ms | 5ms |

**R-Lat2.** These budgets exclude cold-start hydration and cache-miss rehydration. Those are observable via `snapshot_age_ms` and `/health/hydration`.

**R-Lat3.** The hot path uses **zero serialization** except at the MCP boundary (JSON to the harness). All internal data flow uses typed Rust values through DashMap, ArcSwap, and StableGraph.

**R-Lat4.** The hot path uses **zero disk I/O** for reads. Writes touch disk only via async WAL fsync (scheduled, does not block the caller past `O_SYNC` semantics).

**R-Lat5.** The hot path uses **zero network I/O**. All network happens off-thread: SSE reception in a background task, batch sync in a background task.

**R-Lat6.** No LLM call is on the hot path. No LLM call is anywhere in Exocortex (see R-D6, §4.4).

---

## 16. Resource Envelope

| Limit | `mcp-standalone` default | `mcp-client` default | `backend-node` default |
|---|---|---|---|
| `in_memory_max_memories` | 100k | 100k (per-user visibility slice) | 10M per resident region |
| `in_memory_max_relationships` | 500k | 500k | 50M per resident region |
| `reasoning_queue_max` | 1000 | 1000 | 10000 |
| `reasoning_batch_size` | 10 | 10 | 50 |
| `chain_max_depth` | 5 | 5 | 5 |
| `all_paths_max_depth` | 3 | 3 | 4 |
| `context_max_tokens` | 4000 | 4000 | 32000 |
| `writer_queue_max` | 100 | 100 | 1000 |
| `crepe_kfact_hop` (interactive) | 2 | 2 | 2 |
| `crepe_kfact_hop` (enrichment) | 3 | 3 | 3 |
| `cache_max_bytes` | 2 GB | 2 GB | 16 GB |
| `wal_max_bytes` (client) | 100 MB | 100 MB | — |
| `sse_reconnect_replay_seconds` | — | — | 900 |
| `staleness_target_ms` | — | 500 (client→backend) | 100 (peer→peer) |
| `owner_lease_seconds` | — | — | 60 |
| `owner_grace_period_seconds` | — | — | 15 |
| `session_wrapup_max_memories` | 10 | 10 | 10 |

**R-R1.** No query executes without a depth bound. Enforced by CI static check.
**R-R2.** Cluster analysis and all-paths search MUST report `time_budget_exceeded: true` and truncate.
**R-R3.** Every op reports `nodes_examined`, `edges_traversed`, `wall_clock_ms`, `snapshot_version`.

---

## 17. Tenancy Model — Personal and Org, Same Product

### 17.1 The unit of tenancy is the org; a personal deployment is a single-user org

Exocortex has a single tenancy model, and it scales down to one user and up to a thousand without a fork. A tenant is an organization. Every organization gets **exactly one graph**. All memories produced by all users in that org land in that single graph, distinguished by author, visibility, and project labels — not by living in separate graphs.

**Personal deployments** run `mcp-standalone` and get a single-user org: one graph, one member, embedded storage, no cluster. The `Visibility` labels still exist — the personal user is simultaneously the author, the project owner, and the org — but nothing has to be shared across users, so `Private` is the effective default. This is the same product, running with a population of one.

**Org deployments** run `backend-node` in cluster mode with N users. The org's graph is the same shape as the personal graph; only the visibility label distribution changes (many `Team`/`Org` memories rather than mostly `Private`). This is the whole point of an org deployment: two teammates who solved the same OAuth bug six months apart find each other's memory through Dreams' cross-domain finder. Graph-per-user forecloses that.

The tenancy model is uniform. What varies between personal and org is the population count, the cache budget (§17.4), and the consolidation partitioning (§17.3). None of that is a schema difference. The same Rust binary powers both.

### 17.2 Visibility does the isolation work

Every memory and every relationship carries a `Visibility` label. Visibility — not graph identity — is what prevents one user from reading another's private notes.

```rust
pub enum Visibility {
    Private { author: UserId },
    Project { project: ProjectId },
    Team    { team: TeamId },
    Org,        // any org member
    Public,     // rare; reserved for cross-org sharing in v2
}
```

**R-MT1.** Every memory and every relationship has a `Visibility`. There is no default; every write path assigns one explicitly.
**R-MT2.** Visibility is enforced at the **storage-adapter boundary** and again at every traversal step. A user's snapshot of the org graph is a visibility-filtered view; a `search_memories` call cannot return a memory the caller cannot see, and a `traverse_relationships` walk cannot follow an edge to a memory the caller cannot see even if intermediate hops are visible.
**R-MT3.** Visibility widening is an explicit operation (`promote_visibility(memory_id, Private → Project)`), authored, audited, and logged. Narrowing is only allowed if no other memory currently depends on the wider visibility.
**R-MT4.** Visibility violations are a hard error class in the storage adapter, exercised in CI. A read that would return a visibility-violating result MUST return `PermissionDenied`, never a filtered subset silently.
**R-MT5.** Every metric, log, and audit event includes the org's graph name and, where relevant, the caller's user ID and effective visibility set.

### 17.3 Consolidation ownership at org scale

One owner per graph works for user-scale graphs. It does not work at org scale — a 1000-developer org's graph is too large for a single node to consolidate under one lease, and any single Dreams-cycle failure has org-wide blast radius.

The solution is **partitioned Dreams ownership** within the single org graph:

**R-MT6.** The Dreams partitioner slices an org graph into **consolidation regions** and assigns one lease per region. A region is a slice of the graph identified by a stable key — v1 uses `(project_id, memory_type)` as the region key. Each region has its own `OwnerRole::DreamsCycle { region }` lease; multiple regions can be under consolidation concurrently on different backend nodes.

**R-MT7.** Region membership is a graph property computable from the memory's labels alone. Adding a memory to a region is a no-op (no rewriting of edges). A memory in Project A with `MemoryType::Solution` belongs to region `(A, Solution)` for consolidation purposes; a `Solves` edge crossing into Project B does not put the memory into two regions — the edge is consolidated as part of a **cross-region reconciliation pass** that runs after per-region consolidation completes.

**R-MT8.** Cross-region reconciliation runs under a single `OwnerRole::CrossRegion` lease, but only touches edges that cross region boundaries. It is bounded in the number of edges it can rewrite per cycle (`cross_region_edge_budget`, default 10000) so a bad reconciliation cannot cascade org-wide.

**R-MT9.** MCR² is computed per region, not org-wide. Org-wide ΔR would be dominated by the largest region and would hide regressions in smaller ones. Every `ConsolidationResult` is stamped with its region key.

**R-MT10.** A Dreams cycle regression (R-Mcr3) is scoped to its region. Rollback affects that region only. Cross-region reconciliation regressions roll back only the cross-region edges written in that pass.

### 17.4 Cache sizing and routing at org scale

One 10M-memory resident cache does not fit a 1000-dev org. The org graph is materially larger than any single node's cache budget, and the working set is naturally partitioned by who is asking.

**R-MT11.** The backend cluster maintains **per-region cache residency**, not per-graph. A backend node holds the regions its users are actively querying, plus any regions assigned to it for consolidation ownership. 2Q admission (R-M12) operates at region granularity within the org graph, not at graph granularity.

**R-MT12.** Sticky-per-user consistent-hash routing at the cluster edge — requests from a given user route to a preferred backend node, so the node holding that user's hot regions serves the reads. Overflow spills to peers. Users don't share sticky affinity; two developers on the same team may hit different nodes.

**R-MT13.** The `mcp-client` running on a developer's laptop caches only the regions that developer's visibility set permits and that they have actually touched. The 100k `in_memory_max_memories` client budget is per-user working set, not per-org. Users on 1000-dev orgs are not expected to cache the entire org graph locally; they cache their visibility-filtered slice of it.

**R-MT14.** SSE subscriptions are visibility-filtered at the backend. A client subscribed to the org graph receives only deltas for memories and edges it is allowed to see. Cross-tenant subscription (subscribing to another org's graph) is impossible — authentication scopes the client to exactly one org.

### 17.5 Per-region trigger overrides and quiet-hours preference

**R-MT15.** Dreams is event-driven (§12.2), not scheduled per user. Trigger thresholds (`memory_threshold`, `edge_threshold`, `age_floor_days`, `min_interval_hours`) are per-region, defaulted per org, and overridable per region. Where a user is the near-exclusive author of a region (>80% authorship), the org may set that region's `dreams_prefer_hours` to the user's timezone (`chrono-tz`); mixed-authorship regions default to the org's canonical timezone (default UTC). The preference reorders queue draining under low load; it never blocks a fired trigger from running (R-Dr14).

### 17.6 Blast-radius mitigations

Single-graph-per-org concentrates risk. The following R-MT items exist because we chose sharing over isolation and now have to earn back the safety we gave up:

**R-MT16.** Every consolidation, prune, and cross-region-reconciliation write is tagged with its region key, lease epoch, and owner node ID. Rollback is scoped by these tags — an operator can revert the last cycle of one region without affecting the rest of the graph.

**R-MT17.** Dreams cycles are **rate-limited per region**. `region_dreams_min_interval` (default 6h) prevents a runaway scheduler from re-running the same region back-to-back.

**R-MT18.** Aggressive prune (R-Dr9) is region-scoped and MCR²-gated per region. A prune batch that would degrade MCR² in its region is flagged and, at the operator's option, blocked (`--consolidation-rollback-on-regression`).

**R-MT19.** Visibility violations are treated as data-integrity incidents, not soft errors. CI includes fuzz tests that construct adversarial visibility configurations and verify no leak path exists.

### 17.7 What single-graph-per-org does NOT solve

- **Multi-org federation.** Two orgs each with an Exocortex deployment do not share a graph. Cross-org knowledge sharing (including federation between a personal deployment and an employer's org deployment) is a v2 problem and an open question (§24).
- **Chinese-wall isolation.** If an org needs cryptographic guarantees that team A cannot possibly read team B's memories — not just RBAC — they should deploy separate Exocortex instances per team. Visibility labels are RBAC, not encryption.
- **Regulatory data residency.** Data lives where the FalkorDB backend lives. Orgs with EU/US residency requirements deploy separate regional backends today.
- **Personal-to-org portability.** A user leaving one org and wanting to bring their `Private` memories into their next org is not a supported migration in v1. See open question 14 (§24).

---

## 18. Data Sources — the Ingestion Protocol on the wire

§7.13 defines the ontology-side contract: producers speak `IngestBatch`, the kernel validates and admits. This section pins the wire format down to bytes, defines the adapter contract, lists v1 producers, and lists v2 targets — including the first-party Iceberg / S3 Tables out-of-process worker.

### 18.1 Wire format — semantics

The canonical schema is **§18.6** (`proto/ingest.proto`, compiled into `exocortex-wire`, §2.6.3). Every producer — in-process or out-of-process — speaks that schema over tonic gRPC (`exocortex.ingest.v1.IngestService`); in-process producers use the same message shapes directly. This subsection defines the semantics the schema carries; it deliberately contains no proto of its own, because a second copy is how the two schemas diverged in the first place.

- **Identity scope.** `org_id` scopes `MemoryId` derivation (R-T18a) and every visibility check.
- **Source admission.** A producer is admitted by `RegisterSource` — `(org_id, source_uri, producer_id, ceiling)`. The ceiling is org-admin-configured. Every batch echoes the ceiling; a mismatch is `UNKNOWN_SOURCE` (R-I3).
- **Row linkage.** `MemoryDraft.draft_key` is the producer-local id; `RelationshipDraft.from_draft_key`/`to_draft_key` reference it. The kernel resolves draft keys to `MemoryId`s inside the batch transaction — cross-batch references are not permitted in v1.
- **Entities.** Not on the wire. The backend extracts them (R-T18, §7.2).
- **Provenance stamping.** If `snapshot` is present, every produced row is stamped `Provenance::ExternalSnapshot` and every memory MUST carry an `ExternalKey` (`MISSING_EXTERNAL_KEY` otherwise). Without `snapshot`, session-wrapup batches get `Provenance::Asserted`.
- **Idempotency.** `(producer_id, batch_id)` is the dedupe key; replay returns the original ack (`DUPLICATE_BATCH`).
- **Integrity.** `checksum` covers the canonical serialization; `ProducerIdentity.hmac_signature` authenticates the producer (`UNAUTHORIZED`).

**R-I8.** Every `IngestBatch` MUST carry a valid `hmac_signature` computed by the registered producer's key. Batches with a missing or invalid signature are rejected with `UNAUTHORIZED` before any validation runs.

**R-I1.** Producers stream `IngestBatch` messages; the kernel returns one `IngestAck` per batch. Order within a stream is preserved; ordering across streams from the same producer is not guaranteed — producers depending on cross-batch order MUST serialize.

**R-I2.** `IngestBatch` MUST fit within `ingest_max_batch_bytes` (default 4 MiB). Larger imports MUST be split by the producer.

**R-I3.** Producers MUST call `RegisterSource` before their first `Submit` for a given `(org_id, source_uri)`. `RegisterSource` records the visibility ceiling. Attempting to submit with a ceiling that does not equal the registered value is `UNKNOWN_SOURCE`.

### 18.2 Adapter contract

An **adapter** is a process that reads some external system and produces `IngestBatch` messages. The kernel does not link to any adapter code. Adapters are separate binaries, or separate crates loaded into a separate `exocortex-worker` process; they never share an address space with the interactive read path.

The adapter contract:

1. **Registration.** The adapter registers its `source_uri` and visibility ceiling via `RegisterSource` at startup. Ceilings are org-admin-configured, not adapter-configured.
2. **Change detection.** The adapter is responsible for detecting when the external source has changed (Iceberg snapshot bump, Delta table version bump, Parquet directory mtime, custom feed sequence number). The kernel does not poll the external system.
3. **Snapshot stamping.** External-source batches MUST include `ExternalSnapshotInfo`. The kernel stamps every memory/edge in the batch with `Provenance::ExternalSnapshot` (§7.9).
4. **Identity derivation.** External-source batches MUST include `ExternalKey` on every proposed memory. The kernel derives `MemoryId` deterministically per R-T18a. Path-based, offset-based, or timestamp-based identity is not permitted; adapters that cannot supply an `ExternalKey` fall back to content-hash identity and are documented as best-effort.
5. **Idempotency.** Adapters use `batch_id` values that are stable and monotonic per producer. Re-submitting a `batch_id` returns the original `IngestAck`.
6. **Rate.** Adapters honor `RATE_LIMITED` responses with exponential backoff. The kernel does not queue on the producer's behalf.
7. **Isolation.** Adapters run in a separate process (`exocortex-worker`) so a crashed adapter cannot crash the kernel, and adapter-specific dependency stacks (DuckDB, iceberg-rust, delta-rs, aws-sdk) do not link into the kernel binary.

Adapters do NOT get access to Functions (§7.12). Adapters are strictly write-side. This is what "the kernel never reads external bytes on an interactive path" enforces at deployment.

### 18.3 v1 producers

v1 ships exactly one producer:

- **`session-wrapup`** — the in-process producer at the end of each coding session (§13). It is a reference implementation of the Ingestion Protocol; every requirement in §18.1–§18.2 applies to it. `source_uri = "session://<session_id>"`, no `ExternalSnapshotInfo` (the batch is an origin, not a re-sync), no `ExternalKey`. The kernel stamps `Provenance::Asserted` in the absence of `snapshot`.

### 18.4 v2 targets

v2 ships out-of-process adapters, each a separate `exocortex-worker` variant. The kernel does not gain any linked-in dependencies for these.

- **`iceberg-adapter`** (v2). Reads Apache Iceberg tables through the REST catalog. Detects snapshot changes via catalog polling. Uses [`iceberg-rust`](https://crates.io/crates/iceberg) for metadata reads and DuckDB for Parquet reads inside the worker process. Handles Iceberg's schema evolution by emitting `INCOMPATIBLE_ONTOLOGY` if a rename would break identity derivation. **This includes Amazon S3 Tables** — an S3 Tables bucket exposes an Iceberg REST catalog endpoint, so the same adapter reads S3 Tables through its S3-Tables catalog API. Auth is a separate profile (SigV4 vs. generic bearer), configured per source. Recommended for orgs whose analytics data lives in Iceberg or S3 Tables and who want it flowing into the Ontology as `Provenance::ExternalSnapshot` memories.
- **`delta-adapter`** (v2). Reads Delta Lake tables via [`delta-rs`](https://crates.io/crates/deltalake). Same shape as `iceberg-adapter` — different snapshot semantics (Delta log versions), same protocol, same output.
- **`parquet-dir-adapter`** (v2). Reads a directory of Parquet files as a bounded import. NOT recommended for live sources because there is no snapshot concept — the adapter emits a warning that the resulting `Provenance::ExternalSnapshot` records use a synthetic `snapshot_id` derived from the directory's file set hash. Useful for one-shot imports; treated as a documented limitation.
- **Custom adapters.** Third parties writing an adapter to a proprietary source (e.g., a mortgage servicing system feed) implement `IngestService` client-side and register their own `source_flavor`. Nothing in the kernel needs to know they exist.

### 18.5 What the kernel DOES NOT do

**R-I4.** The kernel does NOT link to `iceberg-rust`, `delta-rs`, `duckdb`, or any AWS or GCP SDK. Attempting to add any of these to `exocortex-kernel`'s `Cargo.toml` is a CI reject.

**R-I5.** The kernel does NOT poll external systems. Change detection is the adapter's responsibility.

**R-I6.** The kernel does NOT translate external schema evolution into ontology evolution. If a source column disappears, the adapter chooses whether to (a) close prior assertions via `valid_until`, (b) reject the change and require an operator to update the adapter's mapping, or (c) emit a new `mapping_version`. The kernel provides the primitives; the policy is per-adapter.

**R-I7.** The kernel does NOT expose the Ingestion Protocol to Functions or to the harness. Ingestion is a write-side capability. A harness cannot masquerade as an adapter.

### 18.6 Full `ingest.proto` (M6 deliverable)

Lives at `proto/ingest.proto`, compiled by `exocortex-wire`'s `build.rs` via `tonic-build` (§2.6.3). **This is the only ingest schema in the repo** — §7.13 and §18.1 reference it; nothing restates it. It merges the earlier draft pair: `RegisterSource` + ceiling enforcement + `draft_key` linkage + external-snapshot stamping + a unified `RejectCode` enum. Entities are deliberately absent from the wire — the backend extracts them (R-T18, §7.2). Batches are atomic: the first row-level violation rejects the whole batch and names the offending draft_key (R-T17).

```proto
syntax = "proto3";
package exocortex.ingest.v1;

import "google/protobuf/timestamp.proto";

service IngestService {
  // Batched, validated write path. Idempotent by (producer_id, batch_id).
  rpc Submit(IngestBatch) returns (IngestAck);
  // Streaming variant for high-volume adapters. Each SubmitOne acks
  // independently; the server may reject individual batches without tearing
  // down the stream.
  rpc SubmitStream(stream SubmitOne) returns (stream SubmitAck);
  // Register a source and its visibility ceiling before the first Submit (R-I3).
  rpc RegisterSource(RegisterSourceRequest) returns (RegisterSourceResponse);
  // Fetch the currently accepted ontology fingerprint before submitting.
  rpc Fingerprint(FingerprintRequest) returns (FingerprintResponse);
}

message RegisterSourceRequest {
  string     org_id        = 1;
  string     source_uri    = 2;
  string     producer_id   = 3;
  Visibility ceiling       = 4;  // org-admin-configured, not adapter-configured (§18.2)
  string     source_flavor = 5;  // "session" | "iceberg" | "delta" | "parquet-dir" | "custom"
}
message RegisterSourceResponse { Visibility ceiling = 1; }

message IngestBatch {
  string org_id                = 1;   // scopes identity derivation (R-T18a)
  string source_uri            = 2;   // "session://…" or "iceberg://…"
  string producer_id           = 3;   // registered producer
  string batch_id              = 4;   // producer-scoped monotonic id (idempotency key)
  string mapping_version       = 5;
  bytes  ontology_fingerprint  = 6;   // 32 bytes; kernel rejects on mismatch
  Visibility ceiling           = 7;   // MUST equal the registered value (R-I3)
  string checksum              = 8;   // hex; BLAKE3 over canonical serialization
  google.protobuf.Timestamp observed_at = 9;
  google.protobuf.Timestamp recorded_at = 10;
  ExternalSnapshotInfo snapshot = 11; // present iff producer is external-source (R-T16a)

  repeated MemoryDraft       memories      = 20;
  repeated RelationshipDraft relationships = 21;
  ProducerIdentity           producer      = 22;
}

message MemoryDraft {
  string draft_key   = 1;   // producer-local id; relationships link via draft keys
  string id          = 2;   // suggested ULID; final MemoryId assigned by the kernel
  string memory_type = 3;   // MUST resolve to a registered MemoryType
  string title       = 4;   // 1..=200 chars (R-T5)
  string content     = 5;   // free text; single-string payload (§7.5)
  repeated string tags = 6;
  Visibility visibility = 7;   // ≤ ceiling (R-T11a); required, no default (R-T6)
  google.protobuf.Timestamp valid_from  = 8;   // empty = recorded_at (R-T7)
  google.protobuf.Timestamp valid_until = 9;   // optional
  ExternalKey external_key = 10;  // required iff batch.snapshot present
}

message RelationshipDraft {
  string from_draft_key = 1;
  string to_draft_key   = 2;
  string kind           = 3;   // MUST resolve to a registered RelKindId
  float  strength       = 4;   // 0.0..1.0; 0 = RelMeta default
  float  confidence     = 5;   // 0.0..1.0
  string context        = 6;
  Visibility visibility = 7;   // ≤ ceiling
}

message ExternalSnapshotInfo {
  string snapshot_id   = 1;
  bytes  schema_hash   = 2;   // 32 bytes
  string source_flavor = 3;   // "iceberg" | "delta" | "parquet-dir" | "custom"
}

message ExternalKey {
  bytes  table_uuid      = 1;   // 16 bytes
  string logical_pk      = 2;
  uint32 mapping_version = 3;
}

enum Visibility {
  PRIVATE = 0;
  PROJECT = 1;
  TEAM    = 2;
  ORG     = 3;
  PUBLIC  = 4;   // reserved; v1 read paths treat as ORG (R-T11)
}

message ProducerIdentity {
  string node_id       = 1;
  string agent_id      = 2;   // optional; harness only
  string adapter_id    = 3;   // optional; adapter only
  bytes  hmac_signature= 4;   // over remainder of batch
}

message IngestAck {
  string batch_id      = 1;
  uint32 accepted      = 2;
  uint32 rejected      = 3;
  repeated RejectRow rejections = 4;
  uint64 assigned_lsn  = 5;
}
message RejectRow {
  string draft_key  = 1;   // producer-local, for triage
  RejectCode code   = 2;
  string detail     = 3;
}
enum RejectCode {
  UNKNOWN               = 0;
  INCOMPATIBLE_ONTOLOGY = 1;   // fingerprint mismatch
  UNKNOWN_SOURCE        = 2;   // producer_id not registered, or ceiling mismatch (R-I3)
  UNKNOWN_MEMORY_TYPE   = 3;
  UNKNOWN_KIND          = 4;
  INVALID_TYPE_TRIPLE   = 5;   // R-T17
  VISIBILITY_WIDENING   = 6;   // R-T11a
  MISSING_EXTERNAL_KEY  = 7;   // external batch without ExternalKey
  DUPLICATE_BATCH       = 8;   // batch_id already committed (idempotent replay)
  BAD_CHECKSUM          = 9;
  UNAUTHORIZED          = 10;  // HMAC missing/invalid
  RATE_LIMITED          = 11;
}
message SubmitOne { oneof body { IngestBatch batch = 1; } }
message SubmitAck { IngestAck ack = 1; }
message FingerprintRequest {}
message FingerprintResponse { bytes fingerprint = 1; string kernel_version = 2; repeated string packs = 3; }
```

### 18.7 `IngestService` tonic implementation skeleton

```rust
// crates/exocortex-ingest/src/service.rs
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use exocortex_kernel::Ontology;
use exocortex_storage::Storage;
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, IngestAck, SubmitOne, SubmitAck,
    RejectRow, RejectCode, FingerprintRequest, FingerprintResponse,
    RegisterSourceRequest, RegisterSourceResponse,
};

pub struct IngestServer<S: Storage> {
    storage:  Arc<S>,
    ontology: Arc<Ontology>,
    hmac_key: [u8; 32],
}

impl<S: Storage> IngestServer<S> {
    pub fn new(storage: Arc<S>, ontology: Arc<Ontology>, hmac_key: [u8; 32]) -> Self {
        Self { storage, ontology, hmac_key }
    }
    fn verify_hmac(&self, b: &IngestBatch) -> Result<(), Status> {
        let sig = &b.producer.as_ref().ok_or_else(|| Status::unauthenticated("no producer"))?.hmac_signature;
        if sig.is_empty() { return Err(Status::unauthenticated("missing hmac")); }
        // pseudo: recompute with hmac-sha256 and constant_time_eq
        Ok(())
    }
    fn ontology_matches(&self, b: &IngestBatch) -> bool {
        b.ontology_fingerprint.as_slice() == self.ontology.fingerprint.0.as_slice()
    }
}

#[tonic::async_trait]
impl<S: Storage + 'static> IngestService for IngestServer<S> {
    #[instrument(skip(self, req))]
    async fn submit(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        let batch = req.into_inner();
        self.verify_hmac(&batch)?;
        if !self.ontology_matches(&batch) {
            return Ok(Response::new(IngestAck {
                batch_id: batch.batch_id, accepted: 0,
                rejected: (batch.memories.len() + batch.relationships.len()) as u32,
                rejections: batch.memories.iter().map(|m| RejectRow {
                    draft_key: m.draft_key.clone(), code: RejectCode::IncompatibleOntology as i32,
                    detail: "ontology fingerprint mismatch".into(),
                }).collect(),
                assigned_lsn: 0,
            }));
        }
        // 0. Pre-checks (§7.13 pipeline, in order): producer registered + ceiling
        //    equality (UNKNOWN_SOURCE), batch_id not previously committed
        //    (DUPLICATE_BATCH — idempotent replay returns the original ack).
        // 1. Validate every draft. Collect rejections; don't stop on first error.
        let mut rejections = Vec::new();
        let mut ok_mem = Vec::with_capacity(batch.memories.len());
        for m in &batch.memories {
            match self.ontology.validate_memory_draft(m) {
                Ok(kernel_m) => ok_mem.push(kernel_m),
                Err(e) => rejections.push(RejectRow {
                    draft_key: m.draft_key.clone(),
                    code: map_reject(&e) as i32,
                    detail: format!("{e:?}"),
                }),
            }
        }
        let mut ok_rel = Vec::with_capacity(batch.relationships.len());
        for r in &batch.relationships {
            match self.ontology.validate_relationship_draft(r, &ok_mem) {
                Ok(kernel_r) => ok_rel.push(kernel_r),
                Err(e) => rejections.push(RejectRow {
                    draft_key: format!("{}->{}", r.from_draft_key, r.to_draft_key),
                    code: map_reject(&e) as i32,
                    detail: format!("{e:?}"),
                }),
            }
        }
        // 2. Persist accepted rows in one transactional batch — atomic per
        //    R-T17: if any row fails validation the whole batch is rejected above.
        let commit = self.storage.upsert_batch(&ok_mem, &ok_rel).await
            .map_err(|e| Status::internal(format!("storage: {e}")))?;
        let assigned_lsn = commit.last().map(|c| c.lsn).unwrap_or(0);
        Ok(Response::new(IngestAck {
            batch_id: batch.batch_id,
            accepted: (ok_mem.len() + ok_rel.len()) as u32,
            rejected: rejections.len() as u32,
            rejections, assigned_lsn,
        }))
    }
    async fn submit_stream(&self, req: Request<Streaming<SubmitOne>>) -> Result<Response<Self::SubmitStreamStream>, Status> {
        // Fan-in to `submit`, one row at a time, streaming acks back.
        todo!("iterate stream, call submit per body, forward ack")
    }
    type SubmitStreamStream = futures::stream::BoxStream<'static, Result<SubmitAck, Status>>;

    async fn fingerprint(&self, _req: Request<FingerprintRequest>) -> Result<Response<FingerprintResponse>, Status> {
        Ok(Response::new(FingerprintResponse {
            fingerprint: self.ontology.fingerprint.0.to_vec(),
            kernel_version: env!("CARGO_PKG_VERSION").into(),
            packs: self.ontology.packs.iter().map(|p| p.name.to_string()).collect(),
        }))
    }

    async fn register_source(&self, _req: Request<RegisterSourceRequest>)
        -> Result<Response<RegisterSourceResponse>, Status> {
        // Persist the admission record: (org_id, source_uri, producer_id) -> ceiling.
        // Ceilings are org-admin-configured; the request value is cross-checked
        // against server config before recording (§18.2).
        todo!()
    }
}

/// Maps kernel validation errors to the §18.6 RejectCode enum.
fn map_reject(e: &exocortex_kernel::KernelError) -> RejectCode {
    use exocortex_kernel::KernelError::*;
    match e {
        UnknownKind(_)          => RejectCode::UnknownKind,
        InvalidTypeTriple { .. } => RejectCode::InvalidTypeTriple,
        VisibilityWidening { .. } => RejectCode::VisibilityWidening,
        TitleBounds | EmptyContent | SummaryBounds | MetadataTooLarge
                                => RejectCode::Unknown,  // row-level R-T5 bounds
        _                       => RejectCode::Unknown,
    }
}
```

### 18.8 Implementation steps (M6, ingest slice)

1. **Add `proto/ingest.proto`** verbatim from §18.6.
2. **Verify `build.rs` in `exocortex-wire`** (§2.6.3, scaffolded at M0) compiles it with `tonic_build::configure().build_server(true)`.
3. **Add `Ontology::validate_memory_draft`** and `validate_relationship_draft` (backed by §7.19 step 7). Unit tests: 20 legal / 20 illegal drafts each.
4. **Add `IngestServer` skeleton** verbatim from §18.7. Fill `todo!()` sites for streaming.
5. **HMAC verification**: use `hmac::Hmac<Sha256>` and `subtle::ConstantTimeEq`. Bench: <10µs per verification for a 1MB batch.
6. **Idempotency**: keep an in-memory LRU of the last 1000 `(producer_id, batch_id)` → ack; duplicate batches short-circuit to the original `IngestAck`.
7. **End-to-end integration test**: a `TestAdapter` produces a 50-row batch → `IngestServer` on top of `InMemoryStorage` → assert 50 accepted, 0 rejected, LSN monotonic.
8. **Failure-mode tests**: mismatched fingerprint (all-rejected, `INCOMPATIBLE_ONTOLOGY`), one row with a bad triple (whole batch rejected with `INVALID_TYPE_TRIPLE` naming the draft_key), missing HMAC (call rejected with `UNAUTHORIZED`), visibility above the registered ceiling (`VISIBILITY_WIDENING`), external-source batch missing `ExternalKey` (`MISSING_EXTERNAL_KEY`).

**M6 acceptance criteria (ingest slice):** steps 1–8 pass on `cargo test -p exocortex-ingest`.

---

## 19. Observability

**R-O1.** Structured logging via `tracing` with trace propagation through async and cluster boundaries.

**R-O2.** Prometheus metrics at `/metrics` (HTTP/backend) or stderr (MCP):

- `exocortex_memories_total{graph}`, `_relationships_total{graph,type,provenance}`
- `exocortex_reasoning_queue_depth{graph}`, `_dropped_total{graph}`, `_lag_ms{graph}`
- `exocortex_rules_executed_total{graph,rule,tier,engine}` — engine ∈ {crepe, steel}
- `exocortex_ops_duration_seconds{op,graph}` (histogram) — hits latency SLOs
- `exocortex_snapshot_publish_total{graph}`
- `exocortex_contradiction_detected_total{graph,type_a,type_b}`
- `exocortex_dreams_mcr2_rate_reduction{graph}`
- `exocortex_dreams_discoveries_total{graph,type,quality}`
- `exocortex_cluster_peers{state}` (backend only)
- `exocortex_cluster_invalidations_published_total{graph}` (backend only)
- `exocortex_cluster_owner_lease_transitions_total{graph,role}` (backend only)
- `exocortex_client_sse_lag_ms{graph}` (client only)
- `exocortex_client_wal_pending_entries` (client only)
- `exocortex_cache_snapshot_age_ms{graph}` (histogram)
- `exocortex_cache_rebuild_total{graph,reason}`
- `exocortex_2q_admission_events_total{graph,decision}` — decision ∈ {admit_a1in, promote_am, evict_a1in, evict_am, ghost_hit}

**R-O3.** OpenTelemetry export supported.
**R-O4.** `/health/ready` returns 200 only when: hydration complete, storage reachable, local cache populated, reasoning workers running, AND (backend-node) cluster membership stable OR (mcp-client) SSE connection healthy.
**R-O5.** `/health/cluster` (backend only): peer list with states, invalidation lag per graph, owner leases held by this node, LSN gap detection status.
**R-O6.** `/health/sync` (client only): SSE connection state, last backend LSN observed, pending WAL entries, seconds since last successful backend contact.

---

## 20. Security

**R-Sec1.** MCP-stdio mode has no network surface — the MCP tool interface only. HTTP and backend-node modes require API key or bearer auth. No `--insecure` flag in release builds.
**R-Sec2.** Cypher parameters MUST always be parameterized. Storage trait has no string-interpolation escape hatch.
**R-Sec3.** All timestamps are timezone-aware UTC.
**R-Sec4.** Cluster invalidation deltas MUST be HMAC-signed with a cluster-shared key.
**R-Sec5.** SSE payloads MUST be HMAC-signed with a per-client shared secret.
**R-Sec6.** Owner leases are cryptographically bound to the acquiring node (signed lease token with epoch). Fencing tokens (R-C3) prevent stale-lease writes.
**R-Sec7.** Client auth: bearer token or mTLS. Tokens scope to a user's graphs only.
**R-Sec8.** No LLM calls means no third-party API keys in Exocortex config. Reduced attack surface.

---

## 21. Operation Registry — MCP and HTTP Parity

```rust
inventory::submit! {
    OperationDef {
        name: "end_session",
        mcp_tool_name: "exocortex.end_session",
        http_method: Method::POST,
        http_path: "/session/wrapup",
        handler: |ctx, req| Box::pin(services::session::end(ctx, req)),
        input_schema: schema_for!(SessionWrapup),
        output_schema: schema_for!(SessionWrapupResponse),
    }
}
```

**R-P1.** No operation exists on only one surface. The registry drives both catalogues at build time.
**R-P2.** Every capability is core, not surface-specific.

### 21.1 `Operation` trait and codegen

```rust
// crates/exocortex-ops/src/lib.rs
//! Every capability implements Operation. inventory::submit! registers each
//! implementation once; MCP and HTTP surfaces enumerate the registry at
//! startup to build tool and route catalogues.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

#[async_trait]
pub trait Operation: Send + Sync + 'static {
    type Input:  DeserializeOwned + JsonSchema + Send;
    type Output: Serialize + JsonSchema + Send;
    fn name(&self) -> &'static str;
    fn mcp_tool_name(&self) -> &'static str;
    fn http_method(&self) -> http::Method;
    fn http_path(&self) -> &'static str;
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError>;
}

pub struct OpContext {
    pub visibility_ctx: exocortex_storage::VisibilityContext,
    pub storage:  std::sync::Arc<dyn exocortex_storage::Storage>,
    pub cache:    std::sync::Arc<exocortex_cache::LocalCache>,
    pub deadline: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("bad input: {0}")]      BadInput(String),
    #[error("unauthorized: {0}")]   Unauthorized(String),
    #[error("not found")]           NotFound,
    #[error("deadline exceeded")]   DeadlineExceeded,
    #[error("storage: {0}")]        Storage(String),
    #[error("{0}")]                 Other(String),
}

/// Type-erased registration for inventory. Each Operation impl submits one of
/// these; MCP and HTTP surfaces iterate `inventory::iter::<OperationEntry>` at
/// startup.
pub struct OperationEntry {
    pub name: &'static str,
    pub mcp_tool_name: &'static str,
    pub http_method: fn() -> http::Method,
    pub http_path: &'static str,
    pub input_schema: fn() -> schemars::schema::RootSchema,
    pub output_schema: fn() -> schemars::schema::RootSchema,
    pub handler: for<'a> fn(&'a OpContext, serde_json::Value)
        -> futures::future::BoxFuture<'a, Result<serde_json::Value, OpError>>,
}
inventory::collect!(OperationEntry);
```

Example: registering an operation.

```rust
// crates/exocortex-ops/src/find_related.rs
use super::*;

pub struct FindRelated;
#[derive(serde::Deserialize, JsonSchema)]
pub struct FindRelatedInput { pub anchor: exocortex_kernel::MemoryId, pub k: u8 }
#[derive(serde::Serialize, JsonSchema)]
pub struct FindRelatedOutput { pub memories: Vec<exocortex_kernel::Memory> }

#[async_trait]
impl Operation for FindRelated {
    type Input = FindRelatedInput; type Output = FindRelatedOutput;
    fn name(&self) -> &'static str { "find_related" }
    fn mcp_tool_name(&self) -> &'static str { "exocortex.find_related" }
    fn http_method(&self) -> http::Method { http::Method::POST }
    fn http_path(&self) -> &'static str { "/v1/find_related" }
    async fn handle(&self, ctx: &OpContext, input: Self::Input) -> Result<Self::Output, OpError> {
        let spec = exocortex_storage::TraversalSpec {
            direction: exocortex_storage::Direction::Both,
            kinds: Default::default(),
            max_depth: input.k.min(4),
            max_nodes: 128,
            visibility_ctx: ctx.visibility_ctx.clone(),
            as_of: None,
        };
        let memories = ctx.cache.traverse(&ctx.visibility_ctx.org_id, &input.anchor, &spec);
        Ok(FindRelatedOutput { memories })
    }
}

inventory::submit! { OperationEntry {
    name: "find_related",
    mcp_tool_name: "exocortex.find_related",
    http_method: || http::Method::POST,
    http_path: "/v1/find_related",
    input_schema: || schemars::schema_for!(FindRelatedInput),
    output_schema: || schemars::schema_for!(FindRelatedOutput),
    handler: |ctx, v| Box::pin(async move {
        let input: FindRelatedInput = serde_json::from_value(v).map_err(|e| OpError::BadInput(e.to_string()))?;
        let out = FindRelated.handle(ctx, input).await?;
        Ok(serde_json::to_value(out).unwrap())
    }),
}}
```

Surface generation:

```rust
// crates/exocortex-client/src/tool_bind.rs
pub fn install_all_tools(server: &mut rmcp::ServerBuilder, ctx: std::sync::Arc<exocortex_ops::OpContext>) {
    for entry in inventory::iter::<exocortex_ops::OperationEntry>() {
        let ctx = ctx.clone();
        server.tool(rmcp::Tool {
            name: entry.mcp_tool_name.into(),
            description: entry.name.into(),
            input_schema: (entry.input_schema)(),
            handler: Box::new(move |args| {
                let ctx = ctx.clone();
                let h = entry.handler;
                Box::pin(async move { (h)(&ctx, args).await.map_err(Into::into) })
            }),
        });
    }
}
```

```rust
// crates/exocortex-server/src/http_bind.rs
pub fn install_all_routes(router: axum::Router, ctx: std::sync::Arc<exocortex_ops::OpContext>) -> axum::Router {
    let mut r = router;
    for entry in inventory::iter::<exocortex_ops::OperationEntry>() {
        let ctx = ctx.clone();
        let h = entry.handler;
        let handler = move |axum::Json(body): axum::Json<serde_json::Value>| {
            let ctx = ctx.clone();
            async move {
                match (h)(&ctx, body).await {
                    Ok(v)  => Ok::<_, axum::http::StatusCode>(axum::Json(v)),
                    Err(_) => Err(axum::http::StatusCode::BAD_REQUEST),
                }
            }
        };
        r = r.route(entry.http_path,
            match (entry.http_method)() {
                http::Method::GET  => axum::routing::get(handler),
                http::Method::POST => axum::routing::post(handler),
                _ => panic!("unsupported method for {}", entry.name),
            });
    }
    r
}
```

### 21.2 Parity check (xtask)

```rust
// xtask/src/parity.rs — invoked by `cargo xtask gen-schemas` (§2.7).
//! The parity + drift gate: fails CI if any Operation is not registered on
//! both surfaces or if input/output schemas drift from the checked-in goldens.

pub fn run() -> anyhow::Result<()> {
    let entries: Vec<_> = inventory::iter::<exocortex_operations::OperationEntry>().collect();
    let mut seen_names = std::collections::HashSet::new();
    for e in &entries {
        if !seen_names.insert(e.name) { anyhow::bail!("duplicate op name {}", e.name); }
        assert!(!e.mcp_tool_name.is_empty(), "{}: missing mcp_tool_name", e.name);
        assert!(!e.http_path.is_empty(), "{}: missing http_path", e.name);
        let i = (e.input_schema)();
        let o = (e.output_schema)();
        anyhow::ensure!(i.schema.metadata.as_ref().and_then(|m| m.title.clone()).is_some(),
            "{}: input schema missing title", e.name);
        anyhow::ensure!(o.schema.metadata.as_ref().and_then(|m| m.title.clone()).is_some(),
            "{}: output schema missing title", e.name);
    }
    println!("parity ok: {} operations", entries.len());
    Ok(())
}
```

### 21.3 Implementation steps (M7)

1. **Scaffold `exocortex-ops`** with the `Operation` trait, `OpContext`, and `OperationEntry` verbatim from §21.1.
2. **Migrate existing session-wrapup, find_related, get_memory, traverse to the trait.**
3. **Add MCP binder** (§21.1) in `exocortex-client`.
4. **Add HTTP binder** (§21.1) in `exocortex-server`.
5. **Wire the §21.2 parity check into `cargo xtask gen-schemas`** (§2.7). Add to `.github/workflows/ci.yml` as a required check.
6. **Integration test**: register a `TestOp`, start MCP server + HTTP server, hit both surfaces, assert identical outputs for identical inputs.
7. **Schema-drift golden files**: `crates/exocortex-ops/tests/golden/*.json`. `cargo xtask gen-schemas` regenerates; CI fails if the diff isn't checked in.
8. **Implement the audit log (§21.4)**: `audit_append`/`audit_range` templates in the §6.4 catalogue, the `list_audit_records` admin operation, and CI assertions that `promote_visibility` and `accept_discovery` produce records.

**M7 acceptance criteria:** steps 1–8 pass on `cargo test -p exocortex-ops && cargo xtask gen-schemas`.

### 21.4 Audit log

Every Action execution (§7.11) appends exactly one immutable audit record before the ack returns. The audit log is a Storage-backed append-only table — `audit_append` / `audit_range` templates in the §6.4 catalogue, registered in M7 — not a separate system:

```rust
pub struct AuditRecord {
    pub action:       SmolStr,              // e.g. "commit_wrapup", "promote_visibility"
    pub actor:        SmolStr,              // user or agent identity
    pub org_id:       SmolStr,
    pub input_digest: [u8; 32],             // BLAKE3 of canonical input
    pub output_ids:   SmallVec<[SmolStr; 8]>,
    pub fingerprint:  [u8; 32],             // OntologyFingerprint at execution (R-T21)
    pub lease_epoch:  Option<u64>,          // set for owner-only actions
    pub recorded_at:  DateTime<Utc>,
    pub lsn:          u64,
}
```

**R-A1.** Audit writes share the Action's storage transaction — a committed action always has its record; a rolled-back action never does.
**R-A2.** `promote_visibility` (R-T11a) and `accept_discovery` records reference their target memory/edge ids, so "why is this wider / why was this accepted?" is answerable from the log alone (R-Dr10, criterion 28).
**R-A3.** The audit read surface is a registered admin operation (`GET /v1/audit?since_lsn=`) — same registry, same parity rule (R-P1).

---

## 22. Invariants — Consolidated

| ID | Invariant | Enforced by |
|---|---|---|
| CR-1 | Registered `RelKindId`s (kernel constants + pack `kinds!` blocks) are the sole source of truth for edge labels. | Build-time codegen; OntologyFingerprint |
| CR-2 | `bidirectional` and `inverse_type` are orthogonal. | Trait implementation |
| CR-3 | Every persisted edge carries typed `Provenance`. `Proposed` never persists. | Rust type system |
| CR-4 | Bi-temporal fields survive every round trip. | `Storage` trait contract |
| CR-5 | Reasoning queue overflow is observable, never silent. | R-L7 |
| CR-6 | No unbounded traversal exists in any query path. | CI static check |
| CR-7 | `OntologyFingerprint` mismatch halts startup / cluster admission / client connect. | R-T21, R-D5 |
| CR-8 | No serialization on reasoning-engine hot paths. | Steel + Crepe integration |
| CR-9 | Deployment mode selects surface, never capability. | Operation registry |
| CR-10 | Cypher is used for persistence and read-only queries only. No rule logic in Cypher. | Rule catalogue location |
| CR-11 | Discovery cannot render as a relationship. | Type-system separation |
| CR-12 | Discovery quality computed once. | R-Dr6 |
| CR-13 | Transitive discovery excludes already-derived pairs. | R-Dr7 |
| CR-14 | Local cache is not the system of record. Storage is. | R-M8, R-M9 |
| CR-15 | Every mutation produces a monotonic per-graph LSN (local or backend). | Storage + WAL contracts |
| CR-16 | Peer caches converge within `staleness_target_ms`. | Cluster + SSE tests |
| CR-17 | Owner-only operations acquire a lease before executing. | Chubby-style lease with fencing |
| CR-18 | No cluster admits a peer with divergent `OntologyFingerprint`. | Membership handshake |
| CR-19 | **Exocortex MUST NOT call an LLM.** | Absence of `LLMProvider` trait; CI grep |
| CR-20 | Interactive latency SLOs (R-Lat1) MUST hold. Regressions break CI. | Benchmark suite |
| CR-21 | Session capture happens only via `end_session`. No per-turn capture in v1. | Operation registry (no such tool exists in v1) |
| CR-22 | Every memory and every relationship carries an explicit `Visibility`. No default. Visibility violations return `PermissionDenied`, never a silent filter. | R-MT1, R-MT2, R-MT4 |
| CR-23 | Cluster peers agree on both `OntologyFingerprint` AND wire schema version before admission. Protobuf fields evolve additively. | R-W2, R-W3 |
| CR-24 | Consolidation, Dreams, prune, and cross-region reconciliation are region-scoped inside the org graph. Rollback and MCR² evaluation are region-scoped. | R-MT6, R-MT9, R-MT10, R-MT16 |
| CR-25 | Every kernel-space `RelKindId` constant is bound to exactly one concrete kind by the loaded pack set at link time. Missing or duplicate bindings refuse to link. Packs cannot rebind kernel rules R1–R9. | R-Pk1, R-Pk2, R-Pk3, R-Pk5 |
| CR-26 | External data enters the graph only through the Ingestion Protocol (`IngestService.Submit`). `exocortex-kernel` MUST NOT link `iceberg-rust`, `delta-rs`, `duckdb`, or `aws-sdk-*`; adapters run out-of-process as `exocortex-worker`. | R-I1, R-I5 |
| CR-27 | Source-derived memories are never automatically widened. Every ingestion source registers a visibility ceiling; the Ingestion Protocol validator rejects batches that exceed it. Widening happens only through a human-authored, audited `PromoteVisibility` Action. | R-T11a |
| CR-28 | External-source `MemoryId`s are derived from `(org_id, source_uri, table_uuid, logical_pk, mapping_version)` — never from file paths, row offsets, or timestamps. Source reorganization does not fork memory identity. | R-T18a |
| CR-29 | `Provenance::ExternalSnapshot` is the only variant carrying external-system coordinates. A re-sync of the same source produces new assertions with distinct `snapshot_id`/`observed_at`; it never overwrites prior assertions. Bi-temporality survives source mutation. | R-T16a |

---

## 23. Success Criteria

At v1.0:

1. Single artifact deploys as mcp-client, mcp-standalone, or backend-node without codebase forks.
2. The ontology (kernel + `exocortex-pack-dev-v1`) is defined exactly once, in Rust; every other surface is generated from it. `OntologyFingerprint` is stable across a matching kernel + pack set and diverges on any change to the effective ontology.
3. All 9 catalogued rules execute in every deployment mode — 8 Crepe + 1 Steel (R6 `reverse_solves` is Steel). Type-from-edge inference rules (R1 `type_from_solves`, R2 `type_from_fixes`, R3 `type_from_causes`) fire against `Solves` / `Fixes` / `Causes` edges respectively.
4. Bi-temporal round-trips work.
5. `SimilarTo` edges are self-describing.
6. Inverse materialization works.
7. Reasoning queue overflow is observable.
8. Entity extraction works on all backends.
9. `MemoryType::Conversation` is available on all surfaces.
10. Discovery renders as proposal, not relationship.
11. Discovery quality is reconciled between surface and metrics.
12. Transitive discovery excludes derived pairs.
13. Contradiction records support resolution.
14. Embeddings and MCR² are model-version-stamped.
15. Backend cluster with 3 nodes passes: cross-node cache convergence <500ms p95, owner lease transition without duplicate consolidation, partition-and-heal without cache corruption, epoch fencing rejects stale-lease writes.
16. Cache rebuild from storage is a first-class operation, exercised in CI.
17. Interactive latency SLOs (R-Lat1) pass in benchmark suite.
18. Session wrapup end-to-end: harness directive → structured JSON → `end_session` → local cache + WAL + sync → backend commit → SSE broadcast → sibling client observes update within 500ms.
19. Zero LLM calls in Exocortex during the full v1.0 test suite. Confirmed by mocked-provider audit and codebase grep.
20. Dreams event-driven cycle runs end-to-end (trigger → queue → lease → consolidation + discovery + prune → counter reset) on a 3-node backend cluster with owner leases and produces zero prose output. Trigger predicates fire on synthetic write bursts; `age_floor_days` fires on a stale-but-live region.
21. Single-graph-per-org tenancy proven: an org with 100 users, 3 projects, and a mix of Private/Project/Team/Org visibility passes the visibility fuzz suite with zero leaks and Dreams' cross-domain finder surfaces a cross-user pattern that neither user could have found alone.
22. Region-scoped Dreams proven: a 3-node backend concurrently consolidates 3 different `(project, memory_type)` regions of the same org graph, each with its own lease and MCR² score. Cross-region reconciliation completes without violating the per-region MCR² budget.
23. Cluster wire protocol proven: protobuf-encoded invalidation deltas fan out across the 3-node cluster at target throughput, wire-schema version mismatch refuses peer admission, and an additive-only wire-schema change ships to one node without breaking the others.
24. Graph-sparsity diagnostic proven: `GraphSparsity` is computed before and after every Dreams cycle in test, stamped into `ConsolidationResult`, and a synthetic hairball-producing merge cycle triggers `hairball_regression: true` (R-Mcr6) with default tolerance. Both ΔR and hairball guardrails fire independently.

25. **Dev-v1 as a pack.** The 13 memory types, 12 entity types, and 48 relationship kinds of dev-v1 all ship inside `exocortex-pack-dev-v1`, registered via the `pack!` macro and `inventory::submit!`. The kernel crate names none of them directly. A build that omits the dev-v1 pack fails to link because the kernel-constant `RelKindId`s (`SOLVES`, `FIXES`, `CAUSES`, `IN_SESSION`, …) are unbound (R-Pk2).

26. **Second-pack demo.** A minimal `exocortex-pack-example-v1` crate registers a handful of alternative memory/entity types and kinds, compiles alongside dev-v1 in a single binary, exposes its kinds to Crepe rules without rebinding kernel rules R1–R9, and produces a different `OntologyFingerprint` than a dev-v1-only build. This validates the pack seam before v2 legal/sales/medical packs.

27. **Ingestion Protocol conformance (v1 scope).** The `session-wrapup` producer passes a golden `IngestBatch` protobuf test suite: well-formed batches accepted, and every `RejectCode` (§18.6) triggered by a targeted malformed batch — including `VISIBILITY_WIDENING` under a lowered ceiling and `MISSING_EXTERNAL_KEY` on a synthetic external batch. The two-sync failure-mode example from §7.9 replays deterministically against `InMemoryStorage` with synthetic `ExternalSnapshotInfo`/`ExternalKey` data — a snapshot bump produces *additional* assertions (not overwrites) per R-T16a, with no Iceberg dependency. The live `iceberg-adapter` round-trip is M9+/v2 (§18.4).

28. **No-widening rule enforced.** A source registered with `Team` ceiling submitting a memory at `Org` visibility is rejected with `VISIBILITY_WIDENING`. The `promote_visibility` Action is the only path that succeeds and it writes an audit-log entry (§21.4).

29. **Identity derivation is layout-immune (kernel-level).** Property tests on `MemoryId::from_external` (§2.6.1): identical `(org_id, source_uri, table_uuid, logical_pk, mapping_version)` always yields identical ids; path-, offset-, and timestamp-shaped inputs never enter the derivation; a changed `mapping_version` deliberately forks identity. The full Iceberg re-partition replay (rename, compact, re-partition; same ids) runs with the M9+ adapter prototype.

30. **Kernel forbids table-reader dependencies.** A CI check fails the build if `exocortex-kernel/Cargo.toml` adds `iceberg`, `deltalake`, `duckdb`, or any `aws-sdk-*` crate (R-I4). The same crates ARE allowed under `exocortex-worker/Cargo.toml`.

---

## 24. Open Questions

1. **Embedding runtime — backend only, or optional client-side?** ~~Backend-side is simpler and shares one model across all clients. Client-side would let `mcp-standalone` compute embeddings offline.~~ **Closed: backend-only.** Aligns with principle 6 (§0.4) — the interactive path is pure graph traversal, embeddings are backend-only, `mcp-standalone` inherits the same backend `FalkorDBStorage` in embedded mode with the embedding runtime co-located. Client cache holds no embeddings.
2. **Consistent-hash routing at the backend edge.** How aggressive should sticky-per-graph routing be? Full sticky (best cache warmth) versus load-balanced (better hot-graph handling). Lean: sticky-with-overflow, defer tuning to load tests.
3. **WAL storage format — `rkyv` zero-copy vs `bincode`.** `rkyv` is faster to read (recovery, replay) but the format is more brittle across versions. Lean: `bincode` with schema version header for v1, `rkyv` if we hit recovery-speed problems.
4. **Cross-cluster federation (multi-region backends).** Not in v1. Design allows a second cluster to subscribe as an SSE client, but that's a fragile shortcut. Real federation is a v2 problem.
5. **Embedding model swap procedure.** Dual-write? Blue/green corpora? Not yet specified. Given the model-version-stamped MCR² and cross-model prohibition (R-Mcr1), the answer is probably blue/green corpora with an explicit reindex operation.
6. **FalkorDB SSPL redistribution** — ~~needs legal review for hosted-service resale.~~ **Closed as a design concern.** Per principle 6 (§0.4), v1 commits to FalkorDB. If SSPL becomes a real business problem (hosted-service resale, a specific customer contract), the port happens then. The database adapter (§6.0) means a Memgraph or Neo4j replacement is a new `Storage` impl, not a codebase rewrite. Not designed for now.
7. **Directive discoverability.** Do the top three coding harnesses (Claude Code, Codex, Cursor) reliably support MCP resource auto-loading? If not, config-file directives may be the only realistic path in v1.
8. **Wrapup schema evolution.** Adding fields to `SessionWrapup` between v1 and v2 means older harnesses send v1 payloads to v2 servers. Design forward-compatible additive fields; document deprecation policy.
9. **Quiet-hours preference for mixed-authorship regions.** §17.5 defaults mixed regions to org-canonical timezone for `dreams_prefer_hours`. Should a heavily-mixed region instead prefer the timezone of the last-authored-in user, or the earliest-quiet-hour across all authors? Lean: canonical UTC for v1; revisit on operator feedback. Note: this is a *preference*, not a schedule — Dreams is event-driven (§12.2), so this question only affects queue-drain ordering under low load.
10. **Cross-region reconciliation cadence.** §17.3 defines a `CrossRegion` lease that runs after per-region consolidation. Should it run every cycle, or only when per-region cycles produced cross-boundary edges? Lean: on-demand, gated on edge counts, in v1.
11. **Client-side SSE binary payloads.** §9.6 keeps SSE JSON for v1. If measured parse cost on the client change feed becomes a bottleneck, is the answer SSE-with-base64-protobuf, or a switch to a WebSocket+protobuf channel? Lean: WebSocket switch is the honest answer, deferred to v2.

12. **Ingest-time SimilarTo seeding.** §12.1 defers all `SimilarTo` writes to Dreams (`SimilarityHnsw`) to keep the write path off the embedding runtime. If ingest embedding cost is measured well below the write budget, is it worth writing coarse `SimilarityCosine` edges at ingest to give the graph a warm SimilarTo skeleton before the first Dreams cycle? Lean: no in v1; the cold-start window is short once event-driven Dreams (§12.2) starts firing on real load.

13. **Org-tuned embeddings via CgMCR² (v2 north-star).** The v1 design uses a pluggable frozen embedding model. The obvious lever for making the memory system meaningfully better than a generic embedding provides is to fine-tune a small embedding model on the org's own graph, using CgMCR²'s joint clustering + rate-reduction objective. This crosses the "no training in the backend" invariant, requires deciding on a fine-tuning corpus (single-org? cross-org with visibility guarantees?), and requires a model-version-stamping protocol across fine-tunes (extending R-Mcr1). Scope for v2. Not a footnote — this is the direction the graph-as-durable-artifact premise ultimately points at.

14. **Ontology export and portability.** If a personal-deployment user takes a job at a new org, or if an org offboards a departing employee, what happens to the graph? Options: (a) a signed `.exocortex-bundle` export format that captures memories + edges + provenance + the current `OntologyFingerprint`, importable into a matching-fingerprint destination; (b) a lossy Markdown export for humans; (c) both. The pack seam makes (a) tractable because the ontology is by-value in the bundle; without pack versioning it would be impossible. Lean: bundle format specified in v1 as an appendix, implemented in v1.1 or v2. Sub-questions: does the bundle include `Provenance::ExternalSnapshot` records that reference sources the destination cannot reach? Does re-import re-derive Crepe rules, or import them frozen? Does a bundle carry its own signed kernel-and-pack manifest so the destination can verify?

15. **Cross-Ontology federation.** Two Exocortex deployments with different pack sets — e.g., a personal deployment with `exocortex-pack-dev-v1` and an employer's org deployment with `exocortex-pack-dev-v1` + `exocortex-pack-mortgage-v1` — want to share memories that reference only the common (dev-v1) subset. Naive federation fails because `OntologyFingerprint`s do not match. A principled answer requires a **fingerprint of the common subset** and a Function that projects memories/edges into the common subset for cross-deployment ingest. This is the sharp form of open question 4. Lean: not v1. Design constraints locked in v1 that make it tractable later: kernel-vs-pack separation, per-pack fingerprints, `Provenance::ExternalSnapshot` (a federated peer is just another external source, from the ingest-protocol perspective).

---

## Appendix A — Crate Selections

| Concern | Crate | Rationale |
|---|---|---|
| FalkorDB client | `falkordb` (official) | Typed decoding, embedded + networked |
| Async runtime | `tokio` | Standard |
| Graph topology | `petgraph` (`StableGraph`) | Stable indices, CSR adjacency |
| Concurrent maps | `dashmap` | Sharded, lock-free reads |
| Snapshotting | `arc-swap` | Lock-free reader / single-writer publish |
| Interning | `lasso` | Thread-safe interner with `Spur` handles |
| Bitmap sets | `roaring` | Compressed integer sets |
| Small strings | `smol_str` | Inline short strings |
| **Datalog** | **`crepe`** | Compile-time Datalog — derivation engine |
| **Scheme** | **`steel-core`** | Zero-copy Rust interop — belief evolution and explanation |
| HTTP framework | `axum` | `tokio`-native |
| MCP transport | `rmcp` | Official Rust MCP crate |
| Timezone | `chrono-tz` | Per-user Dreams schedules |
| Tracing | `tracing` + `tracing-opentelemetry` | Standard |
| Metrics | `metrics` + `metrics-exporter-prometheus` | R-O2 `/metrics` endpoint |
| Codegen | `schemars`, `inventory` | Generate MCP + OpenAPI schemas |
| **Cluster gossip** (backend only) | `chitchat` | Failure detection and membership (pinned §2.2) |
| **Invalidation transport** (backend) | `redis` pub-sub + `prost`-encoded payloads | Zero net-new infrastructure; binary encoding for high-throughput fan-out |
| **Cluster RPC** (backend only) | `tonic` (gRPC over HTTP/2) | Typed node-to-node calls; targeted refetch, hydration probes, control-plane |
| **Protobuf codegen** | `prost` + `prost-build` | Standard Rust protobuf; compiles from `exocortex-wire` schemas |
| **Coordinator** (backend) | `redis` `SET NX EX` + `WATCH` | Chubby-style leases with fencing |
| **SSE server** | `axum` + `tokio-stream` | Backend push channel to clients |
| **SSE client** | `eventsource-client` | Long-lived reconnecting subscriber |
| **WAL** | `sled` or custom append log | Local durable write log |
| **WAL codec** | `bincode` (v1); `rkyv` deferred | Compact serialization |
| **Signing** | `hmac` + `sha2` | Cluster deltas + SSE payloads + lease tokens |
| **Embedding runtime** (backend default) | `fastembed` | ONNX runtime, ships bge-small |

## Appendix B — Glossary

**Backend cluster** — the N-node server-side deployment that owns durable storage, runs Dreams cycles, and pushes change feeds to subscribed clients.

**MCP client (Exocortex mode `mcp-client`)** — the local per-developer Exocortex process that talks MCP to the coding harness and HTTPS+SSE to the backend cluster. Runs the same code as the backend but does no owner-only work.

**Chubby-style lease** — a lease with grace period and fencing epoch, borrowed from Google's Chubby paper. Prevents split-brain during partitions. Used for consolidation, Dreams, backfill, cleanup owner election.

**Fencing token** — a monotonic per-lease epoch. Storage rejects writes tagged with a stale epoch. Standard distributed-systems technique for preventing zombie-owner writes.

**2Q admission** — an LRU replacement that uses a small FIFO admission queue and a ghost queue to resist scan pollution. Used for graph-atomic cache eviction.

**SSE change feed** — the one-way server-to-client push channel over HTTPS. Backend publishes deltas as commits happen; client applies in backend-LSN order.

**Local LSN / backend LSN** — the two monotonic identifier spaces for writes. Local LSNs are assigned by the client's WAL. Backend LSNs are assigned by FalkorDB. Reconciled on sync ack.

**Session wrapup** — the batch of 1-5 structured memories a coding harness produces at session end, sent via `exocortex.end_session`. The primary and only capture path in v1.

**Two-language reasoning** — the invariant that all derivation is Datalog (Crepe) and all belief evolution or explanation is Scheme (Steel). Cypher is storage.

**Proposed provenance** — the sixth `Provenance` variant. Candidate connection surfaced by discovery; never becomes an edge until accepted via the `AcceptDiscovery` Action (§7.11).

**Operation registry** — the compile-time typed operation catalogue; source of truth for both MCP tool schemas and HTTP OpenAPI specs.

**`OntologyFingerprint`** — SHA-256 over the effective ontology (kernel + registered packs). Gates cluster membership and client connections. Successor to the old "schema fingerprint" terminology.

**Ontology** — the effective set of types, kinds, and rules the Exocortex kernel and its registered packs define. "The ontology" as a noun refers to this whole effective surface; "kernel ontology" refers to just the kernel's part; a specific pack's contribution is called that pack's ontology (e.g., "the dev-v1 ontology").

**Kernel and pack** — the two layers of the ontology. The kernel (`exocortex-kernel`) defines universal machinery (§7.0); packs (`exocortex-pack-*`) are Rust crates that register concrete `MemoryType`, `EntityType`, `RelKindId`, `type_triples!`, and `crepe_rules!` against the kernel. v1 ships `exocortex-pack-dev-v1` — 13 memory types, 12 entity types, 48 relationship kinds optimized for coding sessions.

**`RelKindId`** — interned `u32` handle for a relationship kind. Kernel-space ids (high bit clear) are declared as kernel constants and are semantically referenced by kernel rules; pack-space ids (high bit set) are declared by the pack's `kinds!` block. Replaces the earlier closed `RelationshipType` enum.

**Actions and Functions** — the two typed surfaces of Palantir Foundry's Ontology, adopted in Exocortex. Actions are typed writes (§7.11); Functions are typed reads (§7.12). Cross-org and open-web retrieval are never Functions.

**Ingestion Protocol** — the versioned protobuf schema (`IngestBatch` / `IngestAck`, §18.1) that every producer speaks to hand memories, entities, and relationships to the kernel. Session-wrapup is the first adapter; v2 first-party Iceberg / S3 Tables / Delta adapters plug into the same protocol.

**Adapter** — a process that reads an external source (Iceberg, Delta, Parquet directory, custom feed) and speaks the Ingestion Protocol. Adapters run out-of-process (`exocortex-worker`) so their dependencies (DuckDB, iceberg-rust, delta-rs, aws-sdk) never link into `exocortex-kernel`. Not to be confused with the storage adapter (§6.0), which is a kernel-internal seam.

**Org graph** — the single graph that holds all memories for one organization. Every user in the org reads and writes into this graph, filtered by `Visibility`. See §17.

**Visibility** — the label on every memory and relationship (`Private`, `Project`, `Team`, `Org`, `Public`) that determines which users can see it. Enforced at storage-adapter boundary and at every traversal step. Isolation lives in visibility, not in graph identity.

**Consolidation region** — a slice of the org graph identified by a stable key (v1: `(project_id, memory_type)`) that Dreams operates on under its own lease and MCR² score. See §17.3.

**Cross-region reconciliation** — a Dreams pass that runs after per-region consolidation and touches only edges crossing region boundaries. Bounded in edge budget to prevent org-wide blast radius.

**Wire version** — the additive-only version identifier on cluster-internal protobuf messages, distinct from the `OntologyFingerprint`. Both must match for peer admission. See §9.6.

**`exocortex-wire`** — the crate housing the protobuf schemas and `tonic`-generated stubs for cluster-internal messages. Client-facing wire formats (MCP JSON-RPC, SSE JSON) do not live here.
