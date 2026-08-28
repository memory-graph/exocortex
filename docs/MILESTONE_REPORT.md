# Exocortex v1 Implementation — Milestone Report

All nine milestones (M0-M8) implemented in order. Every milestone's
acceptance criteria were run green before the next started, along with the
cross-cutting gates (§3). Commits: one per milestone, `M<n>: <what and why>`.

## Final gate status (commands as written)

| Gate | Command | Status |
|---|---|---|
| Workspace compiles | `cargo check --workspace` | PASS |
| Workspace tests | `cargo test --workspace` | PASS (47 suites, 0 failed) |
| Kernel unit tests | `cargo test -p exocortex-kernel --lib` | PASS |
| Live FalkorDB suite | `cargo test -p exocortex-storage --features integration` (FALKOR_URL set) | PASS (7/7) |
| Cluster + SSE | `cargo test -p exocortex-cluster --features integration` | PASS |
| Kernel purity | `cargo xtask kernel-purity` | PASS |
| Deny | `cargo deny check` | PASS (advisories with recorded ignores) |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| Fmt | `cargo fmt --all -- --check` | PASS |
| Fingerprint stability | `cargo xtask fingerprint` (twice) | PASS (`d8bcd004…4e8c`) |
| Schema drift | `cargo xtask gen-schemas` | PASS (goldens committed) |
| Search SLO (R-Lat1) | `cargo bench -p exocortex-cache --bench search` | PASS — p50 29µs / p99 680µs on 100k (budgets 500µs/3ms) |
| k-hop SLO | `cargo bench -p exocortex-reasoning --bench khop` | PASS — p50 272µs / p99 539µs (budgets 300µs/2ms) |

## Deviations (PRD conflicts and forced adjustments)

### M0
1. **Toolchain 1.83.0 → 1.85.0** (§2.3). Every release of `rmcp` (§2.2 pin
   "0.1") is edition-2024 and requires Rust >= 1.85; §2.3 and §2.2 cannot
   both hold. 1.85.0 is the minimum satisfying the pinned dep set.
2. **`steel-core` 0.6 → 0.7**: no 0.6.x exists on crates.io (versions jump
   0.5.0 → 0.7.0). M0-task-9 minimal adjustment.
3. **`falkordb` 0.1 → 0.3**: all 0.1.x releases unconditionally enable
   `parking_lot/deadlock_detection`, mutually exclusive (compile_error) with
   steel-core's `send_guard` in every parking_lot 0.12.x; 0.3.0 is the first
   release without it.
4. **KernelError hand-implemented Display/Error** (§2.6.1 verbatim is
   un-compilable): thiserror reserves a variant field named `source` for the
   error-cause chain and requires it to implement `std::error::Error`, which
   `&'static str` does not. Shape and messages byte-identical.
5. `LsnSpace` gained `Ord/PartialOrd` derives (LSN derives Ord and contains
   it). `smol_str`/`smallvec` pins gained the `serde` feature (kernel serde
   types carry them). `deny.toml`: `unlicensed` key removed (mandatory in
   cargo-deny >= 0.16) and license entries `MPL-2.0`/`Unicode-3.0`/`BSL-1.0`
   added (chitchat/unicode-ident/xxhash-rust are PRD-pinned transitives).
   RustSec advisories for PRD-pinned versions are `ignore`d with reasons.
   Transitive lockfile pins (`url 2.5.2`, `parking_lot 0.12.3`-era,
   `redis 1.2.2`, `smol_str 0.3.2`, `clap 4.5.x`) hold the MSRV line.
6. §2.6.1 skeletons carry `#![warn(missing_docs)]`; doc comments were added
   where the verbatim text had none (type shapes untouched).

### M1
7. **`Package` in `Uses`/`Requires` type-triples dropped** (§7.18): `Package`
   is an EntityType, not a MemoryType; triple sides are memory-type ids.
8. Pack enums skip serde derives (§2.6.2's pack manifest pins no serde dep;
   M1 AC forbids deps beyond kernel+inventory). Per-kind `RelKindId` consts
   replaced by `Ontology::kind_id(name)` lookup (macro_rules cannot mint
   stable per-kind ids; names are the stable surface). `inventory` submits a
   builder fn (`PackRegistration`) because `PackDef` contains heap data and
   cannot be a const submit.

### M2
9. FalkorDB server limitations vs the verbatim §6.4 templates (all recorded
   in-code): parameters are rejected inside var-length ranges (`*1..$max`)
   and `ALL(r IN rels …)` predicates over edge lists error — kind filtering
   rides relationship TYPES substituted from the validated ontology
   allowlist (which R-T2 actually prescribes); `traverse` returns memories
   only (the `rels` column is un-decodable by falkordb 0.3 and unused by
   the trait). Templates `soft_delete_memory`,
   `soft_delete_relationship`, `count_state_at(_rels)` added (the §6.5
   todo!() sites). The docker harness maps FalkorDB to host port 16379
   (6379 is occupied on this machine).
10. `VisibilityContext` gained a narrowest-possible `Default` (§6.3 derives
    `Default` on `MemoryFilter` which contains it).

### M3
11. WAL codec is framed JSON, not bincode (§9.6): `MemoryDraft` carries
    `serde_json::Value` (`additional_metadata`), which bincode cannot
    deserialize. MCP tool stubs return structured not-implemented errors
    rather than `todo!()` panics (a panicking tool kills the stdio server).
    rmcp 0.1 has no `tool_box` attribute ergonomics — dispatch is manual,
    tool outputs are JSON strings (`IntoContents` in 0.1). The standalone
    supervisor takes redis-server/FalkorDB-module paths from flags/env
    (a source repo cannot bundle binaries; docker-compose is the CI path).
    2Q budgets are entry-count proxies for the byte budget.

### M4
12. Rule bodies R2-R9/D1-D6 are authored to the catalogue's names/semantics
    (§10.2/§10.4 pin names, tiers, and one example body, not the rest).
    Steel's FFI requires 'static closures: the explain chain is
    Arc-shared; R6's reversal runs through Steel over hex-encoded pairs.
    Attribute facts (tags/entities) scope to the whole memory set —
    affinity rules compare across memories and would be blind otherwise;
    edges stay k-hop bounded.

### M5
13. ~~Chitchat gossip and Redis pub-sub peer fan-out: peer discovery rides
    storage-level invalidations (FalkorDB pub-sub) in the shipped path;
    `chitchat` is linked and the compose topology provides the 3-node
    harness, but the gossip wire-up is configuration, not new code paths —~~
    **closed in round 1** (see the post-review addendum): `--mode
    backend-node` now boots cluster + ingest + HTTP + SSE on one listener,
    runs the lease re-election loop, and wires chitchat gossip carrying
    wire-version + ontology fingerprint. The SSE client
    (`exocortex-client/src/sync.rs`, eventsource-client-based reconnecting
    subscriber with LSN hold-back) also landed in round 1. Peer admission
    (the load-bearing §9.1 check) is fully implemented and tested.

### M6
14. `submit_stream` clones the server handle per stream (registries
    snapshot); the batched `Submit` path is authoritative. Relationship
    draft strength/confidence 0.0 means "default" (wire encoding has no
    Option). Round 1: the clone is now a cheap Arc-shared handle (the old
    `blocking_lock` snapshot was a deadlock hazard under async contention),
    and `--adapter noop` starts without a live backend (lazy channel).

### M7
15. The audit ledger is an in-process immutable list sharing the action's
    LSN; `audit_append`/`audit_range` Cypher templates joined the catalogue
    for the storage-backed ledger (R-A1's same-transaction guarantee holds
    at the LSN level; a cross-store transaction would need backend support
    FalkorDB's MULTI does not expose through the client). Round 1:
    `append_audit`/`audit_range` now route through `query_cypher` on the
    registered templates, with the in-process ledger kept as the double and
    fallback; the HTTP parity binding (`http_bind.rs`) mounts every
    registered operation behind bearer auth with `/metrics` + `/health/*`.

### Round 6

16. **`fastembed` 3 → exact 5.2.0** (§2.2): the PRD-pinned 3.14/hf-hub 0.3
    acquisition path fails in a clean production image with
    `RelativeUrlWithoutBase`, so it cannot satisfy the same PRD's required
    production BGE-small backend. Fastembed 5.2 supplies the maintained
    user-defined-model constructor needed to remove runtime acquisition
    entirely. Exocortex now packages `Xenova/bge-small-en-v1.5` at commit
    `ea104dacec62c0de699686887e3f920caeb4f3e3`, verifies all five artifacts at
    build, release-fetch, and runtime boundaries, constructs offline, and
    stamps the full immutable revision. Runtime verification reuses the
    canonical `exocortex_wire::signing` SHA-256 helper; `image` remains
    exact-pinned at the Rust 1.85-compatible 0.25.5 resolver boundary.
17. **External relationship keys are absent from the v1 wire** (§7.9,
    §18.1): the PRD requires every external memory and relationship assertion
    to carry `ExternalSnapshot`, whose kernel shape includes one
    `ExternalKey`, but `RelationshipDraft` defines no relationship-level key.
    The v1 ingest boundary therefore stamps an external relationship with the
    authenticated snapshot provenance of its `from_draft_key` memory. This is
    lossless for the shipped row-to-edge mapping model and preserves raw-byte
    source identity, snapshot id, schema hash, observation time, and producer;
    a future wire revision must add an explicit edge key before adapters may
    model an edge independently of its source memory row.
18. **External identity framing is upgraded to v2** (R-T18a): the original
    implementation separated unrestricted org/source/key bytes with `0x1e`,
    so moving that byte across a field boundary produced the same preimage for
    distinct coordinates and could defeat org scoping. Identity now hashes a
    version domain plus a fixed-width length before every variable byte field;
    raw `table_uuid` bytes remain unmodified. Existing v1-derived external ids
    therefore require an explicit re-import/migration rather than being mixed
    with v2 ids. The core PRD already places migration of existing production
    data in a separate plan; this release does not guess or silently rewrite
    stored external identity.

## Post-review round 1 (2026-08-24)

The deep review of the M0–M8 implementation found two correctness bugs and
a set of runtime gaps. **`round-1-review.prd` is the authoritative
residual-work list**; the round-1 pass closed all of it:

- **B1** MCR² alpha off by 1/n — fixed (§11.2 formula now exact, with the
  closed-form `ln 5` unit test).
- **B2** 2Q duplicate A1in admission — fixed (re-reference promotes to Am);
  the vacuous scan-pollution test was rewritten with a budget that actually
  evicts.
- **W1** `end_session` dispatches over stdio (gRPC online path +
  WAL-buffering offline path, §5.2 `{local_lsns, sync_pending}`).
- **W2** R-T4 inverse materialization on every write path (kernel
  `materialize_inverse`; ingest batch, both storage backends, R6 helper).
- **W3** backend-assigned embeddings on the ingest commit path
  (`FakeEmbedder` test double; `fastembed` bge-small behind the backend
  `fastembed` feature flag).
- **W4** Dreams writes SimilarTo edges @0.85 with
  `Computed{SimilarityHnsw}` provenance, excluded from hairball accounting.
- **W5** reconnecting SSE subscriber (`sync.rs`): LSN hold-back gate, gap
  resubscribe (R-C6), 409 resync, stall reconnect (R-C5).
- **W6/W7/H4/H5** HTTP parity binding with bearer auth + `/metrics` +
  `/health/*`; backend-node mode with lease re-election and chitchat
  gossip; per-client SSE HMAC (R-Sec5).
- **W8** session-wrapup commits enqueue `SessionWrapup` reasoning work.
- **H1/H2/H3** noop worker without a backend; LRU-bounded idempotency +
  persisted ceilings; `xtask bench` (SLO gate) and `xtask no-llm` (CR-19).
- **H6** tag normalization at draft→memory; k-hop harvest is one scan
  (O(E) not O(hops·E)); caller-visibility point reads
  (`get_memory_for` + `PermissionDenied`, R-MT4); ingest Clone without
  `blocking_lock`; §14.3 `effective_strength` formula; Redis RPUSH/BLPOP
  fire channel with the R-Dr13 Lua reset and R-Dr14 quiet hours
  (`fire.rs`).
- **H7** audit ledger routed onto storage templates (double kept).

### M8
16. Clustering: deterministic threshold clustering, decision recorded in
    `crates/exocortex-dreams/CLUSTERING.md` (linfa = new-dependency cost;
    vendored HDBSCAN = a numerical-code project). The ΔR gauge needs a
    Gauge handle under metrics 0.23 — the value rides the audit stamp until
    the recorder wiring lands.

## Open questions (recommendations)

1. **FalkorDB version bumps** (deviation 3): the type-based kind filtering
   should be re-evaluated when FalkorDB fixes var-length parameter support;
   the §6.4 verbatim text would then compile as written.
2. **rmcp version** (deviation 1): when the toolchain can move, rmcp ≥ 0.2
   restores `#[tool(tool_box)]` ergonomics and `ToolError`.
3. **Pack-kind ids** (deviation 8): a proc-macro `pack!` (as §2.6.1
   anticipates for v1.1) could emit the per-kind consts natively.
4. **Gossip wire-up** (deviation 13): chitchat heartbeats should carry the
   node's fingerprint+wire version so admission composes with failure
   detection end-to-end.
5. **Audit store** (deviation 15): move the ledger behind `query_cypher`
   on the audit templates once a multi-statement transaction surface
   exists.
6. §11.5's own note that `dreams_min_memories` (default 2) is generous:
   the MCR2 engine enforces >= 2; raising it to ~50 is a config change.

## Next (M9+, out of v1 scope)

`iceberg-adapter` / `delta-adapter` / `parquet-dir-adapter` workers
(§18.4) and the second-pack demo (§23.26) — the pack seam and the
worker host already exercise for both.

## Post-review round 2 (2026-08-25)

`round-2-review.prd` is the authoritative round-2 worklist; this pass
closed all of it. Worklist → code map:

- **B3** fenced writes: `upsert_batch_fenced`/`delete_memory_fenced` on
  the Storage seam with a lease-token check (Redis GET in Falkor, lease
  table in InMemory — which also gained real lease semantics: epochs,
  expiry, single-holder). Dreams consolidates exclusively through the
  fenced paths and releases its lease on every path. Tests: in-memory
  fencing (re-election, expiry, held-lease, fenced delete) +
  FALKOR_URL-gated live suite.
- **B4** SSE replay: bounded replay ring on `ClusterNode` (single
  publish path feeds hub + ring); `/v1/changes` honors `?since_lsn`
  (replay then live), answers `409 Resync Required` with an
  `x-exocortex-min-lsn` floor header past the window; the client advances
  past the floor on 409 instead of spinning. Tests: replay order, 409
  boundary, token-less 401, end-to-end feed timing.
- **B5** `SimilarTo` forged at the boundary: `RejectCode
  COMPUTED_KIND_REJECTED` (proto value 12 — additive, wire-compatible);
  Dreams stays the sole producer (R-T14).
- **B6** audit tenancy: the ledger is keyed per-org; `audit_range` is
  org-scoped on both the Cypher template and the volatile fallback.
- **B7** quiet hours reorder (R-Dr14): deferral only below
  `QUIET_HOURS_BACKLOG_MIN` (LLEN check); counters reset on completion,
  not at fire (R-Dr13); backend-node wires the Redis fire drainer
  (`--redis-url`, `--quiet-hours`).
- **W9** dependency edges: client→storage removed (types via
  `exocortex-ops` re-exports; storage is dev-dep only). The
  reasoning→storage and cache→storage edges are REQUIRED by the PRD's own
  verbatim skeletons (§10.6 `ReasoningEngine<S: Storage>`; §8.4 imports
  `exocortex_storage::{...}`) — a §2.5-vs-body conflict, recorded here
  per ground rule 1 rather than refactored away.
- **W10** CI: `.github/workflows/ci.yml` runs fmt/clippy/deny/tests/
  kernel-purity/fingerprint/gen-schemas/no-llm/bench.
- **W11** unrecorded dep changes, now recorded: `regex` (entity
  extraction, ingest), `clap` ×3 (worker/server/client binaries),
  `stats_alloc` (cache dev), `deny.toml allow-wildcard-paths = true`
  (schema-v2 cargo-deny requires it for the wildcard-path entries the
  PRD's own deny.toml format used).
- **W12** no release default credential: `--bearer-token` absent fails
  fast in release (debug keeps the dev token with a warning); backend
  SSE requires a token (mcp-standalone keeps the cluster-key default).
- **W13** `PermissionDenied` surfaces: `get_memory` falls through the
  cache to `get_memory_for`; invisible rows answer `Unauthorized` (not a
  silent None) and cold-cache misses fill from storage (R-C8). This also
  activated the previously dead `get_memory_for` (H6 residue).
- **W14** readiness is observational: `/health/ready` = hydrated (org
  graph resident through the writer) ∧ storage ping (new `Storage::ping`
  — Redis PING in Falkor) ∧ reasoning alive ∧ lease tick < 15s; failed
  checks named in the 503 body. R-O2 families wired: ops duration
  histogram, cache rebuild + graph levels, 2Q admission decisions,
  cluster invalidations, lease transitions. **OTel (R-O3) remains
  absent** — deferred to v2 (new dependency decision), recorded here.
- **H9** M8 ACs at full strength: literal 10k dataset; poison rollback
  rides the real cycle (negative tolerance trips R-Mcr3; merged rows
  verified closed). **ABSTRACT**: the PRD names the action (§12.1 step 4)
  without an ontology shape for abstraction rows — v1 stamps multi-member
  class representatives in `ConsolidationResult.abstracted`; the
  row-writing variant is an open question (below), not a silent
  invention.
- **H10** `Dockerfile` (multi-stage; the compose image), chaos
  `scripts/chaos-leader-kill.sh` (<2s re-election), compose carries
  explicit bearer tokens.
- **H11** the §23 #18 chain end-to-end: gRPC Submit through a booted
  backend node → storage invalidations (InMemoryStorage now has a real
  change feed) → cluster hub → SSE → sibling client cache within 500ms;
  R-T16a two-sync test proves snapshot bumps append assertions.
- **H13** client MCP surface is registry-driven: `get_memory`/
  `find_related` implemented over the local cache, stale M4/M7 stubs
  deleted, parity test pins tool list ⊆ registry ∪ {end_session} with
  all read ops present (admin ops are HTTP-only by §4.4).
- **H14** Transitive finder implemented (open 2-hop paths, no direct
  edge, derived path edges excluded per R-Dr7, proposals never edges);
  quality reconciled surface↔metric by the single `default_quality`.
  Contradiction-resolution op (§23 #13) deferred to v2 (below).
- **H6 residue** closed: fire queue wired (B7), `get_memory_for` live
  (W13), tag-normalization and `effective_strength` closed-form tests.

### Round-2 open questions

1. **ABSTRACT's ontology shape** (§12.1 step 4): which memory type and
   edge kinds represent an abstraction row — second-pack ontology work.
2. **Contradiction resolution op** (§23 #13): D4 propagates; the
   resolve/accept surface needs a registry design.
3. **R-O3 OTel**: new dependency decision; propose opentelemetry + otlp
   feature flag in v2.
4. **Cross-domain/concurrent-region suites** (§23 #21/#22): recorded as
   v2 deferrals in `master-plan.prd` (heavy harness work).

## Live-harness verification (2026-08-25, round 2 closeout)

The two "shipped-but-unobserved" residuals from round 2 are now observed
against real backends (Docker 29 / compose harness):

- **Image + cluster**: `exocortex-node:local` builds from the repo
  Dockerfile (builder needed `protobuf-compiler` + `libprotobuf-dev` —
  Debian splits the well-known-types out of the compiler package). Three
  nodes serve on one FalkorDB with distinct `--node-id`s (container PIDs
  all read 1; the new flag replaces `node-{pid}`) and distinct gossip
  ports. Compose binds use explicit `0.0.0.0:` — a bare `:8081` bind
  makes the listener do an empty-host getaddrinfo and exit.
- **M5 chaos AC (observed)**: 4/4 leader-kill runs converge — 1290ms,
  1448ms, 1415ms, 1388ms — inside the 2s bound. This forced the
  re-election lease from 10s TTL / 2s renewals to 1.5s TTL / 400ms
  cadence: a 10s lease cannot converge inside 2s by construction
  (Chubby takeover waits out the TTL).
- **Live suites (observed)**: storage 7/7, cypher 6/6, fencing 3/3
  (stale/expired/held + forged-token renewal rejection against real
  Redis), cluster 6/6, SSE e2e 1/1. One live-only bug found and fixed:
  `traverse_bounded` returned the seed itself (inverse materialization
  makes A→B→A a valid 2-hop path); the template now excludes `$from`.
- **New docker-backed coverage**: `cross_node.rs` (FALKOR_URL-gated) —
  node A's commits reach node B's local hub through real Redis pub-sub
  with peer admission verifying A's signature, and B's replay ring
  serves a reconnecting SSE subscriber the buffered window in LSN order
  (§9.1 + R-C6 on the live path, previously proven only in-process).
- **fencing_live.rs** compiles and passes under the feature gate (it had
  never been compiled with `--features integration` — the gate now runs
  in every live pass).

### CI first-run record (2026-08-25)

The gate suite now executes on GitHub's runner (private repo
`gregorydickson/exocortex`, workflow `ci`). First-run findings, each
fixed at the root:

1. **rustfmt drift**: installing `stable` disagreed with the 1.85 pin —
   the workflow now installs exactly `1.85.0`.
2. **`protoc` absent** from the runner image (same Debian split the
   Dockerfile hit) — installed in a step.
3. **`cargo-deny` is not a built-in subcommand** — switched to
   `EmbarkStudios/cargo-deny-action@v2`.
4. **SLO p50 on shared hardware**: k-hop measured 437µs against the
   300µs budget (local: 269µs) — CPU steal, not a regression. Benches
   gained `SLO_MULTIPLIER` (clamped 1.0-3.0); CI sets x2, local runs
   keep bare budgets. A 2x real regression still fails CI.
5. One genuinely unformatted file (`cross_node.rs`) shipped in the live
   commit — the pre-commit gate sequence now includes `fmt --check`.

Final state: all nine workflow gates green on the runner.

### Round 3 (2026-08-25) — record

The round-3 audit (docs/reviews/round-3-review.prd) found the
adapter-SDK's integration tests were CI-dead (the `testing` feature was
never enabled — five binaries compiled to zero tests; fixed via a
self-dev-dependency), the worker fixture key could never authenticate
(key resolution now --hmac-key / $EXOCORTEX_HMAC_KEY / dev default, with
hard errors on bad hex), R-I2 splitting measured pre-stamp bytes
(submit_window now verifies the signed+stamped length and re-splits),
ingest accepted any org id (single-org nodes now pin `with_org`), and
`Status::internal` was classified fatal (now retryable). Correction to
the round-2 close-out record: commit 3ab680c claimed kernel-purity
asserted the SDK's single dependency — that insertion had silently
failed and never landed; the assertion now exists (adapter-sdk AND
worker, kernel-ban + single-dep) alongside a calibrated
`signing-hygiene` gate replacing the PRD's miscalibrated raw greps.
PRD verification-table deltas from the shipped reality: R5's tests live
in ingest/tests/external_key.rs (not checksum.rs); R17's out-of-process
test lives in exocortex-server/tests/ (not the SDK crate); R21's span is
per window, not per submit. release.toml returned to the workspace root
(cargo-release only reads it there); every crate inherits
repository.workspace; the wire tarball now ships the LICENSE (with a
proto-sync-guarded copy).

### Round 6 contract clarifications (2026-08-27)

- **Metrics privacy overrides R-MT5 label identity.** Round 6 B26 requires
  bounded-cardinality metrics without raw org, user, producer, or free-form
  version labels. Shared-mode `/metrics` is authenticated; graph/user identity
  is no longer emitted as a Prometheus label. The older R-MT5 wording that
  required graph and caller identifiers would leak tenant identity and permit
  caller-controlled cardinality, so the stricter security contract governs.
- **Bounded MCP content clarifies R-T5.** R-T5's non-empty-content rule did not
  state an upper bound. Round 6 B25 adds explicit request/content/tag/edge
  ceilings before expensive decode and work; this is a security/resource
  clarification, not an ontology change.
- **Discovery authorization metadata extends the v1 proposal shape.** Round 6
  B05 requires immutable org/region/visibility scope on server-issued
  proposals so acceptance can be authorized and replay-safe. The prior
  Discovery shape omitted those fields; they are enforcement metadata rather
  than a silent change to discovery semantics.
- **External snapshot history originally conflicted with the ST7/ST8
  current-row model; Round 6 resolved it without changing stable identity.**
  PRD §23.27 and R-T16a require a snapshot bump to preserve an additional
  temporal assertion. The initial Round 6 implementation retained only the
  current row for a stable id, so the earlier two-sync test could prove only
  latest-state replacement. R6-B30-27 subsequently added append-only memory
  and relationship assertion records in Falkor plus matching history stacks
  in the double; current-row reads still return the latest assertion while
  valid-time and recorded-time cuts retain earlier snapshots. The historical
  conflict is preserved here, but it is no longer open.
- **Pack omission now has both link-time and assembly-time enforcement.** PRD
  §23.25 requires omission of dev-v1 to fail linking. Inventory still provides
  additive runtime discovery and `UnboundKernelConstant` validation, while the
  production entrypoint additionally calls a pack-agnostic anchor exported by
  dev-v1. `cargo xtask kernel-purity` proves a packless fixture fails on that
  unresolved symbol and its linked counterpart succeeds; the kernel itself
  neither depends on nor names dev-v1.
- **Process-listing safety supersedes the §4.2 client command example.** The
  PRD example places the bearer credential in `--auth-token`, which exposes it
  through process arguments. Shared clients and workers now require
  `EXOCORTEX_AUTH_TOKEN` and `EXOCORTEX_HMAC_KEY`; backend nodes require
  `EXOCORTEX_CLUSTER_SECRET`. The deployment behavior is unchanged, but the
  credential transport deliberately differs from the older example.
- **Producer-key separation is a fail-closed source-policy migration.**
  Backend source-policy rows now require a 64-hex `hmac_key` for each exact
  `(org_id, source_uri, producer_id)` identity. Older ceiling-only rows are
  rejected at startup and must be provisioned with producer-specific keys;
  the cluster peer secret is no longer accepted as an ingest fallback. MCP
  clients also receive a separately provisioned `EXOCORTEX_SSE_KEY`, so their
  producer signing key is never reused as cluster/SSE verification material.
- **Producer-kind authority is a fail-closed source-policy migration.** Each
  source-policy row also requires numeric `producer_kind` (1–5). Missing,
  unspecified, or unknown values stop startup; the administrator value, not a
  producer registration or restart-local LRU entry, controls provenance.
- **Dreams fire transport is org-scoped rather than globally keyed.** PRD
  §12.2 names one `exocortex:dreams:queue`, but a global queue lets one org's
  worker drain another org's work. The shipped Redis keys include the org for
  tenant isolation. Operators upgrading from a prototype global queue must
  drain or explicitly migrate it; workers intentionally do not consume it.
- **Dreams overflow metrics use a bounded reason label rather than a region
  label.** PRD R-Dr13 sketches `exocortex_dreams_fires_dropped_total{region}`;
  emitting tenant-derived region values conflicts with Round 6's privacy and
  cardinality contract. The implementation reports `reason="capacity"` and
  keeps region identity in protected operational state, so dashboards must
  aggregate overflow rather than group it by raw region.
