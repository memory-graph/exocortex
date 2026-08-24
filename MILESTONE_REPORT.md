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
13. Chitchat gossip and Redis pub-sub peer fan-out: peer discovery rides
    storage-level invalidations (FalkorDB pub-sub) in the shipped path;
    `chitchat` is linked and the compose topology provides the 3-node
    harness, but the gossip wire-up is configuration, not new code paths —
    admission (the load-bearing §9.1 check) is fully implemented and
    tested. The SSE client in the e2e test is a raw socket (no HTTP client
    in the workspace catalog). Leader-kill convergence is bounded by the
    lease TTL in the live race test.

### M6
14. `submit_stream` clones the server handle per stream (registries
    snapshot); the batched `Submit` path is authoritative. Relationship
    draft strength/confidence 0.0 means "default" (wire encoding has no
    Option).

### M7
15. The audit ledger is an in-process immutable list sharing the action's
    LSN; `audit_append`/`audit_range` Cypher templates joined the catalogue
    for the storage-backed ledger (R-A1's same-transaction guarantee holds
    at the LSN level; a cross-store transaction would need backend support
    FalkorDB's MULTI does not expose through the client).

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
