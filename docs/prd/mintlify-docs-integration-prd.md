# `exocortex-adapter-mintlify` — PRD

**Owner:** Gregory Dickson
**Status:** Draft
**Parent design docs:**
- `exocortex-core-prd.md` §7 (Ontology), §13 (Session Capture), §17 (Tenancy), §18 (Ingestion Protocol), Appendix B (Glossary)

## 0. Summary

`exocortex-adapter-mintlify` is a reference Ingestion Protocol adapter that turns any Mintlify docs site into a source of typed graph memories. It watches a git repo containing MDX pages, extracts `exocortex:` frontmatter from pages whose authors have opted in, and emits `IngestBatch` messages to the Exocortex kernel.

The adapter ships as a standalone binary outside the core Exocortex workspace. It is a **custom adapter** per `exocortex-core-prd.md` §18.4 — it implements the `IngestService` client contract and registers its own `source_flavor`, but is not part of the kernel or its dependency tree. It serves as the reference implementation for orgs building their own doc-site adapters.

The adapter is **pack-agnostic**: it validates `memory_type` and `relationships` against whatever ontology pack the deploying org has registered. It knows nothing about specific domain types — the kernel enforces type validity at ingest time.

The design invariant: **the docs repo is the source of truth for docs.** The graph is a derived, structured index of the parts of the docs that map onto the ontology — not a copy of the prose.

## 1. Problem and non-goals

### 1.1 Problem

- Engineering docs sites (Mintlify, but the pattern generalizes) contain named entities — agents, integrations, runbooks, modules, endpoints — that the Exocortex reasoning layer cannot reference as first-class graph nodes.
- Session-wrapup memories from coding sessions link to *code* (via `MemoryContext.files_involved`) but have no representation of the entities described in docs. Reasoning rules like R7 (`co_occurrence_affinity`) cannot connect "the audit pipeline" as a graph node across sessions if it has never been named as a memory.
- There is no deterministic, LLM-free path from a docs page to a structured memory. Manual memory creation doesn't scale; LLM summarization violates R-D6 (no LLM in the Exocortex backend).

### 1.2 Non-goals

- **Bulk-ingesting all pages as `Memory` rows.** Only pages with explicit `exocortex:` frontmatter are ingested. Absence is a valid, expected signal.
- **LLM-driven docs summarization.** Violates R-D6. Author-declared frontmatter is the deterministic alternative.
- **Replacing the docs site with a graph view.** The Mintlify site remains the human-readable surface.
- **Real-time sync.** The adapter runs on doc-repo commits, not on read. Latency to graph is minutes, not milliseconds — this is a write-side flow, not a read-path concern.
- **Docs as a Function surface.** Adapters do not implement `Function`s (§18.2); the docs site is a *write* into the graph, not a *read* from it.
- **Defining domain-specific memory types.** The adapter is pack-agnostic. Which types exist is the deploying org's ontology pack's concern.
- **Migrating away from Mintlify.** Out of scope; if it happens later, the adapter's `source_flavor` gets a new implementation but the graph shape is unchanged.

## 2. The `exocortex:` frontmatter schema

The critical design choice. This is what makes structured ingest possible without an LLM.

### 2.1 Schema

```yaml
---
title: "How the auth service calls the policy engine"
description: "Architecture doc describing the auth→policy handoff."
# ... existing Mintlify frontmatter ...

exocortex:
  # REQUIRED. Stable id across renames. Format: <namespace>.<entity-kind>.<slug>.
  # Changing this creates a new memory; the old one becomes orphaned.
  id: "myorg.integration.auth-policy-bridge"

  # REQUIRED. Must be a memory type registered by the org's ontology pack.
  # Case-sensitive. The adapter does not validate this — the kernel rejects
  # unknown types with INVALID_TYPE_TRIPLE.
  memory_type: Integration

  # OPTIONAL. Visibility ceiling for this memory. Defaults to Org.
  # Must be <= the source_uri's registered ceiling (§17).
  visibility: Org

  # OPTIONAL. Importance in [0, 1], clamped by F01. Defaults to 0.5.
  # Feeds §14 scoring.
  importance: 0.8

  # OPTIONAL. Named entities the doc references. The extractor seeds these
  # as Entity nodes; matching name+type in other memories creates the same
  # Entity by content-hash identity (§7.2 R-T18).
  entities:
    - { type: Technology, name: "AuthService" }
    - { type: Technology, name: "PolicyEngine" }

  # OPTIONAL. Relationships declared by this doc. Each is a
  # RelationshipDraft on the wire (§18.6). `from` and `to` reference
  # entities by name; the adapter resolves them within the batch.
  relationships:
    - { from: "AuthService", kind: IntegratesWith, to: "PolicyEngine" }

  # OPTIONAL. File paths (relative to consuming repos) this page describes.
  # Populates MemoryContext.files_involved so R7 can co-occur this memory
  # with sessions that touch those files.
  references:
    - "services/auth/handler.py"
    - "services/policy/dispatch.py"

  # OPTIONAL. Free-form tags that end up on Memory.tags. Used by §14
  # scoring and by R9 (similar_tags_affinity).
  tags: ["auth", "policy", "core-flow"]

  # OPTIONAL. Marks this page as a Runbook step sequence. When present,
  # the adapter emits Precedes edges between the steps in order.
  runbook_steps:
    - id: "step-1"
      name: "Fetch snapshot"
    - id: "step-2"
      name: "Evaluate policy rules"
    - id: "step-3"
      name: "Compose result"

  # OPTIONAL. Marks a deprecation. Creates a Problem memory and a
  # Replaces edge to the successor's exocortex.id.
  deprecates:
    - "myorg.integration.legacy-auth"
---
```

### 2.2 Schema rules

- **Absence is legal.** Pages without an `exocortex:` block are docs-only. The adapter skips them silently.
- **`id` is stable.** The adapter refuses to ingest a page whose `id` conflicts with an existing `MemoryId` derived from a *different* `logical_pk`. Renames that keep the `id` are safe; renames that change the `id` create a new memory and orphan the old one (documented, not enforced).
- **`memory_type` must be pack-registered.** Unknown types fail the batch with `INVALID_TYPE_TRIPLE`. The adapter does not maintain a type list — it forwards to the kernel and reports rejections.
- **`relationships` are validated against `type_triples!`.** A relationship declared in frontmatter that violates the pack's type-triple table fails the batch with `INVALID_TYPE_TRIPLE` naming the offending row.
- **Entity identity is by name+type.** When multiple pages declare the same entity (same `name` and `type` in the `entities` list), they reference the same `Entity` node via content-hash identity (`exocortex-core-prd.md` §7.2 R-T18). `importance` and `tags` are properties of the *page's memory*, not of the entity node — there is no conflict to resolve. The entity is the join point; each page is its own memory with its own metadata.
- **`schema_hash` covers this schema.** Bumping the schema (adding a required field, tightening a validator) forces a full re-ingest via `ExternalSnapshotInfo.schema_hash` change.

### 2.3 What gets ingested — and what does not

**Ingested (opt-in via frontmatter):**

- Pages that describe a *named entity* matching a type in the org's ontology pack.
- Pages that declare *relationships* between named entities.
- Pages that document a *deprecation* as a `Problem` linked via `Replaces` to its successor.

**NOT ingested:**

- Pages without an `exocortex:` frontmatter block.
- Marketing intros, welcome pages, getting-started prose.
- Screenshots and rendered diagrams. (Diagram *source* can become a `Documents` edge from an architecture doc to the entities it depicts — but the image itself is not a memory.)
- Reference tables that already exist in code (config schemas, error code lists). Code is truth; ingesting the doc version creates two sources of truth for one fact.
- Anything that duplicates what session-capture already writes (a runbook step that a session executed becomes a session memory; the runbook page is the *template*, not the *execution*).

## 3. Adapter architecture

### 3.1 Position in the memorygraph architecture

- **Producer type:** Custom adapter per `exocortex-core-prd.md` §18.4. Out-of-process, never linked into the kernel.
- **Wire contract:** tonic gRPC to `exocortex.ingest.v1.IngestService`; canonical schema in `exocortex-core-prd.md` §18.6.
- **Provenance:** `Provenance::ExternalSnapshot` with an `ExternalSnapshotInfo { source_flavor: "mintlify", schema_hash, snapshot_id, observed_at }` — the docs repo has a snapshot concept (git commit SHA), so it qualifies.
- **Identity:** Every proposed memory carries `ExternalKey { table_uuid: <docs-repo-uuid>, logical_pk: <frontmatter.exocortex.id>, mapping_version: <adapter-version> }`. Adapter enforces that `exocortex.id` is stable across renames — that is the rule that makes the graph immune to page reorganization (R-T18a).
- **Trust circle:** Adapter runs in a separate process from the kernel. Its dependency stack (git, MDX parser, frontmatter YAML) does not link into any interactive read-path crate.

### 3.2 Repository and crate layout

The adapter lives in its own repository, **not** inside the Exocortex core workspace. Per `exocortex-core-prd.md` §18.4, custom adapters are external binaries that implement the `IngestService` client contract. The adapter's only compile-time dependency on Exocortex is `exocortex-wire` (the generated proto types).

```
exocortex-adapter-mintlify/
├── Cargo.toml
├── src/
│   ├── main.rs          # bin: config, register_source, main loop
│   ├── git_watcher.rs   # git polling, commit diffs, changed-file set
│   ├── mdx_parser.rs    # frontmatter extraction (YAML) + MDX body parsing
│   ├── frontmatter.rs   # `exocortex:` schema + validation
│   ├── batch_builder.rs # IngestBatch construction, checksum, HMAC signing (§18.6)
│   └── config.rs        # source_uri, HMAC key location, backend endpoint
└── tests/
    ├── golden_batches/  # frontmatter → expected IngestBatch protobuf
    └── integration.rs   # spins up a mock IngestService, replays a fixture repo
```

### 3.3 Dependencies (adapter-only, never linked into kernel)

- `git2` — git repo access.
- `serde_yaml` — frontmatter parsing.
- `pulldown-cmark` with MDX extensions — body parsing.
- `tonic` (client-side only) — talks to `IngestService`.
- `exocortex-wire` — the generated proto types (the ONLY Exocortex crate the adapter depends on).

### 3.4 Configuration

All adapter behavior is driven by configuration, not compiled-in constants:

| Parameter | Source | Description |
|---|---|---|
| `docs_repo_path` | env/CLI | Local path to the Mintlify docs git repo |
| `source_uri` | env/CLI | Org-chosen URI (e.g. `mintlify://myorg/docs`) |
| `org_id` | env/CLI | Org identity for `RegisterSourceRequest` |
| `producer_id` | env/CLI | Adapter instance identity |
| `backend_url` | env/CLI | `IngestService` gRPC endpoint |
| `poll_interval` | env/CLI | Git polling interval (default 60s) |
| `hmac_key_path` | env/CLI | Path to HMAC signing key |
| `default_branch` | env/CLI | Branch to watch (default `main`) |
| `visibility_ceiling` | env/CLI | Max visibility for memories from this source (default `Org`) |

### 3.5 Main loop

```rust
// crates/adapters/exocortex-adapter-mintlify/src/main.rs
#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    let mut client = IngestServiceClient::connect(cfg.backend_url.clone()).await?;

    // Idempotent registration.
    client.register_source(RegisterSourceRequest {
        org_id: cfg.org_id.clone(),
        source_uri: cfg.source_uri.clone(),
        producer_id: cfg.producer_id.clone(),
        ceiling: cfg.visibility_ceiling as i32,
    }).await?;

    let repo = GitRepo::open(&cfg.docs_repo_path)?;
    let mut last_sha = repo.head_sha()?;

    loop {
        tokio::time::sleep(cfg.poll_interval).await;
        let cur_sha = repo.head_sha()?;
        if cur_sha == last_sha { continue; }

        let changed = repo.changed_mdx_files(&last_sha, &cur_sha)?;
        for path in changed {
            let page = repo.read_at(&cur_sha, &path)?;
            let Some(fm) = parse_exocortex_frontmatter(&page)? else { continue };
            let batch = build_batch(&cfg, &fm, &cur_sha, &page)?;
            let ack = client.submit(batch).await?;
            if let Some(reject) = ack.reject {
                report_reject(&path, &reject);
            }
        }
        last_sha = cur_sha;
    }
}
```

Key properties:
- Idempotent on `batch_id = "<producer_id>:<commit_sha>:<page_id>"`. Re-running the adapter after a crash replays without duplication.
- **`last_sha` only advances after all pages in the diff have been successfully submitted or permanently rejected.** A transient transport error (network timeout, connection refused) retries with exponential backoff; the SHA does not advance until the page succeeds or the adapter is restarted.
- `RATE_LIMITED` responses from the kernel trigger exponential backoff per `exocortex-core-prd.md` §18.2 item 6. The adapter does not advance `last_sha` while rate-limited.
- HMAC signing: `batch_builder` reads the key from `hmac_key_path` (§3.4) and signs each `IngestBatch` per the wire contract in `exocortex-core-prd.md` §18.6. The kernel rejects unsigned or mis-signed batches with `HMAC_INVALID`.
- Never reads external bytes on the interactive path — the adapter is a separate process.
- No LLM anywhere in the code path (grep-enforced in CI).

### 3.6 Change detection

Git polling at configurable interval (default 60s). Not a webhook because:
- Webhooks require public ingress to the adapter, which crosses a trust boundary.
- The docs repo is source-of-truth; polling latency of 60s is acceptable for a write-side flow.
- Webhooks are a future optimization if the docs repo grows to the point where polling is expensive.

### 3.7 Deleted pages

When a page with `exocortex:` frontmatter is deleted from the watched branch, the adapter closes its memories via `valid_until = commit_time`, not hard-delete. This preserves bi-temporal history.

## 4. CI validation (deployer-provided)

Deploying orgs should add a `mintlify-frontmatter-validate` CI job to their docs repo:

1. Parse every MDX file's frontmatter.
2. If `exocortex:` is present, validate it against the schema (§2.1).
3. Optionally run a dry-run of the adapter's type-triple validator (shipped as a standalone binary).
4. Fail the PR if any `exocortex:` block would be rejected by the kernel.

The adapter crate exports a `validate` library entry point and a `exocortex-adapter-mintlify validate <path>` CLI subcommand for this purpose.

## 5. Deployment prerequisites

The adapter is a *client* of the Ingestion Protocol (§18.6). It does not ship until:

- [ ] The Ingestion Protocol (`exocortex-ingest`) has passed acceptance.
- [ ] The deploying org has a registered ontology pack containing the memory types referenced by their docs' `exocortex:` frontmatter.
- [ ] The deploying org has a `source_uri` registered with an appropriate visibility ceiling.

## 6. Acceptance criteria

- [ ] Adapter binary (`exocortex-adapter-mintlify`) runs as a standalone process, watches a docs repo via git polling, and emits `IngestBatch` on each commit that changes a page with `exocortex:` frontmatter.
- [ ] Adapter is fully configurable via environment/CLI — zero hardcoded org identifiers.
- [ ] `MemoryId`s are stable across a page rename that preserves `exocortex.id`.
- [ ] Batches with unknown `memory_type` are rejected by the kernel with `INVALID_TYPE_TRIPLE` — the adapter reports the rejection, not crashes.
- [ ] Batches with invalid `relationships` are rejected with `INVALID_TYPE_TRIPLE` naming the offending row.
- [ ] Deleted pages emit `valid_until` closures, not hard deletes.
- [ ] Session-wrapup memories link to docs-derived memories through R7 (`co_occurrence_affinity`) when `MemoryContext.files_involved` overlaps with a docs-derived entity's `references`.
- [ ] Zero LLM calls in the adapter or the ingest path (grep test in CI).
- [ ] `xtask kernel-purity` confirms the adapter depends only on `exocortex-wire`.
- [ ] Transient submit failures retry with exponential backoff; `last_sha` does not advance past unsubmitted pages.
- [ ] `RATE_LIMITED` responses trigger backoff per `exocortex-core-prd.md` §18.2 item 6.
- [ ] Golden-batch tests cover: basic frontmatter, runbook steps, relationships, deprecations, missing optional fields, absent `exocortex:` block (skipped), and invalid `memory_type` (rejected).

## 7. Open questions

1. **Docs versioning across Mintlify releases.** Does the adapter treat a version bump as a new snapshot or a new source? Recommendation: same `source_uri`, new `snapshot_id` derived from the commit SHA at the version-tag commit. Bump `schema_hash` if the version bump changes the frontmatter schema.
2. **Deleted pages.** The adapter should close memories via `valid_until = commit_time`. Covered in §3.7; needs acceptance test.
3. **Draft PRs / branch previews.** Should the adapter ingest from a non-default branch? Recommendation: no. Only the configured default branch is a source. Preview branches are for humans reviewing the docs, not for the graph.
4. **Pack-unknown `memory_type` at CI time.** When a page has `exocortex:` but the pack doesn't recognize the type, the kernel rejects the batch. The CI validator (§4) can optionally check against a local pack manifest to catch this earlier. Not required for the adapter itself.
5. **Cross-repo `references` validation.** A doc page's `references` may point into repos the adapter doesn't have. The adapter records references verbatim; a separate cross-repo validator is a future concern.

## 8. Success metrics

- **Freshness.** Wall time between a docs commit and the corresponding `IngestBatch` ack. Target: <2 minutes p99.
- **Correctness.** Fraction of `IngestBatch`es that ack without a `reject`. Target: >99%. Rejects should always trace back to a frontmatter error in the docs repo.
- **Reasoning payoff.** Fraction of sessions whose wrapup memories connect to at least one docs-derived memory via R7 or R9. Target: >30% within 3 months of deployment. Below this, the adapter is not earning its cache footprint.
- **No LLM regression.** Zero LLM calls originating from the adapter path. Enforced by `xtask no-llm` CI check.

## 9. Decision log

- **Why not bulk-ingest all pages as `General` memories?** Untyped memories don't feed the reasoning rules (R1–R3 need typed nodes and typed edges); the cache pressure would be unjustified.
- **Why not an LLM-driven summarizer?** Violates R-D6 (no LLM in the Exocortex backend). Author-declared frontmatter is the deterministic alternative; it also puts the docs author in control of the graph shape.
- **Why a separate ontology pack per org instead of extending `pack-dev-v1`?** `pack-dev-v1` is the generic developer pack that ships with the kernel. Domain-specific types (agents, runbooks, integrations, rule modules) belong in domain packs — the pack system exists precisely so orgs can extend the ontology without forking the kernel (`exocortex-core-prd.md` §7).
- **Why git polling instead of a webhook?** Trust boundary; the adapter should not require public ingress. Configurable polling interval is acceptable for a write-side flow.
- **Why frontmatter YAML instead of a separate sidecar file per page?** Frontmatter is already how Mintlify pages carry metadata; a sidecar doubles the number of files to keep in sync.
- **Why is the adapter pack-agnostic?** The adapter's job is parsing and transport. Type validation belongs to the kernel, which already does it. Duplicating that logic in the adapter creates two sources of truth for type validity.
