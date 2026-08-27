# Bug PRD — standalone mode writes are never readable

| Bug PRD | | Three defects in `exocortex-client` that make the zero-setup personal mode write-durable but read-blind |
|:---|:---|:---|
| **Author**: Gregory Dickson | **Status**: **Closed** — shipped as `exocortex-client 0.2.1` (2026-08-26; F3+F4 landed as one commit: replacing the boot seed left the synth path dead code; all seven acceptance criteria covered, see `client/tests/standalone_readback.rs`) **Created**: 2026-08-26 | **Visibility**: Internal |

## Summary

The README's headline path — install, add to the agent's MCP config,
paste the block — produces a system where **every write the agent makes
is invisible to every read, permanently**. Three defects compound:

1. **No live read-back.** `end_session_offline` (`mcp.rs:243`) appends
   the batch to the WAL, then calls only
   `cache.advance_local_lsn` (`cache/src/lib.rs:777`) — which bumps a
   counter. The buffered memories never enter the served snapshot, so
   `search_memories` cannot return what the same session just wrote.
2. **No boot re-seed.** Startup (`main.rs`) publishes either synthetic
   filler (standalone) or an empty snapshot (`--backend`). No path ever
   reads the WAL back into the snapshot. A restart is just as blind as
   the live session was — despite the data being durably, id-stably on
   disk the whole time (WAL entries carry their assigned `memory_ids`).
3. **The reads that DO return are fabricated.** In standalone mode the
   snapshot is seeded with four synthetic memories ("Fix flaky auth
   test" & co., `main.rs:77`). CL5 gated synthetic data out of backend
   mode; in personal mode it remains — so the agent's first `search_memories`
   returns inventions while its real writes return nothing. Worse than
   empty: it teaches the agent the tools "work" on content that does not
   exist.

**Why this is serious.** The product thesis is compounding ("sessions
compound across time because you write to it"). The zero-setup mode —
the mode every first user lands in — implements only the write half.
The prior deployment's success (memory-graph) was built on exactly this
loop working with zero setup (embedded storage); its absence here makes
standalone a demo of the write path rather than a usable personal mode.

**Not a defect** (checked, documented as design): in `--backend` mode,
read-your-writes rides the SSE feed back from the server commit
(`e2e_chain.rs::wrapup_chain_grpc_to_sse_to_sibling_client`, ~500ms
window). That path is intact. This PRD is about the no-backend mode.

## Root cause

Three pieces exist and none are linked:

- the WAL is a complete embedded store (entries carry drafts whose
  `edge_hints` are already resolved against the assigned ids, plus the
  ids, draft keys, and tags in parallel arrays — everything a snapshot
  needs);
- `GraphSnapshot::insert_memory` maintains the full read surface
  (by_id, by_entity, by_type, by_tag, and the CR3-corrected search
  arena) — it is exactly the ingestion primitive needed;
- the offline write path and the boot path simply never call it. The
  wiring was left at the M3 state ("buffer-only", MILESTONE_REPORT
  deviation) and every round since (W1's drain, CL6's LSN advance)
  improved the buffer without closing the read loop.

## Fix plan

Five changes: the wiring lives in `exocortex-client`, plus one small
additive API in `exocortex-cache` (F2 — the snapshot insert methods are
private today). No protocol change. Ordered; F1 unblocks F2/F3/F5.

### F1 — one materializer: WAL entry → kernel `Memory`

A single function (client-side, next to the offline path) that builds
the full `Memory` from a stored draft + assigned id: tag normalization
(kernel `normalize_tags`), `Provenance::Asserted { CodingAgent }` (D8's
kind, same as the backend stamps), context stamps (session/project/user
— the same field set `end_session_offline` builds for its validation
probe), `LSN::new_local(entry.local_lsn)`. One implementation, used by
both F2 and F3; the drift between "what validation probed" and "what
the snapshot serves" is the bug class W2 killed at the wire, avoided
here by construction.

The materializer also resolves each stored draft's `edge_hints` into
`Relationship`s and runs kernel `validate_triple` on each before it is
served: the offline write path deliberately defers triple validation
to drain time (`mcp.rs`, the §4.5 comment), but standalone never
drains — without this, an edge with a bogus kind or target would sit
in the read surface forever, unvalidated. W2's one-rulebook
discipline, applied at the read surface.

### F2 — live write-back (standalone)

After `wal.append_batch_full` succeeds: ONE copy-on-write publish. Add
a real public cache method — `apply_local(org, memories, edges, lsn)`
— that clones the snapshot once, runs `insert_memory` +
`insert_relationship` on each materialized row, stamps
`last_local_lsn`, and stores. It REPLACES the `advance_local_lsn` call,
not wraps it: an insert-publish followed by an advance-publish would
clone the snapshot twice per write, and the LSN stamp still matters
for R-M7. Do not ship this through the test-named surface
(`push_test_memory`) or the `#[doc(hidden)]` benches-only `publish` —
standalone boot already leans on that hidden one (`main.rs:199`);
F3 moves boot onto the public API too. Failure of the publish
degrades to today's behavior (logged, never fails the ack); the WAL
remains the source of truth.

### F3 — boot seeding (standalone only)

When `--backend` is absent: build the startup snapshot from **all** WAL
entries (`from_storage` cannot be used — the WAL is not a `Storage`;
iterate entries, F1-materialize, insert). This replaces `synth_snapshot`
entirely (F4). When `--backend` is present: seed nothing (empty, as
today) — the drain commits rows server-side, SSE/reseed delivers them,
and WAL ids ≠ server ids, so seeding would duplicate. Mode switching is
therefore always clean: standalone rows only ever exist in a standalone
snapshot.

**All entries means all states** — `Pending`, `Synced`, and `Failed`:
standalone never delivers any of them server-side, so the WAL is their
only read path (AC6 pins this).

**Ordering requirement:** once F4 removes the synthetic seed, nothing
seeds the org graph at boot — and `apply_local`/`advance_local_lsn`
no-op when no graph is resident (`cache/src/lib.rs:779` guards on
`graphs.get(org)`). Standalone boot must publish an empty
`GraphSnapshot` for the org before the first write lands, or F2's
publish silently drops the batch. Empty-but-present, then F2
populates it (AC1 runs on a fresh `--data-dir` and so pins this order
end to end).

### F4 — remove the synthetic seed

`synth_snapshot()` and its call site die. Standalone starts honest:
empty until the first write, then real. Benches/tests that need
fixtures build their own (they already do — `push_test_memory`).
README's "standalone with synthetic seed data" phrasing updates.

### F5 — local grouping parity (D6)

Standalone materialization mints the same `Conversation` node +
`InSession` edges the backend's commit path would (deterministic ids
from `(org, flavor, key)`). **Reuse is blocked by the dep graph:**
`grouping_node`/`grouping_edge` live in `exocortex-ingest`, which
(a) the client does not depend on, (b) dev-depends on
`exocortex-client` — a src-dep would be a cycle — and (c) would
transitively drag `exocortex-dreams` into the personal-mode binary.
So: duplicate the two pure builders client-side (~60 lines of plain
kernel-type construction), and pin them with a parity test in
`client/tests/` that dev-deps `exocortex-ingest` and asserts identical
ids, titles, provenance, and edge properties for the same inputs —
W2's golden-table discipline, applied to builders instead of
validators. Reason: an agent that learns
"my writes group into conversations" in standalone must not lose that
when a backend appears; `find_related` over `InSession` edges is part
of the taught read pattern. `derived_confidence` is stamped (0.8 base
— no evidence events locally).

### Explicitly deferred

- **In-backend-mode offline buffering visibility**: if a backend is
  configured but unreachable, buffered rows stay invisible until drain.
  Correct scope for v1: rare state, self-healing at startup, and
  surfacing them would need settle-driven eviction (ids diverge).
  Recorded as open question OQ1, revisit with dogfood data.
- **Embedded FalkorDBLite backend** (the memory-graph architecture):
  the real v2 answer for personal-mode scale. Not this PRD: it adds a
  third `Storage` impl (the ST3-ST10 double-parity suite obligation),
  and the WAL already is a sufficient embedded store for personal
  volume (~15 writes/day at the prior deployment's heaviest).

## Acceptance

Each fails on current `main`:

1. **In-session read-back** — write offline, `search_memories(title
   term)` in the same process returns it (`standalone_readback.rs`;
   fresh `--data-dir`, so the test also pins F3's ordering rule: the
   empty org graph is published at boot, before the first write).
2. **Cross-restart read-back** — restart a second client over the same
   `--data-dir`; the write is still searchable; ids are byte-stable
   (the WAL-stored ids, asserted not regenerated).
3. **Edges readable** — `find_related(fix_id)` traverses the
   `Fixes` edge to the `Problem` written in the same batch; the
   `Conversation` node and `InSession` edges exist and match D6's shape
   (same node id derivation as the backend, asserted in test).
4. **No synthetic data** — a fresh `--data-dir` answers honestly empty
   (`search_memories` → `[]`), and no code path publishes synth rows.
5. **Backend mode unchanged** — `e2e_chain` (gRPC → SSE → sibling)
   green as-is; `backend_mode_search_returns_no_synthetic_memories`
   still passes.
6. **WAL drain unaffected** — `wal_drain_settles_pending_entries` and
   the roundtrip suite green; after a drain, standalone-mode restart
   still seeds — `Pending`, `Synced`, and `Failed` entries included:
   in standalone nothing else will ever deliver them.
7. **Cross-batch dangling edges** — an edge whose `to_memory_id`
   targets an id with no local row is NOT materialized
   (`insert_relationship` drops unknown endpoints silently — pin that
   behavior in a test before F5 makes grouping edges depend on it).

## Fix order and release

Land the pending rounds-4–5 / A-PRD working tree FIRST — every test
this PRD leans on (the drain suite, the parity gates, the current
`end_session_offline` shape) exists only uncommitted, and "fails on
current main" (above) only means something once that tree is in.

Then F1 → F2 → F3 → F4 → F5, one commit each, tests with each. Ships
as `exocortex-client 0.2.1` (no proto change, no fingerprint change,
no schema golden change; F2's cache API and F5's builders are
additive, workspace-internal). Master plan backlog row added; close
it there.
