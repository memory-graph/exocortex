# Bug PRD — codebase defect audit

| Bug PRD | | 70 confirmed defects across all 14 crates: seven write-path parity defects (W1–W7) plus 63 found by a full-codebase sweep |
|:---|:---|:---|
| **Author**: Gregory Dickson | **Status**: Open **Created**: 2026-08-26 **Rev**: 2 (scope expanded from write-path parity to the whole codebase) | **Visibility**: Internal |

## Summary

Rev 1 of this document recorded seven defects (W1–W7) found incidentally while
reviewing `agent-instructions-prd.md` against the code it describes. Those seven
were not the result of looking for bugs — they were what fell out of checking
whether one PRD's claims matched one write path. That hit rate justified looking
properly.

Rev 2 records the result of that sweep: **63 further confirmed defects**, across
every crate in the workspace. Seven candidate findings were refuted during
verification and are recorded in §7 so they are not re-found.

The workspace is 138 tests / 0 failed across 68 suites, with every gate in
`AGENTS.md` green. None of that is wrong. The defects live where the gates do
not look.

**The single structural finding, which subsumes most of the individual ones:**
every quality gate in this repository tests one crate in isolation, and nothing
tests a seam. Where two components must agree — the Falkor adapter and the
in-memory double, the online and offline write paths, the pack's declared rules
and the engine's implemented rules, MCP and HTTP for one registered operation,
the kernel's validator and ingest's copy — they were never asserted to agree,
and in every one of those five cases they now do not. Several security controls
are in the same category: written, tested in isolation, and wired to nothing.

## 0. How these were found, and how far to trust them

Fourteen agents in seven finder/verifier pairs, one pair per subsystem, each
finder given the spec's rule ids, the hard rules from `AGENTS.md`, and W1–W7 and
backlog R1/R2 as a do-not-report list. Every candidate was then handed to an
independent verifier prompted to **refute** it — open the cited line, trace
reachability, search for a covering test, and default to rejection when
uncertain.

72 candidates in, 65 confirmed, 7 refuted. Two confirmed findings were the same
defect found independently by two groups (the SSE feed and `RegisterSource`),
merged here, leaving **63**.

A 90% confirmation rate is high enough to be suspicious of the verifiers, so ten
of the most consequential findings were then checked by hand against the source
— all four security findings among them. **All ten held, at the exact cited
lines.** Several verifier notes narrowed a finding rather than confirming it
whole (KP1's live-derivation claim, WS2's persistence claim); those narrowings
are preserved inline as **Verifier note** and the finding text reflects the
narrower claim.

What this process does not give you: proof of absence. A clean subsystem here
means seven agents and one reviewer did not find a defect in it, not that none
exists. Treat §5's compact entries as leads with citations — each was verified
once, not hand-checked by a human.

## 1. Severity and reading guide

| Severity | Meaning |
|---|---|
| **data loss** | Committed or acknowledged data is destroyed or never arrives |
| **security** | An authentication, authorisation, or visibility control does not hold |
| **idempotency** | Replay or restart changes the outcome |
| **correctness** | Wrong results, violated spec invariants, or enforcement that cannot fire |
| **resource** | Unbounded growth or exhaustion with no backpressure |
| **layering** | An invariant is enforced in the wrong crate, so other consumers cannot see it |

IDs are stable and subsystem-prefixed: **W** write-path parity (rev 1),
**KP** kernel/pack, **WS** wire/signing, **ST** storage, **CR** cache/reasoning,
**CS** cluster/server, **IN** ingest/ops/dreams, **CL** client/SDK/worker.
W1–W7 keep their rev-1 numbering because `agent-instructions-prd.md` rev 3
references them.

## 2. The five patterns

Fixing these individually is possible. Fixing the patterns is cheaper, and each
pattern comes with a gate that would have caught its whole class.

**2.1 — The double and the adapter are different databases.** `InMemoryStorage`
and the FalkorDB adapter diverge on version history (ST6), what
`stream_all_*` emits (ST7), what an over-ceiling read returns (ST4), whether
`Visibility::Public` is readable at all (ST3), whether a relationship delete does
anything (ST8), invalidation on delete (ST9), and `get_state_at` semantics
(ST10). Every test that uses the double to prove backend behaviour proves
nothing about the backend. *Gate: one conformance suite, run twice — once
against the double, once against live Falkor — asserting identical results.*

**2.2 — Enforcement that cannot fire.** Peer admission has no caller (CS2). The
kernel's own validator has no non-test caller (W2). The Dreams write-counter
trigger can never fire (IN5). Rule D4 is computed and discarded (CR8); D6's
writeback is structurally incapable of writing (CR9). The backend re-election
lease fences nothing (CS4). `OpContext.deadline` has no reader (IN12).
`RelationshipId::derive`'s snapshot parameter is dead. Each of these is code
that reads as a control and is not one. *Gate: a dead-enforcement lint — any
`pub fn` in a security or invariant path with zero non-test callers fails the
build or carries an explicit `#[allow]` with a reason.*

**2.3 — Self-declared trust.** Producers declare the ceiling they are then
validated against (WS2), `RegisterSource` requires no authentication at all
(WS1), and the SSE change feed accepts any non-empty token string (CS1). In each
case a control exists, is tested, and validates the caller's own claim about
itself. *Gate: an auth-coverage test that enumerates every network-reachable
endpoint and asserts each one rejects an unauthenticated call.*

**2.4 — Two artifacts, one truth, never compared.** The pack declares six
Datalog rules; the engine implements five, one with a weakened body (KP1). The
pack's `crepe_rules!` text is captured by `stringify!` and discarded. MCP and
HTTP serve different implementations and different output shapes for the same
registered operation, against CR-9 (IN11). This is the same failure as W2's
three validators. *Gate: cross-artifact equivalence tests — pack rule ids vs.
engine outputs, MCP schema+result vs. HTTP schema+result.*

**2.5 — Errors that erase themselves.** Invalidation decode errors are dropped
silently on both consumers (CS7). `audit_range` substitutes the process-local
ledger for the durable one on storage error (IN13). `submit_stream` ends the ack
stream silently on any inbound error (IN14). `Wal::pending_count` skips
undecodable entries. W4 drops the backend's own rejection explanation. *Gate:
none needed — these are individually small fixes, but they compound: several
other findings here are hard to diagnose in production precisely because of
these.*

## 3. Defect index

| ID | Severity | Defect | Primary site |
|----|----------|--------|--------------|
| W1 | data loss | Offline WAL is never drained; buffered writes never reach the backend | `crates/exocortex-client/src/wal.rs:155` |
| W2 | correctness | Three divergent write-path validators | `crates/exocortex-kernel/src/validator.rs:19` |
| W3 | correctness | Online ingest drops `session_id`/`project_id`/`user_id` | `crates/exocortex-ingest/src/service.rs:303` |
| W4 | usability | `RejectRow.detail` discarded before the caller sees it | `crates/exocortex-client/src/tools/end_session.rs:73` |
| W5 | correctness | Relationship visibility hardcoded to `Project` | `crates/exocortex-client/src/tools/end_session.rs:150` |
| W6 | layering | Computed-only kind is a string literal in the ingest crate | `crates/exocortex-ingest/src/service.rs:33` |
| W7 | idempotency | Duplicate-batch dedup is in-memory only | `crates/exocortex-ingest/src/service.rs` |
| KP1 | correctness | Pack D1–D6 rule text is never compiled; the executing rules are a hand-copy in exocortex-reasoning that already disagrees (D1 guard dropped, D5 missing) | `crates/exocortex-pack-dev-v1/src/lib.rs:136` |
| KP2 | correctness | Ontology::from_packs canonicalizes kind ids across packs but leaves memory/entity type ids pack-local, so every type triple in the second pack is evaluated against the wrong ids | `crates/exocortex-kernel/src/ontology.rs:112` |
| KP3 | correctness | R-T5 title/summary bounds are measured in bytes, so a valid non-ASCII title is rejected and the write is dropped | `crates/exocortex-kernel/src/validator.rs:25` |
| KP4 | correctness | F01's [0,1] clamp is bypassed by its derived Deserialize, and Memory is deserialized straight out of FalkorDB props_json | `crates/exocortex-kernel/src/memory.rs:10` |
| KP5 | correctness | The kernel Action/Function surface is dead: REQUIRED_VISIBILITY_CEILING and the R-Lat1 latency budgets have no readers, and the SLO gate hardcodes its own copies of the numbers | `crates/exocortex-kernel/src/actions.rs:16` |
| WS1 | security | RegisterSource is unauthenticated while Submit requires HMAC; an unauthenticated caller can overwrite or LRU-evict every registered producer, and the poisoned registry is persisted to disk | `proto/ingest.proto:19` |
| WS2 | correctness | Producers self-declare the ceiling they are then checked against, so R-I3 ceiling equality and the SDK's CeilingMismatch check are dead enforcement | `proto/ingest.proto:23` |
| WS3 | correctness | ClusterNode::admit — the entire R-W2/R-W3/R-W4 peer-admission gate — has zero non-test callers; no inbound peer envelope path exists | `crates/exocortex-cluster/src/node.rs:209` |
| WS4 | correctness | proto3 float trap: RelationshipDraft.strength/confidence of NaN is persisted verbatim, and confidence == 0.0 is silently rewritten to 0.8 with no note in the schema | `crates/exocortex-ingest/src/service.rs:425` |
| WS5 | security | vis_from_i32 coerces every unknown or negative Visibility discriminant to PUBLIC, the widest value, instead of rejecting | `crates/exocortex-ingest/src/service.rs:196` |
| ST1 | correctness | FalkorDB soft-delete updates node properties but not props_json, so every reader sees a deleted memory as still live | `crates/exocortex-storage/src/cypher.rs:221` |
| ST2 | resource | stream_all_memories loops forever after any soft delete because the cursor advances from the stale props_json LSN | `crates/exocortex-storage/src/falkor.rs:732` |
| ST3 | correctness | Memories with Visibility::Public are permanently unreadable on FalkorDB (every read template caps at `<= Org`) | `crates/exocortex-storage/src/falkor.rs:546` |
| ST4 | correctness | get_memory_for silently returns Ok(None) for an existing over-ceiling row on FalkorDB where the double returns PermissionDenied | `crates/exocortex-storage/src/falkor.rs:565` |
| ST5 | idempotency | upsert_batch_fenced checks the lease once up front, then commits rows one round-trip at a time — a stale owner's rows still land | `crates/exocortex-storage/src/falkor.rs:895` |
| ST6 | data loss | upsert_batch is a non-transactional loop despite claiming MULTI/EXEC rollback, so a mid-batch failure leaves a partial commit | `crates/exocortex-storage/src/falkor.rs:483` |
| ST7 | correctness | FalkorDB keeps no memory version history (MERGE overwrites in place) while the double keeps a version stack, and the double's property test asserts history reads Falkor cannot serve | `crates/exocortex-storage/src/cypher.rs:46` |
| ST8 | correctness | The double's stream_all_memories / stream_all_relationships emit every historical version, Falkor emits one current row per id | `crates/exocortex-storage/src/in_memory.rs:322` |
| ST9 | correctness | soft_delete_relationship matches `:RELATES`, a relationship type the adapter never creates, so delete_relationship is a silent no-op that reports success | `crates/exocortex-storage/src/cypher.rs:231` |
| ST10 | idempotency | Redis lease acquire/release are non-atomic multi-command sequences: a crash between SET NX and EXPIRE leaves an immortal lease, and release can delete another node's lease | `crates/exocortex-storage/src/falkor.rs:815` |
| ST11 | correctness | InMemoryStorage::delete_relationship publishes no invalidation, so relationship deletions never reach the change feed on the double | `crates/exocortex-storage/src/in_memory.rs:217` |
| ST12 | correctness | InMemoryStorage::get_state_at counts an id if ANY historical version is valid and ignores visibility entirely, so snapshot counts disagree with FalkorDB | `crates/exocortex-storage/src/in_memory.rs:294` |
| CR1 | correctness | Cache apply(MemoryUpserted) adds a second node instead of replacing the existing one, so stale versions stay searchable forever | `crates/exocortex-cache/src/lib.rs:390` |
| CR2 | correctness | Reseed resurrects soft-deleted memories: from_storage applies no valid_until/invalidated_by filter | `crates/exocortex-cache/src/lib.rs:239` |
| CR3 | correctness | search_offsets is indexed by insertion order but read as a NodeIndex; StableGraph reuses freed indices, so search returns the wrong memory | `crates/exocortex-cache/src/lib.rs:571` |
| CR4 | correctness | traverse ignores TraversalSpec.direction: Direction::In always returns empty and Direction::Both follows only outgoing edges | `crates/exocortex-cache/src/lib.rs:517` |
| CR5 | resource | insert_relationship adds a parallel duplicate edge on every re-upsert of the same RelationshipId | `crates/exocortex-cache/src/lib.rs:185` |
| CR6 | correctness | Derived edges from R7, R8 and R9 collide on one RelationshipId, so later rules silently overwrite earlier ones' provenance and confidence | `crates/exocortex-reasoning/src/engine.rs:174` |
| CR7 | correctness | derived_confidence feeds the neighborhood edge count where §14.2 requires the shared entity/tag count | `crates/exocortex-reasoning/src/engine.rs:313` |
| CR8 | correctness | Rule D4 (contradiction_propagates) is evaluated on every fixpoint and then discarded — no writeback, no reader | `crates/exocortex-reasoning/src/engine.rs:231` |
| CR9 | correctness | D6 writeback is structurally incapable of writing anything: the derived id is byte-identical to the edge the rule consumed | `crates/exocortex-reasoning/src/engine.rs:230` |
| CR10 | correctness | Search scoring counts Computed and Extracted edges as explicit relationships, so Dreams SimilarTo edges inflate rank at 0.30 each | `crates/exocortex-cache/src/lib.rs:588` |
| CR11 | resource | R7/R9 attribute facts are harvested from every memory in storage, bypassing the CR-6 neighborhood cap and producing a quadratic derived-edge blowup | `crates/exocortex-reasoning/src/engine.rs:143` |
| CR12 | resource | Every derived edge persists the entire k-hop neighborhood's edge list as its provenance evidence | `crates/exocortex-reasoning/src/engine.rs:183` |
| CR13 | resource | 2Q byte accounting is never updated by the invalidation path and eviction is unreachable for a resident org, so the cache budget is unenforced | `crates/exocortex-cache/src/lib.rs:417` |
| CS1 | data loss | Empty replay ring answers "you are current" instead of 409 Resync, silently dropping every invalidation between the client's since_lsn and the node's LSN | `crates/exocortex-cluster/src/node.rs:102` |
| CS2 | security | Peer admission is dead code: ClusterNode::run signs unauthenticated Redis pub-sub payloads with the cluster HMAC and serves them to SSE clients as authentic | `crates/exocortex-cluster/src/node.rs:157` |
| CS3 | security | /v1/changes is mounted outside the bearer-auth layer and accepts any non-empty ?token= value, so the change feed is unauthenticated | `crates/exocortex-server/src/sse.rs:73` |
| CS4 | correctness | The backend-node re-election lease is on a key no fenced write ever uses, so the elected "leader" fences nothing and Dreams runs on every node | `crates/exocortex-server/src/backend.rs:52` |
| CS5 | resource | mcp-standalone never supervises the FalkorDB child: no restart on crash, no kill on exit, and the port it picks is published nowhere | `crates/exocortex-server/src/supervisor.rs:73` |
| CS6 | correctness | backend_lsn and sync_lsn are never written on backend-node, so /health/sync always reports lag 0 | `crates/exocortex-server/src/http_bind.rs:179` |
| CS7 | correctness | Both consumers of the storage invalidation stream discard decode errors silently, permanently losing the change with no metric or log | `crates/exocortex-cluster/src/node.rs:160` |
| CS8 | correctness | replay_since overflows on since_lsn = u64::MAX, panicking the SSE handler in debug builds | `crates/exocortex-cluster/src/node.rs:106` |
| IN1 | correctness | Consolidation ignores RegionKey.org and .project, so a region-scoped lease authorises graph-wide merges, strengthens and prunes | `crates/exocortex-dreams/src/lib.rs:314` |
| IN2 | security | promote_visibility reads the target with the unscoped get_memory, so any caller can widen a memory they are not permitted to see | `crates/exocortex-ops/src/operations.rs:323` |
| IN3 | correctness | accept_discovery ignores the caller-supplied kind and creates every edge as RelKindId(0) with no ontology or triple validation | `crates/exocortex-ops/src/operations.rs:427` |
| IN4 | correctness | The Dreams write-counter trigger can never fire: seconds_since_last_cycle is never set and on_write has no non-test callers | `crates/exocortex-dreams/src/trigger.rs:43` |
| IN5 | correctness | A merge that leaves fewer than two anchors aborts the cycle after the merge committed: no audit record, no regression check, no rollback | `crates/exocortex-dreams/src/lib.rs:284` |
| IN6 | correctness | Ingest entity ids are always derived from the literal org string "org" — with_org does not rebuild the extractor | `crates/exocortex-ingest/src/service.rs:99` |
| IN7 | idempotency | The client mints a fresh batch_id and fresh memory ids on every end_session attempt, so the server's idempotency registry can never match a retry | `crates/exocortex-client/src/tools/end_session.rs:161` |
| IN8 | correctness | Dreams strengthen re-applies the §14.3 decay to the already-decayed stored strength, so repeated cycles monotonically weaken every edge | `crates/exocortex-dreams/src/lib.rs:413` |
| IN9 | correctness | Entity extraction keeps only the first match of each pattern, silently dropping every other entity of that type in the text | `crates/exocortex-ingest/src/entities.rs:135` |
| IN10 | correctness | MCP and HTTP serve different implementations and different output shapes for the same registered operations | `crates/exocortex-client/src/mcp.rs:176` |
| IN11 | correctness | OpContext.deadline is a startup constant that no handler reads, and OpError::DeadlineExceeded is never constructed | `crates/exocortex-ops/src/lib.rs:31` |
| IN12 | correctness | audit_range silently substitutes the process-local ledger for the durable one on storage error or on any empty range | `crates/exocortex-ops/src/audit.rs:114` |
| IN13 | correctness | submit_stream drops rows that carry no body and ends the ack stream silently on any inbound error, so acks need not match submissions | `crates/exocortex-ingest/src/service.rs:658` |
| CL1 | correctness | Offline end_session silently discards the harness-supplied tags; the online path keeps them | `crates/exocortex-client/src/mcp.rs:304` |
| CL2 | data loss | batch_id is content-independent while the server dedups on (producer_id, batch_id) alone, so a re-partitioned or seed-reused window silently drops rows and still advances the cursor | `crates/exocortex-adapter-sdk/src/split.rs:97` |
| CL3 | security | MCP client panics on a short --hmac-key and silently substitutes an all-zero key on a malformed one | `crates/exocortex-client/src/main.rs:46` |
| CL4 | security | --auth-token is parsed and never used anywhere, so the client talks to the backend unauthenticated | `crates/exocortex-client/src/main.rs:28` |
| CL5 | correctness | The client always publishes a fabricated synthetic snapshot, so search/get/find_related return invented memories even when --backend is configured | `crates/exocortex-client/src/main.rs:135` |
| CL6 | correctness | The R-M7 read stamp's local_lsn is hardwired to 0 — nothing ever advances the cache's WAL frontier, so offline writes are invisible to read-your-writes | `crates/exocortex-client/src/mcp.rs:167` |
| CL7 | correctness | search_memories reports memory_type as the fake string "type:<u8>" while get_memory reports the raw u8 for the same field | `crates/exocortex-client/src/mcp.rs:152` |
## 4. Critical findings

Sixteen defects that either break a security control, destroy data, or make a
subsystem structurally unable to do its job. Full treatment; the remaining 47
are in §5 with the same citations in compact form.

The seven write-path defects from rev 1 (W1–W7) remain in §6 unchanged — W1
(the WAL that is never drained) belongs in this tier on severity and is kept
with its cohort so the agent PRD's references stay stable.

### CS3 — /v1/changes is mounted outside the bearer-auth layer and accepts any non-empty ?token= value, so the change feed is unauthenticated

**Severity:** security. **Invariant:** R-Sec7.

**Site:** `crates/exocortex-server/src/sse.rs:73` (also: crates/exocortex-server/src/http_bind.rs:99, crates/exocortex-server/src/http_bind.rs:202, crates/exocortex-server/src/backend.rs:256, crates/exocortex-server/tests/sse_replay.rs:138)

**Current behaviour.** `HttpBind::router` applies the bearer middleware to the `ops` router only (http_bind.rs:99-101) and then merges the SSE router into the result afterwards (http_bind.rs:201-204), so `/v1/changes` never passes through `auth()`. Inside the handler, `SseAuth::RequiredToken` only checks `token.is_none()` (sse.rs:73-75); the token is never compared against anything — it is fed straight into `derive_client_sse_key` (sse.rs:96), which accepts arbitrary input. `backend.rs:256` wires exactly this combination for backend-node.

**Failure scenario.** An unauthenticated caller with network reach to `--bind` issues `GET /v1/changes?token=x&since_lsn=0`. `token` is `Some("x")`, the presence check passes, the request never sees the bearer middleware, and the caller receives the full org-wide stream of memory/relationship upserts and deletes (ids and backend LSNs) indefinitely. Even `?token=` (empty value) works: `split_once('=')` yields `("token", "")`, which is `Some("")`, not `None`.

**Fix.** Put the SSE route behind the same bearer middleware (merge it into `ops` before the `.layer(...)` call), and/or validate the per-client token against a provisioned set rather than treating presence as proof.

**Verification.** A test asserting `GET /v1/changes?token=anything` without an `Authorization: Bearer <token>` header returns 401 on a backend-node router. The current test at crates/exocortex-server/tests/sse_replay.rs:137-138 pins the defect: it asserts 200 for the literal token `"t"`, which was never provisioned.

---

### WS1 — RegisterSource is unauthenticated while Submit requires HMAC; an unauthenticated caller can overwrite or LRU-evict every registered producer, and the poisoned registry is persisted to disk

**Severity:** security. **Invariant:** R-I3.

**Site:** `proto/ingest.proto:19` (also: crates/exocortex-ingest/src/service.rs:704, crates/exocortex-ingest/src/service.rs:705, crates/exocortex-ingest/src/service.rs:29, crates/exocortex-ingest/src/service.rs:541)

**Current behaviour.** `RegisterSourceRequest` (proto/ingest.proto:19-25) carries no `ProducerIdentity` and no `hmac_signature` — unlike `IngestBatch`, which does (ingest.proto:43, :93). The handler at service.rs:691-710 checks only `org_guard` and then does an unconditional `sources.put((org_id, source_uri, producer_id), ceiling)` (service.rs:704) followed by `persist_sources` (service.rs:705). `sources` is a 1000-entry `lru::LruCache` (service.rs:29, :105-107). `Submit` gates on HMAC (service.rs:180) but the registry that feeds it does not.

**Failure scenario.** An attacker who can reach the gRPC port but holds no HMAC key issues 1000 `RegisterSource` calls with distinct `producer_id` values. The LRU evicts every legitimate registration. Every subsequent signed `Submit` from a real producer hits `let Some(ceiling) = registered_ceiling else` (service.rs:541) and is rejected with `UNKNOWN_SOURCE`/"producer not registered" — a total, silent write outage where each batch is acked as fully rejected rather than erroring. `persist_sources` writes the evicted state to `sources_file`, so a restart reloads the poisoned registry. A narrower variant: one call re-registering an existing (org, source_uri, producer_id) with a different ceiling makes the victim's next batch fail the R-I3 equality check at service.rs:548.

**Fix.** Put the same producer identity + HMAC on `RegisterSourceRequest` that `IngestBatch` carries and verify it before mutating the registry; reject a re-registration that changes an existing ceiling instead of overwriting it. An unauthenticated RPC must not be able to mutate the state an authenticated RPC depends on.

**Verification.** Test: register producer P with ceiling ORG, submit successfully, then issue 1000 unsigned RegisterSource calls with fresh producer_ids, then re-submit P's batch. It is rejected UNKNOWN_SOURCE today; it must still succeed.

**Verifier note.** The 'restart reloads the poisoned registry' half is not reachable in any shipped binary: with_sources_file (service.rs:143) is called only from crates/exocortex-ingest/tests/ingest.rs:305; backend.rs never sets sources_file, so persist_sources returns early (service.rs:161-163). The eviction/overwrite half stands on its own.

---

### IN2 — promote_visibility reads the target with the unscoped get_memory, so any caller can widen a memory they are not permitted to see

**Severity:** security. **Invariant:** R-MT4 / CR-22 / R-T11a.

**Site:** `crates/exocortex-ops/src/operations.rs:323` (also: crates/exocortex-ops/src/operations.rs:182, crates/exocortex-storage/src/trait_.rs:69, crates/exocortex-server/src/backend.rs:212)

**Current behaviour.** `PromoteVisibilityOp::handle` loads the target with `ctx.storage.get_memory(&id)` (operations.rs:321-326), whose trait doc (trait_.rs:68-69) says it reads at the historical Org ceiling and is "kept for internal paths that re-check visibility above the seam". This handler performs no such re-check: it never compares `m.visibility` or the row's author against `ctx.visibility_ctx`. The read sibling `GetMemory` in the same file uses `get_memory_for(&id, &ctx.visibility_ctx)` (operations.rs:182) precisely so an invisible row yields PermissionDenied.

**Failure scenario.** Alice authors a Private memory. Bob (or, over HTTP, anyone holding the bearer token, since the shared OpContext is built with user "backend" at backend.rs:212-216) POSTs `/v1/promote_visibility {memory_id: <alice's id>, to: "org"}`. The handler loads the row through the Org-ceiling read, sees `Org >= Private`, sets visibility to Org and commits. A row the caller could not even read through `get_memory` is now published to the whole org, and the audit record credits actor "backend".

**Fix.** Load the target through `get_memory_for(&id, &ctx.visibility_ctx)` and map PermissionDenied to `OpError::Unauthorized`, matching GetMemory; only then apply the widening check.

**Verification.** A test mirroring `get_memory_surfaces_permission_denied_not_silent_none` (parity.rs:268) but calling PromoteVisibilityOp as a non-author against another user's Private memory, asserting `OpError::Unauthorized` and that the stored visibility is unchanged. No current test exercises promote_visibility with a caller who lacks visibility.

**Verifier note.** Severity framing needs one qualifier: in the shipped backend every HTTP caller shares one principal — ops_vc(&org, "backend", Visibility::Org) at crates/exocortex-server/src/backend.rs:212-216 — so 'Bob widens Alice's memory' is really 'any bearer-token holder widens any author's Private memory'. The privilege differential between GetMemory and PromoteVisibility is real either way, and embedded/registry callers that pass a real VisibilityContext get the full described bypass.

---

### CL3 — MCP client panics on a short --hmac-key and silently substitutes an all-zero key on a malformed one

**Severity:** security.

**Site:** `crates/exocortex-client/src/main.rs:46` (also: crates/exocortex-client/src/main.rs:159, crates/exocortex-worker/src/main.rs:80)

**Current behaviour.** `decode_hex32` (main.rs:44-49) slices `&hex[i*2..i*2+2]` for i in 0..32 without checking the length, so any argument shorter than 64 chars panics with an out-of-range byte index (and any multi-byte character panics on a char boundary). Where it does return `Err`, the call site at main.rs:157-160 does `.and_then(|hex| decode_hex32(hex).ok()).unwrap_or([0u8; 32])` — a wrong-but-hex key becomes the all-zero key with no warning. The worker's own `decode_hex32` (worker/src/main.rs:79-90) checks the length and its call site hard-errors, with a comment saying exactly why: 'silently falling back to a known key would ship a credential bug'. The two copies of the same function disagree.

**Failure scenario.** `exocortex-mcp-client --backend http://host:50051 --hmac-key deadbeef` aborts the process with an index-out-of-range panic before serving stdio. `--hmac-key <64 chars with one typo'd non-hex char>` starts fine, signs every wrapup batch with `[0u8;32]`, and every `end_session` comes back rejected with `BadChecksum` — or is accepted unauthenticated if the backend also runs a zero default key.

**Fix.** Delete the client's copy and share the worker's hardened `decode_hex32` (length check + hard error), and make an unparsable `--hmac-key` fail startup rather than degrade to a zero key.

**Verification.** `decode_hex32("abc")` returns `Err` instead of panicking, and a test that starts the binary with a malformed `--hmac-key` asserts a non-zero exit with a diagnostic rather than a serving process.

---

### CS2 — Peer admission is dead code: ClusterNode::run signs unauthenticated Redis pub-sub payloads with the cluster HMAC and serves them to SSE clients as authentic

**Severity:** security. **Invariant:** R-W2/R-W3/R-Sec4 (§9.1 peer admission).

**Site:** `crates/exocortex-cluster/src/node.rs:157` (also: crates/exocortex-cluster/src/node.rs:209, crates/exocortex-storage/src/falkor.rs:930, crates/exocortex-cluster/tests/cross_node.rs:121)

**Current behaviour.** `run()` reads `storage.subscribe_invalidations()` — for FalkorStorage a plain Redis SUBSCRIBE whose payload is unauthenticated JSON (falkor.rs:930-953) — and passes each item straight to `self.envelope(inv)` (node.rs:161), which stamps the LOCAL wire version, the LOCAL ontology fingerprint and the LOCAL `node_id`, then HMACs it with the cluster key (node.rs:173-187). `ClusterNode::admit` (node.rs:209-218), the function that checks wire version, fingerprint and HMAC, has zero non-test callers anywhere in the workspace (only crates/exocortex-cluster/tests/cluster.rs and cross_node.rs call it). There is no inbound peer-envelope path at all.

**Failure scenario.** Anything that can reach the FalkorDB Redis instance runs `PUBLISH <channel> '{"MemoryDeleted":{"id":"<victim>","lsn":99999}}'`. Every backend node's `run()` loop picks it up, signs it with the cluster HMAC, and fans it out over `/v1/changes`. Every subscribed client's `decode_envelope` verifies the signature successfully and evicts/deletes the memory from its cache. A forged invalidation is laundered into an authentic one by the very node whose job §9.1 says is to admit peers. The cross_node.rs:119-122 assertion `"peer admission verifies A's signature"` is false — node B re-signed A's invalidation itself, so `admit` is checking B's own signature.

**Fix.** Either make the peer transport carry signed envelopes and run every inbound envelope through `admit()` before `publish_envelope`, or acknowledge the storage channel as the trust boundary and authenticate it there. As written the HMAC proves only "some node in this cluster read this off Redis", which is not what R-Sec4 claims.

**Verification.** An integration test that publishes a raw JSON invalidation directly onto the storage pub-sub channel (bypassing any node) and asserts that no SSE subscriber receives a validly-signed envelope for it.

**Verifier note.** falkor.rs subscribe_invalidations spans 921-955 (not 930-953); the deserialize is at 944-947

---

### WS2 — Producers self-declare the ceiling they are then checked against, so R-I3 ceiling equality and the SDK's CeilingMismatch check are dead enforcement

**Severity:** correctness. **Invariant:** R-I3/R-T11a.

**Site:** `proto/ingest.proto:23` (also: crates/exocortex-ingest/src/service.rs:701, crates/exocortex-ingest/src/service.rs:548, crates/exocortex-adapter-sdk/src/lib.rs:257, crates/exocortex-adapter-sdk/src/lib.rs:263, crates/exocortex-client/src/tools/end_session.rs:190)

**Current behaviour.** proto/ingest.proto:23 states the ceiling is "org-admin-configured, not adapter-configured (§18.2)". In practice the producer sends its own ceiling in `RegisterSourceRequest` (adapter-sdk lib.rs:257 sends `config.ceiling`; end_session.rs:190 sends a hardcoded `3`), the server stores whatever arrived (service.rs:701-704) and echoes it back verbatim (service.rs:707-709). The SDK then asserts `registered != config.ceiling` (lib.rs:263) — comparing the value it just sent against the identical value echoed back. The server's R-I3 check at service.rs:548 compares `batch.ceiling` against that same self-supplied value.

**Failure scenario.** An adapter configured with `ceiling: 4` (PUBLIC) calls `connect_with`. RegisterSource stores PUBLIC, echoes PUBLIC, `SdkError::CeilingMismatch` cannot fire. Every batch then carries `ceiling: 4`, matches at service.rs:548, and `validate_memory`'s `vis.within(ceiling)` (service.rs:220) accepts memories at any visibility up to PUBLIC. The adapter has granted itself the widest tenancy scope the system has; nothing in the write path can refuse it. `RejectCode::VISIBILITY_WIDENING` is unreachable for any self-registering producer.

**Fix.** The ceiling must come from admin-side configuration keyed by (org_id, source_uri, producer_id) that the producer cannot write; `RegisterSource` should return the configured ceiling and reject a request whose proposed ceiling exceeds it, rather than storing the proposal. Then `SdkError::CeilingMismatch` and R-T11a become live checks.

**Verification.** Test: pre-seed the registry with ceiling PROJECT for producer P out-of-band, then have the SDK connect with `config.ceiling = ORG`. `connect_with` must return `CeilingMismatch`. Today the pre-seeded value is silently overwritten with ORG and connect succeeds.

---

### ST1 — FalkorDB soft-delete updates node properties but not props_json, so every reader sees a deleted memory as still live

**Severity:** correctness.

**Site:** `crates/exocortex-storage/src/cypher.rs:221` (also: crates/exocortex-storage/src/falkor.rs:502, crates/exocortex-storage/src/falkor.rs:145, crates/exocortex-storage/src/in_memory.rs:185, crates/exocortex-storage/tests/in_memory_props.rs:188)

**Current behaviour.** `soft_delete_memory` does `SET m.valid_until = $now, m.lsn = $lsn` on the node only. Every Falkor read path reconstructs the `Memory` exclusively from the `props_json` node property (`memory_from_value`, falkor.rs:145), which still carries the pre-delete `valid_until: null`. The InMemory double, by contrast, mutates the row itself (in_memory.rs:185), so `get_memory` there returns `valid_until: Some(..)`.

**Failure scenario.** upsert memory M into FalkorDB, then `delete_memory(&M.id)`. `get_memory(&M.id)` returns `Some(m)` with `m.valid_until == None` — the memory reads back as live and undeleted. Cache reseed (`GraphSnapshot::from_storage`, exocortex-cache/src/lib.rs:238) reinserts it as a live node. `valid_at` disagrees with `get_memory` for the same row (the `valid_at` template filters on the node property and excludes it).

**Fix.** Have `soft_delete_memory` rewrite `props_json` too (read-modify-write, or make the adapter fetch the row, patch `valid_until`/`lsn`, and re-`SET props_json`), so the serialized row and the node properties can never disagree.

**Verification.** A cross-backend parity test asserting `after_delete.valid_until.is_some()` for both `InMemoryStorage` and `FalkorStorage`. Today that assertion exists only against the double (tests/in_memory_props.rs:188) and passes; the identical assertion against Falkor fails.

**Verifier note.** cypher.rs:221 is `SET m.valid_until = $now, m.lsn = $lsn` — cited line is correct. falkor.rs:502 is the run_template("soft_delete_memory") call — correct.

---

### ST2 — stream_all_memories loops forever after any soft delete because the cursor advances from the stale props_json LSN

**Severity:** resource.

**Site:** `crates/exocortex-storage/src/falkor.rs:732` (also: crates/exocortex-storage/src/cypher.rs:175, crates/exocortex-storage/src/cypher.rs:221)

**Current behaviour.** The pager selects on the node property (`MATCH (m:Memory) WHERE m.lsn > $after_lsn`, cypher.rs:175) but advances the cursor from the LSN parsed out of `props_json` (`cursor = cursor.max(m.lsn.value)`). `soft_delete_memory` bumps `m.lsn` without touching `props_json`, so for a deleted row the node LSN is permanently greater than the props LSN.

**Failure scenario.** Upsert memory M (lsn 1), then `delete_memory(&M.id)` (node lsn 2, props lsn still 1). Any later `stream_all_memories()`: page 1 with after_lsn=0 returns M, cursor becomes 1; page 2 with after_lsn=1 returns M again (node lsn 2 > 1), cursor stays 1; the loop never sees `rows.is_empty()` and pushes M into `all` forever. The call never returns and the process OOMs. Both consumers hit this: cache reseed (exocortex-cache/src/lib.rs:238) and every Dreams pass (exocortex-dreams/src/lib.rs:312, 345, 370, 522).

**Fix.** Advance the cursor from the value the query filtered on — return `m.lsn AS lsn` alongside the node and use that — and/or keep `props_json` in sync on soft delete. Also add a defensive guard that breaks when a page fails to advance the cursor.

**Verification.** Integration test: upsert 3 memories, delete one, then drain `stream_all_memories()` under a timeout and assert it terminates with 3 rows. Today it hangs.

**Verifier note.** Add the precondition: the loop is infinite only while no memory has a props_json lsn >= the deleted node's lsn (true immediately after a delete; a later upsert unsticks the cursor).

---

### ST6 — upsert_batch is a non-transactional loop despite claiming MULTI/EXEC rollback, so a mid-batch failure leaves a partial commit

**Severity:** data loss. **Invariant:** R-T17.

**Site:** `crates/exocortex-storage/src/falkor.rs:483` (also: crates/exocortex-storage/src/in_memory.rs:166, crates/exocortex-storage/src/trait_.rs:52)

**Current behaviour.** The comment says "Single FalkorDB transaction: MULTI/EXEC over the FalkorDB Redis connection. On any per-row failure, the whole batch rolls back", but the body is `for m in ms { out.push(self.upsert_memory(m).await?) }` followed by the same loop over relationships — each an independent query with its own LSN, and `?` aborts leaving earlier rows committed. The double has the identical non-atomic loop. The trait contract (trait_.rs:52) and the PRD ("Persist accepted rows in one transactional batch — atomic per batch", PRD line 5199) both require all-or-nothing.

**Failure scenario.** A batch of 3 memories and 2 relationships where the second relationship names a `RelKindId` absent from the ontology (`kind_label` errors, falkor.rs:407) — or, more realistically, the Falkor connection drops after the third row. `upsert_batch` returns `Err`, ingest acks the batch as rejected, and the producer replays it; meanwhile rows 1-3 are already durable and have consumed LSNs, so the change feed has published upserts for a batch the caller was told did not happen.

**Fix.** Wrap the batch in a real transaction (MULTI/EXEC or a single multi-statement Cypher query built from the registered templates) so a per-row failure rolls back, and mirror the same all-or-nothing behaviour in the double.

**Verification.** Test that submits a batch whose last row is guaranteed to fail and asserts none of the earlier rows are readable afterwards. Today the earlier rows are readable on both backends.

**Verifier note.** Trigger should be stated as a mid-batch backend/connection failure; the unknown-RelKindId path is blocked upstream by ontology resolution in ingest.

---

### ST5 — upsert_batch_fenced checks the lease once up front, then commits rows one round-trip at a time — a stale owner's rows still land

**Severity:** idempotency. **Invariant:** R-C3.

**Site:** `crates/exocortex-storage/src/falkor.rs:895` (also: crates/exocortex-storage/src/falkor.rs:1004, crates/exocortex-storage/src/falkor.rs:478, crates/exocortex-storage/src/trait_.rs:137, crates/exocortex-dreams/src/lib.rs:193)

**Current behaviour.** `upsert_batch_fenced` does one Redis `GET` of the lease key (`check_lease_current`, falkor.rs:1004) and then calls `upsert_batch`, which is a sequential loop issuing an independent Redis INCR plus a Falkor query per row (falkor.rs:485-492). Nothing re-checks the lease during the loop, and the fencing token is never written into the graph, so the epoch check is not atomic with any of the writes. The doc comment on the trait (trait_.rs:134-138) claims a stale lease "rejects ... before any row lands".

**Failure scenario.** Dreams acquires a 60s lease and never renews it (exocortex-dreams/src/lib.rs:193-197). `strengthen_edges` streams every relationship and issues one `upsert_batch_fenced` with N updates (exocortex-dreams/src/lib.rs:427). The pre-flight check passes at t=0; the batch is still writing at t=61s when the Redis key expires. Node B acquires the lease at t=62s, bumps the epoch, and rewrites relationship X. The old owner, still looping, writes its stale copy of X at t=65s and silently clobbers the new owner's row — exactly the R-C3 fencing violation, with no error surfaced to either node.

**Fix.** Make the fence part of the commit: stamp the fencing epoch on the rows and reject server-side when it is below the current epoch, or re-verify the lease atomically per commit unit (Lua CAS on the lease key gating an LSN allocation), and bound batch duration against the remaining lease TTL.

**Verification.** A test that acquires a short-TTL lease, starts a fenced batch, expires/re-acquires the lease mid-batch, and asserts no row from the stale owner is present afterwards. No such test exists (tests/fencing.rs and tests/fencing_live.rs only check a lease that is already stale before the call).

---

### CR1 — Cache apply(MemoryUpserted) adds a second node instead of replacing the existing one, so stale versions stay searchable forever

**Severity:** correctness.

**Site:** `crates/exocortex-cache/src/lib.rs:390` (also: crates/exocortex-cache/src/lib.rs:130, crates/exocortex-cache/src/lib.rs:483, crates/exocortex-dreams/src/lib.rs:382, crates/exocortex-server/src/backend.rs:117)

**Current behaviour.** `apply` fetches the new row and calls `next.insert_memory(m)` (lib.rs:390). `insert_memory` (lib.rs:130-149) never looks up `by_id` and never removes a prior node: it unconditionally pushes a new arena key, calls `petgraph.add_node`, and then overwrites `by_id[id]` with the new NodeIndex. The previous node keeps living in `petgraph` with the old weight, the old arena key, and the old bitmap entries, but is no longer reachable from `by_id`.

**Failure scenario.** Dreams `merge` closes a duplicate memory by re-upserting the SAME id with `valid_until`/`invalidated_by` set (crates/exocortex-dreams/src/lib.rs:379-384). Storage emits `Invalidation::MemoryUpserted{id}`; the backend change-feed bridge (crates/exocortex-server/src/backend.rs:104-127) submits it; `apply` adds a second node. The pre-merge copy is now unreachable from `by_id` but still a node weight, so `search` (which scans the arena → node_weight, lib.rs:563-600) and `view` (lib.rs:221-225) still return it. Same shape for a visibility narrowing: a memory edited from `Org` to `Private` leaves the old `Org` node resident, and any org member searching still gets the pre-narrowing title/tags — a visibility regression that `get_memory` (lib.rs:483, by_id) does not exhibit. Worse, a later `MemoryDeleted` calls `remove_memory` (lib.rs:151), which removes only the `by_id` node — every orphan copy survives the delete permanently.

**Fix.** In `insert_memory`, if `by_id` already contains `m.id`, remove the existing node (and its arena key / bitmap entries) before adding the new one — i.e. make it an upsert, not an append.

**Verification.** Reseed one memory, then apply `MemoryUpserted` for the SAME id with a narrowed visibility (or with valid_until set), then assert `cache.search(...)` returns exactly one hit and that hit is the new version. Today it returns two, one of them the stale wider-visibility row. No existing test re-upserts an existing id (tests/cache.rs:110 uses a fresh id).

---

### CR2 — Reseed resurrects soft-deleted memories: from_storage applies no valid_until/invalidated_by filter

**Severity:** correctness.

**Site:** `crates/exocortex-cache/src/lib.rs:239` (also: crates/exocortex-storage/src/cypher.rs:175, crates/exocortex-storage/src/falkor.rs:495, crates/exocortex-server/src/backend.rs:86)

**Current behaviour.** `GraphSnapshot::from_storage` inserts every row `stream_all_memories` yields, with no check on `valid_until`, `invalidated_by`, or duplicate ids (lib.rs:238-241). Nothing on the cache read path (`get_memory` 480, `search` 537, `traverse` 491, `visible` 209) filters closed rows either.

**Failure scenario.** Deletes in FalkorDB are soft: `delete_memory` sets `valid_until = now()` and bumps the lsn (crates/exocortex-storage/src/falkor.rs:495-509), and the `stream_memories` template is `MATCH (m:Memory) WHERE m.lsn > $after_lsn` with no validity predicate (crates/exocortex-storage/src/cypher.rs:174-177). Sequence: user deletes memory M → live node drops it via `Invalidation::MemoryDeleted` → operator restarts the backend node → `reseed_from_storage` at crates/exocortex-server/src/backend.rs:86 streams M back (it still has a row, now with a HIGHER lsn) → M is a node again and `search`/`get_memory`/`find_related` return it. The delete does not survive a restart. Dreams `merge` (dreams/src/lib.rs:379-384) closes rows the same way, so merged-away duplicates also come back on reseed.

**Fix.** Filter in `from_storage`: skip rows with `valid_until <= now()` or `invalidated_by.is_some()`, and keep only the highest-lsn row per `MemoryId`. Equivalently, add the validity predicate to the `stream_memories`/`stream_relationships` templates.

**Verification.** Upsert a memory, `delete_memory` it, then `GraphSnapshot::from_storage(&storage)` and assert the id is absent from `by_id` and from `search`. Today it is present.

---

### CR3 — search_offsets is indexed by insertion order but read as a NodeIndex; StableGraph reuses freed indices, so search returns the wrong memory

**Severity:** correctness.

**Site:** `crates/exocortex-cache/src/lib.rs:571` (also: crates/exocortex-cache/src/lib.rs:143, crates/exocortex-cache/src/lib.rs:167, crates/exocortex-cache/src/lib.rs:177)

**Current behaviour.** `insert_memory` appends one offset per insert (`search_offsets.push(...)`, lib.rs:143) and separately calls `petgraph.add_node` (lib.rs:146). `search` maps an arena hit position back to a node by `offsets.binary_search(pos)` → `NodeIndex::new(idx)` (lib.rs:565-571), i.e. it assumes offsets index == node index. `remove_memory` makes the same assumption in reverse when it blanks the key range (lib.rs:167-172). But `remove_memory` calls `petgraph.remove_node` (lib.rs:177) without removing the offsets slot, and `StableGraph::add_node` reuses freed indices from its free list (verified in petgraph-0.7.1 src/graph_impl/stable_graph/mod.rs:264-273).

**Failure scenario.** Insert A,B,C,D (offsets 0..3, nodes 0..3). Apply `MemoryDeleted{D}` → node 3 freed, offsets still length 4. Apply `MemoryUpserted{E}` → offsets slot 4, node index 3 (reused). Apply `MemoryUpserted{F}` → offsets slot 5, node index 4. Now `search("<E's title>")` finds E's key at `offsets[4]`, resolves to `NodeIndex::new(4)` = **F**, and returns F — a memory whose title and tags do not contain the query — while F's own key resolves to `NodeIndex::new(5)`, which is vacant, so F is unfindable by its own title. The same off-by-N makes `remove_memory` blank an unrelated memory's arena key, silently deleting it from the search index while it stays in the graph.

**Fix.** Key the arena by NodeIndex, not by insertion order: store offsets in a `DashMap<NodeIndex, (u32,u32)>` (or a Vec indexed by `ix.index()` that is resized/updated on insert), and use `ix.index()` on both the write and the blanking path.

**Verification.** Build a snapshot of 4 memories, delete the last, insert two more through `apply`, then assert `search(new_memory.title)` returns that memory. Today it returns a different node or nothing. `tests/cache.rs` never mixes a delete with a subsequent insert, which is why this passes CI.

**Verifier note.** The free-list evidence is in petgraph 0.6.5 (the version actually locked for this crate), src/graph_impl/stable_graph/mod.rs:264-273, not 0.7.1; behaviour is identical.

---

### CL2 — batch_id is content-independent while the server dedups on (producer_id, batch_id) alone, so a re-partitioned or seed-reused window silently drops rows and still advances the cursor

**Severity:** data loss. **Invariant:** R11 stable batch ids / §18.2 obligation 5 idempotency.

**Site:** `crates/exocortex-adapter-sdk/src/split.rs:97` (also: crates/exocortex-adapter-sdk/src/lib.rs:320, crates/exocortex-adapter-sdk/src/lib.rs:426, crates/exocortex-ingest/src/service.rs:520)

**Current behaviour.** `batch_id = "{producer_id}:{seed}:{index}"` (split.rs:97) where `index` is a per-`split_unit` counter that restarts at 0 for every unit (split.rs:72), and `submit_window` calls `split_unit` once per unit in a loop (lib.rs:320). The id encodes nothing about the batch's contents. The server's idempotency store is keyed on `(producer_id, batch_id)` only (ingest/src/service.rs:520) and returns the *original* ack with a `DuplicateBatch` reject row. The SDK classifies `DuplicateBatch` as `Disposition::Success` (classify.rs:32), counts `duplicates += 1`, treats the batch as settled, and advances the durable cursor at lib.rs:426. Nothing anywhere checks that two batches sharing an id share contents, and nothing validates that the seeds in one window are distinct.

**Failure scenario.** Two ways in, both silent. (a) A caller passes a window whose units share a `batch_id_seed` — e.g. one seed per polling window, one unit per source table. Unit A emits `p:win-7:0` and unit B emits `p:win-7:0`. B is deduped against A's ack; every row in B is never persisted, `WindowOutcome` reports `duplicates: 1, cursor_advanced: true`, and the cursor moves past B forever. (b) No caller error at all: run 1 submits `p:S:0={a}` under a tightened budget (the re-split loop at lib.rs:315-363 derives the budget from the observed stamped size), then the process dies before lib.rs:426. The operator raises `max_batch_bytes` and restarts; run 2 re-splits the same unit into a single `p:S:0={a,b,c}`. The server matches the id, replays the old ack, and `b` and `c` are lost while the cursor advances.

**Fix.** Make the id content-bound — e.g. `"{producer_id}:{seed}:{index}:{canonical_checksum}"` — or have the ingest service compare the stored batch's checksum against the incoming one and reject a mismatched replay with a hard error instead of `DuplicateBatch`. Independently, `submit_window` should reject a window whose units do not have distinct `batch_id_seed`s.

**Verification.** A cross-crate test that submits unit A as `p:S:0`, then a different unit that also splits to `p:S:0`, against the real `IngestService` (not `testing::MockServer`, which has no dedup), and asserts the second unit's rows are readable from storage. It fails today, and the SDK reports success.

---

### CL5 — The client always publishes a fabricated synthetic snapshot, so search/get/find_related return invented memories even when --backend is configured

**Severity:** correctness.

**Site:** `crates/exocortex-client/src/main.rs:135` (also: crates/exocortex-client/src/sync.rs:259, crates/exocortex-client/src/mcp.rs:139)

**Current behaviour.** `cache.publish(&args.org, synth_snapshot())` at main.rs:135 runs unconditionally, before and regardless of the `--backend` branch at main.rs:155. `synth_snapshot()` (main.rs:61-117) fabricates four memories ('Fix flaky auth test', 'Parser handles nested generics', …) stamped `Provenance::Asserted { author: "synthetic" }` with `project_id: "demo"`. The one thing that would replace them, `sync::run_sse_sync`, has zero non-test callers (`grep -rn run_sse_sync crates/` hits only tests/sync.rs and server/tests/e2e_chain.rs), so the cache is never fed from the backend in the shipped binary.

**Failure scenario.** Run `exocortex-mcp-client --backend http://prod:8080 --org acme` and call `exocortex.search_memories {"query":"auth"}`. The harness receives 'Fix flaky auth test' with a real-looking hex id and score, as an org-visible memory of org `acme`. It is not a memory; it is startup filler. Unlike the unimplemented Functions, which return a structured error via `call_tool`'s fallback (mcp.rs:458), this surface answers with plausible fabricated rows and no marker distinguishing them.

**Fix.** Gate `synth_snapshot()` behind an explicit standalone/dev flag (or `--backend.is_none()`), and when `--backend` is set either wire `run_sse_sync` or publish an empty snapshot so reads are honestly empty.

**Verification.** A stdio smoke test launched with `--backend <unreachable>` asserting `search_memories` returns zero memories rather than the four synthetic titles. Today's test (tests/stdio_smoke.rs:112) asserts the opposite and passes with no backend distinction.

---

### IN1 — Consolidation ignores RegionKey.org and .project, so a region-scoped lease authorises graph-wide merges, strengthens and prunes

**Severity:** correctness. **Invariant:** R-Dr3 / R-C3.

**Site:** `crates/exocortex-dreams/src/lib.rs:314` (also: crates/exocortex-dreams/src/lib.rs:186, crates/exocortex-dreams/src/lib.rs:403, crates/exocortex-dreams/src/lib.rs:522, crates/exocortex-dreams/src/lib.rs:355)

**Current behaviour.** `try_consolidate` takes the lease `LeaseKey::Dreams { org, region: "<project>:<memory_type>" }` (lib.rs:186-189), but `select_anchors` streams `stream_all_memories()` and filters ONLY on `m.memory_type != region.memory_type` (lib.rs:314) — `region.org` and `region.project` are never consulted. `strengthen` (lib.rs:403-420) streams every relationship in the store and bumps evidence/strength on all of them (its `_a` anchor argument is unused), `prune` (lib.rs:522-527) scans every memory, and `sparsity` explicitly discards the region (`let _ = region;`, lib.rs:355).

**Failure scenario.** Org has projects `alpha` and `beta`. A fire for region (org, alpha, type=3) acquires the lease keyed `alpha:3`, then merges a pair of near-duplicate type-3 memories that both belong to project `beta`, closes them, bumps evidence_count on every edge in the store, and writes SimilarTo edges across the project boundary at Visibility::Org. Worse, a concurrent fire for (org, beta, 3) acquires a DIFFERENT lease key (`beta:3`), so both cycles hold valid, non-conflicting leases while writing the same row set — every `upsert_batch_fenced` passes its own fencing check and the R-C3 guarantee that one owner mutates a region at a time is void.

**Fix.** Filter anchors, strengthen targets and prune candidates by the region's org and project (memory context project_id / tenant), not just memory_type, so the write set is a subset of what the held lease covers.

**Verification.** Seed two projects with duplicate type-3 memories, run `try_consolidate` for project alpha, and assert no project-beta memory has `valid_until` set and no project-beta relationship's `evidence_count` changed.

---

## 5. Remaining confirmed findings

Forty-seven defects, verified once each and not individually hand-checked.
Citations are exact; treat the reasoning as a strong lead rather than a
finished diagnosis.

### Kernel and ontology pack

**KP1 — Pack D1–D6 rule text is never compiled; the executing rules are a hand-copy in exocortex-reasoning that already disagrees (D1 guard dropped, D5 missing)**  
*correctness* · `crates/exocortex-pack-dev-v1/src/lib.rs:136` (also: crates/exocortex-kernel/src/macros.rs:94, crates/exocortex-kernel/src/pack.rs:103, crates/exocortex-reasoning/src/rules.rs:157, crates/exocortex-reasoning/src/rules.rs:185) · R-Pk3

- **Current:** `crepe_rules! { ... }` in the pack is captured only as `stringify!` text (macros.rs:94 `CREPE_RULES_SRC`) and the only thing done with it is `rule_ids_from_source` (pack.rs:103), which harvests the head-predicate identifier of each `;`-terminated chunk. The rule bodies are discarded. The rules that actually evaluate are re-authored by hand in `exocortex-reasoning/src/rules.rs`. The two copies already differ: pack D1 (lib.rs:138) is `implied_solves(a,b) <- edge(a,b,Fixes), memory(a, MemoryType::Fix, _)` — guarded on the source memory being a `Fix` — while the implemented D1 (rules.rs:157) is `ImpliedSolves(a, b) <- Edge(a, b, k), (k == FIXES);` with the type guard removed. Pack D5 `shared_target` (lib.rs:146) has no counterpart at all in rules.rs: there is no `SharedTarget` @output and no `shared_target` field on `Derived` (rules.rs:193-212). No test compares the pack's declared rule set to the engine's.
- **Fails when:** A `Fixes` edge whose source memory is not a `Fix` — writable today through `Storage::upsert_batch`, which performs no R-T17 check (the only two `triples_by_kind` readers are kernel validator.rs:50, which has zero non-test callers, and ingest service.rs:388), or through the client offline path — makes the engine emit `ImpliedSolves(a,b)` and therefore a Derived kernel-constant `SOLVES` edge (engine.rs:299 `"D1" => kinds::SOLVES`) that the pack's own D1 forbids. Separately, every consumer that reads `PackDef.rule_ids` (6 ids, asserted at pack-dev-v1/tests/loads_correctly.rs:72) is told D5 is part of the effective ontology while D5 can never fire.
- **Fix:** Make one artifact authoritative: either compile the pack's `crepe_rules!` text into the reasoning program, or delete the pack block and derive `rule_ids` from the reasoning program. Failing that, add a cross-crate gate asserting `pack_def().rule_ids` matches the set of rules `exocortex-reasoning` implements, and restore D1's `memory(a, Fix, _)` guard in rules.rs.
- **Verify:** A test in a crate that links both: for each id in `exocortex_pack_dev_v1::pack_def().rule_ids`, assert a corresponding `Derived` field exists — fails today on `shared_target`. Plus a rules test that feeds `Edge(task, problem, FIXES)` with `task.memory_type == Task` and asserts `implied_solves` is empty — fails today.
- **Verifier note:** Reachable defect is the absent D5 and the uncompared pack-vs-engine rule sets; the D1 missing type guard is currently masked by the R-T17 check at crates/exocortex-ingest/src/service.rs:384-400 and is a latent divergence, not a live wrong derivation. Derived-fields citation is crates/exocortex-reasoning/src/rules.rs:193-212 (rules.rs:185 is inside the Derived struct docs, not a D-rule).

**KP2 — Ontology::from_packs canonicalizes kind ids across packs but leaves memory/entity type ids pack-local, so every type triple in the second pack is evaluated against the wrong ids**  
*correctness* · `crates/exocortex-kernel/src/ontology.rs:112` (also: crates/exocortex-kernel/src/macros.rs:140, crates/exocortex-kernel/src/macros.rs:50, crates/exocortex-kernel/src/ontology.rs:72) · R-T17

- **Current:** `from_packs` assigns memory/entity type ids with a running offset across packs (ontology.rs:112-121 — `memory_type_names.len() as u8`), and the comment at ontology.rs:106-107 says this is deliberate so "multiple packs can never collide on u8 ids". But the `pack!` builder resolves `type_triples!` names through a *pack-local* map — `mt_by_name` at macros.rs:139-140 uses `MemoryType::$mt as u8`, the declaration index — and `MemoryType::id()` (macros.rs:50) returns that same declaration index. The remap loop at ontology.rs:60-77 rewrites `k.id`, `k.inverse` and `t.kind` for pack-space kinds, but never touches `t.from_types` / `t.to_types`. It also inserts `memory_type_by_name` entries without checking for a name already claimed by an earlier pack (ontology.rs:114), so a shared name silently resolves to the later pack's id.
- **Fails when:** Register a second pack `zz-pack` (sorted after `exocortex-pack-dev-v1`) declaring `memory_types! { Note, Ticket }` and `type_triples! { Foo => (Note, Ticket) }`. Its `TypeTriple.from_types` is `[0]` (pack-local `Note`), but the ontology assigns `Note` id 13. Any write of a `Note` with a `Foo` edge arrives at ingest with `memory_type == 13`, the triple lists `0`, `matches_triple` (validator.rs:67) / the ingest copy (service.rs:388-400) return false, and every legitimate write from the second pack is rejected `InvalidTypeTriple`. Symmetrically, a dev-v1 `Task` (id 0) satisfies zz-pack's `Foo` triple and is wrongly accepted.
- **Fix:** Apply the same per-pack offset to `TypeTriple.from_types`/`to_types` in the remap loop, and make the emitted `MemoryType::id()`/`EntityType::id()` resolve through the ontology instead of returning the declaration index. Also reject duplicate memory/entity type names across packs the way R-Pk1 rejects duplicate pack names.
- **Verify:** `Ontology::from_packs(vec![dev_v1, second_pack])` then assert `onto.triples_by_kind[foo][0].from_types == Some(vec![onto.memory_type_id("Note").unwrap()])` — fails today.
- **Verifier note:** Cite the offending id assignment at crates/exocortex-kernel/src/ontology.rs:114 (ontology.rs:112 is the `for p in &packs` header) and the un-remapped sides at ontology.rs:72-76.

**KP3 — R-T5 title/summary bounds are measured in bytes, so a valid non-ASCII title is rejected and the write is dropped**  
*correctness* · `crates/exocortex-kernel/src/validator.rs:25` (also: crates/exocortex-kernel/src/validator.rs:32, crates/exocortex-ingest/src/service.rs:215) · R-T5

- **Current:** `draft.title.len()` and `s.len()` are byte lengths (`SmolStr` derefs to `str`), but R-T5 (PRD line 2982: "`title` MUST be 1–200 characters ... `summary` MUST be ≤500 characters") and the error text itself (error.rs:65 "title must be 1..=200 chars") are character counts. The ingest copy at service.rs:215 (`m.title.len() > 200`) repeats the same byte-length test on a `String`.
- **Fails when:** A harness commits a memory titled with 120 CJK or emoji characters (~360 bytes). The kernel validator returns `KernelError::TitleBounds` and ingest rejects the row, even though the title is 120 characters — well inside the 200-character bound. The memory is silently dropped from the batch and never persisted.
- **Fix:** Use `title.chars().count()` and `summary.chars().count()` in both validator.rs and ingest service.rs:215, so the enforced bound matches the bound the spec and the error message state.
- **Verify:** `validate_draft` with a title of 200 multi-byte characters must return `Ok`; today it returns `TitleBounds`.

**KP4 — F01's [0,1] clamp is bypassed by its derived Deserialize, and Memory is deserialized straight out of FalkorDB props_json**  
*correctness* · `crates/exocortex-kernel/src/memory.rs:10` (also: crates/exocortex-kernel/src/memory.rs:49, crates/exocortex-storage/src/falkor.rs:150, crates/exocortex-cache/src/lib.rs:594) · R-T5

- **Current:** `F01` is a newtype whose only constructor validates the range (`F01::new`, memory.rs:14-20), but the type derives `Deserialize` (memory.rs:10), so `serde` reconstructs `F01(f32)` from any JSON number without ever calling `new`. `Memory` — which holds `importance: F01`, `confidence: F01`, `effectiveness: Option<F01>` (memory.rs:49-53) — is rehydrated from an untrusted external process by `memory_from_value` at falkor.rs:139-150, which does a bare `serde_json::from_str` over the node's `props_json` string.
- **Fails when:** A `Memory` node whose `props_json` carries `"importance": 1000.0` (written by any other client of the shared FalkorDB graph, a partially-applied migration, or a hand-edit) deserializes without error into a `Memory` with `importance.get() == 1000.0`. The cache ranking formula at cache/lib.rs:594 (`... + m.importance.get() * 0.50 + recency`) then pins that memory at the top of every `search_memories` result, and nothing anywhere reports a validation failure. `F01` gives the false impression that this is impossible.
- **Fix:** Replace the derived `Deserialize` on `F01` with a hand impl that funnels through `F01::new` and errors on out-of-range/NaN (`deserialize_with` or `#[serde(try_from = "f32")]`).
- **Verify:** `serde_json::from_str::<F01>("1000.0")` must be an `Err`; today it is `Ok(F01(1000.0))`.

**KP5 — The kernel Action/Function surface is dead: REQUIRED_VISIBILITY_CEILING and the R-Lat1 latency budgets have no readers, and the SLO gate hardcodes its own copies of the numbers**  
*correctness* · `crates/exocortex-kernel/src/actions.rs:16` (also: crates/exocortex-kernel/src/functions.rs:16, crates/exocortex-cache/benches/search.rs:132, crates/exocortex-ops/src/operations.rs:275) · R-Lat1

- **Current:** `Action::REQUIRED_VISIBILITY_CEILING` (actions.rs:16, documented "Author must be within source ceiling") and `Function::P50_BUDGET_US`/`P99_BUDGET_US` (functions.rs:16-18, documented "enforced by perf CI (§15, R-Lat1)") have zero references outside `crates/exocortex-kernel/src` — a repo-wide grep for either constant name matches only the kernel sources and the stale `target/package/` copy. `exocortex-ops` re-declares its own `PromoteVisibilityOp`/`PromoteVisibilityInput` (operations.rs:275-300) rather than implementing `kernel::actions::PromoteVisibility`, and the SLO gate re-states the budgets as literals: `p50 < Duration::from_micros(500) && p99 < Duration::from_millis(3)` at cache/benches/search.rs:132 versus `P50_BUDGET_US = 500 / P99_BUDGET_US = 3_000` at functions.rs:34-35.
- **Fails when:** Tighten `SearchMemories::P50_BUDGET_US` from 500 to 200 to reflect a new SLO: nothing recompiles differently, `cargo xtask` still gates at 500µs (search.rs:132), and the kernel constant is a comment. The reverse is worse — relax the bench literal to 5ms and the kernel still advertises a 500µs contract to every reader of the typed Function surface, including the PRD conformance audit. Likewise, `CommitWrapup::REQUIRED_VISIBILITY_CEILING = Org` is the declared R-T11a ceiling for the wrapup path, but the ceiling actually applied is whatever ingest/ops passes in `SourceCeiling` (validator.rs:22), which no code derives from the constant.
- **Fix:** Either have the perf gate read the budgets off `Function::P50_BUDGET_US`/`P99_BUDGET_US` and have ops implement `kernel::actions::Action` (deriving its `SourceCeiling` from `REQUIRED_VISIBILITY_CEILING`), or delete the two dead traits so no reader mistakes them for enforcement.
- **Verify:** A compile-time assertion in cache/benches/search.rs that its p50 literal equals `<SearchMemories as Function>::P50_BUDGET_US` — impossible today because the bench does not depend on the constant at all.

### Wire, signing, and the protocol contract

**WS3 — ClusterNode::admit — the entire R-W2/R-W3/R-W4 peer-admission gate — has zero non-test callers; no inbound peer envelope path exists**  
*correctness* · `crates/exocortex-cluster/src/node.rs:209` (also: crates/exocortex-cluster/src/node.rs:158, crates/exocortex-cluster/src/node.rs:141, proto/cluster.proto:6) · R-W2/R-W3/R-W4

- **Current:** `admit()` (node.rs:209-217) checks wire version, ontology fingerprint, and HMAC — the three fields `InvalidationEnvelope` exists to carry (proto/cluster.proto:6-10). A workspace-wide grep for `.admit(` outside `exocortex-cache` (an unrelated same-named method) returns only `crates/exocortex-cluster/tests/cluster.rs` and `crates/exocortex-cluster/tests/cross_node.rs`. `ClusterNode::run` (node.rs:150-168) subscribes only to *local* storage invalidations and publishes them; the comment at node.rs:163-166 says peer fan-out over Redis pub-sub "is wired at M5 server start", but no Redis pub-sub subscribe exists anywhere in `crates/` (only the Dreams fire queue in `fire.rs` uses Redis).
- **Fails when:** Deploy two backend nodes with mismatched ontology packs — the exact case R-W3/CR-18 exists to catch. Node B never receives an envelope from node A through any admitted path, so the fingerprint mismatch is never detected at the cluster layer; the operators get no `OntologyMismatch` and the divergence surfaces only as inconsistent client caches. Conversely, `publish_envelope` is `pub` (node.rs:141) and performs no verification at all, so any future wiring that reaches for it (the obvious call for a Redis subscriber) bypasses admission entirely, and the tests would still pass.
- **Fix:** Either wire the inbound peer path through `admit()` before `publish_envelope`, or make `publish_envelope` private and expose only an `admit_and_publish`. A gate with no callers is not a gate, and the ordering trap (`publish_envelope` being the public, unchecked entry point) makes the eventual wiring likely to skip it.
- **Verify:** A test that constructs an envelope from a node with a different fingerprint, hands it to node B's *production* ingress path, and asserts it is not fanned out. No such path exists to test today — that is the finding.
- **Verifier note:** node.rs:158 in the site list is the run-loop body; the storage subscribe is node.rs:157 and run() starts at :151.

**WS4 — proto3 float trap: RelationshipDraft.strength/confidence of NaN is persisted verbatim, and confidence == 0.0 is silently rewritten to 0.8 with no note in the schema**  
*correctness* · `crates/exocortex-ingest/src/service.rs:425` (also: crates/exocortex-ingest/src/service.rs:416, proto/ingest.proto:63, proto/ingest.proto:64, crates/exocortex-kernel/src/relationship.rs:46) · §18.6

- **Current:** proto/ingest.proto:63 documents `strength` as "0.0..1.0; 0 = RelMeta default" — the sentinel is in the schema. proto/ingest.proto:64 documents `confidence` as "0.0..1.0" with NO sentinel, yet service.rs:425-429 applies the same substitution: `confidence: if r.confidence == 0.0 { 0.8 } else { r.confidence.clamp(0.0, 1.0) }`. Separately, both branches use `f32::clamp`, which returns NaN for a NaN input, and `RelationshipProperties::{strength, confidence}` are raw `f32` (relationship.rs:46,48) with no validation — unlike `Memory.confidence`, which goes through `F01::new` (service.rs:317).
- **Fails when:** (a) An adapter asserts `confidence: 0.0` meaning "no confidence in this edge". The server persists 0.8. The producer's checksum covered 0.0, the graph holds 0.8, and nothing rejects or reports the substitution — a silent wrong write, and the schema the producer read gave no warning. (b) An adapter (or a non-Rust producer, since the proto is the public contract) sends `strength: NaN` — a legal proto3 float. `NaN == 0.0` is false, so the default branch is skipped; `NaN.clamp(0.0, 1.0)` returns NaN; NaN is written to the graph. Every subsequent comparison against that strength is false, so `effective_strength` (mcr2.rs:372) propagates NaN and the edge sorts unpredictably or vanishes from every ranked result, permanently and undetectably.
- **Fix:** Reject non-finite and out-of-range strength/confidence with a RejectRow rather than clamping (the same reject-don't-coerce posture B8/B9 already applies to table_uuid and schema_hash at service.rs:227+). If 0.0 is to remain a sentinel for confidence, document it on proto/ingest.proto:64 the way strength documents it on :63 — otherwise the schema and the server disagree about what 0.0 means.
- **Verify:** Test: submit a signed batch with a relationship at `strength: f32::NAN`, assert the batch is rejected. Today it is accepted and `relationship.properties.strength.is_nan()` is true in storage. Second test: submit `confidence: 0.0`, assert either rejection or a persisted 0.0 — today it silently becomes 0.8.
- **Verifier note:** The NaN consequence is worse than stated, not milder: FalkorStorage::props_json (crates/exocortex-storage/src/falkor.rs:977-989) runs serde_json::to_value on the row, which maps a non-finite f32 to Value::Null, so the edge is written with strength/confidence null and the read-back deserialization at falkor.rs:765-776 fails with 'bad rel props_json' — the relationship becomes unreadable, not merely mis-ranked. The finding's aside that Memory.confidence 'goes through F01::new (service.rs:317)' is loose: MemoryDraft has no confidence field and :317 is a hardcoded 0.8.

**WS5 — vis_from_i32 coerces every unknown or negative Visibility discriminant to PUBLIC, the widest value, instead of rejecting**  
*security* · `crates/exocortex-ingest/src/service.rs:196` (also: crates/exocortex-ingest/src/service.rs:701, proto/ingest.proto:81, crates/exocortex-kernel/src/visibility.rs:25) · R-T11a

- **Current:** `fn vis_from_i32` (service.rs:191-197) matches 0..3 explicitly and falls through with `_ => Visibility::Public`. proto3 permits any int32 in an enum field and prost preserves it as `i32`, so 5, 99, and -1 all arrive intact and all become PUBLIC — the widest tenancy scope. The same function decodes `RegisterSourceRequest.ceiling` (service.rs:701), so a garbage ceiling also becomes PUBLIC.
- **Fails when:** A non-Rust adapter, or a Rust one built against a newer copy of the enum, sends `ceiling: 7` on RegisterSource and `visibility: 7` on its drafts. Both coerce to PUBLIC. `Public.within(Public)` is true (visibility.rs:25), the R-I3 equality check passes because both sides coerced identically, and memories the producer believed were narrowly scoped are persisted at the widest visibility the system supports. The failure is entirely silent — no RejectRow, no log. The default-fallthrough also means a future enum addition automatically reads as PUBLIC on an old node instead of failing closed.
- **Fix:** Return an error for any discriminant outside 0..=4 (VISIBILITY_WIDENING or a new INVALID_VISIBILITY code) rather than defaulting. If a default is unavoidable, it must fail closed to PRIVATE, never open to PUBLIC — the same reject-don't-coerce rule §18.6 already applies to table_uuid and schema_hash widths.
- **Verify:** Test: signed batch with `ceiling: 99` and a memory at `visibility: 99`; assert rejection. Today the batch is accepted and the memory lands at PUBLIC.
- **Verifier note:** Impact is narrower than the write-up implies: a draft with visibility 7 coerces to Public and is then REJECTED by `vis.within(ceiling)` at service.rs:219-221 unless the ceiling is also Public, so the widening lands only through the self-declared ceiling — which Finding 4 already shows a producer can simply set to 4 outright. The real defect is fail-open coercion of an unknown discriminant, not a new privilege the producer did not already have.

### Storage: the Falkor adapter and the in-memory double

**ST3 — Memories with Visibility::Public are permanently unreadable on FalkorDB (every read template caps at `<= Org`)**  
*correctness* · `crates/exocortex-storage/src/falkor.rs:546` (also: crates/exocortex-storage/src/cypher.rs:110, crates/exocortex-storage/src/falkor.rs:686, crates/exocortex-storage/src/falkor.rs:653, crates/exocortex-ingest/src/service.rs:196) · R-T11

- **Current:** `get_memory`, `valid_at` and `get_state_at` pass `max_visibility = Visibility::Org as u8` (=3) into templates whose predicate is `m.visibility <= $max_visibility`. `Visibility::Public` is 4, so a Public row never matches. R-T11 (PRD line 3002) requires v1 read paths to treat `Public` as `Org`, not to drop it. The InMemory double applies no ceiling at all in `get_memory` (in_memory.rs:233-241) and returns the row.
- **Fails when:** An ingest batch whose `MemoryDraft.visibility` is 4 (or any value >3 — `vis_from_i32` maps every unknown value to `Public`, exocortex-ingest/src/service.rs:196) commits successfully; `Storage::get_memory` on the same id then returns `Ok(None)` forever, `valid_at` returns `None`, and `get_state_at` omits it from the snapshot counts. The write is acked and durable but invisible. The same sequence against `InMemoryStorage` returns the row, so no in-process test can catch it.
- **Fix:** Clamp the read ceiling per R-T11 (`Public` → treated as `Org`) at the storage boundary — either normalise `Public` to `Org` on write, or use `4` as the internal ceiling for the Org-level read paths — and make the double apply the same ceiling so the two agree.
- **Verify:** Parity test over both backends: upsert a `Visibility::Public` memory, assert `get_memory` returns it on both. Falkor fails today.

**ST4 — get_memory_for silently returns Ok(None) for an existing over-ceiling row on FalkorDB where the double returns PermissionDenied**  
*correctness* · `crates/exocortex-storage/src/falkor.rs:565` (also: crates/exocortex-storage/src/cypher.rs:110, crates/exocortex-storage/src/in_memory.rs:257, crates/exocortex-storage/src/trait_.rs:70) · R-MT4

- **Current:** The Falkor implementation delegates the ceiling check to the `get_memory_by_id` template (`WHERE m.visibility <= $max_visibility`), so a row above the caller's ceiling is filtered out server-side and the adapter falls into the `_ => Ok(None)` arm. It only raises `PermissionDenied` for the `Private`-authorship case (falkor.rs:569). The double raises `PermissionDenied` for both cases (in_memory.rs:257-262), which is what the trait doc (trait_.rs:70-73) and R-MT4 (PRD line 4865: "MUST return `PermissionDenied`, never a filtered subset silently") require.
- **Fails when:** Memory M exists with `Visibility::Org`; caller's `VisibilityContext.max_visibility` is `Project`. On FalkorDB `get_memory_for(&M.id, vc)` yields `Ok(None)` — the op layer reports "not found" and a client cannot distinguish a missing row from a forbidden one. On `InMemoryStorage` the identical call yields `Err(PermissionDenied)`. The R-MT4 acceptance test (crates/exocortex-ops/tests/parity.rs:268) only exercises the `Private` branch and only against the double, so the divergence is untested.
- **Fix:** Fetch at the historical ceiling and apply the visibility decision in Rust (as the double does), returning `PermissionDenied` whenever a row exists but is outside the caller's scope; the `traverse_bounded` and `find_by_entity` templates silently filter for the same reason and need the same treatment.
- **Verify:** Cross-backend test: upsert an `Org` memory, read it with a `Project`-ceiling context, assert `Err(PermissionDenied)` on both backends. Falkor returns `Ok(None)`.
- **Verifier note:** falkor.rs:565 is the run_template call; the precise defect line is falkor.rs:563 (`"max_visibility": vc.max_visibility as u8`) plus the `_ => Ok(None)` fallthrough at falkor.rs:573-574.

**ST7 — FalkorDB keeps no memory version history (MERGE overwrites in place) while the double keeps a version stack, and the double's property test asserts history reads Falkor cannot serve**  
*correctness* · `crates/exocortex-storage/src/cypher.rs:46` (also: crates/exocortex-storage/src/in_memory.rs:156, crates/exocortex-storage/src/in_memory.rs:310, crates/exocortex-storage/tests/in_memory_props.rs:124)

- **Current:** `upsert_memory` is `MERGE (m:Memory {id: $id}) SET ...` — one node per id, each write destroying the previous version's `valid_from`/`valid_until`/`props_json`. Relationships deliberately DELETE-then-CREATE "giving us stable bi-temporal history" (cypher.rs:62-64), but memories do not. `InMemoryStorage::upsert_memory` pushes onto a per-id history stack and `valid_at` searches that stack in reverse (in_memory.rs:310-315).
- **Fails when:** Write memory M with `valid_from = t0, valid_until = t1`, then write the same id with `valid_from = t1, valid_until = None`. `valid_at(&M.id, t0)` returns the first version's content on `InMemoryStorage` and `None` on FalkorDB (the surviving node's `valid_from = t1 > t0`). The property test `bi_temporal_roundtrip_prop` (tests/in_memory_props.rs:124-152) asserts exactly the first behaviour against the double, so the bi-temporal acceptance criterion is proven only where it holds; the live integration test sidesteps it by using two different ids and a comment (tests/integration.rs:239-241).
- **Fix:** Either give memories the same DELETE-then-CREATE/versioned-row treatment relationships get (keying rows by id+recorded_at, which is what the `valid_at` template's `ORDER BY m.recorded_at DESC LIMIT 1` already presumes), or make the double overwrite in place so both backends have identical, honestly-tested semantics.
- **Verify:** Run the exact body of `bi_temporal_roundtrip_prop` against `FalkorStorage`: `valid_at(id, t0)` returns `None` instead of the superseded version.
- **Verifier note:** Frame as: the double implements a versioning model the production backend does not have, and the property test certifies it — rather than 'Falkor is missing history'.

**ST8 — The double's stream_all_memories / stream_all_relationships emit every historical version, Falkor emits one current row per id**  
*correctness* · `crates/exocortex-storage/src/in_memory.rs:322` (also: crates/exocortex-storage/src/in_memory.rs:333, crates/exocortex-storage/src/falkor.rs:709, crates/exocortex-storage/src/trait_.rs:102, crates/exocortex-dreams/src/lib.rs:403, crates/exocortex-cache/src/lib.rs:238)

- **Current:** Both double streams do `values().flat_map(|h| h.iter().cloned())` over the per-id history stack, yielding N rows for an id written N times. Falkor's pagers walk `MATCH (m:Memory)` / `MATCH ()-[r]->()`, which after MERGE/DELETE-then-CREATE hold exactly one row per id. The trait documents "current versions" (trait_.rs:102-105).
- **Fails when:** Dreams `strengthen_edges` (exocortex-dreams/src/lib.rs:403-430) streams every relationship, bumps `evidence_count`, and writes the results back with `upsert_batch_fenced`; the `res.strengthened` de-dup guard is only populated after the loop. On the double, the second consolidation cycle sees relationship X twice (two stack entries), bumps and rewrites it twice in one pass, and grows the stack to 4 — evidence counts and strengths diverge from what the same code produces on FalkorDB, and the growth is super-linear per cycle. Cache reseed (`GraphSnapshot::from_storage`) likewise inserts superseded versions.
- **Fix:** Make the double stream only `h.last()` per id (the current version), matching the Falkor pagers and the trait doc.
- **Verify:** Test: upsert the same memory id twice, drain `stream_all_memories()`, assert exactly one row. The double returns two.

**ST9 — soft_delete_relationship matches `:RELATES`, a relationship type the adapter never creates, so delete_relationship is a silent no-op that reports success**  
*correctness* · `crates/exocortex-storage/src/cypher.rs:231` (also: crates/exocortex-storage/src/cypher.rs:91, crates/exocortex-storage/src/falkor.rs:531, crates/exocortex-storage/src/in_memory.rs:217)

- **Current:** `upsert_relationship` creates edges with a per-kind type substituted into the `__KIND_TYPE__` placeholder (cypher.rs:91) — the M2 amendment recorded two lines above deliberately dropped the generic `:RELATES` type. `soft_delete_relationship` was never updated and still matches `()-[r:RELATES {id: $rel_id}]->()`; `RELATES` appears nowhere else in the workspace except this template and two comments. `delete_relationship` ignores the (always empty) row set, publishes `Invalidation::RelationshipDeleted`, and returns a successful `CommitRecord`.
- **Fails when:** On FalkorDB, `delete_relationship(&id)` for any existing edge closes nothing: `valid_until` stays null, the edge keeps matching `count_state_at_rels` and the relationship stream, yet the caller gets `Ok(CommitRecord)` and every SSE subscriber and local cache evicts the edge (exocortex-cache/src/lib.rs:412). Backend and caches now disagree permanently, and a cache reseed resurrects the "deleted" edge. `InMemoryStorage::delete_relationship` really does close `valid_until`, so the double hides it.
- **Fix:** Match the edge by id without pinning a type (`MATCH ()-[r]->() WHERE r.id = $rel_id AND r.valid_until IS NULL`), or substitute the kind type the same way the upsert template does; and return an error (or a distinguishable no-op) when no row matched instead of unconditionally publishing an invalidation.
- **Verify:** Integration test: upsert a relationship, `delete_relationship`, then assert `get_state_at(now).relationship_count == 0`. It stays 1.
- **Verifier note:** Downgrade the failure scenario from 'backend and caches disagree permanently' to 'latent: the only delete-relationship implementation is a guaranteed no-op that reports success; it has no production caller yet, so it will fail the first time one is wired'.

**ST10 — Redis lease acquire/release are non-atomic multi-command sequences: a crash between SET NX and EXPIRE leaves an immortal lease, and release can delete another node's lease**  
*idempotency* · `crates/exocortex-storage/src/falkor.rs:815` (also: crates/exocortex-storage/src/falkor.rs:824, crates/exocortex-storage/src/falkor.rs:874, crates/exocortex-storage/src/falkor.rs:1004) · R-C2

- **Current:** `acquire_lease` issues `SET NX` and then a separate `EXPIRE` (falkor.rs:815-829); `release_lease` issues `GET` and then a separate `DEL` (falkor.rs:874-885). Neither is a CAS. `check_lease_current` (falkor.rs:1004-1020) verifies only that the stored token matches — it never checks `expires_at`, so it trusts the TTL that `EXPIRE` may never have set. The PRD calls for `SET NX EX` plus `WATCH` (R-C2, PRD line 3669), and the double additionally requires `expires_at > now` (in_memory.rs:110).
- **Fails when:** (a) A node wins `SET NX` and is killed (or the Redis call errors) before `EXPIRE` lands: the lease key exists with no TTL. No other node can ever acquire `LeaseKey::Dreams{...}` for that region again — consolidation for the region is dead until an operator deletes the key by hand — and if the process survives, its long-dead lease keeps passing `check_lease_current` forever. (b) Node A's lease expires between its `GET` and `DEL` in `release_lease` while node B acquires; A's `DEL` removes B's key, so B's next fenced write is rejected with `FencedWriteRejected` mid-cycle and a third node can acquire the "free" key.
- **Fix:** Use `SET key token NX EX ttl` as one command for acquisition, and a compare-and-delete / compare-and-expire Lua script for release and renew (renew has the same GET-then-EXPIRE window, falkor.rs:845-859). Have `check_lease_current` also verify the key's remaining TTL rather than relying on a TTL that may never have been set.
- **Verify:** Test that sets the lease key without a TTL (simulating the interrupted acquire) and asserts a later `acquire_lease` eventually succeeds; and a release test where the key is replaced by another holder's token between GET and DEL, asserting the other holder's lease survives.
- **Verifier note:** Cite falkor.rs:818 (set_nx) and falkor.rs:827 (expire) for acquire, falkor.rs:882 (del) for release; the (b) race window is GET→DEL only, not the whole 874-885 range.

**ST11 — InMemoryStorage::delete_relationship publishes no invalidation, so relationship deletions never reach the change feed on the double**  
*correctness* · `crates/exocortex-storage/src/in_memory.rs:217` (also: crates/exocortex-storage/src/falkor.rs:533, crates/exocortex-cache/src/lib.rs:412)

- **Current:** Every other write on the double publishes to `self.feed` (upsert_memory in_memory.rs:158, delete_memory 190, upsert_relationship_row 132), and the Falkor adapter publishes `Invalidation::RelationshipDeleted` (falkor.rs:533). `InMemoryStorage::delete_relationship` mutates the row and returns without touching the feed — and, unlike the other three, does not even take the feed path.
- **Fails when:** Run the backend node with `--storage=memory` (crates/exocortex-server/src/main.rs:175) or any in-process test: after `delete_relationship(&id)`, no SSE event is emitted, so `LocalCache`'s `RelationshipDeleted` handler (exocortex-cache/src/lib.rs:412) never runs and the deleted edge stays in every subscriber's cached graph indefinitely. Any test of the delete→invalidate→evict path written against the double passes vacuously.
- **Fix:** Publish `Invalidation::RelationshipDeleted { id, lsn }` from `InMemoryStorage::delete_relationship`, matching the Falkor adapter.
- **Verify:** Subscribe to `subscribe_invalidations`, delete a relationship, assert a `RelationshipDeleted` arrives. Nothing arrives on the double.
- **Verifier note:** Drop the 'run the backend node with --storage=memory' production framing; crates/exocortex-server/src/main.rs:172-173 documents that backend as CI/dev only.

**ST12 — InMemoryStorage::get_state_at counts an id if ANY historical version is valid and ignores visibility entirely, so snapshot counts disagree with FalkorDB**  
*correctness* · `crates/exocortex-storage/src/in_memory.rs:294` (also: crates/exocortex-storage/src/cypher.rs:242, crates/exocortex-storage/src/falkor.rs:651)

- **Current:** The double filters with `h.iter().any(|m| valid(...))` over the whole per-id version stack and applies no visibility predicate. Falkor's `count_state_at` counts the single surviving node with `valid_from <= $at AND (valid_until IS NULL OR valid_until > $at) AND m.visibility <= $max_visibility` where the ceiling is `Visibility::Org`.
- **Fails when:** (a) Upsert memory M twice, then `delete_memory(&M.id)`: the double only closes `valid_until` on the last stack entry, so the earlier entry still has `valid_until == None`, `any()` is true, and `get_state_at(now)` keeps counting the deleted memory forever; Falkor counts 0. (b) Upsert one `Visibility::Public` memory: the double counts 1, Falkor counts 0. Either way, the CR-4 snapshot count returned by the double is not the count the production backend returns for the same write sequence.
- **Fix:** Evaluate validity against the current (last) version only and apply the same `<= ceiling` visibility predicate the Falkor template uses — once the `Public`/R-T11 ceiling question is settled, apply the same rule to both.
- **Verify:** Cross-backend test: upsert-twice-then-delete a memory and assert `get_state_at(now).memory_count == 0` on both backends. The double returns 1.
- **Verifier note:** The `any()` calls are at in_memory.rs:296 and in_memory.rs:300; line 294 is the `memory_count: store` field start.

### Cache and reasoning

**CR4 — traverse ignores TraversalSpec.direction: Direction::In always returns empty and Direction::Both follows only outgoing edges**  
*correctness* · `crates/exocortex-cache/src/lib.rs:517`

- **Current:** The BFS iterates `snap.petgraph.edges(n)` (lib.rs:506), which on a directed StableGraph yields OUTGOING edges only. The direction match (lib.rs:514-518) then picks `e.target()` for `Out`, `e.source()` for `In`, and `e.target()` for `Both`. For an outgoing edge `e.source() == n`, which is already in `seen`, so every `In` candidate is skipped at lib.rs:519. `Both` is textually identical to `Out`.
- **Fails when:** Both production callers of `find_related` pass `Direction::Both`: crates/exocortex-ops/src/operations.rs:107 (HTTP/registry op) and crates/exocortex-client/src/mcp.rs:213 (MCP tool). Given `A -Causes-> B` and anchoring on B, `find_related(B, k=2)` returns an empty neighborhood; A is never reported even though the relationship exists and is visible. Anchoring on A returns B. So the k-hop neighborhood is silently half of what the caller asked for, and `Direction::In` is unreachable dead code. This also diverges from the storage-side traversal, which returns outgoing hits for every direction value (crates/exocortex-storage/src/cypher.rs:134 `MATCH (a)-[rels*1..$max_depth]->(b)`), so the two traversal implementations do not agree for the same spec.
- **Fix:** Select the edge iterator from the spec: `edges_directed(n, Outgoing)` for `Out`, `edges_directed(n, Incoming)` for `In`, and the chain of both for `Both`, taking `e.target()`/`e.source()` as the non-`n` endpoint accordingly.
- **Verify:** Build A -Causes-> B, traverse from B with `Direction::Both`, assert A is returned. Today the result is empty. No test in tests/cache.rs or tests/parity.rs exercises `traverse` with an inbound edge.
- **Verifier note:** The 'A Causes B, anchor B returns empty' scenario is wrong: R-T4 materialises the inverse row for every non-bidirectional kind (crates/exocortex-storage/src/in_memory.rs:198-216 via exocortex_kernel::materialize_inverse, relationship.rs:71-91), and Causes declares inverse CausedBy, bi:false (crates/exocortex-pack-dev-v1/src/lib.rs:33), so B has an outgoing CausedBy edge to A and Both-as-Out still reaches A when spec.kinds is empty. The reachable failure is on BIDIRECTIONAL kinds, where materialize_inverse returns None (relationship.rs:74-76): SimilarTo, RelatedTo, Contradicts, AnalogousTo, ParallelTo (pack-dev-v1 lib.rs:54-55, 61-64). A Dreams SimilarTo A->B or a derived RelatedTo A->B is stored once, so find_related(B) never returns A. The second failure is a caller passing a non-empty spec.kinds: the inverse edge carries the inverse kind and is filtered at lib.rs:508, so Both loses the reverse hop entirely. The 'diverges from storage traversal' claim is also wrong — cypher.rs:134 is likewise outgoing-only, so the two agree.

**CR5 — insert_relationship adds a parallel duplicate edge on every re-upsert of the same RelationshipId**  
*resource* · `crates/exocortex-cache/src/lib.rs:185`

- **Current:** `insert_relationship` calls `petgraph.add_edge(*a, *b, r)` unconditionally (lib.rs:182-187) with no lookup by `r.id`. `apply(RelationshipUpserted)` routes every relationship invalidation through it (lib.rs:406-409). `remove_relationship` (lib.rs:189-206) breaks after finding the FIRST edge with that id, so it can only ever remove one copy.
- **Fails when:** Dreams `strengthen` re-upserts existing relationships every consolidation cycle to bump `evidence_count` and recompute strength (crates/exocortex-dreams/src/lib.rs:420-428). Each of those emits `RelationshipUpserted{id}` → the change-feed bridge (crates/exocortex-server/src/backend.rs:117) → `apply` → one more parallel edge in the cache graph for the same relationship. After N Dreams cycles a single relationship is N+1 edges. Two consequences: (a) unbounded edge growth with no dedupe and no backpressure — `est_bytes` grows by 256 per duplicate but see the 2Q accounting finding, so nothing evicts; (b) search scoring at lib.rs:588 counts each duplicate as a separate `explicit` relationship worth 0.30, so a memory's score climbs by 0.30 per Dreams cycle purely from duplicates, permanently distorting `search_memories` ranking. A single `RelationshipDeleted` afterwards removes only one copy.
- **Fix:** Keep a `by_rel_id: DashMap<RelationshipId, EdgeIndex>` on the snapshot; on insert, replace the existing edge's weight (or remove-then-add) instead of appending; on remove, use the map instead of the O(V·E) scan.
- **Verify:** Apply the same `RelationshipUpserted{id}` twice and assert `snapshot.petgraph.edge_count() == 1` and that the search score of the `from` memory does not change between the two applies. Today edge_count is 2 and the score rises by 0.30.

**CR6 — Derived edges from R7, R8 and R9 collide on one RelationshipId, so later rules silently overwrite earlier ones' provenance and confidence**  
*correctness* · `crates/exocortex-reasoning/src/engine.rs:174` (also: crates/exocortex-reasoning/src/engine.rs:305, crates/exocortex-reasoning/src/engine.rs:246, crates/exocortex-kernel/src/ids.rs:74)

- **Current:** `write_back`'s `push` closure computes `RelationshipId::derive(from, kind, to, None)` (engine.rs:174). The preimage is `from || kind || to || ""` (crates/exocortex-kernel/src/ids.rs:74-81) — it does not include `rule_id`. `derived_kind` maps R7, R8 and R9 all to `RelatedTo` via the catch-all arm (engine.rs:305). The three rules are pushed in fixed order R7 (211), R8 (214), R9 (217) into the same `new_rels` vec, and the dedup filter at engine.rs:246-249 only checks ids already in STORAGE, not ids duplicated within the batch.
- **Fails when:** Two memories X and Y that (a) both `Solves` the same problem P, (b) share an entity, and (c) share a tag — the normal shape for two solutions to one bug. R8 fires (bridge, strength 0.3, confidence 0.8, rule_id "R8"), R7 fires, R9 fires. All three produce `derive(X, RelatedTo, Y)` — one id, three rows in the same `upsert_batch` (engine.rs:251). Storage applies them in order (crates/exocortex-storage/src/in_memory.rs:174-177; falkor.rs:487-490), so R9's row wins: the persisted edge is stamped `Provenance::Derived { rule_id: "R9" }` with R9's confidence, and the R8 bridge derivation is lost. On every subsequent run the id is in `existing`, so the wrong attribution is frozen permanently. `explain_edge` (crates/exocortex-reasoning/src/explain.rs:165) then reports the wrong rule for that derivation.
- **Fix:** Include the rule id in the derived identity — e.g. `RelationshipId::derive(from, kind, to, Some(rule_id))` — or give each affinity rule a distinct kind. Also dedupe `new_rels` by id before the batch so one rule cannot overwrite another inside a single call.
- **Verify:** Storage with X,Y both Solves P, sharing an entity and a tag; run `k_hop_reason(X, 2)`; assert three distinct RelatedTo edges exist (or at minimum that the R8 bridge is retrievable). Today exactly one edge exists, attributed to R9. tests/rules.rs only asserts `rules::evaluate` output (line 143-159) and never inspects the written rows' rule_id.

**CR7 — derived_confidence feeds the neighborhood edge count where §14.2 requires the shared entity/tag count**  
*correctness* · `crates/exocortex-reasoning/src/engine.rs:313` (also: crates/exocortex-reasoning/src/engine.rs:187, crates/exocortex-reasoning/src/engine.rs:312, crates/exocortex-cache/src/lib.rs:586, docs/prd/exocortex-core-prd.md:4763)

- **Current:** `derived_confidence(rule_id, evidence)` is called with `evidence.len()` (engine.rs:187), where `evidence` is the list of every distinct relationship id touched by the k-hop BFS (engine.rs:85, 115-117). For R7 and R9 it computes `(evidence as f32 / 5.0).min(1.0)` (engine.rs:313-314). PRD §14.2 (docs/prd/exocortex-core-prd.md:4763-4764) specifies `shared_count / 5.0` for R7 and `shared_tag_count / 5.0` for R9 — the number of shared entities/tags for THAT pair, a quantity the code never computes. Line 312 additionally hardcodes `1.0/3.0` for R4/R5 where §14.2 (line 4762) specifies `1.0 / depth`, and these rules are depth 2, so the value should be 0.5.
- **Fails when:** Seed memory with no relationships at all (a fresh session's first memory): the BFS finds no edges, `evidence` is empty, so EVERY R7/R9 affinity edge derived in that pass is written with confidence 0.0 despite the pair sharing entities or tags. Conversely, a seed sitting in a dense neighborhood of 5+ edges gives confidence 1.0 to a pair sharing exactly one tag. These confidences are persisted and then consumed by the cache search scorer at crates/exocortex-cache/src/lib.rs:586 (`inferred += er.properties.confidence * 0.15`), so §14.1 ranking is computed from a number that has no relationship to the §14.2 formula. Because the writeback is id-idempotent (engine.rs:246-249), the confidence assigned by whichever seed happened to run first is never corrected.
- **Fix:** Carry the per-pair join cardinality out of the rule program (e.g. emit `CoOccurrenceAffinity(a, b, shared_count)` / `SimilarTagsAffinity(a, b, shared_tag_count)`), pass that count to `derived_confidence`, and change the R4/R5 arm to `1.0 / depth`.
- **Verify:** Two memories sharing exactly 2 tags and no relationships → assert the derived RelatedTo edge has confidence 0.4. Today it is 0.0. Two memories sharing 1 tag inside a 10-edge neighborhood → assert 0.2; today it is 1.0.
- **Verifier note:** PRD line numbers are off by one: §14.2's table rows are at docs/prd/exocortex-core-prd.md:4761 (transitive, 1.0/depth), 4762 (R7 shared_count/5.0) and 4763 (R9 shared_tag_count/5.0), not 4762-4764.

**CR8 — Rule D4 (contradiction_propagates) is evaluated on every fixpoint and then discarded — no writeback, no reader**  
*correctness* · `crates/exocortex-reasoning/src/engine.rs:231` (also: crates/exocortex-reasoning/src/rules.rs:170, crates/exocortex-reasoning/src/rules.rs:208, crates/exocortex-pack-dev-v1/src/lib.rs:144)

- **Current:** `write_back` consumes R4, R5, R7, R8, R9, D1, D2, D3 and D6 (engine.rs:205-231) but never `derived.contradiction_propagates`. That field (crates/exocortex-reasoning/src/rules.rs:208) is populated by the D4 rule (rules.rs:169-172) and by `evaluate` (rules.rs:270), and `Derived::total()` counts it (rules.rs:227), but nothing anywhere in the workspace reads it — `grep contradiction_propagates` matches only rules.rs and the pack declaration.
- **Fails when:** Store `A Contradicts B` and `B Confirms C`. D4 fires and produces `(A, C)` in the fixpoint. `write_back` never touches it, so no edge is written and no metric is incremented. The pack declares D4 as a shipped rule (crates/exocortex-pack-dev-v1/src/lib.rs:143-144: 'if A Contradicts B and B Confirms C then A Contradicts C'), so a caller who relies on contradiction propagation gets nothing, silently — the rule is dead enforcement. There is also no test: tests/rules.rs covers R1-R9, D1, D2, D3 and D6 but has no D4 case.
- **Fix:** Add `for (a, b) in derived.contradiction_propagates { push(a, b, "D4", 0.5); }` to write_back and a `"D4" => onto.kind_id("Contradicts")` arm to `derived_kind`, plus a rules.rs test asserting the edge lands.
- **Verify:** Storage with A-Contradicts->B and B-Confirms->C; run `k_hop_reason(A, 2)`; assert a Contradicts edge A→C exists with Derived{rule_id:"D4"}. Today no such edge is ever written.

**CR9 — D6 writeback is structurally incapable of writing anything: the derived id is byte-identical to the edge the rule consumed**  
*correctness* · `crates/exocortex-reasoning/src/engine.rs:230` (also: crates/exocortex-reasoning/src/rules.rs:175, crates/exocortex-reasoning/src/engine.rs:302, crates/exocortex-ingest/src/service.rs:407)

- **Current:** D6 is `SessionCohort(m, s) <- Edge(m, s, IN_SESSION)` (crates/exocortex-reasoning/src/rules.rs:175) — it re-emits its own input pair. `write_back` pushes it as `push(a, b, "D6", 0.6)` (engine.rs:229-231), `derived_kind` maps "D6" to `kinds::IN_SESSION` (engine.rs:302), and the id is `RelationshipId::derive(from, IN_SESSION, to, None)` (engine.rs:174). Ingest assigns relationship ids with the identical call — `RelationshipId::derive(from_mem.id, kind, to_mem.id, None)` (crates/exocortex-ingest/src/service.rs:407).
- **Fails when:** A session commits `M InSession S`. Ingest stores it under id `derive(M, IN_SESSION, S, None)`. Reasoning runs, D6 fires on that exact edge, and `push` computes the same id. The `existing` set built at engine.rs:238-245 (every relationship id in storage) therefore always contains it, so the filter at engine.rs:246-249 always drops it. D6 can never contribute a row under any input; `exocortex_rules_executed_total` never counts it. Either the writeback is wrong (D6's output should be memory↔memory cohort pairs, as the pack comment says: 'all memories InSession S are candidates for MCR2 grouping', crates/exocortex-pack-dev-v1/src/lib.rs:147) or the rule head is wrong; as shipped it is dead code that looks live.
- **Fix:** Make D6 emit cohort pairs — `SessionCohort(m1, m2) <- Edge(m1, s, IN_SESSION), Edge(m2, s, IN_SESSION), (m1 != m2)` — and write it back as a RelatedTo/affinity edge rather than as another InSession edge.
- **Verify:** Storage with M1 InSession S and M2 InSession S; run `k_hop_reason(M1, 2)`; assert some new derived edge exists that did not before. Today the relationship count is unchanged. tests/rules.rs:162-179 only asserts the fixpoint output, never the writeback.

**CR10 — Search scoring counts Computed and Extracted edges as explicit relationships, so Dreams SimilarTo edges inflate rank at 0.30 each**  
*correctness* · `crates/exocortex-cache/src/lib.rs:588` (also: crates/exocortex-kernel/src/provenance.rs:22, crates/exocortex-dreams/src/lib.rs:490)

- **Current:** The §14.1 scorer branches on `Provenance::Derived` only (lib.rs:584-589): Derived edges contribute `confidence * 0.15`, and EVERYTHING else falls into the `else` arm contributing a flat 1.0 to `explicit`, worth 0.30 in the final score (lib.rs:593-594). `Provenance::Computed` (internal similarity/co-occurrence producers) and `Provenance::Extracted` are not explicit assertions — see crates/exocortex-kernel/src/provenance.rs:21-34.
- **Fails when:** Dreams writes SimilarTo edges stamped `Computed { SimilarityHnsw }` (crates/exocortex-dreams/src/lib.rs:490-509). A memory that Dreams links to 10 similar anchors gains `10 * 0.30 = 3.0` on its search score — treated exactly as if a human had asserted 10 relationships — where §14.1 (docs/prd/exocortex-core-prd.md:4747-4753) puts inferred edges in the `Σ inferred_edge_confidence × 0.15` term, capping the same 10 edges at 1.5 and weighting them by confidence. Base match is 1.0, so a single Dreams pass can more than quadruple a memory's score and permanently dominate `search_memories` ranking over genuinely well-connected memories.
- **Fix:** Treat `Computed` and `Extracted` as inferred: count them in the `inferred` term (using `confidence` for Computed, `extraction_confidence`-scaled confidence for Extracted) and reserve `explicit` for `Provenance::Asserted`.
- **Verify:** Two memories with equal importance and recency; give one 10 Asserted edges and the other 10 Computed SimilarTo edges at confidence 0.9; assert the Asserted one ranks higher. Today they tie at +3.0 vs +3.0.
- **Verifier note:** The Computed stamp on Dreams SimilarTo edges is at crates/exocortex-dreams/src/lib.rs:465-468 (the write is at 503-507); line 490 is the idempotency comment.

**CR11 — R7/R9 attribute facts are harvested from every memory in storage, bypassing the CR-6 neighborhood cap and producing a quadratic derived-edge blowup**  
*resource* · `crates/exocortex-reasoning/src/engine.rs:143` (also: crates/exocortex-reasoning/src/rules.rs:154, crates/exocortex-reasoning/src/engine.rs:101, crates/exocortex-reasoning/src/engine.rs:161)

- **Current:** After bounding the BFS at `MAX_NODES = 512` (engine.rs:101) and scoping edge facts to the neighborhood (engine.rs:133), the attribute harvest deliberately scans ALL memories with no scoping and no cap (engine.rs:143-152). R7 joins `EntityFact(a,e), EntityFact(b,e), (a != b)` and R9 joins `TagFact(a,t), TagFact(b,t), (a != b)` (crates/exocortex-reasoning/src/rules.rs:145-146, 153-154), both unbounded and symmetric. Every resulting pair goes straight into `write_back` (engine.rs:211-219) with no limit.
- **Fails when:** A repository where 10,000 memories carry the tag `rust`: R9 alone materializes 10,000 × 9,999 ≈ 10^8 ordered pairs in the Crepe fixpoint (each a MemoryId pair held in memory), then `write_back` builds ~10^8 `Relationship` structs in `new_rels` before it even queries `existing` — OOM long before the first write. The `MAX_NODES` cap that is supposed to be the CR-6 bound is applied only to the edge BFS and cannot restrain this. `session_reason` (engine.rs:159-163) makes it worse by re-running the whole pass, including the full memory scan and both full relationship scans, once per committed memory. The queue-overflow guard at engine.rs:53-58 provides no backpressure here because the blowup is inside a single work item. (Secondary: the neighborhood cap is itself soft — `neighborhood.insert(*other) && neighborhood.len() <= MAX_NODES` at engine.rs:112 inserts before testing, so the set overruns 512 by up to one frontier's degree.)
- **Fix:** Cap the attribute join: harvest tag/entity facts only for memories in the neighborhood (or a bounded expansion of it), skip attributes whose posting list exceeds a threshold (high-frequency tags carry no affinity signal), and hard-cap the number of derived pairs per pass with an observable drop counter.
- **Verify:** Seed storage with 2,000 memories all carrying one common tag and one relationship; run `k_hop_reason`; assert derived edges written is bounded by a configured cap. Today the pass allocates ~4M pair tuples for 2,000 memories and scales quadratically.

**CR12 — Every derived edge persists the entire k-hop neighborhood's edge list as its provenance evidence**  
*resource* · `crates/exocortex-reasoning/src/engine.rs:183` (also: crates/exocortex-reasoning/src/explain.rs:150, crates/exocortex-kernel/src/provenance.rs:18)

- **Current:** `evidence` accumulates every distinct relationship id the BFS walks over (engine.rs:85, 115-117) and is then cloned wholesale into EVERY derived row: `evidence: evidence.to_vec()` inside the `push` closure (engine.rs:181-184). `Provenance::Derived.evidence` is documented as 'the supporting edge set' for that assertion (crates/exocortex-kernel/src/provenance.rs:18-19), not the neighborhood.
- **Fails when:** A 512-node neighborhood with 3,000 edges, producing 5,000 derived affinity rows (easy given the unbounded R7/R9 join): each row carries a `Vec<RelationshipId>` of 3,000 × 16 bytes, so the batch handed to `upsert_batch` (engine.rs:251) is ~240 MB of provenance for 5,000 logical edges — serialized into storage per row. Correctness-wise, `explain_from_storage` walks `evidence` as the parent set (crates/exocortex-reasoning/src/explain.rs:149-153) and the Steel `walk` recurses over every parent to depth 5 (crates/exocortex-reasoning/scripts/explain.scm:5-15), so an explanation tree for one affinity edge enumerates all 3,000 unrelated edges as its causes — the tree is not an explanation, and it blows the 1ms/5ms `explain_edge` budget in §15 (docs/prd/exocortex-core-prd.md:4794) by orders of magnitude.
- **Fix:** Have the rule program emit the supporting edge ids per derived fact (Crepe can carry them in the head) and stamp only those on each row; for the attribute rules, evidence should be empty or the attribute provenance, not the edge neighborhood.
- **Verify:** Derive one R4 transitive edge from exactly two DependsOn hops inside a 50-edge neighborhood; assert `provenance.evidence.len() == 2` and that the two ids are the two hops. Today it is 50 and includes every unrelated edge.

**CR13 — 2Q byte accounting is never updated by the invalidation path and eviction is unreachable for a resident org, so the cache budget is unenforced**  
*resource* · `crates/exocortex-cache/src/lib.rs:417` (also: crates/exocortex-cache/src/lib.rs:427, crates/exocortex-cache/src/lib.rs:456, crates/exocortex-server/src/backend.rs:117)

- **Current:** `apply` clones the snapshot, mutates `est_bytes` through insert/remove (lib.rs:131, 154, 184), and publishes it with `g.store(Arc::new(next))` (lib.rs:417) — it never touches `tq.bytes`. Only `Reseed` (lib.rs:350-359) and `publish` (lib.rs:669-678) reconcile `tq.bytes`. Separately, `admit` runs its eviction loop (lib.rs:456-475) only after falling through to the new-admission branch; the three re-reference branches return early at lib.rs:432, 440 and 451, so once an org is in Am or A1in no publish ever re-checks the budget.
- **Fails when:** v1 is one org per client/node (lib.rs:378, `org_of_write`). The backend node reseeds once at boot (crates/exocortex-server/src/backend.rs:86 → `Reseed` → `admit` puts the org in A1in) and then serves every subsequent write through the change-feed bridge (backend.rs:104-127) as `CacheWrite::Apply`. From that point on: the snapshot's `est_bytes` grows with every memory and every duplicate edge (see the parallel-edge finding), `tq.bytes` stays frozen at its boot value, and even if it did grow, `admit` for an already-admitted org returns at lib.rs:432 before reaching the `while tq.bytes > self.budget` loop. The declared 2 GiB budget (backend.rs:78) therefore constrains nothing on a long-running node; the process grows until the OS kills it, with no eviction, no metric, and no backpressure.
- **Fix:** Reconcile `tq.bytes` in `apply` the same way `Reseed` does (subtract the old snapshot's `est_bytes`, add the new one's), and run the budget check unconditionally at the end of `admit` rather than only on the fresh-admission path.
- **Verify:** Publish one org, then drive `apply(MemoryUpserted)` until the snapshot's `est_bytes` exceeds the configured budget; assert `resident_orgs()` drops or that an `evict_*` admission event is recorded. Today `tq.bytes` is unchanged and no eviction fires. `tests/cache.rs:129` only exercises the budget through repeated `publish` of DISTINCT orgs, which is the one path that does account.

### Cluster and server

**CS1 — Empty replay ring answers "you are current" instead of 409 Resync, silently dropping every invalidation between the client's since_lsn and the node's LSN**  
*data loss* · `crates/exocortex-cluster/src/node.rs:102` (also: crates/exocortex-server/src/sse.rs:79, crates/exocortex-client/src/sync.rs:211, crates/exocortex-server/tests/sse_replay.rs:146) · R-C6

- **Current:** `replay_since` returns `Replay::Fresh(vec![])` whenever the ring is empty (node.rs:102-104), regardless of how far `since_lsn` is behind the backend frontier. The ring is process-local and starts empty on every node start (node.rs:86); it is only fed by `publish_envelope` from this node's own `run()` loop (node.rs:141-145, 157-167). The SSE handler treats `Fresh(vec![])` as a clean reconnect and streams live deltas (sse.rs:79-91). On the client, `LsnGate::push` is not yet anchored, so the FIRST live envelope re-anchors the gate to whatever LSN it carries (`self.next = lsn; self.anchored = true;`, sync.rs:211-217) — the gap is neither detected nor reported.
- **Fails when:** Backend frontier is at LSN 900. Node B restarts (or a load balancer moves the subscriber from node A to freshly-started node B). Client reconnects to `GET /v1/changes?since_lsn=500`. B's ring is empty -> `Fresh(vec![])` -> HTTP 200, no replay. Next commit publishes LSN 901; the client's gate anchors at 901 and applies it. Invalidations 501..900 are never delivered and never detected: the local cache keeps serving rows that were deleted or superseded, permanently, with no 409, no metric, and no log.
- **Fix:** Track the highest LSN this node has ever observed (or read the storage frontier) independently of ring occupancy, and return `Replay::TooOld` whenever `since_lsn < frontier` cannot be bridged by the ring — including when the ring is empty and `since_lsn` is below the frontier. `replay_floor()` must then report a real floor rather than the `unwrap_or(1)` placeholder at node.rs:125.
- **Verify:** Publish envelopes 1..=3 through a node, drop and rebuild the ClusterNode (or construct a fresh one) and assert `replay_since(2)` is `Replay::TooOld`, and that `GET /v1/changes?since_lsn=2` answers 409 with `x-exocortex-min-lsn`. The existing test at crates/exocortex-server/tests/sse_replay.rs:145-148 asserts the opposite ("empty ring bridges any since_lsn") but only exercises `since_lsn=0`, where nothing can be lost; it generalises an accident to a rule.
- **Verifier note:** replay_floor's placeholder is at node.rs:117-124 (the `unwrap_or(1)` on line 123), not exactly 125

**CS4 — The backend-node re-election lease is on a key no fenced write ever uses, so the elected "leader" fences nothing and Dreams runs on every node**  
*correctness* · `crates/exocortex-server/src/backend.rs:52` (also: crates/exocortex-server/src/backend.rs:284, crates/exocortex-server/src/backend.rs:74, crates/exocortex-dreams/src/lib.rs:186) · R-C3/R-Dr3

- **Current:** `dreams_lease_key` builds `LeaseKey::Dreams { org: "org", region: "*:*" }` (backend.rs:52-57, org hardcoded at backend.rs:74) and the re-election task acquires/renews it forever (backend.rs:284-330), recording only `leader_node_id`/`lease_epoch` into the health snapshot. The `OwnerLease` is never handed to `upsert_batch_fenced`/`delete_memory_fenced` and never handed to the Dreams engine. Dreams is spawned unconditionally on every node (backend.rs:163-166) and acquires its OWN lease under a different key, `LeaseKey::Dreams { org: region.org, region: "{project}:{memory_type}" }` (dreams/lib.rs:186-189). `"*:*"` never equals `"{project}:{memory_type}"`, and `"org"` never equals a real `region.org`, so the two keys can never collide.
- **Fails when:** Three backend nodes start. Node A wins `Dreams{"org","*:*"}` and reports itself leader; B and C poll and fail every 400ms. A fire event for region `(acme, api, 3)` then reaches all three nodes; each calls `try_consolidate`, which contends on `Dreams{"acme","api:3"}` — a lease the re-election loop has never touched. Leadership as measured by the M5 leader-kill acceptance test and reported by /health/cluster is decoupled from which node actually performs owner-only work; killing the reported leader changes nothing about Dreams ownership, and the acceptance criterion is measured against a lease that gates no write. Additionally, on FalkorStorage each failed poll still runs `INCR <key>:epoch` before the `SET NX` (falkor.rs:808-820), so two follower nodes bump the fencing-epoch counter for that key ~5 times/second forever.
- **Fix:** Either drive the re-election loop off the same key(s) Dreams fences with and pass the acquired `OwnerLease` into the Dreams engine so consolidation is gated by the elected lease, or delete the loop and stop reporting its outcome as leadership.
- **Verify:** A two-node in-memory test asserting that the lease acquired by `run_backend_node`'s re-election task is the same `LeaseKey` that `DreamsEngine::try_consolidate` contends for, and that a node which loses re-election performs no fenced Dreams write.

**CS5 — mcp-standalone never supervises the FalkorDB child: no restart on crash, no kill on exit, and the port it picks is published nowhere**  
*resource* · `crates/exocortex-server/src/supervisor.rs:73` (also: crates/exocortex-server/src/main.rs:94, crates/exocortex-server/src/main.rs:102, crates/exocortex-server/src/supervisor.rs:26, crates/exocortex-server/src/supervisor.rs:138)

- **Current:** `spawn_supervised` spawns the child, waits for one PING, and returns (supervisor.rs:73-110). There is no restart loop anywhere in production code; `max_restarts` is annotated `#[allow(dead_code)] // enforced by the M5 lifecycle loop` (supervisor.rs:26-27) and is read by nothing outside the unit test. `SupervisedServer.child` is likewise `#[allow(dead_code)]` with the comment "kill on drop arrives with M5" (supervisor.rs:32-35) — `std::process::Child` does not kill on drop. `main.rs:102-107` then enters `loop { std::thread::sleep(1s) }`, which never calls `try_wait`. The port comes from `free_port()` (main.rs:94, supervisor.rs:129-132), which binds :0, reads the assignment, drops the listener, and is only ever emitted to a tracing line (main.rs:103).
- **Fails when:** Run `exocortex-node --mode mcp-standalone`. Kill the child redis-server (OOM, crash, `kill`). The parent keeps sleeping forever, reports nothing, restarts nothing, and any client pointed at the recorded port gets connection-refused permanently. Separately, sending SIGKILL to the parent leaves the redis-server child orphaned and still holding the data dir and port. And because the port is ephemeral and written to no file or env, nothing but a human reading stderr can discover where the supervised store landed.
- **Fix:** Replace the sleep loop with a real supervision loop that `try_wait`s the child, restarts within `max_restarts`, kills the child on parent shutdown (signal handler + `Drop`), and writes the chosen port to a discoverable location (or take a fixed `--storage-port`).
- **Verify:** A test that spawns a supervised child, kills it, and asserts the supervisor restarts it up to `max_restarts` then exits non-zero. The existing `supervisor_restarts_a_crashed_child` test (supervisor.rs:138-178) calls no production function at all — it spawns `/bin/sleep` by hand and asserts a local counter equals `max_restarts`, so it passes no matter what `spawn_supervised` does.

**CS6 — backend_lsn and sync_lsn are never written on backend-node, so /health/sync always reports lag 0**  
*correctness* · `crates/exocortex-server/src/http_bind.rs:179` (also: crates/exocortex-server/src/backend.rs:223, crates/exocortex-server/src/backend.rs:293, crates/exocortex-server/src/http_bind.rs:194) · R-O6

- **Current:** `HealthSnapshot::backend_lsn` and `sync_lsn` (http_bind.rs:33-36) are written by nothing: `run_backend_node` sets `node_id`/`hydrated` (backend.rs:223-227), `storage_ok` (237-241), `reasoning_alive` (246-250), and `leader_node_id`/`lease_epoch`/`last_lease_tick` (293-310), and no other code path in either crate assigns those two fields — a workspace-wide grep for `backend_lsn`/`sync_lsn` under crates/exocortex-server finds only the struct fields and the JSON renderers. Both therefore stay at their `Default` value of 0 for the process lifetime, and `"lag": h.backend_lsn.saturating_sub(h.sync_lsn)` (http_bind.rs:179) is `0u64.saturating_sub(0)`.
- **Fails when:** Backend-node runs; the cache change-feed bridge task (backend.rs:106-128) loses its subscription and sits in the 1s retry loop while writes keep committing, so the node's cache falls arbitrarily far behind. `GET /health/sync` reports `{"sync_lsn":0,"backend_lsn":0,"lag":0}` — a healthy answer — and `/health/cluster` and `/health/hydration` both report `backend_lsn: 0`. Any alerting built on R-O6 replication lag can never fire, and an operator diagnosing stale reads is told the node is fully caught up.
- **Fix:** Feed the snapshot from real sources: `backend_lsn` from the storage frontier (the same value `FalkorStorage::last_backend_lsn` reads) and `sync_lsn` from `ctx.cache.version(&org).backend_lsn`, which http_bind.rs:194 already proves is reachable from this router.
- **Verify:** Commit N writes through a running backend-node and assert `/health/sync` reports a non-zero `backend_lsn`; then stall the cache bridge and assert `lag` becomes non-zero.

**CS7 — Both consumers of the storage invalidation stream discard decode errors silently, permanently losing the change with no metric or log**  
*correctness* · `crates/exocortex-cluster/src/node.rs:160` (also: crates/exocortex-server/src/backend.rs:117, crates/exocortex-storage/src/falkor.rs:944)

- **Current:** `FalkorStorage::subscribe_invalidations` yields `Err(StorageError::Backend("bad invalidation: ..."))` for a payload that fails to deserialize (falkor.rs:944-951) — the stream stays open and continues. Both consumers of that stream throw the error away without recording anything: `ClusterNode::run` does `let Ok(inv) = inv else { continue };` (node.rs:160), and the backend cache bridge does `if let Ok(inv) = item { ... }` (backend.rs:117-119). Neither increments a counter nor emits a tracing event, and neither treats the loss as a reason to reseed.
- **Fails when:** A rolling upgrade puts a node emitting an `Invalidation` variant the older node's serde enum cannot parse (or a single truncated pub-sub payload) on the channel. The older node's `run()` drops it, so the envelope is never published to the replay ring or the SSE hub, and the cache bridge drops it too, so the local cache never applies it. The LSN simply never appears anywhere on that node: SSE subscribers of that node see a hole with no gap marker in the ring, and the node's own cache serves the stale row indefinitely. Nothing in logs or /metrics records that a change was lost.
- **Fix:** Log and count the error in both consumers (`exocortex_cluster_invalidation_decode_errors_total`), and treat a decode failure as a reseed/resync trigger rather than a no-op, since the LSN sequence is now known to be incomplete.
- **Verify:** Publish a malformed payload on the storage channel and assert a non-zero decode-error counter and a warn-level log from both `ClusterNode::run` and the backend cache bridge.
- **Verifier note:** backend.rs's swallowing `if let Ok(inv) = item` is at 117-119 as cited; falkor.rs's error construction is at 946, not 944

**CS8 — replay_since overflows on since_lsn = u64::MAX, panicking the SSE handler in debug builds**  
*correctness* · `crates/exocortex-cluster/src/node.rs:106` (also: crates/exocortex-server/src/sse.rs:66)

- **Current:** `if since_lsn + 1 < floor` (node.rs:106) adds to a caller-supplied `u64` with no checked/saturating arithmetic. The value comes straight off the query string via `v.parse::<u64>().ok()` (sse.rs:66) with no range check.
- **Fails when:** `GET /v1/changes?since_lsn=18446744073709551615` against a node with a non-empty ring. In a debug build (the default for `cargo run` and for the dev-token path this binary already special-cases) `since_lsn + 1` overflows and panics inside the axum handler, aborting the connection task. In a release build it wraps to 0, `0 < floor` is true, and the handler answers 409 with `x-exocortex-min-lsn`; the client at sync.rs:326-329 then does `next_lsn = next_lsn.max(min)`, which leaves `next_lsn` at u64::MAX, so it reconnects with the same value and 409-loops forever.
- **Fix:** Use `since_lsn.saturating_add(1)` (or compare `since_lsn < floor.saturating_sub(1)`), and reject an out-of-range `since_lsn` with 400 rather than silently coercing it.
- **Verify:** A test issuing `GET /v1/changes?since_lsn=18446744073709551615` against a populated ring and asserting a well-formed response rather than a dropped connection.

### Ingest, operations, and Dreams

**IN3 — accept_discovery ignores the caller-supplied kind and creates every edge as RelKindId(0) with no ontology or triple validation**  
*correctness* · `crates/exocortex-ops/src/operations.rs:427` (also: crates/exocortex-ops/src/operations.rs:387, crates/exocortex-ops/src/operations.rs:421, crates/exocortex-ingest/src/service.rs:371) · R-T17 / R-Dr2

- **Current:** `AcceptDiscoveryInput` declares `kind: String` (operations.rs:386-387) and that field is published in the JSON Schema on both surfaces, but `handle` never reads it: the relationship id is derived with `RelKindId(0)` (operations.rs:421-426) and `kind` is set to `RelKindId(0)` (operations.rs:427). There is also no `ontology.kind_id` resolution and no R-T17 triple check — the equivalent ingest path does both (service.rs:371-404).
- **Fails when:** A caller accepts a Transitive proposal with `kind: "Solves"`. The committed edge has kind id 0 (whatever the first kind in the effective ontology happens to be), silently mislabelling the relation; traversals filtered by kind return the wrong set. Because no triple check runs, the same call can create a (from_type, kind, to_type) combination the ontology forbids — a shape the Ingestion Protocol rejects with InvalidTypeTriple — through an operation that is mounted on the authenticated HTTP surface.
- **Fix:** Resolve `input.kind` through the ontology, reject UnknownKind, run the same R-T17 triple validation the ingest path uses, and derive the id from the resolved kind. Also reject computed-only kinds here, as service.rs:378 does.
- **Verify:** Call AcceptDiscoveryOp with kind "Solves" and assert the committed relationship's `kind` equals `ontology.kind_id("Solves")`. The existing test (parity.rs:186-199) passes kind "Solves" but asserts only that `edge_id` is non-empty, so the defect is invisible to it.
- **Verifier note:** One sub-claim is moot rather than wrong: 'reject computed-only kinds here, as service.rs:378 does' cannot bite as written, because input.kind is discarded entirely — a caller passing "SimilarTo" gets kind 0, not a SimilarTo edge. The defect is the ignored kind plus the absent triple check.

**IN4 — The Dreams write-counter trigger can never fire: seconds_since_last_cycle is never set and on_write has no non-test callers**  
*correctness* · `crates/exocortex-dreams/src/trigger.rs:43` (also: crates/exocortex-dreams/src/lib.rs:148, crates/exocortex-dreams/src/lib.rs:175, crates/exocortex-dreams/src/fire.rs:131) · §12.2 / R-Dr13

- **Current:** `DreamsTrigger::should_fire` returns false whenever `c.seconds_since_last_cycle < min_interval` (6h by default, trigger.rs:42-45). The only production writer of `RegionWriteCounters` is `on_write` (lib.rs:148-156), which increments `memories_since_last_cycle` only, and `run` resets the struct to `Default` (lib.rs:175) — `seconds_since_last_cycle` is left at 0 forever despite the field doc "updated on read". So `should_fire` is unconditionally false in the running node. Independently, `on_write` has zero non-test callers anywhere in the workspace (the ingest commit path at service.rs:612-630 never notifies Dreams), and `RedisFireQueue::fire` (fire.rs:131) has no callers either — nothing in the repo ever RPUSHes onto `exocortex:dreams:queue` that backend.rs:180 drains.
- **Fails when:** Operator runs the backend node with no Redis URL (the default). Memories accumulate indefinitely; no consolidation cycle ever runs, because the only path to `tx_fire` in production is `notify` from the Redis drainer, and the in-process trigger predicate is dead in two independent ways. With Redis configured, the drainer still only consumes — no component in the workspace produces fire messages.
- **Fix:** Have the ingest commit path call `on_write` for the committed region, stamp `seconds_since_last_cycle` from a per-region last-cycle timestamp before evaluating the predicate, and wire `RedisFireQueue::fire` on the producing side.
- **Verify:** An end-to-end test that submits enough batches to cross `memory_threshold` and asserts a consolidation cycle ran. The existing trigger tests (trigger.rs:56-87) construct `RegionWriteCounters` by hand with a non-zero `seconds_since_last_cycle`, a value no production code path ever produces.

**IN5 — A merge that leaves fewer than two anchors aborts the cycle after the merge committed: no audit record, no regression check, no rollback**  
*correctness* · `crates/exocortex-dreams/src/lib.rs:284` (also: crates/exocortex-dreams/src/mcr2.rs:153, crates/exocortex-dreams/src/lib.rs:303, crates/exocortex-dreams/src/lib.rs:429) · R-Dr4 / R-Dr10

- **Current:** `consolidate_under` commits merges (lib.rs:237-241) and strengthens (lib.rs:262) before computing `res.mcr2_after = self.score_with(&remaining)?` (lib.rs:284). `MCR2Engine::compute` returns `Err(TooFew(2))` when the input has fewer than 2 entries (mcr2.rs:153-155), and the `?` propagates straight out of `consolidate_under`, skipping the R-Mcr3 regression check (lib.rs:288), the rollback, and `write_audit` (lib.rs:303). The same applies to any `upsert_batch_fenced` error inside `strengthen` (lib.rs:429).
- **Fails when:** A region contains exactly two embedded memories with cosine >= 0.92. The cycle merges one away and commits the closure. `remaining` now has length 1, `score_with` returns TooFew, `try_consolidate` returns Err, and `run` logs "consolidation failed" (lib.rs:169). The memory is permanently closed, but no ConsolidationResult is stamped — R-Dr4 requires an audit record for the cycle, and R-Dr10's "why is this gone" question now has no answer. A fenced-write rejection mid-cycle produces the same half-applied, unaudited state.
- **Fix:** Treat a post-cycle set too small to score as a non-error (carry `mcr2_before` forward, or record ΔR as unavailable) and, more generally, write the audit stamp on every exit path from `consolidate_under` once any write has committed.
- **Verify:** A test with exactly two duplicate embedded memories in a region: assert `try_consolidate` returns Ok and that the ConsolidationResult (or ledger) records the merge.

**IN6 — Ingest entity ids are always derived from the literal org string "org" — with_org does not rebuild the extractor**  
*correctness* · `crates/exocortex-ingest/src/service.rs:99` (also: crates/exocortex-ingest/src/service.rs:113, crates/exocortex-ingest/src/service.rs:128, crates/exocortex-ingest/src/entities.rs:162, crates/exocortex-server/src/backend.rs:201) · R-T18 / §17.2

- **Current:** `IngestServer::new` hardcodes `let org = "org".to_string();` (service.rs:99) and builds `extractor: EntityExtractor::new(&org)` (service.rs:113). `with_org` (service.rs:128-131) sets only `org_guard` and leaves the extractor untouched. `EntityExtractor::entity_ids` hashes `self.org_id` into every EntityId (entities.rs:162), and `EntityId::from_parts` is documented as org-scoped (crates/exocortex-kernel/src/ids.rs:89-92). The shipped backend builds the server exactly this way: `IngestServer::new(...).with_reasoning(...).with_org(&org)` (backend.rs:201-203).
- **Fails when:** A node pinned to org "acme" commits a memory mentioning `tokio`; the stored EntityId is `blake3("entity-v1" | "org" | 4 | "tokio")`. A node pinned to org "globex" against the same FalkorDB stores the identical id for its own memories. `find_by_entity` (storage trait_.rs:84) for that entity id then returns both orgs' memories — the org scoping the id derivation exists to provide is not applied, and the value used is a constant that no deployment configures.
- **Fix:** Rebuild the extractor in `with_org` (or take the org in `new`) so `EntityExtractor::new` receives the node's actual org id.
- **Verify:** Build `IngestServer::new(..).with_org("acme")`, submit a memory containing a known entity, and assert the stored EntityId equals `EntityId::from_parts("acme", t, name)`. No existing test checks the org component of an extracted entity id.
- **Verifier note:** The concrete cross-org collision scenario is one step weaker than stated: FalkorConfig.graph_name (crates/exocortex-storage/src/falkor.rs:36) is per-node config, so two orgs collide only if an operator points both nodes at the same graph name. The defect itself — with_org pinning the guard but not the extractor, leaving org scoping unapplied — is unconditional and is the seam worth reporting.

**IN7 — The client mints a fresh batch_id and fresh memory ids on every end_session attempt, so the server's idempotency registry can never match a retry**  
*idempotency* · `crates/exocortex-client/src/tools/end_session.rs:161` (also: crates/exocortex-client/src/tools/end_session.rs:132, crates/exocortex-ingest/src/service.rs:517, crates/exocortex-ingest/src/service.rs:638) · §18.8.5

- **Current:** The ingest server keys idempotency on `(producer_id, batch_id)` (service.rs:517-529, stored at 638-641) and every in-repo producer is `"session-wrapup"`, so the batch_id is the whole key. `EndSessionTool::handle` generates `batch_id: uuid::Uuid::now_v7()` (end_session.rs:161) and a new draft `id: uuid::Uuid::now_v7()` per memory (end_session.rs:132) inside the call, on every invocation. Non-snapshot memories also get a fresh `MemoryId::new_v7()` server-side (service.rs:241) since `external_key` is always None for session wrapup (end_session.rs:140).
- **Fails when:** `client.submit(batch)` commits server-side but the response is lost (connection reset, deadline). `handle` maps the transport error to `rmcp::Error::internal_error` (end_session.rs:196-198) and the harness retries `end_session` with the same drafts. The retry carries a new batch_id, misses the seen-batch LRU entirely, and commits a second, id-distinct copy of every memory and edge. The idempotency registry — the only defence the protocol has against duplicate wrapups — is unreachable from the only producer in the tree.
- **Fix:** Derive the batch_id deterministically from the wrapup content (e.g. session_id plus a content hash of the drafts) and hold it across retries, so a replay hits the (producer_id, batch_id) entry the server keeps.
- **Verify:** A test that submits the same wrapup twice through EndSessionTool and asserts the second ack is the DuplicateBatch replay. crates/exocortex-ingest/tests/ingest.rs:272 exercises replay only by re-sending a hand-built batch with a fixed batch_id, which the client never produces.

**IN8 — Dreams strengthen re-applies the §14.3 decay to the already-decayed stored strength, so repeated cycles monotonically weaken every edge**  
*correctness* · `crates/exocortex-dreams/src/lib.rs:413` (also: crates/exocortex-dreams/src/mcr2.rs:372) · §14.3

- **Current:** `strengthen` reads the persisted `r.properties.strength` and writes back `effective_strength(r.properties.strength, evidence_count, success_rate.unwrap_or(1.0), age_days)` (lib.rs:411-419). `effective_strength` (mcr2.rs:372-377) is a derivation from a BASE strength: it multiplies by `success` (0.5..1.0) and `decay` (0.5..1.0). Feeding its own output back in compounds both factors once per cycle.
- **Fails when:** An asserted edge with strength 0.5, evidence_count 1, no success_rate, recorded 100 days ago (decay factor 0.5). Cycle 1 stores (0.5 + 0.00) * 1.0 * 0.5 = 0.25. Cycle 2 stores (0.25 + 0.05) * 0.5 = 0.15. Cycle 3 stores 0.11. The action named "strengthen" drives every surviving edge's strength toward zero as cycles accumulate, degrading ranking and traversal weighting; the effect is invisible in a single-cycle test.
- **Fix:** Keep the authored base strength as a separate stored field and recompute the effective value from it each cycle, or make `strengthen` a pure evidence_count update with the decay applied at read time.
- **Verify:** Run `try_consolidate` twice over the same graph and assert an edge's strength after cycle 2 is >= its strength after cycle 1. Every current dreams test runs a single cycle.

**IN9 — Entity extraction keeps only the first match of each pattern, silently dropping every other entity of that type in the text**  
*correctness* · `crates/exocortex-ingest/src/entities.rs:135` (also: crates/exocortex-ingest/src/entities.rs:140, crates/exocortex-ingest/src/entities.rs:206) · R-T18

- **Current:** `extract` iterates the RegexSet's matched PATTERN indices, and for each matched pattern calls `pat.find(&text)` (entities.rs:135), which returns only the leftmost match. `hits` therefore holds at most one string per pattern, not one per occurrence. Ten of the twelve types have a single pattern, so at most one entity of those types is ever extracted from a memory. The same bug makes `let ambiguous = hits.len() > 1` (entities.rs:140) unreachable for those ten types, so the R-T18 ambiguous-confidence value 0.6 can never be assigned to them.
- **Fails when:** A memory whose content is "fixed the parser in src/lexer.rs and src/parser.rs" yields exactly one File entity, `src/lexer.rs`. `Memory.context.entities` (set by attach_entities, entities.rs:206) omits `src/parser.rs`, so `find_by_entity` for src/parser.rs never returns this memory — a silently incomplete index with no error anywhere.
- **Fix:** Use `pat.find_iter(&text)` and collect every occurrence (deduped by canonical name), and base the ambiguity flag on distinct match count rather than pattern count.
- **Verify:** `extract("a.rs and b.rs", &[])` should return two File entities; today it returns one.
- **Verifier note:** attach_entities is at entities.rs:205 (signature), not 206 — the finding's 206 is the first body line. entity_ids/from_parts at 162 is correct.

**IN10 — MCP and HTTP serve different implementations and different output shapes for the same registered operations**  
*correctness* · `crates/exocortex-client/src/mcp.rs:176` (also: crates/exocortex-ops/src/operations.rs:169, crates/exocortex-ops/src/operations.rs:104, crates/exocortex-client/src/mcp.rs:449, crates/exocortex-ops/tests/parity.rs:113) · CR-9 / R-P1 / R-P2

- **Current:** The registry is described as the single implementation behind both surfaces (lib.rs:2-5, http_bind.rs:2-6), and the HTTP surface really does dispatch `entry.handler` (http_bind.rs:90-96). The MCP surface does not: `call_tool` hard-dispatches to hand-written methods (mcp.rs:447-455) and the registry loop at mcp.rs:426 is used only to LIST names. Those hand-written methods diverge from the registered ops: `get_memory` over MCP (mcp.rs:176-193) consults only the cache and returns `{"memory": null}` on a miss, whereas the registered `GetMemory` (operations.rs:169-192) falls through to `get_memory_for` and raises Unauthorized on PermissionDenied; its success shape is `{id,title,memory_type}` versus the op's `{memory:{id,title,memory_type,visibility}}`. `find_related` returns a bare array `[{id,title}]` (mcp.rs:213-224) versus `{memories:[{id,title,memory_type,visibility}]}`; `search_memories` returns a `snapshot_version` and `memory_type: "type:N"` (mcp.rs:139-166) versus `{memories, scores}`.
- **Fails when:** A harness calls `exocortex.get_memory` over MCP for a memory that exists in storage but is absent from the local cache snapshot, and for a memory it is not permitted to see. Both return `{"memory": null}`. The same two calls over HTTP return the row and HTTP 403 respectively. A client written against the published input/output schemas — which are generated from the registry for BOTH catalogues — cannot parse the MCP responses at all.
- **Fix:** Dispatch MCP tool calls through `entry.handler` with an OpContext, as the HTTP bind does, so there is one implementation per operation.
- **Verify:** A cross-surface test that runs the same input through `entry.handler` and through the MCP `call_tool` path and asserts byte-identical JSON. The existing CR-9 test (parity.rs:113-129) compares the registry handler against the typed op in the SAME crate — both are the ops implementation, so it can never detect this divergence.
- **Verifier note:** Two inaccuracies to fix before this is written up. (1) The registry loop is at mcp.rs:427, not 426. (2) 'a client written against the published input/output schemas — generated from the registry for BOTH catalogues — cannot parse the MCP responses' is wrong: list_tools seeds the catalogue from the rmcp-derived Self::*_tool_attr() (mcp.rs:421-426) and adds a registry entry only when the name is absent, which it never is, so MCP advertises the hand-written signatures. The defensible claim is the divergence itself plus get_memory's self-inconsistent hit/miss shape at mcp.rs:184-196.

**IN11 — OpContext.deadline is a startup constant that no handler reads, and OpError::DeadlineExceeded is never constructed**  
*correctness* · `crates/exocortex-ops/src/lib.rs:31` (also: crates/exocortex-ops/src/lib.rs:47, crates/exocortex-server/src/backend.rs:219, crates/exocortex-server/src/http_bind.rs:273) · R-R3 / R-R2

- **Current:** `OpContext.deadline` is documented as "Deadline for this operation (R-R3 budget enforcement)" (lib.rs:30-31). The backend builds ONE shared `Arc<OpContext>` at startup with `deadline: Utc::now() + 30s` (backend.rs:219) and every HTTP request reuses it (http_bind.rs:51, 91). No operation handler in operations.rs reads `ctx.deadline`, and a workspace-wide grep finds no construction of `OpError::DeadlineExceeded` — only its declaration (lib.rs:47) and the HTTP status mapping (http_bind.rs:273).
- **Fails when:** A traversal or search issued 60 seconds after node start already has a deadline 30 seconds in the past, and one issued in the first second has 30 seconds of budget — the same operation gets a different (and after the first 30s, always-expired) budget purely as a function of node uptime, and neither case changes behaviour because nothing checks it. The REQUEST_TIMEOUT arm of the HTTP error mapping is unreachable code.
- **Fix:** Compute the deadline per request from the configured budget (a per-call field or an extractor-supplied value) and check it in the handlers and at the storage boundary, or remove the field and the error variant rather than shipping an enforcement surface that cannot fire.
- **Verify:** A test that constructs an OpContext with a deadline already in the past and asserts the handler returns `OpError::DeadlineExceeded`; today it returns Ok.
- **Verifier note:** OpError::DeadlineExceeded is the variant on lib.rs:48; lib.rs:47 is its doc comment. The struct-field cite (lib.rs:31) is exact.

**IN12 — audit_range silently substitutes the process-local ledger for the durable one on storage error or on any empty range**  
*correctness* · `crates/exocortex-ops/src/audit.rs:114` (also: crates/exocortex-ops/src/audit.rs:86, crates/exocortex-ops/src/audit.rs:139, crates/exocortex-ops/src/operations.rs:525) · R-A1 / R-A3

- **Current:** `audit_range` runs the `audit_range` template and falls through to the in-process `LEDGER` both when the query returns `Err` (the `if let Ok` at audit.rs:114 discards the error) and when it returns zero rows (audit.rs:115-117). `append_audit` likewise downgrades a failed durable write to the in-memory ledger and returns Ok with the intended LSN (audit.rs:86-89), and `LEDGER` is a process-global static (audit.rs:139) that dies with the process.
- **Fails when:** FalkorDB is unreachable. `promote_visibility` commits (the memory write went to a different, reachable path or was retried), `append_audit` logs a warning and returns Ok, and the ack reports an audit_lsn that corresponds to nothing durable. An operator then calls `GET /v1/audit?since_lsn=0` (operations.rs:525): the storage query errors, the error is swallowed, and the response is the handful of records this process happens to hold — presented as a complete, authoritative audit range with HTTP 200. After a restart the same call returns an empty list. R-A3's readers cannot distinguish "no records in range" from "the ledger is unavailable".
- **Fix:** Propagate the storage error as `OpError::Storage` instead of falling through, and treat an `Ok` empty result set as authoritative rather than as a cue to consult the volatile double. If the in-process ledger must remain, mark the response as degraded.
- **Verify:** A test with a storage backend whose `query_cypher` returns Err: `audit_range` must return Err, not a silently truncated list.

**IN13 — submit_stream drops rows that carry no body and ends the ack stream silently on any inbound error, so acks need not match submissions**  
*correctness* · `crates/exocortex-ingest/src/service.rs:658` (also: crates/exocortex-ingest/src/service.rs:659, crates/exocortex-ingest/src/service.rs:662) · §18.7

- **Current:** The fan-in loop is `while let Some(Ok(one)) = inbound.next().await` (service.rs:658): an `Err` from the inbound stream terminates the loop and the spawned task, closing `tx` and ending the response stream with no status. Inside, `if let Some(b) = one.body.map(...)` (service.rs:659-661) drops a `SubmitOne` with `body: None` without emitting any ack, and the `let _ = tx.send(...)` at 662 discards send failures.
- **Fails when:** A producer streams three batches; the second arrives with an empty `body` oneof (an older or buggy adapter build). The server acks batches 1 and 3 and never mentions 2. The producer, matching acks positionally or counting them, either mis-attributes batch 3's ack to batch 2 or hangs waiting for a third ack that never comes — and no rejection row or gRPC status ever records that a submission was discarded. A mid-stream decode error produces the same outcome for every batch after it.
- **Fix:** Emit an ack (RejectCode::Unknown with a detail) for a body-less SubmitOne, and terminate the response stream with an explicit `Status` when the inbound stream errors, so every submitted row is accounted for in exactly one ack.
- **Verify:** A streaming test that sends a `SubmitOne { body: None }` between two valid batches and asserts three acks come back. No test currently drives submit_stream with a malformed row.

### Client, adapter SDK, and worker

**CL1 — Offline end_session silently discards the harness-supplied tags; the online path keeps them**  
*correctness* · `crates/exocortex-client/src/mcp.rs:304` (also: crates/exocortex-client/src/tools/end_session.rs:136, crates/exocortex-client/src/tools/end_session.rs:43, crates/exocortex-kernel/src/draft.rs:11) · section 4 byte-identical semantics between online (gRPC ingest) and offline (client WAL) write paths

- **Current:** `MemoryDraftInput` carries `tags` (end_session.rs:42-43). The online path forwards them to the wire draft (`tags: m.tags`, end_session.rs:136). The offline path builds `exocortex_kernel::MemoryDraft` (mcp.rs:304-331) which has no `tags` field at all (kernel/src/draft.rs:11-29) and never reads `m.tags` — grep for `tags` in mcp.rs returns only doc comments. The tags are dropped before the WAL append at mcp.rs:368, so they are gone from the durable record, not merely unstamped.
- **Fails when:** Call `exocortex.end_session` with `{"draft_key":"a","memory_type":"Problem","title":"Flaky test","visibility":"org","tags":["ci"]}` while offline (this is literally the payload in crates/exocortex-client/tests/stdio_smoke.rs:160). The WAL entry contains no `ci` tag. When the drain lands (W1) it can only rebuild a wire batch without tags, so the memory is permanently untaggable — and `search_memories`, which matches over "titles and tags" (mcp.rs:73, cache search_arena), can never find it by tag. The identical call online persists the tag.
- **Fix:** Either add `tags` to `exocortex_kernel::MemoryDraft` and stamp it on the offline path, or reject non-empty `tags` on the offline path instead of silently dropping them. A shared draft-normalisation function used by both paths is the real fix.
- **Verify:** A seam test that runs the same `EndSessionArgs` through `EndSessionTool::handle` (capturing the wire batch) and through `end_session_offline` (reading the WAL entry back) and asserts the two carry the same tag set. It fails today: online = ["ci"], offline = absent.
- **Verifier note:** the offline MemoryDraft literal begins at crates/exocortex-client/src/mcp.rs:305 (line 304 is `let id = MemoryId::new_v7();`)

**CL4 — --auth-token is parsed and never used anywhere, so the client talks to the backend unauthenticated**  
*security* · `crates/exocortex-client/src/main.rs:28` (also: crates/exocortex-client/src/tools/end_session.rs:184)

- **Current:** `Args::auth_token` is declared at main.rs:26-28 and documented as 'Bearer token for the backend'. `grep -rn auth_token crates/` returns exactly one hit — that declaration. The gRPC channel built at main.rs:161-163 attaches no interceptor and `EndSessionTool` (end_session.rs:84-97) has no token field, so neither `register_source` nor `submit` ever carries a credential.
- **Fails when:** An operator wires `exocortex-mcp-client --backend https://... --auth-token $TOKEN` into a harness config believing the session-wrapup traffic is authenticated. Every RegisterSource/Submit goes out with no Authorization metadata. If the backend enforces auth the writes fail with `Unauthorized` for a reason the flag appears to have addressed; if it does not, the deployment is silently open.
- **Fix:** Either attach the token as an `authorization: Bearer` metadata interceptor on the channel and thread it through `EndSessionTool`, or remove the flag so the gap is visible.
- **Verify:** A test that starts a tonic service asserting on request metadata, runs the client with `--auth-token t`, and checks the submit carried `authorization: Bearer t`.
- **Verifier note:** the concrete failure ('backend enforces auth, writes fail') is not reachable against the shipped server — gRPC ingest has no bearer check; the defect is the inert flag itself

**CL6 — The R-M7 read stamp's local_lsn is hardwired to 0 — nothing ever advances the cache's WAL frontier, so offline writes are invisible to read-your-writes**  
*correctness* · `crates/exocortex-client/src/mcp.rs:167` (also: crates/exocortex-cache/src/lib.rs:107, crates/exocortex-cache/src/lib.rs:617, crates/exocortex-client/src/wal.rs:129, crates/exocortex-client/src/mcp.rs:377) · R-M7

- **Current:** `SearchMemoriesOutput.snapshot_version.local_lsn` is documented as 'Local WAL frontier' (mcp.rs:109-110) and is read from `CacheVersion::local_lsn` (mcp.rs:167), which is `GraphSnapshot::last_local_lsn` (cache/src/lib.rs:617). `grep -rn last_local_lsn crates/` yields four hits: the field declaration (48), the `= 0` initialiser (107), the read (617), and a copy-through (712). There is no writer anywhere in the workspace. Meanwhile `Wal::append_batch` assigns real monotonic local LSNs (wal.rs:104-129) and `end_session_offline` hands them back to the harness as `local_lsns` (mcp.rs:377).
- **Fails when:** Harness calls `end_session` offline, gets `{"local_lsns":[7],"sync_pending":true}`, then polls `search_memories` until `snapshot_version.local_lsn >= 7` to read its own write — the documented purpose of the stamp. `local_lsn` is 0 on every call, forever. Worse, the fallback at mcp.rs:159-163 also synthesises `{local_lsn: 0, backend_lsn: 0}` when the org has no snapshot at all, so an uninitialised cache and a live one are indistinguishable to the caller.
- **Fix:** Have the WAL append publish its assigned LSN into the snapshot the cache serves (or have the client track it alongside the cache and stamp it in mcp.rs), and return an explicit error/absent stamp rather than a fabricated `{0,0}` when `version()` is `None`.
- **Verify:** A test that appends to the WAL via `end_session_offline`, then asserts `search_memories`'s `snapshot_version.local_lsn` equals the returned `local_lsns` max. It fails today with 0 vs 7.

**CL7 — search_memories reports memory_type as the fake string "type:<u8>" while get_memory reports the raw u8 for the same field**  
*correctness* · `crates/exocortex-client/src/mcp.rs:152` (also: crates/exocortex-client/src/mcp.rs:100, crates/exocortex-client/src/mcp.rs:188)

- **Current:** `ScoredMemory.memory_type` is documented as 'Memory type name in the effective ontology' (mcp.rs:100-101) but is filled with `format!("type:{}", m.memory_type)` (mcp.rs:152), i.e. the literal text `type:7` for the interned u8. `get_memory`, over the identical `Memory`, declares `memory_type: u8` (mcp.rs:188) and emits the bare number (mcp.rs:193). The client does hold an `Ontology` (mcp.rs:36) that can resolve the label — `end_session_offline` uses `ontology.memory_type_id` for the inverse direction at mcp.rs:285.
- **Fails when:** A harness reads `memory_type` from a `search_memories` hit and feeds it back into `end_session` (the natural round trip: find a related memory, record a follow-up of the same type). `end_session_offline` calls `ontology.memory_type_id("type:7")`, which is not a registered label, and the write is rejected with `unknown-memory-type`. A harness that instead correlates search results with `get_memory` results compares `"type:7"` against `7` and never matches.
- **Fix:** Resolve the label through the ontology in both Functions and emit the same representation from each — a shared `fn type_label(&self, id: u8) -> String`.
- **Verify:** A test asserting `search_memories(...).memories[0].memory_type` is a registered ontology label and equals the `memory_type` `get_memory` reports for the same id.

## 6. Write-path parity defects (W1–W7, rev 1)

Unchanged from rev 1 and reproduced in full in the sections below, because
`agent-instructions-prd.md` rev 3 depends on W1–W4 and W6 by id. In brief:

- **W1** — the offline WAL is never drained. `Wal::mark_synced`/`mark_failed`
  have zero non-test callers; `main.rs:141-151` opens and attaches the WAL and
  wires no flush. Writes are acked `sync_pending: true` and never sync.
- **W2** — three divergent write-path validators; the offline one validates
  nothing but type-name resolution and visibility-label spelling.
- **W3** — the online path builds `MemoryContext` with `session_id: None`,
  `project_id: None`, `user_id: None` while the offline path stamps all three.
- **W4** — `RejectionSummary` drops the wire `RejectRow.detail`.
- **W5** — every `RelationshipDraft` is written at `Project` visibility.
- **W6** — `COMPUTED_ONLY_KIND` is a `&str` literal in the ingest crate.
- **W7** — duplicate-batch dedup is in-memory only.

Full write-ups follow.

### 6.1 — W1 — The offline WAL is a write-only sink

**Severity:** data loss. Silent, unbounded, and reported to the caller as success.

**Current behaviour.** `crates/exocortex-client/src/mcp.rs:270` (`end_session_offline`)
resolves drafts, assigns ids, appends to the sled WAL, and returns
`{ local_lsns, sync_pending: true }`. The `sync_pending: true` is a promise that
something will later sync.

Nothing does. `Wal::mark_synced` (`wal.rs:155`) and `Wal::mark_failed`
(`wal.rs:160`) have **zero non-test callers** in the workspace — the only
references outside `src/wal.rs` are in `crates/exocortex-client/tests/wal_roundtrip.rs`.
`main.rs:141` opens the WAL, `main.rs:151` attaches it via `with_offline_wal`,
and no drain task, flush-on-reconnect, or replay path is ever constructed.
`crates/exocortex-client/src/sync.rs` — despite the name — is the SSE
change-feed *read* consumer for cache invalidation, not a WAL flush.

Every entry stays `WalState::Pending` forever, until the 90%-full warning
(`near_full`, `wal.rs:139`) fires and then `WalError::Full` starts rejecting
new writes at `wal.rs:124`.

**Failure scenario.** A developer runs `exocortex-mcp-client` without
`--backend`, or with a backend that is briefly unreachable at startup. Their
agent follows the playbook perfectly for three weeks. Every `end_session`
returns success with a local LSN. The graph is empty. At some point the WAL
crosses its byte budget and writes begin failing with `wal-full`, which is the
first and only signal the user receives that three weeks of memory went
nowhere.

**Relationship to backlog R2.** `docs/master-plan.prd` R2 records "WAL replay
must rebuild+sign" as accepted v1 residue. That item describes a *correctness
requirement on* the replay path (drafts must be re-signed at replay, not
replayed as stale signed bytes). It reads as though a replay path exists and
needs hardening. It does not exist. R2 should be restated as a subclause of W1.

**Fix.** Implement the drain:
1. A `Wal::drain_pending()` iterator yielding `Pending` entries in LSN order.
2. A flush task in `main.rs`, run on startup and on backend-reconnect, that
   rebuilds each entry into an `IngestBatch`, signs it fresh via
   `exocortex_wire::signing::prepare_batch` (never replaying stored signed
   bytes — R2's requirement), submits, and calls `mark_synced` /
   `mark_failed` per the `classify` disposition
   (`exocortex-adapter-sdk/src/classify.rs:29` is the correct triage table;
   the client should not grow a second one).
3. `Failed` entries are terminal and must be surfaced — see D5's
   `--tail-audit` in the agent PRD, which is the natural reporting surface.
4. Batch ids must be stable across replay attempts so `DUPLICATE_BATCH`
   dedupes a re-submitted entry instead of double-committing it. Store the
   `batch_id` on the WAL entry at append time.

**Verification.** Integration test: append two entries offline, start a
backend, run the flush, assert both entries reach `Synced` with backend LSNs
and that the ingest service committed exactly two batches. Second test: flush
against a backend returning `UNAUTHORIZED`, assert entries land `Failed` and
are not retried. Third test: flush the same entry twice, assert the second
attempt acks `DUPLICATE_BATCH` and commits nothing new.


### 6.2 — W2 — Three write-path validators, three different rulebooks

**Severity:** correctness. Identical input produces different verdicts
depending on transport.

**Current behaviour.**

| Path | Location | Title bounds | Content | Type triples | Visibility |
|---|---|---|---|---|---|
| Kernel | `exocortex-kernel/src/validator.rs:19` | `KernelError::TitleBounds` | checked | **from-side only** (`matches_triple` defers the to-side, `validator.rs:66-72`) | no-widening |
| Online ingest | `exocortex-ingest/src/service.rs:201` | rejects as `RejectCode::Unknown` | checked | full from+to (`service.rs:388-397`) | no-widening |
| Offline WAL | `exocortex-client/src/mcp.rs:270` | **none** | **none** | **none** | label parse only |

The offline path validates exactly two things: that the memory type name
resolves in the ontology (`mcp.rs:287`) and that the visibility label is one of
four strings (`mcp.rs:293`). Title length, content emptiness, and every type
triple go unchecked.

**Failure scenarios.**

1. *Divergent verdict.* `Fix —Fixes→ Command` is accepted by the offline path
   (no triple check) and rejected online with `INVALID_TYPE_TRIPLE`
   (`service.rs:397`). The same batch succeeds or fails based on whether the
   client had `--backend` configured. This is a direct `§4` violation.
2. *Deferred rejection.* An agent writes a 10,000-character title offline. The
   WAL accepts it and reports success. Once W1 is fixed and the drain runs,
   the backend rejects it as `Unknown` — hours later, in a background task,
   with no agent in the loop to correct it. The playbook's "resubmit in the
   same turn" instruction is unreachable by construction.
3. *Half-checked triples online.* The kernel's `matches_triple` returns `true`
   for the to-side when `to` is `None` ("deferred to the peer draft"), but no
   peer-draft pass exists in the kernel. Any caller relying on
   `kernel::validate_draft` alone gets from-side-only enforcement.

**Fix.** This is the agent PRD's D2a, hoisted here because it is a live bug
rather than a feature prerequisite:

1. `exocortex-kernel` owns two complete functions: `validate_draft` (all field
   bounds — title, content, summary, metadata — plus no-widening) and
   `validate_triple(onto, from_type, kind, to_type)` with both sides required.
2. `exocortex-ingest::validate_memory` / `validate_relationship` call them and
   map `KernelError → RejectCode` through one shared mapping.
3. `end_session_offline` calls them and maps `KernelError` → the client's
   structured JSON error.
4. The `KernelError → RejectCode` mapping is exhaustive and compile-checked, in
   the same style as `adapter-sdk/classify.rs`.

**Verification.** A golden-fixture suite: one table of batches (valid, and one
per violation class) run through both offline-validate and ingest-validate,
asserting identical verdicts row for row. Promote to a gate —
`cargo xtask write-path-parity` — so a fourth path cannot appear without
joining the table. `Fix —Fixes→ Command` is a required row.


### 6.3 — W3 — Online ingest silently drops `session_id`

**Severity:** correctness. Context loss that is invisible at write time.

**Current behaviour.** `crates/exocortex-ingest/src/service.rs:303` constructs
`MemoryContext` with a literal `session_id: None`. The offline path stamps it
at `crates/exocortex-client/src/mcp.rs:316`. The session id survives an online
write only as a substring of `source_uri = session://<id>`
(`end_session.rs:159`), which nothing parses.

Note also that the online path drops `project_id`, `user_id`, and every other
context field in the same struct literal — the offline path populates
`project_id` and `user_id`. `session_id` is called out here because D6 depends
on it, but the fix should cover the whole struct.

**Failure scenario.** Reasoning rule D6 (`session_cohort`) is written against
session grouping. Any query, rule, or consolidation pass filtering on
`context.session_id` sees offline-written memories and is blind to
online-written ones — the same conversation, split by transport.

**Fix.** `IngestBatch` carries no first-class session field, so either:
(a) parse the id from `source_uri` when `source_flavor = "session"`, or
(b) add an optional `MemoryContext` submessage to `MemoryDraft` (proto3
additive) and let producers populate it.

(b) is the better shape and generalizes past coding agents — a docs adapter has
a run id, not a session id — but (a) is sufficient for parity today and
requires no wire change. Recommend (a) now, (b) when the second producer lands.
Whichever is chosen, the stamping must move into shared commit-shaping code so
online and offline cannot drift again.

**Verification.** Golden-fixture row in the W2 parity suite: submit the same
batch through both paths, assert the committed `MemoryContext` is field-for-field
identical.


### 6.4 — W4 — The backend explains the rejection; the client deletes the explanation

**Severity:** usability. Turns a self-correctable error into an opaque one.

**Current behaviour.** Wire `RejectRow` carries `{ draft_key, code, detail }`
(`proto/ingest.proto:150-154`). The client's `RejectionSummary`
(`crates/exocortex-client/src/tools/end_session.rs:73-79`) has two fields:
`draft_key` and `code`. `detail` is dropped in the mapping.

**Failure scenario.** An agent submits an edge with a bad triple and receives
`{ draft_key: "m2", code: "InvalidTypeTriple" }`. The backend knew and sent
which kind, which from-type, and which to-type were involved. The agent gets a
category name and has to guess. Given a 48-kind catalogue, guessing is
expensive and frequently wrong.

**Fix.** Add `detail: String` to `RejectionSummary` and populate it verbatim
from `RejectRow.detail`. Two lines. Highest value-per-byte fix in this
document.

**Verification.** Unit test asserting a rejecting submit produces a
`RejectionSummary` whose `detail` equals the wire row's.


### 6.5 — W5 — Every relationship is written at `Project` visibility

**Severity:** correctness. Produces orphan nodes at org read scope.

**Current behaviour.** `crates/exocortex-client/src/tools/end_session.rs:150`
sets `visibility: 1` on every `RelationshipDraft`, with the comment
"Project; ≤ the registered ceiling." The comment is true and irrelevant: the
edge visibility ignores the visibility of the memories it connects.

**Failure scenario.** An agent writes a `Technology` memory at `org` visibility
— exactly what the playbook instructs for cross-project knowledge — with a
`Uses` edge to a `Command` memory. The memories are org-visible; the edge is
project-visible. A reader at org scope in a different project sees two
disconnected nodes. The edge, which is the entire value proposition of a graph
over a list, is invisible to precisely the audience the `org` label was chosen
to reach.

**Fix.** Derive edge visibility from its endpoints — the narrower of the two
endpoint visibilities is the safe default (an edge must not be more visible
than either thing it connects). Alternatively accept an optional per-edge
visibility on `EdgeHintInput` and default to the derived value. The derived
default should be computed in the shared validation code from W2 so both paths
agree.

**Verification.** Unit test: two `org` memories with an edge produce an `org`
edge; an `org` memory edged to a `private` memory produces a `private` edge.


### 6.6 — W6 — An ontology fact lives in the ingest crate as a string literal

**Severity:** layering. Makes the ontology unreadable as a source of truth.

**Current behaviour.** `crates/exocortex-ingest/src/service.rs:33`:

```rust
const COMPUTED_ONLY_KIND: &str = "SimilarTo";
```

compared by string at `service.rs:378` to enforce R-T14. `exocortex-pack-dev-v1`
declares 48 kinds and carries no marker distinguishing computed-only ones —
`grep -rn "computed" crates/exocortex-pack-dev-v1/src/` returns nothing.

**Failure scenario.** Any consumer reading the ontology to learn what it may
write — an adapter author, a UI, or the agent PRD's `xtask gen-playbook`
generator — sees 48 legal kinds. 47 is the truth. The agent PRD's generated
kind catalogue would faithfully generate a table instructing agents to use
`SimilarTo`, and every such edge would be rejected with
`COMPUTED_KIND_REJECTED`. The CI drift gate would pass, because the generated
table matches its source; the source is the thing that's wrong.

Adding a second computed-only kind means editing a string constant in a service
crate and hoping every other consumer notices.

**Fix.** Add a `computed_only: bool` (or a `provenance_class` enum) to the kind
definition in the `pack!` macro and the dev-v1 pack. `exocortex-ingest` reads
the flag through the ontology instead of comparing strings. Note this changes
the pack definition — confirm whether it perturbs the ontology fingerprint
(`d8bcd004…4e8c`); if it does, that is an intended ontology change and the gate
value updates in the same commit with the reason recorded.

**Verification.** Assert `ontology.kind_by_name("SimilarTo").computed_only` is
true and that no other dev-v1 kind sets it. Existing R-T14 rejection tests must
pass unchanged.


### 6.7 — W7 — Duplicate-batch dedup does not survive a restart

**Severity:** idempotency. Pre-existing; recorded here for its interaction with
structural-row minting.

**Current behaviour.** Recorded in `docs/master-plan.prd` backlog R2:
"per-server-restart duplicate replays (in-memory dedup)." The `(producer_id,
batch_id)` idempotency guarantee holds only within one server process lifetime.

**Failure scenario.** Any design that leans on batch idempotency to prevent
duplicate structural rows breaks across a restart. This directly affects the
agent PRD's D6, whose step 4 argues that "batch idempotency prevents duplicate
`InSession` edges." Post-restart, a replayed batch re-commits and re-mints
those edges. The `Conversation` node survives (deterministic id); its edges
multiply.

**Fix.** Either persist the `(producer_id, batch_id)` dedup set, or make
structural-edge minting idempotent on its own via a deterministic relationship
id derived from `(from_id, kind, to_id)`. The second is cheaper and is the
right answer regardless, since it also protects against a producer legitimately
re-asserting the same edge in a different batch.

**Verification.** Submit a batch, restart the service, resubmit the identical
batch, assert edge count is unchanged.

---

## 7. Refuted candidates

Seven candidates were reported and killed during verification. They are recorded
so the same ground is not re-covered — each entry says what the verifier read
and why the finding does not hold. **These are not open items.**

**OntologyFingerprint cannot detect a rule change: it hashes only head-predicate names, and those names are disjoint from the rule ids stamped into Provenance::Derived**  
*Group:* kernel-pack  
*Cited:* `crates/exocortex-kernel/src/fingerprint.rs:16`  
*Why it was killed:* The mechanical facts are right (fingerprint.rs:16-28 bincodes PackDef; PackDef carries only `rule_ids`, pack.rs:28; harvested ids are head predicates, pack.rs:103-118; provenance ids are D1..D6/R1..R9, engine.rs:296-306) — but the conclusion is not a defect. R-T21 in the PRD (line 3190) enumerates the fingerprint's inputs exactly: "all types, all RelKindId metadata, the type-triple table, and the kernel/pack versions". Rule bodies are deliberately not among them, and pack.rs:24-26 states the design in the source: "Rules are compiled into the reasoning crate at build time, not shipped in PackDef. PackDef only carries the rule-id list for fingerprinting." So the fingerprint is behaving to spec. The namespace-mismatch half has no consequence either: I grepped `rule_ids` across the workspace — the only consumers are OntologyFingerprint::compute, a count assertion (pack-dev-v1/tests/loads_correctly.rs:72), and an equality assertion in kernel/tests/pack_registration.rs:67. Nothing resolves a provenance rule_id through rule_ids, so the disjoint namespaces produce no wrong result. Both failure scenarios require editing source code, not supplying an input; neither names a shipped wrong outcome. Also note the reasoning rules live outside PackDef entirely, so no PackDef-based fingerprint could ever cover them — the proposed fix would not close the stated hole.

**RelationshipId::derive's snapshot parameter is dead: every caller passes None, including ingest, which has the snapshot_id in hand**  
*Group:* kernel-pack  
*Cited:* `crates/exocortex-kernel/src/ids.rs:68`  
*Why it was killed:* The load-bearing claim is factually wrong at one of its own citations. I grepped every `RelationshipId::derive` call in crates/: crates/exocortex-ops/src/operations.rs:421 passes `Some(&input.discovery_id)` — it reads `RelationshipId::derive(from, RelKindId(0), to, Some(&input.discovery_id))` inside AcceptDiscoveryOp::handle. So the parameter is not dead and is exercised by a shipped write path; the finding lists that exact line as a `None` caller. The remaining sub-claim (ingest service.rs:407 passes None) is accurately cited, but nothing in the spec requires snapshot-scoped edge identity — I grepped R-T18a (PRD lines 3161, 3115, 4954) and it governs MemoryId derivation from ExternalKey, saying nothing about folding snapshot_id into RelationshipId. The ids.rs:65-67 doc says only that re-derivation within the same snapshot is idempotent, which passing None satisfies (it is idempotent across all snapshots). Re-asserting the same (from, kind, to) in S2 with new strength upserting the same row is ordinary edge-upsert semantics, not data loss; the finding does not show a spec rule it breaks.

**A single unverifiable SSE envelope wedges the client's LSN gate into a permanent resubscribe loop; the cache silently stops updating**  
*Group:* wire-signing  
*Cited:* `crates/exocortex-client/src/sync.rs:308`  
*Why it was killed:* Line facts are right (sse.rs:106 and :116 resign only when verify passes, then yield unconditionally; sync.rs:308 drops without touching the gate; sync.rs:311-313 sets next_lsn=missing, and replay_since(node.rs:99-113) re-serves anything with lsn>since, so the identical envelope comes back). But I could not reach the trigger. The only producer of envelopes in the hub is ClusterNode::envelope (node.rs:173-188), which signs with self.hmac_key, and verify_hmac (node.rs:190) checks the same key, so every envelope on the hub verifies; publish_envelope's only non-test caller is run() (node.rs:162). An envelope with a non-verifying HMAC therefore requires the inbound peer path that Finding 5 correctly says does not exist. Second, the whole loop is unreachable in shipped binaries: grep -rn run_sse_sync over crates/ returns only its definition (sync.rs:259) and tests — no binary wires the SSE subscriber at all, so 'the client serves stale memories indefinitely' cannot happen today, and the missing client-side subscribe is already backlog R1. The latent design weakness (a decode failure leaves a hole the gate can never close) is real, but as written the failure scenario is not reachable and it is contingent on Finding 5.

**Three independent HMAC implementations of InvalidationEnvelope live outside exocortex_wire::signing, with no shared code and no cross-crate agreement test**  
*Group:* wire-signing  
*Cited:* `crates/exocortex-cluster/src/node.rs:183`  
*Why it was killed:* The code facts check out (node.rs:182-186 sign, node.rs:190-206 verify, server/sse.rs:131-137 resign, client/sync.rs:86-109 verify, and wire/signing.rs:1-22 owns only the batch helpers while its doc at :14-15 claims sole ownership of Hmac<Sha256>). But the load-bearing claim — 'nothing asserts the four agree' / 'no compiler or test coupling exists to catch the divergence' — is false. crates/exocortex-client/tests/sync.rs stands up the REAL server SSE router over a REAL ClusterNode and drives the REAL client verify path end to end: sse_router at :162-170 and :240-248, cluster.envelope+publish_envelope at :204, :285, :424, client run_sse_sync asserting the cache observed the row at :215-218 and :296-299, including the per-client derived-key path (:263-268). crates/exocortex-server/tests/e2e_chain.rs:120 does the same with derive_client_sse_key. Any one-sided change to the covered region breaks those tests. This is a layering/DRY observation, not a defect with a reachable wrong outcome.

**Dreams rollback never restores a merged memory — it re-closes the row the merge already closed**  
*Group:* ingest-ops-dreams  
*Cited:* `crates/exocortex-dreams/src/lib.rs:538`  
*Why it was killed:* I read lib.rs:533-541 (rollback), 362-386 (merge) and 284-303 (consolidate_under). The mechanics the finding describes are accurate: merge sets valid_until/invalidated_by on the newer row and pushes it into res.merged; rollback only calls delete_memory_fenced on those ids. But the finding's invariant is wrong. docs/prd/exocortex-core-prd.md:4525 defines §12.5 step 8 verbatim: 'Implement rollback: bi-temporal, never destructive — close valid_until = now() on every row the cycle wrote'. The spec asks rollback to CLOSE rows, not to reopen them; R-Mcr3 (prd:4209) is a warn-and-flag rule with an optional operator rollback flag, not a guarantee that a degrading cycle is undone. crates/exocortex-dreams/tests/dreams.rs:189-195 asserts exactly the shipped behaviour (merged rows stay closed after rollback). So the headline 'data loss / memory the operator never asked to delete' is a misreading: the row was closed by the merge, which is the designed bi-temporal outcome. One narrower sub-claim does survive and is the only part worth carrying forward.

**end_session discards the RegisterSource response and hardcodes ceiling 3, so the R-I3 ceiling equality the SDK enforces is never checked on the client write path**  
*Group:* client-sdk-worker  
*Cited:* `crates/exocortex-client/src/tools/end_session.rs:185`  
*Why it was killed:* The cites are accurate (end_session.rs:185 is `let _ = client`, .await at 193; ceiling: 3 at end_session.rs:164 and again in the register request at 190; adapter-sdk/src/lib.rs:252-268 does compare and raise CeilingMismatch). But I read the server side: ingest/src/service.rs:691-710 register_source unconditionally does `sources.put((org, uri, producer), ceiling)` with the *requested* ceiling and echoes it back. It never preserves a prior registration and never clamps. So the described trigger — 'the backend has this producer registered at ceiling 2, RegisterSource returns 2' — cannot happen against the shipped server: the client's own register call rewrites the entry to 3 immediately before submit, and the equality check at service.rs:531-546 therefore always passes. The residual real bit is that a RegisterSource transport error is swallowed at line 185, but the subsequent submit surfaces its own UnknownSource/transport error with the same cause, so no information a caller needs is actually lost. This is a latent parity gap against a hypothetical different server, not a defect with a producible wrong outcome, and the finding's own failure scenario is refuted by service.rs:701-709.

**Wal::pending_count silently skips entries it cannot decode, so a codec-version bump or a corrupt record reports zero pending while the bytes still count against the budget**  
*Group:* client-sdk-worker  
*Cited:* `crates/exocortex-client/src/wal.rs:149`  
*Why it was killed:* The code reads as described — wal.rs:149 is `.filter_map(|v| decode_entry(&v).ok())` and wal.rs:196-197 rejects any first byte != WAL_CODEC_VERSION — but the failure scenario is not reachable. `grep -rn pending_count crates/` returns only the definition at wal.rs:144 and three assertions inside wal.rs's own `mod tests` (259, 261, 263). There is no production caller anywhere, and main.rs:142 (which the finding cites as the caller) is `if wal.near_full()`, which goes through used_bytes and never touches decode_entry. So no operator, log line, or drain currently consults pending_count; nothing observes the zero. Charging undecodable records against used_bytes is also correct — the bytes really are occupying the budget. What is left is an unused function with a lossy filter, which is the same dead-WAL-API family already written up as W1 (mark_synced/mark_failed have zero non-test callers), and the codec-bump half additionally requires a version bump that has not happened (WAL_CODEC_VERSION is still 1 at wal.rs:184).

## 8. Fix order

Grouped by what has to be true before the next thing is safe, not by severity
alone. Within a phase, items are independent.

**Phase A — close the network surface.** CS1, WS1, WS2, CL3, IN2. Five defects
that between them mean the change feed, the producer registry, the visibility
ceiling, and `promote_visibility` are all effectively unauthenticated. These are
the only findings here that a third party can reach without already being inside
the system. Nothing else should ship before them.

**Phase B — stop losing committed data.** ST1, ST2, ST5, CR1, CR2, W1. Soft
deletes that readers cannot see, a stream that never terminates after one, a
non-transactional batch commit, a cache that accumulates stale versions and
resurrects deleted ones, and a WAL that never drains.

**Phase C — make the double honest.** ST3, ST4, ST6, ST7, ST8, ST9, ST10, plus
the conformance suite from §2.1. Until this lands, every storage-backed test in
the workspace is weaker than it appears, which means every fix in phases A, B,
and D is being verified against a database that behaves differently from the
one in production. Arguably this belongs first; it is third because the security
and data-loss defects are live now and this one makes future work trustworthy.

**Phase D — reconnect the dead enforcement.** CS2, CS4, IN5, CR8, CR9, KP1,
IN12, W2. Controls that exist and do not run. Each needs a decision — wire it up
or delete it — and deleting is a legitimate answer for anything the v1 design no
longer wants.

**Phase E — parity and correctness.** W3, W5, W6, IN11, KP3, KP4, WS4, WS5, and
the remaining §5 entries.

**Phase F — resource bounds.** CR6, CR7, CR10, CS5. Unbounded growth that has
not bitten yet because no deployment is large enough.

W4 is two lines and blocks nothing; do it whenever.

## 9. Proposed gates

Each closes a whole class rather than one defect. In `AGENTS.md` gate style:

```sh
cargo xtask storage-conformance   # §2.1 — one suite, double and live Falkor, identical results
cargo xtask write-path-parity     # W2  — offline-validate and ingest-validate agree
cargo xtask dead-enforcement      # §2.2 — invariant/security fns with no non-test caller
cargo xtask auth-coverage         # §2.3 — every network endpoint rejects an unauthenticated call
cargo xtask artifact-equivalence  # §2.4 — pack rules vs engine; MCP schema+result vs HTTP
```

The existing gates are good at what they check. The gap is that all of them
check one crate, and every defect in this document lives between two.

## 10. Plan lifecycle

Per `AGENTS.md`, these enter `docs/master-plan.prd` Backlog with this document
as their source and close there with commit evidence. W1–W7 are already listed;
rev 2's 63 additions join them. Backlog item R2 is superseded in part — its
WAL-replay clause folds into W1, its duplicate-replay clause into W7.

Given the volume, the plan carries one row per subsystem group with a pointer
into this document, rather than 63 rows, except for Phase A which is itemised.
