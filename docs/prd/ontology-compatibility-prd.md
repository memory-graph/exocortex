# Exocortex — Ontology Compatibility PRD

**Author:** Gregory Dickson
**Status:** Draft
**Created:** 2026-08-27
**Repo:** [memory-graph/exocortex](https://github.com/memory-graph/exocortex)

---

## 0. Summary

Two Exocortex components — a node and its database, two cluster peers, an
adapter and an ingest server, a backup file and the binary reading it — must
agree on the ontology before they exchange anything. Today that agreement is
decided by one mechanism: byte equality of a SHA-256 over every registered
`PackDef`. It is enforced at six boundaries, fail-closed, with no negotiation
and no override.

The guarantee is correct and worth keeping. The mechanism is blunter than the
guarantee requires, in four specific ways that cost real capability: rolling
upgrades are impossible, purely additive ontology changes are punished exactly
as hard as breaking ones, a pack patch-version bump invalidates the fleet, and
a backup becomes unrestorable the moment the pack set moves.

This PRD keeps the guarantee and narrows the mechanism. It ships a two-level
fingerprint (one hash that gates, one that only reports), a per-boundary
compatibility policy replacing the single global equality test, superset
acceptance so additive change is a non-event, and — the part that makes the
rest shippable — a migration for the fingerprint already pinned inside every
existing graph.

**This is not a rewrite of the ontology identity scheme.** §5 explains what we
are deliberately not touching and why.

---

## 1. Problem

### 1.1 What is hashed

`OntologyFingerprint::compute` (`crates/exocortex-kernel/src/fingerprint.rs:16`)
is SHA-256 over `bincode::serialize(PackDef)` for every registered pack, in
name-sorted order, length-prefixed, under a kernel domain separator. It hashes
the **whole struct**, not a curated identity.

`PackDef` (`crates/exocortex-kernel/src/pack.rs:9`) carries:

```rust
name, version, kernel_min,                    // release metadata
memory_type_names, entity_type_names,         // positional -> u8 id
kinds, type_triples, rule_ids                 // the rulebook
```

Only the second and third lines describe what the ontology *means*. The first
line is release metadata, and it is inside the gate: **shipping a pack patch
release with no semantic change at all produces a different fingerprint**, with
every consequence in §1.2.

### 1.2 Where it is enforced

Six boundaries, all exact byte equality, all fail-closed:

| Site | Behaviour on mismatch |
|---|---|
| `storage/src/falkor.rs:280` `pin_fingerprint` | **The node refuses to start** against its own graph (R-D5) |
| `ingest/src/service.rs:308` `ontology_matches` | Batch rejected; the producer cannot write |
| `cluster/src/node.rs:254` `admit` | `OntologyMismatch`; peers drop each other's invalidation envelopes |
| `client/src/sync.rs:140` | SSE envelope discarded |
| `adapter-sdk/src/lib.rs:516` | `SdkError::FingerprintMismatch`, fatal |
| `client/src/backup.rs:98` | Restore aborts before touching the WAL |

There is no version negotiation, no compatibility window, and no override at
any of the six.

### 1.3 What that costs

1. **Rolling upgrades are impossible.** Mid-rollout, old and new nodes compute
   different fingerprints and reject each other's invalidations. Neither side
   errors visibly — cache coherence silently degrades for the duration of the
   window. The only safe deployment is a simultaneous fleet restart.

2. **Additive change is punished like breaking change.** Appending a memory
   type remaps no existing id and cannot reinterpret any stored row. It
   produces exactly the same total rejection as renaming a kind.

3. **Release metadata gates data.** `version` and `kernel_min` are in the
   enforced hash (§1.1). This one is closer to a defect than a design choice.

4. **Backups expire when the ontology moves.** `backup.rs:98` hard-fails on
   mismatch — and the check is partly redundant, because `backup.rs:103`
   already revalidates every draft against the current rulebook and aborts the
   import on any rejection. Where revalidation is the real safety net, the
   equality gate is refusing restores that would have been caught anyway. For a
   system whose premise is an append-only compounding asset, "your data is
   hostage to the exact pack set that wrote it" is the wrong durability story.

### 1.4 Why it is this strict — the defence is real

This is not gratuitous. The `pack!` macro emits memory and entity types as
`#[repr(u8)]` enums **whose declaration order is the ontology id**. A pack that
inserts a type in the middle of its list silently reinterprets every stored
memory of every later type — a `Fix` becomes a `Problem`, with no error at any
layer. The fingerprint is the guard against that, and against the subtler case
where two nodes disagree about which triples are valid while both believe they
are healthy.

**The strictness is downstream of the identity scheme, not an independent
choice.** Any proposal that loosens the gate has to say why the positional-id
hazard is still covered. This PRD's answer is §3.1's split: the compatibility
hash covers exactly the name→id mapping that hazard lives in.

---

## 2. Design principles

**C1 — Compatibility is a question per boundary, not one global equality.**
A restore can revalidate every row before accepting it; a cluster peer cannot
revalidate an invalidation envelope. Those two boundaries have different safe
answers and should stop sharing one test.

**C2 — Gate on meaning, report the build.** Exactly one hash decides whether
two components may talk. A second, broader hash is computed, logged, and
surfaced in diagnostics, and never gates anything. Operators keep full
build-level attribution without paying for it in compatibility.

**C3 — Additive change is a non-event.** Appending a memory type, an entity
type, a kind, or a triple must not stop a node, split a cluster, or expire a
backup. If a change cannot reinterpret existing data, it must not be treated as
though it can.

**C4 — The pinned graph is in scope, not after.** Every existing deployment has
a fingerprint persisted in its graph and a node that refuses to start when the
runtime value differs. A change to *what gets hashed* changes the runtime value
for everyone. Migration is a deliverable of this PRD, not a follow-up — without
it, this PRD bricks every existing install.

**C5 — Unproven means incompatible.** Every relaxation here is a rule that
provably preserves meaning. Anything outside a stated rule keeps today's
behaviour: refuse, loudly. No heuristics, no "probably fine," no operator
override flag.

---

## 3. Deliverables

### 3.1 D1 — Two-level fingerprint

**Compatibility fingerprint** — hashes only what changes meaning:

- the ordered `name -> u8` mapping for memory types and entity types (the
  positional-id hazard of §1.4 lives here and nowhere else),
- kind ids with their names and directions, including R-T4 inverse companions,
- type triples,
- rule ids,
- the kernel domain separator.

**Build fingerprint** — hashes the full `PackDef` set as today, including
`version` and `kernel_min`.

Only the compatibility fingerprint is ever compared for admission. The build
fingerprint appears in `--verify`, `/health`, diagnostics, and the audit
ledger, so an operator can still answer "are these two nodes running the same
binary?" — a question worth answering and a bad reason to reject a write.

This subsumes §1.3's third cost: release metadata leaves the gate by
construction.

### 3.2 D2 — Per-boundary compatibility policy

One table, one place in the kernel, consulted by all six sites. Each boundary
declares the strongest rule it can honestly support:

| Boundary | Rule |
|---|---|
| Node ↔ its graph (`pin_fingerprint`) | Compatible-or-superset; the node's ontology must be a superset of the pinned one. Start is allowed; the pinned value is advanced (D4) |
| Ingest ↔ producer | Producer's compatibility fingerprint must be a subset the server recognises — a producer may know less than the server, never more |
| Cluster peer ↔ peer | Exact compatibility-fingerprint equality. An invalidation cannot be revalidated, so this boundary keeps the strict rule |
| SSE subscriber | Same as cluster peer |
| Adapter SDK | Same as ingest, surfaced as a typed, actionable error naming the specific divergence |
| Backup restore | Superset accepted, because every draft is already revalidated against the current rulebook before the WAL is touched (`backup.rs:103`) |

The point of the table is that these are **different questions**. Today they
share an answer by accident of implementation.

### 3.3 D3 — Superset acceptance and rolling upgrade

Define subset/superset over the compatibility fingerprint's inputs: ontology B
is a superset of A when every memory type, entity type, kind, and triple in A
appears in B **with an identical id**, and B adds only new ones at unused ids.

That definition makes the safe deployment order expressible and testable:
upgrade nodes first (superset), then producers. During the window a new node
accepts an old producer's writes; an old node rejects a new producer's, which
is correct and loud rather than silent.

**Acceptance:** a two-node test where node A runs `{dev-v1}` and node B runs
`{dev-v1 + one appended type}`; B accepts A's batches, A rejects B's with a
legible error, and neither drops an invalidation envelope silently.

### 3.4 D4 — Pinned-graph migration

Every existing graph has a fingerprint written into it, and every existing
binary refuses to start when the value differs. Changing the hash changes the
value for every deployment simultaneously.

**What ships:** the persisted fingerprint record gains a scheme tag. A graph
pinned under scheme v1 with today's value is recognised, checked against the
v1-scheme recomputation of the running ontology, and — when compatible —
rewritten in place to the v2 record carrying both fingerprints. A graph whose
v1 value does not match refuses to start exactly as it does now.

**Acceptance:** a fixture graph pinned with `e1f7d17b…ddc9b2` boots against a
post-change binary, is rewritten to a v2 record, and boots again unchanged.
This mirrors the existing storage-schema migration fixture
(`falkor.rs:380`), which already proves startup migration from a downgraded
fixture and is the pattern to copy.

### 3.5 D5 — The defect half, shippable independently

Removing `version` and `kernel_min` from the enforced hash is small, is
arguably a defect fix rather than an evolution, and delivers §1.3's third cost
on its own. It is called out separately so it can ship ahead of D1–D4 if the
larger design needs more time — **but it still moves the fingerprint**, so it
carries D4's migration or it waits for it. There is no version of this that is
free.

---

## 4. Gates, goldens, and the values in prose

The current fingerprint appears in the `xtask fingerprint` gate,
`crates/exocortex-pack-dev-v1/tests/dev_v1_fingerprint.txt`, the master plan's
baselines, `AGENTS.md`'s gate list, and `CLAUDE.md`. All of them move together,
once, in the commit that lands D1 — and they change shape, not just value:
where those documents name one fingerprint they will name two, and only the
compatibility one is the "if this moved you broke something" value.

The `xtask fingerprint` gate gains a second assertion: the compatibility
fingerprint is byte-stable across clean builds **and** unchanged by a
release-metadata-only edit. That second half is the regression test for §1.3's
third cost, and it fails today.

---

## 5. Out of scope

### 5.1 Not in this PRD — sequenced in the master plan

- **Replacing positional `u8` ids** with explicit or name-derived ids. This is
  the root cause of §1.4: with stable ids, most drift becomes detectable
  per-field and the global gate could relax much further. It is also a change
  to the on-disk meaning of every stored row, needs its own migration, and
  would be the largest single change ever made to the kernel. Named here so it
  is a decision rather than an omission; sequenced in the master plan, not
  smuggled into this PRD.

### 5.2 Not doing — design choices

- **An operator override flag.** No `--ignore-fingerprint-mismatch`. The
  failure it would paper over is silent data reinterpretation, and an escape
  hatch would be used exactly when it is least safe (C5).
- **Semantic-version-based compatibility.** "Same major version means
  compatible" is a promise about a human's intent, not a property of the
  ontology. The compatibility hash is derived from the ontology itself.
- **Runtime ontology negotiation.** No handshake in which two components agree
  on a reduced common ontology. Compatible or not; a partial agreement is a
  silent-divergence generator.

---

## 6. Success criteria

Each lands as a row in `docs/acceptance/section-23.tsv` with a runnable
command.

**S1 — Release metadata cannot gate data.** Editing a pack's `version` or
`kernel_min` changes the build fingerprint and leaves the compatibility
fingerprint byte-identical.
*Command:* `cargo xtask fingerprint --verify-metadata-independence`

**S2 — Additive change is a non-event.** Appending a memory type to a pack
leaves every pre-existing name→id mapping intact and produces a superset
verdict against the prior ontology.
*Command:* `cargo test -p exocortex-kernel --test compatibility`

**S3 — Every boundary states its rule.** All six sites consult the D2 policy
table; no site compares fingerprints directly. A new boundary that compares
without declaring a rule fails the gate.
*Command:* `cargo xtask compatibility-policy`

**S4 — Rolling upgrade works and fails correctly.** The D3 two-node test:
superset node accepts subset producer, subset node rejects superset producer
with a legible error, no invalidation is silently dropped.
*Command:* `cargo test -p exocortex-cluster --test rolling_upgrade`

**S5 — Existing graphs survive.** A fixture graph pinned at `e1f7d17b…ddc9b2`
boots against the post-change binary, is rewritten to a v2 record, and boots
again unchanged.
*Command:* `cargo test -p exocortex-storage --features integration --test fingerprint_migration`

**S6 — Restores stop expiring.** A backup written under `{dev-v1}` restores
into a binary running `{dev-v1 + appended type}`, with every draft revalidated;
a backup written under an incompatible ontology still refuses.
*Command:* `cargo test -p exocortex-client --test backup_restore`

---

## 7. Sequence

Ordering is by dependency, not schedule — no dates, durations, or estimates.
This is **Wave 0** in `docs/master-plan.prd`: it precedes Wave 1 because the
palantir-expansion PRD's pack-verb work adds fields to `PackDef` and moves the
fingerprint for every pack including dev-v1. Either that move happens as an
unmanaged fleet-wide event, or this lands first and it becomes a managed one.

| Step | Deliverable | Depends on |
|---|---|---|
| **0a** | D1 two-level fingerprint: compatibility + build hashes in the kernel | — |
| **0b** | D4 pinned-graph migration: scheme-tagged record, v1→v2 rewrite, fixture test | 0a |
| **0c** | D2 per-boundary policy table; all six sites converted to consult it | 0a |
| **0d** | D3 superset definition + rolling-upgrade test | 0c |
| **0e** | D5 metadata out of the enforced hash (folded into 0a's hash definition; separable only if 0a slips) | 0a, 0b |
| **0f** | Gates, goldens, and prose updated in one commit: `xtask fingerprint`, `dev_v1_fingerprint.txt`, master plan baselines, `AGENTS.md`, `CLAUDE.md` | 0a-0e |

The constraining chain is 0a → 0b → 0c → 0d. Step 0f is deliberately last and
deliberately one commit: the fleet-visible value changes exactly once.

---

## 8. Open questions

1. **Does the build fingerprint belong in the audit ledger per write, or only
   in diagnostics?** Per-write is more forensically complete and costs bytes on
   every row. Assumption: diagnostics only; revisit if a real incident wants it.
2. **Should `rule_ids` sit in the compatibility hash or the build hash?** Rule
   bodies compile into the reasoning crate and are not in `PackDef` at all
   (`pack.rs:25`), so the ids are a proxy for behaviour the hash cannot see.
   Assumption: compatibility, because a derived edge appearing or vanishing
   changes what a reader observes.
3. **How does a subset producer discover what it is missing?** Today the error
   names two opaque hashes. Assumption: the typed error carries the specific
   divergent names. Confirm during D2.
4. **Does superset acceptance need a bound?** Nothing stops a node from being a
   superset of an ontology from many versions ago. Assumption: no bound in v1 —
   the id-stability rule is the real constraint and it does not weaken with age.

---

## 9. Relationship to other documents

- **`docs/prd/exocortex-core-prd.md`** — §7.17 defines the fingerprint and R-D5
  the refuse-to-start rule. This PRD refines both; it does not remove either
  guarantee. Any §-reference in code that points at the current single-hash
  behaviour is updated in step 0f.
- **`docs/prd/exocortex-palantir-expansion-prd.md`** — that PRD's §4.1 claimed
  action and function *bodies* could change without moving the fingerprint.
  That is not achievable while the hash covers a bincode of the whole
  `PackDef`. This PRD is what makes a version of that claim true, and the
  palantir PRD's step 1a depends on Wave 0.
- **`docs/master-plan.prd`** — this PRD lands as `OC-PRD`, Wave 0.
- **`docs/prd/backup-restore-prd.md`** — D2's backup row relaxes a gate that
  PRD specified. The all-or-nothing revalidation it requires is unchanged and
  is the reason the relaxation is safe.
