# Bug PRD — external-key identity and snapshot provenance are lossy

| Bug PRD | | Two defects in `exocortex-ingest` that corrupt or discard external-source identity metadata |
|:---|:---|:---|
| **Author**: Gregory Dickson | **Status**: Open **Created**: 2026-08-25 | **Visibility**: Internal |

## Summary

`crates/exocortex-ingest/src/service.rs` mangles `ExternalKey.table_uuid` through
`String::from_utf8_lossy` and hardcodes `ExternalSnapshot.schema_hash` to zeros.
Both fields are `§18.6`-normative inputs that only external-source adapters
populate. `§18.3` ships exactly one producer — `session-wrapup` — and it sends
neither field (`crates/exocortex-client/src/tools/end_session.rs:173`,
`external_key: None`; `snapshot: None`). **Zero rows with an `external_key`
exist in any deployment**, so both defects are currently latent and both fixes
are currently free of migration cost. That stops being true the moment the first
external adapter (Iceberg, D1) commits a batch.

## Defect 1 — `table_uuid` collides under `from_utf8_lossy`

**Severity:** correctness / data integrity. Silent, unrecoverable identity collision.

**Current behaviour.** `§18.6` declares `ExternalKey.table_uuid` as `bytes` with
the comment `// 16 bytes`. Three sites convert those raw bytes to a `String`:

- `crates/exocortex-ingest/src/service.rs:246` — building `ExternalKey.table_uuid` for `Provenance::ExternalSnapshot`
- `crates/exocortex-ingest/src/service.rs:304` — feeding `MemoryId::from_external`
- `crates/exocortex-kernel/src/ids.rs:15` — `from_external` accepts `table_uuid: &str` and hashes `table_uuid.as_bytes()` (`ids.rs:24`)

`String::from_utf8_lossy` replaces every invalid UTF-8 sequence with `U+FFFD`.
A UUID is 16 uniformly random bytes; the overwhelming majority are not valid
UTF-8. Two distinct table UUIDs whose invalid byte runs occupy the same positions
therefore normalize to the **same** replacement-character string, hash to the
same `MemoryId` input, and collide.

**Failure scenario.** An org registers two Iceberg tables in one catalog. Table A
(`uuid = 0x8f3a...`) and Table B (`uuid = 0xc1e7...`) each contain a row with
`logical_pk = "1"` under the same `mapping_version`. Both UUIDs lossy-convert to
a string of replacement characters. `MemoryId::from_external` receives identical
inputs for both, derives one `MemoryId`, and `upsert_batch` silently overwrites
Table A's memory with Table B's content. No reject code fires; the ack reports
success. R-T18a's determinism guarantee holds — it just guarantees the wrong id.

**Fix.** Change `MemoryId::from_external` to take `table_uuid: &[u8]` and hash the
raw bytes. Pass `&k.table_uuid` directly at `service.rs:304`. Store the raw bytes
(or a hex/UUID rendering) on `kernel::ExternalKey` at `service.rs:246` rather than
a lossy string. Optionally reject `table_uuid.len() != 16` with `MISSING_EXTERNAL_KEY`
or a new code, since `§18.6` fixes the width.

**Verification.** Unit test in `crates/exocortex-kernel`: two 16-byte UUIDs that
are both invalid UTF-8 and lossy-normalize identically MUST derive different
`MemoryId`s. This test fails on the current implementation.

## Defect 2 — `ExternalSnapshotInfo.schema_hash` is discarded

**Severity:** feature gap with a silent-success failure mode.

**Current behaviour.** `crates/exocortex-ingest/src/service.rs:240` hardcodes
`schema_hash: [0u8; 32]` when constructing `Provenance::ExternalSnapshot`, while
`snapshot_id` (`service.rs:236-239`) is read from the wire. The wire field
`IngestBatch.snapshot.schema_hash` (`§18.6`, 32 bytes) is never read.

**Failure scenario.** `mintlify-docs-integration-prd.md` §2.2 states that bumping
the frontmatter schema "forces a full re-ingest via `ExternalSnapshotInfo.schema_hash`
change." An adapter bumps its schema hash and re-submits. The kernel stores zeros
both times. Nothing downstream can distinguish schema generations; the documented
re-ingest trigger is a no-op that acks successfully. Every adapter built on the
Mintlify PRD's stated contract inherits this.

**Fix.** Propagate `batch.snapshot.schema_hash` into
`Provenance::ExternalSnapshot.schema_hash`. Reject a `snapshot` whose
`schema_hash.len() != 32` rather than silently truncating or zero-padding.

**Verification.** Integration test in `crates/exocortex-ingest/tests/`: submit a
batch with a non-zero `schema_hash`, read the committed memory back, assert the
stored provenance carries the submitted bytes. Note that
`crates/exocortex-ingest/tests/ingest.rs:222` and `:247` currently pass
`schema_hash: vec![0; 32]`, so no existing test distinguishes pass from fail.

## Why these are filed separately from the adapter-SDK PRD

Both are pre-existing defects in shipped M6 code, not gaps in the SDK being built.
Keeping them separate preserves the master-plan's audit trail: the SDK PRD adds
capability, this PRD closes defects. The Iceberg adapter (D1) depends on both
being closed — an Iceberg adapter is precisely a producer of real 16-byte table
UUIDs and real schema hashes.

## Acceptance criteria

- [ ] `MemoryId::from_external` accepts `&[u8]` for `table_uuid`; no `from_utf8_lossy` remains on any `ExternalKey` path (grep gate).
- [ ] Kernel unit test: two lossy-colliding 16-byte UUIDs derive distinct `MemoryId`s.
- [ ] `Provenance::ExternalSnapshot.schema_hash` equals the submitted `IngestBatch.snapshot.schema_hash`.
- [ ] Ingest integration test asserts non-zero `schema_hash` round-trips through commit and read-back.
- [ ] A `snapshot` with `schema_hash.len() != 32` is rejected, not silently coerced.
- [ ] Full gate pipeline green: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo xtask kernel-purity`, `cargo xtask no-llm`.
