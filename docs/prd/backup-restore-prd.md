# PRD — memory backup & restore

| PRD | | Standalone-mode memory portability: the WAL as a file you can keep |
|:---|:---|:---|
| **Author**: Gregory Dickson | **Status**: **Closed** — shipped as `exocortex-client 0.2.2` (2026-08-27; all five acceptance criteria in `client/tests/backup_restore.rs`; the idempotent-import test additionally surfaced and pinned a real cache defect — stale search-arena keys on repeated upserts — fixed in the same release) **Created**: 2026-08-27 | **Visibility**: Internal |

## Summary

Standalone mode made the WAL the embedded store (SR-PRD): every write
is durable, id-searchable, and byte-stable across restarts — but it
lives in one sled directory on one machine. This PRD adds the two
operations that make that data *portable*: export it to a single
deterministic file, and restore it into any data-dir. Disaster
recovery, machine migration, and "keep my agent's memory in the repo
it works on" all fall out of those two operations.

## Scope and semantics

Backup/restore operates on the **local WAL** (all entries, all states).
Both are one-shot CLI modes on `exocortex-mcp-client`, alongside
`--verify` and `--tail-audit`.

### The format (`exocortex-backup` v1)

```json
{
  "format": "exocortex-backup",
  "version": 1,
  "created_at": "2026-08-27T10:00:00Z",
  "ontology_fingerprint": "e1f7d17b…ddc9b2",
  "entries": [ …WalEntry, in local-LSN order… ]
}
```

- `WalEntry` is already the WAL's own serde codec — the backup is the
  store's native row shape, not a parallel schema that can drift.
- Entries serialize in LSN order: two exports of the same WAL are
  byte-identical modulo `created_at`, so backups diff cleanly and a
  repo-committed backup is reviewable.

### Export (`--export <file>`)

Dump every entry — `Pending`, `Synced`, `Failed` alike; the backup is
a faithful WAL clone, not a "just the unsynced part" view. Exits 0
with a one-line summary (`N entries → path`); an empty WAL exports a
valid empty backup.

### Import (`--import <file>`)

All-or-nothing restore into the target data-dir's WAL:

1. **Fingerprint gate (fail closed).** The backup's
   `ontology_fingerprint` must equal the binary's effective ontology.
   A mismatch aborts before anything is written — restoring data
   typed against a different pack set is a silent-corruption bug, not
   an convenience to warn through.
2. **Revalidation.** Every entry's drafts pass the same kernel
   `validate_draft` the offline write path runs (W2's one rulebook).
   Any rejection aborts the whole import.
3. **Append, re-keyed, state preserved.** Entries append with fresh
   local LSNs (the target WAL keeps its own sequence); `memory_ids`,
   `batch_id`, `draft_keys`, `tags`, and `state` are preserved
   verbatim. Preserving state matters: already-`Synced` entries do not
   re-drain to a backend (server-side batch-id idempotency would catch
   it anyway — belt and braces), and `Failed` entries keep their
   operator-visible history.
4. **Idempotent by construction.** Importing the same file twice (or
   into a WAL that already holds the entries) cannot corrupt or
   double: rows carry their ids, snapshot insertion upserts by id
   (CR1), and the drain's `(producer_id, batch_id)` idempotency entry
   de-duplicates server-side.

### Explicitly deferred (v2 candidates)

- **Backend/org backup** — exporting from FalkorDB needs a
  storage-side scan operation (streaming, org-scoped, admin-authed)
  and a restore path through the ingestion boundary with ceiling and
  provenance checks. Real feature, separate PRD.
- **Cross-org migration** — restore-into-a-different-org re-keys
  external identities; not this PRD.
- **Scheduled/automatic backups** — a wrapper concern once the file
  format exists; cron/systemd timers do it fine.

## Acceptance (each fails on current `main`)

1. **Round trip** — write two batches (one with an edge), `--export`,
   wipe the data-dir, `--import`, restart: every memory is searchable
   and every id is byte-identical to the pre-export ids; the edge
   traverses via `find_related` (`backup_restore.rs`).
2. **Idempotent import** — importing the same backup twice leaves the
   served graph unchanged (no duplicate rows, ids stable).
3. **Fingerprint gate** — a backup whose fingerprint is tampered
   aborts the import with a non-zero exit and leaves the target WAL
   untouched.
4. **Empty round trip** — export of a fresh data-dir is a valid empty
   backup; importing it is a no-op success.
5. **All states ride** — an entry marked `Synced` before export is
   `Synced` after import (`states_for_test`), and does not re-drain.

## Release

Ships as `exocortex-client 0.2.2`. No proto change, no fingerprint
change, no schema golden change; two new one-shot CLI flags + README
rows + master-plan rows.
