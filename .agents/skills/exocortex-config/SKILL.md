---
name: exocortex-config
description: "Set up, verify, or repair the exocortex agent-memory integration: MCP server wiring for any harness (Claude Code, Crush, Codex, Cursor), the instruction block, --verify checklist interpretation, dead-connection triage, WAL drain state, and install-path differences. Use when exocortex tools are missing or erroring, when wiring a new harness, or when interpreting an install or verify failure."
user-invocable: true
metadata:
  short-description: "Exocortex harness integration: wire, verify, repair"
---

# Exocortex config — wire, verify, repair

The integration has three moving parts: the MCP server a harness
spawns, the instruction block riding in the agent's context, and the
local state (WAL + graph). When memories "aren't working", one of the
three is misconfigured or dead. Work top to bottom.

## 1. What should be wired (the common mistake)

Harnesses must spawn the **installed entrypoint**, never the raw
client:

```
exocortex --mode mcp-standalone --org <org> --user <user>
```

- The entrypoint supervises a loopback backend, serves reads from a
  durable graph, and **drains the local WAL at startup**.
- The raw `exocortex-mcp-client` runs offline-WAL-only: writes buffer
  forever, nothing ever syncs. Wiring it is the classic stale-dogfood
  mistake (pending entry count grows in `--verify`).
- `--mode mcp-client --backend <url>` is only for pointing at an
  external shared backend node.

Wiring snippets:

- **Claude Code:** `claude mcp add exocortex -- exocortex --mode mcp-standalone --org my-org --user me`
- **Crush** (`~/.config/crush/crushrc`):
  `mcp add exocortex --type stdio --command ~/.cargo/bin/exocortex --args --mode --args mcp-standalone --args --org --args my-org --args --user --args me`
- **Generic MCP JSON:** command `exocortex`, args `["--mode","mcp-standalone","--org","my-org","--user","me"]`

## 2. The instruction block

`exocortex-mcp-client --install-block <file>` installs it into the
harness's always-loaded context (`CLAUDE.md`, `AGENTS.md`,
`.cursorrules`) — idempotent, version-marked, reruns replace in place.
For Crush, keep it in the file named by
`option global-context-path "$HOME/.config/crush/AGENTS.md"`. The block
is the contract: write checklist, read patterns, rejection loop, and
the absent-tools clause — if `exocortex.*` tools are missing, the
agent must SAY so, never skip silently.

## 3. Verify the install

```
exocortex-mcp-client --verify
```

Row meanings:

- `harness` (RED when unwired or no config found) — no known harness
  config (crushrc, ~/.claude.json, ~/.claude/settings.json) names this
  install's binaries; wire per §1, or point at a custom config with
  `EXOCORTEX_VERIFY_HARNESS_CONFIG=<file>`.
- `ontology` / `playbook` rows — fingerprints must match the goldens
  in the repo's AGENTS.md; a mismatch means binary and state come from
  different versions.
- `wal: N pending entries` (RED, offline mode) — writes are buffering
  with no backend. This is the signature of raw-client wiring; migrate
  to the entrypoint and the entries drain at next startup.
- `partition` (RED on a pair mismatch) — the local WAL holds writes
  stamped under other org/user pairs than the configured `--org/--user`;
  running with the wrong pair starts an empty graph next door to the
  live data. Re-run with the pair the row names.
- `store` rows (RED) — either store processes on the data dir with no
  live supervisor (orphaned/foreign writers: kill them, one AOF must
  have one writer), or a stale `port` file whose port answers nothing
  (left by an earlier boot; delete it or re-run standalone — nothing
  consumes it today).
- Drain-time `InvalidTypeTriple` terminal rejections in stderr are
  old offline-accepted entries the server rejects at sync — expected,
  audit-marked `Failed`, not data loss of the valid rows.

## 4. Dead-connection triage (no exocortex.* tools in the session)

1. Check the harness's MCP status/logs. In Crush: the session info
   shows `exocortex = error: ...`; the project log
   (`.crush/logs/crush.log`) carries
   `"MCP client failed to initialize"` with the error.
2. Probe the exact configured command with a JSON-RPC initialize:

   ```sh
   printf '%s\n' \
     '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
     '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
     | exocortex --mode mcp-standalone --org personal --user me
   ```

   A healthy server answers `initialize` then `tools/list`. EOF before
   answering = the process dies at startup (check the WAL lock: a
   still-running older server instance can hold it).
3. Restart the harness — MCP servers connect only at session start; a
   mid-session fix needs a new session.

## 5. Install paths

- **Installer** (README one-liner): entrypoint + client/node/worker
  binaries + embedding model + standalone Falkor runtime + this skill
  (`~/.agents/skills/exocortex-config/`).
- **`cargo install --git`**: client binary only, no runtime —
  standalone mode unavailable; fine for `--mode mcp-client` against an
  external node.
- **Checkout**: `cargo build --release -p exocortex-client -p exocortex-server`,
  then `scripts/exocortex` with `EXOCORTEX_REDIS_SERVER` /
  `EXOCORTEX_FALKORDB_MODULE` pointing at a runtime. The repo's
  `.agents/skills/` copy of this skill loads automatically.

Version skew: `exocortex-mcp-client --version` reports the binary; the
compatibility fingerprint in `--verify` output must match the repo
golden it was built from — `a427b3ad…ed9d` on current main.
