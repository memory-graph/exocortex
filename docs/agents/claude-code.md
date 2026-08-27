# Exocortex for Claude Code

First-party integration appendix (D4 of the agent-instructions PRD). The
playbook ships with the client; this page is the exact, complete set of
edits for this harness.

## 1. Install

```sh
curl -fsSL https://github.com/exocortex/exocortex/releases/latest/download/install.sh | sh
```

## 2. MCP config

`~/.claude.json` (or the workspace `.mcp.json`), under `mcpServers`:

```json
{
  "mcpServers": {
    "exocortex": {
      "command": "exocortex-mcp-client",
      "args": ["--backend", "https://your-node:7443", "--org", "your-org"],
      "env": {"EXOCORTEX_HMAC_KEY": "<64-hex producer key>",
              "EXOCORTEX_AUTH_TOKEN": "<bearer>"}
    }
  }
}
```

Standalone (no backend, offline WAL embedded store): omit the backend flag and
credential environment. On first run the client installs the playbook under the OS data
home and prints a notice on stderr; `--verify` prints its exact path.

## 3. The `CLAUDE.md` block

```sh
exocortex-mcp-client --dump-block >> CLAUDE.md
```

This is the load-bearing artifact — it rides in context on every turn.
Verify the install:

```sh
exocortex-mcp-client --verify
```

## 4. What "accepted code edit" means here

Edits go through an explicit accept/reject prompt. An edit is
**accepted** when:

- the user pressed accept, or
- auto-accept is on and the edit landed on disk.

Only accepted edits satisfy checklist item 1. Rejected or reverted
edits do not — write nothing for them unless the *reason* for rejection
was itself a decision worth recording.

## 5. How to verify it worked

```sh
exocortex-mcp-client --tail-audit --last 5
```

If the row count went up after a productive turn, the wrapup fired.
Rows stay `[pending]` until a backend is reachable (the WAL drains at
startup); `pending` is buffered, not lost.

## 6. Common failure modes

| Symptom | Diagnosis |
|---|---|
| Agent never calls `end_session` | The `CLAUDE.md` block is missing or the file isn't loaded — check the block exists verbatim |
| `not-connected` error | No `--backend` and no WAL — rerun with `--data-dir` writable |
| Every write rejected `Unauthorized` | Wrong/missing `EXOCORTEX_HMAC_KEY` (`exocortex-mcp-client --verify` flags it red) |
| Rejections the agent never mentions | Expected behavior per the block ("never drop silently") — if the agent stays silent, tighten the block's wording in your copy |
| `ONTOLOGY FINGERPRINT MISMATCH` | Client and backend run different pack versions — upgrade both |
| Playbook stale after upgrade | First run of the new binary rewrites it; `--verify` confirms the version |
