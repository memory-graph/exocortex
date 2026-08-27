# Exocortex for Cursor

First-party integration appendix (D4 of the agent-instructions PRD).
Same playbook as Claude Code; this page is the harness-specific part.

## 1. Install

```sh
curl -fsSL https://github.com/exocortex/exocortex/releases/latest/download/install.sh | sh
```

## 2. MCP config

Cursor reads `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "exocortex": {
      "command": "exocortex-mcp-client",
      "args": ["--backend", "https://your-node:7443", "--org", "your-org",
               "--hmac-key", "<64-hex producer key>", "--auth-token", "<bearer>"]
    }
  }
}
```

Standalone (offline WAL embedded store): omit all four flags. On first
run the client installs the playbook under the OS data home; `--verify`
prints its exact path.

## 3. The instruction block

Cursor reads `.cursorrules` (or `.cursor/rules/*.mdc`):

```sh
exocortex-mcp-client --dump-block >> .cursorrules
```

This is the load-bearing artifact — it rides in context on every turn.
Verify:

```sh
exocortex-mcp-client --verify
```

## 4. What "accepted code edit" means here

Edits appear inline. An edit is **accepted** when the user did not
immediately revert or reject it within the same turn. **If the user is
silent, treat the edit as accepted.**

## 5. How to verify it worked

```sh
exocortex-mcp-client --tail-audit --last 5
```

Row count up after a productive turn ⇒ the wrapup fired. `[pending]`
rows are buffered in the WAL, not lost.

## 6. Common failure modes

| Symptom | Diagnosis |
|---|---|
| Agent never calls `end_session` | The block is missing from `.cursorrules` — check it exists verbatim |
| `not-connected` error | No `--backend` and no WAL — check `--data-dir` writable |
| Every write rejected `Unauthorized` | Wrong/missing `--hmac-key` (`--verify` flags it red) |
| Duplicate memories from reverted edits | Reverted-in-same-turn edits are NOT accepted — the block's checklist is the contract; tighten your copy if drift persists |
| `ONTOLOGY FINGERPRINT MISMATCH` | Client and backend run different pack versions — upgrade both |
