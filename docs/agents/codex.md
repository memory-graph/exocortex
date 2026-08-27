# Exocortex for Codex

First-party integration appendix (D4 of the agent-instructions PRD).
Same playbook as Claude Code; this page is the harness-specific part.

## 1. Install

```sh
curl -fsSL https://github.com/exocortex/exocortex/releases/latest/download/install.sh | sh
```

## 2. MCP config

Codex reads `~/.codex/config.toml`:

```toml
[mcp_servers.exocortex]
command = "exocortex-mcp-client"
args = ["--backend", "https://your-node:7443", "--org", "your-org"]
env = { EXOCORTEX_HMAC_KEY = "<64-hex producer key>", EXOCORTEX_AUTH_TOKEN = "<bearer>" }
```

Standalone (offline WAL embedded store): omit the backend flag and credential
environment. On first
run the client installs the playbook under the OS data home; `--verify`
prints its exact path.

## 3. The `AGENTS.md` block

```sh
exocortex-mcp-client --dump-block >> AGENTS.md
```

This is the load-bearing artifact — it rides in context on every turn.
Verify:

```sh
exocortex-mcp-client --verify
```

## 4. What "accepted code edit" means here

Edits inside a task are **accepted** iff the task completed with a
non-error exit. A task that failed midway has no accepted edits — write
nothing for the edits themselves, but a failed *approach* with a novel
failure is checklist item 3 material (a "why" claim about the codebase).

## 5. How to verify it worked

```sh
exocortex-mcp-client --tail-audit --last 5
```

Row count up after a productive turn ⇒ the wrapup fired. `[pending]`
rows are buffered in the WAL, not lost.

## 6. Common failure modes

| Symptom | Diagnosis |
|---|---|
| Agent never calls `end_session` | `AGENTS.md` block missing — check it exists verbatim |
| `not-connected` error | No `--backend` and no WAL — check `--data-dir` writable |
| Every write rejected `Unauthorized` | Wrong/missing `EXOCORTEX_HMAC_KEY` (`--verify` flags it red) |
| Rejections never surfaced | The block requires surfacing unfixable rejections in the final message — check your copy wasn't trimmed |
| `ONTOLOGY FINGERPRINT MISMATCH` | Client and backend run different pack versions — upgrade both |
