## Exocortex — writing to memory

You have `exocortex.*` MCP tools. At the end of every turn, before
your final message, run the checklist below. If ANY item fires, call
`exocortex.end_session` with 1–5 typed memory drafts and any edges —
it validates locally first and tells you exactly what to fix. If none
fire, write nothing.

**Write if this turn:**
- Made a code edit the user accepted
- Ran a non-obvious command that produced the intended result
- Answered a "why/how" question with a claim about the codebase
- Decided against an alternative for a stated reason
- The user said "remember this"
- Identified a problem, solved or not — future `Solves` edges need
  them

**Types you'll usually write:** `Fix`, `Solution`, `Problem`, `Error`,
`CodePattern`, `Command`, `Technology` (full list: playbook).

**Draft fields:** `draft_key`, `memory_type`, `title`, `content`,
`visibility`, `tags`.

**Edges:** typed; `exocortex-mcp-client --verify` locates the 48-kind
catalogue. Link within the batch by `draft_key`, or
to an existing memory by `to_memory_id` (32-hex id from search
results). When in doubt, use `RelatedTo`. Never assert `SimilarTo`
(computed-only).

**Supersession:** if an existing memory is now wrong, write the
corrected memory and link it `Replaces` or `Contradicts`. Act on
`end_session`'s `similar_to` near-duplicate suggestions; never
restate what exists. Cite successors, not `superseded_by`-marked
memories. Never invent confidence scores — the backend derives them
from evidence.

**Titles:** ≤200 chars, subject-verb-object, specific. Good: "Fixed
OAuth token refresh race in exchange()". Bad: "auth fix".

**Visibility:** Default `project`. Escalate to `org` only for
cross-project knowledge. `private` for user preferences.

**Reading:** Once at session start, `search_memories("<project-terms>",
limit=10)`. When stuck, `search_memories("<exact error>")`. Do not
search on every turn.

**Rejections:** read the `code` and `detail`, fix, resubmit same
turn. Never drop one silently — surface any unfixable rejection.

Session ids are client-stamped. Full reference:
the playbook path printed by `exocortex-mcp-client --verify`.
