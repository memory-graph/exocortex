# Research note — JSON-LD for agent memory vs. exocortex

| | |
|:---|:---|
| **Date**: 2026-09-21 | **Status**: Complete (SPK1; feeds D34) |
| **Source**: "Graphs aren't hard" — a JSON-LD/verifiable-credentials-for-agent-memory essay (expansion/compaction as the document↔graph hinge; IRIs for merge-by-identity; production lessons from VCs) | **Consumed by**: `docs/prd/jsonld-interchange-prd.md` (D34) |

## The essay's claims

1. Graphs are easy; the interaction layer — the human-edited document
   vs. the machine-read payload — is the hard part. JSON-LD's
   expansion/compaction is a lossless bidirectional mapping between
   them.
2. Expansion never fails: unmapped keys are silently dropped and every
   tool reports success. Integrations rot invisibly for months.
3. Agents should get the COMPACTED document plus a term table;
   expansion belongs only at the boundary where data is stored,
   merged, or signed.
4. With global identifiers (IRIs), merging is set union — no
   reconciliation code. "A memory that concatenates is a transcript; a
   memory that merges is a knowledge base."
5. Pin and vendor your `@context`; test the expansion, not the
   document; give agents identities, not keys.
6. Signatures in VCs cover the graph (post-canonicalization), not the
   bytes.

## Mapping against exocortex

| Essay claim | Exocortex's existing mechanism | Verdict |
|---|---|---|
| Compact for agents, expand at the boundary | MCP self-contained tool schemas (D28) + the SDK validation manifest shipped as data (D21c: type-name→id table, ceilings) — a term table in everything but name | Already the design |
| Expansion never fails (silent drops) | The fail-closed validator stack: one kernel rulebook (W2), `RejectCode`s with correction hints, unknown kind/visibility/producer refusal, schema-drift and rewind refusal | Already the counter-design; D34 makes it a rule for the new surface (refuse-on-unmapped) |
| Pin and vendor your `@context` | The compatibility fingerprint: exact-match admission at six enforced sites, authorized-moves-only, pinned goldens | Stronger than a vendored file |
| Test the expansion, not the document | write-path-parity (W2 golden table) + `manifest_parity.rs` row-for-row | Extended by D34 S2 (expansion-parity harness over the emitted artifact) |
| Merge = set union on IRIs | ExternalKey joins (D13) merge structured identities with zero reconciliation; fuzzy identity stays human-governed (D17 Fellegi-Sunter proposals, never auto-writes); agents address 32-hex MemoryIds | Deliberate divergence: deterministic closed world, no LLM |
| Merges make a knowledge base | Bi-temporal validity, `Proposed`-never-persists, Dreams consolidation with rollback — deletion/revision semantics that set union cannot express | Already beyond the essay's model |
| Signature covers the graph | One canonical signing implementation (`exocortex_wire::signing`), content digests + HMAC, signing-hygiene gate | Equivalent within the system; the graph-level (VC) form is D34 S3, demand-gated |

## What was taken, and why only that

The single genuine gap the essay exposed: **portable interchange** — an
artifact the outside world can consume and merge by identity without
adopting the exocortex protocol. That became D34
(`docs/prd/jsonld-interchange-prd.md`). Everything else was external
validation of decisions already made and gate-enforced; re-opening
them would be churn without new information.

Two ideas explicitly rejected, with reasons:

- **A human-editable artifact as the write surface** (the essay's
  "file a person edits"): human edits require the import/merge path,
  which D34 scopes out — the closed world exists to keep provenance,
  ceilings, and fail-closed validation real. Revisit only as a governed
  adapter if demand appears.
- **Set-union merge semantics**: adopted only where identity is
  structured (ExternalKey); abandoned where it would launder ambiguity
  (D17's scored proposals instead).

## Decisions this note informs (in D34)

1. **IRI base ownership** (S1 owner decision): the essay's "pin and
   vendor" cuts both ways — an unowned base is a silent dependency on
   someone else's DNS. Decide before S1 opens.
2. **S2's expansion-parity dev-dep**: the essay's habit #2 is the
   justification — a hand-rolled expander tests our own bug twice;
   the independent processor is the one place a (test-only,
   rule-9-recorded) dependency earns its keep.
3. **S3 trigger signal**: watch for signed-interchange (VC-shaped)
   artifacts becoming table stakes in agent-memory tooling; that is
   the demand gate, not internal preference.
