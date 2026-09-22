# PRD — JSON-LD interchange export

| PRD | | Portable agent-memory artifacts: the graph, in a shape the ecosystem can merge |
|:---|:---|:---|
| **Author**: Gregory Dickson | **Status**: **Proposed** (D34; staged — S1/S2 on owner go, S3 demand-gated) **Created**: 2026-09-21 | **Visibility**: Internal |

## Summary

Exocortex's export surface is provenance-rich but private-format (D22's
JSONL corpus + lineage manifest). Meanwhile an interchange convention
for agent memory is consolidating around JSON-LD: compacted documents
for readers, a `@context` as the term table, IRIs for identity, and
signatures over the artifact. This PRD adds a **JSON-LD export flavor**
that maps the typed ontology to generated contexts and emits a
portable, mergeable-by-identity artifact — without opening the closed
world. Nothing here changes the internal model, the MCP surface, or
the compatibility fingerprint: it is a read-side projection, the same
way the corpus exporter is.

The design constraints come from where JSON-LD systems fail in
production (the "hard-won lessons" this PRD is answering):

- **C1 — compacted for consumers.** Agents and tools read short keys;
  term resolution happens at the boundary. Our equivalent already
  exists internally (the D21c validation manifest is a term table);
  this PRD exposes the same discipline outward.
- **C2 — expansion must be CHECKED, never assumed.** JSON-LD expansion
  silently drops unmapped keys and every tool reports success —
  integrations rot invisibly for months. The export therefore REFUSES
  on any unmapped element, naming it.
- **C3 — the context is pinned, vendored, and deterministic.** A
  drifting context is a silent semantic change; ours is generated from
  the packs, byte-stable, and versioned by the compatibility
  fingerprint.
- **C4 — identities, not keys.** Rows carry stable IRIs plus their
  RAW external coordinates as typed properties, so a foreign graph can
  union on the identifiers the SOURCE system already owns.

## Scope

One new one-shot mode on the node (the D22/org-backup pattern):
`--export-jsonld <dir> [--corpus-as-of RFC3339]`. Rides the corpus
exporter's bi-temporal cut (`recorded_at <= T && valid_from <= T &&
valid_until open or > T`), its both-endpoints rule for edges, its
computed-only-kind exclusions, and its D24 egress machinery.

### Non-goals

- **No import / merge into exocortex.** The closed world stays closed:
  foreign JSON-LD entering the graph would be a governed adapter (the
  D1/D18/D19 shape, signed Ingestion Protocol, ceilings, provenance)
  and is explicitly out of scope until real demand exists.
- No RDF store, no SPARQL, no query surface change.
- No compatibility-fingerprint move (a projection of the ontology, not
  a change to it — the OC-PRD D2 policy table governs any future
  argument otherwise).
- No LLM anywhere (rule 1, as ever).

## Requirements

### J1 — Export flavor

`--export-jsonld` emits: `memories.jsonld` (compacted documents, the
vendored `@context` inline and as a `context.jsonld` sidecar),
`edges.jsonld` (node references by IRI), `lineage.jsonld` (provenance
kind, author, producer, RAW external coordinates per R-T18a, LSN),
and `manifest.json` (cut, compatibility fingerprint, context digest,
canonicalization id, excluded computed-only kinds, the D24 aggregate
egress verdict). Mode-0600 atomic writes, the corpus-export pattern.

### J2 — Context generation

`@context` generated from the composed pack set: memory types, entity
types, relationship kinds, and the context fields, each mapped to a
stable IRI under a documentation-owned base (`https://exocortex.dev/v1/…`).
Generation is deterministic — **name-sorted declarations** (the
round-12 id-stability lesson: order is semantics), byte-stable across
runs, and stamped with the compatibility fingerprint. A pack append
extends the context (superset); the generated context is itself a
golden (J5).

### J3 — Refuse on unmapped (the anti-silent-drop rule)

Every element in the cut — every memory type, entity type, edge kind,
and non-null context field — must map to a context term. Any unmapped
element aborts the export with the element named. It is exactly the
JSON-LD failure mode this PRD exists to counter, made loud: a pack
change that would silently alter export meaning surfaces as a refusal,
never as a quietly narrowed artifact.

### J4 — Identity as IRIs

- Memories: `urn:exocortex:<32-hex-id>`.
- External identity: ExternalKey coordinates ride as typed properties
  (`exocortex:externalKey/<source_flavor>` with the source system's
  own identifiers verbatim — the linear issue number, the GitHub
  owner/repo/number, the git sha) so a foreign graph merges on the
  identifiers the source already owns, without reconciliation code.
  Raw bytes stay raw (B8 discipline); non-UTF8 `table_uuid`s hex-encode
  with a stated encoding marker.

### J5 — Goldens and parity

- A `gen-schemas`-style golden gate pins the generated `@context` and
  a fixture corpus's exported bytes; drift fails CI.
- **Stage 2 (expansion-parity harness):** the exported documents are
  expanded by an INDEPENDENT processor and asserted lossless against
  the internal cut — "test the expansion, not the document." This is
  the one place a dependency is justified: a test-only JSON-LD dev-dep
  (rule-9 record in PUBLISHING.md, reasons stated), because a
  hand-rolled expander would test our own bug twice. The dev-dep
  choice is an owner decision recorded before S2 lands.

### J6 — Canonical form and signature

Emitted documents serialize in OUR canonical byte form (sorted keys,
no whitespace), the manifest names it
(`"canonicalization": "exocortex-jsonld-v1"`), and the artifact set is
signed with the ONE canonical signing implementation
(`exocortex_wire::signing`, content digests + HMAC) — the same
discipline as every other artifact we emit. **Out of scope:** RDF
Dataset Canonicalization (URDNA2015) and the W3C Verifiable
Credentials proof suite — that is stage 3, demand-gated; we do not
hand-roll RDF canonicalization.

## Stages

| Stage | Contents | Gate |
|---|---|---|
| S1 | J1-J3, J5 goldens, J6 | Owner go (this PRD's acceptance) |
| S2 | J4 full identity surface + the expansion-parity harness | Owner go + the J5 dev-dep decision |
| S3 | VC-style signed wrapper, standard proofs | Demand-gated: real interchange adoption |

## Acceptance criteria

1. `--export-jsonld` on a fixture corpus produces byte-stable output
   across two runs (goldens in the `gen-schemas` gate; fails on drift).
2. A pack element absent from the context (fixture) aborts the export,
   exit nonzero, naming the element — fail-without-it test.
3. The emitted memories re-expand (S2 harness) to exactly the internal
   cut, row for row, field for field — no dropped keys, no invented
   ones.
4. The manifest carries the compatibility fingerprint, context digest,
   canonicalization id, excluded kinds, and the D24 egress verdict;
   signature verification fails on any tampered file.
5. Bi-temporal cuts and edge-endpoint rules agree with the D22 corpus
   exporter row-for-row on the same input (parity test).
6. No new runtime dependency (S1); the S2 dev-dep, if taken, is
   recorded in PUBLISHING.md with reasons before it lands.

## Open questions

1. The IRI base ownership story (`exocortex.dev` vs a github-pages
   context host) — an owner decision when S1 opens.
2. Whether edges should also emit an `@graph`-bundled single-file
   variant for tools that dislike multi-file artifacts — decide at S1
   implementation from the fixture consumers' behavior, not before.
