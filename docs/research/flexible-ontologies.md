# Flexible / Non-Static Ontologies — Landscape vs. Exocortex (SPK2)

Research note, 2026-10-19. Trigger: D61's widening fought the static
ontology three ways (pack independence blocked the natural from-side;
the fingerprint ritual is heavyweight; type ids are position-based).
The owner flagged "flexible ontologies" as an emerging thing. This
note maps the 2025–26 landscape claim-by-claim against exocortex's
mechanisms, SPK1-style: what already exists here, what's genuinely
adoptable, and what's explicitly rejected with reasons.

## The landscape in one paragraph

The 2025–26 convergence: **ontology authority moves from build time to
run time, and evolution is driven by evidence** (agent trajectories,
task feedback, temporal contradiction), while staying **gated and
versioned** because ungated LLM-induced schemas are noisy. Three
families: (1) LLM-era memory systems with runtime-inferred schemas
(Zep/Graphiti "prescribed or learned", Mem0 schema-free, Cognee
optional-ontology, GraphRAG induced); (2) schema-as-data databases
(TerminusDB: schema changes are first-class commits with inferred
weakening migrations; TypeDB: types/rules editable at runtime; RDF
stores: ontology is triples, DRAGON-AI LLM-generates axioms); (3) the
new wave — EvoOntology (arXiv:2609.15779, ontology as an evolving MCP
server) and OaK (arXiv:2608.22974, task-driven ontology kernel) —
both gated, versioned, evidence-driven.

## Claim-by-claim mapping

### 1. "Ontology as a service the agent queries at runtime" (EvoOntology's MCP shape)

**Exocortex already has this.** The ontology IS served to agents at
runtime: `--verify` prints ontology/playbook rows; the playbook
(regen-gated against the pack) teaches the type table; every read
carries `memory_type_label` resolved from the live composed set. The
innovation in EvoOntology is the *co-evolution loop* (agent
trajectories refine the ontology), not the serving.

**Adoptable**: nothing structural. The co-evolution loop is rejected
below (§5).

### 2. "Evolution driven by evidence, gated" (EvoOntology/OaK core claim)

**Exocortex's equivalent is the plan lifecycle** — the fingerprint
moves only when a plan row authorizes it, and rows enter through the
owner's direction (D61's widening was prompted by a real use case
hitting a real wall, not speculation). That IS evidence-driven,
gated evolution — with the gate being the owner, not an eval harness.

**Adoptable**: a lighter-weight evidence channel — the recurring D61
class is "the ontology lacks the triple the data wants"; those show up
as `InvalidTypeTriple` ingest rejections. **A rejected-triples ledger
could accumulate in the audit stream (deterministic, no LLM) and
surface in `--verify` as ontology-demand telemetry** — the owner's
gate stays, but demand becomes visible instead of discovered by
accident in a coding session. Candidate: a row, small.

### 3. "Schema-as-data; schema changes are first-class commits with inferred weakening migrations" (TerminusDB)

**This is the closest match to what exocortex already does** — OC-PRD
D2's policy table *is* a weakening/strengthening classification
(superset = compatible; the D61 widening was TerminusDB's "extend
enum" exactly). The fingerprint is the schema-version commit. What
TerminusDB has that exocortex lacks: **inferring the migration class
automatically from the diff**. Exocortex derives the class
mechanically (`compatibility-policy` gate) but the golden-update
ritual is manual.

**Adoptable**: making the ritual one command — `cargo xtask
ontology-evolve` that diffs old/new fingerprints, asserts the D2
class, and writes golden + AGENTS + CLAUDE + plan stub in one shot.
Purely mechanical (no determinism cost). Candidate: a row, cheap,
removes the ceremony that made D61 slower than the change itself.

### 4. "Bi-temporal edges; contradiction closes, never deletes" (Graphiti)

**Exocortex has had this since M0** — `valid_from`/`valid_until` +
`recorded_at` + LSN versioning IS the tri-temporal shape (D56's count
queries are point-in-time state reconstruction). Graphiti's insight
("temporal is the flexibility that matters") is validation of the
existing design, not a gap.

**Adoptable**: nothing.

### 5. "LLM-induced schemas / learned ontologies" (Graphiti's learned mode, Mem0, DRAGON-AI, OaK)

**Explicitly rejected, three ways.**
(a) **R-D6**: no LLM anywhere in this codebase — an LLM deciding
types is an LLM in the write path's authority.
(b) **Determinism**: sub-millisecond deterministic reads and a
byte-stable compatibility contract cannot rest on a
model-that-changed-its-mind; the fingerprint's whole value is that
two components decide exchangeability *without* judgment.
(c) **D61's own evidence**: the value of a *pinned* to-side
(`Answers → Question` only) showed up immediately — the widening
test proves non-Question to-sides still reject. Schema-free graphs
(Mem0) give that up entirely and get "consistency, validation, and
formal reasoning absent" in return — exocortex's typed triples are
load-bearing (R-T2), not ceremony.

### 6. "Types/rules editable at runtime" (TypeDB, RDF stores)

**Partially adoptable at the layer exocortex already softens**:
MemoryContext carries free-form `additional_metadata` (8 KiB), tags
are open vocabularies, and entities have types the way property
graphs do. The typed *memory/kind* surface is deliberately the rigid
core.

**The real rigidity costs found in D61** — worth fixing regardless of
any industry trend:
- **Position-based ids across packs** (D8: type ids share an offset
  space; appends renumber downstream packs). Names-not-positions in
  storage would make reordering safe — but persisted u8 ids are the
  sub-ms read path, so this is a wire+storage migration (v2-scale,
  not a patch).
- **Pack independence blocks cross-pack triples** — the natural D61
  widening (`Fix | Solution` from-side) failed the study pack's own
  composition. The wildcard was the right call, but a **compose-time
  alignment mechanism** (DOL-style: a tiny "bridges" pack that may
  name types from packs it explicitly depends on, validated only in
  compositions that include both) would express D61 without the
  wildcard's looseness. Candidate: a row when a second cross-pack
  need appears — one instance is a hack, two is a pattern.
- **The 300-word instruction block** can't teach new conventions —
  D61's Question convention landed in the full playbook (ungated) and
  that was enough. No change needed; recorded as the working
  pattern.

### 7. "Task-oriented disposable ontologies" (OaK)

The genuinely new idea: ontologies built *per task* and refined by
task performance, not reusability. For exocortex this maps onto
**Dreams discovery proposals** — already deterministic,
region-scoped, evidence-gated derived structure. A task-scoped
"lens" (temporary traversal/kind filters materialized per query)
would be the exocortex-shaped version: derived, disposable, never
persisted as new types. No row — noted as the shape any future
"task ontology" request should take.

## Decisions this note informs

1. **No pivot.** The static, fingerprinted, pack-composed ontology is
   the product's spine (determinism, sub-ms reads, org-scale
   durability); the industry trend is real but its engines (LLM
   induction) are constitutionally banned here. Exocortex already
   implements the trend's *discipline* (gated, versioned,
   evidence-driven, additive-first) without its engine.
2. **Three candidate rows when wanted**: rejected-triple demand
   ledger (§2); `ontology-evolve` one-command ritual (§3); cross-pack
   bridge packs on second occurrence (§6).
3. **Names-not-positions** stays parked with v2 migration-class work
   — real cost, real fix, wrong scale for a patch.

## Sources

- EvoOntology — arXiv:2609.15779, github.com/ruc-datalab/EvoOntology
- OaK (Dynamic Ontology) — arXiv:2608.22974
- Zep/Graphiti — arXiv:2501.13956, github.com/getzep/graphiti
- Mem0 graph memory — docs.mem0.ai/platform/features/graph-memory
- Cognee ontologies — docs.cognee.ai (core-concepts/further-concepts/ontologies)
- Letta memory blocks — docs.letta.com/v1-sdk/memory/memory-blocks
- TerminusDB schema migration — terminusdb.org/blog/2023-04-24-schema-migration,
  terminusdb.com/docs/releases/11-0-6-automatic-schema-migrations
- TypeDB vs TerminusDB — inferensys.com (ontology-engineering-platforms)
- DRAGON-AI — J. Biomedical Semantics, 2024 (10.1186/s13326-024-00320-3)
- CoA-Text2OWL — Procedia Computer Science, 2025 (S187705092502914X)
