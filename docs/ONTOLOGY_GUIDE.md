# The Exocortex Ontology Development Guide

![An ontology pack as an engineering blueprint — typed nodes, typed edges, and derivation rules on a drafting grid](../images/ontology-guide.jpeg)

How to design, write, ship, and evolve an ontology pack for Exocortex.
The short version: the ontology is a Rust crate. You write a `pack!`
block, the kernel compiles and validates it, every surface (MCP tool
schemas, storage, SDK, playbook) is generated from it, and its content
is fingerprinted so every node in a deployment can prove it is talking
about the same ontology.

---

## 1. What an ontology is

An ontology is the vocabulary and rulebook of a domain: the kinds of
things that exist, how they connect, and what follows from those
connections. In Exocortex it has four parts:

| Part | Question it answers | Example |
|---|---|---|
| **Memory types** | What kinds of facts get captured? | `Problem`, `Fix`, `Solution` |
| **Entity types** | What are facts *about*? | `File`, `Function`, `Person` |
| **Relationship kinds** | How do facts connect? | `Fixes`, `Solves`, `Requires` |
| **Rules** | What can be *derived* instead of stored? | `Fixes` + `Fix` ⇒ `Solves` |

Two design stances shape everything else:

- **The envelope is typed; the content is free text.** A memory type is
  a *category tag*, not a payload schema. The harness (an LLM) writes
  the prose; the system enforces the structure around it. This is the
  single most important lesson from our first memory system, validated
  across five storage backends: do not design per-type field schemas
  the LLM must fill correctly — design the vocabulary the LLM tags
  prose with.
- **The intelligence lives in the edges.** Every relationship kind
  carries strength, confidence, evidence counts, and bi-temporal
  bounds. Reasoning (traversal, derivation, consolidation) runs over
  typed edges, never over prose.

Why is it code and not a config file? Because the guarantees are
compile-time: kind tables, type triples, and rules are validated by the
compiler and the kernel at registration; the playbook and tool schemas
are *generated* from the same source (so they cannot drift); and the
fingerprint — a hash of pack content — lets any client or server prove
byte-identity with its peers. Same binary + same packs = same ontology,
provably.

## 2. The anatomy of a pack

A pack is a crate that depends on `exocortex-kernel` and nothing else
internal (the kernel-purity gate enforces this). Its whole ontology is
one `pack!` block:

```rust
use exocortex_kernel::pack;

pack! {
    name: "acme-pack-legal", version: "1.0.0", kernel_min: "1.0.0",

    memory_types! {
        Case, Statute, Ruling, Motion, Brief, LegalNote,
    }

    entity_types! {
        Court, Jurisdiction, StatuteRef, Party, Judge,
    }

    computed_only_kinds! {
        PrecedentCluster,          // Dreams-exclusive; never assertable
    }

    kinds! {
        Cites         => bucket: Context,    inverse: CitedBy,      bi: false, default_strength: 0.85,
        Distinguishes => bucket: Learning,   inverse: DistinguishedBy, bi: false, default_strength: 0.80,
        Overrules     => bucket: Causal,     inverse: OverruledBy,  bi: false, default_strength: 0.95,
        ...
    }

    type_triples! {
        Cites         => (Case | Ruling | Brief, Statute | Ruling | Case),
        Overrules     => (Ruling, Ruling),
        ...
    }

    crepe_rules! {
        precedent_chain(a, c) <- edge(a, b, Cites), edge(b, c, Overrules);
    }
}
```

Block by block:

- **`memory_types!` / `entity_types!`** — plain identifier lists. They
  expand to `#[repr(u8)]` enums compiled into the crate. Type ids are
  assigned per-pack with a running offset, so *adding your pack never
  renumbers another pack's ids* — existing stored rows stay valid.
- **`kinds!`** — one row per relationship kind. `bucket` places it in a
  reasoning family (the eight buckets below); `inverse` names the
  auto-registered companion label (`Solves`/`SolvedBy` — companions are
  registered for you, not counted as kinds); `bi: true` marks a
  genuinely symmetric relation; `default_strength` is the prior the
  reasoning layer uses when a write doesn't carry one; `kernel_const`
  optionally binds a kind to a kernel constant (dev-v1 binds `Solves`,
  `Causes`, `Fixes`, `InSession`) — the macro *asserts* full
  kernel-constant coverage, so a binding can never silently go missing.
- **`computed_only_kinds!`** — kinds only Dreams may write (like
  `SimilarTo`). The ingest boundary rejects any batch that asserts
  them; the marker rides the ontology itself, not a string list.
- **`type_triples!`** — the edge type system: for each kind, which
  **memory types** may sit on each end. `(Fix, Error | Problem)` means
  `Fixes` runs from a `Fix` to an `Error` or `Problem`, and nothing
  else — a write with wrong endpoints is rejected with
  `InvalidTypeTriple` before it ever reaches storage. `(_, _)` opts a
  kind out of endpoint restriction (use deliberately, not by default).
  ⚠ Sides reference memory types only — entity types (`File`, `Judge`)
  cannot appear here. Dev-v1 itself hit this: its original `Uses`/
  `Requires` tables listed `Package`, an entity type, and the M1 build
  caught it; the compiler will catch yours too.
- **`crepe_rules!`** — Datalog (Crepe, compile-time). Rules derive
  facts from edges instead of storing them. Kernel rules R1–R9
  (transitive closure, affinity, problem-solution bridging) already
  exist; pack rules only fire on pack-owned kinds and reference kinds
  *by name* so the kernel injects interned ids at compile time.

Registration is compile-time: the macro emits `inventory::submit!`, and
`load_registered_packs()` at process startup assembles every linked
pack into the effective ontology. A pack that is linked but never
referenced can be dead-stripped — force-link it the way the client
does:

```rust
let _ = std::hint::black_box(acme_pack_legal::pack_def().name.clone());
```

## 3. How to design one

Design methodology, in the order we recommend working:

**1. Start from the retrieval questions.** Write down, in prose, the
questions an agent will ask at the start of a session. Not "what data
do we have" but "what will be asked":

> *"What's blocking the auth migration?" · "Has anything ruled on this
> before?" · "Which control covers this vulnerability?"*

Every type and kind must earn its place by being the answer's shape.
If nothing retrieves it, cut it.

**2. Types are category tags: few, stable, distinguishable.** Aim for
single digits to low teens. A type must be decidable by the writing
agent in one glance at a session ("this is a Problem"), and stable for
years — renaming or removing a type in a live deployment strands every
row written under it (see §6). If two types could describe the same
fact and the agent would agonize over which, merge them.

**3. Design the edges around the inferences you want.** Edges are the
reasoning surface; ask "what chain do I want traversable?" and work
backwards. The eight buckets are reasoning families, choose each kind's
bucket deliberately:

| Bucket | Reasoning it enables | Dev-v1 examples |
|---|---|---|
| Solution | problem→answer bridging | `Solves`, `Improves`, `Replaces` |
| Causal | root-cause chains | `Causes`, `Fixes`, `Blocks`, `Enables` |
| Context | composition & dependency | `Uses`, `Requires`, `Contains` |
| Learning | belief evolution | `Contradicts`, `Confirms`, `BuildsOn` |
| Similarity | consolidation input | `SimilarTo`, `AnalogousTo` |
| Workflow | ordering & agency | `Precedes`, `Creates`, `Automates` |
| Quality | verification | `Validates`, `Tests`, `Documents` |
| Integration | system topology | `Consumes`, `Produces`, `Wraps` |

**4. Enumerate the triples; leave nothing implicit.** The triple table
is where ontology design actually happens — it is the difference
between a graph that reasons and a hairball. `(_, _)` is an escape
hatch, not a default. Constrain endpoints everywhere the domain knows
the answer, and the validator will enforce your design forever, for
free.

**5. Decide inverses and bidirectionality per kind.** Every directed
kind gets a companion inverse label automatically. `bi: true` is for
genuinely symmetric relations (`Contradicts`, `SimilarTo`) — a
*directional* relation marked `bi` loses its ability to mean "from
cause to effect," so default to `bi: false`.

**6. Strengths are priors.** `default_strength` is what a derived or
unweighted edge starts at; the reasoning layer reinforces and decays
from there. Set 0.85–0.95 for definitional edges (`Overrules`),
0.65–0.80 for empirical ones, and reserve low values for weak hints
(`RelatedTo` at 0.30).

**7. Entities name stable referents.** Entities are extracted at ingest
from content + context and link memories into neighborhoods without
hand-wired edges. Pick things with durable identity (`File`,
`Function`, `Court`, `Jurisdiction`) — not abstractions or events.

**8. Derive, don't store.** If a fact follows from edges you already
have, write a rule instead of asking the agent to assert it. Derived
rows carry `Provenance::Derived` with the rule id, so "why does the
system believe this?" stays answerable. Watch the pack rules in dev-v1
(D1–D6) for the idiom; keep them small and let transitivity do the
walking.

**9. Reserve computed-only kinds for Dreams.** If a relationship should
only ever be minted by consolidation (clustering, similarity), mark it
`computed_only_kinds!` — then no producer, including your future self,
can assert it by accident.

**10. Evolve additively.** New types, kinds, triples, rules: always
safe (ids append; fingerprint changes intentionally). Renames and
deletions strand data; there is no v1 migration tooling. Design names
you can live with for years.

## 4. How the default pack works

`exocortex-pack-dev-v1` (13 memory types, 12 entity types, 48 kinds) is
the reference implementation — read it top to bottom, it is ~160 lines:

- **The type set follows the dev loop's natural categories**: work
  state (`Task`, `Problem`, `Solution`, `Fix`, `Error`), code substance
  (`CodePattern`, `Command`, `FileContext`, `Workflow`), environment
  (`Project`, `Technology`), session material (`Conversation`), escape
  hatch (`General`). `General` matters: a write that fits nothing still
  participates via entities and edges instead of being lost.
- **The Problem/Fix/Solution triad is the causal spine**, and its
  triples are tight on purpose: `Fixes => (Fix, Error | Problem)`,
  `Solves => (Solution | Fix, Problem | Error)`. Rule D1 then makes
  `Fix Fixes Problem` imply `Fix Solves Problem` — subsumption you get
  for free from a constrained triple table.
- **Kernel-const bindings anchor the universal rules**: `Solves`,
  `Causes`, `Fixes`, `InSession` are bound so kernel rules R1–R9 can
  fire on them regardless of pack; the macro asserts coverage.
- **`SimilarTo` is the computed-only kind** — similarity is
  Dreams' job, never a producer's.
- **Session semantics are ontological**: a session *is* a
  `Conversation` memory; `InSession => (_, Conversation)` groups every
  wrapup batch, and rule D6 feeds those cohorts to MCR². There is no
  separate session object — that separation was a drift bug in our
  first system, and the pack design makes it unrepresentable.
- **D1–D6 are the pack rules**: implied solves, transitive
  `BuildsOn`, indirect blockers, contradiction propagation, shared
  file-lineage, session cohorts. Each is two lines of Datalog.

## 5. Three worked examples

### 5.1 A legal pack

*Retrieval questions:* "Has anything ruled on this issue before?" ·
"What distinguishes our case from the precedent against us?" · "Which
statutes does this argument depend on?"

```rust
pack! {
    name: "acme-pack-legal", version: "1.0.0", kernel_min: "1.0.0",

    memory_types! {
        Case, Statute, Ruling, Motion, Brief, LegalNote,
    }
    entity_types! { Court, Jurisdiction, StatuteRef, Party, Judge, }

    kinds! {
        // A ruling's relationship to the law and to other rulings
        Cites           => bucket: Context,  inverse: CitedBy,         bi: false, default_strength: 0.85,
        Overrules       => bucket: Causal,   inverse: OverruledBy,     bi: false, default_strength: 0.95,
        Distinguishes   => bucket: Learning, inverse: DistinguishedBy, bi: false, default_strength: 0.80,
        Supports        => bucket: Solution, inverse: SupportedBy,     bi: false, default_strength: 0.80,
        Opposes         => bucket: Solution, inverse: OpposedBy,       bi: false, default_strength: 0.80,
        CommentsOn      => bucket: Context,  inverse: CommentedOn,     bi: false, default_strength: 0.70,
    }
    type_triples! {
        Cites         => (Case | Ruling | Brief | Motion, Statute | Ruling | Case),
        Overrules     => (Ruling, Ruling),
        Distinguishes => (Brief | Case, Case | Ruling),
        Supports      => (Brief | Ruling, Case),
        Opposes       => (Brief | Ruling, Case),
        CommentsOn    => (LegalNote, Case | Ruling | Motion),
    }
    crepe_rules! {
        // An argument's authority chains through citations to overruled law
        undermined(a, c) <- edge(a, b, Cites), edge(b, c, Overrules);
    }
}
```

Design notes: `Overrules` is definitional — 0.95, and triples pinned to
`(Ruling, Ruling)`. `Supports`/`Opposes` reuse the Solution bucket
because a brief *answers* a case the way a fix answers a problem, so
kernel problem-solution bridging semantics apply. The one rule makes
"this citation leans on dead law" a traversal instead of a research
task.

### 5.2 A clinical pack

*Retrieval questions:* "What interventions were tried for this
condition and what happened?" · "Does this treatment conflict with
anything in this patient's history?" · "What guideline covers this
presentation?"

```rust
pack! {
    name: "acme-pack-clinical", version: "1.0.0", kernel_min: "1.0.0",

    memory_types! {
        Symptom, Diagnosis, Treatment, Guideline, AdverseEvent, ClinicalNote,
    }
    entity_types! { Condition, Medication, Procedure, PatientCohort, Trial, }

    computed_only_kinds! { ComorbidityCluster, }

    kinds! {
        Indicates         => bucket: Causal,   inverse: IndicatedBy,     bi: false, default_strength: 0.75,
        Treats            => bucket: Solution, inverse: TreatedBy,       bi: false, default_strength: 0.85,
        Contraindicates   => bucket: Causal,   inverse: ContraindicatedBy, bi: false, default_strength: 0.95,
        Complicates       => bucket: Causal,   inverse: ComplicatedBy,   bi: false, default_strength: 0.80,
        InformedBy        => bucket: Context,  inverse: Informed,        bi: false, default_strength: 0.80,
        Supersedes        => bucket: Solution, inverse: SupersededBy,    bi: false, default_strength: 0.90,
    }
    type_triples! {
        Indicates       => (Symptom | ClinicalNote, Diagnosis),
        Treats          => (Treatment | Guideline, Diagnosis | Symptom),
        Contraindicates => (Treatment, Treatment | AdverseEvent),
        Complicates     => (Diagnosis, Diagnosis | Treatment),
        InformedBy      => (ClinicalNote | Treatment, Guideline),
        Supersedes      => (Guideline, Guideline),
    }
    crepe_rules! {
        // A treatment both treating A and contraindicating B, where B complicates A, is a flag
        interaction_risk(t, a) <- edge(t, a, Treats), edge(t, b, Contraindicates), edge(b, a, Complicates);
    }
}
```

Design notes: `Contraindicates` at 0.95 with pinned endpoints — safety
edges should be loud and hard to fire accidentally. `Supersedes` maps
onto the kernel's supersession machinery (`Replaces`-class semantics),
so stale guidelines get marked with successors automatically.
`ComorbidityCluster` is Dreams-only: clustering is consolidation's job.

### 5.3 A security-operations pack

*Retrieval questions:* "Which assets does this advisory affect?" ·
"What mitigates this class of vulnerability, and what has actually
been verified?" · "Did anything we shipped last quarter expose this?"

```rust
pack! {
    name: "acme-pack-secops", version: "1.0.0", kernel_min: "1.0.0",

    memory_types! {
        Vulnerability, Advisory, Control, Asset, Incident, Postmortem,
    }
    entity_types! { Service, Dependency, CVE, Team, Environment, }

    kinds! {
        Affects      => bucket: Context,    inverse: AffectedBy,   bi: false, default_strength: 0.85,
        Exploits     => bucket: Causal,     inverse: ExploitedBy,  bi: false, default_strength: 0.90,
        Mitigates    => bucket: Solution,   inverse: MitigatedBy,  bi: false, default_strength: 0.85,
        Verifies     => bucket: Quality,    inverse: VerifiedBy,   bi: false, default_strength: 0.80,
        Exposes      => bucket: Integration,inverse: ExposedBy,    bi: false, default_strength: 0.80,
        Caused       => bucket: Causal,     inverse: Caused,       bi: false, default_strength: 0.85,
    }
    type_triples! {
        Affects   => (Vulnerability | Advisory, Asset),
        Exploits  => (Incident, Vulnerability),
        Mitigates => (Control, Vulnerability | Advisory),
        Verifies  => (Postmortem | Control, Control),
        Exposes   => (Asset, Vulnerability),
        Caused    => (Vulnerability, Incident),
    }
    crepe_rules! {
        // An unverified mitigation on an exploited vulnerability is open risk
        open_risk(c, v) <- edge(c, v, Mitigates), edge(i, v, Exploits), !edge(_, c, Verifies);
    }
}
```

Design notes: `open_risk` is the whole product of the pack — one rule
turns "what's actually covered" into a query. Note the `!` prefix —
Crepe's stratified negation — used sparingly for absence-of-
verification; read the kernel rule docs on negation scoping before
leaning on it.

## 6. Authoring workflow

Mechanics, from empty directory to deployed:

1. **Create the crate.** Workspace member, `exocortex-kernel` as its
   only internal dependency. Copy dev-v1's `Cargo.toml` as the shape.
2. **Write the `pack!` block** (§2) and force-link it in the binaries
   that should carry it (`client`, `server`) with the `black_box` idiom.
3. **Let the compiler check you.** The macro rejects unknown type names
   in triples, duplicate names within the pack, and missing
   kernel-constant bindings. Registration (`load_registered_packs`)
   additionally rejects duplicate names *across* packs and assembles
   the per-pack id offsets.
4. **Write the three tests every pack should have** (steal from
   `kernel/tests/pack_registration.rs` and
   `pack-dev-v1/tests/loads_correctly.rs`):
   - *assembles*: `load_registered_packs()` succeeds and resolves every
     type/kind name you authored;
   - *fingerprint is stable*: hash the assembled ontology and pin it in
     a golden file — an accidental ontology change should fail CI;
   - *round-trip*: a sample draft + edge validates clean, a
     triple-violating edge rejects with `InvalidTypeTriple`, and a
     computed-only kind rejects at the boundary.
5. **Update the generated artifacts.** `cargo xtask fingerprint`
   (golden), `cargo xtask gen-schemas` (tool schemas), `cargo xtask
   gen-playbook` (the agent's kind table — your pack's assertable kinds
   join dev-v1's in the generated tables).
6. **Ship it, additively.** Every producer's batches must carry the new
   fingerprint; the ingest boundary hard-rejects mismatches, and
   clients fail fast at boot against a backend whose fingerprint
   differs. That strictness is the feature: no silent ontology skew
   inside a deployment. Removing or renaming things later strands
   stored rows — v1 has no migration tooling, by scope not by accident.

## 7. The rules of the road

| Do | Don't |
|---|---|
| Design from retrieval questions | Model the data you happen to have |
| ~6–14 memory types, stable names | Per-type payload schemas; type sprawl |
| Constrain triples everywhere the domain knows | `(_, _)` by default |
| Derive with rules; store what was *learned* | Ask the agent to assert derivable facts |
| Additive evolution, fingerprint-pinned | Rename/remove types in a live deployment |
| `computed_only_kinds!` for consolidation output | Let producers assert similarity-class edges |
| Kernel consts for kinds the universal rules need | Bind kernel consts casually (coverage is asserted) |

---

*The authoritative spec for the machinery behind this guide is the
[core PRD](prd/exocortex-core-prd.md) (§7 ontology, §10 rules, §11 MCR²);
the reference pack is
[`exocortex-pack-dev-v1`](../crates/exocortex-pack-dev-v1/src/lib.rs).
This guide lives at `docs/ONTOLOGY_GUIDE.md`.*
