# Bug PRD — standalone `end_session` submit hang (intermittent)

**Status:** closed (2026-09-01). **Found by:** the v0.3.0 release
validation (the first run of the packaged-standalone live target since
0.2.2; every local gate run before it skipped the leg loudly with exit
77 because no bundled runtime was present — by design, which is also
how this stayed hidden).

## Root cause (the intermittent half)

rmcp 0.1.x treats stdin EOF as an immediate shutdown: its serve loop
breaks on the first `None` from the input stream and the process exits,
dropping every tool-call task it spawned — INCLUDING requests it had
already read from stdin but whose handler task had not yet been polled
(no deadline, no error, no response). The standalone live harness pipes
all requests at once and closes stdin, so any `end_session` still on
the wire (lazy gRPC connect + HTTP/2 handshake + register_source +
submit against a freshly booted node routinely exceeds the EOF window)
lost its response: `end_session` never answered, `backend_lsn` stayed
0, the node sat idle, and the script's grep failed. The "pass/fail/fail
on three clean runs" intermittency was the EOF race, not an HTTP/2
fault — with stdin held open the same submit round-trips in ~40ms
(verified with h2 frame-level tracing). The earlier "leading
hypotheses" (HTTP/2 handshake race in the shared listener; channel
lost-wakeup) are disproven: the wire path is healthy end to end.

## Fix

1. **EOF-draining stdio reader** (`client/src/eof_drain.rs`): stdin EOF
   is withheld until the process is quiet — no tool call in flight, and
   no dispatch transition in the last 300ms grace window (covering
   requests rmcp read but has not polled yet) — bounded by a 55s budget
   exceeding the end_session gRPC deadlines (20s register + 30s
   submit), so a normal, slow, or deadline-failing submit always
   reaches the harness before exit.
2. **Deterministic regression without the bundled runtime**
   (`client/tests/backend_eof_drain.rs`): an in-process backend node
   (in-memory storage) sits behind a delaying TCP proxy (400ms per
   backend chunk); the real client binary dispatches `end_session` and
   stdin closes the instant after. The submit cannot complete before
   EOF arrives, so the test fails deterministically without the drain
   (proved by stash: `the in-flight submit MUST be answered before
   exit: Disconnected`) and passes with it (`accepted:1`).
3. **The live script sequences its own calls**
   (`scripts/test-standalone-live.sh`): MCP tool calls are concurrent —
   a search piped behind an `end_session` is NOT ordered after it, so
   the old heredoc raced the write by construction. The script now
   writes, waits for the commit + SSE delivery, then searches, and
   asserts BOTH the `accepted:1` ack and the marker round-trip. Eight
   consecutive clean runs pass.

The deterministic half (dropped mortgage pack registration) was fixed
earlier with the binary-level canary; see below. The client-side gRPC
deadlines (a wedged submit surfaces as DeadlineExceeded) and the loud
script failure modes shipped alongside this PRD remain in force.

## The two defects found by the same validation

### 1. Dropped pack registration (FIXED in the same commit as this PRD)

The node binary loaded ONLY `exocortex-pack-dev-v1` while every client
loaded the composed set. A pack crate that nothing in the binary
references has its `inventory` registration dropped by the linker;
dev-v1 survived via the §23 #25 anchor, the mortgage pack had no
reference. The node pinned the dev-v1-only compatibility fingerprint
(`a492546d…`), the client's SSE envelope check (exact compatibility
equality, OC-PRD D2) rejected every seed, and R6-B32's fail-closed
startup surfaced it as `backend did not provide an initial graph seed
within 15s`.

Fix: an explicit `black_box` reference per shipped pack in the node's
`main.rs`, plus `the_binary_registers_every_shipped_pack` — a canary
test compiled INTO the binary's own target, the only place that links
exactly like the binary. Every future pack needs both.

### 2. The intermittent submit hang (OPEN — this PRD's subject)

Even with the fingerprint split fixed, `end_session` against a live
standalone node sometimes never answers: the client's MCP response for
the call is silently dropped (no error, no timeout at the MCP layer),
nothing commits (`backend_lsn` stays 0), and the wrapper's own
`scripts/test-standalone-live.sh` fails at its silent `grep` (exit 1).

## Evidence (2026-08-31, macOS arm64, debug builds)

- Reproduction: `test-standalone-live.sh` with
  `EXOCORTEX_REDIS_SERVER`/`EXOCORTEX_FALKORDB_MODULE` pointing at the
  pinned runtime. Observed: pass, then fail, then fail on three
  consecutive clean runs (orphan processes ruled out).
- The node is idle during the hang (`sample` shows only parked blocking
  threads; no ingest/storage frames) — the gRPC request never reaches
  the service, or its response is lost before dispatch.
- The hanging client parks on its **current_thread** tokio runtime
  awaiting the submit future; the client's `end_session` gRPC calls
  (`register_source` then `submit`) carry **no deadline**, so a lost
  request hangs the tool call until the harness gives up.
- SSE + HTTP operations to the same listener work during the hang, so
  the listener itself is alive; the failure is specific to the
  HTTP/2 (tonic) exchange.
- The same client code path against `--mode backend-node` servers is
  covered by `e2e_chain`/`sync` suites that are green in CI, pointing
  at the standalone serving path or its timing.

## Leading hypotheses (unverified)

1. An HTTP/2 handshake/stream race in the shared `http+grpc` listener
   under the standalone plaintext-loopback ingress.
2. A lost-wakeup between the client's single-threaded runtime and the
   channel connect (first call lazily connects) — the connect future
   pends forever.

## Fix requirements

- Every client gRPC call carries a deadline (the ops layer already
  enforces R-R3 budgets; the raw tonic calls in
  `tools/end_session.rs` predate that discipline). A hung submit must
  surface as `DeadlineExceeded`, never a silent drop.
- `test-standalone-live.sh` must fail LOUDLY (named step output), never
  a bare `set -e` grep exit.
- A deterministic regression test for the standalone submit path that
  runs without the bundled runtime (in-process standalone node + client
  against the loopback listener), so this leg is never dark again.

## Reproduction shortcuts

- The pinned runtime: `scripts/fetch-standalone-runtime.sh` (SHA-512
  verified; boots cleanly standalone — redis + FalkorDB 4.16.3 start
  fine, ruling out the runtime bits).
- Node: `exocortex-node --mode mcp-standalone … --standalone-runtime-file`,
  then the client with `--backend` from that file, then
  `exocortex.end_session`.
