#!/usr/bin/env bash
# Canonical local/CI prerequisite for any publish or tagged release.
set -euo pipefail

# REL2: per-phase wall-time instrumentation. Verdicts are untouched —
# each phase runs exactly as before; the timer only wraps it. Summary
# lands at exit (and in the log) so the budget is measurable, not
# guessed.
# REL2: line-tables-only debuginfo for every matrix build — panic line
# numbers survive, most debuginfo codegen cost goes away. Verdicts are
# unaffected (this is a compile-profile knob, not a test knob).
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-1}"
export CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-1}"

# REL4 lever 3: the measured 12.65h matrix ran beside Docker Desktop's
# VM (~37% of a core for the entire run). Contention is the operator's
# to remove; this warns, never fails.
if pgrep -q -f "Docker.app" 2>/dev/null; then
  echo "[matrix] WARNING: Docker Desktop is running — quit it for a clean cut (measured ~37%-core contention, REL4)" >&2
fi

# REL6: darwin first-spawn health canary. A freshly-built binary's FIRST
# exec pays macOS code-signature assessment; when syspolicyd's queue is
# degraded that assessment wedges indefinitely (zero CPU parked in
# _dyld_start) and the matrix's test phase stalls for hours with no
# diagnosis at the point of wedge. Fail fast instead: compile a tiny
# never-before-seen binary, time its first exec, and refuse to start
# when it exceeds the bound. EXOCORTEX_SKIP_SPAWN_CANARY=1 opts out
# (CI's linux runners never take this branch).
if [ "$(uname -s)" = "Darwin" ] && [ -z "${EXOCORTEX_SKIP_SPAWN_CANARY:-}" ]; then
  _canary_src="$(mktemp -t exocortex_spawn_canary).rs"
  _canary_bin="${_canary_src%.rs}"
  printf 'fn main() {}\n' > "$_canary_src"
  if ! rustc --edition 2021 --crate-name exocortex_spawn_canary \
       -o "$_canary_bin" "$_canary_src" >/dev/null 2>&1; then
    echo "[matrix] spawn canary: rustc unavailable — skipping" >&2
  else
    _canary_max="${EXOCORTEX_SPAWN_CANARY_MAX_SECS:-30}"
    _canary_t0=${SECONDS:-0}
    "$_canary_bin" &
    _canary_pid=$!
    _canary_ok=""
    while [ $(( ${SECONDS:-0} - _canary_t0 )) -lt "$_canary_max" ]; do
      if ! kill -0 "$_canary_pid" 2>/dev/null; then _canary_ok=1; break; fi
      sleep 1
    done
    if [ -n "$_canary_ok" ]; then
      wait "$_canary_pid" 2>/dev/null || true
      printf '[matrix] %2ds  spawn-canary (first-exec assessment)\n' "$(( ${SECONDS:-0} - _canary_t0 ))" >&2
    else
      kill -9 "$_canary_pid" 2>/dev/null || true
      echo "[matrix] spawn canary FAILED: a fresh binary's first exec exceeded ${_canary_max}s — the macOS process-assessment queue is degraded (REL6). Reboot the machine, or publish with PUBLISH_VERIFY=ci (REL5)." >&2
      rm -f "$_canary_src" "$_canary_bin"
      exit 1
    fi
  fi
  rm -f "$_canary_src" "$_canary_bin"
fi

_MATRIX_START=${SECONDS:-0}
_matrix_total_before=$_MATRIX_START
phase() {
  local name="$1"; shift
  local t0=${SECONDS:-0}
  "$@"
  local rc=$?
  printf '[matrix] %4ds  %s\n' "$(( ${SECONDS:-0} - t0 ))" "$name" >&2
  return $rc
}
# set -e kills the function on failure, so also record failures via EXIT.
_matrix_summary() {
  printf '[matrix] %4ds  TOTAL (phases instrumented from second %s)\n' \
    "$(( ${SECONDS:-0} - _MATRIX_START ))" "$_MATRIX_START" >&2
}
trap _matrix_summary EXIT

phase "fmt" cargo fmt --all -- --check
phase "clippy" cargo clippy --workspace --all-targets -- -D warnings
phase "check-fastembed" cargo check -p exocortex-server --all-targets --features fastembed
# REL2: compile and run attributed separately — the budget's biggest
# phase deserves to say which half it spent.
# REL4: nextest is the DEFAULT runner when installed (parallel
# execution across test binaries; doctests stay on cargo test).
# EXOCORTEX_TEST_RUNNER=cargo opts out; a machine without cargo-nextest
# falls back to cargo test unchanged, so the script never depends on a
# tool the tree doesn't ship.
if [ "${EXOCORTEX_TEST_RUNNER:-nextest}" != "cargo" ] && command -v cargo-nextest >/dev/null 2>&1; then
  # No --profile: cargo-nextest 0.9.148 has no built-in `ci` profile,
  # and CI's proven invocation (ci.yml) runs the default profile.
  phase "test-workspace-compile" cargo nextest run --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-fail-fast
  phase "test-workspace-doctests" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --doc --no-fail-fast
else
  phase "test-workspace-compile" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-run
  phase "test-workspace-run" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-fail-fast
fi
# REL4: single-execution trust — the workspace phase above just ran
# every suite the four duplicate-executing gates would re-run on this
# exact tree (same or superset features). The marker is exported ONLY
# here, only after that phase went green (set -e); seam-inventory,
# adapter-contract, storage-conformance's in-memory leg, and
# write-path-parity then run just their static assertions, each
# printing an explicit "trusted from the matrix workspace phase" line.
# Standalone gate invocations never see it and always execute suites;
# the live backend legs are not covered by the workspace phase and run
# regardless.
export EXOCORTEX_MATRIX_TRUST=1
phase "deny" cargo deny check
phase "xtask:kernel-purity" cargo xtask kernel-purity
phase "xtask:fingerprint" cargo xtask fingerprint
phase "xtask:gen-schemas" cargo xtask gen-schemas
phase "xtask:gen-playbook" cargo xtask gen-playbook
phase "xtask:no-llm" cargo xtask no-llm
phase "xtask:proto-sync" cargo xtask proto-sync
phase "xtask:signing-hygiene" cargo xtask signing-hygiene
phase "xtask:compatibility-policy" cargo xtask compatibility-policy
phase "xtask:seam-inventory" cargo xtask seam-inventory
phase "xtask:adapter-contract" cargo xtask adapter-contract
phase "xtask:metrics-hygiene" cargo xtask metrics-hygiene
phase "xtask:wire-standalone" cargo xtask wire-standalone
phase "xtask:bench" cargo xtask bench
phase "xtask:storage-conformance" cargo xtask storage-conformance
# Compile every integration-gated live suite even when its backend or
# token is absent: a gated suite that no longer compiles must fail the
# matrix HERE, not at the first release run that happens to carry the
# token (REL1 — four suites had rotted invisibly behind their gates).
phase "rel1-compile-sweep" cargo test -p exocortex-adapter-github -p exocortex-adapter-linear \
  -p exocortex-adapter-postgres -p exocortex-storage \
  -p exocortex-dreams -p exocortex-cluster \
  --features exocortex-adapter-github/integration,exocortex-adapter-linear/integration,exocortex-adapter-postgres/integration,exocortex-storage/integration,exocortex-dreams/integration,exocortex-cluster/integration \
  --no-run
if [ -n "${POSTGRES_URL:-}" ]; then
  cargo test -p exocortex-adapter-postgres --features integration --test cdc_live -- --nocapture
else
  echo "live Postgres CDC suite UNEXECUTED (POSTGRES_URL unset)"
fi
# The SaaS adapters' live legs skip inside libtest, whose output capture
# hides the skip line from a passing run — echo the leg status at the
# shell level like the Postgres leg so a green run never implies live
# coverage it did not execute.
if [ -n "${GITHUB_TOKEN:-}" ]; then
  cargo test -p exocortex-adapter-github --features integration --test github_live -- --nocapture
else
  echo "live GitHub adapter suite UNEXECUTED (GITHUB_TOKEN unset)"
fi
if [ -n "${LINEAR_API_KEY:-}" ]; then
  cargo test -p exocortex-adapter-linear --features integration --test linear_live -- --nocapture
else
  echo "live Linear adapter suite UNEXECUTED (LINEAR_API_KEY unset)"
fi
# R14 (T6): the D44-S2 live attach suite (two concurrent wrappers, one
# store) runs whenever the bundled runtime is resolvable — it was env-
# gated with NO gate ever exporting the env, so attach could regress
# behind a green matrix. Loud skip otherwise, the storage-conformance
# pattern.
RUNTIME_DIR="${EXOCORTEX_STANDALONE_RUNTIME:-"${CARGO_HOME:-$HOME/.cargo}/share/exocortex/standalone"}"
if [ -x "$RUNTIME_DIR/redis-server" ] && [ -f "$RUNTIME_DIR/falkordb.so" ]; then
  EXOCORTEX_REDIS_SERVER="$RUNTIME_DIR/redis-server" \
  EXOCORTEX_FALKORDB_MODULE="$RUNTIME_DIR/falkordb.so" \
    cargo test -p exocortex-client --test standalone_wrapper
else
  echo "live standalone attach suite UNEXECUTED (no bundled runtime at $RUNTIME_DIR)"
fi
phase "xtask:write-path-parity" cargo xtask write-path-parity
phase "xtask:dead-enforcement" cargo xtask dead-enforcement
phase "xtask:auth-coverage" cargo xtask auth-coverage
phase "xtask:artifact-equivalence" cargo xtask artifact-equivalence
phase "xtask:acceptance-coverage" cargo xtask acceptance-coverage
phase "xtask:deployment-acceptance" cargo xtask deployment-acceptance
phase "xtask:ontology-surfaces" cargo xtask ontology-surfaces
