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
# REL2: CI runs the suites through nextest (parallel execution across
# test binaries; doctests stay on cargo test) by exporting
# EXOCORTEX_TEST_RUNNER=nextest — locally the default stays cargo test
# so the script never depends on a tool the tree doesn't ship.
if [ "${EXOCORTEX_TEST_RUNNER:-}" = "nextest" ] && command -v cargo-nextest >/dev/null 2>&1; then
  phase "test-workspace-compile" cargo nextest run --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-fail-fast --profile ci
  phase "test-workspace-doctests" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --doc --no-fail-fast
else
  phase "test-workspace-compile" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-run
  phase "test-workspace-run" cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/testing,exocortex-server/otlp --no-fail-fast
fi
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
