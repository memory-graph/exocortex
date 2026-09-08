#!/usr/bin/env bash
# Canonical local/CI prerequisite for any publish or tagged release.
set -euo pipefail

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo check -p exocortex-server --all-targets --features fastembed
cargo test --workspace --features exocortex-adapter-sdk/testing,exocortex-server/otlp --no-fail-fast
cargo deny check
cargo xtask kernel-purity
cargo xtask fingerprint
cargo xtask gen-schemas
cargo xtask gen-playbook
cargo xtask no-llm
cargo xtask proto-sync
cargo xtask signing-hygiene
cargo xtask compatibility-policy
cargo xtask seam-inventory
cargo xtask adapter-contract
cargo xtask metrics-hygiene
cargo xtask wire-standalone
cargo xtask bench
cargo xtask storage-conformance
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
cargo xtask write-path-parity
cargo xtask dead-enforcement
cargo xtask auth-coverage
cargo xtask artifact-equivalence
cargo xtask acceptance-coverage
cargo xtask deployment-acceptance
cargo xtask ontology-surfaces
