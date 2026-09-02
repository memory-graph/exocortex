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
if [ -n "$POSTGRES_URL" ]; then
  cargo test -p exocortex-adapter-postgres --features integration --test cdc_live -- --nocapture
else
  echo "live Postgres CDC suite UNEXECUTED (POSTGRES_URL unset)"
fi
cargo xtask write-path-parity
cargo xtask dead-enforcement
cargo xtask auth-coverage
cargo xtask artifact-equivalence
cargo xtask acceptance-coverage
cargo xtask deployment-acceptance
cargo xtask ontology-surfaces
