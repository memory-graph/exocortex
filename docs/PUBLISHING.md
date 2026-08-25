# Publishing

Exocortex publishes to two surfaces: crates.io (the library crates) and
this repository (binaries via `cargo install --git`, plus Docker images
planned for `ghcr.io`).

## crates.io

All 14 crate names were verified available (2026-08-25; exocortex-adapter-sdk joined with A1). Crates share one
workspace version and must publish in dependency order — `cargo release`
derives the order from the graph:

```sh
cargo install cargo-release
cargo release 0.1.0 --dry-run     # verify the plan
cargo release 0.1.0 --execute     # bump, publish in order, tag, push
```

Manual equivalent (order matters — same-batch path deps are fine, but a
crate must exist on crates.io before its dependents verify):

```
exocortex-kernel
  -> exocortex-pack-dev-v1, exocortex-wire
  -> exocortex-adapter-sdk
  -> exocortex-storage
  -> exocortex-cache, exocortex-reasoning
  -> exocortex-cluster
  -> exocortex-ingest, exocortex-ops
  -> exocortex-dreams
  -> exocortex-server, exocortex-client, exocortex-worker
```

Per crate: `cargo publish -p <name>` (run from the repo root so the
workspace license/repository metadata applies).

`xtask` is `publish = false` — it is build tooling and never ships.

## Requirements already in place

- Workspace `license = "AGPL-3.0-or-later"` + committed `LICENSE`
- Every crate inherits `repository.workspace = true` (memory-graph/exocortex)
- Every crate manifest carries a `description`
- Internal deps use `path + version` (crates.io rewrites to the registry
  version at publish)
- `cargo publish -p exocortex-kernel --dry-run` passes (verified:
  packages, compiles from the packaged tarball, uploads cleanly)

## Binaries

- **git install**: `cargo install --git
  https://github.com/memory-graph/exocortex --bin exocortex-node` (needs
  `protoc`)
- **cargo-dist** (planned): release builds for macOS arm64/x64 and Linux
  x64 attached to GitHub Releases on tag push — removes the protoc/Rust
  toolchain requirement for operators
- **ghcr.io** (planned): push the existing Dockerfile image as
  `ghcr.io/memory-graph/exocortex-node`

## exocortex-adapter-sdk

Workspace member; publishes right after `exocortex-wire` in
`scripts/publish.sh` ORDER. The `testing` feature (mock IngestService)
is off by default so downstream dep trees stay clean; adapter authors
opt in with `features = ["testing"]` for their own tests. New dev-deps
recorded with the crate: `tempfile` (adapter-sdk), `tar` + `flate2`
(xtask's wire-standalone gate), `axum`/`futures` optional under
`testing`.
