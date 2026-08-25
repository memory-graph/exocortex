# Publishing

Exocortex publishes to two surfaces: crates.io (the library crates) and
this repository (binaries via `cargo install --git`, plus Docker images
planned for `ghcr.io`).

## crates.io

All 13 crate names were verified available (2026-08-25). Crates share one
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
- `repository` points at `memory-graph/exocortex`
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
