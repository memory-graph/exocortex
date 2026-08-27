# Publishing

Exocortex publishes to two surfaces: crates.io (the library crates) and
this repository (binaries via `cargo install --git`, plus Docker images
planned for `ghcr.io`).

## crates.io

All 14 crate names were verified available (2026-08-25;
exocortex-adapter-sdk joined with A1). Crates share one workspace version and
must publish in dependency order. The supported entry point is the fail-closed
repository script:

```sh
PUBLISH_VERSION=0.2.2 scripts/publish.sh
```

It refuses dirty manifests/lockfiles and mixed package versions, runs the full
mandatory correctness prerequisite before changing a manifest or contacting
crates.io, publishes without `--no-verify`, and restores temporary
dev-dependency edits byte-for-byte. Unrelated worktree changes are neither
rejected nor touched. Its disposable regression is
`bash scripts/tests/publish.sh`.

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

- **Installer (primary)**: tag push (`git tag v0.2.2 && git push
  memory-graph v0.2.2`) triggers `.github/workflows/release.yml` —
  cross-platform release builds (macOS arm64 via macos-14, Intel via
  macos-13, Linux x64) for all three binaries, sha256 checksums, and an
  auto-generated `install.sh` attached to the GitHub Release:

  ```sh
  curl -LsSf     https://github.com/memory-graph/exocortex/releases/latest/download/install.sh | sh
  ```

  The script resolves `latest` (or honors `$INSTALL_VERSION`), downloads the
  archive and its published SHA-256 file, refuses a mismatch before extraction,
  and installs into `$CARGO_HOME/bin`. No Rust
  toolchain or protoc required — that was the point.
  The build and release jobs cannot start until `scripts/verify-release.sh`
  and the disposable publish regression pass on the tagged commit.
- **git install**: `cargo install --git
  https://github.com/memory-graph/exocortex --bin exocortex-node` (needs
  `protoc`)
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

`exocortex-server` directly depends on `axum-server` with its Rustls feature.
The shared backend listener serves HTTP, SSE, and gRPC on one TLS socket;
Axum's built-in `serve` helper accepts only a plaintext `TcpListener`, so the
TLS listener is an explicit runtime dependency rather than application code
reimplementing certificate parsing and HTTP/2 negotiation.

`exocortex-server` also depends directly on `rustls` to select the Ring crypto
provider before constructing that listener. Transitive workspace consumers
enable both supported providers, so Rustls cannot infer one from the unified
feature set at runtime.
