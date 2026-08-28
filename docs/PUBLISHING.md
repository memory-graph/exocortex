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
  cross-platform release builds (macOS arm64 and Intel via macos-14, Linux
  x64) for all three binaries, sha256 checksums, and an
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
  CI and release builds install protobuf 28.3 from the upstream release ZIP
  through `scripts/install-protoc.sh`: the version and all supported-host
  SHA-256 values are committed, so neither a moving package repository nor an
  unchecked archive can change generated build inputs. The Docker build uses
  the same fixed Linux archives through BuildKit's `ADD --checksum`, performs
  no package-manager operation, and runs on digest-pinned Rust, BusyBox, and
  distroless non-root/CA-root images. This keeps compiler tools, extraction
  tools, runtime libraries, and trust roots independent of mutable package
  repositories while retaining readable image names beside their OCI index
  digests.
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

`exocortex-client` directly depends on Hyper and Hyper-Rustls for its SSE
subscriber. The client owns the incremental SSE framing so it can reject an
oversized event while bytes arrive; the former eventsource helper buffered an
entire attacker-controlled event before application admission could run.

The optional `exocortex-ingest/fastembed` feature exact-pins fastembed 5.2 and
does not enable its Hugging Face downloader. Releases package five files from
`Xenova/bge-small-en-v1.5` commit
`ea104dacec62c0de699686887e3f920caeb4f3e3`; Docker BuildKit, the release fetch
script, and the runtime all enforce the same SHA-256 digests. The server loads
only those verified in-memory bytes through fastembed's user-defined-model API,
stamps the full upstream commit on every vector, and fails closed without the
sidecar. `scripts/fetch-embedding-model.sh` builds the sidecar for release
archives, and `scripts/release-install.sh` installs it under
`${CARGO_HOME:-$HOME/.cargo}/share/exocortex/models`. There is no first-start
network acquisition or mutable-revision fallback.

Fastembed 3.14's hf-hub 0.3 path failed clean production startup with a
relative redirect. Moving to 5.2 provides the maintained offline
user-defined-model API used by the immutable sidecar; this PRD dependency
deviation is recorded in `docs/MILESTONE_REPORT.md`. Runtime artifact
verification uses the canonical `exocortex_wire::signing` SHA-256 helper; the
optional feature pins `image`
0.25.5 as a resolver constraint because fastembed 5.2 includes its image
surface unconditionally and later 0.25 patch releases exceed the pinned Rust
1.85 MSRV; Exocortex does not call the image API.

Fastembed's pinned ONNX Runtime download feature still selects `ort-sys`'s
build-time native-TLS downloader. The digest-pinned Bookworm builder supplies
and explicitly verifies `pkg-config` plus OpenSSL for that build-only path; the
distroless runtime image does not carry those tools.
