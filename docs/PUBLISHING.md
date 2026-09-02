# Publishing

Exocortex publishes to two surfaces: crates.io (the library crates) and
this repository (binaries via `cargo install --git`, plus Docker images
planned for `ghcr.io`).

## crates.io

All 14 crate names were verified available (2026-08-25;
exocortex-adapter-sdk joined with A1).

**0.3.0 (2026-08-31)** — the adapter-contract wave, additive throughout:
wire gains the `Preflight` and `GetValidationManifest` RPCs and the
validation-manifest module (+`serde_json`, recorded below); the SDK
gains `AdapterSession::preflight` and the manifest interpreter; the
operation registry gains `preflight_batch` and `resolve_contradiction`
(+`prost-types`); the server gains `--export-corpus` (+`async-trait`
handle); the wire producer-kind enum gains `EXTRACTED` (value 6 — older
servers reject it fail-closed, the correct rolling-upgrade behavior).
The ontology fingerprint is unchanged: no pack content moved.
**0.2.2 (2026-08-27)** — memory backup/restore (`--export`/`--import`). Crates share one workspace version and
must publish in dependency order. The supported entry point is the fail-closed
repository script:

```sh
PUBLISH_VERSION=0.3.0 scripts/publish.sh
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
  -> exocortex-pack-dev-v1, exocortex-pack-mortgage-v1, exocortex-wire
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
- `exocortex-storage` depends on `exocortex-wire` only to reuse the canonical
  signing-owned content digest for durable Dreams settlement effect identity;
  this prevents a second checksum implementation at the storage boundary.
- `exocortex-ops` depends on `steel-core` (PX2) to execute pack-registered
  Function `scheme` bodies through the same embedded interpreter the
  reasoning crate's explain engine uses; JSON is native at this operation
  boundary, and CR-8 keeps the reasoning crate's rule path
  serialization-free. No new workspace-external dependency: `steel-core`
  was already a workspace dependency of `exocortex-reasoning`.
- `exocortex-adapter-git` (D18) adds NO new external dependencies: it
  shells out to the local `git` binary (offline, deterministic) and
  rides `exocortex-adapter-sdk` + `blake3` (both already workspace deps);
  `tempfile` is a dev-only test fixture dependency. It publishes
  independently of the server (adapter crates are leaf binaries).
- `exocortex-pack-mortgage-v1` is a workspace dependency of the server,
  client, xtask, and ops (dev) so its verbs ride the one operation
  registry in every linked binary (PX2 §4.3); it is the second pack and
  publishes in the pack wave alongside dev-v1.
- `cargo publish -p exocortex-kernel --dry-run` passes (verified:
  packages, compiles from the packaged tarball, uploads cleanly)

The release archive's only non-Cargo runtime dependency is the official
`@falkordblite/{linux-x64,darwin-arm64}` package at
`8.2.3-falkordb.4.16.3`. It is required to satisfy the PRD's zero-setup
standalone topology: it supplies the matched Redis server and FalkorDB module
that the existing supervisor launches. `scripts/fetch-standalone-runtime.sh`
pins both platform archives by SHA-512; the application performs no runtime
download. The official package's license declaration and exact component
source/license coordinates ride the packaged `RUNTIME-MANIFEST.txt`.

## Binaries

- **Installer (primary)**: tag push (`git tag v0.3.0 && git push
  memory-graph v0.3.0`) triggers `.github/workflows/release.yml` —
  cross-platform release builds (macOS arm64 on `macos-15`, Intel on the
  native `macos-15-intel` runner, and Linux x64 on the `ubuntu-22.04`
  builder) for all three binaries,
  sha256 checksums, and an
  auto-generated `install.sh` attached to the GitHub Release:

  ```sh
  curl -LsSf     https://github.com/memory-graph/exocortex/releases/latest/download/install.sh | sh
  ```

  The script resolves `latest` (or honors `$INSTALL_VERSION`), downloads the
  archive and its published SHA-256 file, refuses a mismatch before extraction,
  and installs into `$CARGO_HOME/bin`. Linux x64 and macOS arm64 archives also
  carry the SHA-512-pinned official `@falkordblite` Redis 8.2.3/FalkorDB
  4.16.3 runtime under `$CARGO_HOME/share/exocortex/standalone`; release CI
  first executes the extracted archive's wrapper and sibling runtime through
  the write/read/restart target — natively on `macos-15` (the runtime's
  darwin-arm64 module declares `minos 15.0`), and on Linux inside the
  digest-pinned `falkordb/falkordb` trixie userland because the runtime's
  redis-server needs glibc >= 2.38 while the ubuntu-22.04 builder (kept so
  the shipped binaries keep their own glibc 2.35 floor) has 2.35 (D29).
  The runtime is self-contained on macOS: the fetcher bundles the module's
  Homebrew dependencies (libomp, openssl@3 — pinned ghcr.io bottle blobs,
  the last arm64_sequoia builds at the macOS 15 floor) beside the module,
  and the supervisor points the store child's DYLD_LIBRARY_PATH at them,
  so no Homebrew install is required (D29).
  The runtime's floors are macOS 15+ / glibc >= 2.38 for mcp-standalone
  mode; the installer names them when its pre-install `redis-server
  --version` probe fails. Installer tests separately verify resolution
  from `$CARGO_HOME/share/exocortex/standalone`. macOS Intel ships client/backend
  binaries only because upstream has no self-contained x64 runtime (deviation
  19). No Rust toolchain or protoc required.
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
`testing`, `exocortex-ingest` + `tonic` (exocortex-cluster, for the
OC-PRD rolling-upgrade acceptance test that drives a real IngestServer;
both already workspace dependencies, no new external crate).

`exocortex-server` directly depends on `axum-server` with its Rustls feature.

D21 (adapter-contract PRD) additions, all already workspace pins: the
`exocortex-wire` manifest module serializes its JSON through `serde_json`
(published dep list grows by that one crate); the SDK's `testing` feature
optionally gains `serde_json` for the canned manifest; `exocortex-ops`
gains `prost-types` (wire batch timestamps on the `preflight_batch`
operation); `exocortex-server` gains `async-trait` (the registry preflight
handle); `exocortex-ingest` dev-depends on `exocortex-adapter-sdk` for the
manifest-parity golden table. No new external crate anywhere.
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
