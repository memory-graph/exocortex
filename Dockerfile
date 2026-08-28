# Exocortex node image (§4.2: one artifact, two modes).
# Build:  docker build -t exocortex-node:local .
# The cluster compose harness (crates/exocortex-cluster/tests/
# docker-compose-cluster.yml) references this tag.
FROM rust:1.85-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /repo
COPY Cargo.toml Cargo.lock rust-toolchain.toml deny.toml ./
COPY .cargo .cargo
COPY xtask xtask
COPY proto proto
COPY crates crates
RUN cargo build --release -p exocortex-server --bin exocortex-node

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 exocortex \
    && useradd --system --uid 65532 --gid exocortex --no-create-home exocortex
COPY --from=build --chown=65532:65532 /repo/target/release/exocortex-node /usr/local/bin/exocortex-node
# backend-node by default; the deployer must mount its TLS certificate and
# private key at the paths below, plus the required auth/cluster policy flags.
# mcp-standalone needs the redis-server binary plus the FalkorDB module on the
# image (deployment-specific).
USER 65532:65532
ENTRYPOINT ["exocortex-node"]
CMD ["--mode", "backend-node", "--storage", "falkor://falkordb:6379", "--bind", "0.0.0.0:8080", "--tls-cert", "/run/secrets/exocortex/tls.crt", "--tls-key", "/run/secrets/exocortex/tls.key"]
