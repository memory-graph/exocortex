# Exocortex node image (§4.2: one artifact, two modes).
# Build:  docker build -t exocortex-node:local .
# The cluster compose harness (crates/exocortex-cluster/tests/
# docker-compose-cluster.yml) references this tag.
FROM rust:1.85-slim AS build
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
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /repo/target/release/exocortex-node /usr/local/bin/exocortex-node
# backend-node by default; mcp-standalone needs the redis-server binary
# plus the FalkorDB module on the image (deployment-specific).
ENTRYPOINT ["exocortex-node"]
CMD ["--mode", "backend-node", "--storage", "falkor://falkordb:6379", "--bind", "0.0.0.0:8080"]
