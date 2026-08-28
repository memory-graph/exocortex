# Exocortex node image (§4.2: one artifact, two modes).
# Build:  docker build -t exocortex-node:local .
# The cluster compose harness (crates/exocortex-cluster/tests/
# docker-compose-cluster.yml) references this tag.
ARG TARGETARCH

# BuildKit verifies the selected upstream archive before it enters any image.
# Separate stages keep the URL and checksum literal for each supported platform.
FROM scratch AS protoc-amd64
ADD --checksum=sha256:0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip /protoc.zip

FROM scratch AS protoc-arm64
ADD --checksum=sha256:1de522032a8b194002fe35cab86d747848238b5e4de4f99648372079f5b46f9a https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-aarch_64.zip /protoc.zip

FROM protoc-${TARGETARCH} AS protoc-archive

# BuildKit verifies every file from the immutable upstream model revision.
FROM scratch AS bge-small-model
ADD --checksum=sha256:828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35 https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/ea104dacec62c0de699686887e3f920caeb4f3e3/onnx/model.onnx /model/onnx/model.onnx
ADD --checksum=sha256:d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66 https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/ea104dacec62c0de699686887e3f920caeb4f3e3/tokenizer.json /model/tokenizer.json
ADD --checksum=sha256:fa73f90bf92c8cace1fbcb709626306f2bdbc9ea3e5b5f94b440df9b6aa56350 https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/ea104dacec62c0de699686887e3f920caeb4f3e3/config.json /model/config.json
ADD --checksum=sha256:b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3 https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/ea104dacec62c0de699686887e3f920caeb4f3e3/special_tokens_map.json /model/special_tokens_map.json
ADD --checksum=sha256:9261e7d79b44c8195c1cada2b453e55b00aeb81e907a6664974b4d7776172ab3 https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/ea104dacec62c0de699686887e3f920caeb4f3e3/tokenizer_config.json /model/tokenizer_config.json

FROM busybox:1.36.1-musl@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213 AS protoc
COPY --from=protoc-archive /protoc.zip /tmp/protoc.zip
RUN mkdir -p /opt/protoc && unzip -q /tmp/protoc.zip -d /opt/protoc

FROM rust:1.85-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS build
COPY --from=protoc /opt/protoc/bin/protoc /usr/local/bin/protoc
COPY --from=protoc /opt/protoc/include /usr/local/include
RUN test "$(protoc --version)" = "libprotoc 28.3"
# ort-sys downloads the pinned ONNX Runtime archive through native TLS while
# building. Prove the digest-pinned builder carries that build-only toolchain.
RUN command -v pkg-config && pkg-config --exists openssl
WORKDIR /repo
COPY --from=bge-small-model /model /opt/exocortex/models/Xenova_bge-small-en-v1.5-ea104dacec62c0de699686887e3f920caeb4f3e3
COPY Cargo.toml Cargo.lock rust-toolchain.toml deny.toml ./
COPY .cargo .cargo
COPY xtask xtask
COPY proto proto
COPY crates crates
RUN cargo build --release -p exocortex-server --bin exocortex-node --features fastembed
RUN EXOCORTEX_BGE_SMALL_MODEL_DIR=/opt/exocortex/models/Xenova_bge-small-en-v1.5-ea104dacec62c0de699686887e3f920caeb4f3e3 /repo/target/release/exocortex-node --verify-embedder

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build --chown=65532:65532 /repo/target/release/exocortex-node /usr/local/bin/exocortex-node
COPY --from=build --chown=65532:65532 /opt/exocortex/models /opt/exocortex/models
# backend-node by default; the deployer must mount its TLS certificate and
# private key at the paths below, plus the required auth/cluster policy flags.
# mcp-standalone needs the redis-server binary plus the FalkorDB module on the
# image (deployment-specific).
USER 65532:65532
ENTRYPOINT ["exocortex-node"]
CMD ["--mode", "backend-node", "--storage", "falkor://falkordb:6379", "--allow-private-network-plaintext-data-plane", "--bind", "0.0.0.0:8080", "--tls-cert", "/run/secrets/exocortex/tls.crt", "--tls-key", "/run/secrets/exocortex/tls.key"]
