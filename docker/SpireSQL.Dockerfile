# SpireSQL Dockerfile - Minimal distroless image

FROM rust:1.92-bullseye AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    cmake build-essential clang \
    protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /spiredb

# Proto files live in spiredb/apps/ — compute/spire_proto/proto is a symlink to there
COPY spiredb/apps/spiredb_common/priv/proto /spiredb/spiredb/apps/spiredb_common/priv/proto

# Copy Rust workspace
COPY compute /spiredb/compute

WORKDIR /spiredb/compute

ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      export RUSTFLAGS="-C target-cpu=neoverse-n1"; \
    else \
      export RUSTFLAGS="-C target-cpu=x86-64-v3"; \
    fi && \
    cargo build --bin spiresql --release

# Final minimal image
FROM gcr.io/distroless/cc-debian12:latest

LABEL org.opencontainers.image.title="SpireSQL - Spire Compute Layer"
LABEL org.opencontainers.image.description="Distributed SQL query engine for SpireDB"
LABEL org.opencontainers.image.vendor="SpireDB"
LABEL org.opencontainers.image.source="https://github.com/spiredb/spiredb"

COPY --from=build /spiredb/compute/target/release/spiresql /spiresql
COPY --from=build /spiredb/compute/spiresql/spiresql.toml /spiresql.toml

ENV RUST_LOG=info
ENV SPIRE_LOG=info

EXPOSE 5432

ENTRYPOINT ["/spiresql"]
