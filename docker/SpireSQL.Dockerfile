# SpireSQL Dockerfile - Minimal scratch-based image
# Builds the Rust compute layer with dynamic linking for x86-64-v3

FROM rust:1.92-bullseye AS build

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    cmake build-essential clang \
    protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /spiredb

# Copy proto files to match build.rs relative path:
# build.rs looks for ../../spiredb/apps/spiredb_common/priv/proto/ from compute/
COPY spiredb/apps/spiredb_common/priv/proto /spiredb/spiredb/apps/spiredb_common/priv/proto

# Copy Rust workspace
COPY compute /spiredb/compute

WORKDIR /spiredb/compute

# Target x86-64-v3 (AVX2, BMI2, etc.)
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"

# Build spiresql binary
RUN cargo build --bin spiresql --release

# Create lib directory and copy dynamic libraries
RUN mkdir -p /spiredb/lib && \
    cp -LR $(ldd ./target/release/spiresql | grep "=>" | awk '{print $3}' | grep -v "^$") /spiredb/lib/ 2>/dev/null || true

# ============================================================================
# Final minimal image with DNS support
# ============================================================================
FROM gcr.io/distroless/cc-debian12:latest AS main

# OCI container labels
LABEL org.opencontainers.image.title="SpireSQL - Spire Compute Layer"
LABEL org.opencontainers.image.description="Distributed SQL query engine for SpireDB"
LABEL org.opencontainers.image.vendor="SpireDB"
LABEL org.opencontainers.image.source="https://github.com/spiredb/spiredb"
LABEL com.spiredb.arch="x86_64-v3"

# Copy binary
COPY --from=build /spiredb/compute/target/release/spiresql /spiresql

# Copy default config (if exists)
COPY --from=build /spiredb/compute/spiresql/spiresql.toml /spiresql.toml

# Logging
ENV RUST_LOG=info
ENV SPIRE_LOG=info

# PostgreSQL wire protocol port (also used for health checks)
EXPOSE 5432

ENTRYPOINT ["/spiresql"]

