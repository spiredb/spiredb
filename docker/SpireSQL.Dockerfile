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

# Copy proto files first (needed for build)
COPY spiredb/apps/spiredb_common/priv/proto /spiredb/proto

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
# Final minimal image
# ============================================================================
FROM scratch AS main

# OCI container labels
LABEL org.opencontainers.image.title="SpireSQL - Spire Compute Layer"
LABEL org.opencontainers.image.description="Distributed SQL query engine for SpireDB"
LABEL org.opencontainers.image.vendor="SpireDB"
LABEL org.opencontainers.image.source="https://github.com/spiredb/spiredb"
LABEL com.spiredb.arch="x86_64-v3"
LABEL com.spiredb.healthz="/healthz"

# Copy dynamic linker and libraries
COPY --from=build /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY --from=build /spiredb/lib /spiredb/lib

# Copy binary
COPY --from=build /spiredb/compute/target/release/spiresql /spiresql

# Copy default config (if exists)
COPY --from=build /spiredb/compute/spiresql/spiresql.toml /spiresql.toml

# Library path for dynamic linking
ENV LD_LIBRARY_PATH=/spiredb/lib

# Logging
ENV RUST_LOG=info
ENV SPIRE_LOG=info

# PostgreSQL wire protocol port
EXPOSE 5432

# Health check port  
EXPOSE 8080

ENTRYPOINT ["/spiresql", "-c", "/spiresql.toml"]
