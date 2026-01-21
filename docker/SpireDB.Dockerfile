# SpireDB Dockerfile
# Stage 1: Build dependencies and compile
FROM hexpm/elixir:1.18.3-erlang-27.3.3-debian-bookworm-20250407-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install hex and rebar
RUN mix local.hex --force && mix local.rebar --force

# Install rebar3 explicitly
RUN wget https://s3.amazonaws.com/rebar3/rebar3 && \
    chmod +x rebar3 && \
    mv rebar3 /usr/local/bin/

# Set build environment
ENV MIX_ENV=prod

# Copy mix files first for dependency caching
COPY mix.exs mix.lock ./
COPY apps/spiredb_common/mix.exs apps/spiredb_common/
COPY apps/spiredb_pd/mix.exs apps/spiredb_pd/
COPY apps/spiredb_store/mix.exs apps/spiredb_store/

# Fetch dependencies
RUN mix deps.get --only prod

# Copy config
COPY config config

# Initialize git to avoid hook failures
RUN git init && \
    git config user.email "build@spire.zone" && \
    git config user.name "SpireDB Build"

# Compile dependencies (cached layer)
ENV REBAR_BARE_COMPILER_OUTPUT_DIR=/app/_build/prod/lib
ENV REBAR_SKIP_HOOKS=1

RUN mix deps.compile --skip-umbrella-children || \
    (cd deps/rocksdb && REBAR_SKIP_HOOKS=1 rebar3 compile && cd ../..) && \
    mix deps.compile

# Copy application source
COPY apps apps

# Compile application
RUN mix compile

# Build release (clean first to avoid stale artifacts)
RUN mix release spiredb --overwrite

# Stage 2: Runtime image
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libncurses6 \
    libssl3 \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    locales \
    ca-certificates \
    procps \
    sysstat \
    htop \
    strace \
    lsof \
    tcpdump \
    curl \
    iproute2 \
    dnsutils \
    linux-perf \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Create non-root user
RUN useradd --create-home --shell /bin/bash spiredb

WORKDIR /app

# Copy release from builder
COPY --from=builder --chown=spiredb:spiredb /app/_build/prod/rel/spiredb ./

# Create data directories
RUN mkdir -p /var/lib/spiredb/data /var/lib/spiredb/raft /var/lib/spiredb/pd \
    && chown -R spiredb:spiredb /var/lib/spiredb

USER spiredb

# Default environment
ENV SPIRE_RESP_PORT=6379
ENV SPIRE_ROCKSDB_PATH=/var/lib/spiredb/data
ENV SPIRE_RAFT_DATA_DIR=/var/lib/spiredb/raft
ENV SPIRE_RA_DATA_DIR=/var/lib/spiredb/raft
ENV SPIRE_LOG_LEVEL=info
ENV RELEASE_DISTRIBUTION=name
ENV RELEASE_NODE=spiredb@127.0.0.1
# Cluster authentication cookie - MUST be same across all nodes
# Override in production via Kubernetes secret or docker-compose
ENV RELEASE_COOKIE=spiredb_cluster_cookie

EXPOSE 6379 50051 50052

VOLUME ["/var/lib/spiredb"]

ENTRYPOINT ["/app/bin/spiredb"]
CMD ["start"]