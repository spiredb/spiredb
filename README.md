
<p align="center">
  <img src="art/spire-square.svg" alt="SpireDB Logo" width="200">
</p>

<h1 align="center">SpireDB</h1>
<p align="center">
  <b>Stream -> Store -> Scale.</b>
</p>

<p align="center">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
  <a href="https://elixir-lang.org"><img src="https://img.shields.io/badge/Elixir-1.14+-4B275F.svg" alt="Elixir"></a>
  <a href="https://redis.io/docs/reference/protocol-spec/"><img src="https://img.shields.io/badge/Protocol-RESP-DC382D.svg" alt="RESP"></a>
  <a href="https://discord.gg/6GtdpFpU8F"><img src="https://img.shields.io/discord/1266804004327784489?label=Discord&logo=discord&logoColor=white&color=5865F2" alt="Discord"></a>
  <a href="https://ghcr.io/spiredb/spiredb"><img src="https://img.shields.io/badge/Docker-ghcr.io-2496ED.svg?logo=docker&logoColor=white" alt="Docker"></a>
</p>

<br>

SpireDB is building a unified data platform that seamlessly integrates high-performance distributed storage with intelligent compute capabilities.

Currently shipping: **SpireDB** — The foundational storage engine.
- **Protocol**: RESP (Redis-compatible) & Internal gRPC High-Performance Tier.
- **Consistency**: Raft-based distributed consensus.
- **Performance**: Local-First Write architecture with Asynchronous Replication.
- **Backing**: RocksDB for durable, low-latency persistence.

*Coming Soon: **SpireSQL** — The scalable compute and query layer.*

---

## Quick Start

Get the storage engine running in seconds using our production-ready Docker image.

### Run with Docker

```bash
docker run -d \
  --name spiredb \
  -p 6379:6379 \
  -v spiredb_data:/var/lib/spiredb \
  ghcr.io/spiredb/spiredb:latest
```

### Connect

Use any Redis-compatible client to interact with the storage layer:

```bash
redis-cli -p 6379 SET greeting "Hello SpireDB"
# "OK"
redis-cli -p 6379 GET greeting
# "Hello SpireDB"
```

---

## Architecture

The SpireDB platform is designed as a layered distributed system:

```mermaid
graph TD
    Client[Clients] --> LB[Load Balancer]
    LB --> API["Compute Layer / SpireSQL (Coming Soon)"]
    LB --> KV["Storage Layer / SpireDB (Available)"]
    
    subgraph "Storage Cluster"
    KV --> Raft[Raft Consensus]
    Raft --> RocksDB[RocksDB Persistence]
    end
```

### Core Technologies
- **Elixir/OTP**: For massive concurrency and fault tolerance.
- **Ra**: Reliable implementation of the Raft consensus algorithm.
- **RocksDB**: Industry-standard embedded storage engine.
- **NIF Optimization**: Direct native access to storage for zero-latency operations.

---

## Configuration

Control the storage node via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SPIRE_RESP_PORT` | TCP port for client connections | `6379` |
| `SPIRE_ROCKSDB_PATH` | Persistent data location | `/var/lib/spiredb/data` |
| `SPIRE_RAFT_DATA_DIR` | Raft log location | `/var/lib/spiredb/raft` |
| `SPIRE_LOG_LEVEL` | Log verbosity | `info` |

## Development

To contribute to the SpireDB Core:

```bash
# Clone the repository
git clone https://github.com/spiredb/spiredb.git
cd spiredb

# Setup dependencies
make setup

# Run tests
make test

# Start local cluster
make run
```
