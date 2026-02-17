
<p align="center">
  <img src="art/spire-square.svg" alt="SpireDB Logo" width="200">
</p>

<h1 align="center">SpireDB</h1>
<p align="center">
  <b>Stream → Store → Scale.</b>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-AGPL_3.0-blue.svg" alt="License"></a>
  <a href="https://elixir-lang.org"><img src="https://img.shields.io/badge/Elixir-1.14+-4B275F.svg" alt="Elixir"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.75+-B7410E.svg" alt="Rust"></a>
  <a href="https://discord.gg/6GtdpFpU8F"><img src="https://img.shields.io/discord/1266804004327784489?label=Discord&logo=discord&logoColor=white&color=5865F2" alt="Discord"></a>
  <a href="https://ghcr.io/spiredb/spiredb"><img src="https://img.shields.io/badge/SpireDB-ghcr.io-2496ED.svg?logo=docker&logoColor=white" alt="SpireDB Docker"></a>
  <a href="https://ghcr.io/spiredb/spiresql"><img src="https://img.shields.io/badge/SpireSQL-ghcr.io-2496ED.svg?logo=docker&logoColor=white" alt="SpireSQL Docker"></a>
</p>

<br>

SpireDB is a unified data platform that seamlessly integrates high-performance distributed storage with intelligent compute capabilities.

## Components

| Component | Description | Status |
|-----------|-------------|--------|
| **SpireDB** | Distributed storage engine with Raft consensus | ✅ Available |
| **SpireSQL** | SQL compute layer (PostgreSQL wire protocol) | ✅ Available |
| **spire** | Command-line interface for cluster management | ✅ Available |
| **spire-ai** | AI-native SDK for SpireDB — RAG, code search, agents | ✅ Available |

## Features

### Storage (SpireDB)
- **Redis Compatible** — Use any Redis client
- **Distributed** — Automatic sharding and replication
- **Vector Search** — Built-in approximate nearest neighbor search
- **Streams** — Event streaming and processing
- **Transactions** — ACID transactions

### Compute (SpireSQL)
- **PostgreSQL Compatible** — Connect with `psql`, DBeaver, or any Postgres client
- **Distributed Queries** — Parallel execution across shards
- **Join Storage & Compute** — Query your data with SQL

### CLI (spire)
- **Cluster Management** — View cluster status and members
- **Schema Operations** — List tables and indexes
- **SQL Shell** — Interactive SQL console

### Plugins
- **Extensible** — Write custom plugins in Elixir
- **Hot Reload** — Deploy plugins without restart
- **Pre-built Plugins** — Rate limiting, caching, transformations

## Documentation

**[spire.zone/docs](https://spire.zone/docs/#/)**

## License

See [LICENSE](LICENSE) for details.
