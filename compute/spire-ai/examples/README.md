# SpireAI Examples

Examples demonstrating the `spire-ai` SDK. Each example is a standalone binary with its own `Cargo.toml`.

## Prerequisites

All examples require a running **SpireDB** cluster and **Ollama** instance:

```bash
# Start SpireDB (default ports: 50051, 50052, 6379)
# See the main SpireDB README for setup instructions

# Start Ollama and pull the default models
ollama pull qwen3-embedding
ollama pull qwen3-coder:30b
```

## Examples

### coding-agent

Interactive coding assistant with sessions, persistent memory, code search, planning, and file caching.

```bash
cd coding-agent
cargo run -- --project /path/to/your/project
```

**Key flags:**
- `--project <dir>` — project directory (default: `.`)
- `--session <id>` — resume a previous session
- `--ollama-url`, `--embed-model`, `--llm-model` — Ollama configuration
- `--pd-addr`, `--data-addr` — SpireDB endpoints

**Interactive commands:** `/search`, `/plan`, `/memory`, `/sessions`, `/context`, `/cache-stats`, `/help`, `/quit`

### doc-qa

RAG pipeline for document Q&A. Ingest documents, chunk them, and answer questions with sourced responses.

```bash
cd doc-qa
cargo run -- ingest ./docs --chunker markdown
cargo run -- ask "How does authentication work?"
cargo run -- interactive
```

**Subcommands:** `ingest <dir>`, `ask <question>`, `interactive`
**Chunker strategies:** `markdown`, `sentence`, `fixed`

### code-search

Semantic code search using tree-sitter parsing. Index a codebase and search by meaning, symbol name, or build LLM context.

```bash
cd code-search
cargo run -- index /path/to/project
cargo run -- search "error handling"
cargo run -- symbol "Agent"
cargo run -- context "How does the router work?" --max-tokens 4000
cargo run -- interactive
```

**Subcommands:** `index <dir>`, `search <query>`, `symbol <name>`, `context <question>`, `interactive`

### knowledge-base

Typed collection CRUD with `#[derive(Doc)]`, semantic search, and real-time CDC change watching.

```bash
cd knowledge-base
cargo run -- seed
cargo run -- search "billing"
cargo run -- watch          # live CDC stream (requires RESP port 6379)
cargo run -- interactive
```

**Subcommands:** `add`, `search <query>`, `list`, `watch`, `delete <slug>`, `seed`, `interactive`

### offline-search

Index and search local documents entirely offline with Ollama. No cloud APIs needed.

```bash
cd offline-search
cargo run -- index ./my-docs --extensions "md,txt,rs,py"
cargo run -- search "database migration"
cargo run -- similar ./my-docs/README.md
cargo run -- interactive
```

**Subcommands:** `index <dir>`, `search <query>`, `similar <path>`, `interactive`

## Common Options

All examples share these connection flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--ollama-url` | `http://localhost:11434` | Ollama server URL |
| `--embed-model` | `qwen3-embedding` | Embedding model |
| `--pd-addr` | `http://127.0.0.1:50051` | SpireDB PD gRPC |
| `--data-addr` | `http://127.0.0.1:50052` | SpireDB DataAccess gRPC |

## Building

Each example is a standalone project. Build from its directory:

```bash
cd coding-agent
cargo build
```
