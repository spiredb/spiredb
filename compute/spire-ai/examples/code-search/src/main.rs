//! Semantic Code Search — Index and search a codebase using SpireAI.
//!
//! Supports tree-sitter parsing for Rust, Python, JavaScript, TypeScript, and Go.
//!
//! Usage:
//!   cargo run -- index ./my-project
//!   cargo run -- search "error handling in authentication"
//!   cargo run -- symbol "authenticate"
//!   cargo run -- context "How does the cache invalidation work?"

use std::io::{self, BufRead, Write as IoWrite};

use clap::{Parser, Subcommand};

use spire_ai::code::CodeIndex;
use spire_ai::prelude::*;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "code-search", about = "Semantic code search powered by SpireAI")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Ollama URL
    #[arg(long, default_value = "http://localhost:11434", global = true)]
    ollama_url: String,

    /// Ollama embedding model
    #[arg(long, default_value = "qwen3-embedding", global = true)]
    embed_model: String,

    /// SpireDB PD address
    #[arg(long, default_value = "http://127.0.0.1:50051", global = true)]
    pd_addr: String,

    /// SpireDB data address
    #[arg(long, default_value = "http://127.0.0.1:50052", global = true)]
    data_addr: String,

    /// Index name (for isolating different projects)
    #[arg(long, default_value = "default", global = true)]
    index: String,
}

#[derive(Subcommand)]
enum Command {
    /// Index a project directory
    Index {
        /// Project directory to index
        dir: String,
    },
    /// Semantic code search
    Search {
        /// Search query
        query: String,
        /// Maximum results
        #[arg(short = 'k', long, default_value_t = 10)]
        limit: usize,
    },
    /// Find a symbol by name
    Symbol {
        /// Symbol name to find
        name: String,
    },
    /// Build LLM context for a question
    Context {
        /// Question about the code
        question: String,
        /// Maximum tokens in context
        #[arg(long, default_value_t = 4000)]
        max_tokens: usize,
        /// Maximum chunks to include
        #[arg(long, default_value_t = 10)]
        max_chunks: usize,
    },
    /// Interactive search mode
    Interactive,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let spire = Spire::builder()
        .pd_addr(&cli.pd_addr)
        .data_addr(&cli.data_addr)
        .ollama(&cli.ollama_url, &cli.embed_model)
        .build()
        .await?;

    let code_index = spire.code(&cli.index);
    code_index.ensure().await?;

    match cli.command {
        Command::Index { dir } => cmd_index(&code_index, &dir).await?,
        Command::Search { query, limit } => cmd_search(&code_index, &query, limit).await?,
        Command::Symbol { name } => cmd_symbol(&code_index, &name).await?,
        Command::Context {
            question,
            max_tokens,
            max_chunks,
        } => cmd_context(&code_index, &question, max_tokens, max_chunks).await?,
        Command::Interactive => cmd_interactive(&code_index).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_index(code_index: &CodeIndex, dir: &str) -> spire_ai::Result<()> {
    println!("Indexing: {}", dir);
    let result = code_index.index_dir(dir).await?;

    println!("\nResults:");
    println!("  Files:   {}", result.files);
    println!("  Chunks:  {}", result.chunks);
    println!("  Symbols: {}", result.symbols);

    if !result.errors.is_empty() {
        println!("\nWarnings ({}):", result.errors.len());
        for (i, err) in result.errors.iter().take(10).enumerate() {
            println!("  {}. {}", i + 1, err);
        }
        if result.errors.len() > 10 {
            println!("  ... and {} more", result.errors.len() - 10);
        }
    }

    println!("\nIndex ready. Use 'search' or 'symbol' to query.");
    Ok(())
}

async fn cmd_search(code_index: &CodeIndex, query: &str, limit: usize) -> spire_ai::Result<()> {
    let hits = code_index.search(query).await?;

    if hits.is_empty() {
        println!("No results found for: {}", query);
        return Ok(());
    }

    let show = hits.len().min(limit);
    println!("Found {} results (showing {}):\n", hits.len(), show);

    for (i, hit) in hits.iter().take(limit).enumerate() {
        let kind = format!("{:?}", hit.chunk.kind);
        let name = hit.chunk.name.as_deref().unwrap_or("<block>");

        println!(
            "{}. {} ({}) — {}:{}-{}  [{:.3}]",
            i + 1,
            name,
            kind,
            hit.chunk.file,
            hit.chunk.start_line,
            hit.chunk.end_line,
            hit.score
        );

        // Show signature if available
        if let Some(sig) = &hit.chunk.signature {
            println!("   {}", sig);
        }

        // Show doc comment if available
        if let Some(docs) = &hit.chunk.docs {
            let doc_preview = if docs.len() > 100 {
                format!("{}...", &docs[..100])
            } else {
                docs.clone()
            };
            println!("   /// {}", doc_preview.replace('\n', "\n   /// "));
        }

        // Show first few lines of code
        let code_lines: Vec<&str> = hit.chunk.code.lines().take(5).collect();
        for line in &code_lines {
            println!("   {}", line);
        }
        if hit.chunk.code.lines().count() > 5 {
            println!("   ...");
        }
        println!();
    }

    Ok(())
}

async fn cmd_symbol(code_index: &CodeIndex, name: &str) -> spire_ai::Result<()> {
    let chunks = code_index.find_symbol(name).await?;

    if chunks.is_empty() {
        println!("No symbols found matching: {}", name);
        return Ok(());
    }

    println!("Found {} symbols matching '{}':\n", chunks.len(), name);

    for chunk in &chunks {
        let kind = format!("{:?}", chunk.kind);
        println!(
            "  {} ({}) — {}:{}-{}",
            chunk.name.as_deref().unwrap_or("?"),
            kind,
            chunk.file,
            chunk.start_line,
            chunk.end_line
        );

        if let Some(sig) = &chunk.signature {
            println!("    Signature: {}", sig);
        }

        if let Some(parent) = &chunk.parent {
            println!("    Parent: {}", parent);
        }

        if let Some(docs) = &chunk.docs {
            let doc_preview = if docs.len() > 120 {
                format!("{}...", &docs[..120])
            } else {
                docs.clone()
            };
            println!("    Docs: {}", doc_preview);
        }

        println!();
    }

    Ok(())
}

async fn cmd_context(
    code_index: &CodeIndex,
    question: &str,
    max_tokens: usize,
    max_chunks: usize,
) -> spire_ai::Result<()> {
    let ctx = code_index
        .context(question)
        .max_tokens(max_tokens)
        .max_chunks(max_chunks)
        .build()
        .await?;

    println!(
        "Context for: \"{}\"\n({} chunks, ~{} tokens)\n",
        question,
        ctx.chunks.len(),
        ctx.tokens
    );
    println!("{}", "=".repeat(60));
    println!("{}", ctx.text);
    println!("{}", "=".repeat(60));

    println!("\nAs system prompt:");
    println!("{}", "-".repeat(40));
    println!("{}", ctx.as_system_prompt());

    Ok(())
}

async fn cmd_interactive(code_index: &CodeIndex) -> spire_ai::Result<()> {
    println!("Interactive code search (commands: search, symbol, context, quit)\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("code> ");
        io::stdout().flush().ok();

        let mut input = String::new();
        if reader.read_line(&mut input)? == 0 {
            break;
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let (cmd, arg) = match input.find(' ') {
            Some(pos) => (&input[..pos], input[pos + 1..].trim()),
            None => (input, ""),
        };

        match cmd {
            "search" | "s" => {
                if arg.is_empty() {
                    println!("Usage: search <query>");
                } else {
                    cmd_search(code_index, arg, 10).await?;
                }
            }
            "symbol" | "sym" => {
                if arg.is_empty() {
                    println!("Usage: symbol <name>");
                } else {
                    cmd_symbol(code_index, arg).await?;
                }
            }
            "context" | "ctx" => {
                if arg.is_empty() {
                    println!("Usage: context <question>");
                } else {
                    cmd_context(code_index, arg, 4000, 10).await?;
                }
            }
            "quit" | "exit" | "q" => break,
            _ => {
                // Default: treat as search
                cmd_search(code_index, input, 10).await?;
            }
        }
    }

    println!("Goodbye.");
    Ok(())
}
