//! Offline Semantic Search — Index local documents and search with Ollama.
//!
//! Works entirely offline with a local Ollama instance. No cloud APIs needed.
//! Great for privacy-sensitive document search.
//!
//! Usage:
//!   cargo run -- index ./my-docs
//!   cargo run -- search "quarterly revenue summary"
//!   cargo run -- interactive

use std::io::{self, BufRead, Write as IoWrite};

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use spire_ai::prelude::*;

// ---------------------------------------------------------------------------
// Document type
// ---------------------------------------------------------------------------

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
struct LocalDoc {
    #[id]
    path: String,
    #[embed]
    content: String,
    filename: String,
    size_bytes: u64,
    modified: String,
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "offline-search", about = "Offline semantic search over local documents")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Ollama URL (local only — no cloud APIs)
    #[arg(long, default_value = "http://localhost:11434", global = true)]
    ollama_url: String,

    /// Ollama embedding model
    #[arg(long, default_value = "nomic-embed-text", global = true)]
    embed_model: String,

    /// SpireDB PD address
    #[arg(long, default_value = "http://127.0.0.1:50051", global = true)]
    pd_addr: String,

    /// SpireDB data address
    #[arg(long, default_value = "http://127.0.0.1:50052", global = true)]
    data_addr: String,

    /// Collection name
    #[arg(long, default_value = "local_docs", global = true)]
    collection: String,
}

#[derive(Subcommand)]
enum Command {
    /// Index documents from a directory
    Index {
        /// Directory to scan
        dir: String,
        /// File extensions to include (comma-separated, e.g. "txt,md,rs")
        #[arg(long, default_value = "txt,md,rst,org,adoc,rs,py,js,ts,go,java,c,cpp,h,toml,yaml,yml,json")]
        extensions: String,
        /// Maximum file size in KB (skip larger files)
        #[arg(long, default_value_t = 512)]
        max_size_kb: u64,
    },
    /// Search documents
    Search {
        /// Search query
        query: String,
        /// Maximum results
        #[arg(short = 'k', long, default_value_t = 5)]
        limit: usize,
    },
    /// Find documents similar to a given one
    Similar {
        /// Path of document to find similar ones for
        path: String,
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

    println!("Offline Semantic Search (Ollama at {})", cli.ollama_url);
    println!();

    let spire = Spire::builder()
        .pd_addr(&cli.pd_addr)
        .data_addr(&cli.data_addr)
        .ollama(&cli.ollama_url, &cli.embed_model)
        .build()
        .await?;

    let docs: Collection<LocalDoc> = spire.collection(&cli.collection);
    docs.ensure().await?;

    match cli.command {
        Command::Index {
            dir,
            extensions,
            max_size_kb,
        } => cmd_index(&docs, &dir, &extensions, max_size_kb).await?,
        Command::Search { query, limit } => cmd_search(&docs, &query, limit).await?,
        Command::Similar { path } => cmd_similar(&spire, &docs, &path).await?,
        Command::Interactive => cmd_interactive(&docs).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_index(
    docs: &Collection<LocalDoc>,
    dir: &str,
    extensions: &str,
    max_size_kb: u64,
) -> spire_ai::Result<()> {
    let allowed_exts: Vec<&str> = extensions.split(',').map(|s| s.trim()).collect();
    let max_size = max_size_kb * 1024;

    println!("Scanning: {}", dir);
    println!("Extensions: {:?}", allowed_exts);
    println!("Max file size: {} KB\n", max_size_kb);

    let mut documents = Vec::new();
    let mut skipped = 0u64;

    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();

        // Skip directories and hidden files
        if !path.is_file() {
            continue;
        }
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }

        // Check extension
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        if !allowed_exts.contains(&ext) {
            continue;
        }

        // Check size
        let metadata = match path.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if metadata.len() > max_size {
            skipped += 1;
            continue;
        }

        // Read content
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        if content.trim().is_empty() {
            continue;
        }

        let modified: DateTime<Utc> = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
            .into();

        let doc = LocalDoc {
            path: path.to_string_lossy().to_string(),
            content,
            filename: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            size_bytes: metadata.len(),
            modified: modified.to_rfc3339(),
        };

        documents.push(doc);
    }

    if documents.is_empty() {
        println!("No documents found.");
        return Ok(());
    }

    println!("Found {} documents ({} skipped)", documents.len(), skipped);
    println!("Embedding and indexing...\n");

    // Insert in batches
    let batch_size = 10;
    let total = documents.len();
    for (i, batch) in documents.chunks(batch_size).enumerate() {
        docs.insert_many(batch).await?;
        let done = ((i + 1) * batch_size).min(total);
        println!("  Indexed {}/{} documents", done, total);
    }

    println!("\nDone. {} documents indexed.", total);
    Ok(())
}

async fn cmd_search(
    docs: &Collection<LocalDoc>,
    query: &str,
    limit: usize,
) -> spire_ai::Result<()> {
    let hits = docs.search(query).limit(limit).run().await?;

    if hits.is_empty() {
        println!("No results found for: {}", query);
        return Ok(());
    }

    println!("Results for \"{}\":\n", query);
    for (i, hit) in hits.iter().enumerate() {
        println!(
            "{}. [{:.3}] {}",
            i + 1,
            hit.score,
            hit.doc.path
        );
        println!(
            "   {} — {} bytes, modified {}",
            hit.doc.filename, hit.doc.size_bytes, hit.doc.modified
        );

        // Show content preview
        let preview = if hit.doc.content.len() > 200 {
            format!("{}...", &hit.doc.content[..200])
        } else {
            hit.doc.content.clone()
        };
        println!("   {}\n", preview.replace('\n', "\n   "));
    }

    Ok(())
}

async fn cmd_similar(
    spire: &Spire,
    docs: &Collection<LocalDoc>,
    path: &str,
) -> spire_ai::Result<()> {
    // Read the target file
    let content = tokio::fs::read_to_string(path).await.map_err(|e| {
        spire_ai::Error::Other(format!("Cannot read {}: {}", path, e))
    })?;

    // Embed it
    let embedding = spire.embedder().embed(&content).await?;

    // Search with the embedding
    let hits = docs.similar_vec(&embedding).limit(5).run().await?;

    if hits.is_empty() {
        println!("No similar documents found.");
        return Ok(());
    }

    println!("Documents similar to {}:\n", path);
    for (i, hit) in hits.iter().enumerate() {
        // Skip the same file
        if hit.doc.path == path {
            continue;
        }
        println!(
            "{}. [{:.3}] {}",
            i + 1,
            hit.score,
            hit.doc.path
        );
        println!("   {} bytes\n", hit.doc.size_bytes);
    }

    Ok(())
}

async fn cmd_interactive(docs: &Collection<LocalDoc>) -> spire_ai::Result<()> {
    println!("Interactive search (type 'quit' to exit)\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("search> ");
        io::stdout().flush().ok();

        let mut input = String::new();
        if reader.read_line(&mut input)? == 0 {
            break;
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }
        if input == "quit" || input == "exit" || input == "q" {
            break;
        }

        cmd_search(docs, input, 5).await?;
    }

    println!("Goodbye.");
    Ok(())
}
