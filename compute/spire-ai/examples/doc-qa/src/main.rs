//! Document Q&A — RAG pipeline over markdown and text documents.
//!
//! Ingest documents from a directory, chunk them, embed them, and
//! answer natural-language questions with sourced responses.
//!
//! Usage:
//!   cargo run -- ingest ./docs
//!   cargo run -- ask "How does authentication work?"
//!   cargo run -- interactive

use std::io::{self, BufRead, Write as IoWrite};

use clap::{Parser, Subcommand};

use spire_ai::prelude::*;
use spire_ai::rag::chunker::Chunk;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "doc-qa", about = "Document Q&A system with RAG pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Ollama URL
    #[arg(long, default_value = "http://localhost:11434", global = true)]
    ollama_url: String,

    /// Ollama embedding model
    #[arg(long, default_value = "qwen3-embedding", global = true)]
    embed_model: String,

    /// Ollama LLM model
    #[arg(long, default_value = "qwen3-coder:30b", global = true)]
    llm_model: String,

    /// SpireDB PD address
    #[arg(long, default_value = "http://127.0.0.1:50051", global = true)]
    pd_addr: String,

    /// SpireDB data address
    #[arg(long, default_value = "http://127.0.0.1:50052", global = true)]
    data_addr: String,

    /// Pipeline name (for isolating different document sets)
    #[arg(long, default_value = "docs", global = true)]
    pipeline: String,
}

#[derive(Subcommand)]
enum Command {
    /// Ingest documents from a directory
    Ingest {
        /// Directory containing documents to ingest
        dir: String,
        /// Chunking strategy: "markdown" (default), "sentence", or "fixed"
        #[arg(long, default_value = "markdown")]
        chunker: String,
    },
    /// Ask a single question
    Ask {
        /// The question to ask
        question: String,
    },
    /// Enter interactive Q&A mode
    Interactive,
    /// Show pipeline statistics
    Stats,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Connect to SpireDB with Ollama embeddings + LLM
    let spire = Spire::builder()
        .pd_addr(&cli.pd_addr)
        .data_addr(&cli.data_addr)
        .ollama(&cli.ollama_url, &cli.embed_model)
        .ollama_llm(&cli.ollama_url, &cli.llm_model)
        .build()
        .await?;

    match cli.command {
        Command::Ingest { dir, chunker } => {
            ingest(&spire, &cli.pipeline, &dir, &chunker).await?;
        }
        Command::Ask { question } => {
            ask(&spire, &cli.pipeline, &question).await?;
        }
        Command::Interactive => {
            interactive(&spire, &cli.pipeline).await?;
        }
        Command::Stats => {
            stats(&spire, &cli.pipeline).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn ingest(
    spire: &Spire,
    pipeline_name: &str,
    dir: &str,
    chunker_type: &str,
) -> spire_ai::Result<()> {
    println!("Building RAG pipeline ({} chunker)...", chunker_type);

    let mut builder = spire.rag(pipeline_name);
    builder = match chunker_type {
        "sentence" => builder.chunker_sentence(5),
        "fixed" => builder.chunker_fixed(500, 50),
        _ => builder.chunker_markdown(),
    };
    let pipeline = builder
        .retriever_vector(10)
        .reranker_none()
        .build();

    // Ensure backing storage exists
    let collection: Collection<Chunk> = spire.collection(pipeline_name);
    collection.ensure().await?;

    println!("Ingesting documents from: {}", dir);

    let result = pipeline.ingest_dir(dir).await?;
    println!(
        "Ingested: {} chunks from {}",
        result.chunks, result.source
    );

    // Also ingest individual files for nested directories
    let mut total = result.chunks;
    let mut walker = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = walker.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            let sub_result = pipeline
                .ingest_dir(&path.to_string_lossy())
                .await?;
            total += sub_result.chunks;
            if sub_result.chunks > 0 {
                println!(
                    "  + {} chunks from {}",
                    sub_result.chunks,
                    path.display()
                );
            }
        }
    }

    println!("\nTotal: {} chunks ingested.", total);
    Ok(())
}

async fn ask(spire: &Spire, pipeline_name: &str, question: &str) -> spire_ai::Result<()> {
    let pipeline = spire
        .rag(pipeline_name)
        .chunker_markdown()
        .retriever_vector(10)
        .reranker_none()
        .build();

    println!("Retrieving relevant context...");
    let answer = pipeline.query(question).await?;

    println!("\nAnswer:\n{}\n", answer.text);

    if !answer.sources.is_empty() {
        println!("Sources:");
        for (i, sc) in answer.sources.iter().enumerate() {
            let source_preview = if sc.chunk.text.len() > 80 {
                format!("{}...", &sc.chunk.text[..80])
            } else {
                sc.chunk.text.clone()
            };
            println!(
                "  {}. [{:.2}] {} — \"{}\"",
                i + 1,
                sc.score,
                sc.chunk.source,
                source_preview
            );
        }
    }

    Ok(())
}

async fn interactive(spire: &Spire, pipeline_name: &str) -> spire_ai::Result<()> {
    let pipeline = spire
        .rag(pipeline_name)
        .chunker_markdown()
        .retriever_vector(10)
        .reranker_none()
        .build();

    println!("Interactive Q&A mode (type 'quit' to exit)\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("question> ");
        io::stdout().flush().ok();

        let mut input = String::new();
        if reader.read_line(&mut input)? == 0 {
            break;
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }
        if input == "quit" || input == "exit" {
            break;
        }

        // Try full RAG query first, fall back to retrieve-only
        match pipeline.query(input).await {
            Ok(answer) => {
                println!("\n{}\n", answer.text);
                if !answer.sources.is_empty() {
                    println!("  Sources:");
                    for sc in answer.sources.iter().take(3) {
                        println!("    - {} [{:.2}]", sc.chunk.source, sc.score);
                    }
                    println!();
                }
            }
            Err(spire_ai::Error::NoLlm) => {
                // No LLM configured, just show retrieved chunks
                println!("(No LLM configured — showing retrieved chunks)\n");
                let chunks = pipeline.retrieve(input).await?;
                for (i, sc) in chunks.iter().enumerate() {
                    println!("  {}. [{:.2}] {}", i + 1, sc.score, sc.chunk.source);
                    let preview = if sc.chunk.text.len() > 200 {
                        format!("{}...", &sc.chunk.text[..200])
                    } else {
                        sc.chunk.text.clone()
                    };
                    println!("     {}\n", preview);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    println!("Goodbye.");
    Ok(())
}

async fn stats(spire: &Spire, pipeline_name: &str) -> spire_ai::Result<()> {
    let pipeline = spire
        .rag(pipeline_name)
        .chunker_markdown()
        .retriever_vector(10)
        .reranker_none()
        .build();

    let stats = pipeline.stats().await?;
    println!("Pipeline: {}", pipeline_name);
    println!("  Chunks: {}", stats.chunks);
    Ok(())
}
