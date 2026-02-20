//! Knowledge Base — Customer support knowledge base with live CDC updates.
//!
//! Demonstrates typed collections, CRUD operations, semantic search,
//! and real-time change watching via CDC.
//!
//! Usage:
//!   cargo run -- add
//!   cargo run -- search "password reset"
//!   cargo run -- list
//!   cargo run -- watch
//!   cargo run -- delete my-article-slug

use std::io::{self, BufRead, Write as IoWrite};

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use spire_ai::prelude::*;

// ---------------------------------------------------------------------------
// Document type
// ---------------------------------------------------------------------------

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
struct Article {
    #[id]
    slug: String,
    #[embed]
    title: String,
    #[embed]
    body: String,
    category: String,
    author: String,
    published: String, // "true" / "false" — String for simplicity
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "knowledge-base", about = "Customer support knowledge base with live updates")]
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

    /// Stream (RESP) address for CDC
    #[arg(long, default_value = "127.0.0.1:6379", global = true)]
    stream_addr: String,
}

#[derive(Subcommand)]
enum Command {
    /// Add an article interactively
    Add,
    /// Search articles
    Search {
        /// Search query
        query: String,
        /// Maximum results
        #[arg(short = 'k', long, default_value_t = 5)]
        limit: usize,
    },
    /// List all articles (via search)
    List,
    /// Watch for live changes via CDC
    Watch,
    /// Delete an article by slug
    Delete {
        /// Article slug to delete
        slug: String,
    },
    /// Seed with sample articles
    Seed,
    /// Interactive mode
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
        .stream_addr(&cli.stream_addr)
        .ollama(&cli.ollama_url, &cli.embed_model)
        .build()
        .await?;

    let kb: Collection<Article> = spire.collection("knowledge_base");
    kb.ensure().await?;

    match cli.command {
        Command::Add => cmd_add(&kb).await?,
        Command::Search { query, limit } => cmd_search(&kb, &query, limit).await?,
        Command::List => cmd_list(&kb).await?,
        Command::Watch => cmd_watch(&kb).await?,
        Command::Delete { slug } => cmd_delete(&kb, &slug).await?,
        Command::Seed => cmd_seed(&kb).await?,
        Command::Interactive => cmd_interactive(&kb).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_add(kb: &Collection<Article>) -> spire_ai::Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();

    println!("Add a new article:\n");

    let slug = prompt(&mut reader, "Slug: ");
    let title = prompt(&mut reader, "Title: ");
    let category = prompt(&mut reader, "Category: ");
    let author = prompt(&mut reader, "Author: ");

    println!("Body (enter a blank line to finish):");
    let mut body = String::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok();
        if line.trim().is_empty() {
            break;
        }
        body.push_str(&line);
    }

    let article = Article {
        slug: slug.clone(),
        title,
        body: body.trim().to_string(),
        category,
        author,
        published: "true".to_string(),
    };

    kb.insert(&article).await?;
    println!("\nArticle '{}' added.", slug);

    Ok(())
}

async fn cmd_search(kb: &Collection<Article>, query: &str, limit: usize) -> spire_ai::Result<()> {
    let hits = kb.search(query).limit(limit).run().await?;

    if hits.is_empty() {
        println!("No articles found for: {}", query);
        return Ok(());
    }

    println!("Found {} articles:\n", hits.len());
    for (i, hit) in hits.iter().enumerate() {
        println!("  {}. [score: {:.3}] {}", i + 1, hit.score, hit.doc.title);
        println!("     Slug: {}", hit.doc.slug);
        println!("     Category: {} | Author: {}", hit.doc.category, hit.doc.author);
        let body_preview = if hit.doc.body.len() > 150 {
            format!("{}...", &hit.doc.body[..150])
        } else {
            hit.doc.body.clone()
        };
        println!("     {}\n", body_preview);
    }

    Ok(())
}

async fn cmd_list(kb: &Collection<Article>) -> spire_ai::Result<()> {
    let articles = kb.all().await?;

    if articles.is_empty() {
        println!("No articles in the knowledge base.");
        return Ok(());
    }

    println!("Articles ({}):\n", articles.len());
    for article in &articles {
        let status = if article.published == "true" {
            "published"
        } else {
            "draft"
        };
        println!(
            "  [{}] {} — {} ({})",
            article.slug, article.title, article.category, status
        );
    }
    println!();

    Ok(())
}

async fn cmd_watch(kb: &Collection<Article>) -> spire_ai::Result<()> {
    println!("Watching for changes (Ctrl+C to stop)...\n");

    let stream = kb.watch().await?;

    loop {
        match stream.next().await? {
            Some(change) => {
                let op = format!("{:?}", change.op);
                println!("[{}] Document: {}", op, change.id);

                if let Some(ref after) = change.after {
                    println!("  Title: {}", after.title);
                    println!("  Category: {}", after.category);
                }

                if let Some(ref before) = change.before {
                    println!("  (was: {})", before.title);
                }

                println!("  Timestamp: {}\n", change.timestamp);
            }
            None => {
                println!("Stream ended.");
                break;
            }
        }
    }

    Ok(())
}

async fn cmd_delete(kb: &Collection<Article>, slug: &str) -> spire_ai::Result<()> {
    match kb.delete(slug).await? {
        true => println!("Deleted article: {}", slug),
        false => println!("Article not found: {}", slug),
    }
    Ok(())
}

async fn cmd_seed(kb: &Collection<Article>) -> spire_ai::Result<()> {
    let articles = vec![
        Article {
            slug: "password-reset".to_string(),
            title: "How to Reset Your Password".to_string(),
            body: "To reset your password, click the 'Forgot Password' link on the login page. \
                   Enter your email address and we'll send you a reset link. The link expires in 24 hours. \
                   Make sure to check your spam folder if you don't see the email."
                .to_string(),
            category: "account".to_string(),
            author: "support-team".to_string(),
            published: "true".to_string(),
        },
        Article {
            slug: "billing-faq".to_string(),
            title: "Billing and Subscription FAQ".to_string(),
            body: "We offer monthly and annual billing plans. Annual plans receive a 20% discount. \
                   You can upgrade or downgrade your plan at any time from the Settings page. \
                   Refunds are processed within 5-7 business days."
                .to_string(),
            category: "billing".to_string(),
            author: "billing-team".to_string(),
            published: "true".to_string(),
        },
        Article {
            slug: "api-rate-limits".to_string(),
            title: "API Rate Limits and Quotas".to_string(),
            body: "Free tier: 100 requests per minute. Pro tier: 1000 requests per minute. \
                   Enterprise: custom limits. Rate limit headers are included in every response: \
                   X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset. \
                   If you exceed the limit, you'll receive a 429 Too Many Requests response."
                .to_string(),
            category: "api".to_string(),
            author: "engineering".to_string(),
            published: "true".to_string(),
        },
        Article {
            slug: "getting-started".to_string(),
            title: "Getting Started Guide".to_string(),
            body: "Welcome! To get started: 1) Create an account at our website. \
                   2) Generate an API key from the dashboard. 3) Install our SDK: \
                   `pip install our-sdk` for Python or `npm install our-sdk` for Node.js. \
                   4) Follow the quickstart tutorial in our docs."
                .to_string(),
            category: "onboarding".to_string(),
            author: "docs-team".to_string(),
            published: "true".to_string(),
        },
        Article {
            slug: "data-export".to_string(),
            title: "How to Export Your Data".to_string(),
            body: "You can export all your data at any time from Settings > Data > Export. \
                   We support CSV, JSON, and Parquet formats. Large exports are processed \
                   asynchronously and you'll receive an email with a download link when ready. \
                   Exports include all your projects, tasks, and associated metadata."
                .to_string(),
            category: "data".to_string(),
            author: "engineering".to_string(),
            published: "true".to_string(),
        },
    ];

    kb.insert_many(&articles).await?;
    println!("Seeded {} articles.", articles.len());

    Ok(())
}

async fn cmd_interactive(kb: &Collection<Article>) -> spire_ai::Result<()> {
    println!("Knowledge Base interactive mode");
    println!("Commands: search <query>, add, list, delete <slug>, seed, quit\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("kb> ");
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
                    cmd_search(kb, arg, 5).await?;
                }
            }
            "add" => cmd_add(kb).await?,
            "list" | "ls" => cmd_list(kb).await?,
            "delete" | "rm" => {
                if arg.is_empty() {
                    println!("Usage: delete <slug>");
                } else {
                    cmd_delete(kb, arg).await?;
                }
            }
            "seed" => cmd_seed(kb).await?,
            "quit" | "exit" | "q" => break,
            _ => {
                // Default: search
                cmd_search(kb, input, 5).await?;
            }
        }
    }

    println!("Goodbye.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn prompt(reader: &mut io::StdinLock, message: &str) -> String {
    print!("{}", message);
    io::stdout().flush().ok();
    let mut input = String::new();
    reader.read_line(&mut input).ok();
    input.trim().to_string()
}
