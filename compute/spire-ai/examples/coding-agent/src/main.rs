mod agent;
mod commands;
mod display;
mod prompt;
mod session;

use std::io::{self, Write as IoWrite};

use clap::Parser;

use display::*;

#[derive(Parser)]
#[command(
    name = "coding-agent",
    about = "Agentic coding assistant powered by SpireAI"
)]
pub(crate) struct Cli {
    /// Project directory to work with
    #[arg(short, long, default_value = ".")]
    pub project: String,

    /// Ollama URL
    #[arg(long, default_value = "http://localhost:11434")]
    pub ollama_url: String,

    /// Ollama embedding model
    #[arg(long, default_value = "qwen3-embedding")]
    pub embed_model: String,

    /// Ollama LLM model
    #[arg(long, default_value = "qwen3-coder:30b")]
    pub llm_model: String,

    /// Resume a previous session
    #[arg(short, long)]
    pub session: Option<String>,

    /// SpireDB PD address
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pub pd_addr: String,

    /// SpireDB data address
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    pub data_addr: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    println!(
        "\n{BOLD}SpireAI Coding Agent{RESET}\n\
         {DIM}project:{RESET} {}\n\
         {DIM}model:{RESET}   {}\n\
         {DIM}embed:{RESET}   {}\n",
        cli.project, cli.llm_model, cli.embed_model
    );

    let mut agent = agent::Agent::new(&cli).await?;

    println!("{DIM}Type /help for commands, or ask a question.{RESET}\n");

    loop {
        print!("{BOLD}>{RESET} ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break;
        }

        match commands::handle_command(&mut agent, &input).await {
            Ok(true) => continue,
            Ok(false) => {
                println!("{DIM}goodbye{RESET}");
                break;
            }
            Err(e) => {
                eprintln!("\x1b[31mError: {e}\x1b[0m");
            }
        }
    }

    Ok(())
}
