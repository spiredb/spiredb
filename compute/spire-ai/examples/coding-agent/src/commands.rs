use std::io::{self, Write as IoWrite};
use std::path::Path;

use chrono::Utc;

use spire_ai::agent::Importance;

use crate::agent::Agent;
use crate::display::*;
use crate::session::{create_session, ConversationTurn};

pub fn split_command(input: &str) -> (&str, &str) {
    match input.find(' ') {
        Some(pos) => (&input[..pos], input[pos + 1..].trim()),
        None => (input, ""),
    }
}

pub async fn handle_command(agent: &mut Agent, input: &str) -> spire_ai::Result<bool> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(true);
    }

    if input.starts_with('/') {
        let (cmd, arg) = split_command(input);
        match cmd {
            "/help" => cmd_help(),
            "/quit" | "/exit" => return Ok(false),
            "/index" => cmd_index(agent, arg).await?,
            "/session" => cmd_session(agent, arg).await?,
            "/sessions" => cmd_sessions(agent),
            "/cache-stats" => cmd_cache_stats(agent),
            _ => println!("Unknown command: {cmd}. Type /help for commands."),
        }
    } else {
        cmd_agent(agent, input).await?;
    }

    Ok(true)
}

fn cmd_help() {
    println!(
        "\n\
         {BOLD}Commands:{RESET}\n\
         \n\
         {CYAN}/index{RESET} {DIM}<dir>{RESET}       Index project for code search {DIM}(default: project root){RESET}\n\
         {CYAN}/session{RESET} {DIM}[id]{RESET}      Show or switch session\n\
         {CYAN}/sessions{RESET}          List sessions\n\
         {CYAN}/cache-stats{RESET}       File cache statistics\n\
         {CYAN}/help{RESET}              This help\n\
         {CYAN}/quit{RESET}              Exit\n\
         \n\
         {DIM}Everything else goes to the agent. It will read, search, edit, and\n\
         write files autonomously. Write operations require confirmation.{RESET}\n"
    );
}

async fn cmd_index(agent: &mut Agent, arg: &str) -> spire_ai::Result<()> {
    let dir = if arg.is_empty() {
        &agent.project_dir
    } else {
        arg
    };

    let path = Path::new(dir);
    if !path.exists() {
        println!("Directory not found: {dir}");
        return Ok(());
    }

    print!("{DIM}indexing {dir}...{RESET}");
    io::stdout().flush().ok();

    let result = agent.code_index.index_dir(dir).await?;
    agent.indexed = true;

    println!(
        "\r{GREEN}{BOLD}{}{RESET} files  {GREEN}{BOLD}{}{RESET} chunks  {GREEN}{BOLD}{}{RESET} symbols",
        result.files, result.chunks, result.symbols
    );

    if result.symbols == 0 && result.chunks > 0 {
        println!(
            "{DIM}  (0 symbols usually means no supported languages found;\n\
             chunks were created from line-based fallback){RESET}"
        );
    }

    if !result.errors.is_empty() {
        println!("{DIM}  {} warnings (first 3):{RESET}", result.errors.len());
        for err in result.errors.iter().take(3) {
            println!("{DIM}    {err}{RESET}");
        }
    }

    let _ = agent
        .memory
        .remember_with(
            &format!(
                "Indexed {dir}: {} files, {} chunks, {} symbols",
                result.files, result.chunks, result.symbols
            ),
            Importance::Normal,
        )
        .await;

    Ok(())
}

async fn cmd_agent(agent: &mut Agent, input: &str) -> spire_ai::Result<()> {
    // Auto-index once on first query
    if !agent.indexed {
        print!("{DIM}indexing project...{RESET}");
        io::stdout().flush().ok();

        match agent.code_index.index_dir(&agent.project_dir).await {
            Ok(result) => {
                agent.indexed = true;
                println!(
                    "\r{DIM}indexed: {} files, {} chunks, {} symbols{RESET}",
                    result.files, result.chunks, result.symbols
                );
                if result.symbols == 0 && result.chunks > 0 {
                    println!(
                        "{DIM}  (no tree-sitter symbols; code search uses line-based chunks){RESET}"
                    );
                }
            }
            Err(e) => {
                println!("\r{DIM}indexing skipped: {e}{RESET}");
                agent.indexed = true; // don't retry
            }
        }
    }

    println!(); // visual separator before tool calls

    match agent.agent_loop.run(input).await {
        Ok(response) => {
            println!("\n{response}\n");

            // Store conversation
            let now = Utc::now().to_rfc3339();
            agent.session.turn_count += 1;
            agent.session.last_active = now.clone();

            let user_turn = ConversationTurn {
                id: format!("{}-u-{}", agent.session.id, agent.session.turn_count),
                session_id: agent.session.id.clone(),
                role: "user".to_string(),
                content: input.to_string(),
                timestamp: now.clone(),
            };
            let assistant_turn = ConversationTurn {
                id: format!("{}-a-{}", agent.session.id, agent.session.turn_count),
                session_id: agent.session.id.clone(),
                role: "assistant".to_string(),
                content: response,
                timestamp: now,
            };

            let _ = agent.turns.insert(&user_turn).await;
            let _ = agent.turns.insert(&assistant_turn).await;
            let _ = agent.sessions.upsert(&agent.session).await;
        }
        Err(e) => {
            eprintln!("\x1b[31mAgent error: {e}\x1b[0m");
        }
    }

    Ok(())
}

async fn cmd_session(agent: &mut Agent, id: &str) -> spire_ai::Result<()> {
    if id.is_empty() {
        println!(
            "{BOLD}{}{RESET}  {DIM}{} turns, last active {}{RESET}",
            agent.session.id, agent.session.turn_count, agent.session.last_active
        );
        println!("{DIM}  project: {}{RESET}", agent.session.project_dir);
        return Ok(());
    }

    match agent.sessions.get(id).await? {
        Some(s) => {
            agent.session = s;
            println!("{DIM}switched to session {id}{RESET}");
        }
        None => {
            let s = create_session("coding-agent", &agent.project_dir, Some(id.to_string()));
            agent.sessions.upsert(&s).await?;
            agent.session = s;
            println!("{DIM}new session {id}{RESET}");
        }
    }

    Ok(())
}

fn cmd_cache_stats(agent: &Agent) {
    let stats = agent.file_cache.stats();
    println!(
        "{DIM}files tracked:{RESET} {BOLD}{}{RESET}  {DIM}tokens saved:{RESET} {BOLD}{}{RESET}",
        stats.files_tracked, stats.tokens_saved
    );
}

fn cmd_sessions(agent: &Agent) {
    println!(
        "  {BOLD}{}{RESET} {DIM}({} turns, last active {}){RESET} {GREEN}*{RESET}",
        agent.session.id, agent.session.turn_count, agent.session.last_active
    );
}
