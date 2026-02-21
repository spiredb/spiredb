//! Agent loop — drives the LLM <-> tool think-act cycle.

use std::io::{self, Write as IoWrite};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::llm::Llm;
use crate::llm::types::{ChatMessage, ChatResponse};
use crate::tool::ToolRegistry;

// ANSI escape codes
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

/// Callback invoked when a tool requires user confirmation.
///
/// Receives the tool name and arguments. Returns `true` to approve execution.
/// If no callback is set, tools requiring confirmation are auto-approved.
pub type ConfirmFn = Box<dyn Fn(&str, &serde_json::Value) -> bool + Send + Sync>;

/// Configuration for the agent loop.
pub struct AgentLoopConfig {
    /// Maximum number of LLM round-trips before stopping. Default: 20.
    pub max_turns: usize,
    /// Print tool calls and results as they happen. Default: true.
    pub verbose: bool,
    /// System prompt prepended to every conversation.
    pub system_prompt: String,
    /// Called when a tool needs user confirmation. If `None`, auto-approved.
    pub confirm: Option<ConfirmFn>,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_turns: 20,
            verbose: true,
            system_prompt: String::new(),
            confirm: None,
        }
    }
}

/// An agentic loop that drives multi-turn LLM <-> tool interactions.
pub struct AgentLoop {
    llm: Arc<dyn Llm>,
    tools: ToolRegistry,
    config: AgentLoopConfig,
    history: Vec<ChatMessage>,
}

impl AgentLoop {
    /// Create a new agent loop.
    pub fn new(llm: Arc<dyn Llm>, tools: ToolRegistry, config: AgentLoopConfig) -> Self {
        let mut history = Vec::new();
        if !config.system_prompt.is_empty() {
            history.push(ChatMessage::system(&config.system_prompt));
        }
        Self {
            llm,
            tools,
            config,
            history,
        }
    }

    /// Run the agent loop with user input. Returns the final assistant text.
    pub async fn run(&mut self, user_input: &str) -> Result<String> {
        self.history.push(ChatMessage::user(user_input));

        let tool_defs = self.tools.defs();
        let mut turns = 0;

        loop {
            if turns >= self.config.max_turns {
                return Err(Error::Tool(format!(
                    "agent loop exceeded max turns ({})",
                    self.config.max_turns
                )));
            }
            turns += 1;

            if self.config.verbose && turns > 1 {
                print!("{DIM}  thinking...{RESET}");
                io::stdout().flush().ok();
                // Clear the "thinking..." after we get a response (via \r)
            }

            let response = self.llm.chat(&self.history, &tool_defs).await?;

            if self.config.verbose && turns > 1 {
                print!("\r                \r"); // clear "thinking..."
                io::stdout().flush().ok();
            }

            match response {
                ChatResponse::Message(text) => {
                    self.history.push(ChatMessage::assistant(&text));
                    return Ok(text);
                }
                ChatResponse::ToolCalls { text, calls } => {
                    self.history.push(ChatMessage::assistant_tool_calls(
                        text.clone(),
                        calls.clone(),
                    ));

                    if self.config.verbose
                        && let Some(t) = &text
                    {
                        println!("{DIM}{t}{RESET}");
                    }

                    for call in &calls {
                        let tool = self.tools.find(&call.name);
                        let result = match tool {
                            Some(t) => {
                                // Show what we're about to do
                                if self.config.verbose {
                                    self.print_tool_call(&call.name, &call.arguments);
                                }

                                // Confirmation gate
                                if t.needs_confirmation() {
                                    if self.config.verbose {
                                        self.print_confirmation_preview(
                                            &call.name,
                                            &call.arguments,
                                        );
                                    }

                                    let approved = match &self.config.confirm {
                                        Some(confirm_fn) => confirm_fn(&call.name, &call.arguments),
                                        None => true, // auto-approve if no handler
                                    };

                                    if !approved {
                                        if self.config.verbose {
                                            println!("  {DIM}denied{RESET}");
                                        }
                                        let r = crate::tool::ToolResult::ok(
                                            "User denied this tool call.",
                                        );
                                        self.history
                                            .push(ChatMessage::tool_result(&call.id, &r.content));
                                        continue;
                                    }
                                }

                                match t.call(call.arguments.clone()).await {
                                    Ok(r) => r,
                                    Err(e) => crate::tool::ToolResult::error(e.to_string()),
                                }
                            }
                            None => crate::tool::ToolResult::error(format!(
                                "unknown tool: {}",
                                call.name
                            )),
                        };

                        if self.config.verbose {
                            self.print_tool_result(&call.name, &result.content);
                        }

                        self.history
                            .push(ChatMessage::tool_result(&call.id, &result.content));
                    }
                }
            }
        }
    }

    fn print_tool_call(&self, name: &str, args: &serde_json::Value) {
        // Compact representation depending on tool type
        match name {
            "read_file" => {
                let path = args["path"].as_str().unwrap_or("?");
                let range = match (args["offset"].as_u64(), args["limit"].as_u64()) {
                    (Some(o), Some(l)) => format!(" [{o}..{}]", o + l),
                    _ => String::new(),
                };
                println!("  {CYAN}{BOLD}read{RESET} {path}{DIM}{range}{RESET}");
            }
            "write_file" => {
                let path = args["path"].as_str().unwrap_or("?");
                let lines = args["content"]
                    .as_str()
                    .map(|c| c.lines().count())
                    .unwrap_or(0);
                println!("  {YELLOW}{BOLD}write{RESET} {path} {DIM}({lines} lines){RESET}");
            }
            "edit_file" => {
                let path = args["path"].as_str().unwrap_or("?");
                println!("  {YELLOW}{BOLD}edit{RESET} {path}");
            }
            "list_files" => {
                let path = args["path"].as_str().unwrap_or(".");
                println!("  {CYAN}{BOLD}ls{RESET} {path}");
            }
            "search_code" => {
                let query = args["query"].as_str().unwrap_or("?");
                println!("  {CYAN}{BOLD}search{RESET} {DIM}\"{query}\"{RESET}");
            }
            "find_symbol" => {
                let sym = args["name"].as_str().unwrap_or("?");
                println!("  {CYAN}{BOLD}symbol{RESET} {sym}");
            }
            "remember" => {
                let content = args["content"].as_str().unwrap_or("?");
                let short = if content.len() > 60 {
                    format!("{}...", &content[..60])
                } else {
                    content.to_string()
                };
                println!("  {GREEN}{BOLD}remember{RESET} {DIM}\"{short}\"{RESET}");
            }
            "recall" => {
                let query = args["query"].as_str().unwrap_or("?");
                println!("  {CYAN}{BOLD}recall{RESET} {DIM}\"{query}\"{RESET}");
            }
            "bash" => {
                let cmd = args["command"].as_str().unwrap_or("?");
                println!("  {YELLOW}{BOLD}bash{RESET} {DIM}$ {cmd}{RESET}");
            }
            "glob" => {
                let pattern = args["pattern"].as_str().unwrap_or("?");
                println!("  {CYAN}{BOLD}glob{RESET} {DIM}{pattern}{RESET}");
            }
            "grep" => {
                let pattern = args["pattern"].as_str().unwrap_or("?");
                println!("  {CYAN}{BOLD}grep{RESET} {DIM}/{pattern}/{RESET}");
            }
            _ => {
                let args_str = serde_json::to_string(args).unwrap_or_else(|_| args.to_string());
                let short = if args_str.len() > 80 {
                    format!("{}...", &args_str[..80])
                } else {
                    args_str
                };
                println!("  {CYAN}{BOLD}{name}{RESET} {DIM}{short}{RESET}");
            }
        }
    }

    fn print_confirmation_preview(&self, name: &str, args: &serde_json::Value) {
        match name {
            "edit_file" => {
                let old = args["old_string"].as_str().unwrap_or("");
                let new = args["new_string"].as_str().unwrap_or("");
                // Show a compact diff preview
                for line in old.lines().take(8) {
                    println!("    {RED}- {line}{RESET}");
                }
                if old.lines().count() > 8 {
                    println!(
                        "    {DIM}  ... ({} more lines){RESET}",
                        old.lines().count() - 8
                    );
                }
                for line in new.lines().take(8) {
                    println!("    {GREEN}+ {line}{RESET}");
                }
                if new.lines().count() > 8 {
                    println!(
                        "    {DIM}  ... ({} more lines){RESET}",
                        new.lines().count() - 8
                    );
                }
            }
            "write_file" => {
                let content = args["content"].as_str().unwrap_or("");
                let lines = content.lines().count();
                if lines <= 10 {
                    for line in content.lines() {
                        println!("    {GREEN}+ {line}{RESET}");
                    }
                } else {
                    for line in content.lines().take(5) {
                        println!("    {GREEN}+ {line}{RESET}");
                    }
                    println!("    {DIM}  ... ({} more lines){RESET}", lines - 5);
                }
            }
            "bash" => {
                let command = args["command"].as_str().unwrap_or("");
                println!("    {DIM}$ {command}{RESET}");
            }
            _ => {}
        }
    }

    fn print_tool_result(&self, name: &str, content: &str) {
        // Don't repeat the full content for large results — just a summary line
        if content.starts_with("Error:") {
            println!("    {RED}{content}{RESET}");
            return;
        }

        match name {
            "read_file" => {
                // Just show the header line (first line has path + stats)
                if let Some(first_line) = content.lines().next() {
                    let line_count = content.lines().count().saturating_sub(1);
                    println!("    {DIM}{first_line} — {line_count} lines shown{RESET}");
                }
            }
            "edit_file" | "write_file" => {
                // Show the full result (it's already compact)
                println!("    {GREEN}{content}{RESET}");
            }
            "search_code" | "find_symbol" => {
                let count = content.lines().filter(|l| l.starts_with('[')).count();
                if count > 0 {
                    println!("    {DIM}{count} results{RESET}");
                } else {
                    // Short enough to show
                    let short = if content.len() > 120 {
                        format!("{}...", &content[..120])
                    } else {
                        content.to_string()
                    };
                    println!("    {DIM}{short}{RESET}");
                }
            }
            "list_files" => {
                let count = content.lines().count();
                println!("    {DIM}{count} entries{RESET}");
            }
            "remember" => {
                println!("    {DIM}{content}{RESET}");
            }
            "recall" => {
                let count = content.lines().count();
                if count == 1 && content.contains("No relevant") {
                    println!("    {DIM}{content}{RESET}");
                } else {
                    println!("    {DIM}{count} memories{RESET}");
                }
            }
            "bash" => {
                // Show exit code + truncated output
                let first_line = content.lines().next().unwrap_or("");
                let output_lines = content.lines().count().saturating_sub(1);
                if output_lines > 0 {
                    println!("    {DIM}{first_line} ({output_lines} lines of output){RESET}");
                } else {
                    println!("    {DIM}{first_line}{RESET}");
                }
            }
            "glob" => {
                let count = content.lines().count();
                println!("    {DIM}{count} files{RESET}");
            }
            "grep" => {
                let count = content.lines().count();
                println!("    {DIM}{count} matches{RESET}");
            }
            _ => {
                let short = if content.len() > 200 {
                    format!("{}...", &content[..200])
                } else {
                    content.to_string()
                };
                println!("    {DIM}{short}{RESET}");
            }
        }
    }

    /// Get the full conversation history.
    pub fn history(&self) -> &[ChatMessage] {
        &self.history
    }

    /// Replace the conversation history.
    pub fn set_history(&mut self, history: Vec<ChatMessage>) {
        self.history = history;
    }
}
