//! Coding Agent — An interactive coding assistant powered by SpireAI.
//!
//! Features:
//! - Index a codebase with tree-sitter parsing
//! - Semantic code search and symbol lookup
//! - Implementation planning with step-by-step execution
//! - Persistent memory across sessions
//! - LLM-powered code Q&A
//!
//! Usage:
//!   cargo run -- --project ./my-project
//!   cargo run -- --project ./my-project --session abc123
//!   cargo run -- --ollama-url http://localhost:11434

use std::io::{self, BufRead, Write as IoWrite};
use std::path::Path;

use chrono::Utc;
use clap::Parser;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use spire_ai::agent::Importance;
use spire_ai::code::CodeIndex;
use spire_ai::prelude::*;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "coding-agent", about = "Interactive coding agent powered by SpireAI")]
struct Cli {
    /// Project directory to work with
    #[arg(short, long, default_value = ".")]
    project: String,

    /// Ollama URL
    #[arg(long, default_value = "http://localhost:11434")]
    ollama_url: String,

    /// Ollama embedding model
    #[arg(long, default_value = "nomic-embed-text")]
    embed_model: String,

    /// Ollama LLM model
    #[arg(long, default_value = "qwen3-coder:30b")]
    llm_model: String,

    /// Resume a previous session
    #[arg(short, long)]
    session: Option<String>,

    /// SpireDB PD address
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pd_addr: String,

    /// SpireDB data address
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    data_addr: String,
}

// ---------------------------------------------------------------------------
// Document types
// ---------------------------------------------------------------------------

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
struct Session {
    #[id]
    id: String,
    agent_id: String,
    #[embed]
    summary: String,
    project_dir: String,
    created_at: String,
    last_active: String,
    turn_count: u64,
}

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
struct ConversationTurn {
    #[id]
    id: String,
    session_id: String,
    role: String,
    #[embed]
    content: String,
    timestamp: String,
}

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
struct Plan {
    #[id]
    id: String,
    session_id: String,
    #[embed]
    task: String,
    steps: String, // JSON-serialized Vec<PlanStep>
    status: String,
    created_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PlanStep {
    description: String,
    file: String,
    action: String,
    status: String,
    detail: String,
}

// ---------------------------------------------------------------------------
// Agent state
// ---------------------------------------------------------------------------

struct Agent {
    spire: Spire,
    code_index: CodeIndex,
    memory: AgentMemory,
    sessions: Collection<Session>,
    turns: Collection<ConversationTurn>,
    plans: Collection<Plan>,
    session: Session,
    project_dir: String,
    file_cache: FileCache,
    last_plan_id: Option<String>,
}

impl Agent {
    async fn new(cli: &Cli) -> spire_ai::Result<Self> {
        // Connect to SpireDB
        let spire = Spire::builder()
            .pd_addr(&cli.pd_addr)
            .data_addr(&cli.data_addr)
            .ollama(&cli.ollama_url, &cli.embed_model)
            .ollama_llm(&cli.ollama_url, &cli.llm_model)
            .build()
            .await?;

        let agent_id = "coding-agent";

        // Derive a project key for per-project isolation
        let project_key = project_key(&cli.project);
        println!("Project key: {}", project_key);

        // Set up collections — namespaced per project
        let code_index = spire.code(&format!("{project_key}_code"));
        let memory = spire.memory(&format!("{project_key}_{agent_id}"));
        let sessions: Collection<Session> = spire.collection(&format!("{project_key}_sessions"));
        let turns: Collection<ConversationTurn> = spire.collection(&format!("{project_key}_turns"));
        let plans: Collection<Plan> = spire.collection(&format!("{project_key}_plans"));

        // Ensure backing storage exists
        code_index.ensure().await?;
        memory.ensure().await?;
        sessions.ensure().await?;
        turns.ensure().await?;
        plans.ensure().await?;

        // Create or resume session
        let session = match &cli.session {
            Some(id) => {
                // Try to find existing session
                match sessions.search(id).limit(1).first().await? {
                    Some(hit) if hit.doc.id == *id => {
                        println!("Resumed session: {}", id);
                        hit.doc
                    }
                    _ => {
                        println!("Session {} not found, creating new.", id);
                        create_session(agent_id, &cli.project, Some(id.clone()))
                    }
                }
            }
            None => {
                let s = create_session(agent_id, &cli.project, None);
                println!("New session: {}", s.id);
                s
            }
        };

        // Persist new session
        sessions.upsert(&session).await?;

        Ok(Agent {
            spire,
            code_index,
            memory,
            sessions,
            turns,
            plans,
            session,
            project_dir: cli.project.clone(),
            file_cache: FileCache::new(),
            last_plan_id: None,
        })
    }

    // -----------------------------------------------------------------------
    // Commands
    // -----------------------------------------------------------------------

    async fn handle_command(&mut self, input: &str) -> spire_ai::Result<bool> {
        let input = input.trim();
        if input.is_empty() {
            return Ok(true);
        }

        if input.starts_with('/') {
            let (cmd, arg) = split_command(input);
            match cmd {
                "/help" => self.cmd_help(),
                "/quit" | "/exit" => return Ok(false),
                "/index" => self.cmd_index(arg).await?,
                "/search" => self.cmd_search(arg).await?,
                "/symbol" => self.cmd_symbol(arg).await?,
                "/context" => self.cmd_context(arg).await?,
                "/plan" => self.cmd_plan(arg).await?,
                "/plans" => self.cmd_plans().await?,
                "/execute" => self.cmd_execute(arg).await?,
                "/ask" => self.cmd_ask(arg).await?,
                "/remember" => self.cmd_remember(arg).await?,
                "/recall" => self.cmd_recall(arg).await?,
                "/session" => self.cmd_session(arg).await?,
                "/sessions" => self.cmd_sessions().await?,
                "/cache-stats" => self.cmd_cache_stats(),
                _ => println!("Unknown command: {}. Type /help for available commands.", cmd),
            }
        } else {
            // Treat bare input as an /ask
            self.cmd_ask(input).await?;
        }

        Ok(true)
    }

    fn cmd_help(&self) {
        println!(
            r#"
Commands:
  /index <dir>        Index a project directory (default: current project)
  /search <query>     Semantic code search
  /symbol <name>      Find a symbol by name
  /context <question> Show assembled LLM context for a question

  /plan <task>        Create an implementation plan
  /plans              List plans for this session
  /execute [plan_id]  Execute the latest (or specified) plan

  /ask <question>     Ask a question about the code (or just type directly)
  /remember <note>    Store a note in persistent memory
  /recall <query>     Recall relevant memories

  /session [id]       Show or switch session
  /sessions           List all sessions
  /cache-stats        Show file cache statistics
  /help               Show this help
  /quit               Exit
"#
        );
    }

    async fn cmd_index(&mut self, arg: &str) -> spire_ai::Result<()> {
        let dir = if arg.is_empty() {
            &self.project_dir
        } else {
            arg
        };

        let path = Path::new(dir);
        if !path.exists() {
            println!("Directory not found: {}", dir);
            return Ok(());
        }

        println!("Indexing {}...", dir);
        let result = self.code_index.index_dir(dir).await?;
        println!(
            "Indexed {} files, {} chunks, {} symbols.",
            result.files, result.chunks, result.symbols
        );
        if !result.errors.is_empty() {
            println!("Warnings ({}):", result.errors.len());
            for (i, err) in result.errors.iter().take(5).enumerate() {
                println!("  {}. {}", i + 1, err);
            }
            if result.errors.len() > 5 {
                println!("  ... and {} more", result.errors.len() - 5);
            }
        }

        // Remember the indexing action
        self.memory
            .remember_with(
                &format!("Indexed project directory: {} ({} files, {} symbols)", dir, result.files, result.symbols),
                Importance::Normal,
            )
            .await?;

        Ok(())
    }

    async fn cmd_search(&self, query: &str) -> spire_ai::Result<()> {
        if query.is_empty() {
            println!("Usage: /search <query>");
            return Ok(());
        }

        let hits = self.code_index.search(query).await?;
        if hits.is_empty() {
            println!("No results found for: {}", query);
            return Ok(());
        }

        println!("Found {} results:\n", hits.len());
        for (i, hit) in hits.iter().enumerate() {
            let kind = format!("{:?}", hit.chunk.kind);
            let name = hit.chunk.name.as_deref().unwrap_or("<anonymous>");
            println!(
                "  {}. [{:.2}] {} ({}) — {}:{}–{}",
                i + 1,
                hit.score,
                name,
                kind,
                hit.chunk.file,
                hit.chunk.start_line,
                hit.chunk.end_line
            );
            // Show first 3 lines of code
            let preview: String = hit.chunk.code.lines().take(3).collect::<Vec<_>>().join("\n");
            println!("     {}", preview.replace('\n', "\n     "));
            println!();
        }

        Ok(())
    }

    async fn cmd_symbol(&self, name: &str) -> spire_ai::Result<()> {
        if name.is_empty() {
            println!("Usage: /symbol <name>");
            return Ok(());
        }

        let chunks = self.code_index.find_symbol(name).await?;
        if chunks.is_empty() {
            println!("No symbols found matching: {}", name);
            return Ok(());
        }

        println!("Found {} symbols:\n", chunks.len());
        for chunk in &chunks {
            let kind = format!("{:?}", chunk.kind);
            println!(
                "  {} ({}) — {}:{}–{}",
                chunk.name.as_deref().unwrap_or("?"),
                kind,
                chunk.file,
                chunk.start_line,
                chunk.end_line
            );
            if let Some(sig) = &chunk.signature {
                println!("    {}", sig);
            }
            println!();
        }

        Ok(())
    }

    async fn cmd_context(&self, question: &str) -> spire_ai::Result<()> {
        if question.is_empty() {
            println!("Usage: /context <question>");
            return Ok(());
        }

        let ctx = self
            .code_index
            .context(question)
            .max_tokens(4000)
            .max_chunks(10)
            .build()
            .await?;

        println!(
            "Context ({} chunks, ~{} tokens):\n",
            ctx.chunks.len(),
            ctx.tokens
        );
        println!("{}", ctx.text);

        Ok(())
    }

    async fn cmd_plan(&mut self, task: &str) -> spire_ai::Result<()> {
        if task.is_empty() {
            println!("Usage: /plan <task description>");
            return Ok(());
        }

        println!("Analyzing codebase for: {}...", task);

        // Step 1: Search for relevant code (graceful if index not built yet)
        let (hits, context_text) = match self.code_index.search(task).await {
            Ok(h) => {
                let ctx = self
                    .code_index
                    .context(task)
                    .max_tokens(6000)
                    .max_chunks(15)
                    .build()
                    .await
                    .map(|c| c.text)
                    .unwrap_or_default();
                (h, ctx)
            }
            Err(_) => {
                println!("(code index not built — run /index first for better plans)");
                (vec![], String::new())
            }
        };

        // Step 2: Recall relevant memories (graceful if collection not ready)
        let memories = self.memory.recall_limit(task, 5).await.unwrap_or_default();
        let memory_ctx = if memories.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = memories.iter().map(|m| format!("- {}", m.content)).collect();
            format!("\n\nRelevant memories:\n{}", items.join("\n"))
        };

        // Step 3: Detect project type
        let project_type = detect_project_type(&self.project_dir);

        // Step 4: Ask LLM to create plan
        let llm = self.spire.llm().ok_or(spire_ai::Error::NoLlm)?;

        let system = format!(
            "You are a senior software engineer creating an implementation plan.\n\
             This is a {project_type} project. All code you generate MUST use {project_type} conventions.\n\n\
             Analyze the code context and create a detailed step-by-step plan.\n\
             For each step, specify:\n\
             - description: what to do\n\
             - file: which file to modify (relative path, using correct extensions for {project_type})\n\
             - action: one of ReadFile, EditFile, CreateFile, DeleteLines, SearchCode\n\
             - detail: specific changes (old code -> new code, or what to add)\n\n\
             Return the plan as a JSON array of steps:\n\
             [{{\"description\": \"...\", \"file\": \"...\", \"action\": \"...\", \"detail\": \"...\"}}]\n\n\
             ONLY return the JSON array, no other text."
        );

        let user = format!(
            "Project type: {project_type}\nTask: {task}\n\nCode context:\n{}{memory_ctx}\n\n\
             Files found:\n{}\n\n\
             Create a step-by-step implementation plan (JSON array):",
            context_text,
            hits.iter()
                .map(|h| format!("  - {} ({}:{}-{})", h.chunk.file, h.chunk.name.as_deref().unwrap_or("?"), h.chunk.start_line, h.chunk.end_line))
                .collect::<Vec<_>>()
                .join("\n")
        );

        println!("Generating plan...");
        let response = llm.generate_with_system(&system, &user).await?;

        // Parse steps from LLM response
        let steps: Vec<PlanStep> = match serde_json::from_str(&response) {
            Ok(s) => s,
            Err(_) => {
                // Try to extract JSON from response
                if let Some(start) = response.find('[') {
                    if let Some(end) = response.rfind(']') {
                        serde_json::from_str(&response[start..=end]).unwrap_or_else(|_| {
                            vec![PlanStep {
                                description: task.to_string(),
                                file: String::new(),
                                action: "Manual".to_string(),
                                status: "pending".to_string(),
                                detail: response.clone(),
                            }]
                        })
                    } else {
                        vec![PlanStep {
                            description: task.to_string(),
                            file: String::new(),
                            action: "Manual".to_string(),
                            status: "pending".to_string(),
                            detail: response.clone(),
                        }]
                    }
                } else {
                    vec![PlanStep {
                        description: task.to_string(),
                        file: String::new(),
                        action: "Manual".to_string(),
                        status: "pending".to_string(),
                        detail: response.clone(),
                    }]
                }
            }
        };

        // Ensure all steps have pending status
        let steps: Vec<PlanStep> = steps
            .into_iter()
            .map(|mut s| {
                if s.status.is_empty() {
                    s.status = "pending".to_string();
                }
                s
            })
            .collect();

        let plan = Plan {
            id: Uuid::new_v4().to_string(),
            session_id: self.session.id.clone(),
            task: task.to_string(),
            steps: serde_json::to_string(&steps).unwrap_or_default(),
            status: "draft".to_string(),
            created_at: Utc::now().to_rfc3339(),
        };

        // Store plan
        self.plans.insert(&plan).await?;
        self.last_plan_id = Some(plan.id.clone());
        self.memory
            .remember(&format!("Created plan '{}' for task: {}", plan.id, task))
            .await?;

        // Render plan as TODO list
        print_plan_todo(&plan, &steps);
        println!("Plan saved. Use /execute to run it.");

        Ok(())
    }

    async fn cmd_plans(&self) -> spire_ai::Result<()> {
        // Search broadly and filter by session_id
        let results = match self.plans.search("plan").limit(50).run().await {
            Ok(r) => r,
            Err(_) => {
                println!("No plans for this session.");
                return Ok(());
            }
        };

        let session_plans: Vec<_> = results
            .into_iter()
            .filter(|hit| hit.doc.session_id == self.session.id)
            .collect();

        if session_plans.is_empty() {
            println!("No plans for this session.");
            return Ok(());
        }

        println!("Plans:\n");
        for hit in &session_plans {
            let steps: Vec<PlanStep> =
                serde_json::from_str(&hit.doc.steps).unwrap_or_default();
            let done = steps.iter().filter(|s| s.status == "done").count();
            let total = steps.len();
            let marker = if done == total && total > 0 { "done" } else { &hit.doc.status };
            println!(
                "  [{}] {} — {} ({}/{} steps done)",
                marker, &hit.doc.id[..8], hit.doc.task, done, total
            );
        }
        println!();

        Ok(())
    }

    async fn cmd_execute(&mut self, plan_id: &str) -> spire_ai::Result<()> {
        // Find the plan by ID (direct lookup) or use last plan
        let id = if plan_id.is_empty() {
            match &self.last_plan_id {
                Some(id) => id.clone(),
                None => {
                    println!("No recent plan. Use /plan <task> to create one, or /execute <plan_id>.");
                    return Ok(());
                }
            }
        } else {
            plan_id.to_string()
        };

        let plan = match self.plans.get(&id).await? {
            Some(p) => p,
            None => {
                println!("Plan not found: {}", id);
                return Ok(());
            }
        };

        let mut steps: Vec<PlanStep> =
            serde_json::from_str(&plan.steps).unwrap_or_default();

        if steps.is_empty() {
            println!("Plan has no steps.");
            return Ok(());
        }

        print_plan_todo(&plan, &steps);
        println!("Executing...\n");

        let stdin = io::stdin();
        let mut reader = stdin.lock();

        for (i, step) in steps.iter_mut().enumerate() {
            if step.status == "done" {
                println!("  Step {} (done): {}", i + 1, step.description);
                continue;
            }

            println!("  Step {}: [{}] {}", i + 1, step.action, step.description);
            if !step.file.is_empty() {
                println!("    File: {}", step.file);
            }

            match step.action.as_str() {
                "ReadFile" => {
                    let path = resolve_path(&self.project_dir, &step.file);
                    match self.file_cache.read_file(&path) {
                        Ok(ReadResult::Fresh { ref content, lines, tokens_estimated }) => {
                            println!("    Read {} lines (~{} tokens) from {}", lines, tokens_estimated, step.file);
                            for (j, line) in content.lines().take(10).enumerate() {
                                println!("    {:>4} | {}", j + 1, line);
                            }
                            if lines > 10 {
                                println!("    ... ({} more lines)", lines - 10);
                            }
                            step.status = "done".to_string();
                        }
                        Ok(ReadResult::Unchanged { lines, tokens_saved, .. }) => {
                            println!("    File unchanged ({} lines, saved ~{} tokens)", lines, tokens_saved);
                            step.status = "done".to_string();
                        }
                        Ok(ReadResult::Modified { ref diff, lines_changed, tokens_saved }) => {
                            println!("    File modified ({} lines changed, saved ~{} tokens):", lines_changed, tokens_saved);
                            for line in diff.lines().take(20) {
                                println!("    {}", line);
                            }
                            step.status = "done".to_string();
                        }
                        Err(e) => {
                            println!("    Error reading file: {}", e);
                            step.status = "failed".to_string();
                        }
                    }
                }
                "EditFile" => {
                    let path = resolve_path(&self.project_dir, &step.file);
                    println!("    Change: {}", step.detail);
                    print!("    Apply this change? [y/n]: ");
                    io::stdout().flush().ok();

                    let mut answer = String::new();
                    reader.read_line(&mut answer).ok();

                    if answer.trim().eq_ignore_ascii_case("y") {
                        match apply_edit(&path, &step.detail).await {
                            Ok(()) => {
                                println!("    Applied.");
                                step.status = "done".to_string();
                            }
                            Err(e) => {
                                println!("    Error: {}", e);
                                step.status = "failed".to_string();
                            }
                        }
                    } else {
                        println!("    Skipped.");
                        step.status = "skipped".to_string();
                    }
                }
                "CreateFile" => {
                    let path = resolve_path(&self.project_dir, &step.file);
                    println!("    Will create: {}", step.file);
                    print!("    Create this file? [y/n]: ");
                    io::stdout().flush().ok();

                    let mut answer = String::new();
                    reader.read_line(&mut answer).ok();

                    if answer.trim().eq_ignore_ascii_case("y") {
                        // Ensure parent directory exists
                        if let Some(parent) = Path::new(&path).parent() {
                            tokio::fs::create_dir_all(parent).await.ok();
                        }
                        match tokio::fs::write(&path, &step.detail).await {
                            Ok(()) => {
                                println!("    Created.");
                                step.status = "done".to_string();
                            }
                            Err(e) => {
                                println!("    Error: {}", e);
                                step.status = "failed".to_string();
                            }
                        }
                    } else {
                        println!("    Skipped.");
                        step.status = "skipped".to_string();
                    }
                }
                "SearchCode" => {
                    let hits = self.code_index.search(&step.detail).await?;
                    println!("    Found {} results:", hits.len());
                    for hit in hits.iter().take(5) {
                        println!(
                            "      - {}:{}-{} ({:?})",
                            hit.chunk.file, hit.chunk.start_line, hit.chunk.end_line, hit.chunk.kind
                        );
                    }
                    step.status = "done".to_string();
                }
                _ => {
                    println!("    Unknown action: {}. Skipping.", step.action);
                    step.status = "skipped".to_string();
                }
            }
            println!();
        }

        // Update plan
        let done = steps.iter().filter(|s| s.status == "done").count();
        let total = steps.len();
        let status = if done == total { "done" } else { "partial" };

        let updated_plan = Plan {
            steps: serde_json::to_string(&steps).unwrap_or_default(),
            status: status.to_string(),
            ..plan
        };
        self.plans.upsert(&updated_plan).await?;

        println!("Plan execution complete: {}/{} steps done.", done, total);

        self.memory
            .remember(&format!(
                "Executed plan '{}': {}/{} steps completed for task: {}",
                updated_plan.id, done, total, updated_plan.task
            ))
            .await?;

        Ok(())
    }

    async fn cmd_ask(&mut self, question: &str) -> spire_ai::Result<()> {
        if question.is_empty() {
            println!("Usage: /ask <question>");
            return Ok(());
        }

        // Build code context
        let context = self
            .code_index
            .context(question)
            .max_tokens(4000)
            .max_chunks(10)
            .build()
            .await?;

        // Recall relevant memories
        let memories = self.memory.recall_limit(question, 3).await?;
        let memory_section = if memories.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = memories.iter().map(|m| format!("- {}", m.content)).collect();
            format!("\n\nAgent memories:\n{}", items.join("\n"))
        };

        // Get recent conversation turns
        let recent = self
            .turns
            .search(&self.session.id)
            .limit(6)
            .run()
            .await
            .unwrap_or_default();

        let history = if recent.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = recent
                .iter()
                .map(|h| format!("{}: {}", h.doc.role, h.doc.content))
                .collect();
            format!("\n\nRecent conversation:\n{}", items.join("\n"))
        };

        // Ask LLM
        let llm = self.spire.llm().ok_or(spire_ai::Error::NoLlm)?;

        let system = format!(
            "You are an expert coding assistant working on a project. \
             Answer questions about the code clearly and concisely. \
             Reference specific files and line numbers when relevant.\n\n\
             {}\n{}{}",
            context.as_system_prompt(),
            memory_section,
            history
        );

        println!("Thinking...");
        let answer = llm.generate_with_system(&system, question).await?;
        println!("\n{}\n", answer);

        // Store conversation turns
        let now = Utc::now().to_rfc3339();
        self.session.turn_count += 1;
        self.session.last_active = now.clone();

        let user_turn = ConversationTurn {
            id: format!("{}-u-{}", self.session.id, self.session.turn_count),
            session_id: self.session.id.clone(),
            role: "user".to_string(),
            content: question.to_string(),
            timestamp: now.clone(),
        };
        let assistant_turn = ConversationTurn {
            id: format!("{}-a-{}", self.session.id, self.session.turn_count),
            session_id: self.session.id.clone(),
            role: "assistant".to_string(),
            content: answer.clone(),
            timestamp: now,
        };

        self.turns.insert(&user_turn).await?;
        self.turns.insert(&assistant_turn).await?;
        self.sessions.upsert(&self.session).await?;

        Ok(())
    }

    async fn cmd_remember(&self, note: &str) -> spire_ai::Result<()> {
        if note.is_empty() {
            println!("Usage: /remember <note>");
            return Ok(());
        }

        let id = self.memory.remember(note).await?;
        println!("Remembered ({}): {}", &id[..8.min(id.len())], note);
        Ok(())
    }

    async fn cmd_recall(&self, query: &str) -> spire_ai::Result<()> {
        if query.is_empty() {
            println!("Usage: /recall <query>");
            return Ok(());
        }

        let memories = self.memory.recall(query).await?;
        if memories.is_empty() {
            println!("No relevant memories found.");
            return Ok(());
        }

        println!("Recalled {} memories:\n", memories.len());
        for m in &memories {
            let importance = format!("{:?}", m.importance);
            println!("  [{}] {}", importance, m.content);
        }
        println!();

        Ok(())
    }

    async fn cmd_session(&mut self, id: &str) -> spire_ai::Result<()> {
        if id.is_empty() {
            println!("Current session: {}", self.session.id);
            println!("  Project: {}", self.session.project_dir);
            println!("  Created: {}", self.session.created_at);
            println!("  Last active: {}", self.session.last_active);
            println!("  Turns: {}", self.session.turn_count);
            return Ok(());
        }

        // Switch session
        let results = self.sessions.search(id).limit(1).first().await?;
        match results {
            Some(hit) if hit.doc.id == id => {
                self.session = hit.doc;
                println!("Switched to session: {}", id);
            }
            _ => {
                let s = create_session("coding-agent", &self.project_dir, Some(id.to_string()));
                self.sessions.upsert(&s).await?;
                self.session = s;
                println!("Created new session: {}", id);
            }
        }

        Ok(())
    }

    fn cmd_cache_stats(&self) {
        let stats = self.file_cache.stats();
        println!("File Cache Statistics:");
        println!("  Files tracked:  {}", stats.files_tracked);
        println!("  Tokens saved:   {}", stats.tokens_saved);
    }

    async fn cmd_sessions(&self) -> spire_ai::Result<()> {
        let results = self
            .sessions
            .search("coding-agent session")
            .limit(20)
            .run()
            .await?;

        if results.is_empty() {
            println!("No sessions found.");
            return Ok(());
        }

        println!("Sessions:\n");
        for hit in &results {
            let current = if hit.doc.id == self.session.id {
                " (current)"
            } else {
                ""
            };
            println!(
                "  {} — {} turns, last active: {}{}",
                hit.doc.id, hit.doc.turn_count, hit.doc.last_active, current
            );
        }
        println!();

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive a short stable key from the project directory path.
/// This ensures each project gets its own isolated vector databases.
fn project_key(project_dir: &str) -> String {
    use std::hash::{Hash, Hasher};
    let canonical = std::fs::canonicalize(project_dir)
        .unwrap_or_else(|_| Path::new(project_dir).to_path_buf());
    let mut hasher = std::hash::DefaultHasher::new();
    canonical.hash(&mut hasher);
    let hash = hasher.finish();
    // Use last component + short hash for readability
    let name = canonical
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "project".to_string());
    format!("{}_{:08x}", name, hash as u32)
}

fn create_session(agent_id: &str, project_dir: &str, id: Option<String>) -> Session {
    let now = Utc::now().to_rfc3339();
    Session {
        id: id.unwrap_or_else(|| Uuid::new_v4().to_string()[..8].to_string()),
        agent_id: agent_id.to_string(),
        summary: format!("Coding session on {}", project_dir),
        project_dir: project_dir.to_string(),
        created_at: now.clone(),
        last_active: now,
        turn_count: 0,
    }
}

fn split_command(input: &str) -> (&str, &str) {
    match input.find(' ') {
        Some(pos) => (&input[..pos], input[pos + 1..].trim()),
        None => (input, ""),
    }
}

fn detect_project_type(project_dir: &str) -> String {
    let dir = Path::new(project_dir);
    let checks: &[(&str, &str)] = &[
        ("Cargo.toml", "Rust"),
        ("package.json", "JavaScript/TypeScript"),
        ("tsconfig.json", "TypeScript"),
        ("go.mod", "Go"),
        ("pyproject.toml", "Python"),
        ("setup.py", "Python"),
        ("requirements.txt", "Python"),
        ("Gemfile", "Ruby"),
        ("pom.xml", "Java (Maven)"),
        ("build.gradle", "Java/Kotlin (Gradle)"),
        ("mix.exs", "Elixir"),
        ("CMakeLists.txt", "C/C++ (CMake)"),
        ("Makefile", "C/C++ (Make)"),
        ("composer.json", "PHP"),
        ("pubspec.yaml", "Dart/Flutter"),
        ("Package.swift", "Swift"),
        ("Dockerfile", "Docker"),
    ];
    let mut types = Vec::new();
    for (file, lang) in checks {
        if dir.join(file).exists() {
            types.push(*lang);
        }
    }
    if types.is_empty() {
        "unknown".to_string()
    } else {
        types.join(" + ")
    }
}

fn print_plan_todo(plan: &Plan, steps: &[PlanStep]) {
    println!();
    println!("Plan: {} ({})", &plan.id[..8], plan.status);
    println!("Task: {}", plan.task);
    println!("{}", "-".repeat(60));
    for (i, step) in steps.iter().enumerate() {
        let checkbox = if step.status == "done" { "[x]" } else { "[ ]" };
        let action = if step.action.is_empty() {
            String::new()
        } else {
            format!(" ({})", step.action)
        };
        println!("  {} {}. {}{}", checkbox, i + 1, step.description, action);
        if !step.file.is_empty() {
            println!("       -> {}", step.file);
        }
    }
    let done = steps.iter().filter(|s| s.status == "done").count();
    println!("{}", "-".repeat(60));
    println!("  Progress: {}/{} steps", done, steps.len());
    println!();
}

fn resolve_path(project_dir: &str, file: &str) -> String {
    let path = Path::new(file);
    if path.is_absolute() {
        file.to_string()
    } else {
        Path::new(project_dir)
            .join(file)
            .to_string_lossy()
            .to_string()
    }
}

async fn apply_edit(path: &str, detail: &str) -> std::result::Result<(), String> {
    // Simple edit: if detail contains ">>>" separator, treat as old >>> new
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("read: {}", e))?;

    if let Some(sep) = detail.find(">>>") {
        let old = detail[..sep].trim();
        let new = detail[sep + 3..].trim();
        if content.contains(old) {
            let updated = content.replacen(old, new, 1);
            tokio::fs::write(path, updated)
                .await
                .map_err(|e| format!("write: {}", e))?;
            return Ok(());
        } else {
            return Err(format!("Could not find text to replace in {}", path));
        }
    }

    // Fallback: append detail to end of file
    let updated = format!("{}\n{}", content, detail);
    tokio::fs::write(path, updated)
        .await
        .map_err(|e| format!("write: {}", e))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    println!("SpireAI Coding Agent");
    println!("====================");
    println!("Project: {}", cli.project);
    println!("Ollama:  {} (embed: {}, llm: {})", cli.ollama_url, cli.embed_model, cli.llm_model);
    println!();

    let mut agent = Agent::new(&cli).await?;

    println!("Type /help for commands, or just ask a question.\n");

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    loop {
        print!("agent> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if reader.read_line(&mut input)? == 0 {
            break; // EOF
        }

        match agent.handle_command(&input).await {
            Ok(true) => continue,
            Ok(false) => {
                println!("Goodbye.");
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    Ok(())
}
