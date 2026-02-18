use std::io::{self, Write as IoWrite};
use std::path::Path;
use std::sync::Arc;

use spire_ai::agent::{AgentLoop, AgentLoopConfig};
use spire_ai::code::CodeIndex;
use spire_ai::prelude::*;
use spire_ai::tool::builtin::{
    BashTool, EditFileTool, FindSymbolTool, GlobTool, GrepTool, ListFilesTool, ReadFileTool,
    RecallTool, RememberTool, SearchCodeTool, WriteFileTool,
};

use crate::display::*;
use crate::prompt::{build_system_prompt, detect_project_type};
use crate::session::{create_session, project_key, ConversationTurn, Session};
use crate::Cli;

#[allow(dead_code)]
pub(crate) struct Agent {
    pub(crate) spire: Spire,
    pub(crate) code_index: CodeIndex,
    pub(crate) memory: AgentMemory,
    pub(crate) sessions: Collection<Session>,
    pub(crate) turns: Collection<ConversationTurn>,
    pub(crate) session: Session,
    pub(crate) project_dir: String,
    pub(crate) file_cache: Arc<FileCache>,
    pub(crate) indexed: bool,
    pub(crate) agent_loop: AgentLoop,
}

impl Agent {
    pub async fn new(cli: &Cli) -> spire_ai::Result<Self> {
        let spire = Spire::builder()
            .pd_addr(&cli.pd_addr)
            .data_addr(&cli.data_addr)
            .ollama(&cli.ollama_url, &cli.embed_model)
            .ollama_llm(&cli.ollama_url, &cli.llm_model)
            .build()
            .await?;

        let agent_id = "coding-agent";
        let project_key = project_key(&cli.project);

        let code_index = spire.code(&format!("{project_key}_code"));
        let memory = spire.memory(&format!("{project_key}_{agent_id}"));
        let sessions: Collection<Session> =
            spire.collection(&format!("{project_key}_sessions"));
        let turns: Collection<ConversationTurn> =
            spire.collection(&format!("{project_key}_turns"));

        code_index.ensure().await?;
        memory.ensure().await?;
        sessions.ensure().await?;
        turns.ensure().await?;

        let session = match &cli.session {
            Some(id) => match sessions.get(id).await? {
                Some(s) => {
                    println!("{DIM}resumed session {id}{RESET}");
                    s
                }
                None => {
                    let s = create_session(agent_id, &cli.project, Some(id.clone()));
                    println!("{DIM}new session {}{RESET}", s.id);
                    s
                }
            },
            None => {
                let s = create_session(agent_id, &cli.project, None);
                println!("{DIM}session {}{RESET}", s.id);
                s
            }
        };

        sessions.upsert(&session).await?;

        // Build tool registry
        let file_cache = Arc::new(FileCache::new());
        let project_dir = std::fs::canonicalize(&cli.project)
            .unwrap_or_else(|_| Path::new(&cli.project).to_path_buf())
            .to_string_lossy()
            .to_string();

        let mut tools = ToolRegistry::new();
        tools.register(Box::new(ReadFileTool::new(file_cache.clone(), &project_dir)));
        tools.register(Box::new(EditFileTool::new(file_cache.clone(), &project_dir)));
        tools.register(Box::new(WriteFileTool::new(file_cache.clone(), &project_dir)));
        tools.register(Box::new(ListFilesTool::new(&project_dir)));
        tools.register(Box::new(BashTool::new(&project_dir)));
        tools.register(Box::new(GlobTool::new(&project_dir)));
        tools.register(Box::new(GrepTool::new(&project_dir)));
        tools.register(Box::new(SearchCodeTool::new(code_index.clone())));
        tools.register(Box::new(FindSymbolTool::new(code_index.clone())));
        tools.register(Box::new(RememberTool::new(
            spire.memory(&format!("{project_key}_{agent_id}")),
        )));
        tools.register(Box::new(RecallTool::new(
            spire.memory(&format!("{project_key}_{agent_id}")),
        )));

        let project_type = detect_project_type(&project_dir);
        let llm = spire.llm_arc().ok_or(spire_ai::Error::NoLlm)?;

        let config = AgentLoopConfig {
            max_turns: 30,
            verbose: true,
            system_prompt: build_system_prompt(&project_type, &project_dir),
            confirm: Some(Box::new(|_name, _args| {
                // Drain any buffered stdin (e.g. from multi-line paste) before prompting.
                drain_stdin();

                print!("  \x1b[33mAllow?\x1b[0m \x1b[2m[y/n]\x1b[0m ");
                io::stdout().flush().ok();

                let mut answer = String::new();
                io::stdin().read_line(&mut answer).ok();
                answer.trim().eq_ignore_ascii_case("y")
            })),
        };

        let agent_loop = AgentLoop::new(llm, tools, config);

        Ok(Agent {
            spire,
            code_index,
            memory,
            sessions,
            turns,
            session,
            project_dir,
            file_cache,
            indexed: false,
            agent_loop,
        })
    }
}

/// Drain any buffered data from stdin without blocking.
///
/// This prevents leftover lines (e.g. from a multi-line paste in the REPL)
/// from being consumed by confirmation prompts.
#[cfg(unix)]
fn drain_stdin() {
    use std::io::BufRead;

    let stdin = io::stdin();
    let mut handle = stdin.lock();

    // Set stdin to non-blocking so fill_buf/read_line return immediately
    // when the buffer is empty instead of waiting for new input.
    let fd = libc::STDIN_FILENO;
    let old_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    unsafe {
        libc::fcntl(fd, libc::F_SETFL, old_flags | libc::O_NONBLOCK);
    }

    // Read and discard all buffered lines.
    // If data is in BufReader's internal buffer, read_line returns immediately.
    // If the buffer is empty, the non-blocking fd causes WouldBlock → we break.
    loop {
        let mut discard = String::new();
        match handle.read_line(&mut discard) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(_) => break,
        }
    }

    // Restore blocking mode for the actual confirmation read.
    unsafe {
        libc::fcntl(fd, libc::F_SETFL, old_flags);
    }
}

#[cfg(not(unix))]
fn drain_stdin() {
    // No-op on non-Unix platforms.
}
