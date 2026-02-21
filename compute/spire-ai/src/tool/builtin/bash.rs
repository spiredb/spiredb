//! Bash tool — executes shell commands in the project directory.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Executes bash commands in the project directory.
pub struct BashTool {
    project_dir: String,
    timeout_secs: u64,
}

impl BashTool {
    pub fn new(project_dir: impl Into<String>) -> Self {
        Self {
            project_dir: project_dir.into(),
            timeout_secs: 120,
        }
    }
}

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &str {
        "bash"
    }

    fn needs_confirmation(&self) -> bool {
        true
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "bash".into(),
            description: "Run a shell command. Use for building, testing, installing, git, etc."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The bash command to execute."
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default 30, max 120)."
                    }
                },
                "required": ["command"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let command = match args["command"].as_str() {
            Some(c) => c,
            None => return Ok(ToolResult::error("missing required parameter: command")),
        };

        let timeout = args["timeout"]
            .as_u64()
            .unwrap_or(30)
            .min(self.timeout_secs);

        let child = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(command)
            .current_dir(&self.project_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn();

        let child = match child {
            Ok(c) => c,
            Err(e) => return Ok(ToolResult::error(format!("failed to spawn: {e}"))),
        };

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout),
            child.wait_with_output(),
        )
        .await;

        match result {
            Ok(Ok(output)) => {
                let code = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);

                let mut out = format!("exit {code}\n");
                if !stdout.is_empty() {
                    out.push_str(&stdout);
                }
                if !stderr.is_empty() {
                    if !stdout.is_empty() && !stdout.ends_with('\n') {
                        out.push('\n');
                    }
                    out.push_str(&stderr);
                }

                // Truncate very long output
                if out.len() > 30_000 {
                    out.truncate(30_000);
                    out.push_str("\n... (output truncated)");
                }

                Ok(ToolResult::ok(out))
            }
            Ok(Err(e)) => Ok(ToolResult::error(format!("command failed: {e}"))),
            Err(_) => Ok(ToolResult::error(format!("timed out after {timeout}s"))),
        }
    }
}
