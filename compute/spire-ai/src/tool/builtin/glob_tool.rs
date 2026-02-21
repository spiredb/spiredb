//! Glob tool — finds files matching a glob pattern.

use std::path::Path;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Finds files matching a glob pattern relative to the project directory.
pub struct GlobTool {
    project_dir: String,
}

impl GlobTool {
    pub fn new(project_dir: impl Into<String>) -> Self {
        Self {
            project_dir: project_dir.into(),
        }
    }
}

const SKIP_DIRS: &[&str] = &[".git", "target", "node_modules", "__pycache__", ".venv"];

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "glob"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "glob".into(),
            description: "Find files matching a glob pattern (e.g. '**/*.rs', 'src/**/*.ts')."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern relative to project root (e.g. '**/*.rs')."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results (default 100)."
                    }
                },
                "required": ["pattern"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let pattern = match args["pattern"].as_str() {
            Some(p) => p,
            None => return Ok(ToolResult::error("missing required parameter: pattern")),
        };

        let limit = args["limit"].as_u64().unwrap_or(100) as usize;

        let full_pattern = Path::new(&self.project_dir)
            .join(pattern)
            .to_string_lossy()
            .to_string();

        let entries = match glob::glob(&full_pattern) {
            Ok(paths) => paths,
            Err(e) => return Ok(ToolResult::error(format!("invalid pattern: {e}"))),
        };

        let project_prefix = format!("{}/", self.project_dir.trim_end_matches('/'));
        let mut results = Vec::new();

        for entry in entries {
            let path = match entry {
                Ok(p) => p,
                Err(_) => continue,
            };

            let path_str = path.to_string_lossy();

            // Skip hidden/build dirs
            let should_skip = SKIP_DIRS
                .iter()
                .any(|d| path_str.contains(&format!("/{d}/")));
            if should_skip {
                continue;
            }

            let relative = path_str
                .strip_prefix(&project_prefix)
                .unwrap_or(&path_str)
                .to_string();

            results.push(relative);

            if results.len() >= limit {
                break;
            }
        }

        results.sort();

        if results.is_empty() {
            Ok(ToolResult::ok("No files matched."))
        } else {
            let count = results.len();
            let mut output = results.join("\n");
            if count >= limit {
                output.push_str(&format!("\n... (limited to {limit} results)"));
            }
            Ok(ToolResult::ok(output))
        }
    }
}
