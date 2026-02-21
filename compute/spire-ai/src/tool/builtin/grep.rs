//! Grep tool — searches file contents by regex.

use std::io::{BufRead, BufReader};

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Searches file contents using a regex pattern.
pub struct GrepTool {
    project_dir: String,
}

impl GrepTool {
    pub fn new(project_dir: impl Into<String>) -> Self {
        Self {
            project_dir: project_dir.into(),
        }
    }
}

const SKIP_DIRS: &[&str] = &[".git", "target", "node_modules", "__pycache__", ".venv"];

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "grep"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "grep".into(),
            description:
                "Search file contents by regex. Returns matching lines with file paths and line numbers."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for."
                    },
                    "glob": {
                        "type": "string",
                        "description": "Optional file glob filter (e.g. '*.rs', '*.ts')."
                    },
                    "case_insensitive": {
                        "type": "boolean",
                        "description": "Case-insensitive matching (default false)."
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of matching lines (default 50)."
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

        let case_insensitive = args["case_insensitive"].as_bool().unwrap_or(false);
        let max_results = args["max_results"].as_u64().unwrap_or(50) as usize;
        let glob_filter = args["glob"].as_str();

        let re = {
            let mut builder = regex::RegexBuilder::new(pattern);
            builder.case_insensitive(case_insensitive);
            match builder.build() {
                Ok(r) => r,
                Err(e) => return Ok(ToolResult::error(format!("invalid regex: {e}"))),
            }
        };

        let glob_pattern = glob_filter.and_then(|g| glob::Pattern::new(g).ok());

        let project_dir = self.project_dir.clone();

        // Run the blocking walk in a spawn_blocking to avoid blocking the async runtime
        let results = tokio::task::spawn_blocking(move || {
            let mut matches = Vec::new();
            let project_prefix = format!("{}/", project_dir.trim_end_matches('/'));

            for entry in walkdir::WalkDir::new(&project_dir)
                .into_iter()
                .filter_entry(|e| {
                    let name = e.file_name().to_string_lossy();
                    !SKIP_DIRS.contains(&name.as_ref())
                })
            {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                if !entry.file_type().is_file() {
                    continue;
                }

                let path = entry.path();
                let relative = path
                    .to_string_lossy()
                    .strip_prefix(&project_prefix)
                    .unwrap_or(&path.to_string_lossy())
                    .to_string();

                // Apply glob filter on filename
                if let Some(ref pat) = glob_pattern {
                    let file_name = path
                        .file_name()
                        .map(|n| n.to_string_lossy())
                        .unwrap_or_default();
                    if !pat.matches(&file_name) {
                        continue;
                    }
                }

                // Check for binary files (read first 512 bytes)
                let file = match std::fs::File::open(path) {
                    Ok(f) => f,
                    Err(_) => continue,
                };

                let reader = BufReader::new(file);
                for (line_num, line) in reader.lines().enumerate() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => break, // likely binary
                    };

                    if re.is_match(&line) {
                        let truncated = if line.len() > 200 {
                            format!("{}...", &line[..200])
                        } else {
                            line
                        };
                        matches.push(format!("{}:{}: {}", relative, line_num + 1, truncated));

                        if matches.len() >= max_results {
                            return matches;
                        }
                    }
                }
            }

            matches
        })
        .await
        .unwrap_or_default();

        if results.is_empty() {
            Ok(ToolResult::ok("No matches found."))
        } else {
            let count = results.len();
            let mut output = results.join("\n");
            if count >= max_results {
                output.push_str(&format!("\n... (limited to {max_results} results)"));
            }
            Ok(ToolResult::ok(output))
        }
    }
}
