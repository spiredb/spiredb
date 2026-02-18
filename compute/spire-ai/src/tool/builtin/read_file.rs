//! Read file tool — reads file content via FileCache.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::filecache::{FileCache, ReadResult};
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Reads a file, returning cached/diff-aware content with line numbers.
pub struct ReadFileTool {
    file_cache: Arc<FileCache>,
    project_dir: String,
}

impl ReadFileTool {
    pub fn new(file_cache: Arc<FileCache>, project_dir: impl Into<String>) -> Self {
        Self {
            file_cache,
            project_dir: project_dir.into(),
        }
    }

    fn resolve(&self, path: &str) -> String {
        let p = Path::new(path);
        if p.is_absolute() {
            path.to_string()
        } else {
            Path::new(&self.project_dir)
                .join(path)
                .to_string_lossy()
                .to_string()
        }
    }
}

fn numbered(content: &str, start: usize) -> String {
    let width = (start + content.lines().count()).to_string().len();
    content
        .lines()
        .enumerate()
        .map(|(i, line)| format!("{:>width$} | {line}", start + i + 1))
        .collect::<Vec<_>>()
        .join("\n")
}

#[async_trait]
impl Tool for ReadFileTool {
    fn name(&self) -> &str {
        "read_file"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "read_file".into(),
            description:
                "Read a file's contents with line numbers. Subsequent reads of the same file \
                 return only a diff of changes, saving tokens."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path (relative to project root or absolute)"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "0-based line offset to start reading from"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of lines to read"
                    }
                },
                "required": ["path"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("read_file: missing 'path'".into()))?;

        let resolved = self.resolve(path);

        let offset = args["offset"].as_u64().map(|v| v as usize);
        let limit = args["limit"].as_u64().map(|v| v as usize);

        let result = if let (Some(off), Some(lim)) = (offset, limit) {
            self.file_cache.read_file_range(&resolved, off, lim)?
        } else {
            self.file_cache.read_file(&resolved)?
        };

        let start_line = offset.unwrap_or(0);

        let output = match result {
            ReadResult::Fresh {
                content,
                lines,
                tokens_estimated,
            } => {
                let numbered_content = numbered(&content, start_line);
                format!("{path} ({lines} lines, ~{tokens_estimated} tokens)\n{numbered_content}")
            }
            ReadResult::Unchanged {
                path: p,
                lines,
                tokens_saved,
            } => format!("{p}: unchanged ({lines} lines, saved ~{tokens_saved} tokens)"),
            ReadResult::Modified {
                diff,
                lines_changed,
                tokens_saved,
            } => format!(
                "{path}: modified ({lines_changed} lines changed, saved ~{tokens_saved} tokens)\n{diff}"
            ),
        };

        Ok(ToolResult::ok(output))
    }
}
