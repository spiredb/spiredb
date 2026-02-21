//! Write file tool — writes content to a file with diff output.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::filecache::FileCache;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Writes content to a file. Shows a diff when overwriting. Requires user confirmation.
pub struct WriteFileTool {
    file_cache: Arc<FileCache>,
    project_dir: String,
}

impl WriteFileTool {
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

#[async_trait]
impl Tool for WriteFileTool {
    fn name(&self) -> &str {
        "write_file"
    }

    fn needs_confirmation(&self) -> bool {
        true
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "write_file".into(),
            description: "Create a new file or overwrite an existing one. For targeted edits to \
                         existing files, prefer edit_file instead. Creates parent directories \
                         if needed."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path (relative to project root or absolute)"
                    },
                    "content": {
                        "type": "string",
                        "description": "The full file content to write"
                    }
                },
                "required": ["path", "content"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("write_file: missing 'path'".into()))?;
        let content = args["content"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("write_file: missing 'content'".into()))?;

        let resolved = self.resolve(path);
        let lines = content.lines().count();

        // Check if file exists and build summary
        let existed = Path::new(&resolved).exists();
        let summary = if existed {
            let old = tokio::fs::read_to_string(&resolved).await.ok();
            let old_lines = old.as_ref().map(|c| c.lines().count()).unwrap_or(0);
            format!("Overwrote {path} ({old_lines} -> {lines} lines)")
        } else {
            format!("Created {path} ({lines} lines)")
        };

        // Ensure parent directory exists
        if let Some(parent) = Path::new(&resolved).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&resolved, content).await?;

        // Invalidate file cache so next read sees fresh content
        self.file_cache.invalidate(&resolved);

        Ok(ToolResult::ok(summary))
    }
}
