//! Edit file tool — targeted text replacement in files.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::filecache::FileCache;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Targeted text replacement in a file. Requires user confirmation.
pub struct EditFileTool {
    file_cache: Arc<FileCache>,
    project_dir: String,
}

impl EditFileTool {
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
impl Tool for EditFileTool {
    fn name(&self) -> &str {
        "edit_file"
    }

    fn needs_confirmation(&self) -> bool {
        true
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "edit_file".into(),
            description: "Replace a specific text span in a file. You MUST provide the exact \
                          existing text to match (old_string) and the replacement (new_string). \
                          Always read_file first to get the exact content."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path (relative to project root)"
                    },
                    "old_string": {
                        "type": "string",
                        "description": "The exact existing text to find and replace (must be unique in the file)"
                    },
                    "new_string": {
                        "type": "string",
                        "description": "The replacement text"
                    }
                },
                "required": ["path", "old_string", "new_string"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("edit_file: missing 'path'".into()))?;
        let old_string = args["old_string"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("edit_file: missing 'old_string'".into()))?;
        let new_string = args["new_string"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("edit_file: missing 'new_string'".into()))?;

        let resolved = self.resolve(path);

        let content = tokio::fs::read_to_string(&resolved).await.map_err(|e| {
            crate::error::Error::Tool(format!("edit_file: cannot read {path}: {e}"))
        })?;

        // Check the old_string exists
        let count = content.matches(old_string).count();
        if count == 0 {
            return Ok(ToolResult::error(format!(
                "old_string not found in {path}. Use read_file first to see the exact content."
            )));
        }
        if count > 1 {
            return Ok(ToolResult::error(format!(
                "old_string matches {count} locations in {path}. Provide a larger, unique snippet."
            )));
        }

        let updated = content.replacen(old_string, new_string, 1);
        tokio::fs::write(&resolved, &updated).await?;

        // Invalidate file cache so next read_file sees fresh content
        self.file_cache.invalidate(&resolved);

        // Build a compact summary
        let old_lines = old_string.lines().count();
        let new_lines = new_string.lines().count();
        let diff_summary = if old_lines <= 6 && new_lines <= 6 {
            format!(
                "  - {}\n  + {}",
                old_string.lines().collect::<Vec<_>>().join("\n  - "),
                new_string.lines().collect::<Vec<_>>().join("\n  + "),
            )
        } else {
            format!("Replaced {old_lines} lines with {new_lines} lines in {path}")
        };

        Ok(ToolResult::ok(format!(
            "Applied edit to {path}:\n{diff_summary}"
        )))
    }
}
