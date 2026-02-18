//! List files tool — lists files in a directory.

use std::path::Path;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Lists files and directories in a path.
pub struct ListFilesTool {
    project_dir: String,
}

impl ListFilesTool {
    pub fn new(project_dir: impl Into<String>) -> Self {
        Self {
            project_dir: project_dir.into(),
        }
    }

    fn resolve(&self, path: &str) -> String {
        if path.is_empty() || path == "." {
            return self.project_dir.clone();
        }
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
impl Tool for ListFilesTool {
    fn name(&self) -> &str {
        "list_files"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "list_files".into(),
            description: "List files and directories in a path.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory path (relative to project root or absolute). Defaults to project root."
                    }
                }
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let path = args["path"].as_str().unwrap_or(".");
        let resolved = self.resolve(path);

        let mut entries = Vec::new();
        let mut dir = tokio::fs::read_dir(&resolved).await?;

        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip hidden files
            if name.starts_with('.') {
                continue;
            }
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                entries.push(format!("{name}/"));
            } else {
                entries.push(name);
            }
        }

        entries.sort();
        Ok(ToolResult::ok(entries.join("\n")))
    }
}
