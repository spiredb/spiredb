//! Search code tool — semantic code search via CodeIndex.

use async_trait::async_trait;
use serde_json::Value;

use crate::code::CodeIndex;
use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Semantic code search across the indexed codebase.
pub struct SearchCodeTool {
    code_index: CodeIndex,
}

impl SearchCodeTool {
    pub fn new(code_index: CodeIndex) -> Self {
        Self { code_index }
    }
}

#[async_trait]
impl Tool for SearchCodeTool {
    fn name(&self) -> &str {
        "search_code"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "search_code".into(),
            description:
                "Semantic code search. Finds relevant code chunks by meaning, not just keywords."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default: 10)"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let query = args["query"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("search_code: missing 'query'".into()))?;

        let hits = self.code_index.search(query).await?;

        let limit = args["limit"].as_u64().unwrap_or(10) as usize;

        if hits.is_empty() {
            return Ok(ToolResult::ok("No results found."));
        }

        let mut output = String::new();
        for hit in hits.iter().take(limit) {
            let name = hit.chunk.name.as_deref().unwrap_or("<anonymous>");
            let kind = format!("{:?}", hit.chunk.kind);
            output.push_str(&format!(
                "[{:.2}] {} ({}) — {}:{}-{}\n",
                hit.score, name, kind, hit.chunk.file, hit.chunk.start_line, hit.chunk.end_line
            ));
            // Show first 5 lines of code
            let preview: String = hit
                .chunk
                .code
                .lines()
                .take(5)
                .collect::<Vec<_>>()
                .join("\n");
            output.push_str(&preview);
            output.push_str("\n\n");
        }

        Ok(ToolResult::ok(output))
    }
}
