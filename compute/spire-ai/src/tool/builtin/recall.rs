//! Recall tool — searches agent memory.

use async_trait::async_trait;
use serde_json::Value;

use crate::agent::AgentMemory;
use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Search persistent agent memory for relevant information.
pub struct RecallTool {
    memory: AgentMemory,
}

impl RecallTool {
    pub fn new(memory: AgentMemory) -> Self {
        Self { memory }
    }
}

#[async_trait]
impl Tool for RecallTool {
    fn name(&self) -> &str {
        "recall"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "recall".into(),
            description: "Search persistent memory for relevant information.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for relevant memories"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max memories to return (default: 5)"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let query = args["query"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("recall: missing 'query'".into()))?;

        let limit = args["limit"].as_u64().unwrap_or(5) as usize;

        let memories = self.memory.recall_limit(query, limit).await?;

        if memories.is_empty() {
            return Ok(ToolResult::ok("No relevant memories found."));
        }

        let mut output = String::new();
        for m in &memories {
            let importance = format!("{:?}", m.importance);
            output.push_str(&format!("[{}] {}\n", importance, m.content));
        }

        Ok(ToolResult::ok(output))
    }
}
