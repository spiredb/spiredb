//! Remember tool — stores information in agent memory.

use async_trait::async_trait;
use serde_json::Value;

use crate::agent::AgentMemory;
use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Store information in persistent agent memory.
pub struct RememberTool {
    memory: AgentMemory,
}

impl RememberTool {
    pub fn new(memory: AgentMemory) -> Self {
        Self { memory }
    }
}

#[async_trait]
impl Tool for RememberTool {
    fn name(&self) -> &str {
        "remember"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "remember".into(),
            description: "Store important information in persistent memory for later recall."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The information to remember"
                    },
                    "importance": {
                        "type": "string",
                        "enum": ["low", "normal", "high", "critical"],
                        "description": "Importance level (default: normal)"
                    }
                },
                "required": ["content"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let content = args["content"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("remember: missing 'content'".into()))?;

        let importance = match args["importance"].as_str() {
            Some("low") => crate::agent::Importance::Low,
            Some("high") => crate::agent::Importance::High,
            Some("critical") => crate::agent::Importance::Critical,
            _ => crate::agent::Importance::Normal,
        };

        let id = self.memory.remember_with(content, importance).await?;
        Ok(ToolResult::ok(format!("Remembered (id: {id})")))
    }
}
