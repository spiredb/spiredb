//! Find symbol tool — looks up symbols by name via CodeIndex.

use async_trait::async_trait;
use serde_json::Value;

use crate::code::CodeIndex;
use crate::error::Result;
use crate::llm::types::ToolDef;
use crate::tool::{Tool, ToolResult};

/// Find a code symbol by name.
pub struct FindSymbolTool {
    code_index: CodeIndex,
}

impl FindSymbolTool {
    pub fn new(code_index: CodeIndex) -> Self {
        Self { code_index }
    }
}

#[async_trait]
impl Tool for FindSymbolTool {
    fn name(&self) -> &str {
        "find_symbol"
    }

    fn def(&self) -> ToolDef {
        ToolDef {
            name: "find_symbol".into(),
            description: "Find a code symbol (function, struct, class, etc.) by name.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Symbol name to search for"
                    }
                },
                "required": ["name"]
            }),
        }
    }

    async fn call(&self, args: Value) -> Result<ToolResult> {
        let name = args["name"]
            .as_str()
            .ok_or_else(|| crate::error::Error::Tool("find_symbol: missing 'name'".into()))?;

        let chunks = self.code_index.find_symbol(name).await?;

        if chunks.is_empty() {
            return Ok(ToolResult::ok(format!("No symbols found matching: {name}")));
        }

        let mut output = String::new();
        for chunk in &chunks {
            let kind = format!("{:?}", chunk.kind);
            let sym_name = chunk.name.as_deref().unwrap_or("?");
            output.push_str(&format!(
                "{} ({}) — {}:{}-{}\n",
                sym_name, kind, chunk.file, chunk.start_line, chunk.end_line
            ));
            if let Some(sig) = &chunk.signature {
                output.push_str(&format!("  {sig}\n"));
            }
            output.push_str(&chunk.code);
            output.push_str("\n\n");
        }

        Ok(ToolResult::ok(output))
    }
}
