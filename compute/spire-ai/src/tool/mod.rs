//! Tool trait and registry for agent tool-calling.

pub mod builtin;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::llm::types::ToolDef;

/// Result of a tool execution.
#[derive(Debug, Clone)]
pub struct ToolResult {
    pub content: String,
}

impl ToolResult {
    /// Create a successful tool result.
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }

    /// Create an error tool result.
    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            content: format!("Error: {}", msg.into()),
        }
    }
}

/// A callable tool that an agent can invoke.
#[async_trait]
pub trait Tool: Send + Sync {
    /// The unique name of this tool.
    fn name(&self) -> &str;

    /// Whether this tool requires user confirmation before execution.
    fn needs_confirmation(&self) -> bool {
        false
    }

    /// Return the tool definition (name, description, JSON Schema parameters).
    fn def(&self) -> ToolDef;

    /// Execute the tool with the given arguments.
    async fn call(&self, args: Value) -> Result<ToolResult>;
}

/// A registry of tools available to an agent.
pub struct ToolRegistry {
    tools: Vec<Box<dyn Tool>>,
}

impl ToolRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    /// Register a tool.
    pub fn register(&mut self, tool: Box<dyn Tool>) {
        self.tools.push(tool);
    }

    /// Get all tool definitions.
    pub fn defs(&self) -> Vec<ToolDef> {
        self.tools.iter().map(|t| t.def()).collect()
    }

    /// Find a tool by name.
    pub fn find(&self, name: &str) -> Option<&dyn Tool> {
        self.tools
            .iter()
            .find(|t| t.name() == name)
            .map(|t| t.as_ref())
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}
