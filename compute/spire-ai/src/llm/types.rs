//! Chat and tool-calling types for multi-turn LLM interactions.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Role in a chat conversation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
}

/// Content of a chat message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatContent {
    /// Plain text message.
    Text { text: String },
    /// Assistant response containing tool calls (and optional reasoning text).
    ToolCalls {
        text: Option<String>,
        calls: Vec<ToolCallRequest>,
    },
    /// Result from executing a tool.
    ToolResult { call_id: String, result: String },
}

/// A single message in a chat conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: ChatContent,
}

impl ChatMessage {
    /// Create a system message.
    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::System,
            content: ChatContent::Text { text: text.into() },
        }
    }

    /// Create a user message.
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            content: ChatContent::Text { text: text.into() },
        }
    }

    /// Create an assistant text message.
    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Assistant,
            content: ChatContent::Text { text: text.into() },
        }
    }

    /// Create an assistant message with tool calls.
    pub fn assistant_tool_calls(text: Option<String>, calls: Vec<ToolCallRequest>) -> Self {
        Self {
            role: ChatRole::Assistant,
            content: ChatContent::ToolCalls { text, calls },
        }
    }

    /// Create a tool result message.
    pub fn tool_result(call_id: impl Into<String>, result: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Tool,
            content: ChatContent::ToolResult {
                call_id: call_id.into(),
                result: result.into(),
            },
        }
    }

    /// Get the text content if this is a text message.
    pub fn text(&self) -> Option<&str> {
        match &self.content {
            ChatContent::Text { text } => Some(text),
            ChatContent::ToolCalls { text, .. } => text.as_deref(),
            ChatContent::ToolResult { result, .. } => Some(result),
        }
    }
}

/// Definition of a tool the LLM can call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    /// JSON Schema for the tool's parameters.
    pub parameters: Value,
}

/// A tool call request from the LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// Response from a chat completion.
#[derive(Debug, Clone)]
pub enum ChatResponse {
    /// Plain text response (no tool calls).
    Message(String),
    /// The LLM wants to call tools.
    ToolCalls {
        text: Option<String>,
        calls: Vec<ToolCallRequest>,
    },
}

impl ChatResponse {
    /// Get the text content (if any).
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Message(t) => Some(t),
            Self::ToolCalls { text, .. } => text.as_deref(),
        }
    }

    /// Whether this response contains tool calls.
    pub fn has_tool_calls(&self) -> bool {
        matches!(self, Self::ToolCalls { .. })
    }

    /// Get the tool calls (empty vec if Message).
    pub fn tool_calls(&self) -> &[ToolCallRequest] {
        match self {
            Self::Message(_) => &[],
            Self::ToolCalls { calls, .. } => calls,
        }
    }
}
