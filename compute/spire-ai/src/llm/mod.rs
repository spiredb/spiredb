//! LLM providers for spire-ai.

pub mod types;

#[cfg(feature = "ollama")]
pub mod ollama;

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "anthropic")]
pub mod anthropic;

use async_trait::async_trait;

use crate::error::Result;
pub use types::{ChatContent, ChatMessage, ChatResponse, ChatRole, ToolCallRequest, ToolDef};

/// Trait for Large Language Model providers.
#[async_trait]
pub trait Llm: Send + Sync {
    /// Generate a response from a prompt.
    async fn generate(&self, prompt: &str) -> Result<String>;

    /// Generate a response with a system prompt and user message.
    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String>;

    /// Return the model name.
    fn model_name(&self) -> &str;

    /// Multi-turn chat with optional tool definitions.
    ///
    /// Default implementation concatenates messages and calls `generate_with_system`,
    /// ignoring tools. Providers should override for native tool-calling support.
    async fn chat(&self, messages: &[ChatMessage], _tools: &[ToolDef]) -> Result<ChatResponse> {
        let mut system = String::new();
        let mut user_parts = Vec::new();

        for msg in messages {
            match (&msg.role, &msg.content) {
                (ChatRole::System, ChatContent::Text { text }) => {
                    system = text.clone();
                }
                (ChatRole::User, ChatContent::Text { text }) => {
                    user_parts.push(format!("User: {text}"));
                }
                (ChatRole::Assistant, ChatContent::Text { text }) => {
                    user_parts.push(format!("Assistant: {text}"));
                }
                (ChatRole::Tool, ChatContent::ToolResult { result, .. }) => {
                    user_parts.push(format!("Tool result: {result}"));
                }
                (ChatRole::Assistant, ChatContent::ToolCalls { text: Some(t), .. }) => {
                    user_parts.push(format!("Assistant: {t}"));
                }
                _ => {}
            }
        }

        let user = user_parts.join("\n\n");
        let text = if system.is_empty() {
            self.generate(&user).await?
        } else {
            self.generate_with_system(&system, &user).await?
        };

        Ok(ChatResponse::Message(text))
    }
}
