//! LLM providers for spire-ai.

#[cfg(feature = "ollama")]
pub mod ollama;

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "anthropic")]
pub mod anthropic;

use async_trait::async_trait;

use crate::error::Result;

/// Trait for Large Language Model providers.
#[async_trait]
pub trait Llm: Send + Sync {
    /// Generate a response from a prompt.
    async fn generate(&self, prompt: &str) -> Result<String>;

    /// Generate a response with a system prompt and user message.
    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String>;

    /// Return the model name.
    fn model_name(&self) -> &str;
}
