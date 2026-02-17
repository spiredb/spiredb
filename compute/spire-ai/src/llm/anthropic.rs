//! Anthropic Claude LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::llm::Llm;

/// Anthropic Claude LLM provider.
pub struct AnthropicLlm {
    client: reqwest::Client,
    api_key: String,
    model: String,
}

impl AnthropicLlm {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for AnthropicLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        self.generate_with_system("You are a helpful assistant.", prompt)
            .await
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = AnthropicRequest {
            model: &self.model,
            max_tokens: 4096,
            system,
            messages: vec![AnthropicMessage {
                role: "user",
                content: user,
            }],
        };

        let response: AnthropicResponse = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic response parse failed: {e}")))?;

        response
            .content
            .into_iter()
            .next()
            .map(|c| c.text)
            .ok_or_else(|| Error::Llm("Anthropic returned no content".into()))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct AnthropicRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<AnthropicMessage<'a>>,
}

#[derive(Serialize)]
struct AnthropicMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContent>,
}

#[derive(Deserialize)]
struct AnthropicContent {
    text: String,
}
