//! OpenAI LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::llm::Llm;

/// OpenAI LLM provider.
pub struct OpenAiLlm {
    client: reqwest::Client,
    api_key: String,
    model: String,
}

impl OpenAiLlm {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for OpenAiLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        self.generate_with_system("You are a helpful assistant.", prompt)
            .await
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = ChatRequest {
            model: &self.model,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: system,
                },
                ChatMessage {
                    role: "user",
                    content: user,
                },
            ],
        };

        let response: ChatResponse = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI response parse failed: {e}")))?;

        response
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| Error::Llm("OpenAI returned no choices".into()))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    content: String,
}
