//! Ollama LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::llm::Llm;

/// Ollama LLM provider (offline, local).
pub struct OllamaLlm {
    client: reqwest::Client,
    url: String,
    model: String,
}

impl OllamaLlm {
    pub fn new(url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for OllamaLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        let request = OllamaGenerateRequest {
            model: &self.model,
            prompt,
            system: None,
            stream: false,
        };

        let response: OllamaGenerateResponse = self
            .client
            .post(format!("{}/api/generate", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("ollama request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("ollama response parse failed: {e}")))?;

        Ok(response.response)
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = OllamaGenerateRequest {
            model: &self.model,
            prompt: user,
            system: Some(system),
            stream: false,
        };

        let response: OllamaGenerateResponse = self
            .client
            .post(format!("{}/api/generate", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("ollama request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("ollama response parse failed: {e}")))?;

        Ok(response.response)
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct OllamaGenerateRequest<'a> {
    model: &'a str,
    prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaGenerateResponse {
    response: String,
}
