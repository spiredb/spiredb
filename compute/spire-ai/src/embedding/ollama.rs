//! Ollama embedding provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::embedding::Embedder;
use crate::error::{Error, Result};

/// Ollama embedding provider (offline, local).
pub struct OllamaEmbedder {
    client: reqwest::Client,
    url: String,
    model: String,
    dims: usize,
}

impl OllamaEmbedder {
    /// Create with default model (`nomic-embed-text`, 768 dimensions).
    pub fn new(url: impl Into<String>) -> Self {
        Self::with_model(url, "nomic-embed-text")
    }

    /// Create with a specific model.
    pub fn with_model(url: impl Into<String>, model: impl Into<String>) -> Self {
        let model = model.into();
        let dims = known_dimensions(&model);
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            model,
            dims,
        }
    }
}

#[async_trait]
impl Embedder for OllamaEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let request = OllamaEmbedRequest {
            model: &self.model,
            input: vec![text],
        };

        let response: OllamaEmbedResponse = self
            .client
            .post(format!("{}/api/embed", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Embedding(format!("ollama request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Embedding(format!("ollama response parse failed: {e}")))?;

        response
            .embeddings
            .into_iter()
            .next()
            .ok_or_else(|| Error::Embedding("ollama returned no embeddings".into()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let input: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let request = OllamaEmbedRequest {
            model: &self.model,
            input,
        };

        let response: OllamaEmbedResponse = self
            .client
            .post(format!("{}/api/embed", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Embedding(format!("ollama batch request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Embedding(format!("ollama batch response parse failed: {e}")))?;

        if response.embeddings.len() != texts.len() {
            return Err(Error::Embedding(format!(
                "expected {} embeddings, got {}",
                texts.len(),
                response.embeddings.len()
            )));
        }

        Ok(response.embeddings)
    }

    fn dimensions(&self) -> usize {
        self.dims
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct OllamaEmbedRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

#[derive(Deserialize)]
struct OllamaEmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

fn known_dimensions(model: &str) -> usize {
    match model {
        "nomic-embed-text" => 768,
        "mxbai-embed-large" => 1024,
        "all-minilm" => 384,
        "snowflake-arctic-embed" => 1024,
        _ => 768,
    }
}
