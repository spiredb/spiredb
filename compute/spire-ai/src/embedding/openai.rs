//! OpenAI embedding provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::embedding::Embedder;
use crate::error::{Error, Result};

/// OpenAI embedding provider.
pub struct OpenAIEmbedder {
    client: reqwest::Client,
    api_key: String,
    model: String,
    dims: usize,
}

impl OpenAIEmbedder {
    /// Create with default model (`text-embedding-3-small`, 1536 dimensions).
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_model(api_key, "text-embedding-3-small")
    }

    /// Create with a specific model.
    pub fn with_model(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        let model = model.into();
        let dims = known_dimensions(&model);
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model,
            dims,
        }
    }
}

#[async_trait]
impl Embedder for OpenAIEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut results = self.embed_batch(&[text.to_string()]).await?;
        results
            .pop()
            .ok_or_else(|| Error::Embedding("OpenAI returned no embeddings".into()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let request = OpenAIRequest {
            model: &self.model,
            input: texts,
        };

        let response: OpenAIResponse = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Embedding(format!("OpenAI request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Embedding(format!("OpenAI response parse failed: {e}")))?;

        let mut data = response.data;
        data.sort_by_key(|d| d.index);
        Ok(data.into_iter().map(|d| d.embedding).collect())
    }

    fn dimensions(&self) -> usize {
        self.dims
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct OpenAIRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

#[derive(Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbeddingData>,
}

#[derive(Deserialize)]
struct OpenAIEmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

fn known_dimensions(model: &str) -> usize {
    match model {
        "text-embedding-3-small" => 1536,
        "text-embedding-3-large" => 3072,
        "text-embedding-ada-002" => 1536,
        _ => 1536,
    }
}
