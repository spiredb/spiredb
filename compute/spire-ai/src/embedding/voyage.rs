//! Voyage AI embedding provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::embedding::Embedder;
use crate::error::{Error, Result};

/// Voyage AI embedding provider (excellent for code).
pub struct VoyageEmbedder {
    client: reqwest::Client,
    api_key: String,
    model: String,
    dims: usize,
}

impl VoyageEmbedder {
    /// Create with a specific model (e.g., `voyage-3`, `voyage-code-3`).
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
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
impl Embedder for VoyageEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut results = self.embed_batch(&[text.to_string()]).await?;
        results
            .pop()
            .ok_or_else(|| Error::Embedding("Voyage returned no embeddings".into()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let request = VoyageRequest {
            model: &self.model,
            input: texts,
        };

        let response: VoyageResponse = self
            .client
            .post("https://api.voyageai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Embedding(format!("Voyage request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Embedding(format!("Voyage response parse failed: {e}")))?;

        Ok(response.data.into_iter().map(|d| d.embedding).collect())
    }

    fn dimensions(&self) -> usize {
        self.dims
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct VoyageRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

#[derive(Deserialize)]
struct VoyageResponse {
    data: Vec<VoyageEmbeddingData>,
}

#[derive(Deserialize)]
struct VoyageEmbeddingData {
    embedding: Vec<f32>,
}

fn known_dimensions(model: &str) -> usize {
    match model {
        "voyage-3" => 1024,
        "voyage-3-lite" => 512,
        "voyage-code-3" => 1024,
        _ => 1024,
    }
}
