//! Embedding providers for spire-ai.
//!
//! Provides the [`Embedder`] trait and implementations for various providers.

pub mod cache;

#[cfg(feature = "ollama")]
pub mod ollama;

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "voyage")]
pub mod voyage;

use async_trait::async_trait;

use crate::error::Result;

/// Trait for embedding text into vector representations.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embed a single text string.
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Embed multiple texts in a batch (more efficient than calling embed() repeatedly).
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Return the dimensionality of the embedding vectors.
    fn dimensions(&self) -> usize;

    /// Return the model name.
    fn model_name(&self) -> &str;
}

/// A no-op embedder that always returns an error.
///
/// Used when no embedder is configured (e.g., for testing without embeddings).
pub struct NoOpEmbedder;

#[async_trait]
impl Embedder for NoOpEmbedder {
    async fn embed(&self, _text: &str) -> Result<Vec<f32>> {
        Err(crate::error::Error::NoEmbedder)
    }

    async fn embed_batch(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>> {
        Err(crate::error::Error::NoEmbedder)
    }

    fn dimensions(&self) -> usize {
        0
    }

    fn model_name(&self) -> &str {
        "none"
    }
}
