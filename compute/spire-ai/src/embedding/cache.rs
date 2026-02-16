//! Caching wrapper for embedders.
//!
//! Uses kovan-map's lock-free concurrent HashMap for zero-overhead
//! cache lookups under concurrent embedding requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use kovan_map::HashMap;

use crate::embedding::Embedder;
use crate::error::Result;

/// Wraps any [`Embedder`] with an in-memory lock-free cache to avoid re-embedding identical text.
///
/// Uses kovan-map's lock-free HashMap internally — lookups are a single atomic pointer
/// dereference with near-zero collisions.
pub struct CachedEmbedder {
    inner: Arc<dyn Embedder>,
    cache: HashMap<u64, Vec<f32>>,
    size: AtomicUsize,
    max_size: usize,
}

impl CachedEmbedder {
    /// Create a cached embedder with default max size (10,000 entries).
    pub fn new(inner: Arc<dyn Embedder>) -> Self {
        Self::with_max_size(inner, 10_000)
    }

    /// Create a cached embedder with a specific max size.
    pub fn with_max_size(inner: Arc<dyn Embedder>, max_size: usize) -> Self {
        Self {
            inner,
            cache: HashMap::new(),
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    fn hash_text(text: &str) -> u64 {
        ahash::RandomState::with_seeds(0, 0, 0, 0).hash_one(text)
    }

    /// Evict entries when cache exceeds max_size.
    /// Clears the entire cache — simple and lock-free.
    fn maybe_evict(&self) {
        let current = self.size.load(Ordering::Relaxed);
        if current > self.max_size {
            self.cache.clear();
            self.size.store(0, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl Embedder for CachedEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let hash = Self::hash_text(text);

        if let Some(cached) = self.cache.get(&hash) {
            return Ok(cached);
        }

        let embedding = self.inner.embed(text).await?;

        if self.cache.insert(hash, embedding.clone()).is_none() {
            self.size.fetch_add(1, Ordering::Relaxed);
        }

        self.maybe_evict();

        Ok(embedding)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        let mut uncached_indices = Vec::new();
        let mut uncached_texts = Vec::new();

        // Check cache for each text
        for (i, text) in texts.iter().enumerate() {
            let hash = Self::hash_text(text);
            if let Some(cached) = self.cache.get(&hash) {
                results.push(Some(cached));
            } else {
                results.push(None);
                uncached_indices.push(i);
                uncached_texts.push(text.clone());
            }
        }

        // Embed uncached texts in a batch
        if !uncached_texts.is_empty() {
            let embeddings = self.inner.embed_batch(&uncached_texts).await?;
            for (idx, embedding) in uncached_indices.into_iter().zip(embeddings) {
                let hash = Self::hash_text(&texts[idx]);
                if self.cache.insert(hash, embedding.clone()).is_none() {
                    self.size.fetch_add(1, Ordering::Relaxed);
                }
                results[idx] = Some(embedding);
            }
        }

        self.maybe_evict();

        Ok(results.into_iter().map(|r| r.unwrap()).collect())
    }

    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    fn model_name(&self) -> &str {
        self.inner.model_name()
    }
}
