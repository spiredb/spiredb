//! Reranking strategies for RAG pipelines.

use async_trait::async_trait;

use crate::error::Result;
use crate::rag::ScoredChunk;

/// Trait for reranking strategies.
#[async_trait]
pub trait RerankerFn: Send + Sync {
    /// Rerank chunks for better relevance ordering.
    async fn rerank(
        &self,
        query: &str,
        chunks: Vec<ScoredChunk>,
        top_k: usize,
    ) -> Result<Vec<ScoredChunk>>;
}

/// No-op reranker that passes through results unchanged.
pub struct NoReranker;

#[async_trait]
impl RerankerFn for NoReranker {
    async fn rerank(
        &self,
        _query: &str,
        mut chunks: Vec<ScoredChunk>,
        top_k: usize,
    ) -> Result<Vec<ScoredChunk>> {
        chunks.truncate(top_k);
        Ok(chunks)
    }
}
