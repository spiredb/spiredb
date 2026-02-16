//! Retrieval strategies for RAG pipelines.

use async_trait::async_trait;

use crate::collection::Collection;
use crate::error::Result;
use crate::rag::ScoredChunk;
use crate::rag::chunker::Chunk;

/// Trait for retrieval strategies.
#[async_trait]
pub trait RetrieverFn: Send + Sync {
    /// Retrieve relevant chunks for a query.
    async fn retrieve(
        &self,
        query: &str,
        collection: &Collection<Chunk>,
        k: usize,
    ) -> Result<Vec<ScoredChunk>>;
}

/// Pure vector search retriever.
pub struct VectorRetriever {
    k: usize,
}

impl VectorRetriever {
    pub fn new(k: usize) -> Self {
        Self { k }
    }
}

#[async_trait]
impl RetrieverFn for VectorRetriever {
    async fn retrieve(
        &self,
        query: &str,
        collection: &Collection<Chunk>,
        k: usize,
    ) -> Result<Vec<ScoredChunk>> {
        let limit = k.min(self.k);
        let hits = collection.search(query).limit(limit).run().await?;

        Ok(hits
            .into_iter()
            .map(|h| ScoredChunk {
                chunk: h.doc,
                score: h.score,
            })
            .collect())
    }
}
