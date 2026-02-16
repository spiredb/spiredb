//! RAG (Retrieval-Augmented Generation) pipeline.
//!
//! Provides composable chunking, retrieval, and reranking for building
//! question-answering systems on top of SpireDB.

pub mod chunker;
pub mod reranker;
pub mod retriever;

use crate::client::Spire;
use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::types::{Answer, IngestResult, RagStats};

pub use chunker::{Chunk, ChunkerFn};
pub use reranker::RerankerFn;
pub use retriever::RetrieverFn;

/// A scored chunk from retrieval or reranking.
#[derive(Debug, Clone)]
pub struct ScoredChunk {
    pub chunk: Chunk,
    pub score: f32,
}

/// A complete RAG pipeline: ingest -> chunk -> embed -> store -> retrieve -> answer.
pub struct RagPipeline {
    spire: Spire,
    collection: Collection<Chunk>,
    chunker: Box<dyn ChunkerFn>,
    retriever: Box<dyn RetrieverFn>,
    reranker: Box<dyn RerankerFn>,
}

impl RagPipeline {
    /// Ingest raw text with a source identifier.
    pub async fn ingest_text(&self, text: &str, source: &str) -> Result<IngestResult> {
        let chunks = self.chunker.chunk(text, source).await?;
        let count = chunks.len();
        self.collection.insert_many(&chunks).await?;
        Ok(IngestResult {
            source: source.to_string(),
            chunks: count,
        })
    }

    /// Ingest a file (reads content and chunks it).
    pub async fn ingest_file(&self, path: &str) -> Result<IngestResult> {
        let content = tokio::fs::read_to_string(path).await?;
        self.ingest_text(&content, path).await
    }

    /// Ingest all files in a directory.
    pub async fn ingest_dir(&self, path: &str) -> Result<IngestResult> {
        let mut total_chunks = 0;
        let mut entries = tokio::fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Ok(content) = tokio::fs::read_to_string(&path).await {
                    let source = path.to_string_lossy().to_string();
                    let chunks = self.chunker.chunk(&content, &source).await?;
                    total_chunks += chunks.len();
                    self.collection.insert_many(&chunks).await?;
                }
            }
        }

        Ok(IngestResult {
            source: path.to_string(),
            chunks: total_chunks,
        })
    }

    /// Retrieve relevant chunks for a query.
    pub async fn retrieve(&self, query: &str) -> Result<Vec<ScoredChunk>> {
        let candidates = self.retriever.retrieve(query, &self.collection, 20).await?;
        self.reranker.rerank(query, candidates, 5).await
    }

    /// Full RAG: retrieve context then generate an answer using the configured LLM.
    pub async fn query(&self, question: &str) -> Result<Answer> {
        let llm = self.spire.llm().ok_or(Error::NoLlm)?;

        let chunks = self.retrieve(question).await?;

        let context: String = chunks
            .iter()
            .enumerate()
            .map(|(i, sc)| format!("[{}] {}", i + 1, sc.chunk.text))
            .collect::<Vec<_>>()
            .join("\n\n---\n\n");

        let system = "You are a helpful assistant. Answer the question based on the provided context. \
                       If the context doesn't contain enough information, say so.";
        let user = format!("Context:\n{context}\n\nQuestion: {question}\n\nAnswer:");

        let text = llm.generate_with_system(system, &user).await?;

        Ok(Answer {
            text,
            sources: chunks,
        })
    }

    /// Clear all chunks from this pipeline's collection.
    pub async fn clear(&self) -> Result<()> {
        // TODO: Implement bulk delete via DataAccess
        Ok(())
    }

    /// Get statistics for this pipeline.
    pub async fn stats(&self) -> Result<RagStats> {
        // TODO: Implement via DataAccess stats
        Ok(RagStats { chunks: 0 })
    }
}

/// Builder for constructing a [`RagPipeline`].
pub struct RagBuilder {
    spire: Spire,
    name: String,
    chunker: Option<Box<dyn ChunkerFn>>,
    retriever: Option<Box<dyn RetrieverFn>>,
    reranker: Option<Box<dyn RerankerFn>>,
}

impl RagBuilder {
    pub(crate) fn new(spire: Spire, name: String) -> Self {
        Self {
            spire,
            name,
            chunker: None,
            retriever: None,
            reranker: None,
        }
    }

    /// Use fixed-size chunking.
    pub fn chunker_fixed(mut self, size: usize, overlap: usize) -> Self {
        self.chunker = Some(Box::new(chunker::FixedChunker::new(size, overlap)));
        self
    }

    /// Use sentence-based chunking.
    pub fn chunker_sentence(mut self, per_chunk: usize) -> Self {
        self.chunker = Some(Box::new(chunker::SentenceChunker::new(per_chunk)));
        self
    }

    /// Use markdown-aware chunking.
    pub fn chunker_markdown(mut self) -> Self {
        self.chunker = Some(Box::new(chunker::MarkdownChunker));
        self
    }

    /// Use a custom chunker.
    pub fn chunker(mut self, c: Box<dyn ChunkerFn>) -> Self {
        self.chunker = Some(c);
        self
    }

    /// Use vector-only retrieval.
    pub fn retriever_vector(mut self, k: usize) -> Self {
        self.retriever = Some(Box::new(retriever::VectorRetriever::new(k)));
        self
    }

    /// Use a custom retriever.
    pub fn retriever(mut self, r: Box<dyn RetrieverFn>) -> Self {
        self.retriever = Some(r);
        self
    }

    /// Disable reranking.
    pub fn reranker_none(mut self) -> Self {
        self.reranker = Some(Box::new(reranker::NoReranker));
        self
    }

    /// Use a custom reranker.
    pub fn reranker(mut self, r: Box<dyn RerankerFn>) -> Self {
        self.reranker = Some(r);
        self
    }

    /// Build the RAG pipeline with defaults for any unset components.
    pub fn build(self) -> RagPipeline {
        let collection = self.spire.collection::<Chunk>(&self.name);

        RagPipeline {
            spire: self.spire,
            collection,
            chunker: self
                .chunker
                .unwrap_or_else(|| Box::new(chunker::MarkdownChunker)),
            retriever: self
                .retriever
                .unwrap_or_else(|| Box::new(retriever::VectorRetriever::new(10))),
            reranker: self
                .reranker
                .unwrap_or_else(|| Box::new(reranker::NoReranker)),
        }
    }
}
