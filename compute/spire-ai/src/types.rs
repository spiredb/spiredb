//! Shared types for spire-ai.

/// Result of an ingest operation.
#[derive(Debug, Clone)]
pub struct IngestResult {
    /// Source identifier (file path, URL, etc.)
    pub source: String,
    /// Number of chunks created
    pub chunks: usize,
}

/// A generated answer from a RAG query.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The generated text
    pub text: String,
    /// Source chunks used to generate the answer
    pub sources: Vec<crate::rag::ScoredChunk>,
}

/// Statistics for a RAG pipeline.
#[derive(Debug, Clone)]
pub struct RagStats {
    /// Total number of chunks stored
    pub chunks: u64,
}

/// Result of a code indexing operation.
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Number of files indexed
    pub files: usize,
    /// Number of code chunks created
    pub chunks: usize,
    /// Number of symbols extracted
    pub symbols: usize,
    /// Errors encountered during indexing (non-fatal)
    pub errors: Vec<String>,
}
