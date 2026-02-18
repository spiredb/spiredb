//! Error types for spire-ai.

use thiserror::Error;

/// Errors that can occur in spire-ai operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("vector error: {0}")]
    Vector(#[from] spiresql::vector::error::VectorError),

    #[error("stream error: {0}")]
    Stream(#[from] spiresql::stream::error::StreamError),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("no embedder configured")]
    NoEmbedder,

    #[error("LLM error: {0}")]
    Llm(String),

    #[error("no LLM configured")]
    NoLlm,

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("document not found: {0}")]
    NotFound(String),

    #[error("collection not initialized — call ensure() first")]
    NotInitialized,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("tool error: {0}")]
    Tool(String),

    #[error("{0}")]
    Other(String),
}

/// Result type alias for spire-ai.
pub type Result<T> = std::result::Result<T, Error>;
