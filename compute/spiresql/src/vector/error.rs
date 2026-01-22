//! Vector operation errors.

use thiserror::Error;

/// Vector operation error
#[derive(Debug, Error)]
pub enum VectorError {
    /// Connection failed
    #[error("connection failed: {0}")]
    Connection(String),

    /// Index not found
    #[error("index not found: {0}")]
    IndexNotFound(String),

    /// Index already exists
    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for vector operations
pub type VectorResult<T> = Result<T, VectorError>;
