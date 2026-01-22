//! Error types for the streaming API
#![allow(dead_code)]

use thiserror::Error;

/// Errors that can occur during stream operations
#[derive(Debug, Error)]
pub enum StreamError {
    /// Connection to SpireDB failed
    #[error("connection error: {0}")]
    Connection(String),

    /// Request timed out
    #[error("timeout: {0}")]
    Timeout(String),

    /// Invalid configuration
    #[error("configuration error: {0}")]
    Config(String),

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serde(String),

    /// Topic not found
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    /// Partition not found
    #[error("partition not found: {topic}:{partition}")]
    PartitionNotFound { topic: String, partition: u32 },

    /// Consumer group not found
    #[error("group not found: {0}")]
    GroupNotFound(String),

    /// Consumer not found in group
    #[error("consumer not found: {consumer} in group {group}")]
    ConsumerNotFound { group: String, consumer: String },

    /// Offset out of range
    #[error("offset out of range: {topic}:{partition}@{offset}")]
    OffsetOutOfRange {
        topic: String,
        partition: u32,
        offset: u64,
    },

    /// Transaction error
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Authorization error
    #[error("authorization error: {0}")]
    Authorization(String),

    /// Unknown server error
    #[error("server error: {0}")]
    Server(String),

    /// gRPC transport error
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error
    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),

    /// RESP protocol error
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Generic internal error
    #[error("internal error: {0}")]
    Internal(String),
}
