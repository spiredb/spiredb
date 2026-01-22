//! Error types for the streaming API

use std::fmt;

/// Errors that can occur during stream operations
#[derive(Debug)]
pub enum StreamError {
    /// Connection to SpireDB failed
    Connection(String),

    /// Request timed out
    Timeout(String),

    /// Invalid configuration
    Config(String),

    /// Serialization/deserialization error
    Serde(String),

    /// Topic not found
    TopicNotFound(String),

    /// Partition not found
    PartitionNotFound { topic: String, partition: u32 },

    /// Consumer group not found
    GroupNotFound(String),

    /// Consumer not found in group
    ConsumerNotFound { group: String, consumer: String },

    /// Offset out of range
    OffsetOutOfRange {
        topic: String,
        partition: u32,
        offset: u64,
    },

    /// Transaction error
    Transaction(String),

    /// Authorization error
    Authorization(String),

    /// Unknown server error
    Server(String),

    /// gRPC transport error
    Transport(tonic::transport::Error),

    /// gRPC status error
    Status(tonic::Status),

    /// RESP protocol error
    Protocol(String),

    /// Generic internal error
    Internal(String),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(msg) => write!(f, "connection error: {}", msg),
            Self::Timeout(msg) => write!(f, "timeout: {}", msg),
            Self::Config(msg) => write!(f, "configuration error: {}", msg),
            Self::Serde(msg) => write!(f, "serialization error: {}", msg),
            Self::TopicNotFound(topic) => write!(f, "topic not found: {}", topic),
            Self::PartitionNotFound { topic, partition } => {
                write!(f, "partition not found: {}:{}", topic, partition)
            }
            Self::GroupNotFound(group) => write!(f, "group not found: {}", group),
            Self::ConsumerNotFound { group, consumer } => {
                write!(f, "consumer not found: {} in group {}", consumer, group)
            }
            Self::OffsetOutOfRange {
                topic,
                partition,
                offset,
            } => {
                write!(f, "offset out of range: {}:{}@{}", topic, partition, offset)
            }
            Self::Transaction(msg) => write!(f, "transaction error: {}", msg),
            Self::Authorization(msg) => write!(f, "authorization error: {}", msg),
            Self::Server(msg) => write!(f, "server error: {}", msg),
            Self::Transport(e) => write!(f, "transport error: {}", e),
            Self::Status(s) => write!(f, "gRPC status: {}", s),
            Self::Protocol(msg) => write!(f, "protocol error: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            Self::Status(e) => Some(e),
            _ => None,
        }
    }
}

impl From<tonic::transport::Error> for StreamError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::Transport(e)
    }
}

impl From<tonic::Status> for StreamError {
    fn from(s: tonic::Status) -> Self {
        Self::Status(s)
    }
}
