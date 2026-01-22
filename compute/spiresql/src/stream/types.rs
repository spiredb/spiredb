//! Core types for Kafka-like streaming API
#![allow(dead_code)]

use std::collections::HashMap;

/// Record to send to a topic
#[derive(Debug, Clone, Default)]
pub struct Record {
    /// Target topic name
    pub topic: String,
    /// Optional key for partitioning
    pub key: Option<Vec<u8>>,
    /// Record value (payload)
    pub value: Vec<u8>,
    /// Optional headers
    pub headers: Vec<(String, Vec<u8>)>,
    /// Optional timestamp (ms since epoch)
    pub timestamp: Option<u64>,
}

impl Record {
    /// Create a new record for a topic
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            ..Default::default()
        }
    }

    /// Set the key
    pub fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the value
    pub fn value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = value.into();
        self
    }

    /// Add a header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Set the timestamp
    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = Some(ts);
        self
    }
}

/// Record received from a topic
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Source topic
    pub topic: String,
    /// Partition number
    pub partition: u32,
    /// Offset within partition
    pub offset: u64,
    /// Record timestamp (ms since epoch)
    pub timestamp: u64,
    /// Optional key
    pub key: Option<Vec<u8>>,
    /// Record value (payload)
    pub value: Vec<u8>,
    /// Headers
    pub headers: Vec<(String, Vec<u8>)>,
}

/// Metadata returned after successful send
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic the record was sent to
    pub topic: String,
    /// Partition the record was written to
    pub partition: u32,
    /// Offset assigned to the record
    pub offset: u64,
    /// Timestamp assigned to the record
    pub timestamp: u64,
}

/// Topic-partition pair
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: u32,
}

impl TopicPartition {
    /// Create a new topic-partition pair
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Offset and metadata for commit
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    /// Offset to commit
    pub offset: u64,
    /// Optional metadata string
    pub metadata: Option<String>,
}

impl OffsetAndMetadata {
    /// Create with just offset
    pub fn new(offset: u64) -> Self {
        Self {
            offset,
            metadata: None,
        }
    }

    /// Create with offset and metadata
    pub fn with_metadata(offset: u64, metadata: impl Into<String>) -> Self {
        Self {
            offset,
            metadata: Some(metadata.into()),
        }
    }
}

/// Acknowledgement mode for producer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// Fire and forget (no ack)
    None = 0,
    /// Wait for leader acknowledgement
    Leader = 1,
    /// Wait for all in-sync replicas
    #[default]
    All = -1,
}

/// Offset reset behavior when no committed offset exists
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OffsetReset {
    /// Start from the latest (newest) offset
    #[default]
    Latest,
    /// Start from the earliest (oldest) offset
    Earliest,
    /// Error if no offset exists
    None,
}

/// Consumer isolation level for transactional reads
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read all records including uncommitted
    #[default]
    ReadUncommitted,
    /// Only read committed transactional records
    ReadCommitted,
}

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Row inserted
    Insert,
    /// Row updated
    Update,
    /// Row deleted
    Delete,
}

/// CDC change event
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Source table name
    pub table: String,
    /// Operation type
    pub op: Op,
    /// Row state before operation (for Update/Delete)
    pub before: Option<serde_json::Value>,
    /// Row state after operation (for Insert/Update)
    pub after: Option<serde_json::Value>,
    /// Event timestamp (ms since epoch)
    pub timestamp: u64,
    /// Transaction ID if transactional
    pub tx_id: Option<String>,
}

/// Consumer group metadata (for transactional offset commits)
#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    /// Group ID
    pub group_id: String,
    /// Generation ID (increments on rebalance)
    pub generation_id: i32,
    /// Member ID assigned by coordinator
    pub member_id: String,
}

/// Options for pending entries query (XPENDING)
#[derive(Debug, Clone, Default)]
pub struct PendingOptions {
    /// Start ID filter (default: "-")
    pub start: Option<String>,
    /// End ID filter (default: "+")
    pub end: Option<String>,
    /// Max entries to return
    pub count: Option<u32>,
    /// Filter by consumer name
    pub consumer: Option<String>,
    /// Min idle time filter (ms)
    pub min_idle: Option<u64>,
}

/// Summary info for pending entries
#[derive(Debug, Clone)]
pub struct PendingInfo {
    /// Total number of pending entries
    pub count: u64,
    /// Smallest pending ID
    pub min_id: Option<String>,
    /// Largest pending ID
    pub max_id: Option<String>,
    /// Per-consumer pending counts
    pub consumers: HashMap<String, u64>,
}

/// Individual pending entry
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Stream entry ID
    pub id: String,
    /// Consumer that owns this entry
    pub consumer: String,
    /// Time since delivery (ms)
    pub idle_ms: u64,
    /// Number of delivery attempts
    pub delivery_count: u32,
}

/// Options for claiming messages (XCLAIM)
#[derive(Debug, Clone, Default)]
pub struct ClaimOptions {
    /// Set idle time after claim (ms)
    pub idle: Option<u64>,
    /// Set delivery time (ms since epoch)
    pub time: Option<u64>,
    /// Set delivery count
    pub retry_count: Option<u32>,
    /// Claim even if not in PEL
    pub force: bool,
    /// Return only IDs, not full records
    pub just_id: bool,
}

/// Result of auto-claim operation (XAUTOCLAIM)
#[derive(Debug, Clone)]
pub struct AutoClaimResult {
    /// Next ID to continue auto-claim from
    pub next_id: String,
    /// Claimed records
    pub records: Vec<ConsumerRecord>,
    /// IDs that were deleted while pending
    pub deleted_ids: Vec<String>,
}
