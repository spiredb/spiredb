//! Configuration types for streaming API

use super::types::{Acks, IsolationLevel, OffsetReset};
use std::time::Duration;

/// Compression algorithm for producer batches
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression
    #[default]
    None,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

/// Producer configuration
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// SpireDB endpoints (comma-separated)
    pub bootstrap_servers: String,
    /// Acknowledgement mode
    pub acks: Acks,
    /// Enable idempotent producer
    pub idempotence: bool,
    /// Transactional ID for exactly-once
    pub transactional_id: Option<String>,
    /// Batch linger time before sending
    pub linger: Duration,
    /// Max batch size in bytes
    pub batch_size: usize,
    /// Max in-flight requests per connection
    pub max_in_flight: u32,
    /// Request timeout
    pub request_timeout: Duration,
    /// Compression algorithm
    pub compression: Compression,
    /// Max retries for failed sends
    pub retries: u32,
    /// Retry backoff
    pub retry_backoff: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:6379".to_string(),
            acks: Acks::All,
            idempotence: false,
            transactional_id: None,
            linger: Duration::from_millis(5),
            batch_size: 16 * 1024, // 16KB
            max_in_flight: 5,
            request_timeout: Duration::from_secs(30),
            compression: Compression::None,
            retries: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

/// Consumer configuration
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// SpireDB endpoints (comma-separated)
    pub bootstrap_servers: String,
    /// Consumer group ID
    pub group_id: Option<String>,
    /// Enable auto-commit
    pub auto_commit: bool,
    /// Auto-commit interval
    pub auto_commit_interval: Duration,
    /// Offset reset behavior
    pub auto_offset_reset: OffsetReset,
    /// Isolation level for transactions
    pub isolation_level: IsolationLevel,
    /// Max records per poll
    pub max_poll_records: u32,
    /// Max poll interval before rebalance
    pub max_poll_interval: Duration,
    /// Session timeout
    pub session_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Fetch min bytes
    pub fetch_min_bytes: usize,
    /// Fetch max bytes
    pub fetch_max_bytes: usize,
    /// Fetch max wait time
    pub fetch_max_wait: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:6379".to_string(),
            group_id: None,
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            auto_offset_reset: OffsetReset::Latest,
            isolation_level: IsolationLevel::ReadUncommitted,
            max_poll_records: 500,
            max_poll_interval: Duration::from_secs(300),
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(3),
            fetch_min_bytes: 1,
            fetch_max_bytes: 50 * 1024 * 1024, // 50MB
            fetch_max_wait: Duration::from_millis(500),
        }
    }
}
