//! Interceptors for producer and consumer

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::error::StreamError;
use super::types::{ConsumerRecord, OffsetAndMetadata, Record, RecordMetadata, TopicPartition};

/// Producer interceptor trait
///
/// Interceptors are called before send and after acknowledgement.
pub trait ProducerInterceptor: Send + Sync {
    /// Called before a record is sent
    ///
    /// Can modify the record before sending.
    fn on_send(&self, record: Record) -> Record {
        record
    }

    /// Called after acknowledgement (success or failure)
    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&StreamError>) {
        let _ = (metadata, error);
    }
}

/// Consumer interceptor trait
///
/// Interceptors are called after poll and before/after commit.
pub trait ConsumerInterceptor: Send + Sync {
    /// Called after records are fetched
    ///
    /// Can modify or filter records.
    fn on_consume(&self, records: Vec<ConsumerRecord>) -> Vec<ConsumerRecord> {
        records
    }

    /// Called before commit
    fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
        let _ = offsets;
    }
}

/// Metrics interceptor that tracks throughput
#[derive(Debug, Default)]
pub struct MetricsInterceptor {
    /// Number of records sent
    pub records_sent: AtomicU64,
    /// Number of records received
    pub records_received: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Number of errors
    pub errors: AtomicU64,
}

impl MetricsInterceptor {
    /// Create a new metrics interceptor
    pub fn new() -> Self {
        Self::default()
    }

    /// Get count of records sent
    pub fn records_sent(&self) -> u64 {
        self.records_sent.load(Ordering::Relaxed)
    }

    /// Get count of records received
    pub fn records_received(&self) -> u64 {
        self.records_received.load(Ordering::Relaxed)
    }

    /// Get total bytes sent
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    /// Get total bytes received
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Get error count
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

impl ProducerInterceptor for MetricsInterceptor {
    fn on_send(&self, record: Record) -> Record {
        self.records_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent
            .fetch_add(record.value.len() as u64, Ordering::Relaxed);
        record
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&StreamError>) {
        let _ = metadata;
        if error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ConsumerInterceptor for MetricsInterceptor {
    fn on_consume(&self, records: Vec<ConsumerRecord>) -> Vec<ConsumerRecord> {
        let count = records.len() as u64;
        let bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();

        self.records_received.fetch_add(count, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);

        records
    }
}

/// Logging interceptor for debugging
#[derive(Debug, Clone)]
pub struct LoggingInterceptor {
    /// Log level (trace, debug, info)
    pub level: LogLevel,
}

/// Log level for logging interceptor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    /// Trace level (most verbose)
    Trace,
    /// Debug level
    #[default]
    Debug,
    /// Info level
    Info,
}

impl LoggingInterceptor {
    /// Create with default debug level
    pub fn new() -> Self {
        Self {
            level: LogLevel::Debug,
        }
    }

    /// Create with specific level
    pub fn with_level(level: LogLevel) -> Self {
        Self { level }
    }
}

impl Default for LoggingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerInterceptor for LoggingInterceptor {
    fn on_send(&self, record: Record) -> Record {
        match self.level {
            LogLevel::Trace => {
                tracing::trace!(
                    topic = %record.topic,
                    key = ?record.key,
                    value_len = record.value.len(),
                    headers_count = record.headers.len(),
                    "sending record"
                );
            }
            LogLevel::Debug => {
                tracing::debug!(
                    topic = %record.topic,
                    value_len = record.value.len(),
                    "sending record"
                );
            }
            LogLevel::Info => {
                tracing::info!(topic = %record.topic, "sending record");
            }
        }
        record
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&StreamError>) {
        if let Some(e) = error {
            tracing::error!(topic = %metadata.topic, error = %e, "send failed");
        } else {
            tracing::debug!(
                topic = %metadata.topic,
                partition = metadata.partition,
                offset = metadata.offset,
                "record acknowledged"
            );
        }
    }
}

impl ConsumerInterceptor for LoggingInterceptor {
    fn on_consume(&self, records: Vec<ConsumerRecord>) -> Vec<ConsumerRecord> {
        if !records.is_empty() {
            match self.level {
                LogLevel::Trace => {
                    for record in &records {
                        tracing::trace!(
                            topic = %record.topic,
                            partition = record.partition,
                            offset = record.offset,
                            key = ?record.key,
                            value_len = record.value.len(),
                            "consumed record"
                        );
                    }
                }
                LogLevel::Debug => {
                    tracing::debug!(count = records.len(), "consumed records");
                }
                LogLevel::Info => {
                    tracing::info!(count = records.len(), "consumed records");
                }
            }
        }
        records
    }
}
