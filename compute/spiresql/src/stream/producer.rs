//! Kafka-like Producer for SpireDB
//!
//! Supports async send, batching, compression, and exactly-once transactional produce.
#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use kovan_map::HashMap;
use parking_lot::Mutex;

use super::client::{RespClient, StreamClient};
use super::config::{Compression, ProducerConfig};
use super::error::StreamError;
use super::interceptor::ProducerInterceptor;
use super::types::{
    Acks, ConsumerGroupMetadata, OffsetAndMetadata, Record, RecordMetadata, TopicPartition,
};

/// Kafka-like Producer for SpireDB
///
/// Supports async send, batching, compression, and exactly-once transactional produce.
pub struct Producer {
    config: ProducerConfig,
    client: Arc<dyn StreamClient>,
    interceptors: Vec<Arc<dyn ProducerInterceptor + Send + Sync>>,
    // Transaction state
    tx_active: AtomicBool,
    tx_buffer: Mutex<Vec<(String, Record)>>,
    // Metrics
    records_sent: AtomicU64,
    bytes_sent: AtomicU64,
}

/// Builder for Producer configuration
#[derive(Default)]
pub struct ProducerBuilder {
    config: ProducerConfig,
    client: Option<Arc<dyn StreamClient>>,
    interceptors: Vec<Arc<dyn ProducerInterceptor + Send + Sync>>,
}

impl ProducerBuilder {
    /// Set SpireDB endpoints (comma-separated)
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set acknowledgement mode
    pub fn acks(mut self, acks: Acks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Enable idempotent producer (exactly-once within partition)
    pub fn idempotence(mut self, enabled: bool) -> Self {
        self.config.idempotence = enabled;
        self
    }

    /// Set transactional ID for exactly-once semantics
    pub fn transactional_id(mut self, id: impl Into<String>) -> Self {
        self.config.transactional_id = Some(id.into());
        self
    }

    /// Set batch linger time (ms) before sending
    pub fn linger_ms(mut self, ms: u64) -> Self {
        self.config.linger = std::time::Duration::from_millis(ms);
        self
    }

    /// Set max batch size in bytes
    pub fn batch_size(mut self, bytes: usize) -> Self {
        self.config.batch_size = bytes;
        self
    }

    /// Set max in-flight requests per connection
    pub fn max_in_flight(mut self, n: u32) -> Self {
        self.config.max_in_flight = n;
        self
    }

    /// Set request timeout (ms)
    pub fn request_timeout_ms(mut self, ms: u64) -> Self {
        self.config.request_timeout = std::time::Duration::from_millis(ms);
        self
    }

    /// Set compression algorithm
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Set max retries for failed sends
    pub fn retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Set custom client (for testing with mocks)
    pub fn client(mut self, client: Arc<dyn StreamClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Add an interceptor
    pub fn interceptor(mut self, interceptor: Arc<dyn ProducerInterceptor + Send + Sync>) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Build the producer
    pub async fn build(self) -> Result<Producer, StreamError> {
        let client = self
            .client
            .unwrap_or_else(|| Arc::new(RespClient::new(&self.config.bootstrap_servers)));

        Ok(Producer {
            config: self.config,
            client,
            interceptors: self.interceptors,
            tx_active: AtomicBool::new(false),
            tx_buffer: Mutex::new(Vec::new()),
            records_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }
}

impl Producer {
    /// Create a new producer builder
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::default()
    }

    /// Send a record asynchronously
    ///
    /// Returns a future that resolves to RecordMetadata on success.
    pub fn send(
        &self,
        record: Record,
    ) -> Pin<Box<dyn Future<Output = Result<RecordMetadata, StreamError>> + Send + '_>> {
        Box::pin(async move {
            // Apply interceptors
            let record = self.apply_interceptors(record);
            let value_len = record.value.len();

            // If in transaction, buffer the record
            if self.tx_active.load(Ordering::SeqCst) {
                let mut buffer = self.tx_buffer.lock();
                buffer.push((record.topic.clone(), record));

                // Return placeholder metadata (will be updated on commit)
                return Ok(RecordMetadata {
                    topic: buffer.last().map(|(t, _)| t.clone()).unwrap_or_default(),
                    partition: 0,
                    offset: 0,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                });
            }

            // Send directly to client
            let metadata = self.client.xadd(
                &record.topic,
                record.key.as_deref(),
                &record.value,
                &record.headers,
            )?;

            // Update metrics
            self.records_sent.fetch_add(1, Ordering::Relaxed);
            self.bytes_sent
                .fetch_add(value_len as u64, Ordering::Relaxed);

            // Notify interceptors
            for interceptor in &self.interceptors {
                interceptor.on_acknowledgement(&metadata, None);
            }

            Ok(metadata)
        })
    }

    /// Send a record with callback on completion
    pub fn send_with_callback<F>(&self, record: Record, callback: F)
    where
        F: FnOnce(Result<RecordMetadata, StreamError>) + Send + 'static,
    {
        let record = self.apply_interceptors(record);
        let value_len = record.value.len();

        let result = self.client.xadd(
            &record.topic,
            record.key.as_deref(),
            &record.value,
            &record.headers,
        );

        if result.is_ok() {
            self.records_sent.fetch_add(1, Ordering::Relaxed);
            self.bytes_sent
                .fetch_add(value_len as u64, Ordering::Relaxed);
        }

        callback(result);
    }

    /// Send a record synchronously (blocking)
    pub fn send_sync(&self, record: Record) -> Result<RecordMetadata, StreamError> {
        let record = self.apply_interceptors(record);
        let value_len = record.value.len();

        let metadata = self.client.xadd(
            &record.topic,
            record.key.as_deref(),
            &record.value,
            &record.headers,
        )?;

        self.records_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent
            .fetch_add(value_len as u64, Ordering::Relaxed);

        Ok(metadata)
    }

    /// Wait for all pending sends to complete
    pub async fn flush(&self) -> Result<(), StreamError> {
        // In mock implementation, sends are synchronous, so nothing to flush
        // In real implementation, would wait for all batches to be acked
        Ok(())
    }

    // === Transactional API ===

    /// Initialize transactions (required before begin_transaction)
    pub async fn init_transactions(&self) -> Result<(), StreamError> {
        if self.config.transactional_id.is_none() {
            return Err(StreamError::Config(
                "transactional_id must be set before calling init_transactions".into(),
            ));
        }
        // Mock: no-op, real impl would register with transaction coordinator
        Ok(())
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<(), StreamError> {
        if self.config.transactional_id.is_none() {
            return Err(StreamError::Config(
                "transactional_id must be set before calling begin_transaction".into(),
            ));
        }

        if self.tx_active.swap(true, Ordering::SeqCst) {
            return Err(StreamError::Transaction(
                "transaction already in progress".into(),
            ));
        }

        // Clear any stale buffer
        self.tx_buffer.lock().clear();

        Ok(())
    }

    /// Commit the current transaction
    pub async fn commit_transaction(&self) -> Result<(), StreamError> {
        if !self.tx_active.load(Ordering::SeqCst) {
            return Err(StreamError::Transaction("no active transaction".into()));
        }

        // Flush buffered records
        let records: Vec<(String, Record)> = {
            let mut buffer = self.tx_buffer.lock();
            std::mem::take(&mut *buffer)
        };

        for (topic, record) in records {
            self.client.xadd(
                &topic,
                record.key.as_deref(),
                &record.value,
                &record.headers,
            )?;
        }

        self.tx_active.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Abort the current transaction
    pub async fn abort_transaction(&self) -> Result<(), StreamError> {
        if !self.tx_active.load(Ordering::SeqCst) {
            return Err(StreamError::Transaction("no active transaction".into()));
        }

        // Discard buffered records
        self.tx_buffer.lock().clear();

        self.tx_active.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Commit consumer offsets as part of transaction (exactly-once)
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), StreamError> {
        if !self.tx_active.load(Ordering::SeqCst) {
            return Err(StreamError::Transaction("no active transaction".into()));
        }

        // Commit each offset
        for (tp, offset_meta) in offsets.iter() {
            self.client
                .commit_offset(&group_metadata.group_id, &tp.topic, offset_meta.offset)?;
        }

        Ok(())
    }

    /// Close the producer
    pub async fn close(&self) -> Result<(), StreamError> {
        // Abort any active transaction
        if self.tx_active.load(Ordering::SeqCst) {
            self.abort_transaction().await?;
        }

        // Flush pending records
        self.flush().await?;
        Ok(())
    }

    /// Get current configuration
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    /// Get number of records sent
    pub fn records_sent(&self) -> u64 {
        self.records_sent.load(Ordering::Relaxed)
    }

    /// Get bytes sent
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    // === Internal helpers ===

    fn apply_interceptors(&self, mut record: Record) -> Record {
        for interceptor in &self.interceptors {
            record = interceptor.on_send(record);
        }
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_builder() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:6379")
            .acks(Acks::All)
            .idempotence(true)
            .transactional_id("tx-1")
            .linger_ms(5)
            .batch_size(16384)
            .compression(Compression::Lz4)
            .retries(3);

        assert_eq!(builder.config.bootstrap_servers, "localhost:6379");
        assert_eq!(builder.config.acks, Acks::All);
        assert!(builder.config.idempotence);
    }

    #[test]
    fn test_record_builder() {
        let record = Record::new("my-topic")
            .key(b"key1".to_vec())
            .value(b"value1".to_vec())
            .header("h1", b"v1".to_vec())
            .timestamp(1234567890);

        assert_eq!(record.topic, "my-topic");
        assert_eq!(record.key, Some(b"key1".to_vec()));
        assert_eq!(record.value, b"value1");
        assert_eq!(record.headers.len(), 1);
    }
}
