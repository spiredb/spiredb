//! Kafka-like Consumer for SpireDB
//!
//! Supports subscribe/poll pattern, manual/auto commit, consumer groups, and PEL operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kovan_map::HashMap;
use parking_lot::Mutex;

use super::cdc::CdcBuilder;
use super::client::{PendingResult, RespClient, StreamClient};
use super::config::ConsumerConfig;
use super::error::StreamError;
use super::interceptor::ConsumerInterceptor;
use super::types::{
    AutoClaimResult, ClaimOptions, ConsumerGroupMetadata, ConsumerRecord, IsolationLevel,
    OffsetAndMetadata, OffsetReset, PendingInfo, PendingOptions, TopicPartition,
};

/// Kafka-like Consumer for SpireDB
///
/// Supports subscribe/poll pattern, manual/auto commit, consumer groups, and PEL operations.
pub struct Consumer {
    config: ConsumerConfig,
    client: Arc<dyn StreamClient>,
    interceptors: Vec<Arc<dyn ConsumerInterceptor + Send + Sync>>,
    // Use parking_lot::Mutex for Vecs (not concurrent)
    subscription: Mutex<Vec<String>>,
    assignment: Mutex<Vec<TopicPartition>>,
    paused: Mutex<Vec<TopicPartition>>,
    // Use kovan_map::HashMap directly (lock-free concurrent hashmap)
    positions: HashMap<TopicPartition, u64>,
    last_poll_ids: HashMap<String, String>,
    // Metrics
    records_received: AtomicU64,
    bytes_received: AtomicU64,
}

/// Builder for Consumer configuration
#[derive(Default)]
pub struct ConsumerBuilder {
    config: ConsumerConfig,
    client: Option<Arc<dyn StreamClient>>,
    interceptors: Vec<Arc<dyn ConsumerInterceptor + Send + Sync>>,
}

impl ConsumerBuilder {
    /// Set SpireDB endpoints (comma-separated)
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set consumer group ID (enables offset management & rebalancing)
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_id = Some(id.into());
        self
    }

    /// Enable/disable auto-commit (default: true)
    pub fn auto_commit(mut self, enabled: bool) -> Self {
        self.config.auto_commit = enabled;
        self
    }

    /// Set auto-commit interval (ms)
    pub fn auto_commit_interval_ms(mut self, ms: u64) -> Self {
        self.config.auto_commit_interval = Duration::from_millis(ms);
        self
    }

    /// Set offset reset behavior when no committed offset exists
    pub fn auto_offset_reset(mut self, reset: OffsetReset) -> Self {
        self.config.auto_offset_reset = reset;
        self
    }

    /// Set isolation level for transactional reads
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Set max records per poll
    pub fn max_poll_records(mut self, n: u32) -> Self {
        self.config.max_poll_records = n;
        self
    }

    /// Set max poll interval before rebalance (ms)
    pub fn max_poll_interval_ms(mut self, ms: u64) -> Self {
        self.config.max_poll_interval = Duration::from_millis(ms);
        self
    }

    /// Set session timeout (ms)
    pub fn session_timeout_ms(mut self, ms: u64) -> Self {
        self.config.session_timeout = Duration::from_millis(ms);
        self
    }

    /// Set fetch min bytes
    pub fn fetch_min_bytes(mut self, bytes: usize) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set fetch max bytes
    pub fn fetch_max_bytes(mut self, bytes: usize) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set custom client (for testing with mocks)
    pub fn client(mut self, client: Arc<dyn StreamClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Add an interceptor
    pub fn interceptor(mut self, interceptor: Arc<dyn ConsumerInterceptor + Send + Sync>) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Build the consumer
    pub async fn build(self) -> Result<Consumer, StreamError> {
        let client = self
            .client
            .unwrap_or_else(|| Arc::new(RespClient::new(&self.config.bootstrap_servers)));

        Ok(Consumer {
            config: self.config,
            client,
            interceptors: self.interceptors,
            subscription: Mutex::new(Vec::new()),
            assignment: Mutex::new(Vec::new()),
            paused: Mutex::new(Vec::new()),
            positions: HashMap::new(),
            last_poll_ids: HashMap::new(),
            records_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        })
    }
}

impl Consumer {
    /// Create a new consumer builder
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::default()
    }

    /// Subscribe to topics (triggers rebalance for group consumers)
    pub async fn subscribe(&self, topics: &[&str]) -> Result<(), StreamError> {
        let topic_strings: Vec<String> = topics.iter().map(|s| s.to_string()).collect();

        // If using consumer group, create groups on the server
        if let Some(group_id) = &self.config.group_id {
            for topic in topics {
                let start_id = match self.config.auto_offset_reset {
                    OffsetReset::Earliest => "0-0",
                    OffsetReset::Latest => "$",
                    OffsetReset::None => "0-0",
                };
                self.client.xgroup_create(topic, group_id, start_id)?;
            }
        }

        // Update local subscription state
        *self.subscription.lock() = topic_strings.clone();

        // Generate assignment (simplified: one partition per topic)
        *self.assignment.lock() = topic_strings
            .iter()
            .map(|t| TopicPartition::new(t, 0))
            .collect();

        // Initialize last poll IDs (kovan_map is concurrent, no lock needed)
        for topic in topics {
            if self.last_poll_ids.get(*topic).is_none() {
                self.last_poll_ids
                    .insert(topic.to_string(), "0-0".to_string());
            }
        }

        Ok(())
    }

    /// Unsubscribe from all topics
    pub async fn unsubscribe(&self) -> Result<(), StreamError> {
        self.subscription.lock().clear();
        self.assignment.lock().clear();
        self.positions.clear();
        self.last_poll_ids.clear();
        Ok(())
    }

    /// Poll for records (with timeout)
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>, StreamError> {
        let topics: Vec<String> = self.subscription.lock().clone();

        if topics.is_empty() {
            return Ok(Vec::new());
        }

        // Get paused partitions
        let paused: Vec<String> = self
            .paused
            .lock()
            .iter()
            .map(|tp| tp.topic.clone())
            .collect();

        // Filter out paused topics
        let active_topics: Vec<&str> = topics
            .iter()
            .filter(|t| !paused.contains(t))
            .map(|s| s.as_str())
            .collect();

        if active_topics.is_empty() {
            return Ok(Vec::new());
        }

        // Get last poll IDs from concurrent hashmap
        let ids: Vec<String> = active_topics
            .iter()
            .map(|t| {
                self.last_poll_ids
                    .get(*t)
                    .unwrap_or_else(|| "0-0".to_string())
            })
            .collect();

        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        let block_ms = timeout.as_millis() as u64;

        // Choose read method based on group membership
        let results = if let Some(group_id) = &self.config.group_id {
            let consumer_id = format!("consumer-{}", std::process::id());
            self.client.xreadgroup(
                group_id,
                &consumer_id,
                &active_topics,
                Some(self.config.max_poll_records),
                Some(block_ms),
            )?
        } else {
            self.client.xread(
                &active_topics,
                &id_refs,
                Some(self.config.max_poll_records),
                Some(block_ms),
            )?
        };

        // Flatten results and apply interceptors
        let mut all_records = Vec::new();
        for (topic, records) in results {
            for record in records {
                // Update last poll ID (concurrent hashmap, no lock)
                let new_id = format!("{}-{}", record.timestamp, record.offset);
                self.last_poll_ids.insert(topic.clone(), new_id);

                // Update position (concurrent hashmap, no lock)
                self.positions.insert(
                    TopicPartition::new(&topic, record.partition),
                    record.offset + 1,
                );

                // Update metrics
                self.records_received.fetch_add(1, Ordering::Relaxed);
                self.bytes_received
                    .fetch_add(record.value.len() as u64, Ordering::Relaxed);

                // Apply interceptors
                let record = self.apply_interceptors(record);
                all_records.push(record);
            }
        }

        Ok(all_records)
    }

    /// Commit all polled offsets synchronously
    pub async fn commit(&self) -> Result<(), StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for offset commits".into())
            })?;

        // Iterate over concurrent hashmap
        for (tp, offset) in self.positions.iter() {
            self.client.commit_offset(group_id, &tp.topic, offset)?;
        }

        Ok(())
    }

    /// Commit specific offsets synchronously
    pub async fn commit_offsets(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for offset commits".into())
            })?;

        for (tp, offset_meta) in offsets.iter() {
            self.client
                .commit_offset(group_id, &tp.topic, offset_meta.offset)?;
        }

        Ok(())
    }

    /// Commit all polled offsets asynchronously
    pub fn commit_async(&self) {
        if let Some(group_id) = &self.config.group_id {
            for (tp, offset) in self.positions.iter() {
                let _ = self.client.commit_offset(group_id, &tp.topic, offset);
            }
        }
    }

    /// Commit specific offsets asynchronously with callback
    pub fn commit_async_with_callback<F>(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: F,
    ) where
        F: FnOnce(Result<(), StreamError>) + Send + 'static,
    {
        let result = if let Some(group_id) = &self.config.group_id {
            let mut err = None;
            for (tp, offset_meta) in offsets.iter() {
                if let Err(e) = self
                    .client
                    .commit_offset(group_id, &tp.topic, offset_meta.offset)
                {
                    err = Some(e);
                    break;
                }
            }
            err.map_or(Ok(()), Err)
        } else {
            Err(StreamError::Config(
                "group_id must be set for offset commits".into(),
            ))
        };

        callback(result);
    }

    /// Seek to specific offset for a partition
    pub fn seek(&self, tp: TopicPartition, offset: u64) {
        self.positions.insert(tp.clone(), offset);
        self.last_poll_ids.insert(tp.topic, format!("0-{}", offset));
    }

    /// Seek to beginning for partitions
    pub fn seek_to_beginning(&self, partitions: &[TopicPartition]) {
        for tp in partitions {
            self.positions.insert(tp.clone(), 0);
            self.last_poll_ids
                .insert(tp.topic.clone(), "0-0".to_string());
        }
    }

    /// Seek to end for partitions
    pub fn seek_to_end(&self, partitions: &[TopicPartition]) {
        for tp in partitions {
            let len = self.client.xlen(&tp.topic).unwrap_or(0);
            self.positions.insert(tp.clone(), len);
            self.last_poll_ids.insert(tp.topic.clone(), "$".to_string());
        }
    }

    /// Get current position (next offset to be fetched)
    pub fn position(&self, tp: &TopicPartition) -> Option<u64> {
        self.positions.get(tp)
    }

    /// Get committed offset for partitions
    pub async fn committed(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, StreamError> {
        let group_id = self.config.group_id.as_ref().ok_or_else(|| {
            StreamError::Config("group_id must be set to get committed offsets".into())
        })?;

        let result = HashMap::new();
        for tp in partitions {
            if let Some(offset) = self.client.get_committed_offset(group_id, &tp.topic)? {
                result.insert(tp.clone(), OffsetAndMetadata::new(offset));
            }
        }

        Ok(result)
    }

    /// Pause consumption for partitions
    pub fn pause(&self, partitions: &[TopicPartition]) {
        let mut paused = self.paused.lock();
        for tp in partitions {
            if !paused.contains(tp) {
                paused.push(tp.clone());
            }
        }
    }

    /// Resume consumption for partitions
    pub fn resume(&self, partitions: &[TopicPartition]) {
        self.paused.lock().retain(|tp| !partitions.contains(tp));
    }

    /// Get currently paused partitions
    pub fn paused(&self) -> Vec<TopicPartition> {
        self.paused.lock().clone()
    }

    /// Get current subscription
    pub fn subscription(&self) -> Vec<String> {
        self.subscription.lock().clone()
    }

    /// Get assigned partitions
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.assignment.lock().clone()
    }

    /// Get offsets for commit (for transactional producer)
    pub fn offsets_for_commit(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let result = HashMap::new();
        for (tp, offset) in self.positions.iter() {
            result.insert(tp.clone(), OffsetAndMetadata::new(offset));
        }
        result
    }

    /// Get group metadata (for transactional producer)
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.config.group_id.clone().unwrap_or_default(),
            generation_id: 0,
            member_id: format!("consumer-{}", std::process::id()),
        }
    }

    // === Pending Entry List (PEL) Operations ===

    /// Get pending entries for this consumer group (XPENDING)
    pub async fn pending(
        &self,
        topic: &str,
        opts: PendingOptions,
    ) -> Result<PendingInfo, StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for PEL operations".into())
            })?;

        let result: PendingResult = self.client.xpending(
            topic,
            group_id,
            opts.start.as_deref(),
            opts.end.as_deref(),
            opts.count,
            opts.consumer.as_deref(),
        )?;

        Ok(PendingInfo {
            count: result.count,
            min_id: result.min_id,
            max_id: result.max_id,
            consumers: result.consumers,
        })
    }

    /// Claim messages from idle consumers (XCLAIM)
    pub async fn claim(
        &self,
        topic: &str,
        min_idle: Duration,
        ids: &[&str],
        opts: ClaimOptions,
    ) -> Result<Vec<ConsumerRecord>, StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for PEL operations".into())
            })?;

        let consumer_id = format!("consumer-{}", std::process::id());

        self.client.xclaim(
            topic,
            group_id,
            &consumer_id,
            min_idle.as_millis() as u64,
            ids,
            opts.force,
        )
    }

    /// Auto-claim idle messages starting from an ID (XAUTOCLAIM)
    pub async fn auto_claim(
        &self,
        topic: &str,
        min_idle: Duration,
        start_id: &str,
        count: u32,
    ) -> Result<AutoClaimResult, StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for PEL operations".into())
            })?;

        let consumer_id = format!("consumer-{}", std::process::id());

        let response = self.client.xautoclaim(
            topic,
            group_id,
            &consumer_id,
            min_idle.as_millis() as u64,
            start_id,
            count,
        )?;

        Ok(AutoClaimResult {
            next_id: response.next_id,
            records: response.records,
            deleted_ids: response.deleted_ids,
        })
    }

    /// Acknowledge messages (XACK)
    pub async fn ack(&self, topic: &str, ids: &[&str]) -> Result<u32, StreamError> {
        let group_id =
            self.config.group_id.as_ref().ok_or_else(|| {
                StreamError::Config("group_id must be set for PEL operations".into())
            })?;

        self.client.xack(topic, group_id, ids)
    }

    /// Close the consumer
    pub async fn close(&self) -> Result<(), StreamError> {
        self.unsubscribe().await?;
        Ok(())
    }

    /// Get CDC builder for this consumer
    pub fn cdc(&self) -> CdcBuilder<'_> {
        CdcBuilder::from_consumer(self)
    }

    /// Get current configuration
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    /// Get number of records received
    pub fn records_received(&self) -> u64 {
        self.records_received.load(Ordering::Relaxed)
    }

    /// Get bytes received
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    // === Internal helpers ===

    fn apply_interceptors(&self, record: ConsumerRecord) -> ConsumerRecord {
        let mut records = vec![record];
        for interceptor in &self.interceptors {
            records = interceptor.on_consume(records);
        }
        records
            .into_iter()
            .next()
            .unwrap_or_else(|| ConsumerRecord {
                topic: String::new(),
                partition: 0,
                offset: 0,
                key: None,
                value: Vec::new(),
                headers: Vec::new(),
                timestamp: 0,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_builder() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:6379")
            .group_id("my-group")
            .auto_commit(false)
            .auto_commit_interval_ms(1000)
            .auto_offset_reset(OffsetReset::Earliest)
            .max_poll_records(100)
            .fetch_min_bytes(1024)
            .fetch_max_bytes(1048576);

        assert_eq!(builder.config.bootstrap_servers, "localhost:6379");
        assert_eq!(builder.config.group_id, Some("my-group".to_string()));
        assert!(!builder.config.auto_commit);
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("orders", 3);
        assert_eq!(tp.topic, "orders");
        assert_eq!(tp.partition, 3);
    }

    #[test]
    fn test_offset_and_metadata() {
        let offset = OffsetAndMetadata::new(1000);
        assert_eq!(offset.offset, 1000);
        assert!(offset.metadata.is_none());

        let offset = OffsetAndMetadata::with_metadata(2000, "metadata");
        assert_eq!(offset.offset, 2000);
        assert_eq!(offset.metadata.as_deref(), Some("metadata"));
    }

    #[test]
    fn test_pending_options() {
        let opts = PendingOptions {
            start: Some("-".to_string()),
            end: Some("+".to_string()),
            count: Some(10),
            consumer: None,
            min_idle: Some(60000),
        };

        assert_eq!(opts.count, Some(10));
        assert_eq!(opts.min_idle, Some(60000));
    }
}
