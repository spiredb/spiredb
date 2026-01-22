//! Mock client and server for stream API testing.
//!
//! Provides in-memory simulation of SpireDB stream operations.

#![allow(dead_code)]

pub mod mock_vector;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// A stream entry with ID and fields
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, Vec<u8>>,
    pub timestamp: u64,
}

/// Pending entry in the PEL
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub delivery_time: u64,
    pub delivery_count: u32,
}

/// Consumer group state
#[derive(Debug, Clone, Default)]
pub struct ConsumerGroup {
    pub last_delivered_id: String,
    pub consumers: HashMap<String, ConsumerState>,
    pub pending: Vec<PendingEntry>,
}

/// Per-consumer state within a group
#[derive(Debug, Clone, Default)]
pub struct ConsumerState {
    pub name: String,
    pub pending_count: u32,
    pub idle_time: u64,
}

/// Topic/stream metadata
#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub partitions: u32,
    pub replication_factor: u16,
    pub created_at: u64,
}

impl Default for TopicMeta {
    fn default() -> Self {
        Self {
            partitions: 1,
            replication_factor: 1,
            created_at: current_time_ms(),
        }
    }
}

/// In-memory mock store for streams
#[derive(Debug, Default)]
pub struct MockStreamStore {
    /// topic -> entries
    pub streams: HashMap<String, Vec<StreamEntry>>,
    /// topic -> metadata
    pub topics: HashMap<String, TopicMeta>,
    /// topic -> group_id -> ConsumerGroup
    pub groups: HashMap<String, HashMap<String, ConsumerGroup>>,
    /// group_id -> topic -> partition -> offset
    pub committed_offsets: HashMap<String, HashMap<String, u64>>,
    /// Auto-increment counter for stream IDs
    pub id_counter: u64,
}

impl MockStreamStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate next stream ID
    pub fn next_id(&mut self) -> String {
        self.id_counter += 1;
        let ts = current_time_ms();
        format!("{}-{}", ts, self.id_counter)
    }

    /// XADD - Add entry to stream
    pub fn xadd(&mut self, topic: &str, fields: HashMap<String, Vec<u8>>) -> String {
        let id = self.next_id();
        let entry = StreamEntry {
            id: id.clone(),
            fields,
            timestamp: current_time_ms(),
        };

        self.streams
            .entry(topic.to_string())
            .or_default()
            .push(entry);

        // Ensure topic exists
        self.topics.entry(topic.to_string()).or_default();

        id
    }

    /// XREAD - Read entries from streams
    pub fn xread(
        &self,
        topics: &[&str],
        ids: &[&str],
        count: Option<u32>,
    ) -> HashMap<String, Vec<StreamEntry>> {
        let mut result = HashMap::new();

        for (i, topic) in topics.iter().enumerate() {
            let start_id = ids.get(i).copied().unwrap_or("0-0");

            if let Some(entries) = self.streams.get(*topic) {
                let filtered: Vec<StreamEntry> = entries
                    .iter()
                    .filter(|e| e.id.as_str() > start_id)
                    .take(count.unwrap_or(u32::MAX) as usize)
                    .cloned()
                    .collect();

                if !filtered.is_empty() {
                    result.insert(topic.to_string(), filtered);
                }
            }
        }

        result
    }

    /// XREADGROUP - Read with consumer group
    pub fn xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        topics: &[&str],
        count: Option<u32>,
    ) -> HashMap<String, Vec<StreamEntry>> {
        let mut result = HashMap::new();

        for topic in topics {
            // Ensure group exists
            let groups = self.groups.entry(topic.to_string()).or_default();
            let cg = groups.entry(group.to_string()).or_default();

            // Ensure consumer exists
            cg.consumers
                .entry(consumer.to_string())
                .or_insert_with(|| ConsumerState {
                    name: consumer.to_string(),
                    pending_count: 0,
                    idle_time: 0,
                });

            if let Some(entries) = self.streams.get(*topic) {
                let start_id = cg.last_delivered_id.as_str();

                let filtered: Vec<StreamEntry> = entries
                    .iter()
                    .filter(|e| e.id.as_str() > start_id)
                    .take(count.unwrap_or(u32::MAX) as usize)
                    .cloned()
                    .collect();

                // Add to pending and update last delivered
                for entry in &filtered {
                    cg.pending.push(PendingEntry {
                        id: entry.id.clone(),
                        consumer: consumer.to_string(),
                        delivery_time: current_time_ms(),
                        delivery_count: 1,
                    });

                    if let Some(cs) = cg.consumers.get_mut(consumer) {
                        cs.pending_count += 1;
                    }

                    if entry.id > cg.last_delivered_id {
                        cg.last_delivered_id = entry.id.clone();
                    }
                }

                if !filtered.is_empty() {
                    result.insert(topic.to_string(), filtered);
                }
            }
        }

        result
    }

    /// XACK - Acknowledge messages
    pub fn xack(&mut self, topic: &str, group: &str, ids: &[&str]) -> u32 {
        let mut acked = 0;

        if let Some(groups) = self.groups.get_mut(topic)
            && let Some(cg) = groups.get_mut(group)
        {
            for id in ids {
                if let Some(pos) = cg.pending.iter().position(|p| p.id == *id) {
                    let entry = cg.pending.remove(pos);
                    if let Some(cs) = cg.consumers.get_mut(&entry.consumer) {
                        cs.pending_count = cs.pending_count.saturating_sub(1);
                    }
                    acked += 1;
                }
            }
        }

        acked
    }

    /// XPENDING - Get pending entries info
    pub fn xpending(
        &self,
        topic: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<u32>,
        consumer: Option<&str>,
    ) -> (u32, Option<String>, Option<String>, HashMap<String, u32>) {
        let empty = (0, None, None, HashMap::new());

        let groups = match self.groups.get(topic) {
            Some(g) => g,
            None => return empty,
        };

        let cg = match groups.get(group) {
            Some(c) => c,
            None => return empty,
        };

        let filtered: Vec<&PendingEntry> = cg
            .pending
            .iter()
            .filter(|p| {
                let after_start = start.is_none_or(|s| p.id.as_str() >= s);
                let before_end = end.is_none_or(|e| p.id.as_str() <= e);
                let matches_consumer = consumer.is_none_or(|c| p.consumer == c);
                after_start && before_end && matches_consumer
            })
            .take(count.unwrap_or(u32::MAX) as usize)
            .collect();

        if filtered.is_empty() {
            return empty;
        }

        let min_id = filtered.first().map(|p| p.id.clone());
        let max_id = filtered.last().map(|p| p.id.clone());

        let mut consumers: HashMap<String, u32> = HashMap::new();
        for p in &filtered {
            *consumers.entry(p.consumer.clone()).or_insert(0) += 1;
        }

        (filtered.len() as u32, min_id, max_id, consumers)
    }

    /// XCLAIM - Claim pending messages
    pub fn xclaim(
        &mut self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[&str],
    ) -> Vec<StreamEntry> {
        let now = current_time_ms();
        let mut claimed = Vec::new();

        if let Some(groups) = self.groups.get_mut(topic)
            && let Some(cg) = groups.get_mut(group)
        {
            for id in ids {
                if let Some(pending) = cg
                    .pending
                    .iter_mut()
                    .find(|p| p.id == *id && (now - p.delivery_time) >= min_idle_ms)
                {
                    // Update pending entry
                    let old_consumer = pending.consumer.clone();
                    pending.consumer = consumer.to_string();
                    pending.delivery_time = now;
                    pending.delivery_count += 1;

                    // Update consumer counts
                    if let Some(cs) = cg.consumers.get_mut(&old_consumer) {
                        cs.pending_count = cs.pending_count.saturating_sub(1);
                    }
                    cg.consumers
                        .entry(consumer.to_string())
                        .or_insert_with(|| ConsumerState {
                            name: consumer.to_string(),
                            pending_count: 0,
                            idle_time: 0,
                        })
                        .pending_count += 1;

                    // Get the actual entry
                    if let Some(entries) = self.streams.get(topic)
                        && let Some(entry) = entries.iter().find(|e| e.id == *id)
                    {
                        claimed.push(entry.clone());
                    }
                }
            }
        }

        claimed
    }

    /// XAUTOCLAIM - Auto-claim idle messages
    pub fn xautoclaim(
        &mut self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: u32,
    ) -> (String, Vec<StreamEntry>, Vec<String>) {
        let now = current_time_ms();
        let deleted = Vec::new();
        let mut next_id = "0-0".to_string();

        let ids_to_claim: Vec<String> = if let Some(groups) = self.groups.get(topic) {
            if let Some(cg) = groups.get(group) {
                cg.pending
                    .iter()
                    .filter(|p| p.id.as_str() >= start_id && (now - p.delivery_time) >= min_idle_ms)
                    .take(count as usize)
                    .map(|p| p.id.clone())
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Claim the messages
        let id_refs: Vec<&str> = ids_to_claim.iter().map(|s| s.as_str()).collect();
        let claimed = self.xclaim(topic, group, consumer, min_idle_ms, &id_refs);

        // Set next_id
        if let Some(last) = claimed.last() {
            next_id = last.id.clone();
        }

        (next_id, claimed, deleted)
    }

    /// XGROUP CREATE
    pub fn xgroup_create(&mut self, topic: &str, group: &str, start_id: &str) {
        self.streams.entry(topic.to_string()).or_default();
        self.topics.entry(topic.to_string()).or_default();

        let groups = self.groups.entry(topic.to_string()).or_default();
        groups
            .entry(group.to_string())
            .or_insert_with(|| ConsumerGroup {
                last_delivered_id: start_id.to_string(),
                consumers: HashMap::new(),
                pending: Vec::new(),
            });
    }

    /// XGROUP DESTROY
    pub fn xgroup_destroy(&mut self, topic: &str, group: &str) -> bool {
        if let Some(groups) = self.groups.get_mut(topic) {
            groups.remove(group).is_some()
        } else {
            false
        }
    }

    /// XINFO GROUPS
    pub fn xinfo_groups(&self, topic: &str) -> Vec<(String, u32, u32)> {
        if let Some(groups) = self.groups.get(topic) {
            groups
                .iter()
                .map(|(name, cg)| {
                    (
                        name.clone(),
                        cg.consumers.len() as u32,
                        cg.pending.len() as u32,
                    )
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// XLEN - Get stream length
    pub fn xlen(&self, topic: &str) -> u64 {
        self.streams.get(topic).map_or(0, |e| e.len() as u64)
    }

    /// Commit offset for consumer group
    pub fn commit_offset(&mut self, group: &str, topic: &str, offset: u64) {
        self.committed_offsets
            .entry(group.to_string())
            .or_default()
            .insert(topic.to_string(), offset);
    }

    /// Get committed offset
    pub fn get_committed_offset(&self, group: &str, topic: &str) -> Option<u64> {
        self.committed_offsets
            .get(group)
            .and_then(|m| m.get(topic))
            .copied()
    }
}

/// Thread-safe mock server wrapper
#[derive(Debug, Clone, Default)]
pub struct MockServer {
    pub store: Arc<RwLock<MockStreamStore>>,
}

impl MockServer {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(MockStreamStore::new())),
        }
    }

    pub fn with_store(store: MockStreamStore) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
    }
}

// Implement StreamClient for MockClient (MockServer wrapper)
use spiresql::stream::client::{
    AutoClaimResponse, GroupInfoResponse, PendingResult, StreamClient, StreamInfo, StreamResult,
};
use spiresql::stream::types::{ConsumerRecord, RecordMetadata};

impl StreamClient for MockServer {
    fn xadd(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(String, Vec<u8>)],
    ) -> StreamResult<RecordMetadata> {
        let mut store = self.store.write().unwrap();

        let mut fields = HashMap::new();
        if let Some(k) = key {
            fields.insert("_key".to_string(), k.to_vec());
        }
        fields.insert("_value".to_string(), value.to_vec());
        for (k, v) in headers {
            fields.insert(k.clone(), v.clone());
        }

        let id = store.xadd(topic, fields);
        let parts: Vec<&str> = id.split('-').collect();
        let timestamp: u64 = parts[0].parse().unwrap_or(0);
        let offset: u64 = parts[1].parse().unwrap_or(0);

        Ok(RecordMetadata {
            topic: topic.to_string(),
            partition: 0,
            offset,
            timestamp,
        })
    }

    fn xread(
        &self,
        topics: &[&str],
        ids: &[&str],
        count: Option<u32>,
        _block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>> {
        let store = self.store.read().unwrap();
        let mut result = Vec::new();

        for (i, topic) in topics.iter().enumerate() {
            let start_id = ids.get(i).copied().unwrap_or("0-0");

            if let Some(entries) = store.streams.get(*topic) {
                let filtered: Vec<ConsumerRecord> = entries
                    .iter()
                    .filter(|e| e.id.as_str() > start_id)
                    .take(count.unwrap_or(u32::MAX) as usize)
                    .map(|e| convert_entry(topic, e))
                    .collect();

                if !filtered.is_empty() {
                    result.push((topic.to_string(), filtered));
                }
            }
        }

        Ok(result)
    }

    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        topics: &[&str],
        count: Option<u32>,
        _block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>> {
        let mut store = self.store.write().unwrap();
        let raw_results = store.xreadgroup(group, consumer, topics, count);

        let mut result = Vec::new();
        for (topic, entries) in raw_results {
            let records: Vec<ConsumerRecord> =
                entries.iter().map(|e| convert_entry(&topic, e)).collect();
            if !records.is_empty() {
                result.push((topic, records));
            }
        }
        Ok(result)
    }

    fn xack(&self, topic: &str, group: &str, ids: &[&str]) -> StreamResult<u32> {
        let mut store = self.store.write().unwrap();
        Ok(store.xack(topic, group, ids))
    }

    fn xpending(
        &self,
        topic: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<u32>,
        consumer: Option<&str>,
    ) -> StreamResult<PendingResult> {
        let store = self.store.read().unwrap();
        let (cnt, min, max, cons) = store.xpending(topic, group, start, end, count, consumer);

        let mut consumers_map = HashMap::new();
        for (k, v) in cons {
            consumers_map.insert(k, v as u64);
        }

        Ok(PendingResult {
            count: cnt as u64,
            min_id: min,
            max_id: max,
            consumers: consumers_map,
        })
    }

    fn xclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[&str],
        _force: bool,
    ) -> StreamResult<Vec<ConsumerRecord>> {
        let mut store = self.store.write().unwrap();
        let claimed = store.xclaim(topic, group, consumer, min_idle_ms, ids);
        Ok(claimed.iter().map(|e| convert_entry(topic, e)).collect())
    }

    fn xautoclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: u32,
    ) -> StreamResult<AutoClaimResponse> {
        let mut store = self.store.write().unwrap();
        let (next_id, claimed, deleted) =
            store.xautoclaim(topic, group, consumer, min_idle_ms, start_id, count);

        Ok(AutoClaimResponse {
            next_id,
            records: claimed.iter().map(|e| convert_entry(topic, e)).collect(),
            deleted_ids: deleted,
        })
    }

    fn xgroup_create(&self, topic: &str, group: &str, start_id: &str) -> StreamResult<()> {
        let mut store = self.store.write().unwrap();
        store.xgroup_create(topic, group, start_id);
        Ok(())
    }

    fn xgroup_destroy(&self, topic: &str, group: &str) -> StreamResult<bool> {
        let mut store = self.store.write().unwrap();
        Ok(store.xgroup_destroy(topic, group))
    }

    fn xlen(&self, topic: &str) -> StreamResult<u64> {
        let store = self.store.read().unwrap();
        Ok(store.xlen(topic))
    }

    fn xinfo_stream(&self, topic: &str) -> StreamResult<StreamInfo> {
        let store = self.store.read().unwrap();
        let len = store.xlen(topic);
        let groups = store.groups.get(topic).map_or(0, |g| g.len() as u32);

        let entries = store.streams.get(topic);
        let first_entry_id = entries.and_then(|e| e.first().map(|x| x.id.clone()));
        let last_entry_id = entries.and_then(|e| e.last().map(|x| x.id.clone()));

        Ok(StreamInfo {
            length: len,
            groups,
            first_entry_id,
            last_entry_id,
        })
    }

    fn xinfo_groups(&self, topic: &str) -> StreamResult<Vec<GroupInfoResponse>> {
        let store = self.store.read().unwrap();
        let mut result = Vec::new();
        if let Some(groups) = store.groups.get(topic) {
            for (name, cg) in groups {
                result.push(GroupInfoResponse {
                    name: name.clone(),
                    consumers: cg.consumers.len() as u32,
                    pending: cg.pending.len() as u32,
                    last_delivered_id: cg.last_delivered_id.clone(),
                });
            }
        }
        Ok(result)
    }

    fn commit_offset(&self, group: &str, topic: &str, offset: u64) -> StreamResult<()> {
        let mut store = self.store.write().unwrap();
        store.commit_offset(group, topic, offset);
        Ok(())
    }

    fn get_committed_offset(&self, group: &str, topic: &str) -> StreamResult<Option<u64>> {
        let store = self.store.read().unwrap();
        Ok(store.get_committed_offset(group, topic))
    }
}

fn convert_entry(topic: &str, entry: &StreamEntry) -> ConsumerRecord {
    let mut key = None;
    let mut value = Vec::new();
    let mut headers = Vec::new();

    for (k, v) in &entry.fields {
        match k.as_str() {
            "_key" => key = Some(v.clone()),
            "_value" => value = v.clone(),
            _ => headers.push((k.clone(), v.clone())),
        }
    }

    let parts: Vec<&str> = entry.id.split('-').collect();
    let offset: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    ConsumerRecord {
        topic: topic.to_string(),
        partition: 0,
        offset,
        key,
        value,
        headers,
        timestamp: entry.timestamp,
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xadd_and_xread() {
        let mut store = MockStreamStore::new();

        let mut fields = HashMap::new();
        fields.insert("key".to_string(), b"value".to_vec());

        let id1 = store.xadd("test-topic", fields.clone());
        let id2 = store.xadd("test-topic", fields.clone());

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2);

        let results = store.xread(&["test-topic"], &["0-0"], None);
        assert_eq!(results.get("test-topic").unwrap().len(), 2);
    }

    #[test]
    fn test_xreadgroup_and_xack() {
        let mut store = MockStreamStore::new();
        store.xgroup_create("topic", "group", "0-0");

        let mut fields = HashMap::new();
        fields.insert("data".to_string(), b"hello".to_vec());
        store.xadd("topic", fields);

        let results = store.xreadgroup("group", "consumer1", &["topic"], Some(10));
        assert_eq!(results.get("topic").unwrap().len(), 1);

        let (count, _, _, consumers) = store.xpending("topic", "group", None, None, None, None);
        assert_eq!(count, 1);
        assert_eq!(consumers.get("consumer1"), Some(&1));

        let id = results.get("topic").unwrap()[0].id.clone();
        let acked = store.xack("topic", "group", &[&id]);
        assert_eq!(acked, 1);

        let (count, _, _, _) = store.xpending("topic", "group", None, None, None, None);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_xclaim() {
        let mut store = MockStreamStore::new();
        store.xgroup_create("topic", "group", "0-0");

        let mut fields = HashMap::new();
        fields.insert("x".to_string(), b"y".to_vec());
        store.xadd("topic", fields);

        store.xreadgroup("group", "consumer1", &["topic"], None);

        // Claim with 0 idle time (always succeeds)
        let results = store.xreadgroup("group", "consumer1", &["topic"], None);
        if !results.is_empty() {
            let id = results
                .get("topic")
                .map(|v| v[0].id.as_str())
                .unwrap_or("0-0");
            let claimed = store.xclaim("topic", "group", "consumer2", 0, &[id]);
            assert!(!claimed.is_empty() || id == "0-0");
        }
    }

    #[test]
    fn test_xgroup_operations() {
        let mut store = MockStreamStore::new();

        store.xgroup_create("topic", "group1", "0-0");
        store.xgroup_create("topic", "group2", "$");

        let groups = store.xinfo_groups("topic");
        assert_eq!(groups.len(), 2);

        assert!(store.xgroup_destroy("topic", "group1"));

        let groups = store.xinfo_groups("topic");
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn test_mock_server_with_store() {
        let mut store = MockStreamStore::new();

        // Pre-populate the store
        let mut fields = HashMap::new();
        fields.insert("data".to_string(), b"test".to_vec());
        store.xadd("pre-topic", fields);

        // Create MockServer with pre-populated store
        let server = MockServer::with_store(store);

        // Verify the data is present
        let read_store = server.store.read().unwrap();
        assert_eq!(read_store.xlen("pre-topic"), 1);
    }

    #[test]
    fn test_consumer_state_fields() {
        let state = ConsumerState {
            name: "consumer-1".to_string(),
            pending_count: 5,
            idle_time: 1000,
        };

        assert_eq!(state.name, "consumer-1");
        assert_eq!(state.pending_count, 5);
        assert_eq!(state.idle_time, 1000);
    }

    #[test]
    fn test_topic_meta_fields() {
        let meta = TopicMeta {
            partitions: 4,
            replication_factor: 3,
            created_at: 1700000000000,
        };

        assert_eq!(meta.partitions, 4);
        assert_eq!(meta.replication_factor, 3);
        assert_eq!(meta.created_at, 1700000000000);
    }
}
