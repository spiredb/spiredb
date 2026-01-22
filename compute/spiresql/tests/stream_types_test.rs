//! Tests for stream types module.
//!
//! Covers Record, ConsumerRecord, TopicPartition, OffsetAndMetadata, and all enums.

use spiresql::stream::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Record Tests
// ============================================================================

#[test]
fn test_record_new() {
    let record = Record::new("my-topic");
    assert_eq!(record.topic, "my-topic");
    assert!(record.key.is_none());
    assert!(record.value.is_empty());
    assert!(record.headers.is_empty());
    assert!(record.timestamp.is_none());
}

#[test]
fn test_record_builder_pattern() {
    let record = Record::new("orders")
        .key(b"order-123".to_vec())
        .value(b"payload data".to_vec())
        .header("content-type", b"application/json".to_vec())
        .header("source", b"api".to_vec())
        .timestamp(1234567890);

    assert_eq!(record.topic, "orders");
    assert_eq!(record.key, Some(b"order-123".to_vec()));
    assert_eq!(record.value, b"payload data".to_vec());
    assert_eq!(record.headers.len(), 2);
    assert_eq!(
        record.headers[0],
        ("content-type".to_string(), b"application/json".to_vec())
    );
    assert_eq!(record.headers[1], ("source".to_string(), b"api".to_vec()));
    assert_eq!(record.timestamp, Some(1234567890));
}

#[test]
fn test_record_key_from_string() {
    let record = Record::new("topic").key("string-key".as_bytes().to_vec());
    assert_eq!(record.key, Some(b"string-key".to_vec()));
}

#[test]
fn test_record_empty_value() {
    let record = Record::new("topic").value(Vec::new());
    assert!(record.value.is_empty());
}

#[test]
fn test_record_clone() {
    let record = Record::new("topic")
        .key(b"key".to_vec())
        .value(b"value".to_vec());
    let cloned = record.clone();

    assert_eq!(record.topic, cloned.topic);
    assert_eq!(record.key, cloned.key);
    assert_eq!(record.value, cloned.value);
}

#[test]
fn test_record_debug() {
    let record = Record::new("test");
    let debug = format!("{:?}", record);
    assert!(debug.contains("Record"));
    assert!(debug.contains("test"));
}

// ============================================================================
// ConsumerRecord Tests
// ============================================================================

#[test]
fn test_consumer_record_structure() {
    let record = ConsumerRecord {
        topic: "events".to_string(),
        partition: 3,
        offset: 12345,
        timestamp: 1700000000000,
        key: Some(b"event-key".to_vec()),
        value: b"event-data".to_vec(),
        headers: vec![("source".to_string(), b"producer-1".to_vec())],
    };

    assert_eq!(record.topic, "events");
    assert_eq!(record.partition, 3);
    assert_eq!(record.offset, 12345);
    assert_eq!(record.timestamp, 1700000000000);
    assert_eq!(record.key, Some(b"event-key".to_vec()));
    assert_eq!(record.value, b"event-data".to_vec());
    assert_eq!(record.headers.len(), 1);
}

#[test]
fn test_consumer_record_without_key() {
    let record = ConsumerRecord {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: None,
        value: b"data".to_vec(),
        headers: vec![],
    };

    assert!(record.key.is_none());
}

#[test]
fn test_consumer_record_clone() {
    let record = ConsumerRecord {
        topic: "topic".to_string(),
        partition: 1,
        offset: 100,
        timestamp: 1234,
        key: Some(b"k".to_vec()),
        value: b"v".to_vec(),
        headers: vec![],
    };
    let cloned = record.clone();

    assert_eq!(record.offset, cloned.offset);
    assert_eq!(record.partition, cloned.partition);
}

// ============================================================================
// RecordMetadata Tests
// ============================================================================

#[test]
fn test_record_metadata_structure() {
    let metadata = RecordMetadata {
        topic: "orders".to_string(),
        partition: 5,
        offset: 999999,
        timestamp: 1700000000000,
    };

    assert_eq!(metadata.topic, "orders");
    assert_eq!(metadata.partition, 5);
    assert_eq!(metadata.offset, 999999);
    assert_eq!(metadata.timestamp, 1700000000000);
}

#[test]
fn test_record_metadata_clone() {
    let metadata = RecordMetadata {
        topic: "t".to_string(),
        partition: 0,
        offset: 1,
        timestamp: 2,
    };
    let cloned = metadata.clone();
    assert_eq!(metadata.offset, cloned.offset);
}

// ============================================================================
// TopicPartition Tests
// ============================================================================

#[test]
fn test_topic_partition_new() {
    let tp = TopicPartition::new("my-topic", 7);
    assert_eq!(tp.topic, "my-topic");
    assert_eq!(tp.partition, 7);
}

#[test]
fn test_topic_partition_equality() {
    let tp1 = TopicPartition::new("topic", 0);
    let tp2 = TopicPartition::new("topic", 0);
    let tp3 = TopicPartition::new("topic", 1);
    let tp4 = TopicPartition::new("other", 0);

    assert_eq!(tp1, tp2);
    assert_ne!(tp1, tp3);
    assert_ne!(tp1, tp4);
}

#[test]
fn test_topic_partition_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(TopicPartition::new("topic", 0));
    set.insert(TopicPartition::new("topic", 1));
    set.insert(TopicPartition::new("topic", 0)); // duplicate

    assert_eq!(set.len(), 2);
}

#[test]
fn test_topic_partition_clone() {
    let tp = TopicPartition::new("t", 5);
    let cloned = tp.clone();
    assert_eq!(tp, cloned);
}

// ============================================================================
// OffsetAndMetadata Tests
// ============================================================================

#[test]
fn test_offset_and_metadata_new() {
    let oam = OffsetAndMetadata::new(12345);
    assert_eq!(oam.offset, 12345);
    assert!(oam.metadata.is_none());
}

#[test]
fn test_offset_and_metadata_with_metadata() {
    let oam = OffsetAndMetadata::with_metadata(100, "commit-id-abc");
    assert_eq!(oam.offset, 100);
    assert_eq!(oam.metadata, Some("commit-id-abc".to_string()));
}

#[test]
fn test_offset_and_metadata_clone() {
    let oam = OffsetAndMetadata::with_metadata(50, "meta");
    let cloned = oam.clone();
    assert_eq!(oam.offset, cloned.offset);
    assert_eq!(oam.metadata, cloned.metadata);
}

// ============================================================================
// Acks Enum Tests
// ============================================================================

#[test]
fn test_acks_default() {
    let acks: Acks = Default::default();
    assert_eq!(acks, Acks::All);
}

#[test]
fn test_acks_variants() {
    assert_eq!(Acks::None as i32, 0);
    assert_eq!(Acks::Leader as i32, 1);
    assert_eq!(Acks::All as i32, -1);
}

#[test]
fn test_acks_clone_copy() {
    let acks = Acks::Leader;
    let cloned = acks;
    let copied = acks;
    assert_eq!(acks, cloned);
    assert_eq!(acks, copied);
}

#[test]
fn test_acks_debug() {
    assert!(format!("{:?}", Acks::None).contains("None"));
    assert!(format!("{:?}", Acks::Leader).contains("Leader"));
    assert!(format!("{:?}", Acks::All).contains("All"));
}

// ============================================================================
// OffsetReset Enum Tests
// ============================================================================

#[test]
fn test_offset_reset_default() {
    let reset: OffsetReset = Default::default();
    assert_eq!(reset, OffsetReset::Latest);
}

#[test]
fn test_offset_reset_variants() {
    assert_ne!(OffsetReset::Latest, OffsetReset::Earliest);
    assert_ne!(OffsetReset::Latest, OffsetReset::None);
    assert_ne!(OffsetReset::Earliest, OffsetReset::None);
}

#[test]
fn test_offset_reset_clone_copy() {
    let reset = OffsetReset::Earliest;
    let cloned = reset;
    let copied = reset;
    assert_eq!(reset, cloned);
    assert_eq!(reset, copied);
}

// ============================================================================
// IsolationLevel Enum Tests
// ============================================================================

#[test]
fn test_isolation_level_default() {
    let level: IsolationLevel = Default::default();
    assert_eq!(level, IsolationLevel::ReadUncommitted);
}

#[test]
fn test_isolation_level_variants() {
    assert_ne!(
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted
    );
}

#[test]
fn test_isolation_level_clone_copy() {
    let level = IsolationLevel::ReadCommitted;
    let cloned = level;
    assert_eq!(level, cloned);
}

// ============================================================================
// Op Enum Tests
// ============================================================================

#[test]
fn test_op_variants() {
    assert_ne!(Op::Insert, Op::Update);
    assert_ne!(Op::Update, Op::Delete);
    assert_ne!(Op::Insert, Op::Delete);
}

#[test]
fn test_op_clone_copy() {
    let op = Op::Update;
    let cloned = op;
    let copied = op;
    assert_eq!(op, cloned);
    assert_eq!(op, copied);
}

#[test]
fn test_op_debug() {
    assert!(format!("{:?}", Op::Insert).contains("Insert"));
    assert!(format!("{:?}", Op::Update).contains("Update"));
    assert!(format!("{:?}", Op::Delete).contains("Delete"));
}

// ============================================================================
// ChangeEvent Tests
// ============================================================================

#[test]
fn test_change_event_insert() {
    let event = ChangeEvent {
        table: "users".to_string(),
        op: Op::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
        timestamp: 1700000000000,
        tx_id: Some("tx-123".to_string()),
    };

    assert_eq!(event.table, "users");
    assert_eq!(event.op, Op::Insert);
    assert!(event.before.is_none());
    assert!(event.after.is_some());
    assert_eq!(event.tx_id, Some("tx-123".to_string()));
}

#[test]
fn test_change_event_update() {
    let event = ChangeEvent {
        table: "orders".to_string(),
        op: Op::Update,
        before: Some(serde_json::json!({"status": "pending"})),
        after: Some(serde_json::json!({"status": "completed"})),
        timestamp: 1700000000000,
        tx_id: None,
    };

    assert_eq!(event.op, Op::Update);
    assert!(event.before.is_some());
    assert!(event.after.is_some());
}

#[test]
fn test_change_event_delete() {
    let event = ChangeEvent {
        table: "sessions".to_string(),
        op: Op::Delete,
        before: Some(serde_json::json!({"id": "session-abc"})),
        after: None,
        timestamp: 1700000000000,
        tx_id: None,
    };

    assert_eq!(event.op, Op::Delete);
    assert!(event.before.is_some());
    assert!(event.after.is_none());
}

// ============================================================================
// ConsumerGroupMetadata Tests
// ============================================================================

#[test]
fn test_consumer_group_metadata_structure() {
    let metadata = ConsumerGroupMetadata {
        group_id: "my-group".to_string(),
        generation_id: 5,
        member_id: "member-abc-123".to_string(),
    };

    assert_eq!(metadata.group_id, "my-group");
    assert_eq!(metadata.generation_id, 5);
    assert_eq!(metadata.member_id, "member-abc-123");
}

#[test]
fn test_consumer_group_metadata_clone() {
    let metadata = ConsumerGroupMetadata {
        group_id: "g".to_string(),
        generation_id: 1,
        member_id: "m".to_string(),
    };
    let cloned = metadata.clone();
    assert_eq!(metadata.group_id, cloned.group_id);
}

// ============================================================================
// PendingOptions Tests
// ============================================================================

#[test]
fn test_pending_options_default() {
    let opts: PendingOptions = Default::default();
    assert!(opts.start.is_none());
    assert!(opts.end.is_none());
    assert!(opts.count.is_none());
    assert!(opts.consumer.is_none());
    assert!(opts.min_idle.is_none());
}

#[test]
fn test_pending_options_custom() {
    let opts = PendingOptions {
        start: Some("-".to_string()),
        end: Some("+".to_string()),
        count: Some(100),
        consumer: Some("consumer-1".to_string()),
        min_idle: Some(60000),
    };

    assert_eq!(opts.start, Some("-".to_string()));
    assert_eq!(opts.count, Some(100));
    assert_eq!(opts.min_idle, Some(60000));
}

// ============================================================================
// PendingInfo Tests
// ============================================================================

#[test]
fn test_pending_info_empty() {
    let info = PendingInfo {
        count: 0,
        min_id: None,
        max_id: None,
        consumers: HashMap::new(),
    };

    assert_eq!(info.count, 0);
    assert!(info.min_id.is_none());
    assert!(info.consumers.is_empty());
}

#[test]
fn test_pending_info_with_data() {
    let mut consumers = HashMap::new();
    consumers.insert("consumer-1".to_string(), 10);
    consumers.insert("consumer-2".to_string(), 5);

    let info = PendingInfo {
        count: 15,
        min_id: Some("1700000000000-0".to_string()),
        max_id: Some("1700000001000-0".to_string()),
        consumers,
    };

    assert_eq!(info.count, 15);
    assert_eq!(info.consumers.len(), 2);
    assert_eq!(info.consumers.get("consumer-1"), Some(&10));
}

// ============================================================================
// PendingEntry Tests
// ============================================================================

#[test]
fn test_pending_entry_structure() {
    let entry = PendingEntry {
        id: "1700000000000-0".to_string(),
        consumer: "consumer-1".to_string(),
        idle_ms: 5000,
        delivery_count: 3,
    };

    assert_eq!(entry.id, "1700000000000-0");
    assert_eq!(entry.consumer, "consumer-1");
    assert_eq!(entry.idle_ms, 5000);
    assert_eq!(entry.delivery_count, 3);
}

// ============================================================================
// ClaimOptions Tests
// ============================================================================

#[test]
fn test_claim_options_default() {
    let opts: ClaimOptions = Default::default();
    assert!(opts.idle.is_none());
    assert!(opts.time.is_none());
    assert!(opts.retry_count.is_none());
    assert!(!opts.force);
    assert!(!opts.just_id);
}

#[test]
fn test_claim_options_custom() {
    let opts = ClaimOptions {
        idle: Some(10000),
        time: Some(1700000000000),
        retry_count: Some(5),
        force: true,
        just_id: true,
    };

    assert_eq!(opts.idle, Some(10000));
    assert!(opts.force);
    assert!(opts.just_id);
}

// ============================================================================
// AutoClaimResult Tests
// ============================================================================

#[test]
fn test_auto_claim_result_empty() {
    let result = AutoClaimResult {
        next_id: "0-0".to_string(),
        records: vec![],
        deleted_ids: vec![],
    };

    assert_eq!(result.next_id, "0-0");
    assert!(result.records.is_empty());
    assert!(result.deleted_ids.is_empty());
}

#[test]
fn test_auto_claim_result_with_data() {
    let result = AutoClaimResult {
        next_id: "1700000001000-0".to_string(),
        records: vec![ConsumerRecord {
            topic: "topic".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1700000000000,
            key: None,
            value: b"data".to_vec(),
            headers: vec![],
        }],
        deleted_ids: vec!["1700000000500-0".to_string()],
    };

    assert_eq!(result.records.len(), 1);
    assert_eq!(result.deleted_ids.len(), 1);
}
