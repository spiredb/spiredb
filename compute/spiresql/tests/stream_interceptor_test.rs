//! Tests for stream interceptors.
//!
//! Covers ProducerInterceptor, ConsumerInterceptor, MetricsInterceptor, and LoggingInterceptor.

use spiresql::stream::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

// ============================================================================
// MetricsInterceptor Tests
// ============================================================================

#[test]
fn test_metrics_interceptor_new() {
    let metrics = MetricsInterceptor::new();

    assert_eq!(metrics.records_sent(), 0);
    assert_eq!(metrics.records_received(), 0);
    assert_eq!(metrics.bytes_sent(), 0);
    assert_eq!(metrics.bytes_received(), 0);
    assert_eq!(metrics.errors(), 0);
}

#[test]
fn test_metrics_interceptor_default() {
    let metrics: MetricsInterceptor = Default::default();

    assert_eq!(metrics.records_sent(), 0);
    assert_eq!(metrics.errors(), 0);
}

#[test]
fn test_metrics_interceptor_on_send() {
    let metrics = MetricsInterceptor::new();

    let record = Record::new("topic").value(b"hello".to_vec());
    let returned = metrics.on_send(record.clone());

    assert_eq!(metrics.records_sent(), 1);
    assert_eq!(metrics.bytes_sent(), 5);
    assert_eq!(returned.topic, "topic");
}

#[test]
fn test_metrics_interceptor_multiple_sends() {
    let metrics = MetricsInterceptor::new();

    for i in 0..10 {
        let record = Record::new("topic").value(format!("msg-{}", i).into_bytes());
        metrics.on_send(record);
    }

    assert_eq!(metrics.records_sent(), 10);
    assert!(metrics.bytes_sent() > 0);
}

#[test]
fn test_metrics_interceptor_on_acknowledgement_success() {
    let metrics = MetricsInterceptor::new();

    let metadata = RecordMetadata {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
    };

    metrics.on_acknowledgement(&metadata, None);

    assert_eq!(metrics.errors(), 0);
}

#[test]
fn test_metrics_interceptor_on_acknowledgement_error() {
    let metrics = MetricsInterceptor::new();

    let metadata = RecordMetadata {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
    };

    let error = StreamError::Connection("failed".to_string());
    metrics.on_acknowledgement(&metadata, Some(&error));

    assert_eq!(metrics.errors(), 1);
}

#[test]
fn test_metrics_interceptor_on_consume() {
    let metrics = MetricsInterceptor::new();

    let records = vec![
        ConsumerRecord {
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            key: None,
            value: b"data1".to_vec(),
            headers: vec![],
        },
        ConsumerRecord {
            topic: "topic".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            key: None,
            value: b"data2".to_vec(),
            headers: vec![],
        },
    ];

    let returned = metrics.on_consume(records);

    assert_eq!(metrics.records_received(), 2);
    assert_eq!(metrics.bytes_received(), 10);
    assert_eq!(returned.len(), 2);
}

#[test]
fn test_metrics_interceptor_empty_consume() {
    let metrics = MetricsInterceptor::new();

    let records: Vec<ConsumerRecord> = vec![];
    let returned = metrics.on_consume(records);

    assert_eq!(metrics.records_received(), 0);
    assert_eq!(metrics.bytes_received(), 0);
    assert!(returned.is_empty());
}

#[test]
fn test_metrics_interceptor_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let metrics = Arc::new(MetricsInterceptor::new());
    let mut handles = vec![];

    for _ in 0..10 {
        let m = Arc::clone(&metrics);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let record = Record::new("topic").value(b"x".to_vec());
                m.on_send(record);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(metrics.records_sent(), 1000);
    assert_eq!(metrics.bytes_sent(), 1000);
}

#[test]
fn test_metrics_interceptor_debug() {
    let metrics = MetricsInterceptor::new();
    let debug = format!("{:?}", metrics);

    assert!(debug.contains("MetricsInterceptor"));
    assert!(debug.contains("records_sent"));
}

// ============================================================================
// LoggingInterceptor Tests
// ============================================================================

#[test]
fn test_logging_interceptor_new() {
    let logger = LoggingInterceptor::new();
    assert_eq!(logger.level, LogLevel::Debug);
}

#[test]
fn test_logging_interceptor_default() {
    let logger: LoggingInterceptor = Default::default();
    assert_eq!(logger.level, LogLevel::Debug);
}

#[test]
fn test_logging_interceptor_with_level() {
    let logger = LoggingInterceptor::with_level(LogLevel::Trace);
    assert_eq!(logger.level, LogLevel::Trace);

    let logger = LoggingInterceptor::with_level(LogLevel::Info);
    assert_eq!(logger.level, LogLevel::Info);
}

#[test]
fn test_logging_interceptor_on_send() {
    let logger = LoggingInterceptor::new();

    let record = Record::new("test-topic")
        .key(b"key".to_vec())
        .value(b"value".to_vec())
        .header("h1", b"v1".to_vec());

    // Should not panic at any log level
    let returned = logger.on_send(record);
    assert_eq!(returned.topic, "test-topic");
}

#[test]
fn test_logging_interceptor_on_send_all_levels() {
    for level in [LogLevel::Trace, LogLevel::Debug, LogLevel::Info] {
        let logger = LoggingInterceptor::with_level(level);
        let record = Record::new("topic").value(b"data".to_vec());

        let returned = logger.on_send(record);
        assert_eq!(returned.topic, "topic");
    }
}

#[test]
fn test_logging_interceptor_on_acknowledgement_success() {
    let logger = LoggingInterceptor::new();

    let metadata = RecordMetadata {
        topic: "topic".to_string(),
        partition: 0,
        offset: 100,
        timestamp: 1234,
    };

    // Should not panic
    logger.on_acknowledgement(&metadata, None);
}

#[test]
fn test_logging_interceptor_on_acknowledgement_error() {
    let logger = LoggingInterceptor::new();

    let metadata = RecordMetadata {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
    };

    let error = StreamError::Timeout("timed out".to_string());

    // Should not panic
    logger.on_acknowledgement(&metadata, Some(&error));
}

#[test]
fn test_logging_interceptor_on_consume_empty() {
    let logger = LoggingInterceptor::new();

    let records: Vec<ConsumerRecord> = vec![];
    let returned = logger.on_consume(records);

    assert!(returned.is_empty());
}

#[test]
fn test_logging_interceptor_on_consume_with_records() {
    let logger = LoggingInterceptor::new();

    let records = vec![ConsumerRecord {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: Some(b"key".to_vec()),
        value: b"value".to_vec(),
        headers: vec![],
    }];

    let returned = logger.on_consume(records);
    assert_eq!(returned.len(), 1);
}

#[test]
fn test_logging_interceptor_on_consume_all_levels() {
    for level in [LogLevel::Trace, LogLevel::Debug, LogLevel::Info] {
        let logger = LoggingInterceptor::with_level(level);

        let records = vec![ConsumerRecord {
            topic: "topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            key: None,
            value: b"data".to_vec(),
            headers: vec![],
        }];

        let returned = logger.on_consume(records);
        assert_eq!(returned.len(), 1);
    }
}

#[test]
fn test_logging_interceptor_clone() {
    let logger = LoggingInterceptor::with_level(LogLevel::Trace);
    let cloned = logger.clone();

    assert_eq!(logger.level, cloned.level);
}

#[test]
fn test_logging_interceptor_debug() {
    let logger = LoggingInterceptor::new();
    let debug = format!("{:?}", logger);

    assert!(debug.contains("LoggingInterceptor"));
    assert!(debug.contains("level"));
}

// ============================================================================
// LogLevel Tests
// ============================================================================

#[test]
fn test_log_level_default() {
    let level: LogLevel = Default::default();
    assert_eq!(level, LogLevel::Debug);
}

#[test]
fn test_log_level_equality() {
    assert_eq!(LogLevel::Trace, LogLevel::Trace);
    assert_eq!(LogLevel::Debug, LogLevel::Debug);
    assert_eq!(LogLevel::Info, LogLevel::Info);

    assert_ne!(LogLevel::Trace, LogLevel::Debug);
    assert_ne!(LogLevel::Debug, LogLevel::Info);
}

#[test]
fn test_log_level_clone_copy() {
    let level = LogLevel::Trace;
    let cloned = level;
    let copied = level;

    assert_eq!(level, cloned);
    assert_eq!(level, copied);
}

#[test]
fn test_log_level_debug() {
    assert!(format!("{:?}", LogLevel::Trace).contains("Trace"));
    assert!(format!("{:?}", LogLevel::Debug).contains("Debug"));
    assert!(format!("{:?}", LogLevel::Info).contains("Info"));
}

// ============================================================================
// Custom Interceptor Tests
// ============================================================================

struct RecordCounterInterceptor {
    count: std::sync::atomic::AtomicU32,
}

impl RecordCounterInterceptor {
    fn new() -> Self {
        Self {
            count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }
}

impl ProducerInterceptor for RecordCounterInterceptor {
    fn on_send(&self, record: Record) -> Record {
        self.count.fetch_add(1, Ordering::Relaxed);
        record
    }
}

#[test]
fn test_custom_producer_interceptor() {
    let counter = RecordCounterInterceptor::new();

    for _ in 0..5 {
        let record = Record::new("topic").value(b"data".to_vec());
        counter.on_send(record);
    }

    assert_eq!(counter.count(), 5);
}

struct RecordFilterInterceptor {
    filtered_topic: String,
}

impl ConsumerInterceptor for RecordFilterInterceptor {
    fn on_consume(&self, records: Vec<ConsumerRecord>) -> Vec<ConsumerRecord> {
        records
            .into_iter()
            .filter(|r| r.topic != self.filtered_topic)
            .collect()
    }
}

#[test]
fn test_custom_consumer_interceptor_filter() {
    let filter = RecordFilterInterceptor {
        filtered_topic: "skip-me".to_string(),
    };

    let records = vec![
        ConsumerRecord {
            topic: "keep".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            key: None,
            value: vec![],
            headers: vec![],
        },
        ConsumerRecord {
            topic: "skip-me".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            key: None,
            value: vec![],
            headers: vec![],
        },
    ];

    let filtered = filter.on_consume(records);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].topic, "keep");
}

// ============================================================================
// Trait Default Implementation Tests
// ============================================================================

struct NoOpProducerInterceptor;

impl ProducerInterceptor for NoOpProducerInterceptor {}

#[test]
fn test_producer_interceptor_default_on_send() {
    let interceptor = NoOpProducerInterceptor;

    let record = Record::new("topic").value(b"data".to_vec());
    let returned = interceptor.on_send(record.clone());

    assert_eq!(returned.topic, record.topic);
    assert_eq!(returned.value, record.value);
}

#[test]
fn test_producer_interceptor_default_on_acknowledgement() {
    let interceptor = NoOpProducerInterceptor;

    let metadata = RecordMetadata {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
    };

    // Should not panic
    interceptor.on_acknowledgement(&metadata, None);
}

struct NoOpConsumerInterceptor;

impl ConsumerInterceptor for NoOpConsumerInterceptor {}

#[test]
fn test_consumer_interceptor_default_on_consume() {
    let interceptor = NoOpConsumerInterceptor;

    let records = vec![ConsumerRecord {
        topic: "topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: None,
        value: b"data".to_vec(),
        headers: vec![],
    }];

    let returned = interceptor.on_consume(records.clone());
    assert_eq!(returned.len(), 1);
}

#[test]
fn test_consumer_interceptor_default_on_commit() {
    let interceptor = NoOpConsumerInterceptor;

    let mut offsets = HashMap::new();
    offsets.insert(TopicPartition::new("topic", 0), OffsetAndMetadata::new(100));

    // Should not panic
    interceptor.on_commit(&offsets);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_interceptor_large_value() {
    let metrics = MetricsInterceptor::new();

    let large_value = vec![b'x'; 10 * 1024 * 1024]; // 10MB
    let record = Record::new("topic").value(large_value.clone());

    metrics.on_send(record);

    assert_eq!(metrics.bytes_sent(), large_value.len() as u64);
}

#[test]
fn test_interceptor_empty_value() {
    let metrics = MetricsInterceptor::new();

    let record = Record::new("topic").value(Vec::new());
    metrics.on_send(record);

    assert_eq!(metrics.records_sent(), 1);
    assert_eq!(metrics.bytes_sent(), 0);
}
