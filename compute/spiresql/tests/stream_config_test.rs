//! Tests for stream configuration types.
//!
//! Covers ProducerConfig, ConsumerConfig, Compression enum, and defaults.

use spiresql::stream::prelude::*;
use std::time::Duration;

// ============================================================================
// Compression Enum Tests
// ============================================================================

#[test]
fn test_compression_default() {
    let comp: Compression = Default::default();
    assert_eq!(comp, Compression::None);
}

#[test]
fn test_compression_variants() {
    assert_ne!(Compression::None, Compression::Snappy);
    assert_ne!(Compression::Snappy, Compression::Lz4);
    assert_ne!(Compression::Lz4, Compression::Zstd);
}

#[test]
fn test_compression_clone_copy() {
    let comp = Compression::Zstd;
    let cloned = comp;
    let copied = comp;
    assert_eq!(comp, cloned);
    assert_eq!(comp, copied);
}

#[test]
fn test_compression_debug() {
    assert!(format!("{:?}", Compression::None).contains("None"));
    assert!(format!("{:?}", Compression::Snappy).contains("Snappy"));
    assert!(format!("{:?}", Compression::Lz4).contains("Lz4"));
    assert!(format!("{:?}", Compression::Zstd).contains("Zstd"));
}

// ============================================================================
// ProducerConfig Tests
// ============================================================================

#[test]
fn test_producer_config_default() {
    let config = ProducerConfig::default();

    assert_eq!(config.bootstrap_servers, "localhost:6379");
    assert_eq!(config.acks, Acks::All);
    assert!(!config.idempotence);
    assert!(config.transactional_id.is_none());
    assert_eq!(config.linger, Duration::from_millis(5));
    assert_eq!(config.batch_size, 16 * 1024);
    assert_eq!(config.max_in_flight, 5);
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert_eq!(config.compression, Compression::None);
    assert_eq!(config.retries, 3);
    assert_eq!(config.retry_backoff, Duration::from_millis(100));
}

#[test]
fn test_producer_config_custom() {
    let config = ProducerConfig {
        bootstrap_servers: "spiredb:6379".to_string(),
        acks: Acks::Leader,
        idempotence: true,
        transactional_id: Some("tx-producer-1".to_string()),
        linger: Duration::from_millis(10),
        batch_size: 32 * 1024,
        max_in_flight: 1,
        request_timeout: Duration::from_secs(60),
        compression: Compression::Lz4,
        retries: 5,
        retry_backoff: Duration::from_millis(200),
    };

    assert_eq!(config.bootstrap_servers, "spiredb:6379");
    assert_eq!(config.acks, Acks::Leader);
    assert!(config.idempotence);
    assert_eq!(config.transactional_id, Some("tx-producer-1".to_string()));
    assert_eq!(config.compression, Compression::Lz4);
}

#[test]
fn test_producer_config_clone() {
    let config = ProducerConfig::default();
    let cloned = config.clone();

    assert_eq!(config.bootstrap_servers, cloned.bootstrap_servers);
    assert_eq!(config.acks, cloned.acks);
    assert_eq!(config.batch_size, cloned.batch_size);
}

#[test]
fn test_producer_config_debug() {
    let config = ProducerConfig::default();
    let debug = format!("{:?}", config);

    assert!(debug.contains("ProducerConfig"));
    assert!(debug.contains("bootstrap_servers"));
    assert!(debug.contains("acks"));
}

#[test]
fn test_producer_config_idempotence_requires_acks_all() {
    // Idempotence typically requires acks=all and max_in_flight=5 or less
    let config = ProducerConfig {
        idempotence: true,
        acks: Acks::All,
        max_in_flight: 5,
        ..Default::default()
    };

    assert!(config.idempotence);
    assert_eq!(config.acks, Acks::All);
    assert!(config.max_in_flight <= 5);
}

// ============================================================================
// ConsumerConfig Tests
// ============================================================================

#[test]
fn test_consumer_config_default() {
    let config = ConsumerConfig::default();

    assert_eq!(config.bootstrap_servers, "localhost:6379");
    assert!(config.group_id.is_none());
    assert!(config.auto_commit);
    assert_eq!(config.auto_commit_interval, Duration::from_secs(5));
    assert_eq!(
        config.auto_offset_reset,
        OffsetReset::Latest
    );
    assert_eq!(
        config.isolation_level,
        IsolationLevel::ReadUncommitted
    );
    assert_eq!(config.max_poll_records, 500);
    assert_eq!(config.max_poll_interval, Duration::from_secs(300));
    assert_eq!(config.session_timeout, Duration::from_secs(45));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(3));
    assert_eq!(config.fetch_min_bytes, 1);
    assert_eq!(config.fetch_max_bytes, 50 * 1024 * 1024);
    assert_eq!(config.fetch_max_wait, Duration::from_millis(500));
}

#[test]
fn test_consumer_config_with_group() {
    let config = ConsumerConfig {
        group_id: Some("my-consumer-group".to_string()),
        auto_commit: false,
        auto_offset_reset: OffsetReset::Earliest,
        ..Default::default()
    };

    assert_eq!(config.group_id, Some("my-consumer-group".to_string()));
    assert!(!config.auto_commit);
    assert_eq!(
        config.auto_offset_reset,
        OffsetReset::Earliest
    );
}

#[test]
fn test_consumer_config_transactional() {
    let config = ConsumerConfig {
        group_id: Some("txn-consumer".to_string()),
        isolation_level: IsolationLevel::ReadCommitted,
        auto_commit: false,
        ..Default::default()
    };

    assert_eq!(
        config.isolation_level,
        IsolationLevel::ReadCommitted
    );
    assert!(!config.auto_commit);
}

#[test]
fn test_consumer_config_clone() {
    let config = ConsumerConfig {
        group_id: Some("test".to_string()),
        ..Default::default()
    };
    let cloned = config.clone();

    assert_eq!(config.group_id, cloned.group_id);
    assert_eq!(config.max_poll_records, cloned.max_poll_records);
}

#[test]
fn test_consumer_config_debug() {
    let config = ConsumerConfig::default();
    let debug = format!("{:?}", config);

    assert!(debug.contains("ConsumerConfig"));
    assert!(debug.contains("bootstrap_servers"));
    assert!(debug.contains("auto_commit"));
}

// ============================================================================
// Configuration Combinations
// ============================================================================

#[test]
fn test_high_throughput_producer_config() {
    let config = ProducerConfig {
        linger: Duration::from_millis(50),
        batch_size: 1024 * 1024,
        compression: Compression::Lz4,
        acks: Acks::Leader,
        ..Default::default()
    };

    assert!(config.linger > Duration::from_millis(10));
    assert!(config.batch_size >= 1024 * 1024);
    assert_ne!(config.compression, Compression::None);
}

#[test]
fn test_low_latency_producer_config() {
    let config = ProducerConfig {
        linger: Duration::ZERO,
        batch_size: 0,
        acks: Acks::Leader,
        ..Default::default()
    };

    assert_eq!(config.linger, Duration::ZERO);
    assert_eq!(config.batch_size, 0);
}

#[test]
fn test_exactly_once_config() {
    let producer = ProducerConfig {
        idempotence: true,
        transactional_id: Some("eo-producer".to_string()),
        acks: Acks::All,
        max_in_flight: 1,
        ..Default::default()
    };

    let consumer = ConsumerConfig {
        group_id: Some("eo-consumer".to_string()),
        isolation_level: IsolationLevel::ReadCommitted,
        auto_commit: false,
        ..Default::default()
    };

    assert!(producer.idempotence);
    assert!(producer.transactional_id.is_some());
    assert_eq!(
        consumer.isolation_level,
        IsolationLevel::ReadCommitted
    );
    assert!(!consumer.auto_commit);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_producer_config_zero_retries() {
    let config = ProducerConfig {
        retries: 0,
        ..Default::default()
    };
    assert_eq!(config.retries, 0);
}

#[test]
fn test_consumer_config_without_group() {
    // Consumer without group_id operates in standalone mode
    let config = ConsumerConfig {
        group_id: None,
        ..Default::default()
    };
    assert!(config.group_id.is_none());
}

#[test]
fn test_consumer_config_small_poll_records() {
    let config = ConsumerConfig {
        max_poll_records: 1,
        ..Default::default()
    };
    assert_eq!(config.max_poll_records, 1);
}
