//! Tests for stream Consumer API.
//!
//! Covers ConsumerBuilder, subscribe, poll, commit, seek, pause/resume, and PEL operations.

use kovan_map::HashMap;
use spiresql::stream::prelude::*;
use std::sync::Arc;
use std::time::Duration;

mod common;
use common::MockServer;

// ============================================================================
// ConsumerBuilder Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_builder_default() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let config = consumer.config();
    assert_eq!(config.bootstrap_servers, "localhost:6379");
    assert!(config.group_id.is_none());
    assert!(config.auto_commit);
}

#[tokio::test]
async fn test_consumer_builder_custom_servers() {
    let consumer = Consumer::builder()
        .bootstrap_servers("spiredb:6379")
        .build()
        .await
        .unwrap();

    assert_eq!(consumer.config().bootstrap_servers, "spiredb:6379");
}

#[tokio::test]
async fn test_consumer_builder_group_id() {
    let consumer = Consumer::builder()
        .group_id("my-consumer-group")
        .build()
        .await
        .unwrap();

    assert_eq!(
        consumer.config().group_id,
        Some("my-consumer-group".to_string())
    );
}

#[tokio::test]
async fn test_consumer_builder_auto_commit() {
    let consumer = Consumer::builder()
        .auto_commit(false)
        .build()
        .await
        .unwrap();

    assert!(!consumer.config().auto_commit);
}

#[tokio::test]
async fn test_consumer_builder_auto_commit_interval() {
    let consumer = Consumer::builder()
        .auto_commit_interval_ms(1000)
        .build()
        .await
        .unwrap();

    assert_eq!(
        consumer.config().auto_commit_interval,
        Duration::from_millis(1000)
    );
}

#[tokio::test]
async fn test_consumer_builder_auto_offset_reset() {
    let consumer = Consumer::builder()
        .auto_offset_reset(OffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    assert_eq!(consumer.config().auto_offset_reset, OffsetReset::Earliest);
}

#[tokio::test]
async fn test_consumer_builder_isolation_level() {
    let consumer = Consumer::builder()
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .unwrap();

    assert_eq!(
        consumer.config().isolation_level,
        IsolationLevel::ReadCommitted
    );
}

#[tokio::test]
async fn test_consumer_builder_max_poll_records() {
    let consumer = Consumer::builder()
        .max_poll_records(100)
        .build()
        .await
        .unwrap();

    assert_eq!(consumer.config().max_poll_records, 100);
}

#[tokio::test]
async fn test_consumer_builder_max_poll_interval() {
    let consumer = Consumer::builder()
        .max_poll_interval_ms(60000)
        .build()
        .await
        .unwrap();

    assert_eq!(
        consumer.config().max_poll_interval,
        Duration::from_millis(60000)
    );
}

#[tokio::test]
async fn test_consumer_builder_session_timeout() {
    let consumer = Consumer::builder()
        .session_timeout_ms(30000)
        .build()
        .await
        .unwrap();

    assert_eq!(
        consumer.config().session_timeout,
        Duration::from_millis(30000)
    );
}

#[tokio::test]
async fn test_consumer_builder_fetch_bytes() {
    let consumer = Consumer::builder()
        .fetch_min_bytes(1024)
        .fetch_max_bytes(10 * 1024 * 1024)
        .build()
        .await
        .unwrap();

    assert_eq!(consumer.config().fetch_min_bytes, 1024);
    assert_eq!(consumer.config().fetch_max_bytes, 10 * 1024 * 1024);
}

#[tokio::test]
async fn test_consumer_builder_full_config() {
    let consumer = Consumer::builder()
        .bootstrap_servers("spiredb:6379")
        .group_id("test-group")
        .auto_commit(false)
        .auto_commit_interval_ms(5000)
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .max_poll_records(500)
        .max_poll_interval_ms(300000)
        .session_timeout_ms(45000)
        .fetch_min_bytes(1)
        .fetch_max_bytes(50 * 1024 * 1024)
        .build()
        .await
        .unwrap();

    let config = consumer.config();
    assert_eq!(config.group_id, Some("test-group".to_string()));
    assert!(!config.auto_commit);
    assert_eq!(config.auto_offset_reset, OffsetReset::Earliest);
}

// ============================================================================
// Subscribe/Unsubscribe Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_subscribe_single() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["orders"]).await.unwrap();

    let subscription = consumer.subscription();
    assert_eq!(subscription.len(), 1);
    assert!(subscription.contains(&"orders".to_string()));
}

#[tokio::test]
async fn test_consumer_subscribe_multiple() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer
        .subscribe(&["orders", "events", "logs"])
        .await
        .unwrap();

    let subscription = consumer.subscription();
    assert_eq!(subscription.len(), 3);
}

#[tokio::test]
async fn test_consumer_subscribe_replaces_previous() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["topic1"]).await.unwrap();
    consumer.subscribe(&["topic2", "topic3"]).await.unwrap();

    let subscription = consumer.subscription();
    assert_eq!(subscription.len(), 2);
    assert!(!subscription.contains(&"topic1".to_string()));
}

#[tokio::test]
async fn test_consumer_unsubscribe() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();
    assert!(!consumer.subscription().is_empty());

    consumer.unsubscribe().await.unwrap();
    assert!(consumer.subscription().is_empty());
}

#[tokio::test]
async fn test_consumer_unsubscribe_clears_assignment() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();
    consumer.unsubscribe().await.unwrap();

    assert!(consumer.assignment().is_empty());
}

// ============================================================================
// Poll Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_poll_empty() {
    let consumer = Consumer::builder().build().await.unwrap();

    let records = consumer.poll(Duration::from_millis(100)).await.unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_consumer_poll_with_timeout() {
    let consumer = Consumer::builder().build().await.unwrap();

    let start = std::time::Instant::now();
    let _ = consumer.poll(Duration::from_millis(50)).await.unwrap();

    // Should return quickly (stub implementation)
    assert!(start.elapsed() < Duration::from_secs(1));
}

// ============================================================================
// Commit Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_commit_without_group() {
    let consumer = Consumer::builder().build().await.unwrap();

    let result = consumer.commit().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_consumer_commit_with_group() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("commit-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();
    consumer.commit().await.unwrap();
}

#[tokio::test]
async fn test_consumer_commit_offsets_without_group() {
    let consumer = Consumer::builder().build().await.unwrap();

    let offsets = HashMap::new();
    let result = consumer.commit_offsets(offsets).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_consumer_commit_offsets_with_group() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let offsets = HashMap::new();
    offsets.insert(TopicPartition::new("topic", 0), OffsetAndMetadata::new(100));

    consumer.commit_offsets(offsets).await.unwrap();
}

#[test]
fn test_consumer_commit_async() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let consumer = Consumer::builder()
            .group_id("async-group")
            .build()
            .await
            .unwrap();

        // Should not panic
        consumer.commit_async();
    });
}

#[test]
fn test_consumer_commit_async_with_callback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let consumer = Consumer::builder()
            .group_id("callback-group")
            .build()
            .await
            .unwrap();

        let (tx, rx) = std::sync::mpsc::channel();

        let offsets = HashMap::new();
        consumer.commit_async_with_callback(offsets, move |result| {
            tx.send(result.is_ok()).unwrap();
        });

        assert!(rx.recv().unwrap());
    });
}

// ============================================================================
// Seek Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_seek() {
    let consumer = Consumer::builder().build().await.unwrap();

    let tp = TopicPartition::new("topic", 0);
    consumer.seek(tp.clone(), 100);

    assert_eq!(consumer.position(&tp), Some(100));
}

#[tokio::test]
async fn test_consumer_seek_updates_position() {
    let consumer = Consumer::builder().build().await.unwrap();

    let tp = TopicPartition::new("topic", 0);
    consumer.seek(tp.clone(), 50);
    consumer.seek(tp.clone(), 100);

    assert_eq!(consumer.position(&tp), Some(100));
}

#[tokio::test]
async fn test_consumer_seek_to_beginning() {
    let consumer = Consumer::builder().build().await.unwrap();

    let partitions = vec![
        TopicPartition::new("topic", 0),
        TopicPartition::new("topic", 1),
    ];

    consumer.seek_to_beginning(&partitions);

    assert_eq!(consumer.position(&partitions[0]), Some(0));
    assert_eq!(consumer.position(&partitions[1]), Some(0));
}

#[tokio::test]
async fn test_consumer_seek_to_end() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let partitions = vec![TopicPartition::new("topic", 0)];
    consumer.seek_to_end(&partitions);

    // MockServer.xlen returns 0 for empty stream, so position is 0
    assert_eq!(consumer.position(&partitions[0]), Some(0));
}

#[tokio::test]
async fn test_consumer_position_unknown() {
    let consumer = Consumer::builder().build().await.unwrap();

    let tp = TopicPartition::new("unknown", 0);
    assert!(consumer.position(&tp).is_none());
}

// ============================================================================
// Committed Offsets Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_committed_empty() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("test-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let partitions = vec![TopicPartition::new("topic", 0)];
    let committed = consumer.committed(&partitions).await.unwrap();

    assert!(committed.is_empty());
}

// ============================================================================
// Pause/Resume Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_pause() {
    let consumer = Consumer::builder().build().await.unwrap();

    let partitions = vec![
        TopicPartition::new("topic", 0),
        TopicPartition::new("topic", 1),
    ];

    consumer.pause(&partitions);

    let paused = consumer.paused();
    assert_eq!(paused.len(), 2);
}

#[tokio::test]
async fn test_consumer_pause_idempotent() {
    let consumer = Consumer::builder().build().await.unwrap();

    let tp = TopicPartition::new("topic", 0);
    consumer.pause(std::slice::from_ref(&tp));
    consumer.pause(std::slice::from_ref(&tp));

    assert_eq!(consumer.paused().len(), 1);
}

#[tokio::test]
async fn test_consumer_resume() {
    let consumer = Consumer::builder().build().await.unwrap();

    let partitions = vec![
        TopicPartition::new("topic", 0),
        TopicPartition::new("topic", 1),
    ];

    consumer.pause(&partitions);
    consumer.resume(&partitions[..1]);

    let paused = consumer.paused();
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0], partitions[1]);
}

#[tokio::test]
async fn test_consumer_resume_all() {
    let consumer = Consumer::builder().build().await.unwrap();

    let partitions = vec![TopicPartition::new("topic", 0)];

    consumer.pause(&partitions);
    consumer.resume(&partitions);

    assert!(consumer.paused().is_empty());
}

// ============================================================================
// Assignment and Subscription Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_assignment_initially_empty() {
    let consumer = Consumer::builder().build().await.unwrap();
    assert!(consumer.assignment().is_empty());
}

#[tokio::test]
async fn test_consumer_subscription_initially_empty() {
    let consumer = Consumer::builder().build().await.unwrap();
    assert!(consumer.subscription().is_empty());
}

// ============================================================================
// Offsets for Commit Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_offsets_for_commit_empty() {
    let consumer = Consumer::builder().build().await.unwrap();

    let offsets = consumer.offsets_for_commit();
    assert!(offsets.is_empty());
}

#[tokio::test]
async fn test_consumer_offsets_for_commit_with_positions() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.seek(TopicPartition::new("topic", 0), 100);
    consumer.seek(TopicPartition::new("topic", 1), 200);

    let offsets = consumer.offsets_for_commit();
    assert_eq!(offsets.len(), 2);
}

// ============================================================================
// Group Metadata Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_group_metadata_without_group() {
    let consumer = Consumer::builder().build().await.unwrap();

    let metadata = consumer.group_metadata();
    assert!(metadata.group_id.is_empty());
}

#[tokio::test]
async fn test_consumer_group_metadata_with_group() {
    let consumer = Consumer::builder()
        .group_id("test-group")
        .build()
        .await
        .unwrap();

    let metadata = consumer.group_metadata();
    assert_eq!(metadata.group_id, "test-group");
}

// ============================================================================
// PEL Operations Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_pending() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("pel-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let info = consumer
        .pending("topic", PendingOptions::default())
        .await
        .unwrap();

    assert_eq!(info.count, 0);
    assert!(info.min_id.is_none());
    assert!(info.max_id.is_none());
}

#[tokio::test]
async fn test_consumer_pending_with_options() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("pel-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let opts = PendingOptions {
        start: Some("-".to_string()),
        end: Some("+".to_string()),
        count: Some(100),
        consumer: Some("consumer-1".to_string()),
        min_idle: Some(60000),
    };

    let info = consumer.pending("topic", opts).await.unwrap();
    assert_eq!(info.count, 0);
}

#[tokio::test]
async fn test_consumer_claim() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("claim-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let records = consumer
        .claim(
            "topic",
            Duration::from_secs(60),
            &["1-0", "2-0"],
            ClaimOptions::default(),
        )
        .await
        .unwrap();

    assert!(records.is_empty());
}

#[tokio::test]
async fn test_consumer_claim_with_options() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("claim-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let opts = ClaimOptions {
        idle: Some(1000),
        force: true,
        just_id: false,
        time: None,
        retry_count: None,
    };

    let records = consumer
        .claim("topic", Duration::from_secs(60), &["1-0"], opts)
        .await
        .unwrap();

    assert!(records.is_empty());
}

#[tokio::test]
async fn test_consumer_auto_claim() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("autoclaim-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let result = consumer
        .auto_claim("topic", Duration::from_secs(60), "0-0", 10)
        .await
        .unwrap();

    assert_eq!(result.next_id, "0-0");
    assert!(result.records.is_empty());
    assert!(result.deleted_ids.is_empty());
}

// ============================================================================
// Close Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_close() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();
    consumer.close().await.unwrap();

    assert!(consumer.subscription().is_empty());
}

#[tokio::test]
async fn test_consumer_close_already_unsubscribed() {
    let consumer = Consumer::builder().build().await.unwrap();
    consumer.close().await.unwrap();
}

// ============================================================================
// CDC Builder Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_cdc_builder() {
    let consumer = Consumer::builder().build().await.unwrap();

    let cdc = consumer.cdc();

    // Can build from consumer
    let stream = cdc.tables(&["users"]).build().await.unwrap();
    assert_eq!(stream.tables(), &["users".to_string()]);
}

// ============================================================================
// Interceptor Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_builder_with_interceptor() {
    use std::sync::Arc;

    let metrics = Arc::new(MetricsInterceptor::new());
    let consumer = Consumer::builder()
        .interceptor(metrics.clone())
        .build()
        .await
        .unwrap();

    // Subscribe and poll to trigger interceptor
    consumer.subscribe(&["topic"]).await.unwrap();
    let _ = consumer.poll(std::time::Duration::from_millis(10)).await;

    // Interceptor should have been registered (even if no records)
    assert_eq!(metrics.records_received(), 0);
}

#[tokio::test]
async fn test_consumer_builder_multiple_interceptors() {
    use std::sync::Arc;

    let metrics = Arc::new(MetricsInterceptor::new());
    let logging = Arc::new(LoggingInterceptor::new());

    let consumer = Consumer::builder()
        .interceptor(metrics)
        .interceptor(logging)
        .build()
        .await
        .unwrap();

    assert!(consumer.config().bootstrap_servers.contains("localhost"));
}

// ============================================================================
// ACK Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_ack_without_group() {
    let consumer = Consumer::builder().build().await.unwrap();

    let result = consumer.ack("topic", &["1-0", "2-0"]).await;
    assert!(result.is_err()); // Requires group_id
}

#[tokio::test]
async fn test_consumer_ack_with_group() {
    let mock = Arc::new(MockServer::new());
    let consumer = Consumer::builder()
        .group_id("ack-group")
        .client(mock as Arc<dyn StreamClient>)
        .build()
        .await
        .unwrap();

    let acked = consumer.ack("topic", &["1-0", "2-0"]).await.unwrap();
    assert_eq!(acked, 0); // No pending messages in mock
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_consumer_records_received() {
    let consumer = Consumer::builder().build().await.unwrap();

    assert_eq!(consumer.records_received(), 0);
    assert_eq!(consumer.bytes_received(), 0);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_consumer_empty_subscribe() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&[]).await.unwrap();
    assert!(consumer.subscription().is_empty());
}

#[tokio::test]
async fn test_consumer_unicode_topic() {
    let consumer = Consumer::builder().build().await.unwrap();

    consumer.subscribe(&["日本語トピック"]).await.unwrap();
    assert!(
        consumer
            .subscription()
            .contains(&"日本語トピック".to_string())
    );
}
