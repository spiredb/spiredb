//! Tests for stream Producer API.
//!
//! Covers ProducerBuilder, send operations, transactions, and configuration.

use spiresql::stream::prelude::*;
use std::time::Duration;

// ============================================================================
// ProducerBuilder Tests
// ============================================================================

#[tokio::test]
async fn test_producer_builder_default() {
    let producer = Producer::builder().build().await.unwrap();

    let config = producer.config();
    assert_eq!(config.bootstrap_servers, "localhost:6379");
    assert_eq!(config.acks, Acks::All);
}

#[tokio::test]
async fn test_producer_builder_custom_servers() {
    let producer = Producer::builder()
        .bootstrap_servers("spiredb-1:6379,spiredb-2:6379")
        .build()
        .await
        .unwrap();

    assert_eq!(
        producer.config().bootstrap_servers,
        "spiredb-1:6379,spiredb-2:6379"
    );
}

#[tokio::test]
async fn test_producer_builder_acks() {
    let producer = Producer::builder()
        .acks(Acks::Leader)
        .build()
        .await
        .unwrap();

    assert_eq!(producer.config().acks, Acks::Leader);
}

#[tokio::test]
async fn test_producer_builder_idempotence() {
    let producer = Producer::builder().idempotence(true).build().await.unwrap();

    assert!(producer.config().idempotence);
}

#[tokio::test]
async fn test_producer_builder_transactional_id() {
    let producer = Producer::builder()
        .transactional_id("tx-producer-1")
        .build()
        .await
        .unwrap();

    assert_eq!(
        producer.config().transactional_id,
        Some("tx-producer-1".to_string())
    );
}

#[tokio::test]
async fn test_producer_builder_linger_ms() {
    let producer = Producer::builder().linger_ms(50).build().await.unwrap();

    assert_eq!(producer.config().linger, Duration::from_millis(50));
}

#[tokio::test]
async fn test_producer_builder_batch_size() {
    let producer = Producer::builder()
        .batch_size(1024 * 1024)
        .build()
        .await
        .unwrap();

    assert_eq!(producer.config().batch_size, 1024 * 1024);
}

#[tokio::test]
async fn test_producer_builder_max_in_flight() {
    let producer = Producer::builder().max_in_flight(1).build().await.unwrap();

    assert_eq!(producer.config().max_in_flight, 1);
}

#[tokio::test]
async fn test_producer_builder_request_timeout() {
    let producer = Producer::builder()
        .request_timeout_ms(60000)
        .build()
        .await
        .unwrap();

    assert_eq!(
        producer.config().request_timeout,
        Duration::from_millis(60000)
    );
}

#[tokio::test]
async fn test_producer_builder_compression() {
    let producer = Producer::builder()
        .compression(Compression::Lz4)
        .build()
        .await
        .unwrap();

    assert_eq!(producer.config().compression, Compression::Lz4);
}

#[tokio::test]
async fn test_producer_builder_retries() {
    let producer = Producer::builder().retries(10).build().await.unwrap();

    assert_eq!(producer.config().retries, 10);
}

#[tokio::test]
async fn test_producer_builder_full_config() {
    let producer = Producer::builder()
        .bootstrap_servers("spiredb:6379")
        .acks(Acks::All)
        .idempotence(true)
        .transactional_id("my-tx")
        .linger_ms(10)
        .batch_size(32768)
        .max_in_flight(5)
        .request_timeout_ms(30000)
        .compression(Compression::Snappy)
        .retries(3)
        .build()
        .await
        .unwrap();

    let config = producer.config();
    assert_eq!(config.bootstrap_servers, "spiredb:6379");
    assert!(config.idempotence);
    assert_eq!(config.compression, Compression::Snappy);
}

// ============================================================================
// Send Operations Tests
// ============================================================================

#[tokio::test]
async fn test_producer_send_simple() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("test-topic").value(b"hello".to_vec());
    let metadata = producer.send(record).await.unwrap();

    assert_eq!(metadata.topic, "test-topic");
    assert_eq!(metadata.partition, 0);
}

#[tokio::test]
async fn test_producer_send_with_key() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("orders")
        .key(b"order-123".to_vec())
        .value(b"order data".to_vec());

    let metadata = producer.send(record).await.unwrap();
    assert_eq!(metadata.topic, "orders");
}

#[tokio::test]
async fn test_producer_send_with_headers() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("events")
        .header("content-type", b"application/json".to_vec())
        .header("trace-id", b"abc-123".to_vec())
        .value(b"{}".to_vec());

    let metadata = producer.send(record).await.unwrap();
    assert_eq!(metadata.topic, "events");
}

#[tokio::test]
async fn test_producer_send_with_timestamp() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("metrics")
        .timestamp(1700000000000)
        .value(b"metric".to_vec());

    let metadata = producer.send(record).await.unwrap();
    assert_eq!(metadata.timestamp, 1700000000000);
}

#[tokio::test]
async fn test_producer_send_empty_value() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("topic").value(Vec::new());
    let metadata = producer.send(record).await.unwrap();
    assert_eq!(metadata.topic, "topic");
}

#[tokio::test]
async fn test_producer_send_large_value() {
    let producer = Producer::builder().build().await.unwrap();

    let large_data = vec![b'x'; 1024 * 1024]; // 1MB
    let record = Record::new("large").value(large_data);

    let metadata = producer.send(record).await.unwrap();
    assert_eq!(metadata.topic, "large");
}

#[test]
fn test_producer_send_with_callback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let producer = Producer::builder().build().await.unwrap();

        let (tx, rx) = std::sync::mpsc::channel();

        let record = Record::new("callback-topic").value(b"data".to_vec());
        producer.send_with_callback(record, move |result| {
            tx.send(result.is_ok()).unwrap();
        });

        assert!(rx.recv().unwrap());
    });
}

#[test]
fn test_producer_send_sync() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let producer = Producer::builder().build().await.unwrap();

        let record = Record::new("sync-topic").value(b"sync data".to_vec());
        let metadata = producer.send_sync(record).unwrap();

        assert_eq!(metadata.topic, "sync-topic");
    });
}

// ============================================================================
// Flush Tests
// ============================================================================

#[tokio::test]
async fn test_producer_flush() {
    let producer = Producer::builder().build().await.unwrap();

    // Send multiple records
    for i in 0..10 {
        let record = Record::new("flush-topic").value(format!("msg-{}", i).into_bytes());
        let _ = producer.send(record).await;
    }

    // Flush should complete successfully
    producer.flush().await.unwrap();
}

#[tokio::test]
async fn test_producer_flush_empty() {
    let producer = Producer::builder().build().await.unwrap();

    // Flush with nothing pending
    producer.flush().await.unwrap();
}

// ============================================================================
// Transaction API Tests
// ============================================================================

#[tokio::test]
async fn test_producer_init_transactions_without_id() {
    let producer = Producer::builder().build().await.unwrap();

    let result = producer.init_transactions().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_producer_init_transactions_with_id() {
    let producer = Producer::builder()
        .transactional_id("test-tx")
        .build()
        .await
        .unwrap();

    producer.init_transactions().await.unwrap();
}

#[test]
fn test_producer_begin_transaction_without_id() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let producer = Producer::builder().build().await.unwrap();

        let result = producer.begin_transaction();
        assert!(result.is_err());
    });
}

#[test]
fn test_producer_begin_transaction_with_id() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let producer = Producer::builder()
            .transactional_id("tx-1")
            .build()
            .await
            .unwrap();

        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
    });
}

#[tokio::test]
async fn test_producer_commit_transaction() {
    let producer = Producer::builder()
        .transactional_id("tx-commit")
        .build()
        .await
        .unwrap();

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    producer.commit_transaction().await.unwrap();
}

#[tokio::test]
async fn test_producer_abort_transaction() {
    let producer = Producer::builder()
        .transactional_id("tx-abort")
        .build()
        .await
        .unwrap();

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();
    producer.abort_transaction().await.unwrap();
}

#[tokio::test]
async fn test_producer_send_offsets_to_transaction() {
    use kovan_map::HashMap;
    

    let producer = Producer::builder()
        .transactional_id("tx-offsets")
        .build()
        .await
        .unwrap();

    producer.init_transactions().await.unwrap();
    producer.begin_transaction().unwrap();

    let offsets = HashMap::new();
    offsets.insert(TopicPartition::new("topic", 0), OffsetAndMetadata::new(100));

    let metadata = ConsumerGroupMetadata {
        group_id: "group".to_string(),
        generation_id: 1,
        member_id: "member".to_string(),
    };

    producer
        .send_offsets_to_transaction(offsets, metadata)
        .await
        .unwrap();
}

// ============================================================================
// Close Tests
// ============================================================================

#[tokio::test]
async fn test_producer_close() {
    let producer = Producer::builder().build().await.unwrap();

    // Send some records
    let record = Record::new("topic").value(b"data".to_vec());
    let _ = producer.send(record).await;

    // Close should flush and complete successfully
    producer.close().await.unwrap();
}

#[tokio::test]
async fn test_producer_close_empty() {
    let producer = Producer::builder().build().await.unwrap();
    producer.close().await.unwrap();
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_producer_unicode_topic() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("日本語トピック").value(b"data".to_vec());
    let metadata = producer.send(record).await.unwrap();

    assert_eq!(metadata.topic, "日本語トピック");
}

#[tokio::test]
async fn test_producer_special_chars_in_key() {
    let producer = Producer::builder().build().await.unwrap();

    let record = Record::new("topic")
        .key(b"key/with:special$chars".to_vec())
        .value(b"data".to_vec());

    let _ = producer.send(record).await.unwrap();
}

#[tokio::test]
async fn test_producer_binary_data() {
    let producer = Producer::builder().build().await.unwrap();

    let binary_data: Vec<u8> = (0..=255).collect();
    let record = Record::new("binary").value(binary_data);

    let _ = producer.send(record).await.unwrap();
}
