//! Tests for stream CDC module.
//!
//! Covers CdcBuilder, CdcStream, and change event processing.

use spiresql::stream::prelude::*;
use std::time::Duration;

// ============================================================================
// CdcBuilder Tests
// ============================================================================

#[tokio::test]
async fn test_cdc_builder_new() {
    let cdc = CdcBuilder::new("localhost:6379");

    // Should error without tables
    let result = cdc.build().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cdc_builder_with_tables() {
    let cdc = CdcBuilder::new("localhost:6379").tables(&["users", "orders"]);

    let stream = cdc.build().await.unwrap();
    assert_eq!(stream.tables().len(), 2);
    assert!(stream.tables().contains(&"users".to_string()));
    assert!(stream.tables().contains(&"orders".to_string()));
}

#[tokio::test]
async fn test_cdc_builder_with_operations() {
    let cdc = CdcBuilder::new("localhost:6379")
        .tables(&["test"])
        .operations(&[Op::Insert, Op::Update]);

    let stream = cdc.build().await.unwrap();
    let ops = stream.operations();
    assert_eq!(ops.len(), 2);
    assert!(ops.contains(&Op::Insert));
    assert!(ops.contains(&Op::Update));
}

#[tokio::test]
async fn test_cdc_builder_from_now() {
    let cdc = CdcBuilder::new("localhost:6379")
        .tables(&["test"])
        .current();

    let stream = cdc.build().await.unwrap();
    assert_eq!(stream.tables().len(), 1);
}

#[tokio::test]
async fn test_cdc_builder_from_beginning() {
    let cdc = CdcBuilder::new("localhost:6379")
        .tables(&["test"])
        .beginning();

    let stream = cdc.build().await.unwrap();
    assert_eq!(stream.tables().len(), 1);
}

#[tokio::test]
async fn test_cdc_builder_from_consumer() {
    let consumer = Consumer::builder()
        .group_id("cdc-group")
        .build()
        .await
        .unwrap();

    let cdc = CdcBuilder::from_consumer(&consumer).tables(&["events"]);

    let stream = cdc.build().await.unwrap();
    assert!(stream.tables().contains(&"events".to_string()));
}

#[tokio::test]
async fn test_cdc_builder_full_config() {
    let cdc = CdcBuilder::new("localhost:6379")
        .tables(&["users", "orders", "products"])
        .operations(&[Op::Insert, Op::Update, Op::Delete])
        .beginning();

    let stream = cdc.build().await.unwrap();
    assert_eq!(stream.tables().len(), 3);
    assert_eq!(stream.operations().len(), 3);
}

// ============================================================================
// CdcStream Tests
// ============================================================================

#[tokio::test]
async fn test_cdc_stream_poll() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["users"])
        .build()
        .await
        .unwrap();

    let event = stream.poll().await.unwrap();
    assert!(event.is_none()); // Stub returns None
}

#[tokio::test]
async fn test_cdc_stream_poll_timeout() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["users"])
        .build()
        .await
        .unwrap();

    let start = std::time::Instant::now();
    let event = stream
        .poll_timeout(Duration::from_millis(50))
        .await
        .unwrap();

    assert!(event.is_none());
    assert!(start.elapsed() < Duration::from_secs(1)); // Should return quickly (stub)
}

#[tokio::test]
async fn test_cdc_stream_tables() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["a", "b", "c"])
        .build()
        .await
        .unwrap();

    let tables = stream.tables();
    assert_eq!(tables.len(), 3);
}

#[tokio::test]
async fn test_cdc_stream_operations() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["test"])
        .operations(&[Op::Insert])
        .build()
        .await
        .unwrap();

    let ops = stream.operations();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0], Op::Insert);
}

#[tokio::test]
async fn test_cdc_stream_close() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["users"])
        .build()
        .await
        .unwrap();

    stream.close().await.unwrap();
}

#[tokio::test]
async fn test_cdc_builder_subscribe_alias() {
    let stream = CdcBuilder::new("localhost:6379")
        .tables(&["users"])
        .subscribe()
        .await
        .unwrap();

    assert_eq!(stream.tables().len(), 1);
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
        after: Some(serde_json::json!({
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com"
        })),
        timestamp: 1700000000000,
        tx_id: Some("tx-001".to_string()),
    };

    assert_eq!(event.table, "users");
    assert_eq!(event.op, Op::Insert);
    assert!(event.before.is_none());
    assert!(event.after.is_some());

    let after = event.after.unwrap();
    assert_eq!(after["name"], "Alice");
}

#[test]
fn test_change_event_update() {
    let event = ChangeEvent {
        table: "orders".to_string(),
        op: Op::Update,
        before: Some(serde_json::json!({
            "id": 1,
            "status": "pending"
        })),
        after: Some(serde_json::json!({
            "id": 1,
            "status": "completed"
        })),
        timestamp: 1700000000000,
        tx_id: None,
    };

    assert_eq!(event.op, Op::Update);
    assert!(event.before.is_some());
    assert!(event.after.is_some());

    let before = event.before.unwrap();
    let after = event.after.unwrap();
    assert_eq!(before["status"], "pending");
    assert_eq!(after["status"], "completed");
}

#[test]
fn test_change_event_delete() {
    let event = ChangeEvent {
        table: "sessions".to_string(),
        op: Op::Delete,
        before: Some(serde_json::json!({
            "id": "sess-123",
            "user_id": 1
        })),
        after: None,
        timestamp: 1700000000000,
        tx_id: None,
    };

    assert_eq!(event.op, Op::Delete);
    assert!(event.before.is_some());
    assert!(event.after.is_none());
}

#[test]
fn test_change_event_clone() {
    let event = ChangeEvent {
        table: "t".to_string(),
        op: Op::Insert,
        before: None,
        after: Some(serde_json::json!({})),
        timestamp: 0,
        tx_id: None,
    };

    let cloned = event.clone();
    assert_eq!(event.table, cloned.table);
    assert_eq!(event.op, cloned.op);
}

#[test]
fn test_change_event_debug() {
    let event = ChangeEvent {
        table: "test".to_string(),
        op: Op::Insert,
        before: None,
        after: None,
        timestamp: 0,
        tx_id: None,
    };

    let debug = format!("{:?}", event);
    assert!(debug.contains("ChangeEvent"));
    assert!(debug.contains("test"));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_cdc_builder_empty_tables_error() {
    let cdc = CdcBuilder::new("localhost:6379").tables(&[]);

    let result = cdc.build().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cdc_builder_all_operations() {
    let cdc = CdcBuilder::new("localhost:6379")
        .tables(&["test"])
        .operations(&[Op::Insert, Op::Update, Op::Delete]);

    let stream = cdc.build().await.unwrap();
    assert_eq!(stream.operations().len(), 3);
}

#[tokio::test]
async fn test_cdc_builder_unicode_table_names() {
    let cdc = CdcBuilder::new("localhost:6379").tables(&["日本語テーブル", "中文表"]);

    let stream = cdc.build().await.unwrap();
    assert!(stream.tables().contains(&"日本語テーブル".to_string()));
}

#[test]
fn test_change_event_complex_json() {
    let event = ChangeEvent {
        table: "complex".to_string(),
        op: Op::Insert,
        before: None,
        after: Some(serde_json::json!({
            "nested": {
                "array": [1, 2, 3],
                "object": {"key": "value"}
            },
            "tags": ["a", "b", "c"],
            "count": 42,
            "active": true,
            "ratio": 3.15
        })),
        timestamp: 0,
        tx_id: None,
    };

    let after = event.after.unwrap();
    assert_eq!(after["nested"]["array"][0], 1);
    assert_eq!(after["tags"][1], "b");
}
