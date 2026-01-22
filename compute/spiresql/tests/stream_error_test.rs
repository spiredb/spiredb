//! Tests for stream error types.
//!
//! Covers all StreamError variants, Display, Error traits, and From conversions.

use spiresql::stream::prelude::*;
use std::error::Error;

// ============================================================================
// StreamError Variant Tests
// ============================================================================

#[test]
fn test_error_connection() {
    let err = StreamError::Connection("connection refused".to_string());
    assert!(format!("{}", err).contains("connection error"));
    assert!(format!("{}", err).contains("connection refused"));
}

#[test]
fn test_error_timeout() {
    let err = StreamError::Timeout("request timed out after 30s".to_string());
    assert!(format!("{}", err).contains("timeout"));
}

#[test]
fn test_error_config() {
    let err = StreamError::Config("invalid bootstrap servers".to_string());
    assert!(format!("{}", err).contains("configuration error"));
}

#[test]
fn test_error_serde() {
    let err = StreamError::Serde("failed to parse JSON".to_string());
    assert!(format!("{}", err).contains("serialization error"));
}

#[test]
fn test_error_topic_not_found() {
    let err = StreamError::TopicNotFound("orders".to_string());
    let display = format!("{}", err);
    assert!(display.contains("topic not found"));
    assert!(display.contains("orders"));
}

#[test]
fn test_error_partition_not_found() {
    let err = StreamError::PartitionNotFound {
        topic: "events".to_string(),
        partition: 5,
    };
    let display = format!("{}", err);
    assert!(display.contains("partition not found"));
    assert!(display.contains("events"));
    assert!(display.contains("5"));
}

#[test]
fn test_error_group_not_found() {
    let err = StreamError::GroupNotFound("my-consumer-group".to_string());
    let display = format!("{}", err);
    assert!(display.contains("group not found"));
    assert!(display.contains("my-consumer-group"));
}

#[test]
fn test_error_consumer_not_found() {
    let err = StreamError::ConsumerNotFound {
        group: "group-1".to_string(),
        consumer: "consumer-abc".to_string(),
    };
    let display = format!("{}", err);
    assert!(display.contains("consumer not found"));
    assert!(display.contains("consumer-abc"));
    assert!(display.contains("group-1"));
}

#[test]
fn test_error_offset_out_of_range() {
    let err = StreamError::OffsetOutOfRange {
        topic: "logs".to_string(),
        partition: 2,
        offset: 999999,
    };
    let display = format!("{}", err);
    assert!(display.contains("offset out of range"));
    assert!(display.contains("logs"));
    assert!(display.contains("2"));
    assert!(display.contains("999999"));
}

#[test]
fn test_error_transaction() {
    let err = StreamError::Transaction("transaction aborted".to_string());
    assert!(format!("{}", err).contains("transaction error"));
}

#[test]
fn test_error_authorization() {
    let err = StreamError::Authorization("access denied to topic".to_string());
    assert!(format!("{}", err).contains("authorization error"));
}

#[test]
fn test_error_server() {
    let err = StreamError::Server("internal server error".to_string());
    assert!(format!("{}", err).contains("server error"));
}

#[test]
fn test_error_internal() {
    let err = StreamError::Internal("unexpected state".to_string());
    assert!(format!("{}", err).contains("internal error"));
}

// ============================================================================
// Error Trait Tests
// ============================================================================

#[test]
fn test_error_is_error() {
    let err = StreamError::Config("test".to_string());
    let _: &dyn Error = &err;
}

#[test]
fn test_error_source_none_for_simple() {
    let err = StreamError::Config("test".to_string());
    assert!(err.source().is_none());
}

#[test]
fn test_error_debug() {
    let err = StreamError::TopicNotFound("test".to_string());
    let debug = format!("{:?}", err);
    assert!(debug.contains("TopicNotFound"));
}

// ============================================================================
// From Conversions Tests
// ============================================================================

#[test]
fn test_from_tonic_status() {
    let status = tonic::Status::not_found("resource not found");
    let err: StreamError = status.into();

    match err {
        StreamError::Status(s) => {
            assert_eq!(s.code(), tonic::Code::NotFound);
        }
        _ => panic!("Expected Status variant"),
    }
}

#[test]
fn test_error_status_source() {
    let status = tonic::Status::internal("server error");
    let err: StreamError = status.into();

    // Status errors have a source
    assert!(err.source().is_some());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_error_empty_message() {
    let err = StreamError::Config(String::new());
    let display = format!("{}", err);
    assert!(display.contains("configuration error"));
}

#[test]
fn test_error_unicode_message() {
    let err = StreamError::Serde("invalid UTF-8: 日本語テスト".to_string());
    let display = format!("{}", err);
    assert!(display.contains("日本語テスト"));
}

#[test]
fn test_error_long_message() {
    let long_msg = "x".repeat(10000);
    let err = StreamError::Internal(long_msg.clone());
    let display = format!("{}", err);
    assert!(display.contains(&long_msg));
}
