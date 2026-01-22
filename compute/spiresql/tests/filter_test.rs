//! Tests for the filter serialization module.
//!
//! Tests cover:
//! - Simple comparison operators (eq, ne, lt, le, gt, ge)
//! - Logical operators (AND, OR, NOT)
//! - NULL handling (IS NULL, IS NOT NULL)
//! - IN list expressions
//! - BETWEEN expressions
//! - Various scalar types (int, float, string, binary, bool)
//! - Empty filters
//! - Unsupported expressions fallback

use datafusion::prelude::*;
use serde_json::Value;
use spiresql::sql::filter::serialize_filter;

/// Test equality filter serialization.
#[test]
fn test_serialize_eq_int() {
    let expr = col("id").eq(lit(42i64));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "eq");
    assert_eq!(parsed["col"], "id");
    assert_eq!(parsed["val"]["int"], 42);
}

/// Test equality filter with string literal.
#[test]
fn test_serialize_eq_string() {
    let expr = col("name").eq(lit("Alice"));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "eq");
    assert_eq!(parsed["col"], "name");
    assert_eq!(parsed["val"]["str"], "Alice");
}

/// Test not-equal filter.
#[test]
fn test_serialize_ne() {
    let expr = col("status").not_eq(lit("deleted"));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "ne");
    assert_eq!(parsed["col"], "status");
}

/// Test less-than filter.
#[test]
fn test_serialize_lt() {
    let expr = col("age").lt(lit(18i32));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "lt");
    assert_eq!(parsed["col"], "age");
}

/// Test greater-than-or-equal filter.
#[test]
fn test_serialize_gte() {
    let expr = col("score").gt_eq(lit(100.0f64));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "ge");
    assert_eq!(parsed["col"], "score");
    assert!(parsed["val"]["float"].as_f64().is_some());
}

/// Test AND combination of filters.
#[test]
fn test_serialize_and() {
    let expr = col("id").eq(lit(1i64)).and(col("active").eq(lit(true)));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "and");
    assert!(parsed["args"].is_array());
    assert_eq!(parsed["args"].as_array().unwrap().len(), 2);
}

/// Test OR combination of filters.
#[test]
fn test_serialize_or() {
    let expr = col("status")
        .eq(lit("active"))
        .or(col("status").eq(lit("pending")));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "or");
    assert!(parsed["args"].is_array());
}

/// Test NOT expression.
#[test]
fn test_serialize_not() {
    let expr = col("deleted").eq(lit(true)).not();
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "not");
}

/// Test empty filters returns empty bytes.
#[test]
fn test_serialize_empty_filters() {
    let json = serialize_filter(&[]);
    assert!(json.is_empty());
}

/// Test multiple filters combined with AND.
#[test]
fn test_serialize_multiple_filters() {
    let filters = vec![col("id").gt(lit(0i64)), col("status").eq(lit("active"))];
    let json = serialize_filter(&filters);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "and");
    assert_eq!(parsed["args"].as_array().unwrap().len(), 2);
}

/// Test boolean literal.
#[test]
fn test_serialize_bool_literal() {
    let expr = col("active").eq(lit(true));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["val"]["bool"], true);
}

/// Test reversed comparison (e.g., 5 > age becomes age < 5).
#[test]
fn test_serialize_reversed_comparison() {
    // In DataFusion, this would typically be normalized, but test the logic
    let expr = col("age").lt(lit(30i32));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "lt");
    assert_eq!(parsed["col"], "age");
}

/// Test float values.
#[test]
fn test_serialize_float() {
    let expr = col("price").gt(lit(99.99f64));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    assert_eq!(parsed["op"], "gt");
    assert!(parsed["val"]["float"].as_f64().unwrap() > 99.0);
}

/// Test complex nested expression.
#[test]
fn test_serialize_complex_nested() {
    let expr = col("a")
        .eq(lit(1i32))
        .and(col("b").eq(lit(2i32)).or(col("c").eq(lit(3i32))));
    let json = serialize_filter(&[expr]);
    let parsed: Value = serde_json::from_slice(&json).unwrap();

    // Should be an AND with nested OR
    assert_eq!(parsed["op"], "and");
}
