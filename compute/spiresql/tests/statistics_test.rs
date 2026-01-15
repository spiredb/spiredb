//! Tests for the statistics module.
//!
//! Tests cover:
//! - CachedStats structure
//! - ColumnStatistics structure  
//! - Default values

use ahash::HashMap;
use spiresql::statistics::{CachedStats, ColumnStatistics};

/// Test CachedStats structure.
#[test]
fn test_cached_stats_structure() {
    let stats = CachedStats {
        row_count: 1000,
        size_bytes: 102400,
        column_stats: HashMap::default(),
    };

    assert_eq!(stats.row_count, 1000);
    assert_eq!(stats.size_bytes, 102400);
    assert!(stats.column_stats.is_empty());
}

/// Test CachedStats with columns.
#[test]
fn test_cached_stats_with_columns() {
    let mut column_stats = HashMap::default();
    column_stats.insert(
        "id".to_string(),
        ColumnStatistics {
            distinct_count: 1000,
            null_count: 0,
            min_value: None,
            max_value: None,
        },
    );

    let stats = CachedStats {
        row_count: 5000,
        size_bytes: 512000,
        column_stats,
    };

    assert_eq!(stats.column_stats.len(), 1);
    let id_stats = stats.column_stats.get("id").unwrap();
    assert_eq!(id_stats.distinct_count, 1000);
    assert_eq!(id_stats.null_count, 0);
}

/// Test ColumnStatistics with distinct count.
#[test]
fn test_column_statistics_distinct() {
    let stats = ColumnStatistics {
        distinct_count: 25,
        null_count: 10,
        min_value: None,
        max_value: None,
    };

    assert_eq!(stats.distinct_count, 25);
    assert_eq!(stats.null_count, 10);
}

/// Test ColumnStatistics clone.
#[test]
fn test_column_statistics_clone() {
    let stats = ColumnStatistics {
        distinct_count: 50,
        null_count: 0,
        min_value: None,
        max_value: None,
    };
    let cloned = stats.clone();

    assert_eq!(stats.distinct_count, cloned.distinct_count);
}

/// Test empty CachedStats.
#[test]
fn test_empty_cached_stats() {
    let stats = CachedStats {
        row_count: 0,
        size_bytes: 0,
        column_stats: HashMap::default(),
    };

    assert_eq!(stats.row_count, 0);
    assert_eq!(stats.size_bytes, 0);
}

/// Test high cardinality column.
#[test]
fn test_high_cardinality_column() {
    let stats = ColumnStatistics {
        distinct_count: 1_000_000,
        null_count: 0,
        min_value: None,
        max_value: None,
    };

    assert_eq!(stats.distinct_count, 1_000_000);
}

/// Test CachedStats debug.
#[test]
fn test_cached_stats_debug() {
    let stats = CachedStats {
        row_count: 42,
        size_bytes: 4096,
        column_stats: HashMap::default(),
    };
    let debug_str = format!("{:?}", stats);

    assert!(debug_str.contains("42"));
}

/// Test ColumnStatistics debug.
#[test]
fn test_column_statistics_debug() {
    let stats = ColumnStatistics {
        distinct_count: 100,
        null_count: 5,
        min_value: None,
        max_value: None,
    };
    let debug_str = format!("{:?}", stats);

    assert!(debug_str.contains("100"));
}
