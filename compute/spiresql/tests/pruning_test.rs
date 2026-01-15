//! Tests for the pruning module.
//!
//! Tests cover:
//! - Extracting equality bounds (point lookups)
//! - Extracting range bounds (>, <, >=, <=)
//! - AND conjunction of predicates
//! - Non-PK filters (should return unbounded)
//! - Edge cases (empty filters)

use datafusion::logical_expr::lit;
use datafusion::prelude::col;
use spiresql::pruning::{KeyBounds, extract_key_bounds};

/// Test extracting bounds from equality predicate (pk = value).
#[test]
fn test_extract_eq_point_lookup() {
    let filters = vec![col("id").eq(lit("user123"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert_eq!(bounds.start_key, Some(b"user123".to_vec()));
}

/// Test extracting bounds from greater-than predicate.
#[test]
fn test_extract_gt_bound() {
    let filters = vec![col("id").gt(lit("abc"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert!(bounds.start_key.is_some());
    assert!(bounds.end_key.is_none());
}

/// Test extracting bounds from greater-than-or-equal predicate.
#[test]
fn test_extract_gte_bound() {
    let filters = vec![col("id").gt_eq(lit("start"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert_eq!(bounds.start_key, Some(b"start".to_vec()));
    assert!(bounds.end_key.is_none());
}

/// Test extracting bounds from less-than predicate.
#[test]
fn test_extract_lt_bound() {
    let filters = vec![col("id").lt(lit("end"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert!(bounds.start_key.is_none());
    assert_eq!(bounds.end_key, Some(b"end".to_vec()));
}

/// Test extracting bounds from less-than-or-equal predicate.
#[test]
fn test_extract_lte_bound() {
    let filters = vec![col("id").lt_eq(lit("max"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert!(bounds.start_key.is_none());
    assert!(bounds.end_key.is_some());
}

/// Test extracting bounds from range (>= AND <).
#[test]
fn test_extract_range_bounds() {
    let filters = vec![col("id").gt_eq(lit("a")), col("id").lt(lit("z"))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert_eq!(bounds.start_key, Some(b"a".to_vec()));
    assert_eq!(bounds.end_key, Some(b"z".to_vec()));
}

/// Test that non-PK columns don't affect bounds.
#[test]
fn test_non_pk_filter_ignored() {
    let filters = vec![col("name").eq(lit("Alice")), col("age").gt(lit(18i32))];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(!bounds.is_bounded());
}

/// Test empty filters return unbounded.
#[test]
fn test_empty_filters() {
    let filters: Vec<datafusion::prelude::Expr> = vec![];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(!bounds.is_bounded());
    assert_eq!(bounds.start_key, None);
    assert_eq!(bounds.end_key, None);
}

/// Test KeyBounds helper methods.
#[test]
fn test_key_bounds_helpers() {
    let unbounded = KeyBounds::unbounded();
    assert!(!unbounded.is_bounded());
    assert_eq!(unbounded.start_key_slice(), &[] as &[u8]);
    assert_eq!(unbounded.end_key_slice(), &[] as &[u8]);

    let bounded = KeyBounds {
        start_key: Some(vec![1, 2, 3]),
        end_key: Some(vec![4, 5, 6]),
    };
    assert!(bounded.is_bounded());
    assert_eq!(bounded.start_key_slice(), &[1u8, 2, 3]);
    assert_eq!(bounded.end_key_slice(), &[4u8, 5, 6]);
}

/// Test mixed PK and non-PK filters.
#[test]
fn test_mixed_pk_and_non_pk_filters() {
    let filters = vec![
        col("id").gt_eq(lit("start")),
        col("name").eq(lit("test")),
        col("id").lt(lit("end")),
    ];
    let bounds = extract_key_bounds(&filters, "id");

    assert!(bounds.is_bounded());
    assert_eq!(bounds.start_key, Some(b"start".to_vec()));
    assert_eq!(bounds.end_key, Some(b"end".to_vec()));
}

/// Test integer literal extraction.
#[test]
fn test_integer_literal_bounds() {
    let filters = vec![col("user_id").eq(lit(12345i64))];
    let bounds = extract_key_bounds(&filters, "user_id");

    assert!(bounds.is_bounded());
    assert!(bounds.start_key.is_some());
}
