//! Tests for vector types.

use spiresql::vector::types::{Algorithm, IndexParams, SearchOptions};

// ============================================================================
// SearchOptions Tests
// ============================================================================

#[test]
fn test_search_options_default() {
    let opts = SearchOptions::default();
    assert_eq!(opts.k, 10);
    assert_eq!(opts.radius, 0.0);
    assert!(opts.filter.is_none());
    assert!(!opts.return_payload);
}

#[test]
fn test_search_options_k() {
    let opts = SearchOptions::default().k(50);
    assert_eq!(opts.k, 50);
}

#[test]
fn test_search_options_radius() {
    let opts = SearchOptions::default().radius(1.5);
    assert_eq!(opts.radius, 1.5);
}

#[test]
fn test_search_options_filter() {
    let filter = b"field=value".to_vec();
    let opts = SearchOptions::default().filter(filter.clone());
    assert_eq!(opts.filter, Some(filter));
}

#[test]
fn test_search_options_with_payload() {
    let opts = SearchOptions::default().with_payload();
    assert!(opts.return_payload);
}

#[test]
fn test_search_options_chained() {
    let opts = SearchOptions::default()
        .k(20)
        .radius(0.5)
        .with_payload()
        .filter(b"x".to_vec());

    assert_eq!(opts.k, 20);
    assert_eq!(opts.radius, 0.5);
    assert!(opts.return_payload);
    assert!(opts.filter.is_some());
}

// ============================================================================
// Algorithm Tests
// ============================================================================

#[test]
fn test_algorithm_default() {
    let algo = Algorithm::default();
    assert_eq!(algo, Algorithm::Anode);
}

#[test]
fn test_algorithm_as_str() {
    assert_eq!(Algorithm::Anode.as_str(), "ANODE");
    assert_eq!(Algorithm::Manode.as_str(), "MANODE");
}

// ============================================================================
// IndexParams Tests
// ============================================================================

#[test]
fn test_index_params_new() {
    let params = IndexParams::new("my_index", "users", "embedding");

    assert_eq!(params.name, "my_index");
    assert_eq!(params.table_name, "users");
    assert_eq!(params.column_name, "embedding");
    assert_eq!(params.algorithm, Algorithm::Anode);
    assert_eq!(params.dimensions, 128);
    assert_eq!(params.shards, 4);
}

#[test]
fn test_index_params_algorithm() {
    let params = IndexParams::new("idx", "t", "c").algorithm(Algorithm::Manode);
    assert_eq!(params.algorithm, Algorithm::Manode);
}

#[test]
fn test_index_params_dimensions() {
    let params = IndexParams::new("idx", "t", "c").dimensions(768);
    assert_eq!(params.dimensions, 768);
}

#[test]
fn test_index_params_shards() {
    let params = IndexParams::new("idx", "t", "c").shards(16);
    assert_eq!(params.shards, 16);
}

#[test]
fn test_index_params_chained() {
    let params = IndexParams::new("vectors", "products", "features")
        .algorithm(Algorithm::Manode)
        .dimensions(512)
        .shards(8);

    assert_eq!(params.name, "vectors");
    assert_eq!(params.algorithm, Algorithm::Manode);
    assert_eq!(params.dimensions, 512);
    assert_eq!(params.shards, 8);
}
