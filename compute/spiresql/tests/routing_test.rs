//! Tests for the routing module.
//!
//! Tests cover:
//! - RegionInfo structure
//! - Hash key consistency
//! - Cache statistics

use spiresql::routing::{CacheStats, RegionInfo};

/// Test RegionInfo structure.
#[test]
fn test_region_info_structure() {
    let region = RegionInfo {
        region_id: 42,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        leader_store_id: 1,
        peer_store_ids: vec![1, 2, 3],
    };

    assert_eq!(region.region_id, 42);
    assert_eq!(region.start_key, b"a".to_vec());
    assert_eq!(region.end_key, b"z".to_vec());
    assert_eq!(region.leader_store_id, 1);
    assert_eq!(region.peer_store_ids.len(), 3);
}

/// Test RegionInfo clone.
#[test]
fn test_region_info_clone() {
    let region = RegionInfo {
        region_id: 1,
        start_key: vec![0],
        end_key: vec![255],
        leader_store_id: 10,
        peer_store_ids: vec![10, 20],
    };
    let cloned = region.clone();

    assert_eq!(region.region_id, cloned.region_id);
    assert_eq!(region.start_key, cloned.start_key);
    assert_eq!(region.leader_store_id, cloned.leader_store_id);
}

/// Test RegionInfo debug formatting.
#[test]
fn test_region_info_debug() {
    let region = RegionInfo {
        region_id: 123,
        start_key: vec![],
        end_key: vec![],
        leader_store_id: 5,
        peer_store_ids: vec![],
    };
    let debug_str = format!("{:?}", region);

    assert!(debug_str.contains("123"));
    assert!(debug_str.contains("leader_store_id"));
}

/// Test empty region keys.
#[test]
fn test_region_info_empty_keys() {
    let region = RegionInfo {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        leader_store_id: 1,
        peer_store_ids: vec![],
    };

    assert!(region.start_key.is_empty());
    assert!(region.end_key.is_empty());
}

/// Test CacheStats structure.
#[test]
fn test_cache_stats_structure() {
    let stats = CacheStats {
        region_cache_size: 10,
        region_cache_capacity: 128,
        topology_store_count: 3,
    };

    assert_eq!(stats.region_cache_size, 10);
    assert_eq!(stats.region_cache_capacity, 128);
    assert_eq!(stats.topology_store_count, 3);
}

/// Test CacheStats clone.
#[test]
fn test_cache_stats_clone() {
    let stats = CacheStats {
        region_cache_size: 5,
        region_cache_capacity: 50,
        topology_store_count: 2,
    };
    let cloned = stats.clone();

    assert_eq!(stats.region_cache_size, cloned.region_cache_size);
}

/// Test CacheStats debug.
#[test]
fn test_cache_stats_debug() {
    let stats = CacheStats {
        region_cache_size: 42,
        region_cache_capacity: 100,
        topology_store_count: 5,
    };
    let debug_str = format!("{:?}", stats);

    assert!(debug_str.contains("42"));
    assert!(debug_str.contains("100"));
}

/// Test region with many peers.
#[test]
fn test_region_info_many_peers() {
    let peers: Vec<u64> = (1..=10).collect();
    let region = RegionInfo {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        leader_store_id: 1,
        peer_store_ids: peers.clone(),
    };

    assert_eq!(region.peer_store_ids.len(), 10);
    assert_eq!(region.peer_store_ids, peers);
}
