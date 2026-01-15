//! Comprehensive tests for the LRU cache module.
//!
//! Tests cover:
//! - Basic operations (insert, get, remove)
//! - LRU eviction behavior
//! - Thread-safety under concurrent access
//! - Edge cases (empty cache, capacity 1)

use spiresql::cache::{LruCache, SharedLruCache, new_shared_cache};
use std::sync::Arc;
use std::thread;

/// Test basic cache operations: insert, get, remove.
#[test]
fn test_cache_basic_operations() {
    let cache: LruCache<String> = LruCache::new(10);

    // Insert and retrieve
    let key1 = LruCache::<String>::hash_key(&"key1");
    cache.insert(key1, "value1".to_string());
    assert_eq!(cache.get(key1), Some("value1".to_string()));
    assert_eq!(cache.len(), 1);

    // Overwrite existing key
    cache.insert(key1, "value1_updated".to_string());
    assert_eq!(cache.get(key1), Some("value1_updated".to_string()));
    assert_eq!(cache.len(), 1);

    // Remove
    let removed = cache.remove(key1);
    assert_eq!(removed, Some("value1_updated".to_string()));
    assert!(cache.get(key1).is_none());
    assert_eq!(cache.len(), 0);
}

/// Test that LRU eviction works correctly.
#[test]
fn test_cache_lru_eviction() {
    let cache: LruCache<i32> = LruCache::new(3);

    // Insert 3 items
    cache.insert(1, 100);
    thread::sleep(std::time::Duration::from_millis(5));
    cache.insert(2, 200);
    thread::sleep(std::time::Duration::from_millis(5));
    cache.insert(3, 300);

    assert_eq!(cache.len(), 3);

    // Access key 1 to make it recently used
    cache.get_and_touch(1);
    thread::sleep(std::time::Duration::from_millis(5));

    // Insert key 4 - should evict key 2 (oldest untouched)
    cache.insert(4, 400);

    assert_eq!(cache.len(), 3);
    assert!(
        cache.get(1).is_some(),
        "Key 1 should still exist (recently touched)"
    );
    assert!(cache.get(2).is_none(), "Key 2 should be evicted (oldest)");
    assert!(cache.get(3).is_some(), "Key 3 should still exist");
    assert!(cache.get(4).is_some(), "Key 4 should exist (just inserted)");
}

/// Test cache with capacity 1 (edge case).
#[test]
fn test_cache_capacity_one() {
    let cache: LruCache<String> = LruCache::new(1);

    cache.insert(1, "first".to_string());
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(1), Some("first".to_string()));

    // Insert second - should evict first
    cache.insert(2, "second".to_string());
    assert_eq!(cache.len(), 1);
    assert!(cache.get(1).is_none());
    assert_eq!(cache.get(2), Some("second".to_string()));
}

/// Test empty cache operations.
#[test]
fn test_cache_empty_operations() {
    let cache: LruCache<String> = LruCache::new(10);

    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
    assert!(cache.get(12345).is_none());
    assert!(cache.remove(12345).is_none());

    cache.clear();
    assert!(cache.is_empty());
}

/// Test concurrent access to shared cache.
#[test]
fn test_cache_concurrent_access() {
    let cache: SharedLruCache<i32> = new_shared_cache(100);

    let mut handles = vec![];

    // Spawn 10 writer threads
    for i in 0..10 {
        let cache_clone: SharedLruCache<i32> = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let key = (i * 100 + j) as u64;
                cache_clone.insert(key, i * 1000 + j);
            }
        }));
    }

    // Spawn 10 reader threads
    for _ in 0..10 {
        let cache_clone: SharedLruCache<i32> = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for key in 0..1000u64 {
                let _ = cache_clone.get(key);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Cache should have at most 100 entries (capacity)
    assert!(cache.len() <= 100);
}

/// Test clear functionality.
#[test]
fn test_cache_clear() {
    let cache: LruCache<i32> = LruCache::new(10);

    for i in 0..5u64 {
        cache.insert(i, i as i32 * 10);
    }
    assert_eq!(cache.len(), 5);

    cache.clear();
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
}

/// Test capacity getter.
#[test]
fn test_cache_capacity() {
    let cache: LruCache<i32> = LruCache::new(42);
    assert_eq!(cache.capacity(), 42);
}

/// Test hash_key produces consistent results.
#[test]
fn test_hash_key_consistency() {
    let hash1 = LruCache::<i32>::hash_key(&"test_key");
    let hash2 = LruCache::<i32>::hash_key(&"test_key");
    let hash3 = LruCache::<i32>::hash_key(&"different_key");

    assert_eq!(hash1, hash2, "Same keys should produce same hash");
    assert_ne!(
        hash1, hash3,
        "Different keys should produce different hashes"
    );
}
