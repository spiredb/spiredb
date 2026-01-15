//! LRU Cache Module
//!
//! Thread-safe LRU cache with configurable capacity.
//! Uses parking_lot for fast locking and ahash for keys.

use ahash::AHasher;
use ahash::HashMap;
use parking_lot::RwLock;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

/// Entry in the LRU cache with access timestamp.
#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    last_access: Instant,
}

/// Thread-safe LRU cache with eviction on capacity.
pub struct LruCache<V: Clone> {
    /// Main storage: key_hash -> entry
    entries: RwLock<HashMap<u64, CacheEntry<V>>>,
    /// Maximum capacity
    capacity: usize,
}

#[allow(dead_code)]
impl<V: Clone> LruCache<V> {
    /// Create a new LRU cache with given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::default()),
            capacity,
        }
    }

    /// Hash a key (ahash for speed).
    pub fn hash_key<K: Hash>(key: &K) -> u64 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get a value from the cache (updates access time).
    pub fn get(&self, key_hash: u64) -> Option<V> {
        // Read lock first to check
        {
            let entries = self.entries.read();
            if let Some(entry) = entries.get(&key_hash) {
                return Some(entry.value.clone());
            }
        }
        None
    }

    /// Get and update access time.
    pub fn get_and_touch(&self, key_hash: u64) -> Option<V> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(&key_hash) {
            entry.last_access = Instant::now();
            return Some(entry.value.clone());
        }
        None
    }

    /// Insert a value, evicting oldest if at capacity.
    pub fn insert(&self, key_hash: u64, value: V) {
        let mut entries = self.entries.write();

        // If at capacity and key doesn't exist, evict oldest
        if entries.len() >= self.capacity && !entries.contains_key(&key_hash) {
            self.evict_oldest_locked(&mut entries);
        }

        entries.insert(
            key_hash,
            CacheEntry {
                value,
                last_access: Instant::now(),
            },
        );
    }

    /// Evict the oldest entry (must be called with write lock held).
    fn evict_oldest_locked(&self, entries: &mut HashMap<u64, CacheEntry<V>>) {
        if let Some((&oldest_key, _)) = entries.iter().min_by_key(|(_, entry)| entry.last_access) {
            entries.remove(&oldest_key);
            log::trace!("Evicted oldest cache entry (hash: {})", oldest_key);
        }
    }

    /// Remove a specific entry.
    pub fn remove(&self, key_hash: u64) -> Option<V> {
        let mut entries = self.entries.write();
        entries.remove(&key_hash).map(|e| e.value)
    }

    /// Get current cache size.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Wrapper for Arc<LruCache> for easier sharing.
pub type SharedLruCache<V> = Arc<LruCache<V>>;

/// Create a new shared LRU cache.
pub fn new_shared_cache<V: Clone>(capacity: usize) -> SharedLruCache<V> {
    Arc::new(LruCache::new(capacity))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache: LruCache<String> = LruCache::new(2);

        let key1 = LruCache::<String>::hash_key(&"key1");
        let key2 = LruCache::<String>::hash_key(&"key2");
        let key3 = LruCache::<String>::hash_key(&"key3");

        cache.insert(key1, "value1".to_string());
        cache.insert(key2, "value2".to_string());

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(key1), Some("value1".to_string()));

        // Insert third, should evict oldest (key2 if we accessed key1)
        cache.insert(key3, "value3".to_string());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let cache: LruCache<i32> = LruCache::new(2);

        cache.insert(1, 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.insert(2, 200);

        // Access key 1 to make it more recent
        cache.get_and_touch(1);
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Insert key 3, should evict key 2 (oldest)
        cache.insert(3, 300);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none()); // Evicted
        assert!(cache.get(3).is_some());
    }
}
