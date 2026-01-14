//! Region Discovery and Routing
//!
//! Integrates with SpireDB's Cluster Service to discover regions
//! and route queries to the appropriate shards.
//! Uses LRU cache for region and store address caching with eviction.

use crate::cache::{new_shared_cache, SharedLruCache};
use ahash::AHasher;
use spire_proto::spiredb::cluster::{
    cluster_service_client::ClusterServiceClient, GetStoreRequest, GetTableRegionsRequest, Region,
    RegionList, Store,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tonic::transport::Channel;

/// Default cache capacity for regions.
const DEFAULT_REGION_CACHE_CAPACITY: usize = 256;
/// Default cache capacity for store addresses.
const DEFAULT_STORE_CACHE_CAPACITY: usize = 64;

/// Information about a region for routing queries.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub region_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub leader_store_id: u64,
    pub peer_store_ids: Vec<u64>,
}

impl From<Region> for RegionInfo {
    fn from(r: Region) -> Self {
        Self {
            region_id: r.id,
            start_key: r.start_key,
            end_key: r.end_key,
            leader_store_id: r.leader_store_id,
            peer_store_ids: r.peers.into_iter().map(|p| p.store_id).collect(),
        }
    }
}

/// Cached region list.
#[derive(Clone)]
struct CachedRegions {
    regions: Arc<Vec<RegionInfo>>,
}

/// Region router with LRU caching.
pub struct RegionRouter {
    /// Cluster client for region discovery.
    cluster_client: ClusterServiceClient<Channel>,

    /// LRU cache for regions: table_name_hash -> cached regions.
    region_cache: SharedLruCache<CachedRegions>,

    /// LRU cache for store addresses: store_id -> address.
    store_cache: SharedLruCache<String>,
}

impl std::fmt::Debug for RegionRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionRouter")
            .field("region_cache_len", &self.region_cache.len())
            .field("store_cache_len", &self.store_cache.len())
            .finish()
    }
}

impl RegionRouter {
    /// Create a new region router.
    pub fn new(cluster_client: ClusterServiceClient<Channel>) -> Self {
        Self::with_capacity(
            cluster_client,
            DEFAULT_REGION_CACHE_CAPACITY,
            DEFAULT_STORE_CACHE_CAPACITY,
        )
    }

    /// Create a new region router with custom cache capacities.
    pub fn with_capacity(
        cluster_client: ClusterServiceClient<Channel>,
        region_cache_capacity: usize,
        store_cache_capacity: usize,
    ) -> Self {
        Self {
            cluster_client,
            region_cache: new_shared_cache(region_cache_capacity),
            store_cache: new_shared_cache(store_cache_capacity),
        }
    }

    /// Hash a value for lookup (ahash for speed).
    fn hash_key<T: Hash>(key: &T) -> u64 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get all regions for a table (LRU cached).
    pub async fn get_table_regions(
        &self,
        table: &str,
    ) -> Result<Arc<Vec<RegionInfo>>, tonic::Status> {
        let hash = Self::hash_key(&table);

        // Check cache first
        if let Some(cached) = self.region_cache.get_and_touch(hash) {
            return Ok(cached.regions);
        }

        // Cache miss: fetch from cluster service
        self.refresh_table_regions(table).await
    }

    /// Refresh regions from cluster service and update cache.
    async fn refresh_table_regions(
        &self,
        table: &str,
    ) -> Result<Arc<Vec<RegionInfo>>, tonic::Status> {
        let hash = Self::hash_key(&table);

        let request = GetTableRegionsRequest {
            table_name: table.to_string(),
        };

        let mut client = self.cluster_client.clone();
        let response = client.get_table_regions(request).await?;
        let region_list: RegionList = response.into_inner();

        let regions: Vec<RegionInfo> = region_list
            .regions
            .into_iter()
            .map(RegionInfo::from)
            .collect();
        let regions = Arc::new(regions);

        let cached = CachedRegions {
            regions: regions.clone(),
        };

        // Insert with LRU eviction
        self.region_cache.insert(hash, cached);

        log::debug!(
            "Cached {} regions for table '{}' (hash: {})",
            regions.len(),
            table,
            hash
        );

        Ok(regions)
    }

    /// Get store address by store ID (LRU cached).
    pub async fn get_store_address(&self, store_id: u64) -> Result<String, tonic::Status> {
        // Check cache first
        if let Some(addr) = self.store_cache.get_and_touch(store_id) {
            return Ok(addr);
        }

        // Cache miss: fetch from cluster service
        let request = GetStoreRequest { store_id };
        let mut client = self.cluster_client.clone();
        let response = client.get_store(request).await?;
        let store: Store = response.into_inner();

        // Insert with LRU eviction
        self.store_cache.insert(store_id, store.address.clone());

        Ok(store.address)
    }

    /// Get regions that may contain keys in the given range.
    pub async fn get_regions_for_range(
        &self,
        table: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<RegionInfo>, tonic::Status> {
        let all_regions = self.get_table_regions(table).await?;

        // Filter regions that overlap with the key range
        let matching: Vec<RegionInfo> = all_regions
            .iter()
            .filter(|r| {
                // Region overlaps if: region.start < end_key AND region.end > start_key
                (r.end_key.is_empty() || r.end_key.as_slice() > start_key)
                    && (end_key.is_empty() || r.start_key.as_slice() < end_key)
            })
            .cloned()
            .collect();

        Ok(matching)
    }

    /// Invalidate region cache for a table.
    pub fn invalidate_table(&self, table: &str) {
        let hash = Self::hash_key(&table);
        self.region_cache.remove(hash);
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            region_cache_size: self.region_cache.len(),
            region_cache_capacity: self.region_cache.capacity(),
            store_cache_size: self.store_cache.len(),
            store_cache_capacity: self.store_cache.capacity(),
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub region_cache_size: usize,
    pub region_cache_capacity: usize,
    pub store_cache_size: usize,
    pub store_cache_capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_key() {
        let hash1 = RegionRouter::hash_key(&"users");
        let hash2 = RegionRouter::hash_key(&"users");
        let hash3 = RegionRouter::hash_key(&"orders");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
}
