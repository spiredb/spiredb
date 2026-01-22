//! Statistics Provider
//!
//! Fetches table statistics from SpireDB for cost-based query optimization.
//! Uses GetTableStats API for row counts, column cardinality, and min/max values.
//! Implements LRU eviction for cached statistics.

use super::cache::{SharedLruCache, new_shared_cache};
use ahash::{AHasher, HashMap};
use datafusion::common::ScalarValue;
use spire_proto::spiredb::cluster::{
    GetTableStatsRequest, TableStats, schema_service_client::SchemaServiceClient,
};
use std::hash::{Hash, Hasher};
use tonic::transport::Channel;

/// Default cache capacity for statistics.
const DEFAULT_STATS_CACHE_CAPACITY: usize = 128;

/// Decode a statistics value from JSON bytes.
///
/// JSON encoding format (consistent with filter values):
/// - `{"int": 42}`
/// - `{"float": 3.14}`
/// - `{"str": "hello"}`
/// - `{"bytes": "base64..."}`
/// - `{"bool": true}`
fn decode_stat_value(bytes: &[u8]) -> Option<ScalarValue> {
    if bytes.is_empty() {
        return None;
    }

    let json: serde_json::Value = serde_json::from_slice(bytes).ok()?;

    if let Some(v) = json.get("int").and_then(|v| v.as_i64()) {
        return Some(ScalarValue::Int64(Some(v)));
    }
    if let Some(v) = json.get("float").and_then(|v| v.as_f64()) {
        return Some(ScalarValue::Float64(Some(v)));
    }
    if let Some(v) = json.get("str").and_then(|v| v.as_str()) {
        return Some(ScalarValue::Utf8(Some(v.to_string())));
    }
    if let Some(v) = json.get("bool").and_then(|v| v.as_bool()) {
        return Some(ScalarValue::Boolean(Some(v)));
    }
    if let Some(v) = json.get("bytes").and_then(|v| v.as_str()) {
        use base64::Engine;
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(v) {
            return Some(ScalarValue::Binary(Some(decoded)));
        }
    }

    None
}

/// Cached statistics for a table.
#[derive(Clone, Debug)]
pub struct CachedStats {
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

/// Statistics for a single column.
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    pub distinct_count: u64,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
    pub null_count: u64,
}

/// Statistics provider with LRU caching.
pub struct StatisticsProvider {
    /// PD client for stats fetching.
    pd_client: SchemaServiceClient<Channel>,

    /// LRU cache: table_name_hash -> CachedStats.
    stats_cache: SharedLruCache<CachedStats>,
}

impl StatisticsProvider {
    /// Create a new statistics provider.
    pub fn new(pd_client: SchemaServiceClient<Channel>) -> Self {
        Self::with_capacity(pd_client, DEFAULT_STATS_CACHE_CAPACITY)
    }

    /// Create with custom cache capacity.
    pub fn with_capacity(pd_client: SchemaServiceClient<Channel>, capacity: usize) -> Self {
        Self {
            pd_client,
            stats_cache: new_shared_cache(capacity),
        }
    }

    /// Hash a table name (ahash for speed).
    fn hash_table_name(table: &str) -> u64 {
        let mut hasher = AHasher::default();
        table.hash(&mut hasher);
        hasher.finish()
    }

    /// Get cached statistics for a table (LRU cache).
    pub fn get_cached_stats(&self, table: &str) -> Option<CachedStats> {
        let hash = Self::hash_table_name(table);
        self.stats_cache.get_and_touch(hash)
    }

    /// Refresh statistics from SpireDB.
    pub async fn refresh_stats(&self, table: &str) -> Result<CachedStats, tonic::Status> {
        let hash = Self::hash_table_name(table);

        let request = GetTableStatsRequest {
            table_name: table.to_string(),
        };

        let mut client = self.pd_client.clone();
        let response: TableStats = client.get_table_stats(request).await?.into_inner();

        // Convert column stats with min/max decoding
        let column_stats: HashMap<String, ColumnStatistics> = response
            .column_stats
            .into_iter()
            .map(|(name, cs)| {
                (
                    name,
                    ColumnStatistics {
                        distinct_count: cs.distinct_count,
                        min_value: decode_stat_value(&cs.min_value),
                        max_value: decode_stat_value(&cs.max_value),
                        null_count: cs.null_count,
                    },
                )
            })
            .collect();

        let cached = CachedStats {
            row_count: response.row_count,
            size_bytes: response.size_bytes,
            column_stats,
        };

        // Insert with LRU eviction
        self.stats_cache.insert(hash, cached.clone());

        log::debug!(
            "Cached stats for table '{}': {} rows, {} bytes",
            table,
            cached.row_count,
            cached.size_bytes
        );

        Ok(cached)
    }

    /// Get statistics, refreshing if not cached.
    pub async fn get_table_stats(&self, table: &str) -> Result<CachedStats, tonic::Status> {
        if let Some(stats) = self.get_cached_stats(table) {
            return Ok(stats);
        }
        self.refresh_stats(table).await
    }

    /// Invalidate cached stats for a table.
    #[allow(dead_code)]
    pub fn invalidate(&self, table: &str) {
        let hash = Self::hash_table_name(table);
        self.stats_cache.remove(hash);
    }

    /// Get cache statistics.
    #[allow(dead_code)]
    pub fn cache_size(&self) -> usize {
        self.stats_cache.len()
    }
}
