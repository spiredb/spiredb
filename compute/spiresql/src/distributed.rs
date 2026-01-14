//! Distributed Execution
//!
//! Parallel multi-shard query execution with stream handling.
//! Queries multiple regions concurrently and merges results.

use crate::pool::ConnectionPool;
use crate::routing::{RegionInfo, RegionRouter};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use spire_proto::spiredb::data::{TableScanRequest, TableScanResponse};
use std::io::Cursor;
use std::sync::Arc;
use tonic::Streaming;

/// Check if a region overlaps with the given key range.
fn region_overlaps(region: &RegionInfo, start_key: &[u8], end_key: &[u8]) -> bool {
    // Region overlaps if: region.start < end_key AND region.end > start_key
    // Empty end_key means unbounded (to +infinity)
    // Empty region.end_key means region extends to +infinity
    let region_end_ok = region.end_key.is_empty() || region.end_key.as_slice() > start_key;
    let query_end_ok = end_key.is_empty() || region.start_key.as_slice() < end_key;
    region_end_ok && query_end_ok
}

/// Error type for distributed execution.
#[derive(Debug)]
pub enum DistributedError {
    Routing(tonic::Status),
    Connection(Box<dyn std::error::Error + Send + Sync>),
    Grpc(tonic::Status),
    Arrow(datafusion::arrow::error::ArrowError),
    NoRegions,
}

impl std::fmt::Display for DistributedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Routing(e) => write!(f, "Routing error: {}", e),
            Self::Connection(e) => write!(f, "Connection error: {}", e),
            Self::Grpc(e) => write!(f, "gRPC error: {}", e),
            Self::Arrow(e) => write!(f, "Arrow error: {}", e),
            Self::NoRegions => write!(f, "No regions found for table"),
        }
    }
}

impl std::error::Error for DistributedError {}

/// Configuration for distributed execution.
#[derive(Clone, Debug)]
pub struct DistributedConfig {
    /// Maximum number of parallel shard queries.
    pub max_parallel_shards: usize,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            max_parallel_shards: 16,
        }
    }
}

/// Distributed query executor.
#[derive(Debug)]
pub struct DistributedExecutor {
    router: Arc<RegionRouter>,
    pool: Arc<ConnectionPool>,
    config: DistributedConfig,
}

impl DistributedExecutor {
    /// Create a new distributed executor.
    pub fn new(
        router: Arc<RegionRouter>,
        pool: Arc<ConnectionPool>,
        config: DistributedConfig,
    ) -> Self {
        Self {
            router,
            pool,
            config,
        }
    }

    /// Execute a table scan across all regions for a table.
    #[allow(dead_code)]
    pub async fn table_scan(
        &self,
        table_name: &str,
        columns: Vec<String>,
        limit: u32,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        // No pruning - scan all regions (no filter)
        self.table_scan_with_bounds(table_name, columns, limit, &[], &[], Vec::new())
            .await
    }

    /// Execute a table scan with region pruning based on key bounds.
    ///
    /// Only scans regions that may contain keys in the [start_key, end_key) range.
    pub async fn table_scan_with_bounds(
        &self,
        table_name: &str,
        columns: Vec<String>,
        limit: u32,
        start_key: &[u8],
        end_key: &[u8],
        filter_expr: Vec<u8>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        // Get regions, optionally filtered by key range
        let regions = if start_key.is_empty() && end_key.is_empty() {
            self.router
                .get_table_regions(table_name)
                .await
                .map_err(DistributedError::Routing)?
        } else {
            // Use region pruning
            let all = self
                .router
                .get_table_regions(table_name)
                .await
                .map_err(DistributedError::Routing)?;

            // Filter regions by key range
            let filtered: Vec<_> = all
                .iter()
                .filter(|r| region_overlaps(r, start_key, end_key))
                .cloned()
                .collect();

            log::debug!(
                "Region pruning: {} regions match key range (out of {} total)",
                filtered.len(),
                all.len()
            );

            Arc::new(filtered)
        };

        if regions.is_empty() {
            return Err(DistributedError::NoRegions);
        }

        log::debug!(
            "Executing distributed scan on table '{}' across {} regions",
            table_name,
            regions.len()
        );

        // Execute scans in parallel (limited by max_parallel_shards)
        let batches = self
            .parallel_scan(table_name, &regions, columns, limit, filter_expr)
            .await?;

        Ok(batches)
    }

    /// Execute scans on multiple regions in parallel.
    async fn parallel_scan(
        &self,
        table_name: &str,
        regions: &[RegionInfo],
        columns: Vec<String>,
        limit: u32,
        filter_expr: Vec<u8>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        let mut futures = FuturesUnordered::new();
        let mut all_batches = Vec::new();

        // Spawn scan tasks for each region (up to max_parallel_shards)
        for region in regions.iter().take(self.config.max_parallel_shards) {
            let router = self.router.clone();
            let pool = self.pool.clone();
            let table = table_name.to_string();
            let cols = columns.clone();
            let store_id = region.leader_store_id;
            let region_id = region.region_id;
            let filter = filter_expr.clone();

            futures.push(async move {
                Self::scan_single_region(
                    &router, &pool, store_id, region_id, &table, cols, limit, filter,
                )
                .await
            });
        }

        // Collect results
        while let Some(result) = futures.next().await {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    log::warn!("Region scan failed: {}", e);
                    // Continue with other regions (partial results)
                }
            }
        }

        Ok(all_batches)
    }

    /// Scan a single region and consume its stream.
    async fn scan_single_region(
        router: &RegionRouter,
        pool: &ConnectionPool,
        store_id: u64,
        _region_id: u64,
        table_name: &str,
        columns: Vec<String>,
        limit: u32,
        filter_expr: Vec<u8>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        // Get store address from router
        let addr = router
            .get_store_address(store_id)
            .await
            .map_err(DistributedError::Routing)?;

        // Get client from pool
        let mut client = pool
            .get_data_access_client(&addr)
            .await
            .map_err(DistributedError::Connection)?;

        // Create request with filter
        let request = TableScanRequest {
            table_name: table_name.to_string(),
            columns,
            filter_expr,
            limit,
            snapshot_ts: 0, // Latest
            read_follower: false,
        };

        // Execute scan
        let response = client
            .table_scan(request)
            .await
            .map_err(DistributedError::Grpc)?;

        // Consume stream
        let batches = Self::consume_stream(response.into_inner()).await?;

        Ok(batches)
    }

    /// Consume a TableScan stream and decode Arrow batches.
    async fn consume_stream(
        mut stream: Streaming<TableScanResponse>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        let mut batches = Vec::new();

        loop {
            match stream.message().await {
                Ok(Some(response)) => {
                    if !response.arrow_batch.is_empty() {
                        let batch = Self::decode_arrow_ipc(&response.arrow_batch)?;
                        batches.push(batch);
                    }

                    // Check if stream is complete
                    if !response.has_more {
                        if let Some(stats) = response.stats {
                            log::debug!(
                                "Shard complete: {} rows, {} bytes, {}ms",
                                stats.rows_returned,
                                stats.bytes_read,
                                stats.scan_time_ms
                            );
                        }
                        break;
                    }
                }
                Ok(None) => break, // Stream closed
                Err(e) => return Err(DistributedError::Grpc(e)),
            }
        }

        Ok(batches)
    }

    /// Decode Arrow IPC bytes to RecordBatch.
    fn decode_arrow_ipc(data: &[u8]) -> Result<RecordBatch, DistributedError> {
        let cursor = Cursor::new(data);
        let mut reader = StreamReader::try_new(cursor, None).map_err(DistributedError::Arrow)?;

        // Read first batch (should be only one per message)
        reader
            .next()
            .ok_or_else(|| {
                DistributedError::Arrow(datafusion::arrow::error::ArrowError::InvalidArgumentError(
                    "Empty Arrow IPC stream".to_string(),
                ))
            })?
            .map_err(DistributedError::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_config_default() {
        let config = DistributedConfig::default();
        assert_eq!(config.max_parallel_shards, 16);
    }
}
