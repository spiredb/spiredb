//! Distributed Execution
//!
//! Parallel multi-shard query execution with stream handling.
//! Queries multiple regions concurrently and merges results.

use crate::pool::ConnectionPool;
use crate::routing::{RegionInfo, RegionRouter};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};
use spire_proto::spiredb::data::{TableScanRequest, TableScanResponse};
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
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        // No pruning - scan all regions (no filter)
        self.table_scan_with_bounds(table_name, columns, limit, &[], &[], Vec::new(), schema)
            .await
    }

    /// Execute a table scan with region pruning based on key bounds.
    ///
    /// Only scans regions that may contain keys in the [start_key, end_key) range.
    #[allow(clippy::too_many_arguments)]
    pub async fn table_scan_with_bounds(
        &self,
        table_name: &str,
        columns: Vec<String>,
        limit: u32,
        start_key: &[u8],
        end_key: &[u8],
        filter_expr: Vec<u8>,
        schema: &SchemaRef,
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
        // Execute scans in parallel (limited by max_parallel_shards)
        let batches = self
            .parallel_scan(table_name, &regions, columns, limit, filter_expr, schema)
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
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        let concurrency = self.config.max_parallel_shards.max(1);

        log::info!(
            "Scanning table '{}' with {} regions",
            table_name,
            regions.len()
        );
        for (i, r) in regions.iter().enumerate() {
            log::info!(
                "Region {}: ID={}, Start='{:?}', End='{:?}'",
                i,
                r.region_id,
                r.start_key,
                r.end_key
            );
        }

        // Pre-create futures to avoid HRTB lifetime issues with stream::iter+closure
        let tasks: Vec<_> = regions
            .iter()
            .map(|region| {
                let router = self.router.clone();
                let pool = self.pool.clone();
                let table = table_name.to_string();
                let cols = columns.clone();
                let store_id = region.leader_store_id;
                let region_id = region.region_id;
                let filter = filter_expr.clone();
                let schema = schema.clone();
                let start_key = region.start_key.clone();
                let end_key = region.end_key.clone();

                async move {
                    Self::scan_single_region(
                        &router, &pool, store_id, region_id, &table, cols, limit, filter, &schema,
                        start_key, end_key,
                    )
                    .await
                }
            })
            .collect();

        // Use buffer_unordered to limit concurrency
        let mut stream = stream::iter(tasks).buffer_unordered(concurrency);

        let mut all_batches = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    log::warn!("Region scan failed: {}", e);
                    // Continue with other regions (best effort)
                }
            }
        }

        Ok(all_batches)
    }

    /// Scan a single region and consume its stream.
    #[allow(clippy::too_many_arguments)]
    async fn scan_single_region(
        router: &RegionRouter,
        pool: &ConnectionPool,
        store_id: u64,
        _region_id: u64,
        table_name: &str,
        columns: Vec<String>,
        limit: u32,
        filter_expr: Vec<u8>,
        schema: &SchemaRef,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        // Get store address from router (sync - uses topology)
        let addr = router
            .get_store_address(store_id)
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
            start_key,
            end_key,
        };

        // Execute scan
        let response = client
            .table_scan(request)
            .await
            .map_err(DistributedError::Grpc)?;

        // Consume stream
        let batches = Self::consume_stream(response.into_inner(), Some(schema)).await?;

        Ok(batches)
    }

    /// Consume a TableScan stream and decode batches.
    async fn consume_stream(
        mut stream: Streaming<TableScanResponse>,
        schema: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>, DistributedError> {
        let mut batches = Vec::new();

        loop {
            match stream.message().await {
                Ok(Some(response)) => {
                    if !response.arrow_batch.is_empty() {
                        // Only decode if we have a schema
                        if let Some(s) = schema {
                            match Self::decode_spire_batch(&response.arrow_batch, s) {
                                Ok(batch) => batches.push(batch),
                                Err(e) => log::warn!("Failed to decode batch: {}", e),
                            }
                        }
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

    /// Decode SpireDB custom binary format to RecordBatch.
    ///
    /// SpireDB format: count:4B + [key_len:4B + key + val_len:4B + val]...
    /// The `val` is erlang term_to_binary encoded map.
    pub(crate) fn decode_spire_batch(
        data: &[u8],
        schema: &datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<RecordBatch, DistributedError> {
        use datafusion::arrow::array::{
            BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder,
            StringBuilder,
        };
        use datafusion::arrow::datatypes::DataType;
        use std::collections::HashMap;

        if data.len() < 4 {
            return Err(DistributedError::Arrow(
                datafusion::arrow::error::ArrowError::InvalidArgumentError(
                    "Data too short for batch header".to_string(),
                ),
            ));
        }

        // Read count (4 bytes big-endian)
        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut offset = 4;

        // Parse key-value pairs
        let mut rows: Vec<HashMap<String, Vec<u8>>> = Vec::with_capacity(count);

        for _ in 0..count {
            if offset + 4 > data.len() {
                break;
            }

            // Key length
            let key_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > data.len() {
                break;
            }

            // Skip key (we use the decoded value which contains all columns)
            offset += key_len;

            if offset + 4 > data.len() {
                break;
            }

            // Value length
            let val_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + val_len > data.len() {
                break;
            }

            // Value bytes (erlang term_to_binary encoded)
            let val_bytes = &data[offset..offset + val_len];
            offset += val_len;

            // Try to decode Erlang term to extract column values
            // For now, store as raw bytes and decode per-column
            let mut row_map = HashMap::new();
            if let Ok(row) = decode_erlang_row(val_bytes) {
                row_map = row;
            }
            rows.push(row_map);
        }

        // Build Arrow arrays from parsed rows
        let mut arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();

        for field in schema.fields() {
            let col_name = field.name();

            match field.data_type() {
                DataType::Int32 => {
                    let mut builder = Int32Builder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name)
                            && let Ok(s) = String::from_utf8(val.clone())
                            && let Ok(n) = s.parse::<i32>()
                        {
                            builder.append_value(n);
                            continue;
                        }
                        builder.append_null();
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name)
                            && let Ok(s) = String::from_utf8(val.clone())
                            && let Ok(n) = s.parse::<i64>()
                        {
                            builder.append_value(n);
                            continue;
                        }
                        builder.append_null();
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name)
                            && let Ok(s) = String::from_utf8(val.clone())
                            && let Ok(n) = s.parse::<f64>()
                        {
                            builder.append_value(n);
                            continue;
                        }
                        builder.append_null();
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name)
                            && let Ok(s) = String::from_utf8(val.clone())
                        {
                            builder.append_value(s == "true" || s == "1");
                            continue;
                        }
                        builder.append_null();
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Binary => {
                    let mut builder = BinaryBuilder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name) {
                            builder.append_value(val);
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                _ => {
                    // Default: treat as string
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        if let Some(val) = row.get(col_name) {
                            if let Ok(s) = String::from_utf8(val.clone()) {
                                builder.append_value(&s);
                            } else {
                                builder.append_value(format!("{:?}", val));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        RecordBatch::try_new(schema.clone(), arrays).map_err(DistributedError::Arrow)
    }
}

/// Decode Erlang term_to_binary encoded row map.
/// Returns HashMap of column_name -> raw bytes.
fn decode_erlang_row(data: &[u8]) -> Result<std::collections::HashMap<String, Vec<u8>>, ()> {
    use std::collections::HashMap;

    // Erlang External Term Format magic number
    if data.is_empty() || data[0] != 131 {
        return Err(());
    }

    let mut result = HashMap::new();

    // Try to decode as map (131, 116, arity:4, pairs...)
    if data.len() < 6 || data[1] != 116 {
        return Err(());
    }

    let arity = u32::from_be_bytes([data[2], data[3], data[4], data[5]]) as usize;
    let mut offset = 6;

    for _ in 0..arity {
        // Decode key (should be atom or binary)
        let (key, new_offset) = decode_erlang_term(&data[offset..])?;
        offset += new_offset;

        // Decode value
        let (value, new_offset) = decode_erlang_value(&data[offset..])?;
        offset += new_offset;

        result.insert(key, value);
    }

    Ok(result)
}

/// Decode an Erlang term as string key.
fn decode_erlang_term(data: &[u8]) -> Result<(String, usize), ()> {
    if data.is_empty() {
        return Err(());
    }

    match data[0] {
        // Small atom (100)
        100 => {
            if data.len() < 3 {
                return Err(());
            }
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            if data.len() < 3 + len {
                return Err(());
            }
            let s = String::from_utf8_lossy(&data[3..3 + len]).to_string();
            Ok((s, 3 + len))
        }
        // Atom ext (118) - UTF-8 atom
        118 => {
            if data.len() < 3 {
                return Err(());
            }
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            if data.len() < 3 + len {
                return Err(());
            }
            let s = String::from_utf8_lossy(&data[3..3 + len]).to_string();
            Ok((s, 3 + len))
        }
        // Small atom UTF-8 (119)
        119 => {
            if data.len() < 2 {
                return Err(());
            }
            let len = data[1] as usize;
            if data.len() < 2 + len {
                return Err(());
            }
            let s = String::from_utf8_lossy(&data[2..2 + len]).to_string();
            Ok((s, 2 + len))
        }
        // Binary (109)
        109 => {
            if data.len() < 5 {
                return Err(());
            }
            let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err(());
            }
            let s = String::from_utf8_lossy(&data[5..5 + len]).to_string();
            Ok((s, 5 + len))
        }
        _ => Err(()),
    }
}

/// Decode an Erlang term as raw bytes value.
fn decode_erlang_value(data: &[u8]) -> Result<(Vec<u8>, usize), ()> {
    if data.is_empty() {
        return Err(());
    }

    match data[0] {
        // Small integer (97)
        97 => {
            if data.len() < 2 {
                return Err(());
            }
            Ok((data[1].to_string().into_bytes(), 2))
        }
        // Integer (98)
        98 => {
            if data.len() < 5 {
                return Err(());
            }
            let n = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
            Ok((n.to_string().into_bytes(), 5))
        }
        // Binary (109)
        109 => {
            if data.len() < 5 {
                return Err(());
            }
            let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err(());
            }
            Ok((data[5..5 + len].to_vec(), 5 + len))
        }
        // Atom (100) - treat as string
        100 => {
            if data.len() < 3 {
                return Err(());
            }
            let len = u16::from_be_bytes([data[1], data[2]]) as usize;
            if data.len() < 3 + len {
                return Err(());
            }
            Ok((data[3..3 + len].to_vec(), 3 + len))
        }
        // Small atom UTF-8 (119)
        119 => {
            if data.len() < 2 {
                return Err(());
            }
            let len = data[1] as usize;
            if data.len() < 2 + len {
                return Err(());
            }
            Ok((data[2..2 + len].to_vec(), 2 + len))
        }
        // Nil (106) - empty
        106 => Ok((vec![], 1)),
        // Float (70) - new float format
        70 => {
            if data.len() < 9 {
                return Err(());
            }
            let bytes: [u8; 8] = data[1..9].try_into().map_err(|_| ())?;
            let f = f64::from_be_bytes(bytes);
            Ok((f.to_string().into_bytes(), 9))
        }
        _ => {
            // Unknown type - skip by returning empty
            Ok((vec![], 1))
        }
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
