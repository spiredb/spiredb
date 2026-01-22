//! Table Provider implementation
//!
//! This module implements the `TableProvider` trait, allowing the SQL engine to query
//! SpireDB tables. It handles schema inference, statistics provision, and
//! pushing down filters to the storage layer.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

use super::distributed::DistributedExecutor;
use super::distributed_exec::DistributedSpireExec;
use super::exec::SpireExec;
use super::pool::ConnectionPool;
use super::pruning::{KeyBounds, extract_key_bounds};
use super::routing::RegionRouter;
use super::statistics::StatisticsProvider;
use super::topology::ClusterTopology;
use datafusion::catalog::Session;

/// A DataFusion TableProvider that fetches data from SpireDB.
///
/// Uses DistributedSpireExec for multi-shard queries or SpireExec for
/// single-shard queries when filters narrow to one region.
pub struct SpireProvider {
    table_name: String,
    schema: SchemaRef,
    /// Distributed executor for parallel shard queries.
    executor: Arc<DistributedExecutor>,
    /// Primary key column name for region pruning.
    pk_column: String,
    /// Statistics provider for cost-based optimization.
    stats_provider: Arc<StatisticsProvider>,
    /// Connection pool for single-shard direct access.
    connection_pool: Arc<ConnectionPool>,
    /// Region router for looking up region info.
    region_router: Arc<RegionRouter>,
    /// Cluster topology for resolving store addresses.
    cluster_topology: Arc<ClusterTopology>,
}

impl std::fmt::Debug for SpireProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpireProvider")
            .field("table_name", &self.table_name)
            .field("pk_column", &self.pk_column)
            .finish()
    }
}

impl SpireProvider {
    /// Create provider with distributed execution and single-shard fallback.
    #[allow(clippy::too_many_arguments)]
    pub fn with_distributed(
        table_name: String,
        schema: SchemaRef,
        executor: Arc<DistributedExecutor>,
        pk_column: String,
        stats_provider: Arc<StatisticsProvider>,
        connection_pool: Arc<ConnectionPool>,
        region_router: Arc<RegionRouter>,
        cluster_topology: Arc<ClusterTopology>,
    ) -> Self {
        Self {
            table_name,
            schema,
            executor,
            pk_column,
            stats_provider,
            connection_pool,
            region_router,
            cluster_topology,
        }
    }

    /// Check if distributed execution is enabled.
    #[allow(dead_code)]
    pub fn is_distributed(&self) -> bool {
        true
    }

    /// Find regions that match the given key bounds.
    async fn get_matching_regions(
        &self,
        key_bounds: &KeyBounds,
    ) -> Vec<super::routing::RegionInfo> {
        let regions_arc = match self.region_router.get_table_regions(&self.table_name).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("Failed to get regions for {}: {}", self.table_name, e);
                return vec![];
            }
        };

        // Clone out of Arc to get owned Vec
        let regions = (*regions_arc).clone();

        if !key_bounds.is_bounded() {
            return regions;
        }

        // Filter regions by key bounds
        regions
            .into_iter()
            .filter(|r| {
                let matches_start = key_bounds.start_key.as_ref().is_none_or(|start| {
                    r.end_key.is_empty() || r.end_key.as_slice() > start.as_slice()
                });
                let matches_end = key_bounds.end_key.as_ref().is_none_or(|end| {
                    r.start_key.is_empty() || r.start_key.as_slice() < end.as_slice()
                });
                matches_start && matches_end
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for SpireProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Return table statistics for cost-based query optimization.
    ///
    /// DataFusion uses these statistics for:
    /// - Join ordering (smaller tables first)
    /// - Filter selectivity estimation
    /// - Cardinality estimation for aggregations
    fn statistics(&self) -> Option<Statistics> {
        // Try to get cached statistics
        if let Some(cached) = self.stats_provider.get_cached_stats(&self.table_name) {
            log::debug!(
                "Using cached statistics for '{}': {} rows, {} bytes",
                self.table_name,
                cached.row_count,
                cached.size_bytes
            );

            // Convert to DataFusion Statistics
            let column_statistics: Vec<datafusion::common::ColumnStatistics> = self
                .schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(col_stats) = cached.column_stats.get(field.name()) {
                        datafusion::common::ColumnStatistics {
                            null_count: Precision::Exact(col_stats.null_count as usize),
                            distinct_count: Precision::Exact(col_stats.distinct_count as usize),
                            min_value: col_stats
                                .min_value
                                .clone()
                                .map(Precision::Exact)
                                .unwrap_or(Precision::Absent),
                            max_value: col_stats
                                .max_value
                                .clone()
                                .map(Precision::Exact)
                                .unwrap_or(Precision::Absent),
                            sum_value: Precision::Absent, // TODO: Add sum_value to SpireDB ColumnStats proto
                            byte_size: Precision::Absent, // TODO: Add byte_size to SpireDB ColumnStats proto
                        }
                    } else {
                        datafusion::common::ColumnStatistics::new_unknown()
                    }
                })
                .collect();

            return Some(Statistics {
                num_rows: Precision::Exact(cached.row_count as usize),
                total_byte_size: Precision::Exact(cached.size_bytes as usize),
                column_statistics,
            });
        }

        // No cached stats available
        log::debug!("No cached statistics for table '{}'", self.table_name);
        None
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract key bounds from filters to determine region targeting
        let key_bounds = extract_key_bounds(filters, &self.pk_column);
        let matching_regions = self.get_matching_regions(&key_bounds).await;

        // If query targets exactly one region, use direct SpireExec for streaming
        if matching_regions.len() == 1 {
            let region = &matching_regions[0];
            let leader_id = region.leader_store_id;

            if let Some(addr) = self.cluster_topology.get_store_address(leader_id) {
                match self.connection_pool.get_data_access_client(&addr).await {
                    Ok(client) => {
                        log::debug!(
                            "Using single-shard SpireExec for table '{}' (region {}, leader {})",
                            self.table_name,
                            region.region_id,
                            leader_id
                        );
                        return Ok(Arc::new(SpireExec::new(
                            client,
                            self.table_name.clone(),
                            self.schema.clone(),
                            projection.cloned(),
                            filters.to_vec(),
                            limit,
                        )));
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to get client for leader {}: {}, falling back to distributed",
                            leader_id,
                            e
                        );
                    }
                }
            }
        }

        // Default: use distributed execution for multi-shard queries
        log::debug!(
            "Using distributed execution for table '{}' ({} regions, {} filters)",
            self.table_name,
            matching_regions.len(),
            filters.len()
        );
        Ok(Arc::new(DistributedSpireExec::new(
            self.executor.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            projection,
            filters,
            &self.pk_column,
            limit,
        )))
    }
}
