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

use datafusion::catalog::Session;
use spire_proto::spiredb::data::data_access_client::DataAccessClient;
use tonic::transport::Channel;

use crate::distributed::DistributedExecutor;
use crate::distributed_exec::DistributedSpireExec;
use crate::exec::SpireExec;
use crate::statistics::StatisticsProvider;

/// A DataFusion TableProvider that fetches data from SpireDB.
///
/// When a DistributedExecutor is provided, queries use parallel multi-shard
/// execution. Otherwise, queries go to a single node.
pub struct SpireProvider {
    client: DataAccessClient<Channel>,
    table_name: String,
    schema: SchemaRef,
    /// Optional distributed executor for parallel shard queries.
    executor: Option<Arc<DistributedExecutor>>,
    /// Primary key column name for region pruning.
    pk_column: String,
    /// Statistics provider for cost-based optimization.
    stats_provider: Arc<StatisticsProvider>,
}

impl std::fmt::Debug for SpireProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpireProvider")
            .field("table_name", &self.table_name)
            .field("pk_column", &self.pk_column)
            .field("executor", &self.executor.is_some())
            .finish()
    }
}

impl SpireProvider {
    /// Create provider with single-node execution (legacy mode).
    #[allow(dead_code)]
    pub fn new(
        client: DataAccessClient<Channel>,
        table_name: String,
        schema: SchemaRef,
        stats_provider: Arc<StatisticsProvider>,
    ) -> Self {
        Self {
            client,
            table_name,
            schema,
            executor: None,
            pk_column: "id".to_string(),
            stats_provider,
        }
    }

    /// Create provider with distributed execution for parallel shard queries.
    pub fn with_distributed(
        client: DataAccessClient<Channel>,
        table_name: String,
        schema: SchemaRef,
        executor: Arc<DistributedExecutor>,
        pk_column: String,
        stats_provider: Arc<StatisticsProvider>,
    ) -> Self {
        Self {
            client,
            table_name,
            schema,
            executor: Some(executor),
            pk_column,
            stats_provider,
        }
    }

    /// Check if distributed execution is enabled.
    #[allow(dead_code)]
    pub fn is_distributed(&self) -> bool {
        self.executor.is_some()
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use distributed execution if executor is available
        if let Some(ref executor) = self.executor {
            log::debug!(
                "Using distributed execution for table '{}' with {} filters, pk='{}'",
                self.table_name,
                _filters.len(),
                self.pk_column
            );
            return Ok(Arc::new(DistributedSpireExec::new(
                executor.clone(),
                self.table_name.clone(),
                self.schema.clone(),
                _projection,
                _filters,
                &self.pk_column,
                _limit,
            )));
        }

        // Fallback to single-node execution
        log::debug!(
            "Using single-node execution for table '{}'",
            self.table_name
        );
        Ok(Arc::new(SpireExec::new(
            self.client.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            _projection.cloned(),
            _filters.to_vec(),
            _limit,
        )))
    }
}
