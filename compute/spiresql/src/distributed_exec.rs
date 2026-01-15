//! Distributed Execution Plan
//!
//! A DataFusion ExecutionPlan that uses DistributedExecutor for parallel
//! multi-shard query execution with predicate-based region pruning.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use futures::stream::Stream;
use std::fmt;

use crate::distributed::DistributedExecutor;
use crate::filter::serialize_filter;
use crate::pruning::{KeyBounds, extract_key_bounds};

/// Distributed execution plan that queries multiple shards in parallel.
#[derive(Debug)]
pub struct DistributedSpireExec {
    executor: Arc<DistributedExecutor>,
    table_name: String,
    schema: SchemaRef,
    columns: Vec<String>,
    limit: u32,
    /// Key bounds for region pruning (extracted from filters).
    key_bounds: KeyBounds,
    /// Serialized filter expression for pushdown.
    filter_expr: Vec<u8>,
    properties: PlanProperties,
}

impl DistributedSpireExec {
    /// Create a new distributed execution plan.
    ///
    /// # Arguments
    /// * `executor` - Distributed executor for parallel shard queries
    /// * `table_name` - Name of the table to scan
    /// * `schema` - Table schema
    /// * `projection` - Column indices to project (None = all columns)
    /// * `filters` - WHERE clause filters for region pruning
    /// * `pk_column` - Primary key column name for predicate-based pruning
    /// * `limit` - Max rows to return per shard
    pub fn new(
        executor: Arc<DistributedExecutor>,
        table_name: String,
        schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        pk_column: &str,
        limit: Option<usize>,
    ) -> Self {
        // Resolve column names from projection
        let columns = if let Some(proj) = projection {
            proj.iter()
                .map(|&idx| schema.field(idx).name().clone())
                .collect()
        } else {
            // All columns
            schema.fields().iter().map(|f| f.name().clone()).collect()
        };

        // Extract key bounds from filters for region pruning
        let key_bounds = extract_key_bounds(filters, pk_column);
        if key_bounds.is_bounded() {
            log::debug!(
                "Region pruning enabled for table '{}': start={:?}, end={:?}",
                table_name,
                key_bounds.start_key.as_ref().map(|k| k.len()),
                key_bounds.end_key.as_ref().map(|k| k.len())
            );
        }

        // Serialize filters for pushdown to storage
        let filter_expr = serialize_filter(filters);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final, // Batches are collected then returned
            Boundedness::Bounded,
        );

        Self {
            executor,
            table_name,
            schema,
            columns,
            limit: limit.unwrap_or(0) as u32,
            key_bounds,
            filter_expr,
            properties,
        }
    }

    /// Create without pruning (for backwards compatibility).
    #[allow(dead_code)]
    pub fn new_without_pruning(
        executor: Arc<DistributedExecutor>,
        table_name: String,
        schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        Self::new(executor, table_name, schema, projection, &[], "id", limit)
    }
}

impl DisplayAs for DistributedSpireExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSpireExec: table={}, columns={}, limit={}, pruning={}",
            self.table_name,
            self.columns.len(),
            self.limit,
            self.key_bounds.is_bounded()
        )
    }
}

impl ExecutionPlan for DistributedSpireExec {
    fn name(&self) -> &str {
        "DistributedSpireExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = DistributedStream::new(
            self.executor.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            self.columns.clone(),
            self.limit,
            self.key_bounds.clone(),
            self.filter_expr.clone(),
        );
        Ok(Box::pin(stream))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

/// Stream wrapper for distributed execution results.
pub struct DistributedStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl DistributedStream {
    pub fn new(
        executor: Arc<DistributedExecutor>,
        table_name: String,
        schema: SchemaRef,
        columns: Vec<String>,
        limit: u32,
        key_bounds: KeyBounds,
        filter_expr: Vec<u8>,
    ) -> Self {
        let schema_captured = schema.clone();

        let stream = async_stream::try_stream! {
            log::debug!(
                "Starting distributed scan on table '{}' with {} columns, limit {}, filter_len={}",
                table_name,
                columns.len(),
                limit,
                filter_expr.len()
            );

            // Use pruning if bounds are available
            let start_key = key_bounds.start_key.as_deref().unwrap_or(&[]);
            let end_key = key_bounds.end_key.as_deref().unwrap_or(&[]);

            let batches = executor
                .table_scan_with_bounds(&table_name, columns, limit, start_key, end_key, filter_expr)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            log::debug!(
                "Distributed scan complete: {} batches from table '{}'",
                batches.len(),
                table_name
            );

            for batch in batches {
                yield batch;
            }
        };

        Self {
            schema: schema_captured,
            inner: Box::pin(stream),
        }
    }
}

impl Stream for DistributedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for DistributedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
