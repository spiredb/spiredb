use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

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

/// A DataFusion TableProvider that fetches data from SpireDB.
///
/// When a DistributedExecutor is provided, queries use parallel multi-shard
/// execution. Otherwise, queries go to a single node.
#[derive(Debug)]
pub struct SpireProvider {
    client: DataAccessClient<Channel>,
    table_name: String,
    schema: SchemaRef,
    /// Optional distributed executor for parallel shard queries.
    executor: Option<Arc<DistributedExecutor>>,
    /// Primary key column name for region pruning.
    pk_column: String,
}

impl SpireProvider {
    /// Create provider with single-node execution (legacy mode).
    #[allow(dead_code)]
    pub fn new(client: DataAccessClient<Channel>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            client,
            table_name,
            schema,
            executor: None,
            pk_column: "id".to_string(),
        }
    }

    /// Create provider with distributed execution for parallel shard queries.
    pub fn with_distributed(
        client: DataAccessClient<Channel>,
        table_name: String,
        schema: SchemaRef,
        executor: Arc<DistributedExecutor>,
        pk_column: String,
    ) -> Self {
        Self {
            client,
            table_name,
            schema,
            executor: Some(executor),
            pk_column,
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
