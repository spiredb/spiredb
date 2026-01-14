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

/// A DataFusion TableProvider that fetches data from SpireDB.
#[derive(Debug)]
pub struct SpireProvider {
    client: DataAccessClient<Channel>,
    table_name: String,
    schema: SchemaRef,
}

impl SpireProvider {
    pub fn new(client: DataAccessClient<Channel>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            client,
            table_name,
            schema,
        }
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
        use crate::exec::SpireExec;
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
