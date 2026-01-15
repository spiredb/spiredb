use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use futures::stream::Stream;
use spire_proto::spiredb::data::TableScanRequest;
use spire_proto::spiredb::data::data_access_client::DataAccessClient;
use tonic::transport::Channel;

use std::fmt;

use datafusion::prelude::Expr;

/// Execution plan for scanning a SpireDB table.
#[derive(Debug)]
pub struct SpireExec {
    client: DataAccessClient<Channel>,
    table_name: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl SpireExec {
    pub fn new(
        client: DataAccessClient<Channel>,
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            client,
            table_name,
            schema,
            projection,
            filters,
            limit,
            properties,
        }
    }
}

impl DisplayAs for SpireExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpireExec: table={}, filters={:?}, limit={:?}",
            self.table_name, self.filters, self.limit
        )
    }
}

impl ExecutionPlan for SpireExec {
    fn name(&self) -> &str {
        "SpireExec"
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
        let stream = SpireStream::new(
            self.client.clone(),
            self.table_name.clone(),
            self.schema.clone(),
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
        );
        Ok(Box::pin(stream))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

/// Stream that reads from SpireDB GRPC and yields RecordBatches.
pub struct SpireStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl SpireStream {
    pub fn new(
        client: DataAccessClient<Channel>,
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let schema_captured = schema.clone();
        let stream = async_stream::try_stream! {
            let mut client = client.clone();

            let columns = if let Some(proj) = &projection {
                proj.iter()
                    .map(|&idx| schema_captured.field(idx).name().clone())
                    .collect()
            } else {
                vec![]
            };

            // NOTE: Filter pushdown is plumbed here but not yet supported by the SpireDB backend.
            // Attempts to pass filters will result in them being ignored or logged server-side.
            // We log them here for visibility.
            let filter_expr = if !filters.is_empty() {
                log::warn!("Filter pushdown ignored (backend support pending): {:?}", filters);
                vec![]
            } else {
                vec![]
            };

            let req = TableScanRequest {
                table_name: table_name.clone(),
                columns,
                filter_expr,
                limit: limit.unwrap_or(0) as u32,
                snapshot_ts: 0, // Latest
                read_follower: false,
            };

            log::debug!("Sending TableScanRequest: {:?}", req);

            let mut response_stream = client
                .table_scan(req)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_inner();

            while let Some(resp) = response_stream.next().await {
                let resp = resp.map_err(|e| DataFusionError::External(Box::new(e)))?;
                if !resp.arrow_batch.is_empty() {
                    // Elixir backend uses Explorer.DataFrame.dump_ipc_stream(), which produces
                    // a complete Arrow IPC stream (Schema + Batches) for each response batch.
                    let cursor = std::io::Cursor::new(resp.arrow_batch);
                    let reader = StreamReader::try_new(cursor, None)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                    for batch_result in reader {
                        let batch = batch_result.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        yield batch;
                    }
                }
            }
        };

        Self {
            schema,
            inner: Box::pin(stream),
        }
    }
}

impl Stream for SpireStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for SpireStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
