use async_trait::async_trait;
use datafusion::arrow::util::display::array_value_to_string;
use futures::stream;
use mimalloc::MiMalloc;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use spire_proto::spiredb::{
    cluster::schema_service_client::SchemaServiceClient, data::data_access_client::DataAccessClient,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Channel;

mod context;
mod exec;
mod provider;

use context::SpireContext;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    spire_common::init_logging();

    // Connect to SpireDB DataAccess
    let data_access_addr = "http://127.0.0.1:50052";
    log::info!("Connecting to SpireDB DataAccess at {}", data_access_addr);
    let channel = Channel::from_static(data_access_addr).connect().await?;
    let data_access_client = DataAccessClient::new(channel);
    log::info!("Connected to SpireDB DataAccess");

    // Connect to SpireDB PD (Schema Service)
    let pd_addr = "http://127.0.0.1:50051";
    log::info!("Connecting to SpireDB PD (SchemaService) at {}", pd_addr);
    let pd_channel = Channel::from_static(pd_addr).connect().await?;
    let schema_client = SchemaServiceClient::new(pd_channel);
    log::info!("Connected to SpireDB PD");

    let addr = "0.0.0.0:5432";
    log::info!("SpireSQL listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;

    let processor = Arc::new(SpireSqlProcessor {
        client: data_access_client,
        schema_client: schema_client,
    });

    let factory = Arc::new(SpireSqlProcessorFactory { handler: processor });

    loop {
        let (stream, _) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(stream, None, factory).await {
                log::error!("Client error: {}", e);
            }
        });
    }
}

pub struct SpireSqlProcessor {
    client: DataAccessClient<Channel>,
    schema_client: SchemaServiceClient<Channel>,
}

#[async_trait]
impl SimpleQueryHandler for SpireSqlProcessor {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Use SpireContext::new properly
        let ctx = SpireContext::new(self.client.clone(), self.schema_client.clone());
        ctx.register_tables().await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "XX000".to_string(),
                format!("Failed to register tables: {}", e),
            )))
        })?;

        let session_ctx = &ctx.session_context;

        match session_ctx.sql(query).await {
            Ok(df) => {
                let batches = df.collect().await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_string(),
                        "XX000".to_string(),
                        format!("Execution failed: {}", e),
                    )))
                })?;

                let mut rows_data = Vec::new();
                let mut schema_ref = None;

                for batch in batches {
                    if schema_ref.is_none() {
                        schema_ref = Some(batch.schema());
                    }

                    if let Some(schema) = &schema_ref {
                        let fields = schema
                            .fields()
                            .iter()
                            .map(|f| {
                                FieldInfo::new(
                                    f.name().clone(),
                                    None,
                                    None,
                                    map_arrow_type_to_pg_type(f.data_type()),
                                    FieldFormat::Text,
                                )
                            })
                            .collect::<Vec<_>>();
                        let schema_arc = Arc::new(fields);
                        let mut encoder = DataRowEncoder::new(schema_arc);

                        let num_rows = batch.num_rows();
                        for i in 0..num_rows {
                            for col in 0..batch.num_columns() {
                                let array = batch.column(col);
                                if array.is_null(i) {
                                    encoder.encode_field(&None::<String>).map_err(|e| {
                                        PgWireError::UserError(Box::new(ErrorInfo::new(
                                            "FATAL".to_string(),
                                            "XX000".to_string(),
                                            e.to_string(),
                                        )))
                                    })?;
                                } else {
                                    let val_str =
                                        array_value_to_string(array, i).unwrap_or_default();
                                    encoder.encode_field(&val_str).map_err(|e| {
                                        PgWireError::UserError(Box::new(ErrorInfo::new(
                                            "FATAL".to_string(),
                                            "XX000".to_string(),
                                            e.to_string(),
                                        )))
                                    })?;
                                }
                            }
                            rows_data.push(Ok(encoder.take_row()));
                        }
                    }
                }

                if let Some(schema) = schema_ref {
                    // Convert schema to FieldInfos for Response
                    let fields = schema
                        .fields()
                        .iter()
                        .map(|f| {
                            FieldInfo::new(
                                f.name().clone(),
                                None,
                                None,
                                map_arrow_type_to_pg_type(f.data_type()),
                                FieldFormat::Text,
                            )
                        })
                        .collect::<Vec<_>>();

                    let headers = Arc::new(fields);
                    let row_stream = stream::iter(rows_data);

                    Ok(vec![Response::Query(QueryResponse::new(
                        headers, row_stream,
                    ))])
                } else {
                    // Empty result (DDL or empty select)
                    Ok(vec![Response::Execution(Tag::new("OK"))])
                }
            }
            Err(e) => {
                // Return query error
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42000".to_string(),
                    format!("SQL Error: {}", e),
                ))));
            }
        }
    }
}

struct SpireSqlProcessorFactory {
    handler: Arc<SpireSqlProcessor>,
}

impl PgWireServerHandlers for SpireSqlProcessorFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler + Send + Sync> {
        self.handler.clone()
    }
}

fn map_arrow_type_to_pg_type(dt: &datafusion::arrow::datatypes::DataType) -> pgwire::api::Type {
    use datafusion::arrow::datatypes::DataType;
    use pgwire::api::Type;
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR, // Approximate
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::CHAR,
        DataType::UInt16 => Type::INT2,
        DataType::UInt32 => Type::INT4,
        DataType::UInt64 => Type::INT8,
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        _ => Type::UNKNOWN,
    }
}
