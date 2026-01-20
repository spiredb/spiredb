use async_trait::async_trait;
use core_affinity::CoreId;
use datafusion::arrow::util::display::array_value_to_string;
use futures::stream;
use mimalloc::MiMalloc;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type as PgType};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use socket2::{Domain, Protocol, Socket, Type as SockType};
use spire_proto::spiredb::{
    cluster::cluster_service_client::ClusterServiceClient,
    cluster::schema_service_client::SchemaServiceClient,
    data::data_access_client::DataAccessClient,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Channel;

mod cache;
mod config;
mod context;
mod ddl;
mod distributed;
mod distributed_exec;
mod dml;
mod exec;
mod filter;
mod pool;
mod provider;
mod pruning;
mod routing;
mod statistics;
mod topology;

use config::{Config, load_config, print_banner};
use context::SpireContext;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration (CLI args + optional config file)
    let config = load_config();

    // Initialize logging (respect log_level from config)
    // SAFETY: Single-threaded at this point, no concurrent access to env vars
    unsafe {
        std::env::set_var("RUST_LOG", &config.log_level);
    }
    spire_common::init_logging();

    // Print banner
    print_banner();

    // Determine number of workers
    let num_workers = if config.num_workers == 0 {
        num_cpus::get()
    } else {
        config.num_workers
    };

    log::info!(
        "Starting SpireSQL with {} worker threads (thread-per-core mode)",
        num_workers
    );

    // Create shared config
    let config = Arc::new(config);

    // Start worker threads
    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let config = config.clone();

        let handle = std::thread::Builder::new()
            .name(format!("spiresql-worker-{}", worker_id))
            .spawn(move || {
                // Pin to specific core for NUMA-aware execution
                let pinned = core_affinity::set_for_current(CoreId { id: worker_id });
                if !pinned {
                    log::warn!("Worker {} failed to pin to core {}", worker_id, worker_id);
                } else {
                    log::debug!("Worker {} pinned to core {}", worker_id, worker_id);
                }

                // Create single-threaded tokio runtime for this core
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime");

                // Run the worker
                rt.block_on(run_worker(worker_id, config));
            })
            .expect("Failed to spawn worker thread");

        handles.push(handle);
    }

    // Wait for all workers (they run forever unless error)
    for handle in handles {
        if let Err(e) = handle.join() {
            log::error!("Worker thread panicked: {:?}", e);
        }
    }

    Ok(())
}

/// Run a single worker that accepts connections.
async fn run_worker(worker_id: usize, config: Arc<Config>) {
    // Create SO_REUSEPORT socket for kernel-level load balancing
    let addr: SocketAddr = config.listen_addr.parse().unwrap_or_else(|_| {
        log::error!("Invalid listen address: {}", config.listen_addr);
        "0.0.0.0:5432".parse().unwrap()
    });

    let listener = match create_reuseport_listener(&addr) {
        Ok(l) => l,
        Err(e) => {
            log::error!("Worker {} failed to bind to {}: {}", worker_id, addr, e);
            return;
        }
    };

    if worker_id == 0 {
        log::info!("SpireSQL listening on {} (SO_REUSEPORT)", addr);
        log::info!(
            "Query cache: {} (capacity: {})",
            if config.enable_cache {
                "enabled"
            } else {
                "disabled"
            },
            config.query_cache_capacity
        );
    }

    // GRPC reconnection settings for high availability
    let connect_timeout = std::time::Duration::from_secs(5);
    let request_timeout = std::time::Duration::from_secs(30);
    let keepalive_interval = std::time::Duration::from_secs(10);
    let keepalive_timeout = std::time::Duration::from_secs(20);
    let stream_window_size: u32 = 16 * 1024 * 1024;
    let connection_window_size: u32 = 32 * 1024 * 1024;

    // Connect to SpireDB Cluster (all services on same endpoint)
    let channel = match Channel::from_shared(config.cluster_addr.clone()) {
        Ok(c) => c
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .http2_keep_alive_interval(keepalive_interval)
            .keep_alive_timeout(keepalive_timeout)
            .keep_alive_while_idle(true)
            .initial_stream_window_size(stream_window_size)
            .initial_connection_window_size(connection_window_size)
            .connect_lazy(),
        Err(e) => {
            log::error!("Worker {} invalid cluster addr: {}", worker_id, e);
            return;
        }
    };
    let data_access_client = DataAccessClient::new(channel.clone());
    let schema_client = SchemaServiceClient::new(channel.clone());
    let cluster_client = ClusterServiceClient::new(channel);

    if worker_id == 0 {
        log::info!("Cluster channel configured (lazy connect, auto-reconnect)");
    }

    // Create SpireContext
    let ctx = Arc::new(SpireContext::new(
        data_access_client,
        schema_client,
        cluster_client,
        &config,
    ));

    // Register tables once at startup (only worker 0)
    if worker_id == 0
        && let Err(e) = ctx.register_tables().await
    {
        log::error!("Failed to register tables at startup: {}", e);
    }

    let processor = Arc::new(SpireSqlProcessor {
        ctx,
        query_parser: Arc::new(NoopQueryParser::new()),
    });
    let factory = Arc::new(SpireSqlProcessorFactory { handler: processor });

    // Accept loop
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                log::debug!("Worker {} accepted connection from {}", worker_id, peer);
                let factory = factory.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_socket(stream, None, factory).await {
                        log::error!("Client error: {}", e);
                    }
                });
            }
            Err(e) => {
                log::error!("Worker {} accept error: {}", worker_id, e);
            }
        }
    }
}

/// Create a TCP listener with SO_REUSEPORT for kernel-level load balancing.
fn create_reuseport_listener(addr: &SocketAddr) -> std::io::Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(*addr),
        SockType::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&(*addr).into())?;
    socket.listen(1024)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

pub struct SpireSqlProcessor {
    ctx: Arc<SpireContext>,
    query_parser: Arc<NoopQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for SpireSqlProcessor {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let ctx = &self.ctx;

        // Parse SQL to detect DDL/DML statements
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                format!("SQL parse error: {}", e),
            )))
        })?;

        // Process each statement
        for stmt in &statements {
            // Handle SET commands (PostgreSQL client connection settings)
            if let Statement::SetVariable { .. } = stmt {
                // Silently accept SET commands (ignore client settings like extra_float_digits)
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }

            // Handle SHOW commands
            if let Statement::ShowVariable { .. } = stmt {
                // Return empty result for SHOW commands
                return Ok(vec![Response::Execution(Tag::new("SHOW"))]);
            }

            // Try DDL handler first
            let mut ddl_handler = ddl::DdlHandler::new(ctx.schema_service.clone());
            if let Some(response) = ddl_handler.try_execute(stmt).await? {
                // Refresh tables after DDL
                if let Err(e) = ctx.register_tables().await {
                    log::warn!("Failed to refresh tables after DDL: {}", e);
                }
                return Ok(response);
            }

            // Try DML handler
            let mut dml_handler = dml::DmlHandler::new(ctx.data_access.clone());
            if let Some(response) = dml_handler.try_execute(stmt).await? {
                return Ok(response);
            }
        }

        // Fall through to DataFusion for SELECT and other queries

        // Check cache first (context handles hashing internally)
        if let Some(cached_batches) = ctx.get_cached_query(query) {
            log::debug!("Query cache hit for: {}", query);
            return batches_to_pgwire_response(&cached_batches);
        }

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

                // Cache the result (LRU cache with eviction)
                ctx.cache_query_result(query, batches.clone());

                batches_to_pgwire_response(&batches)
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42000".to_string(),
                format!("SQL Error: {}", e),
            )))),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for SpireSqlProcessor {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        let ctx = &self.ctx;

        // Parse SQL to detect DDL/DML statements
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                format!("SQL parse error: {}", e),
            )))
        })?;

        // Process each statement
        for stmt in &statements {
            // Handle SET commands (PostgreSQL client connection settings)
            if let Statement::SetVariable { .. } = stmt {
                return Ok(Response::Execution(Tag::new("SET")));
            }

            // Handle SHOW commands
            if let Statement::ShowVariable { .. } = stmt {
                return Ok(Response::Execution(Tag::new("SHOW")));
            }

            // Try DDL handler first
            let mut ddl_handler = ddl::DdlHandler::new(ctx.schema_service.clone());
            if let Some(response) = ddl_handler.try_execute(stmt).await? {
                // Refresh tables after DDL
                if let Err(e) = ctx.register_tables().await {
                    log::warn!("Failed to refresh tables after DDL: {}", e);
                }
                // Return first response for extended query
                return Ok(response
                    .into_iter()
                    .next()
                    .unwrap_or(Response::Execution(Tag::new("OK"))));
            }

            // Try DML handler
            let mut dml_handler = dml::DmlHandler::new(ctx.data_access.clone());
            if let Some(response) = dml_handler.try_execute(stmt).await? {
                return Ok(response
                    .into_iter()
                    .next()
                    .unwrap_or(Response::Execution(Tag::new("OK"))));
            }
        }

        // Fall through to DataFusion for SELECT
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

                // Convert to single Response
                let responses = batches_to_pgwire_response(&batches)?;
                Ok(responses
                    .into_iter()
                    .next()
                    .unwrap_or(Response::Execution(Tag::new("SELECT 0"))))
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42000".to_string(),
                format!("SQL Error: {}", e),
            )))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Parse the query to get column info
        let param_types = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(PgType::UNKNOWN))
            .collect();

        // For now, return empty column description (will be populated on execute)
        Ok(DescribeStatementResponse::new(param_types, vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // For now, return empty column description
        Ok(DescribePortalResponse::new(vec![]))
    }
}

fn batches_to_pgwire_response(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> PgWireResult<Vec<Response>> {
    let mut rows_data = Vec::new();
    let mut schema_ref = None;

    for batch in batches {
        if schema_ref.is_none() {
            schema_ref = Some(batch.schema());
        }

        let schema = batch.schema();
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

        let num_rows = batch.num_rows();
        for i in 0..num_rows {
            let mut encoder = DataRowEncoder::new(schema_arc.clone());
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
                    let val_str = array_value_to_string(array, i).unwrap_or_default();
                    encoder.encode_field(&val_str).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_string(),
                            "XX000".to_string(),
                            e.to_string(),
                        )))
                    })?;
                }
            }
            rows_data.push(encoder.take_row());
        }
    }

    if let Some(schema) = schema_ref {
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
        let row_stream = stream::iter(rows_data.into_iter().map(Ok));

        Ok(vec![Response::Query(QueryResponse::new(
            headers, row_stream,
        ))])
    } else {
        Ok(vec![Response::Execution(Tag::new("OK"))])
    }
}

struct SpireSqlProcessorFactory {
    handler: Arc<SpireSqlProcessor>,
}

impl PgWireServerHandlers for SpireSqlProcessorFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(pgwire::api::NoopHandler)
    }
}

fn map_arrow_type_to_pg_type(dt: &datafusion::arrow::datatypes::DataType) -> pgwire::api::Type {
    use datafusion::arrow::datatypes::DataType;
    use pgwire::api::Type;
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
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
