use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use spire_proto::spiredb::{
    cluster::cluster_service_client::ClusterServiceClient,
    cluster::schema_service_client::SchemaServiceClient,
    data::data_access_client::DataAccessClient,
};
use std::sync::Arc;
use tonic::transport::Channel;

use crate::cache::{new_shared_cache, SharedLruCache};
use crate::distributed::{DistributedConfig, DistributedExecutor};
use crate::pool::{ConnectionPool, PoolConfig};
use crate::provider::SpireProvider;
use crate::routing::RegionRouter;
use crate::statistics::StatisticsProvider;
use crate::topology::ClusterTopology;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use spire_proto::spiredb::cluster::{ColumnType, Empty};
use std::fmt;

use crate::config::Config;

/// Default query cache capacity.
const DEFAULT_QUERY_CACHE_CAPACITY: usize = 256;

/// Global context for SpireSQL node.
/// Holds connections, distributed execution components, and high-performance caches.
#[allow(dead_code)]
pub struct SpireContext {
    /// Client to talk to SpireDB DataAccess service.
    pub data_access: DataAccessClient<Channel>,

    /// Client to talk to SpireDB Schema Service.
    pub schema_service: SchemaServiceClient<Channel>,

    /// DataFusion Session Context for SQL execution.
    pub session_context: SessionContext,

    /// Region router for shard discovery (LRU cached).
    pub region_router: Arc<RegionRouter>,

    /// Connection pool for storage nodes.
    pub connection_pool: Arc<ConnectionPool>,

    /// Distributed query executor.
    pub distributed_executor: Arc<DistributedExecutor>,

    /// Statistics provider for cost-based optimization.
    pub stats_provider: Arc<StatisticsProvider>,

    /// Cluster topology for dynamic store discovery.
    pub topology: Arc<ClusterTopology>,

    /// LRU query cache: query_hash -> cached results.
    pub query_cache: SharedLruCache<Arc<Vec<RecordBatch>>>,

    /// Whether caching is enabled.
    pub cache_enabled: bool,
}

impl SpireContext {
    /// Create a new SpireContext with all distributed components.
    pub fn new(
        data_access: DataAccessClient<Channel>,
        schema_service: SchemaServiceClient<Channel>,
        cluster_service: ClusterServiceClient<Channel>,
        config: &Config,
    ) -> Self {
        // Create cluster topology watcher and start refresh task
        let topology = Arc::new(ClusterTopology::new(cluster_service.clone()));
        topology.clone().start_refresh_task();

        // Create region router with topology for store lookups
        let region_router = Arc::new(RegionRouter::new(cluster_service, topology.clone()));

        // Create connection pool
        let connection_pool = Arc::new(ConnectionPool::new(PoolConfig::default()));

        // Create distributed executor
        let distributed_executor = Arc::new(DistributedExecutor::new(
            region_router.clone(),
            connection_pool.clone(),
            DistributedConfig::default(),
        ));

        // Create statistics provider
        let stats_provider = Arc::new(StatisticsProvider::new(schema_service.clone()));

        // Create LRU query cache
        let cache_capacity = if config.query_cache_capacity > 0 {
            config.query_cache_capacity
        } else {
            DEFAULT_QUERY_CACHE_CAPACITY
        };
        let query_cache = new_shared_cache(cache_capacity);

        Self {
            data_access,
            schema_service,
            session_context: SessionContext::new(),
            region_router,
            connection_pool,
            distributed_executor,
            stats_provider,
            topology,
            query_cache,
            cache_enabled: config.enable_cache,
        }
    }

    /// Register all tables from SpireDB Schema Service into DataFusion SessionContext.
    pub async fn register_tables(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.schema_service.clone();
        let response = client.list_tables(Empty {}).await?;
        let table_list = response.into_inner();

        for table in table_list.tables {
            let table_name = table.name.clone();

            // Convert columns to Arrow Schema
            let mut fields = Vec::new();
            for col in table.columns {
                let dt = map_column_type(
                    ColumnType::try_from(col.r#type).unwrap_or(ColumnType::TypeBytes),
                );
                fields.push(Field::new(col.name, dt, col.nullable));
            }

            let schema = Arc::new(Schema::new(fields));

            // Get first primary key column for region pruning (default to "id")
            let pk_column = table
                .primary_key
                .first()
                .cloned()
                .unwrap_or_else(|| "id".to_string());

            // Use distributed provider for parallel multi-shard queries
            let provider = SpireProvider::with_distributed(
                self.data_access.clone(),
                table_name.clone(),
                schema,
                self.distributed_executor.clone(),
                pk_column,
                self.stats_provider.clone(),
            );

            self.session_context
                .register_table(&table_name, Arc::new(provider))?;

            // Pre-warm region cache for this table
            if let Err(e) = self.region_router.get_table_regions(&table_name).await {
                log::warn!("Failed to pre-warm region cache for {}: {}", table_name, e);
            }

            // Pre-warm statistics cache
            if let Err(e) = self.stats_provider.get_table_stats(&table_name).await {
                log::warn!("Failed to pre-warm stats for {}: {}", table_name, e);
            }
        }

        Ok(())
    }

    /// Hash a query string for cache lookup (using ahash).
    fn hash_query(query: &str) -> u64 {
        use ahash::AHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = AHasher::default();
        query.hash(&mut hasher);
        hasher.finish()
    }

    /// Retrieve cached query result (LRU cache).
    pub fn get_cached_query(&self, query: &str) -> Option<Arc<Vec<RecordBatch>>> {
        if !self.cache_enabled {
            return None;
        }
        let hash = Self::hash_query(query);
        self.query_cache.get_and_touch(hash)
    }

    /// Store query result in LRU cache.
    pub fn cache_query_result(&self, query: &str, batches: Vec<RecordBatch>) {
        if !self.cache_enabled {
            return;
        }
        let hash = Self::hash_query(query);
        self.query_cache.insert(hash, Arc::new(batches));
    }

    /// Get distributed executor for parallel shard queries.
    #[allow(dead_code)]
    pub fn executor(&self) -> &DistributedExecutor {
        &self.distributed_executor
    }

    /// Get region router for shard discovery.
    #[allow(dead_code)]
    pub fn router(&self) -> &RegionRouter {
        &self.region_router
    }

    /// Get statistics provider.
    #[allow(dead_code)]
    pub fn stats(&self) -> &StatisticsProvider {
        &self.stats_provider
    }
}

fn map_column_type(ct: ColumnType) -> DataType {
    match ct {
        ColumnType::TypeInt8 => DataType::Int8,
        ColumnType::TypeInt16 => DataType::Int16,
        ColumnType::TypeInt32 => DataType::Int32,
        ColumnType::TypeInt64 => DataType::Int64,
        ColumnType::TypeUint8 => DataType::UInt8,
        ColumnType::TypeUint16 => DataType::UInt16,
        ColumnType::TypeUint32 => DataType::UInt32,
        ColumnType::TypeUint64 => DataType::UInt64,
        ColumnType::TypeFloat32 => DataType::Float32,
        ColumnType::TypeFloat64 => DataType::Float64,
        ColumnType::TypeBool => DataType::Boolean,
        ColumnType::TypeString => DataType::Utf8,
        ColumnType::TypeBytes => DataType::Binary,
        ColumnType::TypeDate => DataType::Date32,
        ColumnType::TypeTimestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::TypeDecimal => DataType::Decimal128(38, 10),
        ColumnType::TypeList => DataType::Utf8,
        ColumnType::TypeVector => DataType::Binary,
    }
}

impl fmt::Debug for SpireContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpireContext")
            .field("data_access", &self.data_access)
            .field("schema_service", &self.schema_service)
            .field("session_context", &"SessionContext")
            .field("region_router", &"RegionRouter")
            .field("connection_pool", &"ConnectionPool")
            .field("distributed_executor", &"DistributedExecutor")
            .field("stats_provider", &"StatisticsProvider")
            .field("query_cache", &"LruCache")
            .field("cache_enabled", &self.cache_enabled)
            .finish()
    }
}
