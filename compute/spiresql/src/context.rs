use datafusion::prelude::SessionContext;
use kovan_map::HashMap;
use spire_proto::spiredb::{
    cluster::schema_service_client::SchemaServiceClient, data::data_access_client::DataAccessClient,
};
use std::sync::Arc;
use tonic::transport::Channel;

use crate::provider::SpireProvider;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use spire_proto::spiredb::cluster::{ColumnType, Empty};
use std::fmt;

/// Global context for SpireSQL node.
/// Holds connections and high-performance caches.
#[allow(dead_code)]
pub struct SpireContext {
    /// Client to talk to SpireDB DataAccess service.
    pub data_access: DataAccessClient<Channel>,

    /// Client to talk to SpireDB Schema Service.
    pub schema_service: SchemaServiceClient<Channel>,

    /// DataFusion Session Context for SQL execution.
    pub session_context: SessionContext,

    /// Cache for table metadata (Schema, Location, etc.).
    /// Key: Table Name Hash (u64) - simplified for kovan map requirement
    /// Value: Schema Handle (usize)
    pub metadata_cache: Arc<HashMap<u64, usize>>,

    /// Cache for query results.
    /// Key: Query Hash (u64)
    /// Value: Result Handle (usize)
    pub query_cache: Arc<HashMap<u64, usize>>,
}

impl SpireContext {
    pub fn new(
        data_access: DataAccessClient<Channel>,
        schema_service: SchemaServiceClient<Channel>,
    ) -> Self {
        Self {
            data_access,
            schema_service,
            session_context: SessionContext::new(),
            metadata_cache: Arc::new(HashMap::new()),
            query_cache: Arc::new(HashMap::new()),
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

            let provider = SpireProvider::new(self.data_access.clone(), table_name.clone(), schema);

            self.session_context
                .register_table(&table_name, Arc::new(provider))?;
        }

        Ok(())
    }

    /// Retrieve cached query result handle if available.
    pub fn get_cached_query(&self, query_hash: u64) -> Option<usize> {
        self.query_cache.get(&query_hash)
    }

    /// Store query result handle in cache.
    pub fn cache_query_result(&self, query_hash: u64, result_handle: usize) {
        self.query_cache.insert(query_hash, result_handle);
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
        ColumnType::TypeTimestamp => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        ColumnType::TypeDecimal => DataType::Decimal128(38, 10), // Default precision/scale, should come from ColDef
        ColumnType::TypeList => DataType::Utf8,                  // Simplified for now
        ColumnType::TypeVector => DataType::Binary,              // Vector as binary for now
    }
}

impl fmt::Debug for SpireContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpireContext")
            .field("data_access", &self.data_access)
            .field("schema_service", &self.schema_service)
            .field("session_context", &"SessionContext")
            .field("metadata_cache", &"HashMap<u64, usize>")
            .field("query_cache", &"HashMap<u64, usize>")
            .finish()
    }
}
