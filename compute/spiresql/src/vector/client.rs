//! Vector service client.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tonic::transport::Channel;

use spire_proto::spiredb::data::{
    BatchVectorSearchRequest, VectorDeleteRequest, VectorIndexCreateRequest,
    VectorIndexDropRequest, VectorInsertRequest, VectorSearchRequest,
    vector_service_client::VectorServiceClient,
};

use super::config::VectorConfig;
use super::error::{VectorError, VectorResult};
use super::types::{IndexParams, SearchOptions, VectorResult as VResult};

/// Trait for vector operations (enables mocking)
#[async_trait]
pub trait VectorService: Send + Sync {
    /// Create a vector index
    async fn create_index(&self, params: IndexParams) -> VectorResult<()>;

    /// Drop a vector index
    async fn drop_index(&self, name: &str) -> VectorResult<()>;

    /// Insert a vector with optional payload
    async fn insert(
        &self,
        index: &str,
        doc_id: &[u8],
        vector: &[f32],
        payload: Option<&[u8]>,
    ) -> VectorResult<u64>;

    /// Delete a vector by document ID
    async fn delete(&self, index: &str, doc_id: &[u8]) -> VectorResult<()>;

    /// Search for k nearest neighbors
    async fn search(
        &self,
        index: &str,
        query: &[f32],
        opts: SearchOptions,
    ) -> VectorResult<Vec<VResult>>;

    /// Batch search multiple queries
    async fn batch_search(
        &self,
        index: &str,
        queries: &[Vec<f32>],
        opts: SearchOptions,
    ) -> VectorResult<Vec<Vec<VResult>>>;
}

/// gRPC vector client for SpireDB
pub struct SpireVector {
    inner: VectorServiceClient<Channel>,
}

impl SpireVector {
    /// Connect to SpireDB vector service
    pub async fn connect(config: VectorConfig) -> VectorResult<Self> {
        let channel = Channel::from_shared(config.endpoint.clone())
            .map_err(|e| VectorError::Connection(e.to_string()))?
            .timeout(config.timeout)
            .connect()
            .await
            .map_err(|e| VectorError::Connection(e.to_string()))?;

        Ok(Self {
            inner: VectorServiceClient::new(channel),
        })
    }

    /// Connect with endpoint string
    pub async fn from_endpoint(endpoint: &str) -> VectorResult<Self> {
        Self::connect(VectorConfig::new(endpoint)).await
    }

    /// Create a builder
    pub fn builder() -> SpireVectorBuilder {
        SpireVectorBuilder::default()
    }
}

#[async_trait]
impl VectorService for SpireVector {
    async fn create_index(&self, params: IndexParams) -> VectorResult<()> {
        let mut proto_params = HashMap::new();
        proto_params.insert("dimensions".to_string(), params.dimensions.to_string());
        proto_params.insert("shards".to_string(), params.shards.to_string());

        let request = VectorIndexCreateRequest {
            name: params.name,
            table_name: params.table_name,
            column_name: params.column_name,
            algorithm: params.algorithm.as_str().to_string(),
            params: proto_params,
        };

        self.inner
            .clone()
            .create_index(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::AlreadyExists => {
                    VectorError::IndexAlreadyExists(e.message().to_string())
                }
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        Ok(())
    }

    async fn drop_index(&self, name: &str) -> VectorResult<()> {
        let request = VectorIndexDropRequest {
            name: name.to_string(),
        };

        self.inner
            .clone()
            .drop_index(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => VectorError::IndexNotFound(name.to_string()),
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        Ok(())
    }

    async fn insert(
        &self,
        index: &str,
        doc_id: &[u8],
        vector: &[f32],
        payload: Option<&[u8]>,
    ) -> VectorResult<u64> {
        let vector_bytes: Vec<u8> = vector.iter().flat_map(|f| f.to_ne_bytes()).collect();

        let request = VectorInsertRequest {
            index_name: index.to_string(),
            doc_id: doc_id.to_vec(),
            vector: vector_bytes,
            payload: payload.map(|p| p.to_vec()).unwrap_or_default(),
        };

        let response = self
            .inner
            .clone()
            .insert(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => VectorError::IndexNotFound(index.to_string()),
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        Ok(response.into_inner().internal_id)
    }

    async fn delete(&self, index: &str, doc_id: &[u8]) -> VectorResult<()> {
        let request = VectorDeleteRequest {
            index_name: index.to_string(),
            doc_id: doc_id.to_vec(),
        };

        self.inner
            .clone()
            .delete(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => VectorError::IndexNotFound(index.to_string()),
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        Ok(())
    }

    async fn search(
        &self,
        index: &str,
        query: &[f32],
        opts: SearchOptions,
    ) -> VectorResult<Vec<VResult>> {
        let query_bytes: Vec<u8> = query.iter().flat_map(|f| f.to_ne_bytes()).collect();

        let request = VectorSearchRequest {
            index_name: index.to_string(),
            query_vector: query_bytes,
            k: opts.k,
            radius: opts.radius,
            filter: opts.filter.unwrap_or_default(),
            return_payload: opts.return_payload,
        };

        let response = self
            .inner
            .clone()
            .search(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => VectorError::IndexNotFound(index.to_string()),
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        let results = response
            .into_inner()
            .results
            .into_iter()
            .map(|r| VResult {
                id: r.id,
                distance: r.distance,
                payload: if r.payload.is_empty() {
                    None
                } else {
                    Some(r.payload)
                },
            })
            .collect();

        Ok(results)
    }

    async fn batch_search(
        &self,
        index: &str,
        queries: &[Vec<f32>],
        opts: SearchOptions,
    ) -> VectorResult<Vec<Vec<VResult>>> {
        let query_bytes: Vec<Vec<u8>> = queries
            .iter()
            .map(|q| q.iter().flat_map(|f| f.to_ne_bytes()).collect())
            .collect();

        let request = BatchVectorSearchRequest {
            index_name: index.to_string(),
            query_vectors: query_bytes,
            k: opts.k,
            return_payload: opts.return_payload,
        };

        let response = self
            .inner
            .clone()
            .batch_search(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => VectorError::IndexNotFound(index.to_string()),
                _ => VectorError::Internal(e.message().to_string()),
            })?;

        let results = response
            .into_inner()
            .results
            .into_iter()
            .map(|batch| {
                batch
                    .results
                    .into_iter()
                    .map(|r| VResult {
                        id: r.id,
                        distance: r.distance,
                        payload: if r.payload.is_empty() {
                            None
                        } else {
                            Some(r.payload)
                        },
                    })
                    .collect()
            })
            .collect();

        Ok(results)
    }
}

/// Builder for SpireVector with injectable mock
pub struct SpireVectorBuilder {
    config: VectorConfig,
    mock: Option<Arc<dyn VectorService>>,
}

impl Default for SpireVectorBuilder {
    fn default() -> Self {
        Self {
            config: VectorConfig::default(),
            mock: None,
        }
    }
}

impl SpireVectorBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set endpoint
    pub fn endpoint(mut self, e: impl Into<String>) -> Self {
        self.config.endpoint = e.into();
        self
    }

    /// Set timeout
    pub fn timeout(mut self, t: std::time::Duration) -> Self {
        self.config.timeout = t;
        self
    }

    /// Inject mock client for testing
    pub fn mock(mut self, m: Arc<dyn VectorService>) -> Self {
        self.mock = Some(m);
        self
    }

    /// Get mock if set (for testing)
    pub fn get_mock(&self) -> Option<Arc<dyn VectorService>> {
        self.mock.clone()
    }

    /// Build SpireVector client
    pub async fn build(self) -> VectorResult<SpireVector> {
        SpireVector::connect(self.config).await
    }
}
