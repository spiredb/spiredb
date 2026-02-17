//! Typed document collections with automatic embedding and vector search.

use std::marker::PhantomData;

use spire_proto::spiredb::cluster::{
    ColumnDef, ColumnType, CreateTableRequest, schema_service_client::SchemaServiceClient,
};
use spiresql::vector::types::{Algorithm, IndexParams};

use crate::client::Spire;
use crate::document::Doc;
use crate::error::{Error, Result};
use crate::search::{Filter, Search};
use crate::watch::WatchStream;

fn doc_cache_key(collection: &str, id: &str) -> u64 {
    ahash::RandomState::with_seeds(0, 0, 0, 0).hash_one((collection, id))
}

/// A typed document collection stored in SpireDB.
///
/// Documents are stored as JSON in a SpireDB table, with vector embeddings
/// in a separate vector index for semantic search.
pub struct Collection<T: Doc> {
    pub(crate) spire: Spire,
    pub(crate) name: String,
    pub(crate) _phantom: PhantomData<T>,
}

// Manual Clone impl: T doesn't need to be Clone since we only hold PhantomData<T>
impl<T: Doc> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Self {
            spire: self.spire.clone(),
            name: self.name.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: Doc> Collection<T> {
    pub(crate) fn new(spire: Spire, name: String) -> Self {
        Self {
            spire,
            name,
            _phantom: PhantomData,
        }
    }

    /// The internal table name used in SpireDB.
    pub fn table_name(&self) -> String {
        format!("_ai_{}", self.name)
    }

    /// The internal vector index name used in SpireDB.
    pub fn index_name(&self) -> String {
        format!("_ai_{}_vec", self.name)
    }

    /// Ensure the backing table and vector index exist.
    ///
    /// Creates them if they don't exist. Safe to call multiple times.
    pub async fn ensure(&self) -> Result<()> {
        let table = self.table_name();
        let index = self.index_name();
        let dims = self.spire.inner.embedder.dimensions() as u32;

        // Create table via SchemaService
        let mut schema_client = SchemaServiceClient::new(self.spire.inner.pd_channel.clone());

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                r#type: ColumnType::TypeString.into(),
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "doc".to_string(),
                r#type: ColumnType::TypeBytes.into(),
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "embed_text".to_string(),
                r#type: ColumnType::TypeString.into(),
                nullable: true,
                ..Default::default()
            },
            ColumnDef {
                name: "created_at".to_string(),
                r#type: ColumnType::TypeTimestamp.into(),
                nullable: true,
                ..Default::default()
            },
        ];

        let request = CreateTableRequest {
            name: table.clone(),
            columns,
            primary_key: vec!["id".to_string()],
        };

        match schema_client.create_table(request).await {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                // Table already exists, that's fine
            }
            Err(e) => return Err(Error::Grpc(e)),
        }

        // Create vector index if embedder is configured (dims > 0)
        if dims > 0 {
            let params = IndexParams::new(&index, &table, "embedding")
                .algorithm(Algorithm::Manode)
                .dimensions(dims);

            match self.spire.inner.vector.create_index(params).await {
                Ok(_) => {}
                Err(spiresql::vector::error::VectorError::IndexAlreadyExists(_)) => {}
                Err(e) => return Err(Error::Vector(e)),
            }
        }

        Ok(())
    }

    /// Insert a document. Automatically generates embedding if `embed_text()` is non-empty.
    pub async fn insert(&self, doc: &T) -> Result<String> {
        let id = doc.id().to_string();
        let doc_json = serde_json::to_vec(doc)?;
        let embed_text = doc.embed_text();

        // Cache the doc for later get() lookups
        let cache_key = doc_cache_key(&self.name, &id);
        self.spire
            .inner
            .doc_cache
            .insert(cache_key, doc_json.clone());

        // Generate embedding if text is non-empty
        let embedding = if !embed_text.is_empty() {
            Some(self.spire.inner.embedder.embed(&embed_text).await?)
        } else {
            None
        };

        // Insert vector with doc JSON as payload
        if let Some(ref vec) = embedding {
            self.spire
                .inner
                .vector
                .insert(&self.index_name(), id.as_bytes(), vec, Some(&doc_json))
                .await?;
        }

        Ok(id)
    }

    /// Insert multiple documents in a batch.
    pub async fn insert_many(&self, docs: &[T]) -> Result<Vec<String>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<String> = docs.iter().map(|d| d.id().to_string()).collect();
        let texts: Vec<String> = docs.iter().map(|d| d.embed_text()).collect();

        // Batch embed non-empty texts
        let non_empty: Vec<String> = texts.iter().filter(|t| !t.is_empty()).cloned().collect();

        let embeddings = if !non_empty.is_empty() {
            self.spire.inner.embedder.embed_batch(&non_empty).await?
        } else {
            Vec::new()
        };

        // Map embeddings back to docs
        let mut embed_iter = embeddings.into_iter();
        let index_name = self.index_name();

        for (i, doc) in docs.iter().enumerate() {
            let doc_json = serde_json::to_vec(doc)?;

            // Cache the doc
            let cache_key = doc_cache_key(&self.name, &ids[i]);
            self.spire
                .inner
                .doc_cache
                .insert(cache_key, doc_json.clone());

            if !texts[i].is_empty()
                && let Some(vec) = embed_iter.next()
            {
                self.spire
                    .inner
                    .vector
                    .insert(&index_name, ids[i].as_bytes(), &vec, Some(&doc_json))
                    .await?;
            }
        }

        Ok(ids)
    }

    /// Upsert a document (insert or replace).
    pub async fn upsert(&self, doc: &T) -> Result<String> {
        let id = doc.id().to_string();

        // Delete existing vector if present (ignore not-found)
        let _ = self
            .spire
            .inner
            .vector
            .delete(&self.index_name(), id.as_bytes())
            .await;

        // Insert the new version
        self.insert(doc).await
    }

    /// Delete a document by ID.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        // Remove from cache
        let cache_key = doc_cache_key(&self.name, id);
        self.spire.inner.doc_cache.remove(&cache_key);

        match self
            .spire
            .inner
            .vector
            .delete(&self.index_name(), id.as_bytes())
            .await
        {
            Ok(_) => Ok(true),
            Err(spiresql::vector::error::VectorError::IndexNotFound(_)) => Ok(false),
            Err(e) => Err(Error::Vector(e)),
        }
    }

    /// Get a document by ID.
    ///
    /// Checks the in-memory cache first, then falls back to a VectorGet RPC
    /// to retrieve the payload from SpireDB.
    pub async fn get(&self, id: &str) -> Result<Option<T>> {
        // Fast path: check in-memory cache
        let cache_key = doc_cache_key(&self.name, id);
        if let Some(bytes) = self.spire.inner.doc_cache.get(&cache_key)
            && let Ok(doc) = serde_json::from_slice::<T>(&bytes)
        {
            return Ok(Some(doc));
        }

        // Slow path: fetch from SpireDB via VectorGet RPC
        match self
            .spire
            .inner
            .vector
            .get(&self.index_name(), id.as_bytes())
            .await
        {
            Ok(Some(payload)) => {
                // Cache for next time
                self.spire
                    .inner
                    .doc_cache
                    .insert(cache_key, payload.clone());
                match serde_json::from_slice::<T>(&payload) {
                    Ok(doc) => Ok(Some(doc)),
                    Err(_) => Ok(None),
                }
            }
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    /// Get multiple documents by IDs.
    pub async fn get_many(&self, ids: &[&str]) -> Result<Vec<T>> {
        let mut docs = Vec::new();
        for id in ids {
            if let Some(doc) = self.get(id).await? {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Start a semantic search.
    pub fn search(&self, query: &str) -> Search<T> {
        Search::query(self.clone(), query.to_string())
    }

    /// Find documents similar to an existing document.
    pub fn similar(&self, id: &str) -> Search<T> {
        Search::similar_id(self.clone(), id.to_string())
    }

    /// Find documents similar to a raw vector.
    pub fn similar_vec(&self, vec: &[f32]) -> Search<T> {
        Search::similar_vec(self.clone(), vec.to_vec())
    }

    /// Filter documents using SQL WHERE clause (no vector search).
    pub fn filter(&self, sql_where: &str) -> Filter<T> {
        Filter::new(self.clone(), sql_where.to_string())
    }

    /// Watch for changes to this collection via CDC.
    pub async fn watch(&self) -> Result<WatchStream<T>> {
        WatchStream::new(&self.spire.inner.stream_addr, &self.table_name()).await
    }
}
