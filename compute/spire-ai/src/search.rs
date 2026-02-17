//! Search and filter builders for collections.

use std::marker::PhantomData;

use spiresql::vector::types::SearchOptions;

use crate::collection::Collection;
use crate::document::Doc;
use crate::error::{Error, Result};

/// A search hit with score.
#[derive(Debug, Clone)]
pub struct Hit<T> {
    /// Document ID
    pub id: String,
    /// Similarity score (higher is more similar, 0.0 to 1.0 for cosine)
    pub score: f32,
    /// The matched document
    pub doc: T,
}

/// Semantic search builder for a collection.
pub struct Search<T: Doc> {
    collection: Collection<T>,
    mode: SearchMode,
    filter_sql: Option<String>,
    limit: usize,
    min_score: f32,
}

pub(crate) enum SearchMode {
    Query(String),
    SimilarId(String),
    SimilarVec(Vec<f32>),
}

impl<T: Doc> Search<T> {
    pub(crate) fn query(collection: Collection<T>, query: String) -> Self {
        Self {
            collection,
            mode: SearchMode::Query(query),
            filter_sql: None,
            limit: 10,
            min_score: 0.0,
        }
    }

    pub(crate) fn similar_id(collection: Collection<T>, id: String) -> Self {
        Self {
            collection,
            mode: SearchMode::SimilarId(id),
            filter_sql: None,
            limit: 10,
            min_score: 0.0,
        }
    }

    pub(crate) fn similar_vec(collection: Collection<T>, vec: Vec<f32>) -> Self {
        Self {
            collection,
            mode: SearchMode::SimilarVec(vec),
            filter_sql: None,
            limit: 10,
            min_score: 0.0,
        }
    }

    /// Add a SQL WHERE filter to narrow results.
    pub fn filter(mut self, sql: &str) -> Self {
        self.filter_sql = Some(sql.to_string());
        self
    }

    /// Set the maximum number of results (default: 10).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Set the minimum similarity score threshold (default: 0.0).
    pub fn min_score(mut self, s: f32) -> Self {
        self.min_score = s;
        self
    }

    /// Execute the search and return hits with scores.
    pub async fn run(self) -> Result<Vec<Hit<T>>> {
        let query_vec = match &self.mode {
            SearchMode::Query(text) => self.collection.spire.inner.embedder.embed(text).await?,
            SearchMode::SimilarVec(vec) => vec.clone(),
            SearchMode::SimilarId(_id) => {
                // TODO: Fetch the document's embedding from the vector index
                return Err(Error::Other("similar_id not yet implemented".to_string()));
            }
        };

        let index_name = self.collection.index_name();
        let opts = SearchOptions::default().k(self.limit as u32).with_payload();

        let results = self
            .collection
            .spire
            .inner
            .vector
            .search(&index_name, &query_vec, opts)
            .await?;

        let mut hits = Vec::with_capacity(results.len());
        for result in results {
            // Convert distance to similarity score (cosine: score = 1 - distance)
            let score = 1.0 - result.distance;

            if score < self.min_score {
                continue;
            }

            // Deserialize document from payload
            let id = String::from_utf8_lossy(&result.id).to_string();
            if let Some(payload) = &result.payload {
                match serde_json::from_slice::<T>(payload) {
                    Ok(doc) => {
                        hits.push(Hit { id, score, doc });
                    }
                    Err(_) => {
                        // Skip documents that can't be deserialized
                        continue;
                    }
                }
            }
        }

        // TODO: Apply SQL filter if set

        Ok(hits)
    }

    /// Execute the search and return only documents (no scores).
    pub async fn docs(self) -> Result<Vec<T>> {
        Ok(self.run().await?.into_iter().map(|h| h.doc).collect())
    }

    /// Execute the search and return the first hit.
    pub async fn first(mut self) -> Result<Option<Hit<T>>> {
        self.limit = 1;
        Ok(self.run().await?.into_iter().next())
    }
}

/// SQL-only filter builder (no vector search).
pub struct Filter<T: Doc> {
    collection: Collection<T>,
    sql_where: String,
    order_by: Option<String>,
    limit: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<T: Doc> Filter<T> {
    pub(crate) fn new(collection: Collection<T>, sql_where: String) -> Self {
        Self {
            collection,
            sql_where,
            order_by: None,
            limit: None,
            _phantom: PhantomData,
        }
    }

    /// Add ORDER BY clause.
    pub fn order_by(mut self, col: &str, desc: bool) -> Self {
        let dir = if desc { "DESC" } else { "ASC" };
        self.order_by = Some(format!("{col} {dir}"));
        self
    }

    /// Set the maximum number of results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the filter query.
    pub async fn run(self) -> Result<Vec<T>> {
        // TODO: Implement via DataAccess TableScan with filter
        let _ = (self.collection, self.sql_where, self.order_by, self.limit);
        Ok(Vec::new())
    }

    /// Count matching documents.
    pub async fn count(self) -> Result<u64> {
        // TODO: Implement via DataAccess
        Ok(0)
    }
}
