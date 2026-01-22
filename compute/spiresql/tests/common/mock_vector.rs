//! Mock vector service for testing.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use spiresql::vector::client::VectorService;
use spiresql::vector::error::{VectorError, VectorResult};
use spiresql::vector::types::{IndexParams, SearchOptions, VectorResult as VResult};

/// In-memory vector store for testing
#[derive(Debug, Default)]
pub struct MockVectorStore {
    /// Index name -> IndexParams
    pub indexes: HashMap<String, IndexParams>,
    /// Index name -> doc_id -> (vector, payload)
    pub vectors: HashMap<String, HashMap<Vec<u8>, (Vec<f32>, Option<Vec<u8>>)>>,
    /// Auto-increment ID counter
    pub id_counter: u64,
}

impl MockVectorStore {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Thread-safe mock vector service
#[derive(Debug, Clone, Default)]
pub struct MockVectorService {
    pub store: Arc<RwLock<MockVectorStore>>,
}

impl MockVectorService {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(MockVectorStore::new())),
        }
    }
}

#[async_trait]
impl VectorService for MockVectorService {
    async fn create_index(&self, params: IndexParams) -> VectorResult<()> {
        let mut store = self.store.write().unwrap();
        if store.indexes.contains_key(&params.name) {
            return Err(VectorError::IndexAlreadyExists(params.name.clone()));
        }
        store.vectors.insert(params.name.clone(), HashMap::new());
        store.indexes.insert(params.name.clone(), params);
        Ok(())
    }

    async fn drop_index(&self, name: &str) -> VectorResult<()> {
        let mut store = self.store.write().unwrap();
        if store.indexes.remove(name).is_none() {
            return Err(VectorError::IndexNotFound(name.to_string()));
        }
        store.vectors.remove(name);
        Ok(())
    }

    async fn insert(
        &self,
        index: &str,
        doc_id: &[u8],
        vector: &[f32],
        payload: Option<&[u8]>,
    ) -> VectorResult<u64> {
        let mut store = self.store.write().unwrap();

        if !store.vectors.contains_key(index) {
            return Err(VectorError::IndexNotFound(index.to_string()));
        }

        store.id_counter += 1;
        let id = store.id_counter;

        store.vectors.get_mut(index).unwrap().insert(
            doc_id.to_vec(),
            (vector.to_vec(), payload.map(|p| p.to_vec())),
        );

        Ok(id)
    }

    async fn delete(&self, index: &str, doc_id: &[u8]) -> VectorResult<()> {
        let mut store = self.store.write().unwrap();
        let vectors = store
            .vectors
            .get_mut(index)
            .ok_or_else(|| VectorError::IndexNotFound(index.to_string()))?;

        vectors.remove(doc_id);
        Ok(())
    }

    async fn search(
        &self,
        index: &str,
        query: &[f32],
        opts: SearchOptions,
    ) -> VectorResult<Vec<VResult>> {
        let store = self.store.read().unwrap();
        let vectors = store
            .vectors
            .get(index)
            .ok_or_else(|| VectorError::IndexNotFound(index.to_string()))?;

        // Compute distances and sort
        let mut results: Vec<VResult> = vectors
            .iter()
            .map(|(id, (vec, payload))| {
                let distance = euclidean_distance(query, vec);
                VResult {
                    id: id.clone(),
                    distance,
                    payload: if opts.return_payload {
                        payload.clone()
                    } else {
                        None
                    },
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(opts.k as usize);

        Ok(results)
    }

    async fn batch_search(
        &self,
        index: &str,
        queries: &[Vec<f32>],
        opts: SearchOptions,
    ) -> VectorResult<Vec<Vec<VResult>>> {
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            results.push(self.search(index, query, opts.clone()).await?);
        }
        Ok(results)
    }
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 0.001);
    }
}
