//! Tests for vector client using MockVectorService.

mod common;

use spiresql::vector::client::VectorService;
use spiresql::vector::types::{IndexParams, SearchOptions};

use common::mock_vector::MockVectorService;

// ============================================================================
// Index Management Tests
// ============================================================================

#[tokio::test]
async fn test_create_index() {
    let service = MockVectorService::new();
    let params = IndexParams::new("test_index", "users", "embedding");

    service.create_index(params).await.unwrap();

    let store = service.store.read().unwrap();
    assert!(store.indexes.contains_key("test_index"));
}

#[tokio::test]
async fn test_create_index_already_exists() {
    let service = MockVectorService::new();
    let params = IndexParams::new("test_index", "users", "embedding");

    service.create_index(params.clone()).await.unwrap();
    let result = service.create_index(params).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_index() {
    let service = MockVectorService::new();
    let params = IndexParams::new("test_index", "users", "embedding");

    service.create_index(params).await.unwrap();
    service.drop_index("test_index").await.unwrap();

    let store = service.store.read().unwrap();
    assert!(!store.indexes.contains_key("test_index"));
}

#[tokio::test]
async fn test_drop_index_not_found() {
    let service = MockVectorService::new();
    let result = service.drop_index("nonexistent").await;

    assert!(result.is_err());
}

// ============================================================================
// Insert Tests
// ============================================================================

#[tokio::test]
async fn test_insert_vector() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let doc_id = b"doc-1";
    let vector = vec![1.0, 2.0, 3.0];
    let internal_id = service.insert("idx", doc_id, &vector, None).await.unwrap();

    assert!(internal_id > 0);

    let store = service.store.read().unwrap();
    let vectors = store.vectors.get("idx").unwrap();
    assert!(vectors.contains_key(doc_id.as_slice()));
}

#[tokio::test]
async fn test_insert_vector_with_payload() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let doc_id = b"doc-1";
    let vector = vec![1.0, 2.0, 3.0];
    let payload = b"{\"name\": \"test\"}";

    service
        .insert("idx", doc_id, &vector, Some(payload))
        .await
        .unwrap();

    let store = service.store.read().unwrap();
    let vectors = store.vectors.get("idx").unwrap();
    let (_, stored_payload) = vectors.get(doc_id.as_slice()).unwrap();
    assert_eq!(stored_payload, &Some(payload.to_vec()));
}

#[tokio::test]
async fn test_insert_index_not_found() {
    let service = MockVectorService::new();
    let result = service.insert("nonexistent", b"doc", &[1.0], None).await;

    assert!(result.is_err());
}

// ============================================================================
// Delete Tests
// ============================================================================

#[tokio::test]
async fn test_delete_vector() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let doc_id = b"doc-1";
    service
        .insert("idx", doc_id, &[1.0, 2.0], None)
        .await
        .unwrap();
    service.delete("idx", doc_id).await.unwrap();

    let store = service.store.read().unwrap();
    let vectors = store.vectors.get("idx").unwrap();
    assert!(!vectors.contains_key(doc_id.as_slice()));
}

#[tokio::test]
async fn test_delete_index_not_found() {
    let service = MockVectorService::new();
    let result = service.delete("nonexistent", b"doc").await;

    assert!(result.is_err());
}

// ============================================================================
// Search Tests
// ============================================================================

#[tokio::test]
async fn test_search_empty_index() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let results = service
        .search("idx", &[1.0, 2.0], SearchOptions::default())
        .await
        .unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_search_single_vector() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    service
        .insert("idx", b"doc-1", &[1.0, 0.0], None)
        .await
        .unwrap();

    let results = service
        .search("idx", &[1.0, 0.0], SearchOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, b"doc-1");
    assert_eq!(results[0].distance, 0.0);
}

#[tokio::test]
async fn test_search_multiple_vectors() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    service
        .insert("idx", b"doc-1", &[0.0, 0.0], None)
        .await
        .unwrap();
    service
        .insert("idx", b"doc-2", &[1.0, 0.0], None)
        .await
        .unwrap();
    service
        .insert("idx", b"doc-3", &[2.0, 0.0], None)
        .await
        .unwrap();

    let results = service
        .search("idx", &[0.0, 0.0], SearchOptions::default().k(2))
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, b"doc-1"); // Closest
    assert_eq!(results[1].id, b"doc-2"); // Second closest
}

#[tokio::test]
async fn test_search_with_payload() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let payload = b"test payload";
    service
        .insert("idx", b"doc-1", &[1.0, 0.0], Some(payload))
        .await
        .unwrap();

    let results = service
        .search("idx", &[1.0, 0.0], SearchOptions::default().with_payload())
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].payload, Some(payload.to_vec()));
}

#[tokio::test]
async fn test_search_without_payload() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    service
        .insert("idx", b"doc-1", &[1.0, 0.0], Some(b"payload"))
        .await
        .unwrap();

    let results = service
        .search("idx", &[1.0, 0.0], SearchOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].payload.is_none());
}

#[tokio::test]
async fn test_search_index_not_found() {
    let service = MockVectorService::new();
    let result = service
        .search("nonexistent", &[1.0], SearchOptions::default())
        .await;

    assert!(result.is_err());
}

// ============================================================================
// Batch Search Tests
// ============================================================================

#[tokio::test]
async fn test_batch_search() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    service
        .insert("idx", b"doc-1", &[0.0, 0.0], None)
        .await
        .unwrap();
    service
        .insert("idx", b"doc-2", &[1.0, 0.0], None)
        .await
        .unwrap();

    let queries = vec![vec![0.0, 0.0], vec![1.0, 0.0]];
    let results = service
        .batch_search("idx", &queries, SearchOptions::default().k(1))
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0][0].id, b"doc-1");
    assert_eq!(results[1][0].id, b"doc-2");
}

#[tokio::test]
async fn test_batch_search_empty() {
    let service = MockVectorService::new();
    let params = IndexParams::new("idx", "t", "c");
    service.create_index(params).await.unwrap();

    let queries: Vec<Vec<f32>> = vec![];
    let results = service
        .batch_search("idx", &queries, SearchOptions::default())
        .await
        .unwrap();

    assert!(results.is_empty());
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_vector_config_default() {
    use spiresql::vector::config::VectorConfig;

    let config = VectorConfig::default();
    assert_eq!(config.endpoint, "http://localhost:50052");
}

#[test]
fn test_vector_config_new() {
    use spiresql::vector::config::VectorConfig;

    let config = VectorConfig::new("http://spiredb:50052");
    assert_eq!(config.endpoint, "http://spiredb:50052");
}

#[test]
fn test_vector_config_timeout() {
    use spiresql::vector::config::VectorConfig;
    use std::time::Duration;

    let config = VectorConfig::new("localhost").timeout(Duration::from_secs(60));
    assert_eq!(config.timeout, Duration::from_secs(60));
}

// ============================================================================
// Error Tests
// ============================================================================

#[test]
fn test_vector_error_display() {
    use spiresql::vector::error::VectorError;

    let err = VectorError::IndexNotFound("my_index".to_string());
    assert!(err.to_string().contains("my_index"));

    let err = VectorError::Connection("timeout".to_string());
    assert!(err.to_string().contains("timeout"));
}
