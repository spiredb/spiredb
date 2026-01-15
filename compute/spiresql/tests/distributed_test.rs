//! Tests for the distributed execution module.
//!
//! Tests cover:
//! - DistributedConfig structure
//! - DistributedError types
//! - Configuration defaults

use spiresql::distributed::{DistributedConfig, DistributedError};

/// Test distributed config defaults.
#[test]
fn test_distributed_config_defaults() {
    let config = DistributedConfig::default();

    // Should have reasonable defaults
    assert!(config.max_parallel_shards > 0);
    assert_eq!(config.max_parallel_shards, 16);
}

/// Test distributed config custom values.
#[test]
fn test_distributed_config_custom() {
    let config = DistributedConfig {
        max_parallel_shards: 32,
    };

    assert_eq!(config.max_parallel_shards, 32);
}

/// Test distributed config clone.
#[test]
fn test_distributed_config_clone() {
    let config = DistributedConfig {
        max_parallel_shards: 8,
    };
    let cloned = config.clone();

    assert_eq!(config.max_parallel_shards, cloned.max_parallel_shards);
}

/// Test distributed config debug.
#[test]
fn test_distributed_config_debug() {
    let config = DistributedConfig::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("max_parallel_shards"));
}

/// Test distributed error routing variant.
#[test]
fn test_distributed_error_routing() {
    let error = DistributedError::Routing(tonic::Status::not_found("Region not found"));

    match error {
        DistributedError::Routing(status) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        _ => panic!("Expected Routing error"),
    }
}

/// Test distributed error grpc variant.
#[test]
fn test_distributed_error_grpc() {
    let error = DistributedError::Grpc(tonic::Status::unavailable("Service down"));

    match error {
        DistributedError::Grpc(status) => {
            assert_eq!(status.code(), tonic::Code::Unavailable);
        }
        _ => panic!("Expected Grpc error"),
    }
}

/// Test distributed error no regions variant.
#[test]
fn test_distributed_error_no_regions() {
    let error = DistributedError::NoRegions;

    match error {
        DistributedError::NoRegions => {}
        _ => panic!("Expected NoRegions error"),
    }
}

/// Test distributed error display.
#[test]
fn test_distributed_error_display() {
    let error = DistributedError::Routing(tonic::Status::not_found("Test error"));
    let display_str = format!("{}", error);

    assert!(display_str.contains("Routing"));
}

/// Test distributed error connection variant.
#[test]
fn test_distributed_error_connection() {
    let boxed_err: Box<dyn std::error::Error + Send + Sync> = Box::new(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "refused",
    ));
    let error = DistributedError::Connection(boxed_err);

    match error {
        DistributedError::Connection(_) => {}
        _ => panic!("Expected Connection error"),
    }
}
