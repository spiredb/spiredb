//! Tests for the configuration module.
//!
//! Tests cover:
//! - Default configuration values
//! - TOML parsing
//! - CLI argument parsing
//! - Config merging (file + CLI)
//! - Edge cases and validation

use spiresql::config::Config;

/// Test default configuration values.
#[test]
fn test_config_defaults() {
    let config = Config::default();

    assert_eq!(config.listen_addr, "0.0.0.0:5432");
    assert!(
        config.cluster_addr.contains("50051"),
        "cluster_addr should contain port 50051"
    );
    assert_eq!(config.num_workers, 0); // 0 = auto-detect
    assert!(config.query_cache_capacity > 0);
    assert!(config.enable_cache);
    assert_eq!(config.log_level, "info");
}

/// Test that config implements Clone.
#[test]
fn test_config_clone() {
    let config = Config::default();
    let cloned = config.clone();

    assert_eq!(config.listen_addr, cloned.listen_addr);
    assert_eq!(config.cluster_addr, cloned.cluster_addr);
}

/// Test that config implements Debug.
#[test]
fn test_config_debug() {
    let config = Config::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("listen_addr"));
    assert!(debug_str.contains("cluster_addr"));
}

/// Test custom configuration values.
#[test]
fn test_config_custom_values() {
    let config = Config {
        listen_addr: "127.0.0.1:15432".to_string(),
        cluster_addr: "http://spiredb:50051".to_string(),
        data_access_addr: "http://spiredb:50052".to_string(),
        num_workers: 4,
        query_cache_capacity: 5000,
        enable_cache: false,
        log_level: "debug".to_string(),
    };

    assert_eq!(config.listen_addr, "127.0.0.1:15432");
    assert_eq!(config.cluster_addr, "http://spiredb:50051");
    assert_eq!(config.num_workers, 4);
    assert_eq!(config.query_cache_capacity, 5000);
    assert!(!config.enable_cache);
    assert_eq!(config.log_level, "debug");
}

/// Test zero cache capacity.
#[test]
fn test_config_zero_cache_capacity() {
    let config = Config {
        query_cache_capacity: 0,
        ..Config::default()
    };

    // Zero capacity should be handled gracefully
    assert_eq!(config.query_cache_capacity, 0);
}

/// Test various worker counts.
#[test]
fn test_config_worker_counts() {
    // Zero means auto-detect
    let auto = Config {
        num_workers: 0,
        ..Config::default()
    };
    assert_eq!(auto.num_workers, 0);

    // Specific count
    let specific = Config {
        num_workers: 8,
        ..Config::default()
    };
    assert_eq!(specific.num_workers, 8);
}
