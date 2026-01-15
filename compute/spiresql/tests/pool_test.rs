//! Tests for the connection pool module.
//!
//! Tests cover:
//! - Pool configuration
//! - Pool defaults
//! - Edge cases

use spiresql::pool::PoolConfig;
use std::time::Duration;

/// Test default pool configuration.
#[test]
fn test_pool_config_defaults() {
    let config = PoolConfig::default();

    // Should have reasonable defaults
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert!(config.stream_window_size > 0);
    assert!(config.connection_window_size > 0);
}

/// Test custom pool configuration.
#[test]
fn test_pool_config_custom() {
    let config = PoolConfig {
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(15),
        keepalive_timeout: Duration::from_secs(30),
        stream_window_size: 8 * 1024 * 1024,
        connection_window_size: 16 * 1024 * 1024,
    };

    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.request_timeout, Duration::from_secs(60));
    assert_eq!(config.stream_window_size, 8 * 1024 * 1024);
}

/// Test pool config clone.
#[test]
fn test_pool_config_clone() {
    let config = PoolConfig::default();
    let cloned = config.clone();

    assert_eq!(config.connect_timeout, cloned.connect_timeout);
    assert_eq!(config.request_timeout, cloned.request_timeout);
}

/// Test pool config debug.
#[test]
fn test_pool_config_debug() {
    let config = PoolConfig::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("connect_timeout"));
    assert!(debug_str.contains("request_timeout"));
}

/// Test keepalive settings.
#[test]
fn test_pool_config_keepalive() {
    let config = PoolConfig::default();

    assert_eq!(config.keepalive_interval, Duration::from_secs(10));
    assert_eq!(config.keepalive_timeout, Duration::from_secs(20));
}

/// Test window sizes.
#[test]
fn test_pool_config_window_sizes() {
    let config = PoolConfig::default();

    assert_eq!(config.stream_window_size, 16 * 1024 * 1024);
    assert_eq!(config.connection_window_size, 32 * 1024 * 1024);
}
