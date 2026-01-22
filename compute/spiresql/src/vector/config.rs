//! Vector client configuration.

use std::time::Duration;

/// Configuration for SpireVector client
#[derive(Debug, Clone)]
pub struct VectorConfig {
    /// gRPC endpoint
    pub endpoint: String,
    /// Request timeout
    pub timeout: Duration,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:50052".into(),
            timeout: Duration::from_secs(30),
        }
    }
}

impl VectorConfig {
    /// Create config with endpoint
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Default::default()
        }
    }

    /// Set request timeout
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }
}
