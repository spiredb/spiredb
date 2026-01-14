//! Lock-Free Connection Pool
//!
//! Maintains a pool of gRPC channels to SpireDB storage nodes.
//! Uses kovan_map for lock-free hot-path lookups.

use ahash::AHasher;
use kovan_map::HashMap;
use parking_lot::RwLock;
use spire_proto::spiredb::data::data_access_client::DataAccessClient;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Configuration for connection pool.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// HTTP2 keepalive interval.
    pub keepalive_interval: Duration,
    /// Keepalive timeout.
    pub keepalive_timeout: Duration,
    /// Stream window size.
    pub stream_window_size: u32,
    /// Connection window size.
    pub connection_window_size: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(20),
            stream_window_size: 16 * 1024 * 1024,     // 16MB
            connection_window_size: 32 * 1024 * 1024, // 32MB
        }
    }
}

/// Lock-free connection pool for storage nodes.
pub struct ConnectionPool {
    /// Lock-free index: address_hash (u64) -> channel_index (usize).
    channel_index: Arc<HashMap<u64, usize>>,

    /// Append-only channel storage (lock-free reads, locked writes).
    channels: Arc<RwLock<Vec<Channel>>>,

    /// Pool configuration.
    config: PoolConfig,
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("channel_count", &self.channels.read().len())
            .finish()
    }
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            channel_index: Arc::new(HashMap::new()),
            channels: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Hash an address for lock-free lookup (ahash for speed).
    fn hash_addr(addr: &str) -> u64 {
        let mut hasher = AHasher::default();
        addr.hash(&mut hasher);
        hasher.finish()
    }

    /// Get or create a channel for a storage node (lock-free hot path).
    pub async fn get_channel(
        &self,
        addr: &str,
    ) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
        let hash = Self::hash_addr(addr);

        // Lock-free check: is it cached?
        if let Some(index) = self.channel_index.get(&hash) {
            let channels = self.channels.read();
            if let Some(channel) = channels.get(index) {
                return Ok(channel.clone());
            }
        }

        // Cache miss: create new channel
        self.create_channel(addr).await
    }

    /// Create a new channel and add to pool.
    async fn create_channel(
        &self,
        addr: &str,
    ) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
        let hash = Self::hash_addr(addr);

        let channel = Channel::from_shared(addr.to_string())?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout)
            .http2_keep_alive_interval(self.config.keepalive_interval)
            .keep_alive_timeout(self.config.keepalive_timeout)
            .keep_alive_while_idle(true)
            .initial_stream_window_size(self.config.stream_window_size)
            .initial_connection_window_size(self.config.connection_window_size)
            .connect_lazy();

        // Store in pool
        let index = {
            let mut channels = self.channels.write();
            let idx = channels.len();
            channels.push(channel.clone());
            idx
        };

        // Lock-free index update
        self.channel_index.insert(hash, index);

        log::debug!(
            "Created channel for {} (hash: {}, index: {})",
            addr,
            hash,
            index
        );

        Ok(channel)
    }

    /// Get a DataAccessClient for a storage node.
    pub async fn get_data_access_client(
        &self,
        addr: &str,
    ) -> Result<DataAccessClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        let channel = self.get_channel(addr).await?;
        Ok(DataAccessClient::new(channel))
    }

    /// Get pool statistics.
    #[allow(dead_code)]
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            channel_count: self.channels.read().len(),
        }
    }
}

/// Pool statistics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PoolStats {
    pub channel_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_addr() {
        let hash1 = ConnectionPool::hash_addr("http://127.0.0.1:50052");
        let hash2 = ConnectionPool::hash_addr("http://127.0.0.1:50052");
        let hash3 = ConnectionPool::hash_addr("http://127.0.0.1:50053");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
}
