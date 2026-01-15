//! Cluster Topology Watcher
//!
//! Continuously monitors SpireDB cluster topology via ListStores.
//! Updates store addresses when nodes join/leave.
//! Uses lock-free concurrent hashmap for fast lookups.

use kovan_map::HashMap;
use spire_proto::spiredb::cluster::{
    cluster_service_client::ClusterServiceClient, Empty, StoreState,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Refresh interval for cluster topology (fast failure detection).
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Cluster topology with live store addresses.
pub struct ClusterTopology {
    cluster_client: ClusterServiceClient<Channel>,
    /// store_id -> address (lock-free concurrent map)
    stores: Arc<HashMap<u64, StoreInfo>>,
}

/// Information about a store node.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StoreInfo {
    pub id: u64,
    pub address: String,
}

impl std::fmt::Debug for ClusterTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTopology")
            .field("stores_count", &self.stores.len())
            .finish()
    }
}

impl ClusterTopology {
    /// Create a new cluster topology watcher.
    pub fn new(cluster_client: ClusterServiceClient<Channel>) -> Self {
        Self {
            cluster_client,
            stores: Arc::new(HashMap::new()),
        }
    }

    /// Start background refresh task.
    pub fn start_refresh_task(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.refresh().await {
                    log::warn!("Failed to refresh cluster topology: {}", e);
                }
                tokio::time::sleep(REFRESH_INTERVAL).await;
            }
        });
    }

    /// Refresh store list from ClusterService.
    pub async fn refresh(&self) -> Result<(), tonic::Status> {
        let mut client = self.cluster_client.clone();
        let response = client.list_stores(Empty {}).await?;
        let store_list = response.into_inner();

        let mut count = 0;
        for store in store_list.stores {
            if store.state == StoreState::StoreUp as i32 {
                self.stores.insert(
                    store.id,
                    StoreInfo {
                        id: store.id,
                        address: store.address.clone(),
                    },
                );
                log::debug!("Store {} at {}", store.id, store.address);
                count += 1;
            } else {
                // Remove down stores
                self.stores.remove(&store.id);
            }
        }

        log::info!("Cluster topology refreshed: {} active stores", count);
        Ok(())
    }

    /// Get store address by ID (lock-free).
    pub fn get_store_address(&self, store_id: u64) -> Option<String> {
        self.stores.get(&store_id).map(|s| s.address.clone())
    }

    /// Get all active stores.
    #[allow(dead_code)]
    pub fn all_stores(&self) -> Vec<StoreInfo> {
        self.stores.iter().map(|(_, v)| v.clone()).collect()
    }

    /// Get store count.
    pub fn store_count(&self) -> usize {
        self.stores.len()
    }
}
