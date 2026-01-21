//! Cluster Topology Watcher
//!
//! Continuously monitors SpireDB cluster topology via ListStores.
//! Updates store addresses when nodes join/leave.
//! Uses lock-free concurrent hashmap for fast lookups.

use kovan_map::HashMap;
use spire_proto::spiredb::cluster::{
    Empty, StoreState, cluster_service_client::ClusterServiceClient,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Refresh interval for cluster topology (fast failure detection).
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

use parking_lot::RwLock;

/// Cluster topology with live store addresses.
pub struct ClusterTopology {
    cluster_client: ClusterServiceClient<Channel>,
    /// store_id -> address (lock-free concurrent map)
    stores: Arc<HashMap<u64, StoreInfo>>,
    /// Leader store info (cached)
    leader: RwLock<Option<LeaderInfo>>,
}

/// Information about a store node.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StoreInfo {
    pub id: u64,
    pub address: String,
}

/// Leader store info (cached).
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct LeaderInfo {
    pub address: String,
    pub store_id: u64,
}

impl std::fmt::Debug for ClusterTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTopology")
            .field("stores_count", &self.stores.len())
            .field("leader", &self.leader.read())
            .finish()
    }
}

impl ClusterTopology {
    /// Create a new cluster topology watcher.
    pub fn new(cluster_client: ClusterServiceClient<Channel>) -> Self {
        Self {
            cluster_client,
            stores: Arc::new(HashMap::new()),
            leader: RwLock::new(None),
        }
    }

    /// Get the leader store address.
    /// Schema operations should be routed to this node.
    pub fn get_leader_address(&self) -> Option<LeaderInfo> {
        self.leader.read().clone()
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

        let prev_count = self.stores.len();
        let mut count = 0;
        let mut found_leader: Option<LeaderInfo> = None;

        for store in store_list.stores {
            if store.state == StoreState::StoreUp as i32 {
                // Check if this store is the leader
                if store.is_leader {
                    found_leader = Some(LeaderInfo {
                        address: store.address.clone(),
                        store_id: store.id,
                    });
                }

                self.stores.insert(
                    store.id,
                    StoreInfo {
                        id: store.id,
                        address: store.address.clone(),
                    },
                );
                log::debug!(
                    "Store {} at {} (leader={})",
                    store.id,
                    store.address,
                    store.is_leader
                );
                count += 1;
            } else {
                // Remove down stores
                self.stores.remove(&store.id);
            }
        }

        // Update cached leader
        if let Some(leader) = found_leader {
            *self.leader.write() = Some(leader);
        } else if count > 0 {
            // If no leader reported but stores exist, we might have lost leader
            // Don't clear immediately to allow grace period, but log warning
            log::warn!("No PD leader reported in topology refresh");
        }

        // Only log when store count changes
        if count != prev_count {
            log::info!("Cluster topology updated: {} active stores", count);
        }
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
