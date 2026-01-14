use crate::cli::ClusterCmd;
use spire_common::SpireError;
use spire_proto::spiredb::cluster::cluster_service_client::ClusterServiceClient;
use spire_proto::spiredb::cluster::Empty;
use tonic::transport::Channel;

pub async fn handle_cluster_command(cmd: ClusterCmd, pd_addr: String) -> Result<(), SpireError> {
    let mut client = connect(pd_addr).await?;

    match cmd {
        ClusterCmd::Status => {
            // For status we might just list stores or check health.
            // Let's list stores as a proxy for status for now, or implement a specific status check if proto allows.
            // Proto has ListStores.
            let response = client.list_stores(Empty {}).await?;
            let stores = response.into_inner().stores;
            println!("Cluster Status: {} stores up", stores.len());
            for store in stores {
                println!(
                    "  Store {}: {} ({:?})",
                    store.id, store.address, store.state
                );
            }
        }
        ClusterCmd::Members => {
            let response = client.list_stores(Empty {}).await?;
            let stores = response.into_inner().stores;
            println!("Cluster Members:");
            for store in stores {
                println!(
                    "  - ID: {}, Address: {}, State: {:?}",
                    store.id, store.address, store.state
                );
            }
        }
    }

    Ok(())
}

async fn connect(addr: String) -> Result<ClusterServiceClient<Channel>, SpireError> {
    let endpoint = Channel::from_shared(addr.clone())
        .map_err(|e| SpireError::Internal(format!("Invalid URI: {}", e)))?;
    let channel = endpoint.connect().await.map_err(|e| {
        SpireError::Grpc(tonic::Status::unavailable(format!(
            "Failed to connect to {}: {}",
            addr, e
        )))
    })?;

    Ok(ClusterServiceClient::new(channel))
}
