use crate::cli::DataCmd;
use spire_common::SpireError;
use spire_proto::spiredb::data::data_access_client::DataAccessClient;
use spire_proto::spiredb::data::{RawGetRequest, RawPutRequest};
use tonic::transport::Channel;

pub async fn handle_data_command(cmd: DataCmd, data_addr: String) -> Result<(), SpireError> {
    let mut client = connect(data_addr).await?;

    match cmd {
        DataCmd::Get { key } => {
            let request = RawGetRequest {
                key: key.into_bytes(),
                region_id: 0, // TODO: This should be routed or determined
                snapshot_ts: 0,
                read_follower: false,
            };
            let response = client.raw_get(request).await?;
            let resp_inner = response.into_inner();
            if resp_inner.found {
                println!("{}", String::from_utf8_lossy(&resp_inner.value));
            } else {
                println!("Key not found");
            }
        }
        DataCmd::Put { key, value } => {
            let request = RawPutRequest {
                key: key.into_bytes(),
                value: value.into_bytes(),
            };
            client.raw_put(request).await?;
            println!("OK");
        }
    }

    Ok(())
}

async fn connect(addr: String) -> Result<DataAccessClient<Channel>, SpireError> {
    let endpoint = Channel::from_shared(addr.clone())
        .map_err(|e| SpireError::Internal(format!("Invalid URI: {}", e)))?;
    let channel = endpoint.connect().await.map_err(|e| {
        SpireError::Grpc(tonic::Status::unavailable(format!(
            "Failed to connect to {}: {}",
            addr, e
        )))
    })?;

    Ok(DataAccessClient::new(channel))
}
