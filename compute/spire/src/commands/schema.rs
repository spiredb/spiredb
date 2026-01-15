use crate::cli::SchemaCmd;
use spire_common::SpireError;
use spire_proto::spiredb::cluster::schema_service_client::SchemaServiceClient;
use spire_proto::spiredb::cluster::{Empty, ListIndexesRequest};
use tonic::transport::Channel;

pub async fn handle_schema_command(cmd: SchemaCmd, pd_addr: String) -> Result<(), SpireError> {
    let mut client = connect(pd_addr).await?;

    match cmd {
        SchemaCmd::ListTables => {
            let response = client.list_tables(Empty {}).await?;
            let tables = response.into_inner().tables;
            println!("Tables:");
            for table in tables {
                println!("  - ID: {}, Name: {}", table.id, table.name);
            }
        }
        SchemaCmd::ListIndexes => {
            // Proto requires table_name for ListIndexes currently?
            // Checking proto: message ListIndexesRequest { string table_name = 1; }
            // If table_name is optional in proto (it says "Optional: filter by table"), we can pass empty string?
            // Let's assume empty string lists all or try without arguments if supported.
            let response = client
                .list_indexes(ListIndexesRequest {
                    table_name: "".to_string(),
                })
                .await?;
            let indexes = response.into_inner().indexes;
            println!("Indexes:");
            for index in indexes {
                println!(
                    "  - ID: {}, Name: {}, TableID: {}, Type: {:?}",
                    index.id, index.name, index.table_id, index.r#type
                );
            }
        }
    }

    Ok(())
}

async fn connect(addr: String) -> Result<SchemaServiceClient<Channel>, SpireError> {
    let endpoint = Channel::from_shared(addr.clone())
        .map_err(|e| SpireError::Internal(format!("Invalid URI: {}", e)))?;
    let channel = endpoint.connect().await.map_err(|e| {
        SpireError::Grpc(tonic::Status::unavailable(format!(
            "Failed to connect to {}: {}",
            addr, e
        )))
    })?;

    Ok(SchemaServiceClient::new(channel))
}
