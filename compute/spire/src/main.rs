mod cli;
mod commands;

use clap::Parser;
use cli::{Args, Commands};
use commands::cluster::handle_cluster_command;
use commands::data::handle_data_command;
use commands::plugin::handle_plugin_command;
use commands::schema::handle_schema_command;
use commands::sql::handle_sql_command;
use spire_common::init_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    let args = Args::parse();
    // Load config if needed, but we use args currently.
    // Assuming default spiresql_addr is hardcoded or should be in Args?
    // Args has pd_addr and data_addr but not spiresql_addr in the struct I created earlier.
    // I should add spiresql_addr to Args or just use default.
    let spiresql_addr = "127.0.0.1:5432".to_string(); // hardcoded for now or add to Args

    match args.command {
        Commands::Cluster { cmd } => {
            handle_cluster_command(cmd, args.pd_addr).await?;
        }
        Commands::Plugin { cmd } => {
            handle_plugin_command(cmd, args.pd_addr).await?;
        }
        Commands::Schema { cmd } => {
            handle_schema_command(cmd, args.pd_addr).await?;
        }
        Commands::Data { cmd } => {
            handle_data_command(cmd, args.data_addr).await?;
        }
        Commands::Sql => {
            handle_sql_command(spiresql_addr).await?;
        }
    }

    Ok(())
}
