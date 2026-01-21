#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spiredb/spiredb/master/art/spire-square.svg"
)]
//! SpireDB Command Line Interface (CLI).
//!
//! This crate provides the `spire` binary, which allows users to interact with SpireDB clusters.
//! It supports commands for cluster management, data operations, schema management, and SQL execution.
//!
//! # Usage
//!
//! ```bash
//! spire [COMMAND] [ARGS]
//! ```
//!
//! See `spire --help` for more information.
//!
//! For comprehensive documentation, visit: <https://spire.zone/docs/#/cli>

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
    let spiresql_addr = args.spiresql_addr.clone();

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
