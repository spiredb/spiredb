use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "spire")]
#[command(about = "SpireDB Cluster Management CLI", long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,

    /// PD server address
    #[arg(long, global = true, default_value = "http://127.0.0.1:50051")]
    pub pd_addr: String,

    /// Data server address
    #[arg(long, global = true, default_value = "http://127.0.0.1:50052")]
    pub data_addr: String,

    /// SpireSQL server address
    #[arg(long, global = true, default_value = "127.0.0.1:5432")]
    pub spiresql_addr: String,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Cluster management commands
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
    /// Plugin management commands
    Plugin {
        #[command(subcommand)]
        cmd: PluginCmd,
    },
    /// Schema management commands
    Schema {
        #[command(subcommand)]
        cmd: SchemaCmd,
    },
    /// Data access commands
    Data {
        #[command(subcommand)]
        cmd: DataCmd,
    },
    /// Interactive SQL shell
    Sql,
}

#[derive(Subcommand, Debug)]
pub enum ClusterCmd {
    /// Show cluster status
    Status,
    /// List cluster members
    Members,
}

#[derive(Subcommand, Debug)]
pub enum PluginCmd {
    /// List installed plugins
    List,
    /// Install a plugin
    Install {
        /// Plugin source (hex package name, git URL, or path to tarball)
        source: String,
    },
    /// Uninstall a plugin
    Uninstall { name: String },
    /// Generate a new plugin project
    New { name: String },
    /// Build a plugin package
    Build,
}

#[derive(Subcommand, Debug)]
pub enum SchemaCmd {
    /// List tables
    ListTables,
    /// List indexes
    ListIndexes,
}

#[derive(Subcommand, Debug)]
pub enum DataCmd {
    /// Get a value by key
    Get { key: String },
    /// Set a value
    Put { key: String, value: String },
}
