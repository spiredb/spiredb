use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

/// SpireSQL configuration with defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Listen address for PostgreSQL wire protocol.
    pub listen_addr: String,

    /// SpireDB Cluster gRPC endpoint (PD/Schema/Cluster services on port 50051).
    /// Connects to any node, discovers rest via ListStores.
    pub cluster_addr: String,

    /// SpireDB DataAccess gRPC endpoint (Store services on port 50052).
    /// For INSERT/UPDATE/DELETE operations.
    pub data_access_addr: String,

    /// Maximum number of cached query results.
    pub query_cache_capacity: usize,

    /// Enable query caching.
    pub enable_cache: bool,

    /// Log level (trace, debug, info, warn, error).
    pub log_level: String,

    /// Number of worker threads (0 = auto-detect CPU cores).
    pub num_workers: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5432".to_string(),
            cluster_addr: "http://spiredb:50051".to_string(),
            data_access_addr: "http://spiredb:50052".to_string(),
            query_cache_capacity: 1024,
            enable_cache: true,
            log_level: "info".to_string(),
            num_workers: 0, // 0 = auto-detect
        }
    }
}

impl Config {
    /// Load config from file, falling back to defaults for missing fields.
    pub fn from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Merge CLI args into config (CLI takes precedence).
    pub fn merge_with_cli(&mut self, cli: &CliArgs) {
        if let Some(ref addr) = cli.listen {
            self.listen_addr = addr.clone();
        }
        if let Some(ref addr) = cli.cluster {
            self.cluster_addr = addr.clone();
        }
        if let Some(cap) = cli.cache_capacity {
            self.query_cache_capacity = cap;
        }
        if cli.no_cache {
            self.enable_cache = false;
        }
        if let Some(ref level) = cli.log_level {
            self.log_level = level.clone();
        }
        if let Some(ref addr) = cli.data_access {
            self.data_access_addr = addr.clone();
        }
    }
}

/// SpireSQL - High-performance SQL layer for SpireDB
#[derive(Parser, Debug)]
#[command(name = "spiresql")]
#[command(author, version, about, long_about = None)]
pub struct CliArgs {
    /// Path to configuration file (TOML format)
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Listen address for PostgreSQL wire protocol
    #[arg(short, long)]
    pub listen: Option<String>,

    /// SpireDB Cluster gRPC endpoint (discovers all nodes via ListStores)
    #[arg(long)]
    pub cluster: Option<String>,

    /// SpireDB DataAccess gRPC endpoint (for INSERT/UPDATE/DELETE)
    #[arg(long)]
    pub data_access: Option<String>,

    /// Query cache capacity (number of cached results)
    #[arg(long)]
    pub cache_capacity: Option<usize>,

    /// Disable query caching
    #[arg(long)]
    pub no_cache: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    pub log_level: Option<String>,
}

/// Parse CLI args and load config.
pub fn load_config() -> Config {
    let cli = CliArgs::parse();

    // Start with defaults
    let mut config = if let Some(ref path) = cli.config {
        match Config::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Warning: Failed to load config from {:?}: {}", path, e);
                Config::default()
            }
        }
    } else {
        Config::default()
    };

    // CLI args override config file
    config.merge_with_cli(&cli);

    config
}

/// Print ASCII banner.
pub fn print_banner() {
    const BANNER: &str = r#"
                                                                        
 ▄▄▄▄▄▄▄ ▄▄▄▄▄▄▄   ▄▄▄▄▄ ▄▄▄▄▄▄▄    ▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄   ▄▄▄▄▄   ▄▄▄      
█████▀▀▀ ███▀▀███▄  ███  ███▀▀███▄ ███▀▀▀▀▀ █████▀▀▀ ▄███████▄ ███      
 ▀████▄  ███▄▄███▀  ███  ███▄▄███▀ ███▄▄     ▀████▄  ███   ███ ███      
   ▀████ ███▀▀▀▀    ███  ███▀▀██▄  ███         ▀████ ███▄█▄███ ███      
███████▀ ███       ▄███▄ ███  ▀███ ▀███████ ███████▀  ▀█████▀  ████████ 
                                                           ▀▀           
                                                                        
"#;

    println!("{}", BANNER);
    println!("  Spire Compute Layer");
    println!("  ════════════════════════════════════════\n");
}
