use crate::error::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct SpireConfig {
    pub pd_addr: String,
    pub data_addr: String,
    pub spiresql_addr: String,
    pub log_level: String,
}

impl Default for SpireConfig {
    fn default() -> Self {
        Self {
            pd_addr: "http://127.0.0.1:50051".to_string(),
            data_addr: "http://127.0.0.1:50052".to_string(),
            spiresql_addr: "127.0.0.1:5432".to_string(),
            log_level: "info".to_string(),
        }
    }
}

impl SpireConfig {
    pub fn load() -> Result<Self> {
        let builder = Config::builder()
            .add_source(File::with_name("spire").required(false))
            .add_source(Environment::with_prefix("SPIRE").separator("__"));

        let config = builder.build()?;
        Ok(config.try_deserialize()?)
    }
}
