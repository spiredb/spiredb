use thiserror::Error;

#[derive(Error, Debug)]
pub enum SpireError {
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("GRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Unknown error: {0}")]
    Unknown(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SpireError>;
