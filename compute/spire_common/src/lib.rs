pub mod config;
pub mod error;

pub use config::SpireConfig;
pub use error::SpireError;

pub fn init_logging() {
    tracing_subscriber::fmt::init();
}
