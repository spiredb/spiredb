#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spiredb/spiredb/master/art/spire-square.svg"
)]
//! SpireDB Common Library
//!
//! This crate contains shared utilities, error types, and configuration structures used across
//! the SpireDB ecosystem. It is intended for internal use by SpireDB components.

pub mod config;
pub mod error;

pub use config::SpireConfig;
pub use error::SpireError;

pub fn init_logging() {
    tracing_subscriber::fmt::init();
}
