#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spiredb/spiredb/master/art/spire-square.svg"
)]
//! SpireSQL Library
//!
//! SpireSQL Library
//!
//! This crate provides the comprehensive interface for SpireDB, enabling SQL querying, real-time
//! streaming, vector search, and more. It is designed to be used both as a standalone server and
//! as a library for programmatic access to SpireDB's unified capabilities.
//!
//! The main components are exported for integration testing and advanced usage.

pub mod cache;
pub mod config;
pub mod context;
pub mod ddl;
pub mod distributed;
pub mod distributed_exec;
pub mod dml;
pub mod exec;
pub mod filter;
pub mod pool;
pub mod provider;
pub mod pruning;
pub mod routing;
pub mod statistics;
pub mod topology;
