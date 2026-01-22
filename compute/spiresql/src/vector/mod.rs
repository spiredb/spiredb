//! Vector Search API for SpireDB
//!
//! Provides vector similarity search operations via gRPC.
//!
//! # Operations
//!
//! | Operation     | Description                           |
//! |---------------|---------------------------------------|
//! | `create_index`| Create a vector index                 |
//! | `drop_index`  | Drop a vector index                   |
//! | `insert`      | Insert vector with optional payload   |
//! | `delete`      | Delete vector by document ID          |
//! | `search`      | Find k nearest neighbors              |
//! | `batch_search`| Batch multiple search queries         |
//!
//! # Example
//!
//! ```rust,ignore
//! use spiresql::vector::prelude::*;
//!
//! let v = SpireVector::connect("http://spiredb:50052").await?;
//! v.insert("idx", b"doc-1", &vec, Some(payload)).await?;
//! let results = v.search("idx", &query, SearchOptions::default().k(10)).await?;
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod types;

pub mod prelude {
    pub use super::client::{SpireVector, SpireVectorBuilder, VectorService};
    pub use super::config::VectorConfig;
    pub use super::error::{VectorError, VectorResult};
    pub use super::types::{Algorithm, IndexParams, SearchOptions, VectorResult as VResult};
}
