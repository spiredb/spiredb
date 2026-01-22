//! SQL Execution Infrastructure for SpireDB
//!
//! This module provides the complete SQL query execution layer:
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                      SQL Query Processing                        │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  [context]   Global query context with session state            │
//! │  [config]    Configuration loading and CLI parsing              │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                      DDL/DML Handlers                            │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  [ddl]       CREATE/DROP TABLE/INDEX via gRPC                   │
//! │  [dml]       INSERT/UPDATE/DELETE via gRPC                      │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                   Query Execution Layer                          │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  [exec]           Single-shard execution plans                  │
//! │  [distributed]    Multi-shard parallel execution                │
//! │  [distributed_exec] DataFusion ExecutionPlan impl               │
//! │  [provider]       DataFusion TableProvider impl                 │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                   Optimization & Routing                         │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  [filter]     Predicate serialization for pushdown              │
//! │  [pruning]    Key-range extraction for region filtering         │
//! │  [routing]    Region discovery and query routing                │
//! │  [statistics] Table stats for cost-based optimization           │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                   Infrastructure                                 │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  [cache]      LRU query result caching                          │
//! │  [pool]       Lock-free gRPC connection pooling                 │
//! │  [topology]   Cluster topology monitoring                       │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use spiresql::sql::{context::SpireContext, config::Config, ddl::DdlHandler, dml::DmlHandler};
//!
//! // Initialize context with gRPC connections
//! let ctx = SpireContext::new(schema_client, cluster_client, &config);
//! ctx.register_tables().await?;
//!
//! // Execute SQL queries via DataFusion
//! let results = ctx.session().sql("SELECT * FROM users").await?;
//! ```

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

// Re-exports for convenience
pub use config::Config;
pub use context::SpireContext;
pub use ddl::DdlHandler;
pub use dml::DmlHandler;
pub use pool::ConnectionPool;
pub use routing::RegionRouter;
pub use topology::ClusterTopology;
