//! Kafka-like Streaming API for SpireDB
//!
//! Provides familiar Producer/Consumer patterns on top of SpireDB's streaming infrastructure.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                        Producer                              │
//! ├──────────────────────────────────────────────────────────────┤
//! │  send()        Async send with delivery confirmation         │
//! │  flush()       Wait for all pending sends                    │
//! │  transaction() Exactly-once transactional produce            │
//! ├──────────────────────────────────────────────────────────────┤
//! │                        Consumer                              │
//! ├──────────────────────────────────────────────────────────────┤
//! │  subscribe()   Subscribe to topics                           │
//! │  poll()        Fetch records (with timeout)                  │
//! │  commit()      Manual offset commit                          │
//! │  seek()        Seek to specific offset                       │
//! │  pause/resume  Flow control                                  │
//! ├──────────────────────────────────────────────────────────────┤
//! │                    Consumer Group                            │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Automatic offset management                                 │
//! │  Rebalancing on member join/leave                            │
//! │  Exactly-once with transactional consume-transform-produce   │
//! ├──────────────────────────────────────────────────────────────┤
//! │                    Admin / CDC                               │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Topic management (create, delete, describe)                 │
//! │  CDC subscriptions for table changes                         │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ## Producer
//!
//! ```rust,ignore
//! use spiresql::stream::{Producer, Record, Acks};
//!
//! let producer = Producer::builder()
//!     .bootstrap_servers("spiredb:6379")
//!     .acks(Acks::All)
//!     .idempotence(true)
//!     .build()
//!     .await?;
//!
//! let metadata = producer.send(
//!     Record::new("orders")
//!         .key("order-123")
//!         .value(b"data")
//! ).await?;
//!
//! producer.flush().await?;
//! ```
//!
//! ## Consumer
//!
//! ```rust,ignore
//! use spiresql::stream::{Consumer, OffsetReset};
//!
//! let consumer = Consumer::builder()
//!     .bootstrap_servers("spiredb:6379")
//!     .group_id("order-processor")
//!     .auto_offset_reset(OffsetReset::Earliest)
//!     .build()
//!     .await?;
//!
//! consumer.subscribe(&["orders"]).await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_millis(100)).await?;
//!     for record in records {
//!         process(&record).await;
//!     }
//!     consumer.commit().await?;
//! }
//! ```
//!
//! ## CDC (Change Data Capture)
//!
//! ```rust,ignore
//! use spiresql::stream::{CdcBuilder, Op};
//!
//! let cdc = CdcBuilder::new("spiredb:6379")
//!     .tables(&["users", "orders"])
//!     .operations(&[Op::Insert, Op::Update, Op::Delete])
//!     .from_now()
//!     .build()
//!     .await?;
//!
//! while let Some(change) = cdc.poll().await? {
//!     match change.op {
//!         Op::Insert => handle_insert(change.after),
//!         Op::Update => handle_update(change.before, change.after),
//!         Op::Delete => handle_delete(change.before),
//!     }
//! }
//! ```

// Core types
pub mod types;

// Client
pub mod client;

// Producer
pub mod producer;

// Consumer
pub mod consumer;

// CDC
pub mod cdc;

// Admin
pub mod admin;

// Serialization
pub mod serde;

// Interceptors
pub mod interceptor;

// Configuration
pub mod config;

// Errors
pub mod error;

/// Prelude for convenient imports - `use spiresql::stream::prelude::*;`
#[allow(unused_imports)]
pub mod prelude {
    // Types
    pub use super::types::*;

    // Error types
    pub use super::error::*;

    // Configuration
    pub use super::config::*;

    // Client
    pub use super::client::*;

    // Producer
    pub use super::producer::*;

    // Consumer
    pub use super::consumer::*;

    // CDC
    pub use super::cdc::*;

    // Admin
    pub use super::admin::*;

    // Serde
    pub use super::serde::*;

    // Interceptors
    pub use super::interceptor::*;
}
