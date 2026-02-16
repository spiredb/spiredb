//! SpireAI — High-level AI SDK for SpireDB.
//!
//! Build RAG pipelines, semantic code search, and agent memory
//! on top of SpireDB with minimal boilerplate.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use spire_ai::prelude::*;
//!
//! #[derive(Doc, Serialize, Deserialize, Clone)]
//! struct Article {
//!     #[id]
//!     slug: String,
//!     title: String,
//!     content: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> spire_ai::Result<()> {
//!     let spire = Spire::connect("http://127.0.0.1:50051").await?;
//!     let articles = spire.collection::<Article>("articles");
//!     articles.ensure().await?;
//!
//!     articles.insert(&Article {
//!         slug: "hello".into(),
//!         title: "Hello World".into(),
//!         content: "An introduction to SpireAI.".into(),
//!     }).await?;
//!
//!     let hits = articles.search("introduction").run().await?;
//!     for hit in hits {
//!         println!("{}: {}", hit.score, hit.doc.title);
//!     }
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod collection;
pub mod document;
pub mod search;
pub mod watch;
pub mod embedding;
pub mod llm;
pub mod rag;
pub mod error;
pub mod types;

#[cfg(feature = "code")]
pub mod code;

pub mod agent;

// Re-exports
pub use client::{Spire, SpireBuilder};
pub use collection::Collection;
pub use document::Doc;
pub use search::{Search, Hit, Filter};
pub use watch::{WatchStream, Change};
pub use error::{Error, Result};
pub use types::{IngestResult, IndexResult};

#[cfg(feature = "macros")]
pub use spire_ai_macros::Doc;

/// Prelude — import everything you need with `use spire_ai::prelude::*`.
pub mod prelude {
    pub use crate::client::{Spire, SpireBuilder};
    pub use crate::collection::Collection;
    pub use crate::document::Doc;
    pub use crate::search::{Search, Hit, Filter};
    pub use crate::{WatchStream, Change};
    pub use crate::error::{Error, Result};
    pub use crate::embedding::Embedder;
    pub use crate::rag::{RagPipeline, RagBuilder, ScoredChunk};
    pub use crate::rag::chunker::Chunk;

    #[cfg(feature = "code")]
    pub use crate::code::{CodeIndex, CodeChunk, CodeContext};

    pub use crate::agent::AgentMemory;

    #[cfg(feature = "macros")]
    pub use spire_ai_macros::Doc;

    pub use serde::{Serialize, Deserialize};
}
