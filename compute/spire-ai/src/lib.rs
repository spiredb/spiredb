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
pub mod embedding;
pub mod error;
pub mod llm;
pub mod rag;
pub mod search;
pub mod types;
pub mod watch;

#[cfg(feature = "code")]
pub mod code;

pub mod filecache;

pub mod agent;
pub mod tool;

// Re-exports
pub use client::{Spire, SpireBuilder};
pub use collection::Collection;
pub use document::Doc;
pub use error::{Error, Result};
pub use filecache::FileCache;
pub use search::{Filter, Hit, Search};
pub use types::{IndexResult, IngestResult};
pub use watch::{Change, WatchStream};

#[cfg(feature = "macros")]
pub use spire_ai_macros::Doc;

/// Prelude — import everything you need with `use spire_ai::prelude::*`.
pub mod prelude {
    pub use crate::client::{Spire, SpireBuilder};
    pub use crate::collection::Collection;
    pub use crate::document::Doc;
    pub use crate::embedding::Embedder;
    pub use crate::error::{Error, Result};
    pub use crate::rag::chunker::Chunk;
    pub use crate::rag::{RagBuilder, RagPipeline, ScoredChunk};
    pub use crate::search::{Filter, Hit, Search};
    pub use crate::{Change, WatchStream};

    #[cfg(feature = "code")]
    pub use crate::code::{CodeChunk, CodeContext, CodeIndex};

    pub use crate::filecache::{CacheStats, FileCache, ReadResult};

    pub use crate::agent::{AgentLoop, AgentLoopConfig, AgentMemory};
    pub use crate::llm::{ChatMessage, ChatResponse, ToolDef};
    pub use crate::tool::{Tool, ToolRegistry, ToolResult};

    #[cfg(feature = "macros")]
    pub use spire_ai_macros::Doc;

    pub use serde::{Deserialize, Serialize};
}
