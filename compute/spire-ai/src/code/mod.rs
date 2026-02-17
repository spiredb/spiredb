//! Code indexing and semantic search.
//!
//! Index source code repositories and search them semantically.
//! Uses tree-sitter for language-aware parsing when the `code` feature is enabled.

mod context;
#[cfg(feature = "code")]
mod parser;
mod symbols;
#[cfg(feature = "code")]
mod walker;

pub use context::{CodeContext, ContextBuilder};
pub use symbols::{CodeChunk, CodeHit, SymbolKind};

use crate::client::Spire;
use crate::collection::Collection;
use crate::error::Result;
use crate::types::IndexResult;

/// Index and search a codebase.
#[allow(dead_code)] // spire, name used by index_dir/index_file/search methods
pub struct CodeIndex {
    spire: Spire,
    name: String,
    pub(crate) collection: Collection<CodeChunk>,
}

impl CodeIndex {
    pub(crate) fn new(spire: Spire, name: String) -> Self {
        let collection_name = format!("code_{name}");
        let collection = spire.collection::<CodeChunk>(&collection_name);
        Self {
            spire,
            name,
            collection,
        }
    }

    /// Ensure the backing collection exists.
    pub async fn ensure(&self) -> Result<()> {
        self.collection.ensure().await
    }

    /// Index a directory (respects .gitignore).
    #[cfg(feature = "code")]
    pub async fn index_dir(&self, path: &str) -> Result<IndexResult> {
        let files = walker::walk_dir(path)?;
        let mut total_chunks = 0;
        let mut total_symbols = 0;
        let mut errors = Vec::new();

        for file_path in &files {
            match self.index_file_internal(file_path).await {
                Ok((chunks, symbols)) => {
                    total_chunks += chunks;
                    total_symbols += symbols;
                }
                Err(e) => {
                    errors.push(format!("{file_path}: {e}"));
                }
            }
        }

        Ok(IndexResult {
            files: files.len(),
            chunks: total_chunks,
            symbols: total_symbols,
            errors,
        })
    }

    /// Index a single file.
    #[cfg(feature = "code")]
    pub async fn index_file(&self, path: &str) -> Result<IndexResult> {
        let (chunks, symbols) = self.index_file_internal(path).await?;
        Ok(IndexResult {
            files: 1,
            chunks,
            symbols,
            errors: Vec::new(),
        })
    }

    #[cfg(feature = "code")]
    async fn index_file_internal(&self, path: &str) -> Result<(usize, usize)> {
        let content = tokio::fs::read_to_string(path).await?;
        let language = parser::detect_language(path);
        let chunks = parser::parse_file(path, &content, &language);
        let symbols = chunks.iter().filter(|c| c.name.is_some()).count();
        let count = chunks.len();
        self.collection.insert_many(&chunks).await?;
        Ok((count, symbols))
    }

    /// Semantic code search.
    pub async fn search(&self, query: &str) -> Result<Vec<CodeHit>> {
        let hits = self.collection.search(query).limit(10).run().await?;
        Ok(hits
            .into_iter()
            .map(|h| CodeHit {
                chunk: h.doc,
                score: h.score,
            })
            .collect())
    }

    /// Find symbol by name (SQL filter).
    pub async fn find_symbol(&self, name: &str) -> Result<Vec<CodeChunk>> {
        // Use semantic search with the symbol name as query
        let hits = self.collection.search(name).limit(20).docs().await?;
        Ok(hits
            .into_iter()
            .filter(|c| c.name.as_ref().is_some_and(|n| n.contains(name)))
            .collect())
    }

    /// Get relevant context for an LLM question.
    pub async fn context_for(&self, question: &str) -> Result<CodeContext> {
        self.context(question).build().await
    }

    /// Build a context query with options.
    pub fn context(&self, question: &str) -> ContextBuilder<'_> {
        ContextBuilder::new(self, question.to_string())
    }
}
