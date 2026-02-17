//! Code symbol types.

use serde::{Deserialize, Serialize};

use crate::document::Doc;

/// A chunk of parsed code (function, class, block, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeChunk {
    pub id: String,
    /// The code text
    pub code: String,
    /// File path relative to repo root
    pub file: String,
    /// Language (rust, python, etc.)
    pub language: String,
    /// Symbol type
    pub kind: SymbolKind,
    /// Symbol name (if applicable)
    pub name: Option<String>,
    /// Start line in original file
    pub start_line: usize,
    /// End line in original file
    pub end_line: usize,
    /// Parent symbol (e.g., class for a method)
    pub parent: Option<String>,
    /// Doc comment / docstring
    pub docs: Option<String>,
    /// Function/method signature
    pub signature: Option<String>,
}

impl Doc for CodeChunk {
    fn id(&self) -> &str {
        &self.id
    }

    fn embed_text(&self) -> String {
        let mut text = String::new();
        if let Some(docs) = &self.docs {
            text.push_str(docs);
            text.push_str("\n\n");
        }
        if let Some(sig) = &self.signature {
            text.push_str(sig);
            text.push_str("\n\n");
        }
        text.push_str(&self.code);
        text
    }
}

/// The type of a code symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SymbolKind {
    Function,
    Method,
    Class,
    Struct,
    Trait,
    Interface,
    Enum,
    Module,
    Variable,
    Constant,
    Import,
    Block,
}

/// A code search hit with score.
#[derive(Debug, Clone)]
pub struct CodeHit {
    pub chunk: CodeChunk,
    pub score: f32,
}
