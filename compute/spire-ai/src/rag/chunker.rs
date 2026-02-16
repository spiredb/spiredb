//! Text chunking strategies for RAG pipelines.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

use crate::document::Doc;
use crate::error::Result;

/// A chunk of text from a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: String,
    pub text: String,
    pub source: String,
    pub start: usize,
    pub end: usize,
    pub metadata: serde_json::Value,
}

impl Doc for Chunk {
    fn id(&self) -> &str {
        &self.id
    }

    fn embed_text(&self) -> String {
        self.text.clone()
    }
}

/// Trait for text chunking strategies.
#[async_trait]
pub trait ChunkerFn: Send + Sync {
    /// Split text into chunks.
    async fn chunk(&self, text: &str, source: &str) -> Result<Vec<Chunk>>;
}

/// Fixed-size character chunker with overlap.
pub struct FixedChunker {
    size: usize,
    overlap: usize,
}

impl FixedChunker {
    pub fn new(size: usize, overlap: usize) -> Self {
        Self { size, overlap }
    }
}

#[async_trait]
impl ChunkerFn for FixedChunker {
    async fn chunk(&self, text: &str, source: &str) -> Result<Vec<Chunk>> {
        let chars: Vec<char> = text.chars().collect();
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut idx = 0;

        while start < chars.len() {
            let end = (start + self.size).min(chars.len());
            let chunk_text: String = chars[start..end].iter().collect();

            chunks.push(Chunk {
                id: format!("{source}:chunk-{idx}"),
                text: chunk_text,
                source: source.to_string(),
                start,
                end,
                metadata: serde_json::json!({}),
            });

            if end >= chars.len() {
                break;
            }

            start += self.size - self.overlap;
            idx += 1;
        }

        Ok(chunks)
    }
}

/// Sentence-based chunker that groups sentences together.
pub struct SentenceChunker {
    sentences_per_chunk: usize,
}

impl SentenceChunker {
    pub fn new(sentences_per_chunk: usize) -> Self {
        Self {
            sentences_per_chunk,
        }
    }
}

#[async_trait]
impl ChunkerFn for SentenceChunker {
    async fn chunk(&self, text: &str, source: &str) -> Result<Vec<Chunk>> {
        let sentences: Vec<&str> = text.split_sentence_bounds().collect();
        let mut chunks = Vec::new();
        let mut idx = 0;
        let mut pos = 0;

        for group in sentences.chunks(self.sentences_per_chunk) {
            let chunk_text: String = group.concat();
            let start = pos;
            let end = pos + chunk_text.len();

            if !chunk_text.trim().is_empty() {
                chunks.push(Chunk {
                    id: format!("{source}:sent-{idx}"),
                    text: chunk_text.trim().to_string(),
                    source: source.to_string(),
                    start,
                    end,
                    metadata: serde_json::json!({}),
                });
                idx += 1;
            }

            pos = end;
        }

        Ok(chunks)
    }
}

/// Markdown-aware chunker that splits on headers.
pub struct MarkdownChunker;

#[async_trait]
impl ChunkerFn for MarkdownChunker {
    async fn chunk(&self, text: &str, source: &str) -> Result<Vec<Chunk>> {
        let mut chunks = Vec::new();
        let mut current_text = String::new();
        let mut current_start = 0;
        let mut idx = 0;
        let mut pos = 0;

        for line in text.lines() {
            let is_header = line.starts_with('#');

            if is_header && !current_text.trim().is_empty() {
                // Flush current chunk
                chunks.push(Chunk {
                    id: format!("{source}:md-{idx}"),
                    text: current_text.trim().to_string(),
                    source: source.to_string(),
                    start: current_start,
                    end: pos,
                    metadata: serde_json::json!({}),
                });
                idx += 1;
                current_text.clear();
                current_start = pos;
            }

            current_text.push_str(line);
            current_text.push('\n');
            pos += line.len() + 1;
        }

        // Flush remaining
        if !current_text.trim().is_empty() {
            chunks.push(Chunk {
                id: format!("{source}:md-{idx}"),
                text: current_text.trim().to_string(),
                source: source.to_string(),
                start: current_start,
                end: pos,
                metadata: serde_json::json!({}),
            });
        }

        Ok(chunks)
    }
}
