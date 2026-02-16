//! Code context assembly for LLM consumption.

use crate::code::CodeIndex;
use crate::code::symbols::CodeChunk;
use crate::error::Result;

/// Assembled code context ready for LLM consumption.
#[derive(Debug, Clone)]
pub struct CodeContext {
    /// Formatted context string
    pub text: String,
    /// Source chunks used
    pub chunks: Vec<CodeChunk>,
    /// Approximate token count
    pub tokens: usize,
}

impl CodeContext {
    /// Format as a system prompt.
    pub fn as_system_prompt(&self) -> String {
        format!(
            "You have access to the following code context:\n\n{}",
            self.text
        )
    }

    /// Format as user context for an LLM message.
    pub fn as_user_context(&self) -> String {
        format!("Relevant code:\n\n{}\n\nQuestion: ", self.text)
    }
}

/// Builder for assembling code context.
pub struct ContextBuilder<'a> {
    code_index: &'a CodeIndex,
    question: String,
    max_tokens: usize,
    max_chunks: usize,
}

impl<'a> ContextBuilder<'a> {
    pub(crate) fn new(code_index: &'a CodeIndex, question: String) -> Self {
        Self {
            code_index,
            question,
            max_tokens: 4000,
            max_chunks: 10,
        }
    }

    /// Set the maximum approximate token count (default: 4000).
    pub fn max_tokens(mut self, n: usize) -> Self {
        self.max_tokens = n;
        self
    }

    /// Set the maximum number of chunks to include (default: 10).
    pub fn max_chunks(mut self, n: usize) -> Self {
        self.max_chunks = n;
        self
    }

    /// Build the context.
    pub async fn build(self) -> Result<CodeContext> {
        let hits = self
            .code_index
            .collection
            .search(&self.question)
            .limit(self.max_chunks)
            .run()
            .await?;

        let mut chunks = Vec::new();
        let mut text = String::new();
        let mut approx_tokens = 0;

        for hit in hits {
            let chunk = hit.doc;
            // Rough token estimate: 1 token ~= 4 chars
            let chunk_tokens = chunk.code.len() / 4;

            if approx_tokens + chunk_tokens > self.max_tokens {
                break;
            }

            // Format the chunk
            text.push_str(&format!(
                "// File: {} (lines {}-{})\n",
                chunk.file, chunk.start_line, chunk.end_line
            ));
            if let Some(ref name) = chunk.name {
                text.push_str(&format!("// Symbol: {} ({:?})\n", name, chunk.kind));
            }
            text.push_str(&chunk.code);
            text.push_str("\n\n");

            approx_tokens += chunk_tokens;
            chunks.push(chunk);
        }

        Ok(CodeContext {
            text,
            chunks,
            tokens: approx_tokens,
        })
    }
}
