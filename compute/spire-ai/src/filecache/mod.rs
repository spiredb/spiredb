//! File cache — tracks file content changes via hashing.
//!
//! Returns diffs instead of full content on subsequent reads,
//! reducing token consumption for AI agents.

pub mod diff;
pub mod types;

use std::sync::atomic::{AtomicUsize, Ordering};

use kovan_map::HashMap;

use crate::error::Result;
pub use types::{CacheStats, ReadResult};

#[derive(Clone)]
#[allow(dead_code)]
struct CachedFile {
    content: String,
    content_hash: u64,
    lines: usize,
    tokens_estimated: usize,
}

/// A cache that tracks file content and returns diffs on re-reads.
///
/// Uses kovan-map's lock-free HashMap — safe for concurrent access
/// from multiple async tasks.
pub struct FileCache {
    files: HashMap<u64, CachedFile>,
    total_tokens_saved: AtomicUsize,
}

fn hash(s: &str) -> u64 {
    ahash::RandomState::with_seeds(0, 0, 0, 0).hash_one(s)
}

fn estimate_tokens(s: &str) -> usize {
    (s.len() as f64 * 0.75).ceil() as usize
}

impl FileCache {
    /// Create a new file cache.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            total_tokens_saved: AtomicUsize::new(0),
        }
    }

    /// Read a file, returning a cache-aware result.
    ///
    /// - First read: returns full content (`Fresh`).
    /// - Subsequent read, unchanged: returns `Unchanged`.
    /// - Subsequent read, modified: returns a unified diff (`Modified`).
    pub fn read_file(&self, path: &str) -> Result<ReadResult> {
        let content = std::fs::read_to_string(path)?;
        self.process(path, content)
    }

    /// Read a range of lines from a file.
    ///
    /// `offset` is 0-based line index, `limit` is number of lines.
    /// Caching still operates on the full file content.
    pub fn read_file_range(&self, path: &str, offset: usize, limit: usize) -> Result<ReadResult> {
        let full_content = std::fs::read_to_string(path)?;
        let sliced: String = full_content
            .lines()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>()
            .join("\n");

        let lines = sliced.lines().count();
        let tokens = estimate_tokens(&sliced);

        // Still update the full-file cache
        let path_hash = hash(path);
        let content_hash = hash(&full_content);
        let full_lines = full_content.lines().count();
        let full_tokens = estimate_tokens(&full_content);

        self.files.insert(
            path_hash,
            CachedFile {
                content: full_content,
                content_hash,
                lines: full_lines,
                tokens_estimated: full_tokens,
            },
        );

        Ok(ReadResult::Fresh {
            content: sliced,
            lines,
            tokens_estimated: tokens,
        })
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            files_tracked: self.files.len(),
            tokens_saved: self.total_tokens_saved.load(Ordering::Relaxed),
        }
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.files.clear();
        self.total_tokens_saved.store(0, Ordering::Relaxed);
    }

    /// Invalidate a single file entry.
    pub fn invalidate(&self, path: &str) {
        let path_hash = hash(path);
        self.files.remove(&path_hash);
    }

    fn process(&self, path: &str, content: String) -> Result<ReadResult> {
        let path_hash = hash(path);
        let content_hash = hash(&content);
        let lines = content.lines().count();
        let tokens = estimate_tokens(&content);

        if let Some(cached) = self.files.get(&path_hash) {
            if cached.content_hash == content_hash {
                // Unchanged
                self.total_tokens_saved.fetch_add(tokens, Ordering::Relaxed);
                return Ok(ReadResult::Unchanged {
                    path: path.to_string(),
                    lines: cached.lines,
                    tokens_saved: tokens,
                });
            }

            // Modified — produce diff
            let (diff_text, lines_changed) = diff::unified_diff(path, &cached.content, &content);
            let diff_tokens = estimate_tokens(&diff_text);
            let saved = tokens.saturating_sub(diff_tokens);
            self.total_tokens_saved.fetch_add(saved, Ordering::Relaxed);

            // Update cache
            self.files.insert(
                path_hash,
                CachedFile {
                    content,
                    content_hash,
                    lines,
                    tokens_estimated: tokens,
                },
            );

            return Ok(ReadResult::Modified {
                diff: diff_text,
                lines_changed,
                tokens_saved: saved,
            });
        }

        // Fresh — store and return full content
        self.files.insert(
            path_hash,
            CachedFile {
                content: content.clone(),
                content_hash,
                lines,
                tokens_estimated: tokens,
            },
        );

        Ok(ReadResult::Fresh {
            content,
            lines,
            tokens_estimated: tokens,
        })
    }
}

impl Default for FileCache {
    fn default() -> Self {
        Self::new()
    }
}
