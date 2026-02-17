//! Types for the file cache module.

use serde::{Deserialize, Serialize};

/// Result of reading a file through the cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadResult {
    /// First time reading this file — full content returned.
    Fresh {
        content: String,
        lines: usize,
        tokens_estimated: usize,
    },
    /// File has not changed since last read.
    Unchanged {
        path: String,
        lines: usize,
        tokens_saved: usize,
    },
    /// File changed — unified diff returned instead of full content.
    Modified {
        diff: String,
        lines_changed: usize,
        tokens_saved: usize,
    },
}

/// Statistics about cache usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub files_tracked: usize,
    pub tokens_saved: usize,
}
