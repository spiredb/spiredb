//! Agent memory types.

use serde::{Deserialize, Serialize};

use crate::document::Doc;

/// A stored memory entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    pub id: String,
    pub agent_id: String,
    pub content: String,
    pub importance: Importance,
    pub timestamp: u64,
}

impl Doc for Memory {
    fn id(&self) -> &str {
        &self.id
    }

    fn embed_text(&self) -> String {
        self.content.clone()
    }
}

/// Memory importance level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Importance {
    Low,
    Normal,
    High,
    Critical,
}
