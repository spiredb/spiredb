//! Agent memory for AI agents.
//!
//! Provides long-term memory that agents can use to store and recall
//! information across conversations.

mod types;

pub use types::{Importance, Memory};

use crate::client::Spire;
use crate::collection::Collection;
use crate::error::Result;

/// Long-term memory store for an AI agent.
#[allow(dead_code)] // spire used by remember/recall methods
pub struct AgentMemory {
    spire: Spire,
    agent_id: String,
    collection: Collection<Memory>,
}

impl AgentMemory {
    pub(crate) fn new(spire: Spire, agent_id: String) -> Self {
        let collection_name = format!("memory_{agent_id}");
        let collection = spire.collection::<Memory>(&collection_name);
        Self {
            spire,
            agent_id,
            collection,
        }
    }

    /// Ensure the backing collection exists.
    pub async fn ensure(&self) -> Result<()> {
        self.collection.ensure().await
    }

    /// Store a memory with default importance.
    pub async fn remember(&self, content: &str) -> Result<String> {
        self.remember_with(content, Importance::Normal).await
    }

    /// Store a memory with a specific importance level.
    pub async fn remember_with(&self, content: &str, importance: Importance) -> Result<String> {
        let id = format!(
            "{}-{}",
            self.agent_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        let memory = Memory {
            id: id.clone(),
            agent_id: self.agent_id.clone(),
            content: content.to_string(),
            importance,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.collection.insert(&memory).await?;
        Ok(id)
    }

    /// Recall memories relevant to a query.
    pub async fn recall(&self, query: &str) -> Result<Vec<Memory>> {
        self.recall_limit(query, 10).await
    }

    /// Recall memories with a specific limit.
    pub async fn recall_limit(&self, query: &str, limit: usize) -> Result<Vec<Memory>> {
        self.collection.search(query).limit(limit).docs().await
    }

    /// Forget (delete) a specific memory by ID.
    pub async fn forget(&self, id: &str) -> Result<bool> {
        self.collection.delete(id).await
    }
}
