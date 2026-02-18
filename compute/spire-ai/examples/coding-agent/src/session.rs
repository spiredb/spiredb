use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use spire_ai::prelude::*;

use std::hash::{Hash, Hasher};
use std::path::Path;

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
pub struct Session {
    #[id]
    pub id: String,
    pub agent_id: String,
    #[embed]
    pub summary: String,
    pub project_dir: String,
    pub created_at: String,
    pub last_active: String,
    pub turn_count: u64,
}

#[derive(Doc, Serialize, Deserialize, Clone, Debug)]
pub struct ConversationTurn {
    #[id]
    pub id: String,
    pub session_id: String,
    pub role: String,
    #[embed]
    pub content: String,
    pub timestamp: String,
}

pub fn project_key(project_dir: &str) -> String {
    let canonical = std::fs::canonicalize(project_dir)
        .unwrap_or_else(|_| Path::new(project_dir).to_path_buf());
    let mut hasher = std::hash::DefaultHasher::new();
    canonical.hash(&mut hasher);
    let hash = hasher.finish();
    let name = canonical
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "project".to_string());
    format!("{}_{:08x}", name, hash as u32)
}

pub fn create_session(agent_id: &str, project_dir: &str, id: Option<String>) -> Session {
    let now = Utc::now().to_rfc3339();
    Session {
        id: id.unwrap_or_else(|| Uuid::new_v4().to_string()[..8].to_string()),
        agent_id: agent_id.to_string(),
        summary: format!("Coding session on {project_dir}"),
        project_dir: project_dir.to_string(),
        created_at: now.clone(),
        last_active: now,
        turn_count: 0,
    }
}
