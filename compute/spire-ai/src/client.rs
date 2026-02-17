//! Spire client and builder.

use std::sync::Arc;

use kovan_map::HashMap;
use spiresql::vector::client::{SpireVector, VectorService};
use tonic::transport::Channel;

use crate::collection::Collection;
use crate::document::Doc;
use crate::embedding::{Embedder, NoOpEmbedder, cache::CachedEmbedder};
use crate::error::{Error, Result};
use crate::llm::Llm;
use crate::rag::RagBuilder;

#[cfg(feature = "code")]
use crate::code::CodeIndex;

/// Main entry point for spire-ai. Holds connections to SpireDB services.
#[derive(Clone)]
pub struct Spire {
    pub(crate) inner: Arc<SpireInner>,
}

#[allow(dead_code)] // pd_addr, data_addr, data_channel used in future DataAccess integration
pub(crate) struct SpireInner {
    pub(crate) vector: Arc<dyn VectorService>,
    pub(crate) stream_addr: String,
    pub(crate) pd_addr: String,
    pub(crate) data_addr: String,
    pub(crate) embedder: Arc<dyn Embedder>,
    pub(crate) llm: Option<Arc<dyn Llm>>,
    pub(crate) pd_channel: Channel,
    pub(crate) data_channel: Channel,
    /// In-memory document cache: hash(collection_name + doc_id) -> serialized doc bytes.
    /// Used by Collection::get() until DataAccess TableGet is implemented.
    pub(crate) doc_cache: HashMap<u64, Vec<u8>>,
}

impl Spire {
    /// Connect with Ollama defaults (nomic-embed-text at localhost:11434).
    ///
    /// Expects SpireDB at default addresses:
    /// - PD: `http://127.0.0.1:50051`
    /// - DataAccess/Vector: `http://127.0.0.1:50052`
    /// - Streams: `127.0.0.1:6379`
    #[cfg(feature = "ollama")]
    pub async fn connect(ollama_url: &str) -> Result<Self> {
        SpireBuilder::new()
            .ollama(ollama_url, "nomic-embed-text")
            .build()
            .await
    }

    /// Create a builder for custom configuration.
    pub fn builder() -> SpireBuilder {
        SpireBuilder::new()
    }

    /// Get a typed collection.
    pub fn collection<T: Doc>(&self, name: &str) -> Collection<T> {
        Collection::new(self.clone(), name.to_string())
    }

    /// Create a RAG pipeline builder.
    pub fn rag(&self, name: &str) -> RagBuilder {
        RagBuilder::new(self.clone(), name.to_string())
    }

    /// Create a code index.
    #[cfg(feature = "code")]
    pub fn code(&self, name: &str) -> CodeIndex {
        CodeIndex::new(self.clone(), name.to_string())
    }

    /// Create agent memory.
    pub fn memory(&self, agent_id: &str) -> crate::agent::AgentMemory {
        crate::agent::AgentMemory::new(self.clone(), agent_id.to_string())
    }

    /// Get a reference to the configured embedder.
    pub fn embedder(&self) -> &dyn Embedder {
        self.inner.embedder.as_ref()
    }

    /// Get a reference to the configured LLM (if any).
    pub fn llm(&self) -> Option<&dyn Llm> {
        self.inner.llm.as_deref()
    }
}

/// Builder for configuring a [`Spire`] client.
pub struct SpireBuilder {
    pd_addr: String,
    data_addr: String,
    stream_addr: String,
    embedder: Option<Arc<dyn Embedder>>,
    llm: Option<Arc<dyn Llm>>,
    enable_cache: bool,
}

impl SpireBuilder {
    /// Create a new builder with default addresses.
    pub fn new() -> Self {
        Self {
            pd_addr: "http://127.0.0.1:50051".to_string(),
            data_addr: "http://127.0.0.1:50052".to_string(),
            stream_addr: "127.0.0.1:6379".to_string(),
            embedder: None,
            llm: None,
            enable_cache: true,
        }
    }

    /// Set the PD (Placement Driver) gRPC address.
    pub fn pd_addr(mut self, addr: impl Into<String>) -> Self {
        self.pd_addr = addr.into();
        self
    }

    /// Set the DataAccess/Vector gRPC address.
    pub fn data_addr(mut self, addr: impl Into<String>) -> Self {
        self.data_addr = addr.into();
        self
    }

    /// Set the stream (RESP) address.
    pub fn stream_addr(mut self, addr: impl Into<String>) -> Self {
        self.stream_addr = addr.into();
        self
    }

    // -- Embedder configuration --

    /// Use Ollama with a specific model.
    #[cfg(feature = "ollama")]
    pub fn ollama(mut self, url: &str, model: &str) -> Self {
        self.embedder = Some(Arc::new(
            crate::embedding::ollama::OllamaEmbedder::with_model(url, model),
        ));
        self
    }

    /// Use Ollama with default model (nomic-embed-text at localhost:11434).
    #[cfg(feature = "ollama")]
    pub fn ollama_default(self) -> Self {
        self.ollama("http://localhost:11434", "nomic-embed-text")
    }

    /// Use OpenAI with default model (text-embedding-3-small).
    #[cfg(feature = "openai")]
    pub fn openai(mut self, api_key: &str) -> Self {
        self.embedder = Some(Arc::new(crate::embedding::openai::OpenAIEmbedder::new(
            api_key,
        )));
        self
    }

    /// Use OpenAI with a specific model.
    #[cfg(feature = "openai")]
    pub fn openai_model(mut self, api_key: &str, model: &str) -> Self {
        self.embedder = Some(Arc::new(
            crate::embedding::openai::OpenAIEmbedder::with_model(api_key, model),
        ));
        self
    }

    /// Use Voyage AI with a specific model.
    #[cfg(feature = "voyage")]
    pub fn voyage(mut self, api_key: &str, model: &str) -> Self {
        self.embedder = Some(Arc::new(crate::embedding::voyage::VoyageEmbedder::new(
            api_key, model,
        )));
        self
    }

    /// Use a custom embedder.
    pub fn embedder(mut self, e: Arc<dyn Embedder>) -> Self {
        self.embedder = Some(e);
        self
    }

    /// Disable embeddings (use NoOpEmbedder). Useful for testing.
    pub fn no_embeddings(mut self) -> Self {
        self.embedder = Some(Arc::new(NoOpEmbedder));
        self.enable_cache = false;
        self
    }

    /// Disable the embedding cache.
    pub fn no_cache(mut self) -> Self {
        self.enable_cache = false;
        self
    }

    // -- LLM configuration --

    /// Use Ollama for LLM generation.
    #[cfg(feature = "ollama")]
    pub fn ollama_llm(mut self, url: &str, model: &str) -> Self {
        self.llm = Some(Arc::new(crate::llm::ollama::OllamaLlm::new(url, model)));
        self
    }

    /// Use OpenAI for LLM generation.
    #[cfg(feature = "openai")]
    pub fn openai_llm(mut self, api_key: &str, model: &str) -> Self {
        self.llm = Some(Arc::new(crate::llm::openai::OpenAiLlm::new(api_key, model)));
        self
    }

    /// Use Anthropic Claude for LLM generation.
    #[cfg(feature = "anthropic")]
    pub fn anthropic_llm(mut self, api_key: &str, model: &str) -> Self {
        self.llm = Some(Arc::new(crate::llm::anthropic::AnthropicLlm::new(
            api_key, model,
        )));
        self
    }

    /// Use a custom LLM.
    pub fn llm(mut self, l: Arc<dyn Llm>) -> Self {
        self.llm = Some(l);
        self
    }

    /// Build the Spire client, connecting to SpireDB.
    pub async fn build(self) -> Result<Spire> {
        // Connect vector client
        let vector = SpireVector::from_endpoint(&self.data_addr)
            .await
            .map_err(|e| Error::Connection(format!("vector service: {e}")))?;

        // Create gRPC channels for schema and data
        let pd_channel = Channel::from_shared(self.pd_addr.clone())
            .map_err(|e| Error::Connection(format!("invalid PD address: {e}")))?
            .connect()
            .await
            .map_err(|e| Error::Connection(format!("PD connection failed: {e}")))?;

        let data_channel = Channel::from_shared(self.data_addr.clone())
            .map_err(|e| Error::Connection(format!("invalid data address: {e}")))?
            .connect()
            .await
            .map_err(|e| Error::Connection(format!("data connection failed: {e}")))?;

        // Set up embedder (default to NoOpEmbedder if none configured)
        let embedder: Arc<dyn Embedder> = match self.embedder {
            Some(e) if self.enable_cache => Arc::new(CachedEmbedder::new(e)),
            Some(e) => e,
            None => Arc::new(NoOpEmbedder),
        };

        Ok(Spire {
            inner: Arc::new(SpireInner {
                vector: Arc::new(vector),
                stream_addr: self.stream_addr,
                pd_addr: self.pd_addr,
                data_addr: self.data_addr,
                embedder,
                llm: self.llm,
                pd_channel,
                data_channel,
                doc_cache: HashMap::new(),
            }),
        })
    }
}

impl Default for SpireBuilder {
    fn default() -> Self {
        Self::new()
    }
}
