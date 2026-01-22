//! Core vector types.

/// Vector search result
#[derive(Debug, Clone, PartialEq)]
pub struct VectorResult {
    /// Document ID
    pub id: Vec<u8>,
    /// Distance from query vector
    pub distance: f32,
    /// Optional payload (JSON metadata)
    pub payload: Option<Vec<u8>>,
}

/// Search configuration
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Number of results to return
    pub k: u32,
    /// Range search radius (0 = disabled)
    pub radius: f32,
    /// Metadata filter
    pub filter: Option<Vec<u8>>,
    /// Include payload in results
    pub return_payload: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            k: 10,
            radius: 0.0,
            filter: None,
            return_payload: false,
        }
    }
}

impl SearchOptions {
    /// Set number of results to return
    pub fn k(mut self, k: u32) -> Self {
        self.k = k;
        self
    }

    /// Set range search radius
    pub fn radius(mut self, r: f32) -> Self {
        self.radius = r;
        self
    }

    /// Set metadata filter
    pub fn filter(mut self, f: Vec<u8>) -> Self {
        self.filter = Some(f);
        self
    }

    /// Include payload in results
    pub fn with_payload(mut self) -> Self {
        self.return_payload = true;
        self
    }
}

/// Index algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Algorithm {
    /// Single-shard ANODE index
    #[default]
    Anode,
    /// Multi-shard MANODE index (recommended for databases)
    Manode,
}

impl Algorithm {
    /// Get string representation for proto
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::Anode => "ANODE",
            Algorithm::Manode => "MANODE",
        }
    }
}

/// Index creation parameters
#[derive(Debug, Clone)]
pub struct IndexParams {
    /// Index name
    pub name: String,
    /// Table name
    pub table_name: String,
    /// Column name
    pub column_name: String,
    /// Algorithm type
    pub algorithm: Algorithm,
    /// Vector dimensions
    pub dimensions: u32,
    /// Number of shards (for MANODE)
    pub shards: u32,
}

impl IndexParams {
    /// Create new index parameters
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table_name: table.into(),
            column_name: column.into(),
            algorithm: Algorithm::default(),
            dimensions: 128,
            shards: 4,
        }
    }

    /// Set algorithm
    pub fn algorithm(mut self, a: Algorithm) -> Self {
        self.algorithm = a;
        self
    }

    /// Set dimensions
    pub fn dimensions(mut self, d: u32) -> Self {
        self.dimensions = d;
        self
    }

    /// Set number of shards
    pub fn shards(mut self, s: u32) -> Self {
        self.shards = s;
        self
    }
}
