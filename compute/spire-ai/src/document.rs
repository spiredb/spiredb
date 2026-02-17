//! The `Doc` trait for documents stored in spire-ai collections.

/// A document that can be stored in a [`Collection`](crate::Collection).
///
/// Implement this trait manually or use `#[derive(Doc)]` with the `macros` feature.
///
/// # Examples
///
/// Manual implementation:
/// ```rust
/// use spire_ai::Doc;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Clone)]
/// struct Article {
///     slug: String,
///     title: String,
///     content: String,
/// }
///
/// impl Doc for Article {
///     fn id(&self) -> &str { &self.slug }
///     fn embed_text(&self) -> String {
///         format!("{}\n\n{}", self.title, self.content)
///     }
/// }
/// ```
pub trait Doc: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static {
    /// Return the document's unique identifier.
    fn id(&self) -> &str;

    /// Return text to generate embeddings from.
    ///
    /// Return an empty string if this document should not be embedded.
    /// The default implementation returns an empty string.
    fn embed_text(&self) -> String {
        String::new()
    }
}
