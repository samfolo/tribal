//! Ollama inference provider implementations.

mod embed;
mod inference;
mod tags;

#[cfg(feature = "test-helpers")]
pub use embed::EMBED_PATH;
pub use embed::OllamaEmbeddingProvider;
#[cfg(feature = "test-helpers")]
pub use inference::CHAT_PATH;
pub use inference::OllamaInferenceProvider;
#[cfg(feature = "test-helpers")]
pub use tags::TAGS_PATH;
pub use tags::resolve_revision_token as resolve_ollama_revision_token;
