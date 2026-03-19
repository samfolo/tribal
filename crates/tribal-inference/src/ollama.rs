//! Ollama inference provider implementations.

mod embed;
mod inference;
mod tags;

pub use embed::OllamaEmbeddingProvider;
#[cfg(feature = "test-helpers")]
pub use embed::EMBED_PATH;
pub use inference::OllamaInferenceProvider;
#[cfg(feature = "test-helpers")]
pub use inference::CHAT_PATH;
#[cfg(feature = "test-helpers")]
pub use tags::TAGS_PATH;
