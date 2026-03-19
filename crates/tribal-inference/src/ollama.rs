//! Ollama inference provider implementations.

mod embed;
mod inference;
mod tags;

pub use embed::{EMBED_PATH, OllamaEmbeddingProvider};
pub use inference::{CHAT_PATH, OllamaInferenceProvider};
pub use tags::TAGS_PATH;
