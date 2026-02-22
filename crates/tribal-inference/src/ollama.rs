//! Ollama inference provider implementations.

mod embed;
mod inference;
mod tags;

pub use embed::OllamaEmbeddingProvider;
pub use inference::OllamaInferenceProvider;
