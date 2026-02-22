//! `OpenAI` inference and embedding provider implementations.

mod embed;
mod inference;

pub use embed::OpenAiEmbeddingProvider;
pub use inference::OpenAiInferenceProvider;
