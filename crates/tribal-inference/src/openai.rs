//! `OpenAI` inference and embedding provider implementations.

mod embed;
mod inference;

pub use embed::{EMBED_PATH, OpenAiEmbeddingProvider};
pub use inference::{CHAT_PATH, OpenAiInferenceProvider};
