#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Inference provider abstraction for Tribal: provider traits, Ollama
//! client, cloud API clients, and embedding generation.

mod anthropic;
mod error;
mod http;
mod ollama;
mod openai;
mod provider;
mod registry;
mod request;
mod response;
mod usage;
mod validation;

pub use anthropic::AnthropicInferenceProvider;
pub use error::InferenceError;
#[cfg(feature = "test-helpers")]
pub use anthropic::inference::MESSAGES_PATH as ANTHROPIC_MESSAGES_PATH;
#[cfg(feature = "test-helpers")]
pub use http::{EMBEDDING_PROBE_INPUT, INFERENCE_PROBE_INPUT};
#[cfg(feature = "test-helpers")]
pub use ollama::{
    embed::EMBED_PATH as OLLAMA_EMBED_PATH, inference::CHAT_PATH as OLLAMA_CHAT_PATH,
    tags::TAGS_PATH as OLLAMA_TAGS_PATH,
};
#[cfg(feature = "test-helpers")]
pub use openai::{
    embed::EMBED_PATH as OPENAI_EMBED_PATH, inference::CHAT_PATH as OPENAI_CHAT_PATH,
};
pub use ollama::{OllamaEmbeddingProvider, OllamaInferenceProvider};
pub use openai::{OpenAiEmbeddingProvider, OpenAiInferenceProvider};
pub use provider::{EmbeddingProvider, InferenceProvider, ProviderIdentity};
pub use registry::{
    ProviderKey, ProviderLimits, ProviderRegistry, ProviderRegistryError, RequestClass,
};
pub use request::{CompletionRequest, EmbeddingRequest, Message, ResponseFormat, Role};
pub use response::{CompletionResponse, EmbeddingResponse};
pub use usage::{CompletionUsage, EmbeddingUsage, Usage};
