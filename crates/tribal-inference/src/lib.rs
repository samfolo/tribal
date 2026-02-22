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
pub use ollama::{OllamaEmbeddingProvider, OllamaInferenceProvider};
pub use openai::{OpenAiEmbeddingProvider, OpenAiInferenceProvider};
pub use provider::{EmbeddingProvider, InferenceProvider};
pub use registry::{
    ProviderKey, ProviderLimits, ProviderRegistry, ProviderRegistryError, RequestClass,
};
pub use request::{CompletionRequest, EmbeddingRequest, Message, ResponseFormat, Role};
pub use response::{CompletionResponse, EmbeddingResponse};
pub use usage::{CompletionUsage, EmbeddingUsage, Usage};
