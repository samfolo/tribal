#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Inference provider abstraction for Tribal: provider traits, Ollama
//! client, cloud API clients, and embedding generation.

mod anthropic;
mod capabilities;
mod drift;
mod embedding_capabilities;
mod embedding_factory;
mod error;
mod http;
mod ollama;
mod openai;
mod provider;
mod registry;
mod request;
mod response;
mod schema_dialect;
mod usage;
mod validation;

pub use anthropic::AnthropicInferenceProvider;
#[cfg(feature = "test-helpers")]
pub use anthropic::MESSAGES_PATH as ANTHROPIC_MESSAGES_PATH;
pub use capabilities::{
    MaxOutputTokensParam, ModelCapabilities, SamplingControl, StructuredOutputMode, resolve,
};
pub use drift::probe_digest;
pub use embedding_capabilities::{
    DimensionResolutionError, EmbeddingCapabilities, resolve_dimensions, resolve_embedding,
};
pub use embedding_factory::{UnsupportedEmbeddingProvider, make_embedding_provider};
pub use error::{InferenceError, classify_embedding_error};
pub use http::EMBEDDING_PROBE_INPUT;
#[cfg(feature = "test-helpers")]
pub use http::INFERENCE_PROBE_INPUT;
#[cfg(feature = "test-helpers")]
pub use ollama::{
    CHAT_PATH as OLLAMA_CHAT_PATH, EMBED_PATH as OLLAMA_EMBED_PATH, TAGS_PATH as OLLAMA_TAGS_PATH,
};
pub use ollama::{OllamaEmbeddingProvider, OllamaInferenceProvider, resolve_ollama_revision_token};
#[cfg(feature = "test-helpers")]
pub use openai::{CHAT_PATH as OPENAI_CHAT_PATH, EMBED_PATH as OPENAI_EMBED_PATH};
pub use openai::{OpenAiEmbeddingProvider, OpenAiInferenceProvider};
pub use provider::{BatchEmbeddingResult, EmbeddingProvider, InferenceProvider, ProviderIdentity};
pub use registry::{
    ProviderKey, ProviderLimits, ProviderRegistry, ProviderRegistryError, RequestClass,
};
pub use request::{CompletionRequest, EmbeddingRequest, Message, ResponseFormat, Role};
pub use response::{CompletionResponse, EmbeddingResponse};
pub use schema_dialect::apply_dialect;
#[cfg(feature = "test-helpers")]
pub use schema_dialect::assert_dialect_invariants;
pub use usage::{CompletionUsage, EmbeddingUsage, Usage};
