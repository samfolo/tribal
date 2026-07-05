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
mod gateway;
mod http;
mod ledger;
mod ollama;
mod openai;
mod platform;
mod provider;
mod registry;
mod request;
mod response;
mod schema_dialect;
mod stream;
mod validation;

#[cfg(feature = "test-helpers")]
pub use anthropic::MESSAGES_PATH as ANTHROPIC_MESSAGES_PATH;
pub use capabilities::{
    MaxOutputTokensParam, ModelCapabilities, SamplingControl, StructuredOutputMode,
    effective_stage_parameters, resolve,
};
pub use drift::probe_digest;
pub use embedding_capabilities::{
    DimensionResolutionError, EmbeddingCapabilities, resolve_dimensions, resolve_embedding,
};
pub use error::{InferenceError, classify_embedding_error, embedding_retry_after};
pub use gateway::{
    CompletionStageSpec, CompletionStageSpecs, CredentialError, EmbedGroupError,
    EmbeddingCredentialResolver, EmbeddingTarget, GatewayBuildError, InferenceGateway, PermitWait,
};
#[cfg(feature = "test-helpers")]
pub use gateway::{
    EmptyCredentialResolver, InjectedCompletion, InjectedEmbedding, InjectedProviders,
};
#[cfg(feature = "test-helpers")]
pub use http::{EMBEDDING_PROBE_INPUT, INFERENCE_PROBE_INPUT};
pub use ledger::{LedgerSink, NoopLedgerSink, UsageAttribution};
#[cfg(feature = "test-helpers")]
pub use ollama::{
    CHAT_PATH as OLLAMA_CHAT_PATH, EMBED_PATH as OLLAMA_EMBED_PATH, TAGS_PATH as OLLAMA_TAGS_PATH,
};
#[cfg(feature = "test-helpers")]
pub use openai::{CHAT_PATH as OPENAI_CHAT_PATH, EMBED_PATH as OPENAI_EMBED_PATH};
pub use platform::GatewayMeteringClient;
#[cfg(feature = "test-helpers")]
pub use platform::{ACK_PATH as METERING_ACK_PATH, HOLDS_PATH as METERING_HOLDS_PATH};
pub use provider::{
    BatchEmbeddingResult, CallContext, EmbeddingProvider, InferenceProvider, ProviderIdentity,
};
pub use registry::{
    ProviderKey, ProviderLimits, ProviderRegistry, ProviderRegistryError, RequestClass,
};
pub use request::{
    CompletionRequest, EmbeddingRequest, Message, ResponseFormat, Role, ToolWireDefinition,
};
pub use response::EmbeddingResponse;
pub use schema_dialect::apply_dialect;
#[cfg(feature = "test-helpers")]
pub use schema_dialect::assert_dialect_invariants;
pub use stream::{InferenceEventStream, collect_completion};
