//! Mock inference and embedding providers for deterministic testing.
//!
//! Provides [`MockInferenceProvider`] and [`MockEmbeddingProvider`] with
//! fluent builder APIs, sequential response queues, conditional request
//! matching, error injection, call history capture, and usage accounting.

mod matcher;
mod provider;
mod responses;

pub use matcher::{CompletionMatcher, EmbeddingMatcher};
pub use provider::{
    ConditionalCompletionBuilder, ConditionalEmbeddingBuilder, MockEmbeddingProvider,
    MockEmbeddingProviderBuilder, MockInferenceProvider, MockInferenceProviderBuilder,
};
pub use responses::{
    a_completion_response, a_parse_failure, a_provider_unavailable, an_embedding_failure,
    an_embedding_response, an_llm_call_failure,
};
