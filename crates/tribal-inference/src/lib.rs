#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Inference provider abstraction for Tribal: provider traits, Ollama
//! client, cloud API clients, and embedding generation.

mod error;
mod provider;
mod request;
mod response;
mod usage;

pub use error::InferenceError;
pub use provider::{EmbeddingProvider, InferenceProvider};
pub use request::{CompletionRequest, EmbeddingRequest, Message, ResponseFormat, Role};
pub use response::{CompletionResponse, EmbeddingResponse};
pub use usage::{CompletionUsage, EmbeddingUsage, Usage};
