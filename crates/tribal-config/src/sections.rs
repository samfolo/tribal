//! Configuration section types.

mod auth;
mod database;
mod discovery;
mod embedding;
mod exploration;
mod inference;
mod limits;
mod logging;
mod prompts;
mod provider_kind;
mod root;
mod server;
mod telemetry;
mod transport_kind;
mod worker;

pub use auth::AuthConfig;
pub use database::DatabaseConfig;
pub use discovery::DiscoveryConfig;
pub use embedding::EmbeddingConfig;
pub use exploration::ExplorationConfig;
pub use inference::{InferenceConfig, StageInferenceConfig};
pub use limits::{LimitsConfig, ProviderLimitsConfig};
pub use logging::{LogFormat, LogOutput, LoggingConfig};
pub use prompts::PromptsConfig;
pub use provider_kind::{
    DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL, ProviderKind,
};
pub use root::{TribalConfig, VERSION};
pub use server::{ServerConfig, SseConfig};
pub use telemetry::{FileRotation, TelemetryConfig};
pub use transport_kind::TransportKind;
pub use worker::WorkerConfig;
