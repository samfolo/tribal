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
pub use discovery::{
    DEFAULT_LIMIT as DEFAULT_DISCOVERY_LIMIT, DEFAULT_MAX_LIMIT as DEFAULT_DISCOVERY_MAX_LIMIT,
    DEFAULT_OVERFETCH_MULTIPLIER, DEFAULT_SIMILARITY_THRESHOLD, DiscoveryConfig,
};
pub use embedding::{
    DEFAULT_DIMENSIONS as DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_MODEL as DEFAULT_EMBEDDING_MODEL,
    EmbeddingConfig,
};
pub use exploration::{
    DEFAULT_DEPTH as DEFAULT_EXPLORATION_DEPTH, DEFAULT_LIMIT as DEFAULT_EXPLORATION_LIMIT,
    DEFAULT_MAX_DEPTH as DEFAULT_EXPLORATION_MAX_DEPTH,
    DEFAULT_MAX_LIMIT as DEFAULT_EXPLORATION_MAX_LIMIT, ExplorationConfig,
};
pub use inference::{InferenceConfig, StageInferenceConfig};
pub use limits::{LimitsConfig, ProviderLimitsConfig};
pub use logging::{LogFormat, LogOutput, LoggingConfig};
pub use prompts::PromptsConfig;
pub use provider_kind::{
    DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL, ProviderKind,
};
pub use root::{TribalConfig, VERSION};
pub use server::{DEFAULT_BIND_ADDRESS, MAX_LIFECYCLE_DURATION_MS, ServerConfig, SseConfig};
pub use telemetry::{FileRotation, TelemetryConfig};
pub use transport_kind::TransportKind;
pub use worker::WorkerConfig;
