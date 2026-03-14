//! Configuration section types.

mod auth;
mod discovery;
mod embedding;
mod exploration;
mod inference;
mod limits;
mod prompts;
mod provider_kind;
mod root;
mod server;
mod telemetry;
mod transport_kind;

pub use auth::AuthConfig;
pub use discovery::DiscoveryConfig;
pub use embedding::EmbeddingConfig;
pub use exploration::ExplorationConfig;
pub use inference::{InferenceConfig, StageInferenceConfig};
pub use limits::{LimitsConfig, ProviderLimitsConfig};
pub use prompts::PromptsConfig;
pub use provider_kind::ProviderKind;
pub use root::{TribalConfig, VERSION};
pub use server::{ServerConfig, SseConfig};
pub use telemetry::{FileRotation, TelemetryConfig};
pub use transport_kind::TransportKind;
