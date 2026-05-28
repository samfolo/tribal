//! Configuration section types.

mod auth;
mod credentials;
mod database;
mod discovery;
mod embedding;
mod exploration;
mod inference;
mod limits;
mod logging;
mod oauth;
mod prompts;
mod root;
mod server;
mod telemetry;
mod transport_kind;
mod worker;

pub use auth::{AuthConfig, MAX_TTL_HOURS};
pub use credentials::{
    Auth, CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX, CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX,
    CREDENTIALS_WRITE_FAILED_PREFIX, CREDENTIALS_WRITE_FAILED_SUFFIX, Credentials,
    CredentialsPermissions, CredentialsReadError, CredentialsWriteError, LoadedCredentials,
    read_credentials, write_credentials,
};
pub use database::DatabaseConfig;
pub use discovery::{
    DEFAULT_LIMIT as DEFAULT_DISCOVERY_LIMIT, DEFAULT_MAX_LIMIT as DEFAULT_DISCOVERY_MAX_LIMIT,
    DEFAULT_OVERFETCH_MULTIPLIER, DEFAULT_SIMILARITY_THRESHOLD, DiscoveryConfig,
    MAX_OVERFETCH_MULTIPLIER,
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
pub use oauth::{
    CimdConfig, DEFAULT_ACCESS_TOKEN_TTL_HOURS, DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
    DEFAULT_CIMD_CACHE_MAX_SECONDS, DEFAULT_CIMD_CACHE_MIN_SECONDS,
    DEFAULT_CIMD_FETCH_TIMEOUT_SECONDS, DEFAULT_CIMD_MAX_ENTRIES, DEFAULT_CIMD_MAX_RESPONSE_BYTES,
    MAX_AUTHORIZATION_CODE_TTL_SECONDS, MAX_CIMD_FETCH_TIMEOUT_SECONDS, MAX_CIMD_MAX_ENTRIES,
    MAX_CIMD_MAX_RESPONSE_BYTES, MIN_AUTHORIZATION_CODE_TTL_SECONDS, MIN_CIMD_MAX_RESPONSE_BYTES,
    OAuthConfig,
};
pub use prompts::{PromptSource, PromptsConfig};
pub use root::{TribalConfig, VERSION};
pub use server::{DEFAULT_BIND_ADDRESS, MAX_LIFECYCLE_DURATION_MS, ServerConfig, SseConfig};
pub use telemetry::{FileRotation, TelemetryConfig};
pub use transport_kind::TransportKind;
pub use worker::WorkerConfig;
