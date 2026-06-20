//! Configuration section types.

mod agents;
mod auth;
mod credential_catalogue;
mod credentials;
mod database;
mod discovery;
mod exploration;
mod inference;
mod init;
mod limits;
mod logging;
mod oauth;
mod prompts;
mod root;
mod server;
mod telemetry;
mod transport_kind;
mod worker;

pub use agents::{
    AgentsConfig, DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS, DEFAULT_AGENTIC_MAX_TOTAL_TOKENS,
    DEFAULT_AGENTIC_MAX_TURNS, DEFAULT_AGENTIC_RECHECK_BOUND,
    DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS, DEFAULT_AGENTIC_VERIFY_ROUNDS, ExecutorChoice,
    StageAgentConfig,
};
pub use auth::{AuthConfig, MAX_TTL_HOURS};
pub use credential_catalogue::{
    CredentialCatalogue, CredentialEntry, MissingApiKey, MissingApiKeyKind,
    is_valid_connection_name,
};
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
pub use exploration::{
    DEFAULT_DEPTH as DEFAULT_EXPLORATION_DEPTH, DEFAULT_LIMIT as DEFAULT_EXPLORATION_LIMIT,
    DEFAULT_MAX_DEPTH as DEFAULT_EXPLORATION_MAX_DEPTH,
    DEFAULT_MAX_LIMIT as DEFAULT_EXPLORATION_MAX_LIMIT, ExplorationConfig,
};
pub use inference::{InferenceConfig, StageInferenceConfig};
pub use init::{
    DEFAULT_DIMENSIONS as DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_MODEL as DEFAULT_EMBEDDING_MODEL,
    InitConfig, InitEmbeddingConfig,
};
pub use limits::{LimitsConfig, ProviderLimitsConfig};
pub use logging::{LogFormat, LogOutput, LoggingConfig};
pub use oauth::{
    DEFAULT_ACCESS_TOKEN_TTL_HOURS, DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS,
    MAX_AUTHORIZATION_CODE_TTL_SECONDS, MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
    MIN_AUTHORIZATION_CODE_TTL_SECONDS, OAuthConfig, advertised_oauth_host,
    oauth_onboarding_is_url_only, oauth_surface_is_routable,
};
pub use prompts::{PromptSource, PromptsConfig};
pub use root::{TribalConfig, VERSION};
pub use server::{DEFAULT_BIND_ADDRESS, MAX_LIFECYCLE_DURATION_MS, ServerConfig, SseConfig};
pub use telemetry::{FileRotation, TelemetryConfig};
pub use transport_kind::TransportKind;
pub use worker::WorkerConfig;
