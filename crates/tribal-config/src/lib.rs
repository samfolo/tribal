//! Configuration loading and validation for the Tribal server.
//!
//! Merges up to six sources in precedence order:
//! compiled defaults → command defaults → YAML file → nested env vars
//! → convenience alias env vars → CLI flags.

mod atomic_write;
mod cli_overrides;
mod config_schema;
mod divergence;
mod env;
mod error;
mod loader;
mod operations;
mod paths;
mod redact;
mod render;
mod sections;
mod validation;

pub use atomic_write::write_atomically;
pub use cli_overrides::{
    CliOverrides, DatabaseCliOverrides, EmbeddingCliOverrides, InferenceCliOverrides,
    InferenceStageCliOverrides, InitCliOverrides, PersistedProviderConnection, ServerCliOverrides,
    TelemetryCliOverrides,
};
pub use config_schema::{AudienceTier, GENESIS_KEYS, ReloadClass, audience_tier, reload_class};
#[cfg(feature = "schema")]
pub use config_schema::{ConfigFieldMeta, ConfigSchema, config_schema, structural_schema};
pub use divergence::{
    WARNING_CONFIG_UNPARSEABLE, WARNING_DATABASE_URL_DIVERGENCE, check_config_divergence,
};
pub use env::{
    ENV_ANTHROPIC_API_KEY, ENV_AUTH_TOKEN, ENV_CONFIG_PATH, ENV_NESTED_SEPARATOR,
    ENV_OPENAI_API_KEY, ENV_PREFIX, ENV_PROJECT_ID, ENV_PUBLIC_MCP_URL, env_var_for_path,
    public_mcp_url_override, standard_env_var_name,
};
pub use error::{ConfigError, RemovedProviderShapeSource};
pub use loader::{load_config, load_config_from_yaml};
pub use operations::{
    CliShadow, ConfigViolation, Persisted, PersistedPatch, SetError, UnknownConfigKey, WriteEffect,
    get, get_all, patch, patch_from_yaml, repair_patch, set, set_from_yaml, shadowed_by,
    validate_patch, validate_write,
};
pub use paths::{TRIBAL_DIRECTORY_NAME, default_config_file_path};
pub use redact::{is_secret_key, redact_secrets};
pub use render::{ConfigPersistence, render_minimal_config, render_persisted_config};
pub use sections::{
    AgentsConfig, AuthConfig, ClientRegistrationMode, CustomProcessingSettings,
    DEFAULT_ACCESS_TOKEN_TTL_HOURS, DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS,
    DEFAULT_AGENTIC_MAX_TOTAL_TOKENS, DEFAULT_AGENTIC_MAX_TURNS, DEFAULT_AGENTIC_RECHECK_BOUND,
    DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS, DEFAULT_AGENTIC_VERIFY_ROUNDS,
    DEFAULT_AUTHORIZATION_CODE_TTL_SECONDS, DEFAULT_BIND_ADDRESS, DEFAULT_DISCOVERY_LIMIT,
    DEFAULT_DISCOVERY_MAX_LIMIT, DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EXPLORATION_DEPTH, DEFAULT_EXPLORATION_LIMIT, DEFAULT_EXPLORATION_MAX_DEPTH,
    DEFAULT_EXPLORATION_MAX_LIMIT, DEFAULT_OVERFETCH_MULTIPLIER, DEFAULT_PROVIDER_CONNECTION_NAME,
    DEFAULT_SIMILARITY_THRESHOLD, DatabaseConfig, DiscoveryConfig, ExecutorChoice,
    ExplorationConfig, ExtractionStageSettings, FileRotation, InferenceConfig, InferenceStage,
    InitConfig, InitEmbeddingConfig, LimitsConfig, LogFormat, LogOutput, LoggingConfig,
    MAX_AUTHORIZATION_CODE_TTL_SECONDS, MAX_LIFECYCLE_DURATION_MS, MAX_OVERFETCH_MULTIPLIER,
    MAX_TTL_HOURS, MIN_AUTHORIZATION_CODE_TTL_SECONDS, OAuthConfig, PresetModelSettings,
    ProcessingProfile, PromptSource, PromptsConfig, ProviderConnectionConfig,
    ProviderConnectionResolutionError, ProviderConnectionUsage, ProviderConnectionViolation,
    ProviderConnections, ProviderLimitsConfig, ServerConfig, SseConfig, StageAgentConfig,
    StageExecutionSettings, StageInferenceConfig, StageModelSettings, TelemetryConfig,
    TribalConfig, VERSION, VerifiedStageExecutionSettings, VerifiedStageSettings, WorkerConfig,
    advertised_oauth_host, client_registration_mode, oauth_onboarding_is_url_only,
    oauth_surface_is_routable,
};
pub use validation::{
    ComputedFloor, ConfigPath, Diagnostics, Endpoint, FieldValue, Inclusion, NumericRange,
    OrderRelation, PUBLIC_MCP_URL_REQUIREMENT, ProviderStage, ValidationError, config_warnings,
    is_valid_public_mcp_url, validate,
};

// ---------------------------------------------------------------------------
// Test utilities
// ---------------------------------------------------------------------------

/// Generates a serde roundtrip test for an enum with a compile-time
/// exhaustiveness check.
///
/// If a variant is added to the enum but not listed in the macro invocation,
/// the embedded `match` becomes non-exhaustive and the build fails.
#[cfg(test)]
macro_rules! enum_serde_tests {
    ($test_name:ident, $type:ty { $($variant:path => $json:literal),+ $(,)? }) => {
        #[test]
        fn $test_name() {
            // Compile-time exhaustiveness guard: every variant must be listed.
            #[allow(dead_code)]
            fn check_exhaustiveness(v: $type) {
                match v {
                    $( $variant => {} )+
                }
            }

            let variants: &[($type, &str)] = &[
                $( ($variant, $json), )+
            ];
            for &(variant, expected_json) in variants {
                let json = serde_json::to_string(&variant).expect("should serialise");
                assert_eq!(json, format!("\"{expected_json}\""), "serialised form of {variant:?}");
                let parsed: $type = serde_json::from_str(&json).expect("should deserialise");
                assert_eq!(parsed, variant);
            }
        }
    };
}

#[cfg(test)]
pub(crate) use enum_serde_tests;
