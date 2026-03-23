//! Configuration loading and validation for the Tribal server.
//!
//! Merges up to six sources in precedence order:
//! compiled defaults → command defaults → YAML file → nested env vars
//! → convenience alias env vars → CLI flags.

#![deny(warnings)]
#![warn(clippy::pedantic)]

mod divergence;
mod env;
mod error;
mod loader;
mod paths;
mod render;
mod sections;
mod validation;

pub use divergence::{
    WARNING_CONFIG_UNPARSEABLE, WARNING_DATABASE_URL_DIVERGENCE, check_config_divergence,
};
pub use env::{ENV_CONFIG_PATH, ENV_PREFIX, ENV_PROJECT_ID};
pub use error::ConfigError;
pub use loader::{CliOverrides, DatabaseCliOverrides, ServerCliOverrides, load_config};
pub use render::render_minimal_config;
pub use sections::{
    AuthConfig, DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_BIND_ADDRESS, DEFAULT_DISCOVERY_LIMIT,
    DEFAULT_DISCOVERY_MAX_LIMIT, DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EXPLORATION_DEPTH, DEFAULT_EXPLORATION_LIMIT, DEFAULT_EXPLORATION_MAX_DEPTH,
    DEFAULT_EXPLORATION_MAX_LIMIT, DEFAULT_OLLAMA_BASE_URL, DEFAULT_OPENAI_BASE_URL,
    DatabaseConfig, DiscoveryConfig, EmbeddingConfig, ExplorationConfig, FileRotation,
    InferenceConfig, LimitsConfig, LogFormat, LogOutput, LoggingConfig, MAX_LIFECYCLE_DURATION_MS,
    PromptsConfig, ProviderKind, ProviderLimitsConfig, ServerConfig, SseConfig,
    StageInferenceConfig, TelemetryConfig, TransportKind, TribalConfig, VERSION, WorkerConfig,
};
pub use validation::{ERR_TTL_ZERO, validate};

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
