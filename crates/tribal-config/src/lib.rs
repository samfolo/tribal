//! Configuration loading and validation for the Tribal server.
//!
//! Merges up to six sources in precedence order:
//! compiled defaults → command defaults → YAML file → nested env vars
//! → convenience alias env vars → CLI flags.

#![deny(warnings)]
#![warn(clippy::pedantic)]

mod cli_overrides;
mod divergence;
mod env;
mod error;
mod loader;
mod paths;
mod redact;
mod render;
mod sections;
mod validation;

pub use cli_overrides::{
    CliOverrides, DatabaseCliOverrides, EmbeddingCliOverrides, InferenceCliOverrides,
    InferenceStageCliOverrides, ServerCliOverrides, TelemetryCliOverrides,
};
pub use divergence::{
    WARNING_CONFIG_UNPARSEABLE, WARNING_DATABASE_URL_DIVERGENCE, check_config_divergence,
};
pub use env::{
    ENV_ANTHROPIC_API_KEY, ENV_AUTH_TOKEN, ENV_CONFIG_PATH, ENV_NESTED_SEPARATOR,
    ENV_OPENAI_API_KEY, ENV_PREFIX, ENV_PROJECT_ID, ENV_PUBLIC_MCP_URL, env_var_for_path,
};
pub use error::ConfigError;
pub use loader::load_config;
pub use paths::{
    CREDENTIALS_FILENAME, ConfigDirError, TRIBAL_DIRECTORY_NAME, default_config_file_path,
};
pub use redact::redact_secrets;
pub use render::{ConfigPersistence, render_minimal_config, render_persisted_config};
pub use sections::{
    Auth, AuthConfig, CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX,
    CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX, CREDENTIALS_WRITE_FAILED_PREFIX,
    CREDENTIALS_WRITE_FAILED_SUFFIX, Credentials, CredentialsPermissions, CredentialsReadError,
    CredentialsWriteError, DEFAULT_ANTHROPIC_BASE_URL, DEFAULT_BIND_ADDRESS,
    DEFAULT_DISCOVERY_LIMIT, DEFAULT_DISCOVERY_MAX_LIMIT, DEFAULT_EMBEDDING_DIMENSIONS,
    DEFAULT_EMBEDDING_MODEL, DEFAULT_EXPLORATION_DEPTH, DEFAULT_EXPLORATION_LIMIT,
    DEFAULT_EXPLORATION_MAX_DEPTH, DEFAULT_EXPLORATION_MAX_LIMIT, DEFAULT_OLLAMA_BASE_URL,
    DEFAULT_OPENAI_BASE_URL, DEFAULT_OVERFETCH_MULTIPLIER, DEFAULT_SIMILARITY_THRESHOLD,
    DatabaseConfig, DiscoveryConfig, EmbeddingConfig, ExplorationConfig, FileRotation,
    InferenceConfig, LimitsConfig, LoadedCredentials, LogFormat, LogOutput, LoggingConfig,
    MAX_LIFECYCLE_DURATION_MS, MAX_OVERFETCH_MULTIPLIER, MAX_TTL_HOURS, PromptSource,
    PromptsConfig, ProviderKind, ProviderLimitsConfig, ServerConfig, SseConfig,
    StageInferenceConfig, TelemetryConfig, TransportKind, TribalConfig, VERSION, WorkerConfig,
    read_credentials, write_credentials,
};
pub use validation::{
    ComputedFloor, ConfigPath, Diagnostics, Endpoint, EnumerateFields, FieldValue, Inclusion,
    NumericRange, OrderRelation, ProviderStage, ValidationError, validate,
};

// ---------------------------------------------------------------------------
// Section declaration
// ---------------------------------------------------------------------------

/// Declares a configuration section struct alongside its
/// [`validation::EnumerateFields`] impl from a single field list.  Both
/// the struct definition and the path-enumeration impl derive from the
/// same source of truth, so a field rename or addition can never drift
/// the two out of sync.
///
/// Per-field markers control enumeration behaviour:
/// - `@nested` — the field's type implements
///   [`validation::EnumerateFields`]; the impl recurses into it under
///   the field name as a prefix.
/// - `@skip` — the field is excluded from enumeration entirely (use
///   for `#[serde(skip)]` fields and for runtime-keyed maps).
/// - no marker — the field is a leaf; its path is `{prefix}.{field}`.
macro_rules! config_section {
    // Entry: capture struct attrs, visibility, name, and a flat list of
    // fields (each optionally prefixed with `@nested` or `@skip`).
    // Emits the struct exactly as declared plus the `EnumerateFields`
    // impl, deferring per-field push logic to the `@push` helper so
    // identifiers stay in one hygiene context.
    (
        $(#[$sattr:meta])*
        $vis:vis struct $Name:ident {
            $(
                $(#[$fattr:meta])*
                $( @ $marker:ident )?
                $fv:vis $fname:ident : $fty:ty
            ),* $(,)?
        }
    ) => {
        $(#[$sattr])*
        $vis struct $Name {
            $(
                $(#[$fattr])*
                $fv $fname : $fty,
            )*
        }

        impl $crate::validation::EnumerateFields for $Name {
            fn enumerate(
                prefix: &str,
                out: &mut Vec<$crate::validation::ConfigPath>,
            ) {
                let _ = prefix;
                let _ = out;
                $(
                    $crate::config_section!(@push $($marker)? prefix, out, $fname, $fty);
                )*
            }
        }
    };

    // Default — no marker — push the field name as a leaf path.
    (@push $p:ident, $o:ident, $f:ident, $t:ty) => {
        $o.push($crate::validation::ConfigPath::child($p, stringify!($f)));
    };

    // `@nested` — recurse into the field's own `EnumerateFields` impl,
    // joining the field name to the parent prefix.
    (@push nested $p:ident, $o:ident, $f:ident, $t:ty) => {{
        let child_prefix = if $p.is_empty() {
            stringify!($f).to_string()
        } else {
            format!("{}.{}", $p, stringify!($f))
        };
        <$t as $crate::validation::EnumerateFields>::enumerate(&child_prefix, $o);
    }};

    // `@skip` — emit no path (use for `#[serde(skip)]` fields and
    // runtime-keyed maps).
    (@push skip $p:ident, $o:ident, $f:ident, $t:ty) => {};
}

pub(crate) use config_section;

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
