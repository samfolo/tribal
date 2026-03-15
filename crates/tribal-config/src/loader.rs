//! Figment-based layered configuration loading.
//!
//! Merges four sources in precedence order:
//! compiled defaults → YAML file → environment variables → CLI flags.

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
};
use serde::Serialize;

use crate::{
    LoggingConfig, TelemetryConfig, TribalConfig, env::ENV_PREFIX, error::ConfigError,
    sections::TransportKind,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Separator used to encode nested paths in environment variable names.
///
/// `TRIBAL_DATABASE__URL` maps to `database.url`.
const ENV_SEPARATOR: &str = "__";

/// Top-level scalar fields that should not be overridden via env vars.
#[cfg(test)]
const KNOWN_SCALARS: &[&str] = &["version"];

/// Top-level section names in the configuration.
///
/// Only `TRIBAL_*` env vars whose post-prefix-strip, post-split key starts
/// with one of these sections are accepted.  Stray env vars like
/// `TRIBAL_CONFIG_PATH` or `TRIBAL_PROJECT_ID` are silently ignored.
const KNOWN_SECTIONS: &[&str] = &[
    "server.",
    "database.",
    "auth.",
    "worker.",
    "embedding.",
    "inference.",
    "limits.",
    "prompts.",
    "discovery.",
    "exploration.",
    "logging.",
    "telemetry.",
];

// ---------------------------------------------------------------------------
// CliOverrides
// ---------------------------------------------------------------------------

/// CLI flag overrides merged at the highest precedence.
///
/// Only explicitly-passed values participate in the merge; absent fields
/// are skipped via `skip_serializing_if`.
#[derive(Debug, Default, Serialize)]
pub struct CliOverrides {
    /// Server-related CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerCliOverrides>,
}

/// Server-related CLI flag overrides.
#[derive(Debug, Serialize)]
pub struct ServerCliOverrides {
    /// Transport override from `--transport`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportKind>,

    /// Bind address override from `--bind`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind_address: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Loads and merges configuration from all sources.
///
/// Layer precedence (highest wins):
/// 1. CLI overrides
/// 2. Convenience alias env vars (`TRIBAL_DATABASE_URL`, etc.)
/// 3. Nested env vars (`TRIBAL_DATABASE__URL`, etc.)
/// 4. YAML file
/// 5. Compiled defaults
///
/// A missing YAML file is not an error — defaults are used instead.
///
/// # Errors
///
/// Returns [`ConfigError::Load`] if deserialisation or merging fails.
pub fn load_config(
    config_path: &str,
    cli_overrides: Option<CliOverrides>,
) -> Result<TribalConfig, ConfigError> {
    let expanded_path = shellexpand::tilde(config_path);

    let nested_env = Env::prefixed(ENV_PREFIX)
        .split(ENV_SEPARATOR)
        .filter(|key| {
            let k = key.as_str().to_lowercase();
            KNOWN_SECTIONS.iter().any(|section| k.starts_with(section))
        });

    let alias_env = Env::raw()
        .only(&[
            "TRIBAL_DATABASE_URL",
            "TRIBAL_TRANSPORT",
            "TRIBAL_BIND_ADDRESS",
            "TRIBAL_LOG",
        ])
        .map(|key| match key.as_str() {
            "TRIBAL_DATABASE_URL" => "database.url".into(),
            "TRIBAL_TRANSPORT" => "server.transport".into(),
            "TRIBAL_BIND_ADDRESS" => "server.bind_address".into(),
            "TRIBAL_LOG" => "logging.level".into(),
            _ => key.into(),
        });

    let mut figment = Figment::from(Serialized::defaults(TribalConfig::default()))
        .merge(Yaml::file(expanded_path.as_ref()))
        .merge(nested_env)
        .merge(alias_env);

    if let Some(overrides) = cli_overrides {
        figment = figment.merge(Serialized::globals(overrides));
    }

    let mut config: TribalConfig = figment.extract().map_err(|source| ConfigError::Load {
        source: Box::new(source),
    })?;

    expand_paths(&mut config);
    restore_temp_dir_fallback_flags(&mut config);

    Ok(config)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Restores `used_temp_dir_fallback` flags lost during serde roundtripping.
///
/// `#[serde(skip)]` fields are always `false` after figment extraction.
/// When the user has not overridden the directory, we restore the flag from
/// a freshly computed default so the subscriber can emit a warning.
fn restore_temp_dir_fallback_flags(config: &mut TribalConfig) {
    let default_logging = LoggingConfig::default();
    if config.logging.file_directory == default_logging.file_directory {
        config.logging.used_temp_dir_fallback = default_logging.used_temp_dir_fallback;
    }

    let default_telemetry = TelemetryConfig::default();
    if config.telemetry.file_directory == default_telemetry.file_directory {
        config.telemetry.used_temp_dir_fallback = default_telemetry.used_temp_dir_fallback;
    }
}

fn expand_paths(config: &mut TribalConfig) {
    config.prompts.directory = shellexpand::tilde(&config.prompts.directory).into_owned();
    config.telemetry.file_directory =
        shellexpand::tilde(&config.telemetry.file_directory).into_owned();
    config.logging.file_directory = shellexpand::tilde(&config.logging.file_directory).into_owned();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use figment::Jail;

    use super::*;
    use crate::ProviderKind;

    #[test]
    fn test_defaults_only() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();

            let mut expected = TribalConfig::default();
            expand_paths(&mut expected);
            restore_temp_dir_fallback_flags(&mut expected);
            assert_eq!(config, expected);
            Ok(())
        });
    }

    #[test]
    fn test_yaml_overrides_defaults() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                r#"
database:
  url: "postgres://yaml-host/tribal"
server:
  transport: http
"#,
            )?;

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert_eq!(config.database.url, "postgres://yaml-host/tribal");
            assert_eq!(config.server.transport, TransportKind::Http);
            Ok(())
        });
    }

    #[test]
    fn test_missing_yaml_file_not_error() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("nonexistent.yaml");
            let result = load_config(path.to_str().unwrap(), None);
            assert!(result.is_ok());
            Ok(())
        });
    }

    #[test]
    fn test_tilde_expansion() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert!(
                !config.prompts.directory.starts_with('~'),
                "prompts.directory should be expanded: {}",
                config.prompts.directory
            );
            assert!(
                !config.telemetry.file_directory.starts_with('~'),
                "telemetry.file_directory should be expanded: {}",
                config.telemetry.file_directory
            );
            Ok(())
        });
    }

    #[test]
    fn test_env_var_overrides_yaml() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "database:\n  pool_mcp_max_connections: 4\n")?;
            jail.set_env("TRIBAL_DATABASE__POOL_MCP_MAX_CONNECTIONS", "32");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert_eq!(config.database.pool_mcp_max_connections, 32);
            Ok(())
        });
    }

    #[test]
    fn test_cli_overrides_env_var() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "server:\n  transport: http\n")?;

            let overrides = CliOverrides {
                server: Some(ServerCliOverrides {
                    transport: Some(TransportKind::Sse),
                    bind_address: None,
                }),
            };

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), Some(overrides)).unwrap();
            assert_eq!(config.server.transport, TransportKind::Sse);
            Ok(())
        });
    }

    #[test]
    fn test_convenience_alias_works_alone() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_DATABASE_URL", "postgres://alias/tribal");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert_eq!(config.database.url, "postgres://alias/tribal");
            Ok(())
        });
    }

    #[test]
    fn test_convenience_alias_takes_precedence() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_DATABASE_URL", "postgres://alias-wins/tribal");
            jail.set_env("TRIBAL_DATABASE__URL", "postgres://nested-loses/tribal");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert_eq!(config.database.url, "postgres://alias-wins/tribal");
            Ok(())
        });
    }

    #[test]
    fn test_stray_env_var_ignored() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_CONFIG_PATH", "/some/path");
            jail.set_env("TRIBAL_PROJECT_ID", "my-project");

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None);
            assert!(result.is_ok(), "stray env vars should be silently ignored");
            Ok(())
        });
    }

    #[test]
    fn test_unknown_top_level_yaml_field_produces_error() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "nonexistent_section:\n  foo: bar\n")?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None);
            assert!(result.is_err(), "unknown top-level YAML key should fail");
            Ok(())
        });
    }

    #[test]
    fn test_unknown_nested_yaml_field_produces_error() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "database:\n  nonexistent: true\n")?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None);
            assert!(result.is_err(), "unknown nested YAML key should fail");
            Ok(())
        });
    }

    #[test]
    fn test_env_var_whitelist_covers_all_sections() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_SERVER__SHUTDOWN_DEADLINE_MS", "5000");
            jail.set_env("TRIBAL_DATABASE__ACQUIRE_TIMEOUT_MS", "10000");
            jail.set_env("TRIBAL_AUTH__TOKEN_TTL_HOURS", "24");
            jail.set_env("TRIBAL_WORKER__MAX_CONCURRENT_TASKS", "8");
            jail.set_env("TRIBAL_EMBEDDING__DIMENSIONS", "1024");
            jail.set_env("TRIBAL_INFERENCE__EXTRACTION__TEMPERATURE", "0.5");
            jail.set_env("TRIBAL_LIMITS__PROVIDERS__OLLAMA__MAX_IN_FLIGHT", "4");
            jail.set_env("TRIBAL_PROMPTS__HOT_RELOAD", "true");
            jail.set_env("TRIBAL_DISCOVERY__MAX_LIMIT", "100");
            jail.set_env("TRIBAL_EXPLORATION__MAX_DEPTH", "5");
            jail.set_env("TRIBAL_LOGGING__LEVEL", "debug");
            jail.set_env("TRIBAL_TELEMETRY__SERVICE_NAME", "test-tribal");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();

            assert_eq!(config.server.shutdown_deadline_ms, 5000);
            assert_eq!(config.database.acquire_timeout_ms, 10_000);
            assert_eq!(config.auth.token_ttl_hours, 24);
            assert_eq!(config.worker.max_concurrent_tasks, 8);
            assert_eq!(config.embedding.dimensions, 1024);
            assert!((config.inference.extraction.temperature - 0.5).abs() < f64::EPSILON);
            assert_eq!(
                config.limits.providers[&ProviderKind::Ollama].max_in_flight,
                4
            );
            assert!(config.prompts.hot_reload);
            assert_eq!(config.discovery.max_limit, 100);
            assert_eq!(config.exploration.max_depth, 5);
            assert_eq!(config.logging.level, "debug");
            assert_eq!(config.telemetry.service_name, "test-tribal");
            Ok(())
        });
    }

    #[test]
    fn test_known_sections_covers_all_config_fields() {
        let serialised = serde_json::to_value(TribalConfig::default()).unwrap();
        let top_level_keys: Vec<&str> = serialised
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();

        for key in &top_level_keys {
            if KNOWN_SCALARS.contains(key) {
                continue;
            }
            let prefixed = format!("{key}.");
            assert!(
                KNOWN_SECTIONS.contains(&prefixed.as_str()),
                "config field \"{key}\" is not listed in KNOWN_SECTIONS or \
                 KNOWN_SCALARS — add \"{prefixed}\" to KNOWN_SECTIONS or \
                 \"{key}\" to KNOWN_SCALARS"
            );
        }

        for section in KNOWN_SECTIONS {
            let key = section.trim_end_matches('.');
            assert!(
                top_level_keys.contains(&key),
                "KNOWN_SECTIONS entry \"{section}\" does not correspond to \
                 any top-level config field"
            );
        }
    }

    #[test]
    fn test_temp_dir_fallback_flags_survive_figment_roundtrip() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();

            let defaults = TribalConfig::default();
            assert_eq!(
                config.logging.used_temp_dir_fallback, defaults.logging.used_temp_dir_fallback,
                "logging.used_temp_dir_fallback should match Default after figment roundtrip"
            );
            assert_eq!(
                config.telemetry.used_temp_dir_fallback, defaults.telemetry.used_temp_dir_fallback,
                "telemetry.used_temp_dir_fallback should match Default after figment roundtrip"
            );
            Ok(())
        });
    }

    #[test]
    fn test_deeply_nested_env_vars() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_SERVER__SSE__IDLE_TIMEOUT_MS", "60000");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None).unwrap();
            assert_eq!(config.server.sse.idle_timeout_ms, 60_000);
            Ok(())
        });
    }
}
