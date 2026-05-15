//! Figment-based layered configuration loading.
//!
//! Merges five sources in precedence order:
//! compiled defaults → command defaults → YAML file → environment variables → CLI flags.

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::{
    LoggingConfig, TelemetryConfig, TribalConfig,
    env::ENV_PREFIX,
    error::ConfigError,
    sections::{PromptSource, TransportKind},
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

    /// Database-related CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseCliOverrides>,
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

/// Database-related CLI flag overrides.
#[derive(Debug, Serialize)]
pub struct DatabaseCliOverrides {
    /// Database URL override from `--database-url`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
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
/// 5. Command defaults (subcommand-specific fallbacks)
/// 6. Compiled defaults
///
/// A missing YAML file is not an error — defaults are used instead.
///
/// The `command_defaults` parameter injects subcommand-specific default
/// values between compiled defaults and the YAML file. Each entry is a
/// dot-separated key and a string value (e.g. `("database.url", "...")`).
/// Pass `None` when no command-specific defaults are needed.
///
/// # Errors
///
/// Returns [`ConfigError::Load`] if deserialisation or merging fails.
pub fn load_config(
    config_path: &str,
    cli_overrides: Option<CliOverrides>,
    command_defaults: Option<&[(&str, &str)]>,
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

    let mut figment = Figment::from(Serialized::defaults(TribalConfig::default()));

    if let Some(defaults) = command_defaults {
        let value = build_nested_json(defaults);
        figment = figment.merge(Serialized::defaults(value));
    }

    figment = figment
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

/// Builds a nested `serde_json::Value` from dot-separated key-value pairs.
///
/// All values are stored as JSON strings. Figment coerces string values
/// into the target type during extraction (e.g. `"32"` → `u32`,
/// `"true"` → `bool`), so this is sufficient for all configuration fields.
///
/// For example, `("database.url", "postgres://...")` becomes:
/// `{"database": {"url": "postgres://..."}}`.
fn build_nested_json(pairs: &[(&str, &str)]) -> JsonValue {
    let mut root = serde_json::Map::new();

    for &(key, value) in pairs {
        let parts: Vec<&str> = key.split('.').collect();
        insert_nested(&mut root, &parts, value);
    }

    JsonValue::Object(root)
}

/// Recursively inserts a value into a nested JSON object at the path
/// specified by `parts`.
fn insert_nested(map: &mut serde_json::Map<String, JsonValue>, parts: &[&str], value: &str) {
    match parts {
        [] => {}
        [leaf] => {
            map.insert((*leaf).to_owned(), JsonValue::String(value.to_owned()));
        }
        [head, rest @ ..] => {
            let entry = map
                .entry((*head).to_owned())
                .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
            if let JsonValue::Object(child) = entry {
                insert_nested(child, rest, value);
            }
        }
    }
}

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
    if let PromptSource::Disk { directory, .. } = &mut config.prompts.source {
        *directory = shellexpand::tilde(directory).into_owned();
    }
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
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();

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
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(config.database.url, "postgres://yaml-host/tribal");
            assert_eq!(config.server.transport, TransportKind::Http);
            Ok(())
        });
    }

    #[test]
    fn test_missing_yaml_file_not_error() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("nonexistent.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            assert!(result.is_ok());
            Ok(())
        });
    }

    #[test]
    fn test_tilde_expansion_disk_prompts() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                r"
prompts:
  source:
    kind: disk
    directory: ~/somewhere
",
            )?;

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();

            assert!(
                matches!(
                    &config.prompts.source,
                    PromptSource::Disk { directory, .. } if !directory.starts_with('~'),
                ),
                "prompts directory should be expanded: {:?}",
                config.prompts.source,
            );
            assert!(
                !config.telemetry.file_directory.starts_with('~'),
                "telemetry.file_directory should be expanded: {}",
                config.telemetry.file_directory,
            );
            Ok(())
        });
    }

    #[test]
    fn test_tilde_expansion_embedded_prompts_no_op() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(config.prompts.source, PromptSource::Embedded {});
            Ok(())
        });
    }

    #[test]
    fn test_env_var_overrides_yaml() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "database:\n  pool_mcp_max_connections: 4\n")?;
            jail.set_env("TRIBAL_DATABASE__POOL_MCP_MAX_CONNECTIONS", "32");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
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
                database: None,
            };

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), Some(overrides), None).unwrap();
            assert_eq!(config.server.transport, TransportKind::Sse);
            Ok(())
        });
    }

    #[test]
    fn test_convenience_alias_works_alone() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_DATABASE_URL", "postgres://alias/tribal");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
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
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
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
            let result = load_config(path.to_str().unwrap(), None, None);
            assert!(result.is_ok(), "stray env vars should be silently ignored");
            Ok(())
        });
    }

    #[test]
    fn test_unknown_top_level_yaml_field_produces_error() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "nonexistent_section:\n  foo: bar\n")?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            assert!(result.is_err(), "unknown top-level YAML key should fail");
            Ok(())
        });
    }

    #[test]
    fn test_unknown_nested_yaml_field_produces_error() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "database:\n  nonexistent: true\n")?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
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
            jail.set_env("TRIBAL_PROMPTS__SOURCE__KIND", "disk");
            jail.set_env("TRIBAL_PROMPTS__SOURCE__HOT_RELOAD", "true");
            jail.set_env("TRIBAL_DISCOVERY__MAX_LIMIT", "100");
            jail.set_env("TRIBAL_EXPLORATION__MAX_DEPTH", "5");
            jail.set_env("TRIBAL_LOGGING__LEVEL", "debug");
            jail.set_env("TRIBAL_TELEMETRY__SERVICE_NAME", "test-tribal");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();

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
            assert!(
                matches!(
                    config.prompts.source,
                    PromptSource::Disk {
                        hot_reload: true,
                        ..
                    },
                ),
                "expected Disk variant with hot_reload=true, got {:?}",
                config.prompts.source,
            );
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
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();

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
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(config.server.sse.idle_timeout_ms, 60_000);
            Ok(())
        });
    }

    // -- Command defaults ----------------------------------------------------

    #[test]
    fn test_command_defaults_override_compiled_defaults() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let defaults = [("database.url", "postgres://cmd-default/tribal")];
            let config = load_config(path.to_str().unwrap(), None, Some(&defaults)).unwrap();
            assert_eq!(config.database.url, "postgres://cmd-default/tribal");
            Ok(())
        });
    }

    #[test]
    fn test_yaml_overrides_command_defaults() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                "database:\n  url: \"postgres://yaml-wins/tribal\"\n",
            )?;

            let path = jail.directory().join("tribal.yaml");
            let defaults = [("database.url", "postgres://cmd-default/tribal")];
            let config = load_config(path.to_str().unwrap(), None, Some(&defaults)).unwrap();
            assert_eq!(config.database.url, "postgres://yaml-wins/tribal");
            Ok(())
        });
    }

    #[test]
    fn test_database_cli_overrides_take_highest_precedence() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                "database:\n  url: \"postgres://yaml/tribal\"\n",
            )?;

            let overrides = CliOverrides {
                server: None,
                database: Some(DatabaseCliOverrides {
                    url: Some("postgres://cli-wins/tribal".into()),
                }),
            };

            let path = jail.directory().join("tribal.yaml");
            let defaults = [("database.url", "postgres://cmd-default/tribal")];
            let config =
                load_config(path.to_str().unwrap(), Some(overrides), Some(&defaults)).unwrap();
            assert_eq!(config.database.url, "postgres://cli-wins/tribal");
            Ok(())
        });
    }
}
