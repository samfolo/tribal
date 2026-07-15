//! Figment-based layered configuration loading.
//!
//! Merges five sources in precedence order:
//! compiled defaults → command defaults → YAML file → environment variables → CLI flags.

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
};
use serde_json::Value as JsonValue;
use tribal_domain::{ApiKey, ProviderKind, normalise_endpoint_url};

use crate::{
    CliOverrides, LoggingConfig, TelemetryConfig, TribalConfig,
    env::{
        ALIAS_ENV_VARS, ENV_NESTED_SEPARATOR, ENV_PREFIX, public_mcp_url_override,
        standard_env_var_name,
    },
    error::{ConfigError, RemovedEmbeddingSource},
    sections::PromptSource,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Top-level scalar fields that should not be overridden via env vars.
#[cfg(test)]
const KNOWN_SCALARS: &[&str] = &["version"];

/// Top-level section names in the configuration.
///
/// Only `TRIBAL_*` env vars whose post-prefix-strip, post-split key starts
/// with one of these sections are accepted.  Stray env vars like
/// `TRIBAL_CONFIG_PATH` or `TRIBAL_PROJECT_ID` are silently ignored.
const KNOWN_SECTIONS: &[&str] = &[
    "agents.",
    "server.",
    "database.",
    "auth.",
    "oauth.",
    "worker.",
    "init.",
    "credentials.",
    "inference.",
    "limits.",
    "prompts.",
    "discovery.",
    "exploration.",
    "logging.",
    "telemetry.",
];

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

    if let Some(detected) = detect_removed_embedding_shape(expanded_path.as_ref()) {
        return Err(ConfigError::RemovedEmbeddingShape { detected });
    }

    let figment = base_figment(command_defaults).merge(Yaml::file(expanded_path.as_ref()));
    extract_config(figment, cli_overrides)
}

/// Loads the same configuration cascade from already-observed YAML bytes.
///
/// This entry point lets a filesystem authority parse the exact bytes whose
/// identity and digest it proved without reopening the pathname.
///
/// # Errors
///
/// Returns [`ConfigError::Load`] if deserialisation or merging fails, or
/// [`ConfigError::RemovedEmbeddingShape`] for the retired top-level shape.
pub fn load_config_from_yaml(
    yaml: &str,
    cli_overrides: Option<CliOverrides>,
    command_defaults: Option<&[(&str, &str)]>,
) -> Result<TribalConfig, ConfigError> {
    if let Some(detected) =
        detect_removed_embedding_env().or_else(|| detect_removed_embedding_shape_in(yaml))
    {
        return Err(ConfigError::RemovedEmbeddingShape { detected });
    }

    let figment = base_figment(command_defaults).merge(Yaml::string(yaml));
    extract_config(figment, cli_overrides)
}

fn base_figment(command_defaults: Option<&[(&str, &str)]>) -> Figment {
    let mut figment = Figment::from(Serialized::defaults(TribalConfig::default()));
    if let Some(defaults) = command_defaults {
        let value = build_nested_json(defaults);
        figment = figment.merge(Serialized::defaults(value));
    }
    figment
}

fn extract_config(
    mut figment: Figment,
    cli_overrides: Option<CliOverrides>,
) -> Result<TribalConfig, ConfigError> {
    let nested_env = Env::prefixed(ENV_PREFIX)
        .split(ENV_NESTED_SEPARATOR)
        .filter(|key| {
            let k = key.as_str().to_lowercase();
            KNOWN_SECTIONS.iter().any(|section| k.starts_with(section))
        });

    let alias_names: Vec<&str> = ALIAS_ENV_VARS.iter().map(|&(_, var)| var).collect();
    let alias_env = Env::raw().only(&alias_names).map(|key| {
        ALIAS_ENV_VARS
            .iter()
            .find(|&&(_, var)| var.eq_ignore_ascii_case(key.as_str()))
            .map_or_else(|| key.into(), |&(path, _)| path.into())
    });

    figment = figment.merge(nested_env).merge(alias_env);

    if let Some(overrides) = cli_overrides {
        figment = figment.merge(Serialized::globals(overrides));
    }

    let mut config: TribalConfig = figment.extract().map_err(|source| ConfigError::Load {
        source: Box::new(source),
    })?;

    apply_standard_env_var_fallback(&mut config);
    expand_paths(&mut config);
    restore_temp_dir_fallback_flags(&mut config);
    resolve_public_mcp_url(&mut config);

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

/// Env-var prefix for the removed top-level `embedding` section. The live
/// genesis seed is `TRIBAL_INIT__EMBEDDING__*`, so the trailing `__` here is
/// load-bearing: it matches only the removed flat shape, never the nested one.
const REMOVED_EMBEDDING_ENV_PREFIX: &str = "TRIBAL_EMBEDDING__";

/// Top-level YAML key for the removed flat `embedding` section.
const REMOVED_EMBEDDING_YAML_KEY: &str = "embedding";

/// Detects the removed flat `embedding` config shape, if present.
///
/// Returns the first removed input found: a `TRIBAL_EMBEDDING__*` environment
/// variable, or a top-level `embedding:` mapping in the YAML config file. An
/// unreadable or malformed YAML file is left for the figment loader to report
/// as a parse error; this check only fires on an unambiguous removed shape.
fn detect_removed_embedding_shape(yaml_path: &str) -> Option<RemovedEmbeddingSource> {
    if let Some(detected) = detect_removed_embedding_env() {
        return Some(detected);
    }

    let contents = std::fs::read_to_string(yaml_path).ok()?;
    detect_removed_embedding_shape_in(&contents)
}

fn detect_removed_embedding_env() -> Option<RemovedEmbeddingSource> {
    std::env::vars_os().find_map(|(key, _)| {
        let key = key.to_string_lossy();
        key.starts_with(REMOVED_EMBEDDING_ENV_PREFIX)
            .then(|| RemovedEmbeddingSource::EnvVar {
                name: key.into_owned(),
            })
    })
}

fn detect_removed_embedding_shape_in(yaml: &str) -> Option<RemovedEmbeddingSource> {
    let mapping: serde_yaml::Mapping = serde_yaml::from_str(yaml).ok()?;
    mapping
        .contains_key(serde_yaml::Value::from(REMOVED_EMBEDDING_YAML_KEY))
        .then_some(RemovedEmbeddingSource::YamlSection)
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

/// Resolves the publicly-advertised MCP URL from the environment into the
/// config, so consumers read one resolved value rather than each
/// re-reading the environment. `#[serde(skip)]` leaves the field at its
/// default after extraction, so this populates it.
fn resolve_public_mcp_url(config: &mut TribalConfig) {
    config.server.public_mcp_url = public_mcp_url_override();
}

/// Populates `api_key` for any cloud-provider stage where prior cascade
/// layers left it `None`, using the env var returned by
/// [`standard_env_var_name`](crate::env::standard_env_var_name).
///
/// The OS env lookup is opportunistic: a missing, non-Unicode, or
/// malformed value (empty, whitespace-bearing) leaves `api_key` as
/// `None`, which then triggers the existing `validate_api_key_presence`
/// error if no usable key emerged from any layer. Empty or
/// whitespace-bearing values supplied through YAML or `TRIBAL_*__API_KEY`
/// fail at deserialise time instead, courtesy of [`ApiKey`]'s strict
/// `FromStr` impl.
fn apply_standard_env_var_fallback(config: &mut TribalConfig) {
    apply_genesis_credential_fallback(config);
    apply_stage_fallback(
        &mut config.inference.extraction.api_key,
        config.inference.extraction.provider,
    );
    apply_stage_fallback(
        &mut config.inference.triage.api_key,
        config.inference.triage.provider,
    );
    apply_stage_fallback(
        &mut config.inference.relation.api_key,
        config.inference.relation.provider,
    );
}

fn apply_stage_fallback(api_key: &mut Option<ApiKey>, provider: ProviderKind) {
    if api_key.is_none()
        && provider.requires_api_key()
        && let Some(name) = standard_env_var_name(provider)
        && let Some(parsed) = std::env::var(name).ok().and_then(|v| v.parse().ok())
    {
        *api_key = Some(parsed);
    }
}

/// Supplies the genesis embedding endpoint's catalogue key from the bare
/// provider environment variable when neither an in-file value nor
/// `TRIBAL_CREDENTIALS__<NAME>__API_KEY` did, mirroring the per-stage
/// inference fallback. Scoped to the entry matching the `init.embedding`
/// endpoint, so a second same-kind endpoint staged for a migration is never
/// filled from a shared bare variable.
fn apply_genesis_credential_fallback(config: &mut TribalConfig) {
    let provider = config.init.embedding.provider;
    if !provider.requires_api_key() {
        return;
    }
    let Some(name) = standard_env_var_name(provider) else {
        return;
    };
    let Some(key) = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<ApiKey>().ok())
    else {
        return;
    };
    let Some(base_url) = config
        .init
        .embedding
        .base_url
        .clone()
        .or_else(|| provider.default_base_url().map(ToOwned::to_owned))
    else {
        return;
    };
    if let Ok(normalised) = normalise_endpoint_url(&base_url) {
        config
            .credentials
            .fill_missing_key(provider, &normalised, key);
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
    use tribal_domain::TransportKind;

    use super::*;
    use crate::{
        ENV_ANTHROPIC_API_KEY, ENV_OPENAI_API_KEY, ENV_PUBLIC_MCP_URL,
        cli_overrides::{
            DatabaseCliOverrides, EmbeddingCliOverrides, InferenceCliOverrides,
            InferenceStageCliOverrides, InitCliOverrides, ServerCliOverrides,
            TelemetryCliOverrides,
        },
    };

    #[test]
    fn test_defaults_only() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();

            let mut expected = TribalConfig::default();
            expand_paths(&mut expected);
            restore_temp_dir_fallback_flags(&mut expected);
            resolve_public_mcp_url(&mut expected);
            assert_eq!(config, expected);
            Ok(())
        });
    }

    #[test]
    fn test_public_mcp_url_resolved_from_env() {
        Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            jail.set_env(ENV_PUBLIC_MCP_URL, "https://tribal.example.com/mcp");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(
                config.server.public_mcp_url.as_deref(),
                Some("https://tribal.example.com/mcp"),
            );
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
    fn test_embedded_hot_reload_yaml_rejected_at_load() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                "prompts:\n  source:\n    kind: embedded\n    hot_reload: true\n",
            )?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            assert!(
                result.is_err(),
                "`embedded` variant must reject `hot_reload`, got: {result:?}",
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
                ..CliOverrides::default()
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
    fn test_removed_embedding_yaml_shape_names_the_new_sections() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", "embedding:\n  api_key: sk-old\n")?;

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            let message = match result {
                Err(ConfigError::RemovedEmbeddingShape { detected }) => {
                    assert_eq!(detected, RemovedEmbeddingSource::YamlSection);
                    ConfigError::RemovedEmbeddingShape { detected }.to_string()
                }
                other => panic!("expected RemovedEmbeddingShape, got {other:?}"),
            };
            assert!(
                message.contains("init.embedding"),
                "message must name init.embedding: {message}",
            );
            assert!(
                message.contains("credentials"),
                "message must name the credentials catalogue: {message}",
            );
            Ok(())
        });
    }

    #[test]
    fn test_removed_embedding_env_var_names_the_new_sections() {
        Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_EMBEDDING__API_KEY", "sk-old");

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            let message = match result {
                Err(ConfigError::RemovedEmbeddingShape { detected }) => {
                    assert_eq!(
                        detected,
                        RemovedEmbeddingSource::EnvVar {
                            name: "TRIBAL_EMBEDDING__API_KEY".to_owned(),
                        },
                    );
                    ConfigError::RemovedEmbeddingShape { detected }.to_string()
                }
                other => panic!("expected RemovedEmbeddingShape, got {other:?}"),
            };
            assert!(
                message.contains("init.embedding"),
                "message must name init.embedding: {message}",
            );
            assert!(
                message.contains("credentials"),
                "message must name the credentials catalogue: {message}",
            );
            Ok(())
        });
    }

    #[test]
    fn test_live_init_embedding_env_var_is_not_flagged_as_removed_shape() {
        Jail::expect_with(|jail| {
            // The nested genesis seed is the live shape and must not trip the
            // removed-shape detector.
            jail.set_env("TRIBAL_INIT__EMBEDDING__PROVIDER", "openai");

            let path = jail.directory().join("tribal.yaml");
            let result = load_config(path.to_str().unwrap(), None, None);
            assert!(
                !matches!(result, Err(ConfigError::RemovedEmbeddingShape { .. })),
                "the live TRIBAL_INIT__EMBEDDING__* shape must not be flagged: {result:?}",
            );
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
            jail.set_env("TRIBAL_INIT__EMBEDDING__DIMENSIONS", "1024");
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
            assert_eq!(config.init.embedding.dimensions, Some(1024));
            assert_eq!(config.inference.extraction.temperature, Some(0.5));
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
                database: Some(DatabaseCliOverrides {
                    url: Some("postgres://cli-wins/tribal".into()),
                }),
                ..CliOverrides::default()
            };

            let path = jail.directory().join("tribal.yaml");
            let defaults = [("database.url", "postgres://cmd-default/tribal")];
            let config =
                load_config(path.to_str().unwrap(), Some(overrides), Some(&defaults)).unwrap();
            assert_eq!(config.database.url, "postgres://cli-wins/tribal");
            Ok(())
        });
    }

    // -- Standard provider env-var fallback (closes #144) --------------------

    /// A `credentials:` catalogue declaring the genesis endpoint with an
    /// `openai` provider and the canonical base URL, plus an `init.embedding`
    /// seed pointing at that endpoint. The trailing `api_key` line is
    /// appended by each test that wants an in-file key.
    fn openai_genesis_yaml(api_key_line: &str) -> String {
        format!(
            "init:\n  embedding:\n    provider: openai\n\
             credentials:\n  openai_default:\n    provider_kind: openai\n    \
             base_url: https://api.openai.com\n{api_key_line}",
        )
    }

    fn openai_endpoint() -> String {
        normalise_endpoint_url(ProviderKind::OpenAi.default_base_url().unwrap()).unwrap()
    }

    #[test]
    fn test_openai_api_key_fallback_file_wins() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                &openai_genesis_yaml("    api_key: from-file\n"),
            )?;
            jail.set_env(ENV_OPENAI_API_KEY, "from-standard-env");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(
                config
                    .credentials
                    .resolve_api_key(ProviderKind::OpenAi, &openai_endpoint())
                    .unwrap(),
                "from-file",
            );
            Ok(())
        });
    }

    #[test]
    fn test_tribal_env_api_key_beats_standard_env() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", &openai_genesis_yaml(""))?;
            jail.set_env(
                "TRIBAL_CREDENTIALS__OPENAI_DEFAULT__API_KEY",
                "from-tribal-env",
            );
            jail.set_env(ENV_OPENAI_API_KEY, "from-standard-env");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(
                config
                    .credentials
                    .resolve_api_key(ProviderKind::OpenAi, &openai_endpoint())
                    .unwrap(),
                "from-tribal-env",
            );
            Ok(())
        });
    }

    #[test]
    fn test_openai_api_key_fills_the_genesis_connection() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", &openai_genesis_yaml(""))?;
            jail.set_env(ENV_OPENAI_API_KEY, "from-standard-env");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(
                config
                    .credentials
                    .resolve_api_key(ProviderKind::OpenAi, &openai_endpoint())
                    .unwrap(),
                "from-standard-env",
            );
            Ok(())
        });
    }

    #[test]
    fn test_empty_standard_env_var_leaves_the_genesis_connection_unresolved() {
        Jail::expect_with(|jail| {
            jail.create_file("tribal.yaml", &openai_genesis_yaml(""))?;
            // Docker compose's `${OPENAI_API_KEY:-}` emits an empty value
            // when the host env var is unset; the fallback must treat that
            // as "no key reachable" so the catalogue resolves fail-closed.
            jail.set_env(ENV_OPENAI_API_KEY, "");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert!(
                config
                    .credentials
                    .resolve_api_key(ProviderKind::OpenAi, &openai_endpoint())
                    .is_err(),
                "an empty standard env var must not satisfy the genesis credential",
            );
            Ok(())
        });
    }

    #[test]
    fn test_mixed_provider_mixed_stage_fallback() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "tribal.yaml",
                &format!(
                    "{}inference:\n  triage:\n    provider: anthropic\n",
                    openai_genesis_yaml(""),
                ),
            )?;
            jail.set_env(ENV_OPENAI_API_KEY, "openai-key");
            jail.set_env(ENV_ANTHROPIC_API_KEY, "anthropic-key");

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(
                config
                    .credentials
                    .resolve_api_key(ProviderKind::OpenAi, &openai_endpoint())
                    .unwrap(),
                "openai-key",
            );
            assert_eq!(
                config.inference.triage.api_key.as_ref().map(ApiKey::as_str),
                Some("anthropic-key"),
            );
            Ok(())
        });
    }

    #[test]
    fn test_no_key_providers_ignore_all_standard_env_vars() {
        Jail::expect_with(|jail| {
            // Set every standard env var that any provider exposes, so the
            // sweep below covers each `ProviderKind` against the full set.
            for kind in ProviderKind::ALL {
                if let Some(name) = standard_env_var_name(kind) {
                    jail.set_env(name, "should-be-ignored");
                }
            }

            // For every provider that does not require an API key, the
            // fallback must leave the inference stages as `None` and never
            // synthesise or fill a genesis catalogue key.
            for provider in ProviderKind::ALL
                .into_iter()
                .filter(|k| !k.requires_api_key())
            {
                let yaml = format!(
                    "\
init:
  embedding:
    provider: {provider}
inference:
  extraction:
    provider: {provider}
  triage:
    provider: {provider}
  relation:
    provider: {provider}
"
                );
                jail.create_file("tribal.yaml", &yaml)?;
                let path = jail.directory().join("tribal.yaml");
                let config = load_config(path.to_str().unwrap(), None, None).unwrap();

                assert!(
                    config.credentials.is_empty(),
                    "{provider} genesis synthesised a catalogue key from a standard env var",
                );
                let stages = [
                    ("inference.extraction", &config.inference.extraction.api_key),
                    ("inference.triage", &config.inference.triage.api_key),
                    ("inference.relation", &config.inference.relation.api_key),
                ];
                for (label, api_key) in stages {
                    assert!(
                        api_key.is_none(),
                        "{provider} at {label} consumed a standard env var, got {api_key:?}",
                    );
                }
            }
            Ok(())
        });
    }

    // -- Provider / Telemetry CliOverrides cascade ---------------------------

    #[test]
    fn test_provider_cli_overrides_cascade() {
        Jail::expect_with(|jail| {
            let overrides = CliOverrides {
                init: Some(InitCliOverrides {
                    embedding: Some(EmbeddingCliOverrides {
                        provider: Some(ProviderKind::OpenAi),
                        model: Some("text-embedding-3-small".into()),
                    }),
                }),
                inference: Some(InferenceCliOverrides {
                    extraction: Some(InferenceStageCliOverrides {
                        provider: Some(ProviderKind::Anthropic),
                        model: Some("claude-opus-4".into()),
                    }),
                    triage: Some(InferenceStageCliOverrides {
                        provider: Some(ProviderKind::OpenAi),
                        model: Some("gpt-5".into()),
                    }),
                    relation: Some(InferenceStageCliOverrides {
                        provider: Some(ProviderKind::Anthropic),
                        model: Some("claude-haiku-5".into()),
                    }),
                }),
                ..CliOverrides::default()
            };

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), Some(overrides), None).unwrap();

            assert_eq!(config.init.embedding.provider, ProviderKind::OpenAi);
            assert_eq!(config.init.embedding.model, "text-embedding-3-small");
            assert_eq!(
                config.inference.extraction.provider,
                ProviderKind::Anthropic,
            );
            assert_eq!(config.inference.extraction.model, "claude-opus-4");
            assert_eq!(config.inference.triage.provider, ProviderKind::OpenAi);
            assert_eq!(config.inference.triage.model, "gpt-5");
            assert_eq!(config.inference.relation.provider, ProviderKind::Anthropic);
            assert_eq!(config.inference.relation.model, "claude-haiku-5");
            Ok(())
        });
    }

    #[test]
    fn test_telemetry_cli_overrides_cascade() {
        Jail::expect_with(|jail| {
            let overrides = CliOverrides {
                telemetry: Some(TelemetryCliOverrides {
                    otlp_endpoint: Some("http://collector.internal:4317".into()),
                }),
                ..CliOverrides::default()
            };

            let path = jail.directory().join("tribal.yaml");
            let config = load_config(path.to_str().unwrap(), Some(overrides), None).unwrap();

            assert_eq!(
                config.telemetry.otlp_endpoint.as_deref(),
                Some("http://collector.internal:4317"),
            );
            Ok(())
        });
    }
}
