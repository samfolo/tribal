//! Operator reads and writes over the resolved configuration.
//!
//! The control bridge answers `config.get`/`config.getAll` by redacting the
//! resolved configuration, `config.validate` by checking a proposed write
//! against the whole invariant set, and `config.set` by persisting one key to
//! the YAML file — layer four of the six-layer cascade — and reporting honestly
//! whether it took effect. A higher layer (an environment variable) that also
//! sets the key leaves the write persisted but shadowed. Every returned value
//! is config-native; the wire layer maps it to its DTO, so this crate never
//! depends on the wire contract.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::Value;
use thiserror::Error;

use crate::config_schema::{ReloadClass, reload_class};
use crate::env::{ALIAS_ENV_VARS, env_var_for_path};
use crate::redact::redact_secrets;
use crate::{ConfigError, TribalConfig, validate};

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// What a persisted write to a key achieved.
///
/// `Shadowed` carries the layer that overrides it, so the "shadowed but by
/// what" state is unrepresentable without its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteEffect {
    /// The write took effect immediately.
    Live,
    /// The write is persisted but applies only after a restart.
    NeedsRestart,
    /// A higher-precedence layer overrides the write, so it is persisted but
    /// never effective until that layer is cleared. Carries the overriding
    /// environment variable.
    Shadowed {
        /// The environment variable whose value wins over the file.
        by: String,
    },
}

/// One reason a proposed configuration write is unacceptable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigViolation {
    /// The dotted key the write targeted.
    pub key: String,
    /// A one-line description of what is wrong.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A read of a dotted key the configuration does not define.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown configuration key: {key}")]
pub struct UnknownConfigKey {
    /// The key that did not resolve.
    pub key: String,
}

/// Why a `config.set` did not complete.
#[derive(Debug, Error)]
pub enum SetError {
    /// The proposed write is invalid; the whole write is refused, the file
    /// left unchanged.
    #[error("the proposed configuration write is invalid")]
    Rejected {
        /// Every reason the write was refused.
        violations: Vec<ConfigViolation>,
    },
    /// The existing config file could not be parsed, so it was not overwritten.
    #[error("the config file at {path} could not be parsed, so it was left unchanged")]
    Unparseable {
        /// The config file that failed to parse.
        path: PathBuf,
    },
    /// Serialising or writing the updated configuration failed.
    #[error("could not write the config file at {path}: {source}")]
    Io {
        /// The config file being written.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The updated configuration could not be serialised to YAML.
    #[error("could not serialise the updated configuration: {source}")]
    Serialise {
        /// The underlying serialisation failure.
        #[source]
        source: serde_yaml::Error,
    },
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// The effective value at a dotted key, redacted when the key is a secret.
///
/// # Errors
///
/// Returns [`UnknownConfigKey`] when the key does not resolve to a value.
pub fn get(config: &TribalConfig, key: &str) -> Result<Value, UnknownConfigKey> {
    let document = redacted_document(config);
    lookup(&document, key)
        .cloned()
        .ok_or_else(|| UnknownConfigKey {
            key: key.to_owned(),
        })
}

/// The whole effective configuration as one redacted JSON document.
#[must_use]
pub fn get_all(config: &TribalConfig) -> Value {
    redacted_document(config)
}

/// The resolved configuration serialised to a JSON document with every secret
/// redacted — the read surface `get`/`getAll` answer from.
fn redacted_document(config: &TribalConfig) -> Value {
    let yaml = serde_yaml::to_string(config).expect("a resolved config serialises to YAML");
    let redacted = redact_secrets(&yaml);
    serde_yaml::from_str(&redacted).expect("redacted YAML parses back to a JSON value")
}

/// Follows a dotted key into a JSON document.
fn lookup<'a>(root: &'a Value, key: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in key.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

/// Checks a proposed write against the whole configuration invariant set,
/// without persisting it. An empty result means the write is acceptable.
#[must_use]
pub fn validate_write(config: &TribalConfig, key: &str, value: Value) -> Vec<ConfigViolation> {
    let candidate = match apply_to_config(config, key, value) {
        Ok(candidate) => candidate,
        Err(message) => {
            return vec![ConfigViolation {
                key: key.to_owned(),
                message,
            }];
        }
    };
    match validate(&candidate) {
        Ok(()) => Vec::new(),
        Err(ConfigError::ValidationFailed { diagnostics }) => diagnostics
            .iter()
            .map(|diagnostic| ConfigViolation {
                key: key.to_owned(),
                message: diagnostic.to_string(),
            })
            .collect(),
        Err(other) => vec![ConfigViolation {
            key: key.to_owned(),
            message: other.to_string(),
        }],
    }
}

/// Applies a proposed value at `key` onto the resolved config and deserialises
/// the result, so a type mismatch or unknown key surfaces as an error before
/// any semantic check runs.
fn apply_to_config(config: &TribalConfig, key: &str, value: Value) -> Result<TribalConfig, String> {
    let mut tree = serde_json::to_value(config).expect("a resolved config serialises to JSON");
    apply_value(&mut tree, key, value)?;
    serde_json::from_value(tree).map_err(|error| format!("invalid value for {key}: {error}"))
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

/// Persists a write of one key to the YAML config file and reports its effect.
///
/// The write is validated against the whole configuration first and refused
/// whole if invalid. On success the key is written atomically to the file, and
/// the effect states whether it is live, awaits a restart, or is shadowed by a
/// higher-precedence environment layer.
///
/// # Errors
///
/// Returns [`SetError::Rejected`] when the write is invalid, and the file
/// variants when persistence fails.
pub fn set(
    config: &TribalConfig,
    config_file: &Path,
    key: &str,
    value: Value,
) -> Result<WriteEffect, SetError> {
    let violations = validate_write(config, key, value.clone());
    if !violations.is_empty() {
        return Err(SetError::Rejected { violations });
    }
    persist(config_file, key, value)?;
    Ok(write_effect(key))
}

/// Writes one key into the config file's YAML document, atomically.
fn persist(config_file: &Path, key: &str, value: Value) -> Result<(), SetError> {
    let mut document = read_document(config_file)?;
    apply_value(&mut document, key, value).map_err(|message| SetError::Rejected {
        violations: vec![ConfigViolation {
            key: key.to_owned(),
            message,
        }],
    })?;
    let yaml = serde_yaml::to_string(&document).map_err(|source| SetError::Serialise { source })?;
    write_atomically(config_file, yaml.as_bytes()).map_err(|source| SetError::Io {
        path: config_file.to_owned(),
        source,
    })
}

/// Reads the config file into a JSON document, treating an absent or empty file
/// as an empty object.
fn read_document(config_file: &Path) -> Result<Value, SetError> {
    match std::fs::read_to_string(config_file) {
        Ok(content) => {
            let parsed: Value =
                serde_yaml::from_str(&content).map_err(|_| SetError::Unparseable {
                    path: config_file.to_owned(),
                })?;
            Ok(if parsed.is_null() {
                empty_object()
            } else {
                parsed
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(empty_object()),
        Err(source) => Err(SetError::Io {
            path: config_file.to_owned(),
            source,
        }),
    }
}

/// Sets a dotted key to a value in a JSON document, creating intermediate
/// objects as needed. Fails only when an existing segment is not an object.
fn apply_value(root: &mut Value, key: &str, value: Value) -> Result<(), String> {
    let segments: Vec<&str> = key.split('.').collect();
    let (last, parents) = segments
        .split_last()
        .expect("split yields at least one segment");

    let mut current = root;
    for segment in parents {
        let object = current
            .as_object_mut()
            .ok_or_else(|| non_object_error(key))?;
        current = object
            .entry((*segment).to_owned())
            .or_insert_with(empty_object);
    }
    current
        .as_object_mut()
        .ok_or_else(|| non_object_error(key))?
        .insert((*last).to_owned(), value);
    Ok(())
}

/// Writes bytes to `path` atomically via a sibling tempfile renamed into place,
/// with owner-only permissions (the file may hold a secret). Modelled on the
/// credentials writer.
fn write_atomically(path: &Path, payload: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let mut tempfile = tempfile::NamedTempFile::new_in(parent)?;
    tempfile.write_all(payload)?;
    tempfile.as_file().sync_all()?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tempfile
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }

    tempfile.persist(path).map_err(|error| error.error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Write effect
// ---------------------------------------------------------------------------

/// Classifies how a persisted write to `key` takes effect for the running
/// binary: shadowed by an environment layer, live, or awaiting a restart.
fn write_effect(key: &str) -> WriteEffect {
    if let Some(source) = shadowing_env_var(key) {
        return WriteEffect::Shadowed { by: source };
    }
    match reload_class(key) {
        ReloadClass::Hot => WriteEffect::Live,
        ReloadClass::RequiresRestart | ReloadClass::Unclassified => WriteEffect::NeedsRestart,
    }
}

/// The environment variable that shadows `key`, if one is set.
///
/// The alias layer sits above the nested layer, so a set alias is named first,
/// matching the loader's merge order. A present variable shadows regardless of
/// value: the loader merges it over the file either way.
fn shadowing_env_var(key: &str) -> Option<String> {
    if let Some(&(_, alias)) = ALIAS_ENV_VARS.iter().find(|&&(path, _)| path == key)
        && is_set(alias)
    {
        return Some(alias.to_owned());
    }
    let nested = env_var_for_path(key);
    is_set(&nested).then_some(nested)
}

/// Whether an environment variable is present, which is what makes its layer
/// override the file.
fn is_set(name: &str) -> bool {
    std::env::var_os(name).is_some()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn non_object_error(key: &str) -> String {
    format!("{key} passes through a value that is not an object")
}

#[cfg(test)]
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A resolved config that passes validation, the base every operation reads.
    fn base_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal")
    }

    #[test]
    fn test_get_reads_a_leaf() {
        let config = base_config();
        assert_eq!(get(&config, "server.transport").unwrap(), json!("stdio"));
    }

    #[test]
    fn test_get_unknown_key_is_an_error() {
        let config = base_config();
        assert_eq!(
            get(&config, "server.nonexistent").unwrap_err(),
            UnknownConfigKey {
                key: "server.nonexistent".to_owned()
            },
        );
    }

    #[test]
    fn test_get_all_redacts_both_static_and_catalogue_secrets() {
        let mut config = base_config();
        config.inference.extraction.api_key = Some("sk-static-secret".parse().unwrap());
        config.credentials.insert(
            "openai_default".to_owned(),
            serde_yaml::from_str("provider_kind: openai\nbase_url: https://api.openai.com\napi_key: sk-catalogue-secret")
                .unwrap(),
        );

        let document = get_all(&config).to_string();
        assert!(
            !document.contains("pass@localhost"),
            "db url leaked: {document}"
        );
        assert!(
            !document.contains("sk-static-secret"),
            "static key leaked: {document}"
        );
        assert!(
            !document.contains("sk-catalogue-secret"),
            "catalogue key leaked: {document}"
        );
    }

    #[test]
    fn test_set_persists_and_reads_back_through_the_loader() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let effect = set(&base_config(), &path, "logging.level", json!("debug")).unwrap();
            assert_eq!(effect, WriteEffect::NeedsRestart);

            // The loader reads the persisted value back.
            let reloaded = crate::load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(reloaded.logging.level, "debug");
            Ok(())
        });
    }

    #[test]
    fn test_set_persists_a_secret_to_the_yaml_home() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let secret = "postgres://written:secret@host:5432/db2";
            set(&base_config(), &path, "database.url", json!(secret)).unwrap();

            let reloaded = crate::load_config(path.to_str().unwrap(), None, None).unwrap();
            assert_eq!(reloaded.database.url, secret);
            // And the read path redacts it.
            assert_eq!(get(&reloaded, "database.url").unwrap(), json!("********"));
            Ok(())
        });
    }

    #[test]
    fn test_set_shadowed_by_an_alias_env_layer() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_LOG", "warn");
            let path = jail.directory().join("tribal.yaml");
            let effect = set(&base_config(), &path, "logging.level", json!("debug")).unwrap();
            assert_eq!(
                effect,
                WriteEffect::Shadowed {
                    by: "TRIBAL_LOG".to_owned()
                }
            );

            // The write still persists — the shadow is a higher layer, not a refusal.
            let document = std::fs::read_to_string(&path).unwrap();
            assert!(
                document.contains("debug"),
                "the write must persist: {document}"
            );
            Ok(())
        });
    }

    #[test]
    fn test_set_shadowed_by_a_nested_env_layer() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_DISCOVERY__MAX_LIMIT", "99");
            let path = jail.directory().join("tribal.yaml");
            let effect = set(&base_config(), &path, "discovery.max_limit", json!(42)).unwrap();
            assert_eq!(
                effect,
                WriteEffect::Shadowed {
                    by: "TRIBAL_DISCOVERY__MAX_LIMIT".to_owned()
                },
            );
            Ok(())
        });
    }

    #[test]
    fn test_set_refuses_an_invalid_value_whole() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let error = set(&base_config(), &path, "server.transport", json!("grpc")).unwrap_err();
            assert!(matches!(error, SetError::Rejected { .. }));
            // The file is never created for a refused write.
            assert!(!path.exists(), "a refused write must not touch the file");
            Ok(())
        });
    }

    #[test]
    fn test_validate_write_reports_a_semantic_violation() {
        let config = base_config();
        let violations = validate_write(&config, "database.url", json!(""));
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].key, "database.url");
        assert!(
            violations[0].message.contains("database.url"),
            "message should name the field: {}",
            violations[0].message,
        );
    }

    #[test]
    fn test_validate_write_accepts_a_valid_change() {
        let config = base_config();
        assert!(validate_write(&config, "worker.poll_interval_ms", json!(5000)).is_empty());
    }

    #[test]
    fn test_set_leaves_no_tempfile_and_writes_valid_yaml() {
        figment::Jail::expect_with(|jail| {
            let dir = jail.directory().to_owned();
            let path = dir.join("tribal.yaml");
            set(&base_config(), &path, "logging.level", json!("trace")).unwrap();

            // The document round-trips as valid YAML, and no tempfile is orphaned.
            let content = std::fs::read_to_string(&path).unwrap();
            serde_yaml::from_str::<serde_json::Value>(&content).expect("valid YAML");
            let strays = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.path() != path)
                .count();
            assert_eq!(strays, 0, "no tempfile should be left behind");
            Ok(())
        });
    }
}
