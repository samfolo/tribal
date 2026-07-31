//! Operator reads and writes over the resolved configuration.
//!
//! The control bridge answers `config.get`/`config.getAll` by redacting the
//! resolved configuration, `config.validate` by checking a proposed write
//! against the whole invariant set, and `config.set` by persisting one key to
//! the YAML file — layer four of the six-layer cascade — and reporting honestly
//! whether it took effect. A higher layer (a command-line flag the process
//! launched with, or an environment variable) that also sets the key leaves the
//! write persisted but shadowed. Every returned value
//! is config-native; the wire layer maps it to its DTO, so this crate never
//! depends on the wire contract.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde_json::Value;
use thiserror::Error;

use crate::{
    CliOverrides, ConfigError, TribalConfig,
    atomic_write::write_atomically,
    config_schema::{ReloadClass, reload_class},
    env::{ALIAS_ENV_VARS, env_var_for_path},
    redact::redact_secrets,
    validate,
};

/// Owner-only permissions for the config file, which may hold a secret.
const CONFIG_FILE_MODE: u32 = 0o600;

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
    /// The file already held the requested value, so nothing was written.
    Unchanged,
    /// The write is persisted but applies only after a restart.
    NeedsRestart,
    /// A higher-precedence layer overrides the write, so it is persisted but
    /// never effective until that layer is cleared. Carries the overriding
    /// layer, named.
    Shadowed {
        /// The higher layer whose value wins over the file — an environment
        /// variable's name, or a command-line flag.
        by: String,
    },
}

/// A completed `config.set`: how the write takes effect, and the exact bytes
/// written to the file when persistence occurred — so a caller coordinating
/// with a file watcher records what it wrote rather than re-reading and racing
/// a concurrent edit.
#[derive(Debug, Clone)]
pub struct Persisted {
    /// How the write takes effect for the running binary.
    pub effect: WriteEffect,
    /// The exact bytes written to the config file, absent for an unchanged
    /// write.
    pub document: Option<Vec<u8>>,
    /// The resolved configuration with the write applied — the snapshot the
    /// running process serves once a live write is adopted.
    pub config: TribalConfig,
}

/// A completed atomic multi-field configuration write.
#[derive(Debug, Clone)]
pub struct PersistedPatch {
    /// Per-input write effects in the same order as the requested changes.
    pub effects: Vec<WriteEffect>,
    /// Exact bytes written, absent when every requested value was unchanged.
    pub document: Option<Vec<u8>>,
    /// Resolved configuration with the complete patch applied.
    pub config: TribalConfig,
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
    #[error("the config file at {path} could not be parsed, so it was left unchanged: {source}")]
    Unparseable {
        /// The config file that failed to parse.
        path: PathBuf,
        /// The parse failure, carrying the line and column it stopped at.
        #[source]
        source: serde_yaml::Error,
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

impl SetError {
    /// Validation details when the candidate was rejected before persistence.
    #[must_use]
    pub fn violations(&self) -> Option<&[ConfigViolation]> {
        match self {
            Self::Rejected { violations } => Some(violations),
            Self::Unparseable { .. } | Self::Io { .. } | Self::Serialise { .. } => None,
        }
    }
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
    match validated_candidate(config, key, value) {
        Ok(_) => Vec::new(),
        Err(violations) => violations,
    }
}

/// Checks a complete patch against the whole configuration invariant set.
///
/// Each relationship violation is projected onto every participating field.
/// An empty result means the patch is acceptable.
#[must_use]
pub fn validate_patch(config: &TribalConfig, changes: &[(String, Value)]) -> Vec<ConfigViolation> {
    let mut tree = match serde_json::to_value(config) {
        Ok(tree) => tree,
        Err(source) => {
            return vec![ConfigViolation {
                key: String::new(),
                message: source.to_string(),
            }];
        }
    };
    for (key, value) in changes {
        if let Err(message) = apply_value(&mut tree, key, value.clone()) {
            return vec![ConfigViolation {
                key: key.clone(),
                message,
            }];
        }
    }
    let candidate: TribalConfig = match serde_json::from_value(tree) {
        Ok(candidate) => candidate,
        Err(source) => {
            return vec![ConfigViolation {
                key: String::new(),
                message: source.to_string(),
            }];
        }
    };
    match validate(&candidate) {
        Ok(()) => Vec::new(),
        Err(ConfigError::ValidationFailed { diagnostics }) => diagnostics
            .iter()
            .flat_map(|diagnostic| {
                diagnostic
                    .fields()
                    .into_iter()
                    .map(|field| ConfigViolation {
                        key: field.as_str().to_owned(),
                        message: diagnostic.to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
        Err(other) => vec![ConfigViolation {
            key: String::new(),
            message: other.to_string(),
        }],
    }
}

/// The resolved configuration with `value` applied at `key`, checked against
/// the whole invariant set — or every reason the write is unacceptable.
fn validated_candidate(
    config: &TribalConfig,
    key: &str,
    value: Value,
) -> Result<TribalConfig, Vec<ConfigViolation>> {
    let candidate = apply_to_config(config, key, value).map_err(|message| {
        vec![ConfigViolation {
            key: key.to_owned(),
            message,
        }]
    })?;
    match validate(&candidate) {
        Ok(()) => Ok(candidate),
        Err(ConfigError::ValidationFailed { diagnostics }) => Err(diagnostics
            .iter()
            .map(|diagnostic| ConfigViolation {
                key: key.to_owned(),
                message: diagnostic.to_string(),
            })
            .collect()),
        Err(other) => Err(vec![ConfigViolation {
            key: key.to_owned(),
            message: other.to_string(),
        }]),
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
/// higher-precedence layer — a command-line flag the process launched with, or
/// an environment variable.
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
    cli: &CliShadow,
) -> Result<Persisted, SetError> {
    let document = read_document(config_file)?;
    set_in_document(config, config_file, key, value, cli, document)
}

/// Persists one field against an exact already-observed YAML document.
///
/// # Errors
///
/// Returns [`SetError::Rejected`] when the write is invalid, and the file
/// variants when the supplied YAML cannot be parsed or persistence fails.
pub fn set_from_yaml(
    config: &TribalConfig,
    config_file: &Path,
    yaml: &[u8],
    key: &str,
    value: Value,
    cli: &CliShadow,
) -> Result<Persisted, SetError> {
    let document = parse_document(config_file, yaml)?;
    set_in_document(config, config_file, key, value, cli, document)
}

fn set_in_document(
    config: &TribalConfig,
    config_file: &Path,
    key: &str,
    value: Value,
    cli: &CliShadow,
    document: Value,
) -> Result<Persisted, SetError> {
    let candidate = validated_candidate(config, key, value.clone())
        .map_err(|violations| SetError::Rejected { violations })?;
    if lookup(&document, key) == Some(&value) {
        return Ok(Persisted {
            effect: WriteEffect::Unchanged,
            document: None,
            config: candidate,
        });
    }
    let (persist_key, persist_value) = persistence_entry(&candidate, key, value);
    let document = persist_document(config_file, &persist_key, persist_value, document)?;
    Ok(Persisted {
        effect: write_effect(key, cli),
        document: Some(document),
        config: candidate,
    })
}

/// Validates and persists multiple configuration values as one atomic candidate.
///
/// Structural patch rules such as duplicate and overlapping paths belong to
/// the application service. This function owns config-native type and semantic
/// validation plus the single atomic file replacement.
///
/// # Errors
///
/// Returns [`SetError::Rejected`] when the complete candidate is invalid, and
/// the file variants when reading or atomically replacing the document fails.
pub fn patch(
    config: &TribalConfig,
    config_file: &Path,
    changes: &[(String, Value)],
    cli: &CliShadow,
) -> Result<PersistedPatch, SetError> {
    let document = read_document(config_file)?;
    patch_in_document(config, config_file, changes, cli, document)
}

/// Persists a patch against an exact already-observed YAML document.
///
/// # Errors
///
/// Returns [`SetError::Rejected`] when the complete candidate is invalid, and
/// the file variants when the supplied YAML cannot be parsed or persistence fails.
pub fn patch_from_yaml(
    config: &TribalConfig,
    config_file: &Path,
    yaml: &[u8],
    changes: &[(String, Value)],
    cli: &CliShadow,
) -> Result<PersistedPatch, SetError> {
    let document = parse_document(config_file, yaml)?;
    patch_in_document(config, config_file, changes, cli, document)
}

fn patch_in_document(
    config: &TribalConfig,
    config_file: &Path,
    changes: &[(String, Value)],
    cli: &CliShadow,
    mut document: Value,
) -> Result<PersistedPatch, SetError> {
    let mut candidate_tree = serde_json::to_value(config).map_err(|source| SetError::Rejected {
        violations: vec![ConfigViolation {
            key: String::new(),
            message: source.to_string(),
        }],
    })?;
    for (key, value) in changes {
        apply_value(&mut candidate_tree, key, value.clone()).map_err(|message| {
            SetError::Rejected {
                violations: vec![ConfigViolation {
                    key: key.clone(),
                    message,
                }],
            }
        })?;
    }
    let candidate: TribalConfig =
        serde_json::from_value(candidate_tree).map_err(|source| SetError::Rejected {
            violations: vec![ConfigViolation {
                key: String::new(),
                message: source.to_string(),
            }],
        })?;
    if let Err(error) = validate(&candidate) {
        return Err(SetError::Rejected {
            violations: vec![ConfigViolation {
                key: String::new(),
                message: error.to_string(),
            }],
        });
    }

    let mut document_changed = false;
    let mut effects = Vec::with_capacity(changes.len());
    for (key, value) in changes {
        if lookup(&document, key) == Some(value) {
            effects.push(WriteEffect::Unchanged);
            continue;
        }
        apply_value(&mut document, key, value.clone()).map_err(|message| SetError::Rejected {
            violations: vec![ConfigViolation {
                key: key.clone(),
                message,
            }],
        })?;
        document_changed = true;
        effects.push(write_effect(key, cli));
    }
    if !document_changed {
        return Ok(PersistedPatch {
            effects,
            document: None,
            config: candidate,
        });
    }
    let bytes = serde_yaml::to_string(&document)
        .map_err(|source| SetError::Serialise { source })?
        .into_bytes();
    write_atomically(config_file, &bytes, Some(CONFIG_FILE_MODE)).map_err(|source| {
        SetError::Io {
            path: config_file.to_owned(),
            source,
        }
    })?;
    Ok(PersistedPatch {
        effects,
        document: Some(bytes),
        config: candidate,
    })
}

/// Replaces an invalid document with a validated patch over compiled defaults.
///
/// This is the repair path: the invalid bytes remain untouched unless the
/// complete candidate validates, then one atomic replacement establishes the
/// new durable document.
///
/// # Errors
///
/// Returns [`SetError::Rejected`] when the repair candidate is invalid, and
/// the file variants when serialisation or atomic persistence fails.
pub fn repair_patch(
    config_file: &Path,
    changes: &[(String, Value)],
    cli: &CliShadow,
) -> Result<PersistedPatch, SetError> {
    let base = TribalConfig::default();
    let mut candidate_tree = serde_json::to_value(&base).map_err(|source| SetError::Rejected {
        violations: vec![ConfigViolation {
            key: String::new(),
            message: source.to_string(),
        }],
    })?;
    let mut document = empty_object();
    for (key, value) in changes {
        apply_value(&mut candidate_tree, key, value.clone()).map_err(|message| {
            SetError::Rejected {
                violations: vec![ConfigViolation {
                    key: key.clone(),
                    message,
                }],
            }
        })?;
        apply_value(&mut document, key, value.clone()).map_err(|message| SetError::Rejected {
            violations: vec![ConfigViolation {
                key: key.clone(),
                message,
            }],
        })?;
    }
    let candidate: TribalConfig =
        serde_json::from_value(candidate_tree).map_err(|source| SetError::Rejected {
            violations: vec![ConfigViolation {
                key: String::new(),
                message: source.to_string(),
            }],
        })?;
    if let Err(error) = validate(&candidate) {
        return Err(SetError::Rejected {
            violations: vec![ConfigViolation {
                key: String::new(),
                message: error.to_string(),
            }],
        });
    }
    let bytes = serde_yaml::to_string(&document)
        .map_err(|source| SetError::Serialise { source })?
        .into_bytes();
    write_atomically(config_file, &bytes, Some(CONFIG_FILE_MODE)).map_err(|source| {
        SetError::Io {
            path: config_file.to_owned(),
            source,
        }
    })?;
    Ok(PersistedPatch {
        effects: changes
            .iter()
            .map(|(key, _)| write_effect(key, cli))
            .collect(),
        document: Some(bytes),
        config: candidate,
    })
}

fn persistence_entry(config: &TribalConfig, key: &str, value: Value) -> (String, Value) {
    let Some(connection) = provider_connection_name(key) else {
        return (key.to_owned(), value);
    };
    let entry_key = format!("provider_connections.{connection}");
    let tree = serde_json::to_value(config).expect("a resolved config serialises to JSON");
    match lookup(&tree, &entry_key) {
        Some(entry) => (entry_key, entry.clone()),
        None => (key.to_owned(), value),
    }
}

fn provider_connection_name(key: &str) -> Option<&str> {
    let mut segments = key.split('.');
    (segments.next()? == "provider_connections")
        .then(|| segments.next())
        .flatten()
}

/// Writes one key into the config file's YAML document atomically, returning the
/// bytes it wrote.
fn persist_document(
    config_file: &Path,
    key: &str,
    value: Value,
    mut document: Value,
) -> Result<Vec<u8>, SetError> {
    apply_value(&mut document, key, value).map_err(|message| SetError::Rejected {
        violations: vec![ConfigViolation {
            key: key.to_owned(),
            message,
        }],
    })?;
    let yaml = serde_yaml::to_string(&document).map_err(|source| SetError::Serialise { source })?;
    let bytes = yaml.into_bytes();
    write_atomically(config_file, &bytes, Some(CONFIG_FILE_MODE)).map_err(|source| {
        SetError::Io {
            path: config_file.to_owned(),
            source,
        }
    })?;
    Ok(bytes)
}

/// Reads the config file into a JSON document, treating an absent or empty file
/// as an empty object.
fn read_document(config_file: &Path) -> Result<Value, SetError> {
    match std::fs::read_to_string(config_file) {
        Ok(content) => {
            let parsed: Value =
                serde_yaml::from_str(&content).map_err(|source| SetError::Unparseable {
                    path: config_file.to_owned(),
                    source,
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

fn parse_document(config_file: &Path, yaml: &[u8]) -> Result<Value, SetError> {
    let parsed: Value = serde_yaml::from_slice(yaml).map_err(|source| SetError::Unparseable {
        path: config_file.to_owned(),
        source,
    })?;
    Ok(if parsed.is_null() {
        empty_object()
    } else {
        parsed
    })
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

// ---------------------------------------------------------------------------
// CLI shadow
// ---------------------------------------------------------------------------

/// The command-line overrides a running binary launched with, reduced to the
/// config paths they set.
///
/// The CLI layer is the cascade's highest, so a file write to one of these
/// paths is persisted but shadowed — the process keeps using the flag's value
/// until it restarts without the flag. Built once at startup from the resolved
/// [`CliOverrides`]; the default is empty — the honest answer when no flag was
/// passed.
#[derive(Debug, Clone, Default)]
pub struct CliShadow {
    paths: BTreeSet<String>,
}

impl CliShadow {
    /// Collects the dotted paths the overrides set, walking their serialised
    /// form so the set tracks the type with no hand-maintained mapping. The
    /// synthesised credential skeleton, which no flag sets, does not appear in a
    /// serve-time value and so needs no exclusion.
    #[must_use]
    pub fn from_overrides(overrides: &CliOverrides) -> Self {
        let tree = serde_json::to_value(overrides).unwrap_or(Value::Null);
        let mut paths = BTreeSet::new();
        collect_leaf_paths(&tree, &mut String::new(), &mut paths);
        Self { paths }
    }

    /// Whether a command-line flag set `key`.
    fn shadows(&self, key: &str) -> bool {
        self.paths.contains(key)
    }
}

/// Records every scalar leaf's dotted path under `prefix` into `out`; nulls and
/// empty containers contribute nothing, so the set is exactly the keys the
/// overrides actually carry a value for.
fn collect_leaf_paths(value: &Value, prefix: &mut String, out: &mut BTreeSet<String>) {
    match value {
        Value::Object(members) => {
            for (segment, child) in members {
                let base = prefix.len();
                if !prefix.is_empty() {
                    prefix.push('.');
                }
                prefix.push_str(segment);
                collect_leaf_paths(child, prefix, out);
                prefix.truncate(base);
            }
        }
        Value::Null => {}
        _ => {
            out.insert(prefix.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Write effect
// ---------------------------------------------------------------------------

/// Classifies how a persisted write to `key` takes effect for the running
/// binary: shadowed by a higher cascade layer, live, or awaiting a restart.
fn write_effect(key: &str, cli: &CliShadow) -> WriteEffect {
    if let Some(source) = shadowed_by(key, cli) {
        return WriteEffect::Shadowed { by: source };
    }
    effect_for_class(reload_class(key))
}

/// The write effect an unshadowed key of `class` reports. The liveness-honesty
/// contract (AC11): only [`ReloadClass::Hot`] reports [`WriteEffect::Live`] — a
/// `RequiresRestart` key, and defensively an `Unclassified` one, never claims a
/// live write, so a value the running binary has not adopted is never reported
/// as adopted.
fn effect_for_class(class: ReloadClass) -> WriteEffect {
    match class {
        ReloadClass::Hot => WriteEffect::Live,
        ReloadClass::GenesisOnly | ReloadClass::RequiresRestart | ReloadClass::Unclassified => {
            WriteEffect::NeedsRestart
        }
    }
}

/// The higher cascade layer that shadows a file write to `key`, named, or
/// `None` when the file is the effective source.
///
/// The layers are named in the loader's merge order, highest first: a
/// command-line flag outranks both environment layers, and the alias layer sits
/// above the nested one. A present variable shadows regardless of value — the
/// loader merges it over the file either way. `config.schema` reads this to mark
/// a currently-shadowed key at call time.
#[must_use]
pub fn shadowed_by(key: &str, cli: &CliShadow) -> Option<String> {
    if cli.shadows(key) {
        return Some("a command-line flag".to_owned());
    }
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
    use serde_json::json;
    use tribal_domain::ProviderConnectionName;

    use super::*;
    use crate::DatabaseCliOverrides;

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
    fn test_get_all_redacts_database_and_provider_secrets() {
        let mut config = base_config();
        config.provider_connections.insert(
            ProviderConnectionName::parse("openai_default").unwrap(),
            crate::ProviderConnectionConfig::OpenAi {
                base_url: "https://api.openai.com".to_owned(),
                api_key: Some("sk-provider-secret".parse().unwrap()),
            },
        );

        let document = get_all(&config).to_string();
        assert!(
            !document.contains("pass@localhost"),
            "db url leaked: {document}"
        );
        assert!(
            !document.contains("sk-provider-secret"),
            "provider key leaked: {document}"
        );
    }

    #[test]
    fn test_set_persists_and_reads_back_through_the_loader() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let effect = set(
                &base_config(),
                &path,
                "logging.level",
                json!("debug"),
                &CliShadow::default(),
            )
            .unwrap()
            .effect;
            assert_eq!(effect, WriteEffect::Live);

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
            set(
                &base_config(),
                &path,
                "database.url",
                json!(secret),
                &CliShadow::default(),
            )
            .unwrap();

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
            let effect = set(
                &base_config(),
                &path,
                "logging.level",
                json!("debug"),
                &CliShadow::default(),
            )
            .unwrap()
            .effect;
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
    fn test_set_database_url_shadowed_by_its_alias_env_layer() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TRIBAL_DATABASE_URL", "postgres://env-holder/tribal");
            let path = jail.directory().join("tribal.yaml");
            let effect = set(
                &base_config(),
                &path,
                "database.url",
                json!("postgres://file-target/tribal"),
                &CliShadow::default(),
            )
            .unwrap()
            .effect;
            assert_eq!(
                effect,
                WriteEffect::Shadowed {
                    by: "TRIBAL_DATABASE_URL".to_owned()
                }
            );

            // The write still persists — the shadow is a higher layer, not a refusal.
            let document = std::fs::read_to_string(&path).unwrap();
            assert!(
                document.contains("file-target"),
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
            let effect = set(
                &base_config(),
                &path,
                "discovery.max_limit",
                json!(42),
                &CliShadow::default(),
            )
            .unwrap()
            .effect;
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
    fn test_set_shadowed_by_a_command_line_flag() {
        figment::Jail::expect_with(|jail| {
            // The binary launched with --database-url, so the CLI layer outranks
            // the file: the write persists but never takes effect until a restart
            // drops the flag.
            let overrides = CliOverrides {
                database: Some(DatabaseCliOverrides {
                    url: Some("postgres://cli:pass@host:5432/db".to_owned()),
                }),
                ..CliOverrides::default()
            };
            let cli = CliShadow::from_overrides(&overrides);
            let path = jail.directory().join("tribal.yaml");
            let effect = set(
                &base_config(),
                &path,
                "database.url",
                json!("postgres://file:pass@host:5432/db"),
                &cli,
            )
            .unwrap()
            .effect;
            assert_eq!(
                effect,
                WriteEffect::Shadowed {
                    by: "a command-line flag".to_owned()
                },
            );

            // A key the flag did not set is unshadowed by the CLI layer: the
            // hot key reports live, not shadowed.
            let unshadowed = set(&base_config(), &path, "logging.level", json!("debug"), &cli)
                .unwrap()
                .effect;
            assert_eq!(unshadowed, WriteEffect::Live);
            Ok(())
        });
    }

    /// AC11 liveness honesty: only a `Hot` key reports a live write. A
    /// `RequiresRestart` key — and defensively an `Unclassified` one — reports
    /// `NeedsRestart`, never `Live`, so the surface never claims the running
    /// binary adopted a value it has not.
    #[test]
    fn test_only_a_hot_key_reports_a_live_write() {
        assert_eq!(effect_for_class(ReloadClass::Hot), WriteEffect::Live);
        assert_eq!(
            effect_for_class(ReloadClass::RequiresRestart),
            WriteEffect::NeedsRestart,
        );
        assert_eq!(
            effect_for_class(ReloadClass::GenesisOnly),
            WriteEffect::NeedsRestart,
        );
        assert_eq!(
            effect_for_class(ReloadClass::Unclassified),
            WriteEffect::NeedsRestart,
        );
    }

    #[test]
    fn test_set_equal_to_persisted_value_writes_nothing() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            set(
                &base_config(),
                &path,
                "logging.level",
                json!("debug"),
                &CliShadow::default(),
            )
            .unwrap();
            let before = std::fs::metadata(&path).unwrap().modified().unwrap();

            let persisted = set(
                &base_config(),
                &path,
                "logging.level",
                json!("debug"),
                &CliShadow::default(),
            )
            .unwrap();

            assert_eq!(persisted.effect, WriteEffect::Unchanged);
            assert!(
                persisted.document.is_none(),
                "an unchanged write records no owned bytes",
            );
            assert_eq!(
                std::fs::metadata(&path).unwrap().modified().unwrap(),
                before,
                "the file is not touched",
            );
            Ok(())
        });
    }

    #[test]
    fn test_set_allows_dynamic_provider_connection_paths_that_validate() {
        figment::Jail::expect_with(|jail| {
            let mut config = base_config();
            config.provider_connections.insert(
                ProviderConnectionName::parse("openai_default").unwrap(),
                crate::ProviderConnectionConfig::OpenAi {
                    base_url: "https://api.openai.com".to_owned(),
                    api_key: None,
                },
            );
            let path = jail.directory().join("tribal.yaml");

            let persisted = set(
                &config,
                &path,
                "provider_connections.openai_default.api_key",
                json!("sk-dynamic-secret"),
                &CliShadow::default(),
            )
            .unwrap();

            assert_eq!(persisted.effect, WriteEffect::NeedsRestart);
            let reloaded = crate::load_config(path.to_str().unwrap(), None, None).unwrap();
            let entry = reloaded
                .provider_connections
                .get("openai_default")
                .expect("the provider connection resolves");
            assert_eq!(entry.api_key().unwrap().as_str(), "sk-dynamic-secret");
            Ok(())
        });
    }

    #[test]
    fn test_set_rejects_dynamic_provider_paths_that_violate_connection_rules() {
        figment::Jail::expect_with(|jail| {
            let mut config = base_config();
            config.provider_connections.insert(
                ProviderConnectionName::parse("openai_default").unwrap(),
                crate::ProviderConnectionConfig::OpenAi {
                    base_url: "https://api.openai.com".to_owned(),
                    api_key: None,
                },
            );
            let path = jail.directory().join("tribal.yaml");

            let error = set(
                &config,
                &path,
                "provider_connections.openai_default.base_url",
                json!("not a url"),
                &CliShadow::default(),
            )
            .unwrap_err();

            assert!(matches!(error, SetError::Rejected { .. }));
            assert!(
                !path.exists(),
                "a connection-invalid write never touches the file",
            );
            Ok(())
        });
    }

    #[test]
    fn test_set_refuses_an_unparseable_log_filter_directive_whole() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let error = set(
                &base_config(),
                &path,
                "logging.level",
                json!("not valid [["),
                &CliShadow::default(),
            )
            .unwrap_err();
            assert!(matches!(error, SetError::Rejected { .. }));
            assert!(!path.exists(), "a refused directive never touches the file");
            Ok(())
        });
    }

    #[test]
    fn test_set_returns_the_resolved_config_with_the_write_applied() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let persisted = set(
                &base_config(),
                &path,
                "logging.level",
                json!("debug"),
                &CliShadow::default(),
            )
            .unwrap();
            assert_eq!(persisted.config.logging.level, "debug");
            assert_eq!(
                persisted.config.database.url,
                base_config().database.url,
                "every other key keeps its resolved value",
            );
            Ok(())
        });
    }

    #[test]
    fn test_set_refuses_an_invalid_value_whole() {
        figment::Jail::expect_with(|jail| {
            let path = jail.directory().join("tribal.yaml");
            let error = set(
                &base_config(),
                &path,
                "server.transport",
                json!("grpc"),
                &CliShadow::default(),
            )
            .unwrap_err();
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
            set(
                &base_config(),
                &path,
                "logging.level",
                json!("trace"),
                &CliShadow::default(),
            )
            .unwrap();

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
