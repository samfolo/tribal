//! Stable filesystem observation and revisioned configuration operations.

use std::{
    cell::Cell,
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{self, Read as _},
    os::unix::fs::{MetadataExt as _, OpenOptionsExt as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use tribal_config::{CliShadow, ReloadClass, SetError, TribalConfig, WriteEffect, reload_class};
use tribal_wire::management::{
    ConfigDigest, ConfigDocument, ConfigFieldOutcome, ConfigFilePath, ConfigGetRequest,
    ConfigLiteral, ConfigPatchOutcome, ConfigPatchRefusal, ConfigPatchRequest,
    ConfigPersistenceObservation, ConfigPersistencePhase, ConfigRevision, ConfigSetRequest,
    ConfigValue, ConfigWriteEffect, ConfigWriteOutcome, CredentialSourceKind, InferenceStage,
    ManagementError, ManagementResponseError,
};

/// Maximum time spent proving a stable filesystem winner under raw-file races.
const STABLE_WINNER_RETRY_WINDOW: Duration = Duration::from_secs(5);

/// Exact bytes and revision established by a stable filesystem identity.
#[derive(Debug)]
struct ProvenConfigBytes {
    bytes: Vec<u8>,
    digest: ConfigDigest,
    revision: ConfigRevision,
}

impl ProvenConfigBytes {
    fn is_consistent(&self) -> bool {
        self.digest == ConfigDigest::from_bytes(&self.bytes)
            && self.revision == ConfigRevision::from_digest(&self.digest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
        }
    }
}

/// Synchronous configuration authority shared by manager and one-shot adapters.
#[derive(Debug)]
pub(crate) struct ConfigAuthority {
    path: PathBuf,
    durability_uncertain: Cell<bool>,
}

/// Revision-proven valid configuration used by external probes.
pub(crate) struct ConfigProbeSnapshot {
    pub(crate) path: PathBuf,
    pub(crate) config: TribalConfig,
    pub(crate) revision: ConfigRevision,
}

/// Effective configuration and revision proven from the same durable bytes.
pub(crate) struct ResolvedConfigSnapshot {
    pub(crate) config: Arc<TribalConfig>,
    pub(crate) revision: ConfigRevision,
}

impl std::fmt::Debug for ResolvedConfigSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedConfigSnapshot")
            .field("config", &"<redacted>")
            .field("revision", &self.revision)
            .finish()
    }
}

impl std::fmt::Debug for ConfigProbeSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigProbeSnapshot")
            .field("path", &self.path)
            .field("config", &"<redacted>")
            .field("revision", &self.revision)
            .finish()
    }
}

/// Revision-proven bytes used by readiness checks, including invalid YAML.
pub(crate) struct ConfigCheckSnapshot {
    pub(crate) path: PathBuf,
    pub(crate) bytes: zeroize::Zeroizing<Vec<u8>>,
    pub(crate) revision: ConfigRevision,
}

impl std::fmt::Debug for ConfigCheckSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigCheckSnapshot")
            .field("path", &self.path)
            .field("bytes", &"<redacted>")
            .field("revision", &self.revision)
            .finish()
    }
}

/// Secret-bearing credential origin retained only inside the manager process.
pub(crate) struct CredentialMaterial {
    pub(crate) kind: CredentialSourceKind,
    pub(crate) provider: tribal_domain::ProviderKind,
    pub(crate) base_url: Option<String>,
    pub(crate) secret: zeroize::Zeroizing<String>,
}

impl std::fmt::Debug for CredentialMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialMaterial")
            .field("kind", &self.kind)
            .field("provider", &self.provider)
            .field("base_url", &self.base_url)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Failure reading, validating, or atomically mutating configuration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigAuthorityError {
    #[error("configuration I/O failed at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("configuration winner did not stabilise within the retry window")]
    StableWinnerUnavailable,
    #[error("configuration is invalid: {message}")]
    Invalid { message: String },
    #[error("configuration revision conflict")]
    Conflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    #[error("configuration patch was refused: {reason:?}")]
    PatchRefused { reason: ConfigPatchRefusal },
    #[error("configuration write failed: {source}")]
    Write {
        #[source]
        source: SetError,
    },
    #[error("configuration replacement may have committed before durability failed: {source}")]
    DurabilityUncertain {
        observed_digest: ConfigDigest,
        #[source]
        source: SetError,
    },
    #[error("configuration key is unknown: {key}")]
    UnknownKey { key: String },
    #[error("configuration worker is unavailable")]
    WorkerUnavailable,
}

impl ConfigAuthority {
    /// Binds operations to one already-materialised canonical config path.
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            durability_uncertain: Cell::new(false),
        }
    }

    pub(crate) fn path(&self) -> ConfigFilePath {
        ConfigFilePath {
            path: self.path.to_string_lossy().into_owned(),
        }
    }

    /// Validates one field candidate without persistence.
    pub(crate) fn validate_value(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<Vec<tribal_config::ConfigViolation>, ConfigAuthorityError> {
        let proven = self.stable_winner()?;
        let config = Self::load_valid(&proven)?;
        Ok(tribal_config::validate_write(&config, key, value))
    }

    /// Reads configured credential origins without projecting their values to the wire.
    pub(crate) fn credential_materials(
        &self,
    ) -> Result<Vec<CredentialMaterial>, ConfigAuthorityError> {
        let proven = self.stable_winner()?;
        let config = Self::load_valid(&proven)?;
        let value =
            serde_json::to_value(&config).map_err(|source| ConfigAuthorityError::Invalid {
                message: source.to_string(),
            })?;
        let mut materials = Vec::new();
        for (name, stage) in [
            ("extraction", InferenceStage::Extraction),
            ("triage", InferenceStage::Triage),
            ("relation", InferenceStage::Relation),
        ] {
            let Some(entry) = value
                .get("inference")
                .and_then(|inference| inference.get(name))
            else {
                continue;
            };
            let Some(secret) = entry.get("api_key").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(provider) = entry
                .get("provider")
                .cloned()
                .and_then(|provider| serde_json::from_value(provider).ok())
            else {
                continue;
            };
            materials.push(CredentialMaterial {
                kind: CredentialSourceKind::InferenceStage { stage },
                provider,
                base_url: entry
                    .get("base_url")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| provider.default_base_url().map(str::to_owned)),
                secret: zeroize::Zeroizing::new(secret.to_owned()),
            });
        }
        if let Some(entries) = value
            .get("credentials")
            .and_then(serde_json::Value::as_object)
        {
            for (name, entry) in entries {
                let Some(secret) = entry.get("api_key").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let Some(provider) = entry
                    .get("provider_kind")
                    .cloned()
                    .and_then(|provider| serde_json::from_value(provider).ok())
                else {
                    continue;
                };
                materials.push(CredentialMaterial {
                    kind: CredentialSourceKind::EmbeddingConnection { name: name.clone() },
                    provider,
                    base_url: entry
                        .get("base_url")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    secret: zeroize::Zeroizing::new(secret.to_owned()),
                });
            }
        }
        Ok(materials)
    }

    /// Returns a parsed configuration and the revision proven from the same bytes.
    pub(crate) fn probe_snapshot(&self) -> Result<ConfigProbeSnapshot, ConfigAuthorityError> {
        let snapshot = self.resolved_snapshot()?;
        Ok(ConfigProbeSnapshot {
            path: self.path.clone(),
            config: Arc::unwrap_or_clone(snapshot.config),
            revision: snapshot.revision,
        })
    }

    /// Returns the effective configuration and revision from one stable read.
    pub(crate) fn resolved_snapshot(&self) -> Result<ResolvedConfigSnapshot, ConfigAuthorityError> {
        self.reconcile_if_needed()?;
        let proven = self.stable_winner()?;
        let config = Self::load_valid(&proven)?;
        Ok(ResolvedConfigSnapshot {
            config: Arc::new(config),
            revision: proven.revision,
        })
    }

    /// Returns exact stable bytes for local readiness without reopening the path.
    pub(crate) fn check_snapshot(&self) -> Result<ConfigCheckSnapshot, ConfigAuthorityError> {
        self.reconcile_if_needed()?;
        let proven = self.stable_winner()?;
        Ok(ConfigCheckSnapshot {
            path: self.path.clone(),
            bytes: zeroize::Zeroizing::new(proven.bytes),
            revision: proven.revision,
        })
    }

    /// Current durable document, preserving invalid and unreadable states.
    pub(crate) fn document(&self) -> Result<ConfigDocument, ConfigAuthorityError> {
        let proven = self.stable_winner()?;
        let uncertain = self.durability_uncertain.get();
        match Self::load_valid(&proven) {
            Ok(config) if uncertain => Ok(ConfigDocument::UncertainValid {
                values: ConfigLiteral::new(tribal_config::get_all(&config)),
                observed_digest: proven.digest,
                phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
            }),
            Ok(config) => Ok(ConfigDocument::DurableValid {
                values: ConfigLiteral::new(tribal_config::get_all(&config)),
                revision: proven.revision,
            }),
            Err(_) if uncertain => Ok(ConfigDocument::UncertainInvalid {
                observed_digest: proven.digest,
                phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
            }),
            Err(_) => Ok(ConfigDocument::DurableInvalid {
                revision: proven.revision,
            }),
        }
    }

    /// Current durable value at one validated schema path.
    pub(crate) fn get(
        &self,
        request: ConfigGetRequest,
    ) -> Result<ConfigValue, ConfigAuthorityError> {
        let proven = self.stable_winner()?;
        let config = match Self::load_valid(&proven) {
            Ok(config) => config,
            Err(ConfigAuthorityError::Invalid { .. }) if self.durability_uncertain.get() => {
                return Ok(ConfigValue::UncertainInvalid {
                    key: request.key,
                    observed_digest: proven.digest,
                    phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
                });
            }
            Err(ConfigAuthorityError::Invalid { .. }) => {
                return Ok(ConfigValue::DurableInvalid {
                    key: request.key,
                    revision: proven.revision,
                });
            }
            Err(error) => return Err(error),
        };
        let value = tribal_config::get(&config, request.key.as_str())
            .map_err(|error| ConfigAuthorityError::UnknownKey { key: error.key })?;
        if self.durability_uncertain.get() {
            Ok(ConfigValue::UncertainValid {
                key: request.key,
                value: ConfigLiteral::new(value),
                observed_digest: proven.digest,
                phase: tribal_wire::management::ConfigPersistencePhase::DurabilityUncertain,
            })
        } else {
            Ok(ConfigValue::DurableValid {
                key: request.key,
                value: ConfigLiteral::new(value),
                revision: proven.revision,
            })
        }
    }

    /// Applies one revision-checked field write atomically.
    pub(crate) fn set(
        &self,
        request: ConfigSetRequest,
    ) -> Result<ConfigWriteOutcome, ConfigAuthorityError> {
        let ConfigSetRequest {
            key,
            value,
            expected_revision,
        } = request;
        self.reconcile_if_needed()?;
        let current = self.stable_winner()?;
        check_revision(&expected_revision, &current.revision)?;
        let config = match Self::load_valid(&current) {
            Ok(config) => config,
            Err(ConfigAuthorityError::Invalid { .. }) => {
                let repaired = tribal_config::repair_patch(
                    &self.path,
                    &[(key.as_str().to_owned(), value.expose_sensitive().clone())],
                    &CliShadow::default(),
                )
                .map_err(|source| self.write_error(&current, source))?;
                let revision = repaired
                    .document
                    .as_deref()
                    .map_or(current.revision, revision_for);
                let effect = repaired
                    .effects
                    .into_iter()
                    .next()
                    .map_or(ConfigWriteEffect::Unchanged, |effect| {
                        managed_effect(effect, false)
                    });
                return Ok(ConfigWriteOutcome { effect, revision });
            }
            Err(error) => return Err(error),
        };
        let persisted = tribal_config::set_from_yaml(
            &config,
            &self.path,
            &current.bytes,
            key.as_str(),
            value.expose_sensitive().clone(),
            &CliShadow::default(),
        )
        .map_err(|source| self.write_error(&current, source))?;
        let revision = persisted
            .document
            .as_deref()
            .map_or(current.revision, revision_for);
        Ok(ConfigWriteOutcome {
            effect: managed_effect(persisted.effect, false),
            revision,
        })
    }

    /// Applies a structurally valid revision-checked patch as one file replacement.
    pub(crate) fn patch(
        &self,
        request: ConfigPatchRequest,
    ) -> Result<ConfigPatchOutcome, ConfigAuthorityError> {
        validate_patch_shape(&request)?;
        self.reconcile_if_needed()?;
        let current = self.stable_winner()?;
        check_revision(&request.expected_revision, &current.revision)?;
        let mut changes = request.changes;
        changes.sort_by(|left, right| left.key.as_str().cmp(right.key.as_str()));
        let native = changes
            .iter()
            .map(|change| {
                (
                    change.key.as_str().to_owned(),
                    change.value.expose_sensitive().clone(),
                )
            })
            .collect::<Vec<_>>();
        let persisted = match Self::load_valid(&current) {
            Ok(config) => tribal_config::patch_from_yaml(
                &config,
                &self.path,
                &current.bytes,
                &native,
                &CliShadow::default(),
            ),
            Err(ConfigAuthorityError::Invalid { .. }) => {
                tribal_config::repair_patch(&self.path, &native, &CliShadow::default())
            }
            Err(error) => return Err(error),
        }
        .map_err(|source| self.write_error(&current, source))?;
        let revision = persisted
            .document
            .as_deref()
            .map_or(current.revision, revision_for);
        let fields = changes
            .into_iter()
            .zip(persisted.effects)
            .map(|(change, effect)| ConfigFieldOutcome {
                key: change.key,
                effect: managed_effect(effect, false),
            })
            .collect();
        Ok(ConfigPatchOutcome { fields, revision })
    }

    fn write_error(&self, before: &ProvenConfigBytes, source: SetError) -> ConfigAuthorityError {
        match self.stable_winner() {
            Ok(after) if after.revision != before.revision => {
                self.durability_uncertain.set(true);
                ConfigAuthorityError::DurabilityUncertain {
                    observed_digest: after.digest,
                    source,
                }
            }
            _ => ConfigAuthorityError::Write { source },
        }
    }

    fn reconcile_if_needed(&self) -> Result<(), ConfigAuthorityError> {
        if !self.durability_uncertain.get() {
            return Ok(());
        }
        let _ = self.stable_winner()?;
        self.durability_uncertain.set(false);
        Ok(())
    }

    fn stable_winner(&self) -> Result<ProvenConfigBytes, ConfigAuthorityError> {
        let deadline = Instant::now() + STABLE_WINNER_RETRY_WINDOW;
        loop {
            match read_stable_once(&self.path) {
                Ok(Some(proven)) => return Ok(proven),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(None) => return Err(ConfigAuthorityError::StableWinnerUnavailable),
                Err(source) => {
                    return Err(ConfigAuthorityError::Io {
                        path: self.path.clone(),
                        source,
                    });
                }
            }
        }
    }

    fn load_valid(proven: &ProvenConfigBytes) -> Result<TribalConfig, ConfigAuthorityError> {
        let yaml =
            std::str::from_utf8(&proven.bytes).map_err(|error| ConfigAuthorityError::Invalid {
                message: error.to_string(),
            })?;
        let config = tribal_config::load_config_from_yaml(yaml, None, None).map_err(|error| {
            ConfigAuthorityError::Invalid {
                message: error.to_string(),
            }
        })?;
        tribal_config::validate(&config).map_err(|error| ConfigAuthorityError::Invalid {
            message: error.to_string(),
        })?;
        Ok(config)
    }
}

fn read_stable_once(path: &Path) -> Result<Option<ProvenConfigBytes>, io::Error> {
    let parent_path = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = File::open(parent_path)?;
    let mut first = open_no_follow(path)?;
    let first_before = FileIdentity::from_metadata(&first.metadata()?);
    let mut first_bytes = Vec::new();
    first.read_to_end(&mut first_bytes)?;
    let first_after = FileIdentity::from_metadata(&first.metadata()?);
    if first_before != first_after || first_after.length != first_bytes.len() as u64 {
        return Ok(None);
    }
    let first_digest = ConfigDigest::from_bytes(&first_bytes);
    first.sync_all()?;
    parent.sync_all()?;

    let mut second = open_no_follow(path)?;
    let second_before = FileIdentity::from_metadata(&second.metadata()?);
    let mut second_bytes = Vec::new();
    second.read_to_end(&mut second_bytes)?;
    let second_after = FileIdentity::from_metadata(&second.metadata()?);
    let second_digest = ConfigDigest::from_bytes(&second_bytes);
    if first_after != second_before
        || second_before != second_after
        || second_after.length != second_bytes.len() as u64
        || first_digest != second_digest
        || first_bytes != second_bytes
    {
        return Ok(None);
    }

    let digest = first_digest;
    let revision = ConfigRevision::from_digest(&digest);
    let proven = ProvenConfigBytes {
        bytes: first_bytes,
        digest,
        revision,
    };
    if proven.is_consistent() {
        Ok(Some(proven))
    } else {
        Ok(None)
    }
}

fn open_no_follow(path: &Path) -> Result<File, io::Error> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

fn revision_for(bytes: &[u8]) -> ConfigRevision {
    ConfigRevision::from_digest(&ConfigDigest::from_bytes(bytes))
}

fn check_revision(
    expected: &ConfigRevision,
    actual: &ConfigRevision,
) -> Result<(), ConfigAuthorityError> {
    if expected == actual {
        return Ok(());
    }
    Err(ConfigAuthorityError::Conflict {
        expected: expected.clone(),
        actual: actual.clone(),
    })
}

fn validate_patch_shape(request: &ConfigPatchRequest) -> Result<(), ConfigAuthorityError> {
    if request.changes.is_empty() {
        return Err(ConfigAuthorityError::PatchRefused {
            reason: ConfigPatchRefusal::Empty,
        });
    }
    let mut seen = BTreeSet::new();
    for change in &request.changes {
        if !seen.insert(change.key.as_str()) {
            return Err(ConfigAuthorityError::PatchRefused {
                reason: ConfigPatchRefusal::Duplicate {
                    key: change.key.clone(),
                },
            });
        }
    }
    for (index, left) in request.changes.iter().enumerate() {
        for right in &request.changes[index + 1..] {
            if let Some((parent, child)) = overlapping(&left.key, &right.key) {
                return Err(ConfigAuthorityError::PatchRefused {
                    reason: ConfigPatchRefusal::Overlapping { parent, child },
                });
            }
        }
    }
    let hot = request
        .changes
        .iter()
        .filter(|change| reload_class(change.key.as_str()) == ReloadClass::Hot)
        .count();
    if hot > 1 {
        return Err(ConfigAuthorityError::PatchRefused {
            reason: ConfigPatchRefusal::MultipleHotFields,
        });
    }
    if hot == 1 && request.changes.len() > 1 {
        return Err(ConfigAuthorityError::PatchRefused {
            reason: ConfigPatchRefusal::MixedHotAndNonHot,
        });
    }
    Ok(())
}

fn overlapping(
    left: &tribal_domain::ConfigFieldPath,
    right: &tribal_domain::ConfigFieldPath,
) -> Option<(
    tribal_domain::ConfigFieldPath,
    tribal_domain::ConfigFieldPath,
)> {
    let left_prefix = format!("{}.", left.as_str());
    if right.as_str().starts_with(&left_prefix) {
        return Some((left.clone(), right.clone()));
    }
    let right_prefix = format!("{}.", right.as_str());
    left.as_str()
        .starts_with(&right_prefix)
        .then(|| (right.clone(), left.clone()))
}

fn managed_effect(effect: WriteEffect, runtime_attached: bool) -> ConfigWriteEffect {
    match effect {
        WriteEffect::Unchanged => ConfigWriteEffect::Unchanged,
        WriteEffect::Live if runtime_attached => ConfigWriteEffect::AppliedLive,
        WriteEffect::Live | WriteEffect::NeedsRestart => ConfigWriteEffect::OnNextStart,
        WriteEffect::Shadowed { by } => ConfigWriteEffect::Shadowed { by },
    }
}

pub(super) fn management_error(error: ConfigAuthorityError) -> ManagementResponseError {
    let message = error.to_string();
    let error = match error {
        ConfigAuthorityError::Conflict { expected, actual } => {
            ManagementError::ConfigConflict { expected, actual }
        }
        ConfigAuthorityError::PatchRefused { reason } => {
            ManagementError::ConfigPatchRefused { reason }
        }
        ConfigAuthorityError::Write { source } => {
            let fields = source
                .violations()
                .into_iter()
                .flatten()
                .filter_map(|violation| tribal_domain::ConfigFieldPath::parse(&violation.key).ok())
                .collect();
            if source.violations().is_some() {
                ManagementError::ConfigurationInvalid { fields }
            } else {
                ManagementError::ConfigPersistenceUnavailable {
                    phase: ConfigPersistencePhase::NotCommitted,
                    observation: ConfigPersistenceObservation::Unreadable,
                }
            }
        }
        ConfigAuthorityError::DurabilityUncertain {
            observed_digest, ..
        } => ManagementError::ConfigPersistenceUnavailable {
            phase: ConfigPersistencePhase::DurabilityUncertain,
            observation: ConfigPersistenceObservation::Observed {
                digest: observed_digest,
            },
        },
        ConfigAuthorityError::Io { .. } | ConfigAuthorityError::StableWinnerUnavailable => {
            ManagementError::ConfigPersistenceUnavailable {
                phase: ConfigPersistencePhase::DurabilityUncertain,
                observation: ConfigPersistenceObservation::Unreadable,
            }
        }
        ConfigAuthorityError::Invalid { .. }
        | ConfigAuthorityError::UnknownKey { .. }
        | ConfigAuthorityError::WorkerUnavailable => {
            ManagementError::ConfigurationInvalid { fields: Vec::new() }
        }
    };
    ManagementResponseError { message, error }
}

#[cfg(test)]
mod tests {
    use tribal_wire::management::{ConfigFieldPath, ConfigPatchChange};

    use super::*;

    fn valid_config(path: &Path) -> ConfigAuthority {
        let config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        let bytes = serde_yaml::to_string(&config).expect("valid config serialises");
        std::fs::write(path, bytes).expect("valid config writes");
        ConfigAuthority::new(path.to_owned())
    }

    fn path(raw: &str) -> ConfigFieldPath {
        ConfigFieldPath::parse(raw).expect("fixture field path is valid")
    }

    fn literal(value: serde_json::Value) -> ConfigLiteral {
        ConfigLiteral::new(value)
    }

    fn revision(authority: &ConfigAuthority) -> ConfigRevision {
        authority.stable_winner().expect("stable winner").revision
    }

    #[test]
    fn test_invalid_yaml_has_a_durable_revision_without_values() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        std::fs::write(&config, "database: [").expect("invalid config writes");
        let authority = ConfigAuthority::new(config);
        assert!(matches!(
            authority.document().expect("document observes"),
            ConfigDocument::DurableInvalid { .. }
        ));
    }

    #[test]
    fn test_revisioned_write_repairs_an_invalid_document_atomically() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        std::fs::write(&config, "database: [").expect("invalid config writes");
        let authority = ConfigAuthority::new(config);
        let outcome = authority
            .set(ConfigSetRequest {
                key: path("database.url"),
                value: literal(serde_json::json!(
                    "postgres://user:pass@localhost:5432/tribal"
                )),
                expected_revision: revision(&authority),
            })
            .expect("repair commits");
        assert_eq!(outcome.revision, revision(&authority));
        assert!(matches!(
            authority.document().expect("repaired document reads"),
            ConfigDocument::DurableValid { .. }
        ));
    }

    #[test]
    fn test_stale_set_is_refused_without_persistence() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        let authority = valid_config(&config);
        let before = std::fs::read(&config).expect("config reads");
        let stale = revision_for(b"stale");
        let error = authority
            .set(ConfigSetRequest {
                key: path("logging.level"),
                value: literal(serde_json::json!("debug")),
                expected_revision: stale,
            })
            .expect_err("stale write is refused");
        assert!(matches!(error, ConfigAuthorityError::Conflict { .. }));
        assert_eq!(std::fs::read(&config).expect("config rereads"), before);
    }

    #[test]
    fn test_patch_rejects_empty_duplicate_overlap_and_mixed_hot() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        let authority = valid_config(&config);
        let current = revision(&authority);
        let empty = ConfigPatchRequest {
            changes: Vec::new(),
            expected_revision: current.clone(),
        };
        assert!(matches!(
            authority.patch(empty),
            Err(ConfigAuthorityError::PatchRefused {
                reason: ConfigPatchRefusal::Empty
            })
        ));

        let duplicate = ConfigPatchRequest {
            changes: vec![
                ConfigPatchChange {
                    key: path("worker.poll_interval_ms"),
                    value: literal(serde_json::json!(1000)),
                },
                ConfigPatchChange {
                    key: path("worker.poll_interval_ms"),
                    value: literal(serde_json::json!(2000)),
                },
            ],
            expected_revision: current.clone(),
        };
        assert!(matches!(
            authority.patch(duplicate),
            Err(ConfigAuthorityError::PatchRefused {
                reason: ConfigPatchRefusal::Duplicate { .. }
            })
        ));

        let overlap = ConfigPatchRequest {
            changes: vec![
                ConfigPatchChange {
                    key: path("server.sse"),
                    value: literal(serde_json::json!({})),
                },
                ConfigPatchChange {
                    key: path("server.sse.idle_timeout_ms"),
                    value: literal(serde_json::json!(1000)),
                },
            ],
            expected_revision: current.clone(),
        };
        assert!(matches!(
            authority.patch(overlap),
            Err(ConfigAuthorityError::PatchRefused {
                reason: ConfigPatchRefusal::Overlapping { .. }
            })
        ));

        let mixed = ConfigPatchRequest {
            changes: vec![
                ConfigPatchChange {
                    key: path("logging.level"),
                    value: literal(serde_json::json!("debug")),
                },
                ConfigPatchChange {
                    key: path("worker.poll_interval_ms"),
                    value: literal(serde_json::json!(1000)),
                },
            ],
            expected_revision: current,
        };
        assert!(matches!(
            authority.patch(mixed),
            Err(ConfigAuthorityError::PatchRefused {
                reason: ConfigPatchRefusal::MixedHotAndNonHot
            })
        ));
    }

    #[test]
    fn test_atomic_patch_returns_lexicographic_fields_and_one_revision() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        let authority = valid_config(&config);
        let outcome = authority
            .patch(ConfigPatchRequest {
                changes: vec![
                    ConfigPatchChange {
                        key: path("worker.task_timeout_ms"),
                        value: literal(serde_json::json!(360_000)),
                    },
                    ConfigPatchChange {
                        key: path("worker.poll_interval_ms"),
                        value: literal(serde_json::json!(1_234)),
                    },
                ],
                expected_revision: revision(&authority),
            })
            .expect("patch commits");
        assert_eq!(
            outcome
                .fields
                .iter()
                .map(|field| field.key.as_str())
                .collect::<Vec<_>>(),
            ["worker.poll_interval_ms", "worker.task_timeout_ms"],
        );
        assert!(
            outcome
                .fields
                .iter()
                .all(|field| field.effect == ConfigWriteEffect::OnNextStart)
        );
        assert_eq!(outcome.revision, revision(&authority));
    }

    #[test]
    fn test_stable_winner_revision_hashes_exact_observed_bytes() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        let bytes = b"database: []";
        std::fs::write(&config, bytes).expect("config writes");
        let proven = ConfigAuthority::new(config)
            .stable_winner()
            .expect("stable winner");
        assert_eq!(proven.bytes, bytes);
        assert_eq!(proven.digest, ConfigDigest::from_bytes(bytes));
        assert_eq!(proven.revision, revision_for(bytes));
    }

    #[test]
    fn test_post_replacement_failure_blocks_durable_projection_until_reconciled() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let config = temp.path().join("tribal.yaml");
        let authority = valid_config(&config);
        let before = authority.stable_winner().expect("initial winner");
        let mut updated = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        updated.logging.level = "debug".to_owned();
        std::fs::write(
            &config,
            serde_yaml::to_string(&updated).expect("updated config serialises"),
        )
        .expect("replacement writes");
        let error = authority.write_error(
            &before,
            SetError::Io {
                path: config.clone(),
                source: io::Error::other("directory sync failed"),
            },
        );
        assert!(matches!(
            error,
            ConfigAuthorityError::DurabilityUncertain { .. }
        ));
        assert!(matches!(
            authority.document().expect("uncertain document observes"),
            ConfigDocument::UncertainValid { .. }
        ));

        authority
            .reconcile_if_needed()
            .expect("stable winner reconciles");
        assert!(matches!(
            authority.document().expect("durable document observes"),
            ConfigDocument::DurableValid { .. }
        ));
    }
}
