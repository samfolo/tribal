//! Namespace-bound credential durability and recovery.
#![allow(
    dead_code,
    reason = "token administration consumes this durability seam in the next landing"
)]

use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tribal_db::LocalDefaultCredential;
use tribal_domain::{AuthTokenId, BearerToken, CredentialGenerationId};

use crate::management::authority::{ConfigAuthorityNamespace, credential_paths};

const OWNER_FILE_MODE: u32 = 0o600;
const OWNER_DIRECTORY_MODE: u32 = 0o700;

/// Secret-bearing authentication material retained inside the manager.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub(super) enum Auth {
    Bearer { token: BearerToken },
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer { .. } => formatter
                .debug_struct("Bearer")
                .field("token", &"<redacted>")
                .finish(),
        }
    }
}

/// File envelope joined to the database mapping by three durable identities.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedCredentialEnvelope {
    pub(super) namespace: ConfigAuthorityNamespace,
    pub(super) generation_id: CredentialGenerationId,
    pub(super) token_id: AuthTokenId,
    pub(super) auth: Auth,
}

impl std::fmt::Debug for PersistedCredentialEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PersistedCredentialEnvelope")
            .field("namespace", &self.namespace)
            .field("generation_id", &self.generation_id)
            .field("token_id", &self.token_id)
            .field("auth", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryDisposition {
    Empty,
    Stable,
    PromotedPending,
    ReplaceMapped { token_id: AuthTokenId },
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CredentialStoreError {
    #[error("credential envelope I/O failed at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("credential envelope encoding failed: {source}")]
    Encoding {
        #[source]
        source: serde_json::Error,
    },
    #[error("credential envelope belongs to another authority namespace")]
    NamespaceMismatch,
}

/// Owner-only pending/stable envelope store for one configuration authority.
pub(super) struct CredentialStore {
    namespace: ConfigAuthorityNamespace,
    stable_path: PathBuf,
    pending_path: PathBuf,
}

impl CredentialStore {
    pub(super) fn new(namespace: ConfigAuthorityNamespace) -> Self {
        let (stable_path, pending_path) = credential_paths(&namespace);
        Self {
            namespace,
            stable_path,
            pending_path,
        }
    }

    #[cfg(test)]
    fn with_root(namespace: ConfigAuthorityNamespace, root: &Path) -> Self {
        let directory = root.join("tribal/credentials");
        Self {
            stable_path: directory.join(format!("{namespace}.json")),
            pending_path: directory.join(format!("{namespace}.pending")),
            namespace,
        }
    }

    pub(super) fn stage(
        &self,
        envelope: &PersistedCredentialEnvelope,
    ) -> Result<(), CredentialStoreError> {
        self.validate_namespace(envelope)?;
        let parent = self
            .pending_path
            .parent()
            .expect("credential path has a parent");
        std::fs::create_dir_all(parent).map_err(|source| file_error(parent, source))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(
                parent,
                std::fs::Permissions::from_mode(OWNER_DIRECTORY_MODE),
            )
            .map_err(|source| file_error(parent, source))?;
        }
        let bytes = serde_json::to_vec(envelope)
            .map_err(|source| CredentialStoreError::Encoding { source })?;
        tribal_config::write_atomically(&self.pending_path, &bytes, Some(OWNER_FILE_MODE))
            .map_err(|source| file_error(&self.pending_path, source))
    }

    pub(super) fn promote_pending(&self) -> Result<(), CredentialStoreError> {
        let parent = self
            .stable_path
            .parent()
            .expect("credential path has a parent");
        std::fs::rename(&self.pending_path, &self.stable_path)
            .map_err(|source| file_error(&self.pending_path, source))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| file_error(parent, source))
    }

    pub(super) fn recover(
        &self,
        mapping: Option<&LocalDefaultCredential>,
    ) -> Result<RecoveryDisposition, CredentialStoreError> {
        let stable = self.read(&self.stable_path)?;
        let pending = self.read(&self.pending_path)?;
        let stable_matches = mapping
            .zip(stable.as_ref())
            .is_some_and(|(mapping, envelope)| mapping_matches(mapping, envelope));
        let pending_matches = mapping
            .zip(pending.as_ref())
            .is_some_and(|(mapping, envelope)| mapping_matches(mapping, envelope));

        if stable_matches {
            Self::remove_if_exists(&self.pending_path)?;
            return Ok(RecoveryDisposition::Stable);
        }
        if pending_matches {
            self.promote_pending()?;
            return Ok(RecoveryDisposition::PromotedPending);
        }
        Self::remove_if_exists(&self.pending_path)?;
        if let Some(mapping) = mapping {
            return Ok(RecoveryDisposition::ReplaceMapped {
                token_id: mapping.token_id,
            });
        }
        Self::remove_if_exists(&self.stable_path)?;
        Ok(RecoveryDisposition::Empty)
    }

    pub(super) fn read_stable(
        &self,
    ) -> Result<Option<PersistedCredentialEnvelope>, CredentialStoreError> {
        self.read(&self.stable_path)
    }

    fn read(
        &self,
        path: &Path,
    ) -> Result<Option<PersistedCredentialEnvelope>, CredentialStoreError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(file_error(path, source)),
        };
        let envelope = serde_json::from_slice(&bytes)
            .map_err(|source| CredentialStoreError::Encoding { source })?;
        self.validate_namespace(&envelope)?;
        Ok(Some(envelope))
    }

    fn validate_namespace(
        &self,
        envelope: &PersistedCredentialEnvelope,
    ) -> Result<(), CredentialStoreError> {
        if envelope.namespace == self.namespace {
            Ok(())
        } else {
            Err(CredentialStoreError::NamespaceMismatch)
        }
    }

    fn remove_if_exists(path: &Path) -> Result<(), CredentialStoreError> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(file_error(path, source)),
        }
    }
}

fn mapping_matches(
    mapping: &LocalDefaultCredential,
    envelope: &PersistedCredentialEnvelope,
) -> bool {
    mapping.authority_namespace == envelope.namespace.as_str()
        && mapping.generation_id == envelope.generation_id
        && mapping.token_id == envelope.token_id
}

fn file_error(path: &Path, source: io::Error) -> CredentialStoreError {
    CredentialStoreError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod recovery {
    use chrono::Utc;

    use super::*;

    fn namespace(value: &str) -> ConfigAuthorityNamespace {
        ConfigAuthorityNamespace::from_test(value)
    }

    fn envelope(namespace: &ConfigAuthorityNamespace) -> PersistedCredentialEnvelope {
        PersistedCredentialEnvelope {
            namespace: namespace.clone(),
            generation_id: CredentialGenerationId::new(),
            token_id: AuthTokenId::new(),
            auth: Auth::Bearer {
                token: "secret-token".parse().expect("token parses"),
            },
        }
    }

    fn mapping(envelope: &PersistedCredentialEnvelope) -> LocalDefaultCredential {
        LocalDefaultCredential {
            authority_namespace: envelope.namespace.as_str().to_owned(),
            generation_id: envelope.generation_id,
            token_id: envelope.token_id,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn pre_commit_pending_loses_to_matching_stable_mapping() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        let stable = envelope(&namespace);
        store.stage(&stable).expect("stable stages");
        store.promote_pending().expect("stable promotes");
        let pending = envelope(&namespace);
        store.stage(&pending).expect("replacement stages");

        assert_eq!(
            store.recover(Some(&mapping(&stable))).expect("recovery"),
            RecoveryDisposition::Stable
        );
        assert_eq!(store.read_stable().expect("stable reads"), Some(stable));
        assert!(!store.pending_path.exists());
    }

    #[test]
    fn committed_pending_is_promoted_after_lost_ack_or_pre_rename_crash() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        let committed = envelope(&namespace);
        store.stage(&committed).expect("pending stages");

        assert_eq!(
            store.recover(Some(&mapping(&committed))).expect("recovery"),
            RecoveryDisposition::PromotedPending
        );
        assert_eq!(store.read_stable().expect("stable reads"), Some(committed));
        assert!(!store.pending_path.exists());
    }

    #[test]
    fn distinct_namespaces_never_promote_or_remove_each_others_files() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let first_namespace = namespace("0123456789abcdef01234567");
        let second_namespace = namespace("fedcba9876543210fedcba98");
        let first = CredentialStore::with_root(first_namespace.clone(), root.path());
        let second = CredentialStore::with_root(second_namespace.clone(), root.path());
        let first_envelope = envelope(&first_namespace);
        let second_envelope = envelope(&second_namespace);
        first.stage(&first_envelope).expect("first stages");
        second.stage(&second_envelope).expect("second stages");

        first
            .recover(Some(&mapping(&first_envelope)))
            .expect("first recovers");

        assert_eq!(
            second.read(&second.pending_path).expect("second reads"),
            Some(second_envelope)
        );
    }

    #[test]
    fn envelope_debug_never_exports_the_bearer() {
        let envelope = envelope(&namespace("0123456789abcdef01234567"));
        let debug = format!("{envelope:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn concurrent_recovery_converges_on_one_stable_generation() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = std::sync::Arc::new(CredentialStore::with_root(namespace.clone(), root.path()));
        let stable = envelope(&namespace);
        store.stage(&stable).expect("stable stages");
        store.promote_pending().expect("stable promotes");
        let mapping = std::sync::Arc::new(mapping(&stable));
        let observers: Vec<_> = (0..2)
            .map(|_| {
                let store = std::sync::Arc::clone(&store);
                let mapping = std::sync::Arc::clone(&mapping);
                std::thread::spawn(move || store.recover(Some(&mapping)))
            })
            .collect();

        for observer in observers {
            assert_eq!(
                observer
                    .join()
                    .expect("recovery thread joins")
                    .expect("recovery"),
                RecoveryDisposition::Stable
            );
        }
        assert_eq!(store.read_stable().expect("stable reads"), Some(stable));
    }

    #[test]
    fn mismatched_namespace_fails_before_pending_file_creation() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let current_namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(current_namespace, root.path());
        let foreign = envelope(&namespace("fedcba9876543210fedcba98"));

        assert!(matches!(
            store.stage(&foreign),
            Err(CredentialStoreError::NamespaceMismatch)
        ));
        assert!(!store.pending_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn staged_envelope_and_directory_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        store.stage(&envelope(&namespace)).expect("envelope stages");

        let directory_mode = std::fs::metadata(
            store
                .pending_path
                .parent()
                .expect("credential path has a parent"),
        )
        .expect("directory metadata")
        .permissions()
        .mode()
            & 0o777;
        let file_mode = std::fs::metadata(&store.pending_path)
            .expect("file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, OWNER_DIRECTORY_MODE);
        assert_eq!(file_mode, OWNER_FILE_MODE);
    }
}
