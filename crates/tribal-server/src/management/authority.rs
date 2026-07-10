//! Per-configuration-path authority lease and discovery artifacts.

use std::{
    fs::{File, OpenOptions},
    io,
    os::fd::{AsRawFd as _, OwnedFd},
    os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use tribal_config::{TRIBAL_DIRECTORY_NAME, write_atomically};

const OWNER_FILE_MODE: u32 = 0o600;
const OWNER_DIRECTORY_MODE: u32 = 0o700;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Process role holding a configuration authority lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthorityOwnerKind {
    Manager,
    StandaloneRuntime,
    OneShot,
}

/// Discoverable identity of the current authority owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthorityDescriptor {
    pub(crate) kind: AuthorityOwnerKind,
    pub(crate) instance_id: String,
    pub(crate) pid: u32,
    pub(crate) binary_version: String,
    pub(crate) canonical_config_path: PathBuf,
    pub(crate) socket_path: Option<PathBuf>,
    pub(crate) protocol_version: Option<u16>,
}

/// Artifact paths derived from one canonical configuration path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_field_names)] // each field names a distinct filesystem path artifact
pub(crate) struct AuthorityPaths {
    pub(crate) canonical_config_path: PathBuf,
    pub(crate) lock_path: PathBuf,
    pub(crate) descriptor_path: PathBuf,
    pub(crate) socket_path: PathBuf,
    pub(crate) runtime_control_socket_path: PathBuf,
    pub(crate) custody_socket_path: PathBuf,
    pub(crate) delegated_descriptor_path: PathBuf,
}

/// A held lease; dropping it releases the kernel lock and owned descriptor.
#[derive(Debug)]
pub(crate) struct AuthorityLease {
    file: File,
    paths: AuthorityPaths,
    published_instance_id: Option<String>,
}

/// Non-blocking authority acquisition result.
#[derive(Debug)]
pub(crate) enum AuthorityAcquire {
    Acquired(AuthorityLease),
    Occupied(AuthorityConflict),
}

/// Typed classification of a live or recovering authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthorityConflict {
    Manager(AuthorityDescriptor),
    StandaloneRuntime(AuthorityDescriptor),
    OneShot(AuthorityDescriptor),
    Recovering { canonical_config_path: PathBuf },
}

/// Failure preparing or acquiring per-path authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthorityError {
    #[error("preparing configuration authority path '{}': {source}", path.display())]
    Filesystem {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("serialising authority descriptor: {source}")]
    DescriptorSerialise {
        #[source]
        source: serde_json::Error,
    },
    #[error("authority descriptor for '{}' names a different configuration path", path.display())]
    DescriptorPathMismatch { path: PathBuf },
    #[error("inherited authority descriptor does not name the configured lock file")]
    InheritedDescriptorMismatch,
    #[error("configuration authority is already held for '{}'", path.display())]
    Occupied { path: PathBuf },
}

impl AuthorityLease {
    /// Acquires the production authority derived from platform state paths.
    pub(crate) fn acquire(config_path: &Path) -> Result<AuthorityAcquire, AuthorityError> {
        let state_base = state_base();
        let runtime_base = runtime_base();
        Self::acquire_with_roots(config_path, &state_base, &runtime_base)
    }

    /// Derives the artifacts for recovery without attempting to acquire the lease.
    pub(crate) fn paths_for(config_path: &Path) -> Result<AuthorityPaths, AuthorityError> {
        let state_base = state_base();
        let runtime_base = runtime_base();
        let canonical_config_path = materialise_config(config_path)?;
        derive_paths(&canonical_config_path, &state_base, &runtime_base)
    }

    /// Adopts a locked descriptor inherited from a manager process.
    pub(crate) fn from_inherited(
        config_path: &Path,
        descriptor: OwnedFd,
    ) -> Result<Self, AuthorityError> {
        let state_base = state_base();
        let runtime_base = runtime_base();
        let canonical_config_path = materialise_config(config_path)?;
        let paths = derive_paths(&canonical_config_path, &state_base, &runtime_base)?;
        Self::from_inherited_with_paths(descriptor, paths)
    }

    pub(super) fn from_inherited_with_paths(
        descriptor: OwnedFd,
        paths: AuthorityPaths,
    ) -> Result<Self, AuthorityError> {
        let file = File::from(descriptor);
        let inherited = file
            .metadata()
            .map_err(|source| filesystem(&paths.lock_path, source))?;
        let expected = std::fs::metadata(&paths.lock_path)
            .map_err(|source| filesystem(&paths.lock_path, source))?;
        if inherited.dev() != expected.dev() || inherited.ino() != expected.ino() {
            return Err(AuthorityError::InheritedDescriptorMismatch);
        }
        Ok(Self {
            file,
            paths,
            published_instance_id: None,
        })
    }

    /// Duplicates the locked open-file description for child inheritance.
    pub(crate) fn inheritable_clone(&self) -> Result<File, AuthorityError> {
        let clone = self
            .file
            .try_clone()
            .map_err(|source| filesystem(&self.paths.lock_path, source))?;
        let fd = clone.as_raw_fd();
        // SAFETY: `fd` names the live duplicate owned by `clone`; `F_GETFD` and
        // `F_SETFD` only inspect and clear its close-on-exec bit.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            return Err(filesystem(
                &self.paths.lock_path,
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: the descriptor remains owned by `clone`, and the updated
        // flags preserve every bit except close-on-exec for deliberate inheritance.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
            return Err(filesystem(
                &self.paths.lock_path,
                io::Error::last_os_error(),
            ));
        }
        Ok(clone)
    }

    pub(super) fn acquire_with_roots(
        config_path: &Path,
        state_base: &Path,
        runtime_base: &Path,
    ) -> Result<AuthorityAcquire, AuthorityError> {
        let canonical_config_path = materialise_config(config_path)?;
        let paths = derive_paths(&canonical_config_path, state_base, runtime_base)?;
        ensure_owner_directory(
            paths
                .descriptor_path
                .parent()
                .ok_or_else(|| fs_error(&paths.descriptor_path, io::ErrorKind::InvalidInput))?,
        )?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(OWNER_FILE_MODE)
            .open(&paths.lock_path)
            .map_err(|source| filesystem(&paths.lock_path, source))?;
        std::fs::set_permissions(
            &paths.lock_path,
            std::fs::Permissions::from_mode(OWNER_FILE_MODE),
        )
        .map_err(|source| filesystem(&paths.lock_path, source))?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(AuthorityAcquire::Acquired(Self {
                file,
                paths,
                published_instance_id: None,
            })),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                let conflict = read_conflict(&paths)?;
                Ok(AuthorityAcquire::Occupied(conflict))
            }
            Err(source) => Err(filesystem(&paths.lock_path, source)),
        }
    }

    /// Paths bound to this held lease.
    pub(crate) fn paths(&self) -> &AuthorityPaths {
        &self.paths
    }

    /// Publishes an owner-only descriptor for discovery by contenders.
    pub(crate) fn publish(
        &mut self,
        descriptor: &AuthorityDescriptor,
    ) -> Result<(), AuthorityError> {
        if descriptor.canonical_config_path != self.paths.canonical_config_path {
            return Err(AuthorityError::DescriptorPathMismatch {
                path: descriptor.canonical_config_path.clone(),
            });
        }
        let bytes = serde_json::to_vec_pretty(descriptor)
            .map_err(|source| AuthorityError::DescriptorSerialise { source })?;
        write_atomically(&self.paths.descriptor_path, &bytes, Some(OWNER_FILE_MODE))
            .map_err(|source| filesystem(&self.paths.descriptor_path, source))?;
        self.published_instance_id = Some(descriptor.instance_id.clone());
        Ok(())
    }
}

fn state_base() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(dirs::state_dir)
        .or_else(dirs::data_local_dir)
        .unwrap_or_else(std::env::temp_dir)
}

fn runtime_base() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(dirs::runtime_dir)
        .unwrap_or_else(std::env::temp_dir)
}

impl Drop for AuthorityLease {
    fn drop(&mut self) {
        let owns_descriptor = self
            .published_instance_id
            .as_ref()
            .is_some_and(|instance_id| {
                read_descriptor(&self.paths.descriptor_path)
                    .is_some_and(|descriptor| descriptor.instance_id == *instance_id)
            });
        if owns_descriptor {
            let _ = std::fs::remove_file(&self.paths.descriptor_path);
            let _ = std::fs::remove_file(&self.paths.socket_path);
            let _ = std::fs::remove_file(&self.paths.runtime_control_socket_path);
        }
    }
}

fn materialise_config(path: &Path) -> Result<PathBuf, AuthorityError> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|source| filesystem(path, source))?
            .join(path)
    };
    let parent = absolute
        .parent()
        .ok_or_else(|| fs_error(&absolute, io::ErrorKind::InvalidInput))?;
    std::fs::create_dir_all(parent).map_err(|source| filesystem(parent, source))?;
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(OWNER_FILE_MODE)
        .open(&absolute)
    {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
        Err(source) => return Err(filesystem(&absolute, source)),
    }
    std::fs::set_permissions(&absolute, std::fs::Permissions::from_mode(OWNER_FILE_MODE))
        .map_err(|source| filesystem(&absolute, source))?;
    absolute
        .canonicalize()
        .map_err(|source| filesystem(&absolute, source))
}

fn derive_paths(
    canonical_config_path: &Path,
    state_base: &Path,
    runtime_base: &Path,
) -> Result<AuthorityPaths, AuthorityError> {
    let file_name = canonical_config_path
        .file_name()
        .ok_or_else(|| fs_error(canonical_config_path, io::ErrorKind::InvalidInput))?
        .to_string_lossy();
    let digest = sha2::Sha256::digest(canonical_config_path.as_os_str().as_encoded_bytes());
    let mut key = String::with_capacity(24);
    for byte in &digest[..12] {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    let state_dir = state_base
        .join(TRIBAL_DIRECTORY_NAME)
        .join("management")
        .join(&key);
    let runtime_dir = runtime_base.join(TRIBAL_DIRECTORY_NAME);
    Ok(AuthorityPaths {
        canonical_config_path: canonical_config_path.to_owned(),
        lock_path: canonical_config_path.with_file_name(format!(".{file_name}.authority.lock")),
        descriptor_path: state_dir.join("authority.json"),
        socket_path: runtime_dir.join(format!("manager-{key}.sock")),
        runtime_control_socket_path: runtime_dir.join(format!("runtime-{key}.sock")),
        custody_socket_path: runtime_dir.join(format!("custody-{key}.sock")),
        delegated_descriptor_path: state_dir.join("delegated-runtime.json"),
    })
}

fn ensure_owner_directory(path: &Path) -> Result<(), AuthorityError> {
    std::fs::create_dir_all(path).map_err(|source| filesystem(path, source))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(OWNER_DIRECTORY_MODE))
        .map_err(|source| filesystem(path, source))
}

fn read_conflict(paths: &AuthorityPaths) -> Result<AuthorityConflict, AuthorityError> {
    let Some(descriptor) = read_descriptor(&paths.descriptor_path) else {
        return Ok(AuthorityConflict::Recovering {
            canonical_config_path: paths.canonical_config_path.clone(),
        });
    };
    if descriptor.canonical_config_path != paths.canonical_config_path {
        return Err(AuthorityError::DescriptorPathMismatch {
            path: descriptor.canonical_config_path,
        });
    }
    Ok(match descriptor.kind {
        AuthorityOwnerKind::Manager => AuthorityConflict::Manager(descriptor),
        AuthorityOwnerKind::StandaloneRuntime => AuthorityConflict::StandaloneRuntime(descriptor),
        AuthorityOwnerKind::OneShot => AuthorityConflict::OneShot(descriptor),
    })
}

fn read_descriptor(path: &Path) -> Option<AuthorityDescriptor> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn filesystem(path: &Path, source: io::Error) -> AuthorityError {
    AuthorityError::Filesystem {
        path: path.to_owned(),
        source,
    }
}

fn fs_error(path: &Path, kind: io::ErrorKind) -> AuthorityError {
    filesystem(path, io::Error::new(kind, "invalid authority path"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acquire(
        config: &Path,
        state: &Path,
        runtime: &Path,
    ) -> Result<AuthorityAcquire, AuthorityError> {
        AuthorityLease::acquire_with_roots(config, state, runtime)
    }

    #[test]
    fn test_one_path_has_exactly_one_nonblocking_authority() {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let config = temp.path().join("config/tribal.yaml");
        let first = acquire(
            &config,
            &temp.path().join("state"),
            &temp.path().join("run"),
        )
        .expect("first acquisition succeeds");
        let AuthorityAcquire::Acquired(_first) = first else {
            panic!("first contender must acquire the lease");
        };

        let second = acquire(
            &config,
            &temp.path().join("state"),
            &temp.path().join("run"),
        )
        .expect("second acquisition classifies contention");
        assert!(matches!(
            second,
            AuthorityAcquire::Occupied(AuthorityConflict::Recovering { .. })
        ));
    }

    #[test]
    fn test_distinct_config_paths_have_independent_authorities() {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let state = temp.path().join("state");
        let runtime = temp.path().join("run");
        let first = acquire(&temp.path().join("a.yaml"), &state, &runtime)
            .expect("first path acquisition succeeds");
        let second = acquire(&temp.path().join("b.yaml"), &state, &runtime)
            .expect("second path acquisition succeeds");
        assert!(matches!(first, AuthorityAcquire::Acquired(_)));
        assert!(matches!(second, AuthorityAcquire::Acquired(_)));
    }

    #[test]
    fn test_descriptor_classifies_a_manager_contender() {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let config = temp.path().join("tribal.yaml");
        let state = temp.path().join("state");
        let runtime = temp.path().join("run");
        let first = acquire(&config, &state, &runtime).expect("lease acquisition succeeds");
        let AuthorityAcquire::Acquired(mut lease) = first else {
            panic!("first contender must acquire the lease");
        };
        let descriptor = AuthorityDescriptor {
            kind: AuthorityOwnerKind::Manager,
            instance_id: "manager-one".to_owned(),
            pid: 42,
            binary_version: "test".to_owned(),
            canonical_config_path: lease.paths().canonical_config_path.clone(),
            socket_path: Some(lease.paths().socket_path.clone()),
            protocol_version: Some(1),
        };
        lease.publish(&descriptor).expect("descriptor publishes");

        let contender = acquire(&config, &state, &runtime).expect("contention classifies");
        assert!(matches!(
            contender,
            AuthorityAcquire::Occupied(AuthorityConflict::Manager(found)) if found == descriptor
        ));
    }

    #[test]
    fn test_materialised_config_and_artifacts_are_owner_only() {
        let temp = tempfile::tempdir().expect("temporary authority root");
        let config = temp.path().join("nested/tribal.yaml");
        let state = temp.path().join("state");
        let runtime = temp.path().join("run");
        let acquired = acquire(&config, &state, &runtime).expect("lease acquisition succeeds");
        let AuthorityAcquire::Acquired(lease) = acquired else {
            panic!("lease must be acquired");
        };
        let config_mode = std::fs::metadata(&config)
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o777;
        let lock_mode = std::fs::metadata(&lease.paths().lock_path)
            .expect("lock metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(config_mode, OWNER_FILE_MODE);
        assert_eq!(lock_mode, OWNER_FILE_MODE);
    }
}
