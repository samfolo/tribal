//! Credentials file types and atomic writer.
//!
//! Backs the `$XDG_CONFIG_HOME/tribal/credentials.json` artefact written
//! by `tribal setup`, `tribal token create`, and `tribal bootstrap`.
//! `tribal mcp-config` reads from the same file when rendering an
//! HTTP/SSE wire-up snippet.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;
use tribal_domain::BearerToken;

use crate::paths::{ConfigDirError, credentials_file_path};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// File permissions applied to the persisted credentials file (POSIX).
#[cfg(unix)]
const CREDENTIALS_FILE_MODE: u32 = 0o600;

/// Prefix of the warn-and-success message emitted when a credentials write fails.
///
/// Composed with the resolved path and the underlying `io::Error` at the
/// emission site: `format!("{PREFIX}{path}: {err}{SUFFIX}")`.
pub const CREDENTIALS_WRITE_FAILED_PREFIX: &str = "warning: could not persist credentials.json at ";

/// Suffix of the warn-and-success message — recovery hint and reassurance
/// that the token is still valid despite the file write failing.
pub const CREDENTIALS_WRITE_FAILED_SUFFIX: &str = ". The token has been printed above and is valid in the database; you can recover it from this output or by running `tribal token create` again.";

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Authentication shape carried in [`Credentials`].
///
/// Internally tagged on the `type` discriminator so new shapes (e.g. an
/// `Oauth` variant) can be added without bumping the schema version, while
/// `deny_unknown_fields` still rejects mistyped field names within a
/// variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    /// Bearer-token authentication.
    Bearer {
        /// The minted token. Redacts itself in `Debug` output.
        token: BearerToken,
    },
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// On-disk credentials envelope.
///
/// `deny_unknown_fields` means any new top-level field is a breaking
/// schema change — the version is bumped, and old readers reject newer
/// files cleanly. New variants of [`Auth`] are non-breaking and stay at
/// the same schema version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Envelope schema version. Bumped only for breaking changes to the
    /// top-level fields (e.g. adding a `profiles` map).
    pub schema_version: u32,
    /// The active authentication value.
    pub auth: Auth,
}

impl Credentials {
    /// Schema version emitted by the current binary.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Constructs a v1 envelope around a bearer token.
    #[must_use]
    pub fn bearer(token: BearerToken) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            auth: Auth::Bearer { token },
        }
    }
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Failure persisting [`Credentials`] to disk.
#[derive(Debug, Error)]
pub enum CredentialsWriteError {
    /// Resolution of the credentials path itself failed (e.g. missing
    /// `$XDG_CONFIG_HOME` and `$HOME`).
    #[error(transparent)]
    Path(#[from] ConfigDirError),

    /// The parent directory could not be created.
    #[error("could not create directory {path}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Writing the tempfile in the parent directory failed.
    #[error("could not write tempfile in {parent}")]
    WriteTempfile {
        parent: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Serialising the credentials to JSON failed.
    #[error("could not serialise credentials to JSON")]
    Serialise(#[source] serde_json::Error),

    /// Setting the `0600` file mode failed.
    #[cfg(unix)]
    #[error("could not set permissions on {path}")]
    SetPermissions {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Renaming the tempfile to the target path failed.
    #[error("could not persist tempfile to {path}")]
    Persist {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl CredentialsWriteError {
    /// Returns the credentials.json path the writer was targeting, when
    /// the error happened late enough that a path was known. Earlier
    /// failures (missing `$HOME`) return `None`.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Path(_) | Self::Serialise(_) => None,
            Self::CreateDir { path, .. }
            | Self::SetPermissions { path, .. }
            | Self::Persist { path, .. } => Some(path),
            Self::WriteTempfile { parent, .. } => Some(parent),
        }
    }
}

/// Resolves the credentials.json path under `$XDG_CONFIG_HOME` (or
/// `$HOME/.config` as a POSIX fallback) and writes `creds` atomically.
///
/// Implementation: serialise to JSON → create the parent directory →
/// write a sibling tempfile → set mode `0600` (POSIX) → rename
/// atomically onto the target path. On the happy path no tempfile is
/// left behind.
///
/// # Errors
///
/// Returns [`CredentialsWriteError`] when path resolution, directory
/// creation, tempfile write, permission set, or rename fails. The error
/// carries the resolved path so callers can compose the warn-and-success
/// literal.
pub fn write_credentials(creds: &Credentials) -> Result<PathBuf, CredentialsWriteError> {
    let path = credentials_file_path()?;
    write_credentials_at(&path, creds)?;
    Ok(path)
}

fn write_credentials_at(path: &Path, creds: &Credentials) -> Result<(), CredentialsWriteError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    fs::create_dir_all(parent).map_err(|source| CredentialsWriteError::CreateDir {
        path: parent.to_owned(),
        source,
    })?;

    let payload = serde_json::to_vec_pretty(creds).map_err(CredentialsWriteError::Serialise)?;

    let mut tempfile =
        NamedTempFile::new_in(parent).map_err(|source| CredentialsWriteError::WriteTempfile {
            parent: parent.to_owned(),
            source,
        })?;

    tempfile
        .write_all(&payload)
        .map_err(|source| CredentialsWriteError::WriteTempfile {
            parent: parent.to_owned(),
            source,
        })?;

    tempfile
        .as_file()
        .sync_all()
        .map_err(|source| CredentialsWriteError::WriteTempfile {
            parent: parent.to_owned(),
            source,
        })?;

    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(CREDENTIALS_FILE_MODE);
        tempfile
            .as_file()
            .set_permissions(perms)
            .map_err(|source| CredentialsWriteError::SetPermissions {
                path: path.to_owned(),
                source,
            })?;
    }

    tempfile
        .persist(path)
        .map_err(|err| CredentialsWriteError::Persist {
            path: path.to_owned(),
            source: err.error,
        })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> BearerToken {
        "Iz5pXq-test-token".parse().unwrap()
    }

    fn sample_credentials() -> Credentials {
        Credentials::bearer(sample_token())
    }

    // -- Serde round-trip ---------------------------------------------------

    #[test]
    fn test_serde_roundtrip_v1_bearer() {
        let original = sample_credentials();
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_serialised_shape_matches_spec() {
        let creds = sample_credentials();
        let json: serde_json::Value = serde_json::to_value(&creds).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["auth"]["type"], "bearer");
        assert_eq!(json["auth"]["token"], "Iz5pXq-test-token");
    }

    // -- Deserialise rejection ---------------------------------------------

    #[test]
    fn test_deserialise_rejects_missing_token() {
        let json = r#"{"schema_version": 1, "auth": {"type": "bearer"}}"#;
        let err = serde_json::from_str::<Credentials>(json).unwrap_err();
        assert!(err.to_string().contains("token"), "{err}");
    }

    #[test]
    fn test_deserialise_rejects_wrong_field_name() {
        let json = r#"{"schema_version": 1, "auth": {"type": "bearer", "access_token": "x"}}"#;
        let err = serde_json::from_str::<Credentials>(json).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("access_token") || message.contains("unknown"),
            "{message}",
        );
    }

    #[test]
    fn test_deserialise_rejects_unknown_top_level_field() {
        let json =
            r#"{"schema_version": 1, "auth": {"type": "bearer", "token": "t"}, "extras": "junk"}"#;
        let err = serde_json::from_str::<Credentials>(json).unwrap_err();
        assert!(err.to_string().contains("extras"), "{err}");
    }

    #[test]
    fn test_deserialise_rejects_unknown_auth_variant() {
        let json = r#"{"schema_version": 1, "auth": {"type": "magic", "token": "t"}}"#;
        assert!(serde_json::from_str::<Credentials>(json).is_err());
    }

    // -- Debug redaction ----------------------------------------------------

    #[test]
    fn test_debug_does_not_leak_token() {
        let creds = sample_credentials();
        let debug = format!("{creds:?}");
        assert!(
            !debug.contains("Iz5pXq-test-token"),
            "credentials debug leaked token: {debug}",
        );
    }

    // -- Atomic write -------------------------------------------------------

    #[test]
    fn test_write_credentials_at_writes_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal").join("credentials.json");

        let creds = sample_credentials();
        write_credentials_at(&path, &creds).unwrap();

        let raw = fs::read_to_string(&path).unwrap();
        let parsed: Credentials = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed, creds);
    }

    #[cfg(unix)]
    #[test]
    fn test_write_credentials_at_sets_mode_0600() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");

        write_credentials_at(&path, &sample_credentials()).unwrap();

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, CREDENTIALS_FILE_MODE);
    }

    #[test]
    fn test_write_credentials_at_leaves_no_tempfile_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");

        write_credentials_at(&path, &sample_credentials()).unwrap();

        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "credentials.json")
            .collect();
        assert!(
            leftovers.is_empty(),
            "unexpected tempfile leftovers: {leftovers:?}",
        );
    }

    #[test]
    fn test_write_credentials_at_overwrites_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");

        write_credentials_at(&path, &sample_credentials()).unwrap();

        let next = Credentials::bearer("Iz5pXq-second-token".parse().unwrap());
        write_credentials_at(&path, &next).unwrap();

        let parsed: Credentials =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed, next);
    }

    #[cfg(unix)]
    #[test]
    fn test_write_credentials_at_errors_when_parent_unwritable() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("locked");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();

        let path = parent.join("credentials.json");
        let err = write_credentials_at(&path, &sample_credentials()).unwrap_err();
        assert!(
            matches!(err, CredentialsWriteError::WriteTempfile { .. }),
            "expected WriteTempfile, got: {err:?}",
        );
        assert_eq!(err.path(), Some(parent.as_path()));

        // Restore permissions so the tempdir cleanup succeeds.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
    }
}
