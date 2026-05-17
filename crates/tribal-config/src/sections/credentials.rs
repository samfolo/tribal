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

/// Prefix of the warning emitted when the on-disk credentials file has
/// wider POSIX permissions than the `0600` invariant enforced at write
/// time. Composed with the resolved path at the emission site.
pub const CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX: &str = "warning: credentials.json at ";

/// Suffix of [`CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX`].
pub const CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX: &str =
    " has wider permissions than 0600; restrict with `chmod 600`.";

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
///
/// Each post-resolution variant carries the credentials file path the
/// writer was targeting, plus the underlying `io::Error` formatted via
/// `{source}` so warn-and-success output preserves both pieces of
/// diagnostic information.
#[derive(Debug, Error)]
pub enum CredentialsWriteError {
    /// Resolution of the credentials path itself failed (e.g. missing
    /// `$XDG_CONFIG_HOME` and `$HOME`).
    #[error(transparent)]
    Path(#[from] ConfigDirError),

    /// The parent directory could not be created.
    #[error("could not create parent directory of {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Writing the tempfile alongside the target credentials file failed.
    #[error("could not write tempfile beside {path}: {source}")]
    WriteTempfile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Serialising the credentials to JSON failed.
    #[error("could not serialise credentials to JSON: {0}")]
    Serialise(#[source] serde_json::Error),

    /// Setting the `0600` file mode failed.
    #[cfg(unix)]
    #[error("could not set permissions on {path}: {source}")]
    SetPermissions {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Renaming the tempfile to the target path failed.
    #[error("could not persist tempfile to {path}: {source}")]
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
            | Self::WriteTempfile { path, .. }
            | Self::SetPermissions { path, .. }
            | Self::Persist { path, .. } => Some(path),
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
        path: path.to_owned(),
        source,
    })?;

    let payload = serde_json::to_vec_pretty(creds).map_err(CredentialsWriteError::Serialise)?;

    let mut tempfile =
        NamedTempFile::new_in(parent).map_err(|source| CredentialsWriteError::WriteTempfile {
            path: path.to_owned(),
            source,
        })?;

    tempfile
        .write_all(&payload)
        .map_err(|source| CredentialsWriteError::WriteTempfile {
            path: path.to_owned(),
            source,
        })?;

    tempfile
        .as_file()
        .sync_all()
        .map_err(|source| CredentialsWriteError::WriteTempfile {
            path: path.to_owned(),
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
// Read
// ---------------------------------------------------------------------------

/// Credentials read from disk together with the metadata callers need to
/// render warnings.
#[derive(Debug)]
pub struct LoadedCredentials {
    /// Parsed credentials envelope.
    pub credentials: Credentials,
    /// Path the credentials were read from.
    pub path: PathBuf,
    /// State of the file's POSIX mode bits at read time.
    pub permissions: CredentialsPermissions,
}

/// State of the credentials file's POSIX mode bits.
///
/// The write side sets mode `0600` (Unix) so the file is readable only by
/// its owner. The reader checks the same invariant on load. A permissive
/// mode does not invalidate the credentials — the variant is purely a
/// security signal callers can warn on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialsPermissions {
    /// Mode bits restrict access to the file's owner (`(mode & 0o077) == 0`).
    Locked,
    /// Mode bits grant access to group or other (`(mode & 0o077) != 0`),
    /// making the file readable, writable, or executable by users beyond
    /// its owner. Callers warn-and-proceed.
    Permissive,
    /// Permissions could not be determined: the host is non-Unix or the
    /// file metadata could not be read.
    Unknown,
}

/// Failure loading [`Credentials`] from disk.
///
/// The `#[error]` strings double as the user-facing literals: `mcp-config`
/// renders these directly to stderr via the standard `eprintln!("{err}")`
/// dispatch in `main.rs`.
#[derive(Debug, Error)]
pub enum CredentialsReadError {
    /// Resolution of the credentials path itself failed (e.g. missing
    /// `$XDG_CONFIG_HOME` and `$HOME`).
    #[error(transparent)]
    Path(#[from] ConfigDirError),

    /// The credentials file does not exist at the resolved path.
    #[error(
        "no saved credentials; run `tribal setup` or `tribal bootstrap`, or pass `--token` explicitly."
    )]
    NotFound,

    /// Reading the file failed for an I/O reason other than absence.
    #[error("could not read credentials.json at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// JSON deserialisation failed.
    #[error(
        "credentials.json at {path} is malformed: {source}; re-mint with `tribal token create`"
    )]
    Malformed {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// The on-disk `schema_version` is not what this binary supports.
    #[error(
        "credentials.json schema_version {schema_version} is not supported by this binary; re-mint with `tribal token create`"
    )]
    UnsupportedSchema { schema_version: u32 },
}

/// Resolves the credentials.json path under `$XDG_CONFIG_HOME` (or
/// `$HOME/.config` as a POSIX fallback) and loads its contents.
///
/// Implementation: resolve path → check existence → read bytes → parse as
/// JSON → enforce `schema_version` matches [`Credentials::SCHEMA_VERSION`]
/// → inspect POSIX permissions (Unix only).
///
/// Permissions drift wider than `0600` is reported via the
/// [`CredentialsPermissions`] state on the returned value rather than
/// failing the load — callers warn-and-proceed because the file is still
/// readable.
///
/// # Errors
///
/// Returns [`CredentialsReadError`] when path resolution, file read, JSON
/// parse, or schema-version check fails.
pub fn read_credentials() -> Result<LoadedCredentials, CredentialsReadError> {
    let path = credentials_file_path()?;
    read_credentials_at(&path)
}

/// Minimal envelope used to read [`Credentials::schema_version`] before
/// strict deserialisation. A future envelope with additional top-level
/// fields would fail `Credentials`'s `deny_unknown_fields` check, so the
/// version is inspected first and an [`UnsupportedSchema`] error is
/// returned before the strict parse rejects unknown fields as
/// [`CredentialsReadError::Malformed`].
///
/// [`UnsupportedSchema`]: CredentialsReadError::UnsupportedSchema
#[derive(Deserialize)]
struct CredentialsEnvelope {
    schema_version: u32,
}

fn read_credentials_at(path: &Path) -> Result<LoadedCredentials, CredentialsReadError> {
    let raw = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(CredentialsReadError::NotFound);
        }
        Err(source) => {
            return Err(CredentialsReadError::Read {
                path: path.to_owned(),
                source,
            });
        }
    };

    let envelope: CredentialsEnvelope =
        serde_json::from_slice(&raw).map_err(|source| CredentialsReadError::Malformed {
            path: path.to_owned(),
            source,
        })?;

    if envelope.schema_version != Credentials::SCHEMA_VERSION {
        return Err(CredentialsReadError::UnsupportedSchema {
            schema_version: envelope.schema_version,
        });
    }

    let credentials: Credentials =
        serde_json::from_slice(&raw).map_err(|source| CredentialsReadError::Malformed {
            path: path.to_owned(),
            source,
        })?;

    let permissions = inspect_permissions(path);

    Ok(LoadedCredentials {
        credentials,
        path: path.to_owned(),
        permissions,
    })
}

/// Classifies the POSIX mode bits at `path` against the `0600` invariant.
/// Always returns [`CredentialsPermissions::Unknown`] on non-Unix targets
/// or when metadata cannot be read.
///
/// Drift is defined as the file being readable, writable, or executable
/// by group or other (`(mode & 0o077) != 0`). Modes narrower than `0o600`
/// (e.g. owner-read-only `0o400`) are not drifted — the security signal
/// is "other users on the system can read this", not "the mode differs
/// from the bytes we wrote".
#[cfg(unix)]
fn inspect_permissions(path: &Path) -> CredentialsPermissions {
    let Ok(metadata) = fs::metadata(path) else {
        return CredentialsPermissions::Unknown;
    };
    let mode = metadata.permissions().mode() & 0o777;
    // Bit-mask form is the idiom for POSIX permission checks; the
    // clippy-preferred `trailing_zeros >= 6` obscures the intent.
    #[allow(clippy::verbose_bit_mask)]
    if mode & 0o077 == 0 {
        CredentialsPermissions::Locked
    } else {
        CredentialsPermissions::Permissive
    }
}

#[cfg(not(unix))]
fn inspect_permissions(_path: &Path) -> CredentialsPermissions {
    CredentialsPermissions::Unknown
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::{CREDENTIALS_FILENAME, TRIBAL_DIRECTORY_NAME};

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
        // The error reports the credentials file path, not its parent
        // directory; the message surfaces alongside the underlying
        // io::Error so callers see both the target and the cause.
        assert_eq!(err.path(), Some(path.as_path()));
        let rendered = err.to_string();
        assert!(
            rendered.contains("credentials.json"),
            "warning should name the credentials file: {rendered}",
        );
        assert!(
            rendered.contains("Permission denied"),
            "warning should surface the underlying io::Error: {rendered}",
        );

        // Restore permissions so the tempdir cleanup succeeds.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_write_credentials_at_errors_when_create_dir_fails() {
        // Forces the `CreateDir` branch by making the grandparent
        // directory read-only so `create_dir_all` cannot mkdir the
        // missing `tribal/` parent.
        let dir = tempfile::tempdir().unwrap();
        let outer = dir.path().join("locked");
        fs::create_dir(&outer).unwrap();
        fs::set_permissions(&outer, fs::Permissions::from_mode(0o500)).unwrap();

        let path = outer.join(TRIBAL_DIRECTORY_NAME).join(CREDENTIALS_FILENAME);
        let err = write_credentials_at(&path, &sample_credentials()).unwrap_err();
        assert!(
            matches!(err, CredentialsWriteError::CreateDir { .. }),
            "expected CreateDir, got: {err:?}",
        );
        assert_eq!(err.path(), Some(path.as_path()));
        let rendered = err.to_string();
        assert!(
            rendered.contains(CREDENTIALS_FILENAME),
            "warning should name the credentials file: {rendered}",
        );
        assert!(
            rendered.contains("Permission denied"),
            "warning should surface the underlying io::Error: {rendered}",
        );

        // Restore so the tempdir cleanup can run.
        fs::set_permissions(&outer, fs::Permissions::from_mode(0o700)).ok();
    }

    // -- Read --------------------------------------------------------------

    #[test]
    fn test_read_credentials_at_round_trips_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");

        let original = sample_credentials();
        write_credentials_at(&path, &original).unwrap();

        let loaded = read_credentials_at(&path).unwrap();
        assert_eq!(loaded.credentials, original);
        assert_eq!(loaded.path, path);
    }

    #[test]
    fn test_read_credentials_at_returns_not_found_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.json");
        let err = read_credentials_at(&path).unwrap_err();
        assert!(
            matches!(err, CredentialsReadError::NotFound),
            "expected NotFound, got: {err:?}",
        );
    }

    #[test]
    fn test_read_credentials_at_returns_malformed_for_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        fs::write(&path, b"{ not json").unwrap();

        let err = read_credentials_at(&path).unwrap_err();
        assert!(
            matches!(err, CredentialsReadError::Malformed { .. }),
            "expected Malformed, got: {err:?}",
        );
    }

    #[test]
    fn test_read_credentials_at_returns_unsupported_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let future_version = Credentials::SCHEMA_VERSION + 1;
        let payload = format!(
            r#"{{"schema_version": {future_version}, "auth": {{"type": "bearer", "token": "x"}}}}"#,
        );
        fs::write(&path, payload).unwrap();

        let err = read_credentials_at(&path).unwrap_err();
        assert!(
            matches!(
                err,
                CredentialsReadError::UnsupportedSchema { schema_version }
                    if schema_version == future_version,
            ),
            "expected UnsupportedSchema {{ schema_version: {future_version} }}, got: {err:?}",
        );
    }

    /// A future envelope shape may introduce additional top-level fields
    /// (e.g. a `profiles` map). The current binary's `deny_unknown_fields`
    /// would reject those as malformed if the version were not inspected
    /// first — verify the envelope read returns `UnsupportedSchema` so the
    /// user sees the upgrade hint rather than a misleading JSON-parse
    /// error.
    #[test]
    fn test_read_credentials_at_returns_unsupported_schema_for_future_shape_with_extra_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let future_version = Credentials::SCHEMA_VERSION + 1;
        let payload = format!(
            r#"{{"schema_version": {future_version}, "auth": {{"type": "bearer", "token": "x"}}, "profiles": {{}}}}"#,
        );
        fs::write(&path, payload).unwrap();

        let err = read_credentials_at(&path).unwrap_err();
        assert!(
            matches!(
                err,
                CredentialsReadError::UnsupportedSchema { schema_version }
                    if schema_version == future_version,
            ),
            "expected UnsupportedSchema {{ schema_version: {future_version} }}, got: {err:?}",
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_read_credentials_at_reports_permissions_locked_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        write_credentials_at(&path, &sample_credentials()).unwrap();

        let loaded = read_credentials_at(&path).unwrap();
        assert_eq!(loaded.permissions, CredentialsPermissions::Locked);
    }

    #[cfg(unix)]
    #[test]
    fn test_read_credentials_at_reports_permissions_permissive_for_wider_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        write_credentials_at(&path, &sample_credentials()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        let loaded = read_credentials_at(&path).unwrap();
        assert_eq!(loaded.permissions, CredentialsPermissions::Permissive);
    }

    #[cfg(unix)]
    #[test]
    fn test_read_credentials_at_reports_permissions_locked_for_narrower_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        write_credentials_at(&path, &sample_credentials()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();

        let loaded = read_credentials_at(&path).unwrap();
        assert_eq!(loaded.permissions, CredentialsPermissions::Locked);
    }

    // -- Error display literals --------------------------------------------

    #[test]
    fn test_not_found_display_matches_literal() {
        let err = CredentialsReadError::NotFound;
        assert_eq!(
            err.to_string(),
            "no saved credentials; run `tribal setup` or `tribal bootstrap`, or pass `--token` explicitly.",
        );
    }

    #[test]
    fn test_unsupported_schema_display_matches_literal() {
        let err = CredentialsReadError::UnsupportedSchema { schema_version: 2 };
        assert_eq!(
            err.to_string(),
            "credentials.json schema_version 2 is not supported by this binary; re-mint with `tribal token create`",
        );
    }

    #[test]
    fn test_malformed_display_includes_path_and_recovery_hint() {
        let err = serde_json::from_slice::<Credentials>(b"{ not json").unwrap_err();
        let display = CredentialsReadError::Malformed {
            path: PathBuf::from("/tmp/credentials.json"),
            source: err,
        }
        .to_string();
        assert!(
            display.starts_with("credentials.json at /tmp/credentials.json is malformed: "),
            "unexpected prefix: {display}",
        );
        assert!(
            display.ends_with("; re-mint with `tribal token create`"),
            "unexpected suffix: {display}",
        );
    }
}
