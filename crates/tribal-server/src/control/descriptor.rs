//! The runtime descriptor a client reads to discover a running control plane,
//! and the socket-path derivation the plane binds.
//!
//! The descriptor (`control.json`) lives at a stable, well-known path both sides
//! compute from the same base directories; it records where the socket actually
//! is, so the socket itself may sit at a short runtime path that fits the
//! kernel's `sun_path` field. A descriptor left behind by a crashed server is
//! *stale*: [`RuntimeDescriptor::is_reachable`] settles liveness by connecting,
//! never by trusting the file's mere presence.

use std::{
    io::Write as _,
    os::unix::ffi::OsStrExt as _,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;
use tribal_config::TRIBAL_DIRECTORY_NAME;

use super::error::ControlError;

/// The descriptor filename under the state directory.
const DESCRIPTOR_FILENAME: &str = "control.json";

/// The socket filename under the runtime directory.
const SOCKET_FILENAME: &str = "control.sock";

/// The platform's `sockaddr_un.sun_path` capacity, including the trailing NUL.
/// A bound path must be at least one byte shorter than this.
#[cfg(target_os = "macos")]
const SUN_PATH_CAPACITY: usize = 104;
#[cfg(not(target_os = "macos"))]
const SUN_PATH_CAPACITY: usize = 108;

// ---------------------------------------------------------------------------
// The descriptor
// ---------------------------------------------------------------------------

/// What a client reads from `control.json` to reach and identify a running
/// control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeDescriptor {
    /// The bound control socket's filesystem path.
    pub socket_path: PathBuf,
    /// The control-contract version the server speaks.
    pub protocol_version: u16,
    /// The server process id.
    pub pid: u32,
    /// The per-serve instance identity.
    pub instance_id: String,
    /// The binary's build version.
    pub binary_version: String,
    /// Whether a supervisor owns the process (governs `server.restart`).
    pub supervised: bool,
}

impl RuntimeDescriptor {
    /// Writes the descriptor to `path` atomically, via a sibling tempfile
    /// renamed into place, so a reader never sees a half-written file.
    pub(crate) fn write_atomically(&self, path: &Path) -> Result<(), ControlError> {
        let bytes = serde_json::to_vec_pretty(self).expect("a runtime descriptor serialises");
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(|source| ControlError::Filesystem {
            path: parent.to_owned(),
            source,
        })?;
        let temporary = parent.join(format!(".{DESCRIPTOR_FILENAME}.{}.tmp", std::process::id()));
        write_then_rename(&temporary, path, &bytes)
    }

    /// Reads a descriptor from `path`, returning `None` when it is absent or
    /// unparseable — a missing or corrupt descriptor is simply "no server".
    pub(crate) fn read(path: &Path) -> Option<Self> {
        let bytes = std::fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Whether the described server is actually reachable, by connecting to its
    /// socket. A stale descriptor whose server has died connects to nothing and
    /// reads as dead, never as a live server.
    pub(crate) async fn is_reachable(&self) -> bool {
        UnixStream::connect(&self.socket_path).await.is_ok()
    }
}

/// Writes `bytes` to `temporary`, flushes it to disk, and renames it over
/// `final_path`.
fn write_then_rename(
    temporary: &Path,
    final_path: &Path,
    bytes: &[u8],
) -> Result<(), ControlError> {
    let outcome = (|| {
        let mut file = std::fs::File::create(temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, final_path)
    })();
    outcome.map_err(|source| {
        let _ = std::fs::remove_file(temporary);
        ControlError::Filesystem {
            path: final_path.to_owned(),
            source,
        }
    })
}

// ---------------------------------------------------------------------------
// Path derivation
// ---------------------------------------------------------------------------

/// The stable, well-known descriptor path, under the user's state directory
/// (falling back to the local-data then temp directory, as the log writer
/// does).
pub(crate) fn descriptor_path() -> PathBuf {
    let base = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .unwrap_or_else(std::env::temp_dir);
    base.join(TRIBAL_DIRECTORY_NAME).join(DESCRIPTOR_FILENAME)
}

/// A socket path short enough to bind, under the runtime directory when it fits,
/// else a flat per-runtime-unique name directly under the temp directory.
///
/// # Errors
///
/// Returns [`ControlError::SocketPathTooLong`] when even the fallback exceeds
/// the platform's `sun_path` capacity.
pub(crate) fn socket_path() -> Result<PathBuf, ControlError> {
    resolve_socket_path(
        &dirs::runtime_dir().unwrap_or_else(std::env::temp_dir),
        &std::env::temp_dir(),
    )
}

/// Derives the socket path from a `runtime` and `temp` directory: the runtime
/// path when it fits, else a flat per-runtime-unique name under `temp`, else the
/// too-long error. Split from [`socket_path`] so the fallback and error branches
/// test without the platform's real directories.
fn resolve_socket_path(runtime: &Path, temp: &Path) -> Result<PathBuf, ControlError> {
    let preferred = runtime.join(TRIBAL_DIRECTORY_NAME).join(SOCKET_FILENAME);
    if fits_sun_path(&preferred) {
        return Ok(preferred);
    }

    let digest = short_digest(runtime.as_os_str().as_bytes());
    let fallback = temp.join(format!("tribal-{digest}.sock"));
    if fits_sun_path(&fallback) {
        return Ok(fallback);
    }

    Err(ControlError::SocketPathTooLong {
        length: sun_path_len(&fallback),
        path: fallback,
        limit: SUN_PATH_CAPACITY,
    })
}

/// Whether `path` fits the platform's `sun_path` field, leaving room for the
/// trailing NUL.
fn fits_sun_path(path: &Path) -> bool {
    sun_path_len(path) < SUN_PATH_CAPACITY
}

/// The byte length `path` occupies in `sun_path`, excluding the NUL.
fn sun_path_len(path: &Path) -> usize {
    path.as_os_str().as_bytes().len()
}

/// A short, stable hex digest of `bytes`, so the fallback socket name is unique
/// per runtime directory (hence per user) without leaking its length.
fn short_digest(bytes: &[u8]) -> String {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RuntimeDescriptor {
        RuntimeDescriptor {
            socket_path: PathBuf::from("/run/user/1000/tribal/control.sock"),
            protocol_version: 1,
            pid: 4242,
            instance_id: "host~4242~boot".to_owned(),
            binary_version: "1.2.3".to_owned(),
            supervised: false,
        }
    }

    #[test]
    fn test_a_descriptor_round_trips_through_a_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("control.json");
        let descriptor = sample();
        descriptor.write_atomically(&path).expect("write");
        assert_eq!(RuntimeDescriptor::read(&path), Some(descriptor));
    }

    #[test]
    fn test_an_absent_descriptor_reads_as_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(RuntimeDescriptor::read(&dir.path().join("absent.json")).is_none());
    }

    #[test]
    fn test_a_corrupt_descriptor_reads_as_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("control.json");
        std::fs::write(&path, b"{ not json").expect("write");
        assert!(RuntimeDescriptor::read(&path).is_none());
    }

    #[tokio::test]
    async fn test_a_stale_descriptor_is_not_reachable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut descriptor = sample();
        // Point at a socket path that no server is bound to.
        descriptor.socket_path = dir.path().join("dead.sock");
        assert!(
            !descriptor.is_reachable().await,
            "a descriptor whose socket accepts nothing must read as dead",
        );
    }

    #[test]
    fn test_the_derived_socket_path_fits_sun_path() {
        let path = socket_path().expect("a bindable socket path is derived");
        assert!(
            fits_sun_path(&path),
            "the derived socket path {} is {} bytes, over the {}-byte limit",
            path.display(),
            sun_path_len(&path),
            SUN_PATH_CAPACITY,
        );
    }

    #[test]
    fn test_an_over_long_path_does_not_fit() {
        let long = PathBuf::from("/".to_owned() + &"a".repeat(SUN_PATH_CAPACITY));
        assert!(
            !fits_sun_path(&long),
            "a path at the capacity must be rejected, leaving room for the NUL",
        );
    }

    #[test]
    fn test_socket_path_prefers_the_runtime_directory_when_it_fits() {
        let resolved = resolve_socket_path(Path::new("/run/user/1000"), Path::new("/tmp"))
            .expect("a short runtime path fits");
        assert!(
            resolved.starts_with("/run/user/1000"),
            "the runtime directory is preferred: {resolved:?}",
        );
    }

    #[test]
    fn test_socket_path_falls_back_to_temp_when_the_runtime_path_is_too_long() {
        let long_runtime = PathBuf::from("/".to_owned() + &"x".repeat(SUN_PATH_CAPACITY));
        let resolved = resolve_socket_path(&long_runtime, Path::new("/tmp"))
            .expect("the flat temp fallback fits");
        assert!(
            resolved.starts_with("/tmp"),
            "an over-long runtime path falls back under temp: {resolved:?}",
        );
        assert!(fits_sun_path(&resolved), "the fallback itself must fit");
    }

    #[test]
    fn test_socket_path_errors_when_even_the_fallback_is_too_long() {
        let long = PathBuf::from("/".to_owned() + &"x".repeat(SUN_PATH_CAPACITY));
        let error = resolve_socket_path(&long, &long).expect_err("no path fits");
        assert!(
            matches!(error, ControlError::SocketPathTooLong { .. }),
            "when neither directory fits, the derivation errors rather than binding",
        );
    }

    #[test]
    fn test_the_digest_is_stable_and_short() {
        let bytes = b"/run/user/1000";
        assert_eq!(short_digest(bytes), short_digest(bytes));
        assert_eq!(short_digest(bytes).len(), 16);
    }
}
