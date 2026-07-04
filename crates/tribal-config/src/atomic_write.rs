//! One atomic file write, shared by every writer that must not expose a
//! half-written file.

use std::{io::Write as _, path::Path};

/// Writes `bytes` to `path` atomically: create the parent directory, write a
/// sibling tempfile, fsync it, apply `mode` when given (owner-only for a file
/// that may hold a secret), and rename it into place. A reader of `path` sees
/// either the old contents or the new, never a partial write; the tempfile is
/// removed if any step fails.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when directory creation, the
/// tempfile write, the permission set, or the rename fails.
pub fn write_atomically(path: &Path, bytes: &[u8], mode: Option<u32>) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let mut tempfile = tempfile::NamedTempFile::new_in(parent)?;
    tempfile.write_all(bytes)?;
    tempfile.as_file().sync_all()?;

    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt;
        tempfile
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(mode))?;
    }

    tempfile.persist(path).map_err(|error| error.error)?;
    Ok(())
}
