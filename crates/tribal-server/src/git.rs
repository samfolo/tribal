//! Git remote detection via `gix`.
//!
//! Discovers the git repository from a given directory and extracts the
//! default fetch remote URL as a [`GitRemote`] in canonical form.

use std::path::Path;

use gix::remote::Direction;
use tribal_domain::GitRemote;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Error when `gix::discover` fails to find a git repository.
pub(crate) const NOT_A_GIT_REPOSITORY: &str = "not inside a git repository";

/// Error when no default fetch remote is configured.
pub(crate) const NO_FETCH_REMOTE: &str = "no default fetch remote configured";

/// Error when the default fetch remote is configured but could not be read.
pub(crate) const FETCH_REMOTE_UNREADABLE: &str = "default fetch remote could not be read";

/// Error when the default fetch remote exists but has no URL.
pub(crate) const FETCH_REMOTE_NO_URL: &str = "default fetch remote has no URL";

/// Error when the remote URL has no host component.
pub(crate) const REMOTE_URL_MISSING_HOST: &str = "remote URL has no host component";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Detects the git remote from the current working directory.
///
/// Delegates to [`detect_git_remote_from`] with `"."` as the start
/// directory.
///
/// # Errors
///
/// Returns [`AppError::GitDetection`] if the current directory is not
/// inside a git repository, no default fetch remote is configured, or
/// the remote URL has no host component.
pub(crate) fn detect_git_remote() -> Result<GitRemote, AppError> {
    detect_git_remote_from(Path::new("."))
}

/// Detects the git remote starting from `start_dir`.
///
/// Uses `gix::discover` to find the enclosing repository and reads the
/// default fetch remote URL. The URL is normalised to canonical
/// `host/path` form via [`GitRemote::from_parts`].
///
/// # Errors
///
/// Returns [`AppError::GitDetection`] if:
/// - `start_dir` is not inside a git repository
/// - no default fetch remote is configured
/// - the default fetch remote could not be read
/// - the default fetch remote has no URL
/// - the remote URL has no host component
pub(crate) fn detect_git_remote_from(start_dir: &Path) -> Result<GitRemote, AppError> {
    let repo = gix::discover(start_dir).map_err(|e| {
        tracing::debug!(%e, "git repository discovery failed");
        AppError::GitDetection {
            reason: format!("{NOT_A_GIT_REPOSITORY}: {e}"),
        }
    })?;

    let remote = repo
        .find_default_remote(Direction::Fetch)
        .ok_or_else(|| {
            tracing::debug!("no default fetch remote configured");
            AppError::GitDetection {
                reason: NO_FETCH_REMOTE.to_owned(),
            }
        })?
        .map_err(|e| {
            tracing::debug!(%e, "failed to read default fetch remote");
            AppError::GitDetection {
                reason: format!("{FETCH_REMOTE_UNREADABLE}: {e}"),
            }
        })?;

    let url = remote.url(Direction::Fetch).ok_or_else(|| {
        tracing::debug!("fetch remote has no URL");
        AppError::GitDetection {
            reason: FETCH_REMOTE_NO_URL.to_owned(),
        }
    })?;

    let host = url.host().ok_or_else(|| {
        tracing::debug!(path = %url.path, "remote URL has no host component");
        AppError::GitDetection {
            reason: REMOTE_URL_MISSING_HOST.to_owned(),
        }
    })?;

    let path = url.path.to_string();
    let port = url.port;

    Ok(GitRemote::from_parts(host, &path, port))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    /// Creates a temporary git repository with an origin fetch remote.
    fn init_temp_repo_with_remote(remote_url: &str) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("should create tempdir");
        gix::init(tmp.path()).expect("gix::init should succeed");

        let config_path = tmp.path().join(".git/config");
        let existing = fs::read_to_string(&config_path).unwrap_or_default();
        let remote_section = format!(
            "\n[remote \"origin\"]\n\turl = {remote_url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        );
        fs::write(&config_path, existing + &remote_section).expect("should write git config");

        tmp
    }

    #[test]
    fn test_detect_git_remote_from_repo_with_origin() {
        let tmp = init_temp_repo_with_remote("git@github.com:acme/widgets.git");
        let remote = detect_git_remote_from(tmp.path()).expect("should detect remote");
        assert_eq!(remote.as_str(), "github.com/acme/widgets");
    }

    #[test]
    fn test_detect_git_remote_from_non_repo() {
        let tmp = tempfile::tempdir().expect("should create tempdir");
        let err = detect_git_remote_from(tmp.path()).unwrap_err();
        assert!(
            err.to_string().contains(NOT_A_GIT_REPOSITORY),
            "unexpected error: {err}",
        );
    }
}
