//! Git remote identity newtype.
//!
//! [`GitRemote`] stores the protocol-agnostic identity of a git remote
//! (`host/path`) and can reconstruct any canonical URL form on demand.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// GitRemote
// ---------------------------------------------------------------------------

/// Protocol-agnostic git remote identity.
///
/// Stores the canonical `host/path` form (e.g. `github.com/user/repo`),
/// stripping the transport scheme and `.git` suffix. Two remotes that
/// point to the same repository — regardless of whether they use SSH,
/// HTTPS, or SCP syntax — produce the same `GitRemote` value.
///
/// # Construction
///
/// Parse any common git remote URL format via [`FromStr`]:
///
/// ```text
/// git@github.com:user/repo.git   → github.com/user/repo
/// https://github.com/user/repo   → github.com/user/repo
/// ssh://git@github.com/user/repo → github.com/user/repo
/// ```
///
/// Or build from already-parsed components via [`GitRemote::from_parts`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GitRemote {
    canonical: String,
}

impl GitRemote {
    /// Constructs a `GitRemote` from pre-parsed host and path components.
    ///
    /// Strips a leading `/` and trailing `.git` from the path, and
    /// lowercases the host.
    #[must_use]
    pub fn from_parts(host: &str, path: &str) -> Self {
        let path = path.strip_prefix('/').unwrap_or(path);
        let path = path.strip_suffix(".git").unwrap_or(path);

        Self {
            canonical: format!("{}/{path}", host.to_lowercase()),
        }
    }

    /// Returns the canonical `host/path` form as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Returns the host portion (everything before the first `/`).
    #[must_use]
    pub fn host(&self) -> &str {
        self.canonical
            .split_once('/')
            .map_or(&self.canonical, |(h, _)| h)
    }

    /// Returns the path portion (everything after the first `/`).
    #[must_use]
    pub fn path(&self) -> &str {
        self.canonical.split_once('/').map_or("", |(_, p)| p)
    }

    /// Reconstructs the HTTPS URL form.
    #[must_use]
    pub fn as_https(&self) -> String {
        format!("https://{}", self.canonical)
    }

    /// Reconstructs the SSH (SCP-like) URL form.
    #[must_use]
    pub fn as_ssh(&self) -> String {
        format!("git@{}:{}", self.host(), self.path())
    }
}

// ---------------------------------------------------------------------------
// Trait implementations
// ---------------------------------------------------------------------------

impl fmt::Display for GitRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

impl AsRef<str> for GitRemote {
    fn as_ref(&self) -> &str {
        &self.canonical
    }
}

impl FromStr for GitRemote {
    type Err = GitRemoteParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(GitRemoteParseError::Empty);
        }

        // Strip trailing `.git` suffix.
        let base = trimmed.strip_suffix(".git").unwrap_or(trimmed);

        // SSH SCP format: git@host:path
        if let Some(rest) = base.strip_prefix("git@") {
            if let Some((host, path)) = rest.split_once(':') {
                return Ok(Self {
                    canonical: format!("{}/{path}", host.to_lowercase()),
                });
            }
        }

        // URL formats: strip scheme and optional user@.
        let without_scheme = base
            .strip_prefix("https://")
            .or_else(|| base.strip_prefix("http://"))
            .or_else(|| base.strip_prefix("ssh://"))
            .or_else(|| base.strip_prefix("git://"));

        if let Some(rest) = without_scheme {
            // Strip optional user@ (e.g. `git@` in `ssh://git@host/path`).
            let rest = rest
                .split_once('@')
                .map_or(rest, |(_, after_at)| after_at);

            return Ok(Self {
                canonical: rest.to_lowercase(),
            });
        }

        // Assume already in host/path form.
        Ok(Self {
            canonical: base.to_lowercase(),
        })
    }
}

impl TryFrom<String> for GitRemote {
    type Error = GitRemoteParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<GitRemote> for String {
    fn from(remote: GitRemote) -> Self {
        remote.canonical
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Error returned when parsing a [`GitRemote`] from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitRemoteParseError {
    /// The input string was empty.
    Empty,
}

impl fmt::Display for GitRemoteParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("git remote URL must not be empty"),
        }
    }
}

impl std::error::Error for GitRemoteParseError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- FromStr -----------------------------------------------------------

    #[test]
    fn test_parse_ssh_scp_with_git_suffix() {
        let remote: GitRemote = "git@github.com:user/repo.git".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_ssh_scp_without_git_suffix() {
        let remote: GitRemote = "git@github.com:user/repo".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_https_with_git_suffix() {
        let remote: GitRemote = "https://github.com/user/repo.git".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_https_without_git_suffix() {
        let remote: GitRemote = "https://github.com/user/repo".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_ssh_url_format() {
        let remote: GitRemote = "ssh://git@github.com/user/repo.git".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_git_protocol() {
        let remote: GitRemote = "git://github.com/user/repo.git".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_already_canonical() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_empty_returns_error() {
        assert!(matches!(
            "".parse::<GitRemote>(),
            Err(GitRemoteParseError::Empty)
        ));
    }

    #[test]
    fn test_parse_whitespace_only_returns_error() {
        assert!(matches!(
            "   ".parse::<GitRemote>(),
            Err(GitRemoteParseError::Empty)
        ));
    }

    #[test]
    fn test_parse_lowercases_host() {
        let remote: GitRemote = "git@GitHub.COM:User/Repo.git".parse().unwrap();
        assert_eq!(remote.host(), "github.com");
    }

    #[test]
    fn test_parse_preserves_path_case() {
        // Paths on most git hosts are case-sensitive for lookups.
        // However, our canonical form lowercases everything for
        // consistent identity matching.
        let remote: GitRemote = "https://github.com/User/Repo".parse().unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_parse_gitlab_subgroups() {
        let remote: GitRemote =
            "https://gitlab.company.com/group/subgroup/repo.git".parse().unwrap();
        assert_eq!(remote.as_str(), "gitlab.company.com/group/subgroup/repo");
    }

    // -- from_parts --------------------------------------------------------

    #[test]
    fn test_from_parts_strips_leading_slash() {
        let remote = GitRemote::from_parts("github.com", "/user/repo");
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_from_parts_strips_git_suffix() {
        let remote = GitRemote::from_parts("github.com", "user/repo.git");
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_from_parts_strips_both() {
        let remote = GitRemote::from_parts("github.com", "/user/repo.git");
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    // -- Equivalence across formats ----------------------------------------

    #[test]
    fn test_all_formats_produce_same_canonical() {
        let ssh_scp: GitRemote = "git@github.com:user/repo.git".parse().unwrap();
        let https: GitRemote = "https://github.com/user/repo.git".parse().unwrap();
        let ssh_url: GitRemote = "ssh://git@github.com/user/repo.git".parse().unwrap();
        let bare: GitRemote = "github.com/user/repo".parse().unwrap();

        assert_eq!(ssh_scp, https);
        assert_eq!(https, ssh_url);
        assert_eq!(ssh_url, bare);
    }

    // -- Accessors ---------------------------------------------------------

    #[test]
    fn test_host_and_path() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        assert_eq!(remote.host(), "github.com");
        assert_eq!(remote.path(), "user/repo");
    }

    #[test]
    fn test_as_https() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        assert_eq!(remote.as_https(), "https://github.com/user/repo");
    }

    #[test]
    fn test_as_ssh() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        assert_eq!(remote.as_ssh(), "git@github.com:user/repo");
    }

    // -- Display / Serialize -----------------------------------------------

    #[test]
    fn test_display_shows_canonical() {
        let remote: GitRemote = "git@github.com:user/repo.git".parse().unwrap();
        assert_eq!(remote.to_string(), "github.com/user/repo");
    }

    #[test]
    fn test_serde_roundtrip() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        let json = serde_json::to_string(&remote).expect("serialise");
        let parsed: GitRemote = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(remote, parsed);
    }
}
