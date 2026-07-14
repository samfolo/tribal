//! Git remote identity newtype.
//!
//! [`GitRemote`] stores the protocol-agnostic identity of a git remote
//! (`host/path`) and can reconstruct any canonical URL form on demand.

use std::{fmt, str::FromStr};

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
/// Non-standard ports are preserved in the canonical form as
/// `host:port/path` since different ports may serve different content.
///
/// All major git hosts (GitHub, GitLab, Bitbucket, Gitea) treat
/// organisation and repository names as case-insensitive, so the
/// canonical form is fully lowercased.
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(try_from = "String", into = "String")]
pub struct GitRemote {
    canonical: String,
}

impl GitRemote {
    /// Constructs a `GitRemote` from pre-parsed host, path, and optional
    /// port components.
    ///
    /// Strips a leading `/` and trailing `.git` from the path, and
    /// lowercases both host and path. Default ports (22, 80, 443, 9418)
    /// are stripped; non-standard ports are preserved.
    #[must_use]
    pub fn from_parts(host: &str, path: &str, port: Option<u16>) -> Self {
        let path = path.strip_prefix('/').unwrap_or(path);
        let path = path.strip_suffix(".git").unwrap_or(path);
        let path = path.to_lowercase();
        let host = host.to_lowercase();

        let canonical = match port {
            Some(p) if !is_default_port(p) => format!("{host}:{p}/{path}"),
            _ => format!("{host}/{path}"),
        };

        Self { canonical }
    }

    /// Returns the canonical `host/path` form as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Returns the host portion (everything before the first `/`),
    /// potentially including a non-standard port suffix.
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

    /// Reconstructs the HTTPS URL form (with `.git` suffix).
    #[must_use]
    pub fn as_https(&self) -> String {
        format!("https://{}.git", self.canonical)
    }

    /// Reconstructs the SSH URL form (with `.git` suffix).
    ///
    /// For standard-port remotes, produces SCP-like syntax:
    /// `git@host:path.git`. For non-standard ports, produces full URL
    /// syntax: `ssh://git@host:port/path.git` — SCP-like syntax has no
    /// port field per the git specification.
    #[must_use]
    pub fn as_ssh(&self) -> String {
        let host = self.host();
        let path = self.path();

        if let Some((hostname, port)) = host.rsplit_once(':') {
            format!("ssh://git@{hostname}:{port}/{path}.git")
        } else {
            format!("git@{host}:{path}.git")
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` for port numbers that are default for git transports.
const fn is_default_port(port: u16) -> bool {
    matches!(port, 22 | 443 | 80 | 9418)
}

/// Strips a default port suffix (`:22`, `:443`, `:80`, `:9418`) from
/// a `host:port` string, returning just the host. Non-default ports
/// are preserved.
fn strip_default_port(host_port: &str) -> &str {
    if let Some((host, port_str)) = host_port.rsplit_once(':')
        && let Ok(port) = port_str.parse::<u16>()
        && is_default_port(port)
    {
        return host;
    }
    host_port
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

        // SSH SCP format: [user@]host:path (no slashes before colon).
        // Per the git specification, the colon is the path separator in
        // SCP syntax — there is no port field. We detect SCP by the
        // presence of `user@` followed by `host:path` with no `://`.
        if let Some((user_host, path)) = base.split_once(':')
            && !base.contains("://")
            && let Some((_user, host)) = user_host.split_once('@')
        {
            return Ok(Self {
                canonical: format!("{}/{}", host.to_lowercase(), path.to_lowercase()),
            });
        }

        // URL formats: strip scheme and optional user@.
        let without_scheme = base
            .strip_prefix("https://")
            .or_else(|| base.strip_prefix("http://"))
            .or_else(|| base.strip_prefix("ssh://"))
            .or_else(|| base.strip_prefix("git://"));

        if let Some(rest) = without_scheme {
            // Strip optional user@ (e.g. `git@` in `ssh://git@host/path`).
            let rest = rest.split_once('@').map_or(rest, |(_, after_at)| after_at);

            // Strip default ports from the host portion.
            if let Some((host_port, path)) = rest.split_once('/') {
                let host = strip_default_port(host_port);
                return Ok(Self {
                    canonical: format!("{}/{}", host.to_lowercase(), path.to_lowercase()),
                });
            }

            return Err(GitRemoteParseError::InvalidFormat {
                reason: "missing path component",
            });
        }

        // Assume already in host/path form.
        let canonical = base.to_lowercase();
        if canonical.contains(' ') {
            return Err(GitRemoteParseError::InvalidFormat {
                reason: "contains whitespace",
            });
        }
        if !canonical.contains('/') {
            return Err(GitRemoteParseError::InvalidFormat {
                reason: "missing path component",
            });
        }
        Ok(Self { canonical })
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
    /// The input could not be parsed as a valid git remote URL.
    InvalidFormat {
        /// Description of why the input was rejected.
        reason: &'static str,
    },
}

impl fmt::Display for GitRemoteParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "invalid git remote URL: input is empty"),
            Self::InvalidFormat { reason } => write!(f, "invalid git remote URL: {reason}"),
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

    // -- Normalisation -------------------------------------------------------

    /// All these formats must normalise to `github.com/user/repo`.
    const STANDARD_INPUTS: &[&str] = &[
        // SSH SCP syntax (git@ user)
        "git@github.com:user/repo.git",
        "git@github.com:user/repo",
        // SSH SCP syntax (arbitrary user)
        "deploy@github.com:user/repo.git",
        // HTTPS
        "https://github.com/user/repo.git",
        "https://github.com/user/repo",
        // SSH URL syntax
        "ssh://git@github.com/user/repo.git",
        // Git protocol
        "git://github.com/user/repo.git",
        // HTTP
        "http://github.com/user/repo.git",
        // Already canonical
        "github.com/user/repo",
        // Case normalisation
        "git@GitHub.COM:User/Repo.git",
        "https://GitHub.COM/User/Repo",
        // Default ports (stripped)
        "https://github.com:443/user/repo.git",
        "ssh://git@github.com:22/user/repo.git",
        "git://github.com:9418/user/repo.git",
        "http://github.com:80/user/repo.git",
    ];

    /// Inputs whose canonical form differs from the standard
    /// `github.com/user/repo`.
    const OUTLIER_CASES: &[(&str, &str)] = &[
        // Subgroups
        (
            "https://gitlab.company.com/group/subgroup/repo.git",
            "gitlab.company.com/group/subgroup/repo",
        ),
        // Non-standard port (preserved)
        (
            "https://gitlab.company.com:8443/group/repo.git",
            "gitlab.company.com:8443/group/repo",
        ),
    ];

    const EXPECTED_CANONICAL: &str = "github.com/user/repo";

    #[test]
    fn test_standard_normalisation() {
        for input in STANDARD_INPUTS {
            let remote: GitRemote = input
                .parse()
                .unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"));
            assert_eq!(
                remote.as_str(),
                EXPECTED_CANONICAL,
                "normalisation mismatch for input {input:?}",
            );
        }
    }

    #[test]
    fn test_outlier_normalisation() {
        for (input, expected) in OUTLIER_CASES {
            let remote: GitRemote = input
                .parse()
                .unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"));
            assert_eq!(
                remote.as_str(),
                *expected,
                "normalisation mismatch for input {input:?}",
            );
        }
    }

    // -- Error cases -------------------------------------------------------

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
    fn test_parse_host_only_returns_error() {
        assert!(matches!(
            "github.com".parse::<GitRemote>(),
            Err(GitRemoteParseError::InvalidFormat { .. })
        ));
    }

    #[test]
    fn test_parse_contains_whitespace_returns_error() {
        assert!(matches!(
            "not a url".parse::<GitRemote>(),
            Err(GitRemoteParseError::InvalidFormat { .. })
        ));
    }

    // -- from_parts --------------------------------------------------------

    #[test]
    fn test_from_parts_strips_leading_slash_and_git_suffix() {
        let paths: &[&str] = &["/user/repo", "user/repo.git", "/user/repo.git", "user/repo"];

        for path in paths {
            let remote = GitRemote::from_parts("github.com", path, None);
            assert_eq!(
                remote.as_str(),
                EXPECTED_CANONICAL,
                "from_parts(\"github.com\", {path:?}, None)",
            );
        }
    }

    #[test]
    fn test_from_parts_lowercases_path() {
        let remote = GitRemote::from_parts("GitHub.COM", "User/Repo.git", None);
        assert_eq!(remote.as_str(), EXPECTED_CANONICAL);
    }

    #[test]
    fn test_from_parts_port_handling() {
        let non_standard =
            GitRemote::from_parts("gitlab.company.com", "/group/repo.git", Some(8443));
        assert_eq!(non_standard.as_str(), "gitlab.company.com:8443/group/repo");

        let default = GitRemote::from_parts("github.com", "/user/repo.git", Some(22));
        assert_eq!(default.as_str(), "github.com/user/repo");

        let none = GitRemote::from_parts("github.com", "/user/repo.git", None);
        assert_eq!(none.as_str(), "github.com/user/repo");
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

    #[test]
    fn test_default_ports_equivalent_to_no_port() {
        let with_port: GitRemote = "ssh://git@github.com:22/user/repo.git".parse().unwrap();
        let without_port: GitRemote = "ssh://git@github.com/user/repo.git".parse().unwrap();
        assert_eq!(with_port, without_port);
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
        assert_eq!(remote.as_https(), "https://github.com/user/repo.git");
    }

    #[test]
    fn test_as_ssh() {
        let remote: GitRemote = "github.com/user/repo".parse().unwrap();
        assert_eq!(remote.as_ssh(), "git@github.com:user/repo.git");
    }

    #[test]
    fn test_as_ssh_non_standard_port() {
        let remote: GitRemote = "gitlab.company.com:8443/group/repo".parse().unwrap();
        assert_eq!(
            remote.as_ssh(),
            "ssh://git@gitlab.company.com:8443/group/repo.git",
        );
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

    #[test]
    fn test_serde_deserialises_raw_url() {
        let json = "\"git@github.com:user/repo.git\"";
        let remote: GitRemote = serde_json::from_str(json).expect("deserialise");
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }
}
