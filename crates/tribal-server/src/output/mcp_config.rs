//! Shared identities used by integration rendering and readiness.

use tribal_config::{DEFAULT_BIND_ADDRESS, TribalConfig};
use tribal_domain::GitRemote;

/// Stable MCP server key for one project remote.
pub(crate) fn snippet_key(git_remote: &GitRemote) -> String {
    format!("tribal@{}", git_remote.path())
}

/// Public URL clients should use for a network transport.
pub(crate) fn resolved_advertised_url(config: &TribalConfig) -> String {
    config.server.public_mcp_url.clone().unwrap_or_else(|| {
        let address = config
            .server
            .bind_address
            .as_deref()
            .unwrap_or(DEFAULT_BIND_ADDRESS);
        format!("http://{address}/mcp")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_key_preserves_remote_path() {
        let remote = GitRemote::from_parts("github.com", "org/sub/repo", None);
        assert_eq!(snippet_key(&remote), "tribal@org/sub/repo");
    }

    #[test]
    fn advertised_url_prefers_public_configuration() {
        let mut config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        config.server.public_mcp_url = Some("https://tribal.example/mcp".to_owned());
        assert_eq!(
            resolved_advertised_url(&config),
            "https://tribal.example/mcp"
        );
    }
}
