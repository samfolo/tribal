//! Builders for MCP server-config JSON entries.

use std::path::Path;

use tribal_config::{Auth, DEFAULT_BIND_ADDRESS, TransportKind, TribalConfig};
use tribal_domain::{GitRemote, ProjectId};

/// Inputs for a single MCP server-config entry, partitioned by transport.
///
/// The project rides only with stdio: stdio embeds `serve --project <id>`
/// as its spawn command, so the `Stdio` variant carries the project and
/// the config path. The network transports render a url-plus-auth entry
/// whose project is bound server-side via `serve --project`, so they
/// carry no project. Pairing a project with a network transport is
/// therefore unrepresentable.
pub(crate) enum McpSnippet<'a> {
    Stdio {
        project_id: ProjectId,
        config_path: &'a Path,
    },
    Http {
        auth: Option<&'a Auth>,
        advertised_url: &'a str,
    },
    Sse {
        auth: Option<&'a Auth>,
        advertised_url: &'a str,
    },
}

/// Builds the MCP server entry JSON for a snippet.
///
/// stdio uses `command`/`args` (with an absolute `--config` flag pinning
/// the harness-spawned process to the same config the human used); the
/// network transports use `url` with optional `headers`. Every shape
/// includes a `"type"` discriminator so consumers can dispatch without
/// inspecting which keys are present.
///
/// `auth` is dispatched per [`Auth`] variant, so new variants force the
/// inner builders to update via exhaustive match rather than silently
/// falling through to a "bearer-shaped" assumption.
pub(crate) fn build_snippet_entry(snippet: &McpSnippet<'_>) -> serde_json::Value {
    match snippet {
        McpSnippet::Stdio {
            project_id,
            config_path,
        } => build_stdio_entry(*project_id, config_path),
        McpSnippet::Http {
            auth,
            advertised_url,
        } => build_network_entry(TransportKind::Http, *auth, advertised_url),
        McpSnippet::Sse {
            auth,
            advertised_url,
        } => build_network_entry(TransportKind::Sse, *auth, advertised_url),
    }
}

/// Builds a stdio-transport MCP server entry, propagating the resolved
/// absolute `config_path` so a harness-spawned `tribal serve` reads the
/// same config the human used.
fn build_stdio_entry(project_id: ProjectId, config_path: &Path) -> serde_json::Value {
    serde_json::json!({
        "type": "stdio",
        "command": "tribal",
        "args": [
            "--config",
            config_path.display().to_string(),
            "serve",
            "--project",
            project_id.to_string(),
        ],
    })
}

/// Builds an HTTP or SSE transport MCP server entry.
///
/// The project ID is not included — it is configured on the server
/// side via `tribal serve --project <id>`, not in the client config.
fn build_network_entry(
    transport: TransportKind,
    auth: Option<&Auth>,
    advertised_url: &str,
) -> serde_json::Value {
    let mut entry = serde_json::json!({
        "type": transport.to_string(),
        "url": advertised_url,
    });

    if let Some(auth) = auth {
        let (header_name, header_value) = match auth {
            Auth::Bearer { token } => ("Authorization", format!("Bearer {}", token.as_str())),
        };
        entry["headers"] = serde_json::json!({ (header_name): header_value });
    }

    entry
}

/// The MCP server key: `tribal@namespace/repo`.
pub(crate) fn snippet_key(git_remote: &GitRemote) -> String {
    format!("tribal@{}", git_remote.path())
}

/// Resolves the URL clients should reach for HTTP/SSE transports.
///
/// A configured public URL (the reverse-proxy case) takes precedence;
/// otherwise this falls back to `http://<bind_address>/mcp`.
pub(crate) fn resolved_advertised_url(config: &TribalConfig) -> String {
    config.server.public_mcp_url.clone().unwrap_or_else(|| {
        let addr = config
            .server
            .bind_address
            .as_deref()
            .unwrap_or(DEFAULT_BIND_ADDRESS);
        format!("http://{addr}/mcp")
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tribal_domain::{GitRemote, ProjectId};
    use tribal_test_utils::a_project;

    use super::*;

    /// Constructs a bearer-auth value around `token` for test fixtures.
    fn bearer(token: &str) -> Auth {
        Auth::Bearer {
            token: token.parse().expect("test token parses as BearerToken"),
        }
    }

    /// A representative absolute config path for fixtures.
    fn config_path() -> PathBuf {
        PathBuf::from("/etc/tribal/tribal.yaml")
    }

    /// A representative advertised URL for fixtures.
    fn default_advertised_url() -> String {
        format!("http://{DEFAULT_BIND_ADDRESS}/mcp")
    }

    /// Renders a stdio entry from the given project and config path.
    fn stdio(project_id: ProjectId, config_path: &Path) -> serde_json::Value {
        build_snippet_entry(&McpSnippet::Stdio {
            project_id,
            config_path,
        })
    }

    /// Renders an http entry from the given auth and advertised URL.
    fn http(auth: Option<&Auth>, advertised_url: &str) -> serde_json::Value {
        build_snippet_entry(&McpSnippet::Http {
            auth,
            advertised_url,
        })
    }

    /// Renders an sse entry from the given auth and advertised URL.
    fn sse(auth: Option<&Auth>, advertised_url: &str) -> serde_json::Value {
        build_snippet_entry(&McpSnippet::Sse {
            auth,
            advertised_url,
        })
    }

    // -- Stdio snippet --------------------------------------------------------

    #[test]
    fn test_build_stdio_entry_emits_type_discriminator() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = stdio(project.id(), &config_path());
        assert_eq!(entry["type"], "stdio");
        assert_eq!(entry["command"], "tribal");

        let args = entry["args"].as_array().expect("args should be an array");
        assert_eq!(args[0], "--config");
        assert_eq!(args[1], "/etc/tribal/tribal.yaml");
        assert_eq!(args[2], "serve");
        assert_eq!(args[3], "--project");

        let project_arg = args[4].as_str().expect("project arg should be a string");
        let _: ProjectId = project_arg
            .parse()
            .expect("project arg should be a valid ProjectId");
    }

    #[test]
    fn test_build_stdio_entry_propagates_custom_config_path() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let custom = PathBuf::from("/var/lib/tribal/alt.yaml");

        let entry = stdio(project.id(), &custom);
        let args = entry["args"].as_array().expect("args array");
        assert_eq!(args[1], "/var/lib/tribal/alt.yaml");
    }

    // -- Network snippet ------------------------------------------------------

    #[test]
    fn test_build_http_entry_with_bearer_token() {
        let auth = bearer("test-token-abc");

        let entry = http(Some(&auth), &default_advertised_url());

        assert_eq!(entry["type"], "http");
        assert_eq!(entry["url"], default_advertised_url());
        assert_eq!(entry["headers"]["Authorization"], "Bearer test-token-abc");
        assert!(
            entry.get("command").is_none(),
            "network entry must not have command"
        );
    }

    #[test]
    fn test_build_http_entry_uses_advertised_url() {
        let auth = bearer("tok");
        let url = "https://tribal.example.com/mcp";

        let entry = http(Some(&auth), url);

        assert_eq!(entry["url"], url);
    }

    #[test]
    fn test_build_http_entry_without_auth_omits_headers() {
        let entry = http(None, &default_advertised_url());
        assert_eq!(entry["type"], "http");
        assert!(entry.get("headers").is_none(), "no headers without auth");
    }

    #[test]
    fn test_build_sse_entry_emits_sse_type() {
        let auth = bearer("tok");

        let entry = sse(Some(&auth), &default_advertised_url());

        assert_eq!(entry["type"], "sse");
        assert_eq!(entry["url"], default_advertised_url());
        assert_eq!(entry["headers"]["Authorization"], "Bearer tok");
    }

    // -- Snippet key ----------------------------------------------------------

    #[test]
    fn test_snippet_key_preserves_slashes() {
        let remote = GitRemote::from_parts("gitlab.com", "org/sub/repo", None);
        assert_eq!(snippet_key(&remote), "tribal@org/sub/repo");
    }

    // -- Advertised URL resolution -------------------------------------------

    #[test]
    fn test_resolved_advertised_url_falls_back_to_bind_address() {
        let config = TribalConfig::minimum_valid("postgres://x/y");
        let url = resolved_advertised_url(&config);
        assert_eq!(url, format!("http://{DEFAULT_BIND_ADDRESS}/mcp"));
    }

    #[test]
    fn test_resolved_advertised_url_honours_public_mcp_url() {
        let mut config = TribalConfig::minimum_valid("postgres://x/y");
        config.server.public_mcp_url = Some("https://tribal.example.com/mcp".into());
        let url = resolved_advertised_url(&config);
        assert_eq!(url, "https://tribal.example.com/mcp");
    }
}
