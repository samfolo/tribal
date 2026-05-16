//! Builders for MCP server-config JSON entries.

use std::path::Path;

use tribal_config::{
    Auth, DEFAULT_BIND_ADDRESS, ENV_PUBLIC_MCP_URL, TransportKind, TribalConfig,
};
use tribal_domain::Project;

/// Builds the MCP server entry for a project.
///
/// The shape varies by transport: stdio uses `command`/`args` (with an
/// absolute `--config` flag pinning the harness-spawned process to the
/// same config the human used); HTTP and SSE use `url` with optional
/// `headers`. Both shapes include a `"type"` discriminator so consumers
/// can dispatch without inspecting which keys are present.
///
/// `auth` is dispatched per [`Auth`] variant — new variants force the
/// inner builders to update via exhaustive match rather than silently
/// falling through to a "bearer-shaped" assumption.
pub(crate) fn build_snippet_entry(
    project: &Project,
    transport: TransportKind,
    auth: Option<&Auth>,
    config_path: &Path,
    advertised_url: &str,
) -> serde_json::Value {
    match transport {
        TransportKind::Stdio => build_stdio_entry(project, config_path),
        TransportKind::Http | TransportKind::Sse => {
            build_network_entry(transport, auth, advertised_url)
        }
    }
}

/// Builds a stdio-transport MCP server entry, propagating the resolved
/// absolute `config_path` so a harness-spawned `tribal serve` reads the
/// same config the human used.
fn build_stdio_entry(project: &Project, config_path: &Path) -> serde_json::Value {
    serde_json::json!({
        "type": "stdio",
        "command": "tribal",
        "args": [
            "--config",
            config_path.display().to_string(),
            "serve",
            "--project",
            project.id().to_string(),
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
pub(crate) fn snippet_key(project: &Project) -> String {
    format!("tribal@{}", project.git_remote().path())
}

/// Resolves the URL clients should reach for HTTP/SSE transports.
///
/// `TRIBAL_PUBLIC_MCP_URL` (when set and non-empty) takes precedence so
/// deployments behind a reverse proxy can advertise the public URL.
/// Otherwise falls back to `http://<bind_address>/mcp`.
pub(crate) fn resolved_advertised_url(config: &TribalConfig) -> String {
    std::env::var(ENV_PUBLIC_MCP_URL)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
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
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use std::path::PathBuf;

    use figment::Jail;
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

    // -- Stdio snippet --------------------------------------------------------

    #[test]
    fn test_build_stdio_entry_emits_type_discriminator() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(
            &project,
            TransportKind::Stdio,
            None,
            &config_path(),
            &default_advertised_url(),
        );
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

        let entry = build_snippet_entry(
            &project,
            TransportKind::Stdio,
            None,
            &custom,
            &default_advertised_url(),
        );
        let args = entry["args"].as_array().expect("args array");
        assert_eq!(args[1], "/var/lib/tribal/alt.yaml");
    }

    // -- Network snippet ------------------------------------------------------

    #[test]
    fn test_build_http_entry_with_bearer_token() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("test-token-abc");

        let entry = build_snippet_entry(
            &project,
            TransportKind::Http,
            Some(&auth),
            &config_path(),
            &default_advertised_url(),
        );

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
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("tok");
        let url = "https://tribal.example.com/mcp";

        let entry = build_snippet_entry(
            &project,
            TransportKind::Http,
            Some(&auth),
            &config_path(),
            url,
        );

        assert_eq!(entry["url"], url);
    }

    #[test]
    fn test_build_http_entry_without_auth_omits_headers() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(
            &project,
            TransportKind::Http,
            None,
            &config_path(),
            &default_advertised_url(),
        );
        assert_eq!(entry["type"], "http");
        assert!(entry.get("headers").is_none(), "no headers without auth");
    }

    #[test]
    fn test_build_sse_entry_emits_sse_type() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("tok");

        let entry = build_snippet_entry(
            &project,
            TransportKind::Sse,
            Some(&auth),
            &config_path(),
            &default_advertised_url(),
        );

        assert_eq!(entry["type"], "sse");
        assert_eq!(entry["url"], default_advertised_url());
        assert_eq!(entry["headers"]["Authorization"], "Bearer tok");
    }

    // -- Snippet key ----------------------------------------------------------

    #[test]
    fn test_snippet_key_preserves_slashes() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("gitlab.com", "org/sub/repo", None))
            .build();

        assert_eq!(snippet_key(&project), "tribal@org/sub/repo");
    }

    // -- Advertised URL resolution -------------------------------------------

    #[test]
    fn test_resolved_advertised_url_falls_back_to_bind_address() {
        Jail::expect_with(|jail| {
            jail.clear_env();

            let config = TribalConfig::minimum_valid("postgres://x/y");
            let url = resolved_advertised_url(&config);
            assert_eq!(url, format!("http://{DEFAULT_BIND_ADDRESS}/mcp"));
            Ok(())
        });
    }

    #[test]
    fn test_resolved_advertised_url_honours_env_override() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(ENV_PUBLIC_MCP_URL, "https://tribal.example.com/mcp");

            let config = TribalConfig::minimum_valid("postgres://x/y");
            let url = resolved_advertised_url(&config);
            assert_eq!(url, "https://tribal.example.com/mcp");
            Ok(())
        });
    }

    #[test]
    fn test_resolved_advertised_url_ignores_empty_env() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(ENV_PUBLIC_MCP_URL, "   ");

            let config = TribalConfig::minimum_valid("postgres://x/y");
            let url = resolved_advertised_url(&config);
            assert_eq!(url, format!("http://{DEFAULT_BIND_ADDRESS}/mcp"));
            Ok(())
        });
    }
}
