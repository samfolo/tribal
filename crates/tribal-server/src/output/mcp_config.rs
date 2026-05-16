//! Builders for MCP server-config JSON entries.

use tribal_config::{Auth, DEFAULT_BIND_ADDRESS, TransportKind};
use tribal_domain::Project;

/// Builds the MCP server entry for a project.
///
/// The shape varies by transport: stdio uses `command`/`args`, while
/// HTTP and SSE use `url` with optional `headers`. The optional `auth`
/// is dispatched per [`Auth`] variant — new variants force this builder
/// to update via exhaustive match rather than silently falling through
/// to a "bearer-shaped" assumption.
pub(crate) fn build_snippet_entry(
    project: &Project,
    transport: TransportKind,
    auth: Option<&Auth>,
    bind_address: Option<&str>,
) -> serde_json::Value {
    match transport {
        TransportKind::Stdio => build_stdio_entry(project),
        TransportKind::Http | TransportKind::Sse => build_network_entry(auth, bind_address),
    }
}

/// Builds a stdio-transport MCP server entry.
fn build_stdio_entry(project: &Project) -> serde_json::Value {
    serde_json::json!({
        "command": "tribal",
        "args": ["serve", "--project", project.id().to_string()]
    })
}

/// Builds an HTTP or SSE transport MCP server entry.
///
/// The project ID is not included — it is configured on the server
/// side via `tribal serve --project <id>`, not in the client config.
fn build_network_entry(auth: Option<&Auth>, bind_address: Option<&str>) -> serde_json::Value {
    let addr = bind_address.unwrap_or(DEFAULT_BIND_ADDRESS);
    let url = format!("http://{addr}/mcp");

    let mut entry = serde_json::json!({ "url": url });

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{GitRemote, ProjectId};
    use tribal_test_utils::a_project;

    use super::*;

    /// Constructs a bearer-auth value around `token` for test fixtures.
    fn bearer(token: &str) -> Auth {
        Auth::Bearer {
            token: token.parse().expect("test token parses as BearerToken"),
        }
    }

    // -- Stdio snippet --------------------------------------------------------

    #[test]
    fn test_build_stdio_entry_structure() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(&project, TransportKind::Stdio, None, None);
        assert_eq!(entry["command"], "tribal");

        let args = entry["args"].as_array().expect("args should be an array");
        assert_eq!(args[0], "serve");
        assert_eq!(args[1], "--project");

        let project_arg = args[2].as_str().expect("project arg should be a string");
        let _: ProjectId = project_arg
            .parse()
            .expect("project arg should be a valid ProjectId");
    }

    // -- Network snippet ------------------------------------------------------

    #[test]
    fn test_build_http_entry_with_bearer_token() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("test-token-abc");

        let entry = build_snippet_entry(&project, TransportKind::Http, Some(&auth), None);

        assert_eq!(entry["url"], format!("http://{DEFAULT_BIND_ADDRESS}/mcp"));
        assert_eq!(entry["headers"]["Authorization"], "Bearer test-token-abc");
        assert!(
            entry.get("command").is_none(),
            "network entry must not have command"
        );
    }

    #[test]
    fn test_build_http_entry_custom_bind_address() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("tok");

        let entry = build_snippet_entry(
            &project,
            TransportKind::Http,
            Some(&auth),
            Some("10.0.0.1:9999"),
        );

        assert_eq!(entry["url"], "http://10.0.0.1:9999/mcp");
    }

    #[test]
    fn test_build_http_entry_without_auth_omits_headers() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(&project, TransportKind::Http, None, None);
        assert!(entry.get("headers").is_none(), "no headers without auth");
    }

    #[test]
    fn test_build_sse_entry_same_shape_as_http() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();
        let auth = bearer("tok");

        let entry = build_snippet_entry(&project, TransportKind::Sse, Some(&auth), None);

        assert_eq!(entry["url"], format!("http://{DEFAULT_BIND_ADDRESS}/mcp"));
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
}
