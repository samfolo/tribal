//! Terminal output for `tribal project` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (IDs, MCP snippets) to
//! stdout.

use tribal_config::{DEFAULT_BIND_ADDRESS, TransportKind};
use tribal_domain::Project;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Message when a project is successfully registered.
pub(super) const PROJECT_REGISTERED: &str = "project registered";

/// Message when the project already exists.
pub(super) const PROJECT_ALREADY_EXISTS: &str = "project already registered";

/// Message when no projects exist.
pub(super) const NO_PROJECTS: &str = "no projects registered";

/// Error when HTTP/SSE transport is selected but no token is available
/// and `--skip-validation` is not set.
pub(super) const TOKEN_REQUIRED: &str = "bearer token required for http/sse snippet; pass --token, set TRIBAL_AUTH_TOKEN, or use --skip-validation to omit";

/// Error when a provided token fails validation.
pub(super) const TOKEN_INVALID: &str = "token validation failed";

/// Minimum width for the ID column in the project table.
const MIN_COL_WIDTH_ID: usize = 2;

/// Minimum width for the Name column in the project table.
const MIN_COL_WIDTH_NAME: usize = 4;

/// Minimum width for the Git Remote column in the project table.
const MIN_COL_WIDTH_GIT_REMOTE: usize = 10;

/// Minimum width for the Default Branch column in the project table.
const MIN_COL_WIDTH_DEFAULT_BRANCH: usize = 14;

/// Spacing between table columns, used in format strings as literal spaces
/// and in separator width calculation.
const COL_SEPARATOR: &str = "  ";

// ---------------------------------------------------------------------------
// Register output
// ---------------------------------------------------------------------------

/// Reports the resolved git remote to stderr.
pub(super) fn git_remote_resolved(remote: &str) {
    eprintln!("  git remote: {remote}");
}

/// Reports a successful registration or existing project to stderr.
pub(super) fn registered(project: &Project, already_existed: bool) {
    let msg = if already_existed {
        PROJECT_ALREADY_EXISTS
    } else {
        PROJECT_REGISTERED
    };
    eprintln!("  {msg}: {} ({})", project.name(), project.id());
}

/// Prints the bare project ID to stdout.
///
/// Emitted as the first stdout line so that scripted consumers can
/// capture it via `tribal project register | head -1`.
pub(super) fn project_id(project: &Project) {
    println!("{}", project.id());
}

/// Prints the wrapped MCP configuration snippet to stderr.
///
/// Includes the `mcpServers` wrapper and server key for human
/// readability when copy-pasting.
pub(super) fn mcp_snippet(
    project: &Project,
    transport: TransportKind,
    token: Option<&str>,
    bind_address: Option<&str>,
) {
    let entry = build_snippet_entry(project, transport, token, bind_address);
    let key = snippet_key(project);
    let wrapped = serde_json::json!({
        "mcpServers": {
            (key): entry,
        }
    });
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&wrapped).expect("JSON serialisation cannot fail"),
    );
}

/// Prints the bare MCP server entry to stdout for piping into tools
/// like `claude mcp add-json <name> <json>`.
///
/// No `mcpServers` wrapper, no project ID, no stderr output.
pub(super) fn json_snippet(
    project: &Project,
    transport: TransportKind,
    token: Option<&str>,
    bind_address: Option<&str>,
) {
    let entry = build_snippet_entry(project, transport, token, bind_address);
    println!(
        "{}",
        serde_json::to_string_pretty(&entry).expect("JSON serialisation cannot fail"),
    );
}

/// Builds the MCP server entry for a project.
///
/// The shape varies by transport: stdio uses `command`/`args`, while
/// HTTP and SSE use `url` with optional `headers`.
fn build_snippet_entry(
    project: &Project,
    transport: TransportKind,
    token: Option<&str>,
    bind_address: Option<&str>,
) -> serde_json::Value {
    match transport {
        TransportKind::Stdio => build_stdio_entry(project),
        TransportKind::Http | TransportKind::Sse => build_network_entry(token, bind_address),
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
fn build_network_entry(token: Option<&str>, bind_address: Option<&str>) -> serde_json::Value {
    let addr = bind_address.unwrap_or(DEFAULT_BIND_ADDRESS);
    let url = format!("http://{addr}/mcp");

    let mut entry = serde_json::json!({ "url": url });

    if let Some(tok) = token {
        entry["headers"] = serde_json::json!({
            "Authorization": format!("Bearer {tok}"),
        });
    }

    entry
}

/// The MCP server key: `tribal@namespace/repo`.
fn snippet_key(project: &Project) -> String {
    format!("tribal@{}", project.git_remote().path())
}

// ---------------------------------------------------------------------------
// List output
// ---------------------------------------------------------------------------

/// Prints the project table to stdout, or an empty-state message to stderr.
///
/// Columns: ID, Name, Git Remote, Default Branch. Column widths are
/// calculated dynamically from content.
pub(super) fn project_table(projects: &[Project]) {
    if projects.is_empty() {
        eprintln!("{NO_PROJECTS}");
        return;
    }

    let id_width = projects
        .iter()
        .map(|p| p.id().to_string().len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_ID);
    let name_width = projects
        .iter()
        .map(|p| p.name().len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_NAME);
    let remote_width = projects
        .iter()
        .map(|p| p.git_remote().as_str().len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_GIT_REMOTE);
    let branch_width = projects
        .iter()
        .map(|p| p.default_branch().len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_DEFAULT_BRANCH);

    println!(
        "{:<id_w$}{sep}{:<name_w$}{sep}{:<remote_w$}{sep}{:<branch_w$}",
        "ID",
        "Name",
        "Git Remote",
        "Default Branch",
        sep = COL_SEPARATOR,
        id_w = id_width,
        name_w = name_width,
        remote_w = remote_width,
        branch_w = branch_width,
    );

    let total_width =
        id_width + name_width + remote_width + branch_width + (COL_SEPARATOR.len() * 3);
    println!("{}", "-".repeat(total_width));

    for project in projects {
        println!(
            "{:<id_w$}{sep}{:<name_w$}{sep}{:<remote_w$}{sep}{:<branch_w$}",
            project.id(),
            project.name(),
            project.git_remote(),
            project.default_branch(),
            sep = COL_SEPARATOR,
            id_w = id_width,
            name_w = name_width,
            remote_w = remote_width,
            branch_w = branch_width,
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{GitRemote, ProjectId};
    use tribal_test_utils::a_project;

    use super::*;

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
    fn test_build_http_entry_with_token() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry =
            build_snippet_entry(&project, TransportKind::Http, Some("test-token-abc"), None);

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

        let entry = build_snippet_entry(
            &project,
            TransportKind::Http,
            Some("tok"),
            Some("10.0.0.1:9999"),
        );

        assert_eq!(entry["url"], "http://10.0.0.1:9999/mcp");
    }

    #[test]
    fn test_build_http_entry_without_token_omits_headers() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(&project, TransportKind::Http, None, None);
        assert!(entry.get("headers").is_none(), "no headers without token");
    }

    #[test]
    fn test_build_sse_entry_same_shape_as_http() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let entry = build_snippet_entry(&project, TransportKind::Sse, Some("tok"), None);

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
