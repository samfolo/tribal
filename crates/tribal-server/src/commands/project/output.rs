//! Terminal output for `tribal project` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (IDs, MCP snippets) to
//! stdout.

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

/// Minimum width for the ID column in the project table.
const MIN_COL_WIDTH_ID: usize = 2;

/// Minimum width for the Name column in the project table.
const MIN_COL_WIDTH_NAME: usize = 4;

/// Minimum width for the Git Remote column in the project table.
const MIN_COL_WIDTH_GIT_REMOTE: usize = 10;

/// Minimum width for the Default Branch column in the project table.
const MIN_COL_WIDTH_DEFAULT_BRANCH: usize = 14;

/// Number of spaces between table columns.
const COL_GAP: usize = 2;

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

/// Prints the MCP configuration snippet to stdout.
///
/// Uses the convention `tribal@namespace/repo` as the server key,
/// where `namespace/repo` is the git remote path portion.
pub(super) fn mcp_snippet(project: &Project) {
    let snippet = build_mcp_snippet(project);
    println!(
        "{}",
        serde_json::to_string_pretty(&snippet).expect("JSON serialisation cannot fail"),
    );
}

/// Builds the MCP server configuration JSON for a project.
///
/// The key follows the `tribal@namespace/repo` convention.
fn build_mcp_snippet(project: &Project) -> serde_json::Value {
    let key = format!("tribal@{}", project.git_remote().path());
    serde_json::json!({
        "mcpServers": {
            (key): {
                "command": "tribal",
                "args": ["serve", "--project", project.id().to_string()]
            }
        }
    })
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
        "{:<id_w$}  {:<name_w$}  {:<remote_w$}  {:<branch_w$}",
        "ID",
        "Name",
        "Git Remote",
        "Default Branch",
        id_w = id_width,
        name_w = name_width,
        remote_w = remote_width,
        branch_w = branch_width,
    );

    let total_width = id_width + name_width + remote_width + branch_width + (COL_GAP * 3);
    println!("{}", "-".repeat(total_width));

    for project in projects {
        println!(
            "{:<id_w$}  {:<name_w$}  {:<remote_w$}  {:<branch_w$}",
            project.id(),
            project.name(),
            project.git_remote(),
            project.default_branch(),
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

    // -- MCP snippet ---------------------------------------------------------

    #[test]
    fn test_build_mcp_snippet_structure() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .build();

        let snippet = build_mcp_snippet(&project);
        let servers = &snippet["mcpServers"];

        assert!(
            servers["tribal@acme/widgets"].is_object(),
            "expected key 'tribal@acme/widgets', got: {servers}",
        );

        let entry = &servers["tribal@acme/widgets"];
        assert_eq!(entry["command"], "tribal");

        let args = entry["args"].as_array().expect("args should be an array");
        assert_eq!(args[0], "serve");
        assert_eq!(args[1], "--project");

        let project_arg = args[2].as_str().expect("project arg should be a string");
        let _: ProjectId = project_arg
            .parse()
            .expect("project arg should be a valid ProjectId");
    }

    #[test]
    fn test_build_mcp_snippet_preserves_slashes_in_key() {
        let project = a_project()
            .git_remote(GitRemote::from_parts("gitlab.com", "org/sub/repo", None))
            .build();

        let snippet = build_mcp_snippet(&project);
        let servers = &snippet["mcpServers"];

        assert!(
            servers["tribal@org/sub/repo"].is_object(),
            "slashes in key must be preserved, got: {servers}",
        );
    }
}
