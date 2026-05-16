//! Terminal output for `tribal project` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (IDs, MCP snippets) to
//! stdout.

use tribal_config::{Auth, TransportKind};
use tribal_domain::Project;

use crate::output::{build_snippet_entry, snippet_key};

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
pub(super) const TOKEN_REQUIRED: &str =
    "bearer token required for http/sse snippet; pass --token or set TRIBAL_AUTH_TOKEN";

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

/// Prints the wrapped MCP configuration snippet to stdout.
///
/// Includes the `mcpServers` wrapper and server key for human
/// readability when copy-pasting.
pub(super) fn mcp_snippet(
    project: &Project,
    transport: TransportKind,
    auth: Option<&Auth>,
    bind_address: Option<&str>,
) {
    let entry = build_snippet_entry(project, transport, auth, bind_address);
    let key = snippet_key(project);
    let wrapped = serde_json::json!({
        "mcpServers": {
            (key): entry,
        }
    });
    println!(
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
    auth: Option<&Auth>,
    bind_address: Option<&str>,
) {
    let entry = build_snippet_entry(project, transport, auth, bind_address);
    println!(
        "{}",
        serde_json::to_string_pretty(&entry).expect("JSON serialisation cannot fail"),
    );
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

