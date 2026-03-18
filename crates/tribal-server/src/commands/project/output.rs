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

/// Prints the project ID to stdout.
pub(super) fn project_id(project: &Project) {
    println!("{}", project.id());
}

/// Prints the MCP configuration snippet to stdout.
///
/// Uses the convention `tribal@namespace/repo` as the server key,
/// where `namespace/repo` is the git remote path portion.
pub(super) fn mcp_snippet(project: &Project) {
    let key = format!("tribal@{}", project.git_remote().path());
    let snippet = serde_json::json!({
        "mcpServers": {
            (key): {
                "command": "tribal",
                "args": ["serve", "--project", project.id().to_string()]
            }
        }
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&snippet).expect("JSON serialisation cannot fail"),
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
