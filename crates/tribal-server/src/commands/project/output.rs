//! Terminal output for `tribal project` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (IDs, MCP snippets) to
//! stdout. The register-side helpers take explicit writers so callers
//! that compose multiple commands (`bootstrap`) can redirect output to
//! `io::sink` and assemble their own polished presentation.

use std::io::{self, Write};

use tribal_domain::Project;

use crate::output::snippet_key;

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

/// Reports the resolved git remote to the stderr-equivalent writer.
pub(super) fn git_remote_resolved(out: &mut dyn Write, remote: &str) {
    write_line(out, &format!("  git remote: {remote}"));
}

/// Reports a successful registration or existing project to the
/// stderr-equivalent writer.
pub(super) fn registered(out: &mut dyn Write, project: &Project, already_existed: bool) {
    let msg = if already_existed {
        PROJECT_ALREADY_EXISTS
    } else {
        PROJECT_REGISTERED
    };
    write_line(
        out,
        &format!("  {msg}: {} ({})", project.name(), project.id()),
    );
}

/// Writes the bare project ID to the stdout-equivalent writer.
///
/// Emitted as the first stdout line so scripted consumers can capture
/// it via `tribal project register | head -1` — write failures
/// propagate so a broken pipe surfaces rather than dropping the ID
/// silently.
pub(super) fn project_id(out: &mut dyn Write, project: &Project) -> io::Result<()> {
    try_write_line(out, &project.id().to_string())
}

/// Writes the wrapped MCP configuration snippet to the
/// stdout-equivalent writer.
///
/// Includes the `mcpServers` wrapper and server key for human
/// readability when copy-pasting. The caller pre-builds the inner
/// `entry` so it can also be stashed in a `RegisterOutcome` without
/// double-rendering.
pub(super) fn mcp_snippet(
    out: &mut dyn Write,
    project: &Project,
    entry: &serde_json::Value,
) -> io::Result<()> {
    let key = snippet_key(project.git_remote());
    let wrapped = serde_json::json!({
        "mcpServers": {
            (key): entry,
        }
    });
    let rendered = serde_json::to_string_pretty(&wrapped).expect("JSON serialisation cannot fail");
    try_write_line(out, &rendered)
}

/// Writes the bare MCP server entry to the stdout-equivalent writer
/// for piping into tools like `claude mcp add-json <name> <json>`.
///
/// No `mcpServers` wrapper, no project ID, no stderr output.
pub(super) fn json_snippet(out: &mut dyn Write, entry: &serde_json::Value) -> io::Result<()> {
    let rendered = serde_json::to_string_pretty(entry).expect("JSON serialisation cannot fail");
    try_write_line(out, &rendered)
}

// ---------------------------------------------------------------------------
// Writer helpers
// ---------------------------------------------------------------------------

/// Writes `line` followed by a newline, swallowing IO errors.
///
/// Suitable for progress lines where loss of output is not a correctness
/// issue. For output the caller cannot afford to lose (e.g. the project
/// ID feeding a downstream pipe), use [`try_write_line`].
fn write_line(out: &mut dyn Write, line: &str) {
    let _ = try_write_line(out, line);
}

/// Writes `line` followed by a newline, returning any IO failure.
///
/// Both flavours route through the same code path so that the eventual
/// move to a CLI design system has a single point of substitution.
fn try_write_line(out: &mut dyn Write, line: &str) -> io::Result<()> {
    writeln!(out, "{line}")?;
    out.flush()
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
