//! Terminal output for the `tribal bootstrap` command.
//!
//! Two presentation modes: a single JSON object on stdout (`--json`),
//! and a polished hand-off on stderr (default). The mode is fixed at
//! call time; the renderer never touches the other stream.

use std::io::{self, Write};

use serde_json::Value;
use tribal_config::{ConfigPersistence, TransportKind};
use tribal_domain::{BearerToken, GitRemote, PrincipalId, ProjectId};

use crate::{commands::setup::ConfigFileOutcome, output::snippet_key};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Heading for the post-bootstrap action list.
const NEXT_STEPS_HEADING: &str = "Next steps:";

/// Heading for the bearer-token stash-for-later block (stdio only).
const SAVE_TOKEN_HEADING: &str = "Save this token (it will not be shown again):";

/// Heading for the inline MCP server entry.
const MCP_ENTRY_HEADING: &str = "MCP server entry:";

/// Prefix of the warning emitted when an existing config file blocks
/// flag persistence. The full literal is composed with the resolved
/// path as a runtime substitution.
const FLAG_PERSISTENCE_BLOCKED_PREFIX: &str = "warning: tribal.yaml already exists at ";

/// Suffix of [`FLAG_PERSISTENCE_BLOCKED_PREFIX`].
const FLAG_PERSISTENCE_BLOCKED_SUFFIX: &str = "; supplied flags were not persisted. Edit the file or remove it and re-run to pin the resolved values.";

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of everything the bootstrap renderers need.
pub(super) struct Handoff<'a> {
    pub(super) bearer_token: &'a BearerToken,
    pub(super) principal_key: &'a str,
    pub(super) principal_id: PrincipalId,
    pub(super) project_id: ProjectId,
    pub(super) project_name: &'a str,
    pub(super) git_remote: &'a GitRemote,
    pub(super) transport: TransportKind,
    pub(super) mcp_entry: &'a Value,
    pub(super) config_file: &'a ConfigFileOutcome,
    pub(super) persistence: ConfigPersistence<'a>,
}

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// Writes the bootstrap result as a single JSON object on stdout.
pub(super) fn write_json(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
    let value = serde_json::json!({
        "bearer_token": handoff.bearer_token.as_str(),
        "principal_key": handoff.principal_key,
        "principal_id": handoff.principal_id.to_string(),
        "project_id": handoff.project_id.to_string(),
        "project_name": handoff.project_name,
        "git_remote": handoff.git_remote.as_str(),
        "transport": handoff.transport.to_string(),
        "mcp_config": handoff.mcp_entry,
        "config_path": handoff.config_file.path().display().to_string(),
    });
    let rendered = serde_json::to_string_pretty(&value).expect("JSON serialisation cannot fail");
    try_write_line(out, &rendered)
}

// ---------------------------------------------------------------------------
// Human output
// ---------------------------------------------------------------------------

/// Writes the bootstrap result as a polished hand-off on stderr.
///
/// An action-required warning leads when an existing config file
/// blocked flag persistence. Status lines, the transport-dependent
/// action list, and the MCP entry follow. For stdio the bearer token
/// closes with a stash-for-later block; for http/sse the token appears
/// inside the action list since the user needs to export it before
/// starting the server.
pub(super) fn write_human(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
    match handoff.persistence {
        ConfigPersistence::Persisted(_) => match handoff.config_file {
            ConfigFileOutcome::AlreadyExists { path, .. } => {
                try_write_line(
                    out,
                    &format!(
                        "{FLAG_PERSISTENCE_BLOCKED_PREFIX}{}{FLAG_PERSISTENCE_BLOCKED_SUFFIX}",
                        path.display(),
                    ),
                )?;
                try_write_line(out, "")?;
            }
            ConfigFileOutcome::Written { .. } => {}
        },
        ConfigPersistence::Minimal => {}
    }

    write_status_lines(out, handoff)?;
    try_write_line(out, "")?;
    write_action_list(out, handoff)?;
    try_write_line(out, "")?;
    write_mcp_entry(out, handoff)?;
    if matches!(handoff.transport, TransportKind::Stdio) {
        try_write_line(out, "")?;
        write_save_token_block(out, handoff.bearer_token)?;
    }
    Ok(())
}

fn write_status_lines(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
    try_write_line(out, "  setup complete")?;
    try_write_line(out, &format!("  principal: {}", handoff.principal_key))?;
    try_write_line(
        out,
        &format!(
            "  project registered: {} ({})",
            handoff.project_name, handoff.project_id,
        ),
    )?;
    let config_status = match handoff.config_file {
        ConfigFileOutcome::Written { path } => {
            format!("  config file: written to {}", path.display())
        }
        ConfigFileOutcome::AlreadyExists { path, .. } => {
            format!("  config file: already exists at {}", path.display())
        }
    };
    try_write_line(out, &config_status)
}

fn write_action_list(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
    try_write_line(out, NEXT_STEPS_HEADING)?;
    let mut step = 1u32;
    match handoff.transport {
        TransportKind::Stdio => {
            // stdio harnesses spawn `tribal serve` themselves with the
            // resolved config and project — no manual server start, no
            // token export.
        }
        TransportKind::Http | TransportKind::Sse => {
            try_write_line(out, &format!("  {step}. Export the bearer token:"))?;
            try_write_line(
                out,
                &format!(
                    "       export TRIBAL_AUTH_TOKEN=\"{}\"",
                    handoff.bearer_token.as_str(),
                ),
            )?;
            try_write_line(out, "")?;
            step += 1;
            try_write_line(out, &format!("  {step}. Start the server:"))?;
            try_write_line(
                out,
                &format!(
                    "       tribal --config {} serve --transport {} --project {}",
                    handoff.config_file.path().display(),
                    handoff.transport,
                    handoff.project_id,
                ),
            )?;
            try_write_line(out, "")?;
            step += 1;
        }
    }
    try_write_line(out, &format!("  {step}. Verify the install:"))?;
    try_write_line(out, "       tribal check")?;
    try_write_line(out, "       tribal check --providers")?;
    try_write_line(out, "")?;
    step += 1;
    let snippet = serde_json::to_string(handoff.mcp_entry).expect("JSON serialisation cannot fail");
    try_write_line(out, &format!("  {step}. Wire up your MCP harness:"))?;
    try_write_line(
        out,
        &format!(
            "       claude mcp add-json {} '{}'",
            snippet_key(handoff.git_remote),
            snippet,
        ),
    )
}

fn write_mcp_entry(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
    try_write_line(out, MCP_ENTRY_HEADING)?;
    try_write_line(out, "")?;
    let wrapped = serde_json::json!({
        "mcpServers": {
            snippet_key(handoff.git_remote): handoff.mcp_entry,
        }
    });
    let rendered = serde_json::to_string_pretty(&wrapped).expect("JSON serialisation cannot fail");
    try_write_line(out, &rendered)
}

fn write_save_token_block(out: &mut dyn Write, token: &BearerToken) -> io::Result<()> {
    try_write_line(out, SAVE_TOKEN_HEADING)?;
    try_write_line(out, "")?;
    try_write_line(out, &format!("  {}", token.as_str()))
}

// ---------------------------------------------------------------------------
// Writer helpers
// ---------------------------------------------------------------------------

/// Writes `line` followed by a newline, returning any IO failure.
///
/// Bootstrap propagates write errors: the token and MCP entry must
/// reach the user, so a broken pipe surfaces rather than dropping
/// silently.
fn try_write_line(out: &mut dyn Write, line: &str) -> io::Result<()> {
    writeln!(out, "{line}")?;
    out.flush()
}
