//! Terminal output for the `tribal bootstrap` command.
//!
//! Two presentation modes: a single JSON object on stdout (`--json`),
//! and a polished hand-off on stderr (default). The mode is fixed at
//! call time; the renderer never touches the other stream.

use std::io::{self, Write};

use serde_json::Value;
use tribal_config::TransportKind;
use tribal_domain::{BearerToken, GitRemote, PrincipalId, ProjectId};

use crate::output::snippet_key;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Heading for the post-bootstrap action list.
const NEXT_STEPS_HEADING: &str = "Next steps:";

/// Heading for the bearer-token stash-for-later block (stdio only).
const SAVE_TOKEN_HEADING: &str = "Save this token (it will not be shown again):";

/// Heading for the inline MCP server entry.
const MCP_ENTRY_HEADING: &str = "MCP server entry:";

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of everything the bootstrap renderers need.
///
/// Flat by design: the renderer is the last consumer in the chain, so
/// it reads straight from the resolved values rather than holding a
/// nested outcome graph.
pub(super) struct Handoff<'a> {
    pub(super) bearer_token: &'a BearerToken,
    pub(super) principal_key: &'a str,
    pub(super) principal_id: PrincipalId,
    pub(super) project_id: ProjectId,
    pub(super) project_name: &'a str,
    pub(super) git_remote: &'a GitRemote,
    pub(super) transport: TransportKind,
    pub(super) mcp_config: &'a Value,
    pub(super) config_path: &'a str,
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
        "mcp_config": handoff.mcp_config,
        "config_path": handoff.config_path,
    });
    let rendered = serde_json::to_string_pretty(&value).expect("JSON serialisation cannot fail");
    try_write_line(out, &rendered)
}

// ---------------------------------------------------------------------------
// Human output
// ---------------------------------------------------------------------------

/// Writes the bootstrap result as a polished hand-off on stderr.
///
/// Status lines come first, then a transport-dependent action list,
/// then the MCP entry. For stdio the bearer token appears as a
/// stash-for-later block since it is not part of the wire-up snippet;
/// for http/sse it is surfaced inside the action list because the user
/// needs to export it before starting the server.
pub(super) fn write_human(out: &mut dyn Write, handoff: &Handoff<'_>) -> io::Result<()> {
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
    try_write_line(out, &format!("  config file: {}", handoff.config_path))
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
                    handoff.config_path, handoff.transport, handoff.project_id,
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
    let snippet =
        serde_json::to_string(handoff.mcp_config).expect("JSON serialisation cannot fail");
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
            snippet_key(handoff.git_remote): handoff.mcp_config,
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
