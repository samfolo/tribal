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
    match handoff.config_file {
        ConfigFileOutcome::Written { path } => {
            try_write_line(
                out,
                &format!("  config file: written to {}", path.display()),
            )?;
        }
        ConfigFileOutcome::AlreadyExists { path, warnings } => {
            try_write_line(
                out,
                &format!("  config file: already exists at {}", path.display()),
            )?;
            for warning in warnings {
                try_write_line(out, &format!("  warning: {warning}"))?;
            }
        }
    }
    Ok(())
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
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tribal_config::{
        Auth, CliOverrides, DEFAULT_BIND_ADDRESS, EmbeddingCliOverrides, ProviderKind,
    };
    use tribal_domain::Project;
    use tribal_test_utils::{a_project, assert_json_snapshot, assert_text_snapshot};
    use uuid::Uuid;

    use super::*;
    use crate::output::build_snippet_entry;

    /// Inputs that swing between snapshot cases.
    struct Case<'a> {
        transport: TransportKind,
        persistence: ConfigPersistence<'a>,
        config_file: ConfigFileOutcome,
    }

    fn fixture_bearer_token() -> BearerToken {
        "test-bearer-token"
            .parse()
            .expect("fixture token parses as BearerToken")
    }

    fn fixture_principal_id() -> PrincipalId {
        PrincipalId::from(Uuid::from_u128(1))
    }

    fn fixture_config_path() -> PathBuf {
        PathBuf::from("/etc/tribal/tribal.yaml")
    }

    fn fixture_overrides() -> CliOverrides {
        CliOverrides {
            embedding: Some(EmbeddingCliOverrides {
                provider: Some(ProviderKind::OpenAi),
                model: Some("text-embedding-3-small".into()),
            }),
            ..CliOverrides::default()
        }
    }

    /// Sample divergence warning used in `AlreadyExists` fixtures so the
    /// warning-rendering path is exercised.
    const SAMPLE_WARNING: &str = "database.url differs from the resolved value";

    /// Builds the same `Project` value used to drive every render in
    /// the suite. Pinning `id` and `git_remote` keeps the snapshots
    /// deterministic.
    fn fixture_project() -> Project {
        a_project()
            .id(ProjectId::from(Uuid::from_u128(2)))
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .name("widgets".to_owned())
            .build()
    }

    /// Drives the production builder against a fixture project so the
    /// stderr/JSON renders consume the exact shape callers see in
    /// production.
    fn fixture_mcp_entry(transport: TransportKind, auth: Option<&Auth>) -> Value {
        let project = fixture_project();
        let path = fixture_config_path();
        build_snippet_entry(
            project.id(),
            transport,
            auth,
            &path,
            &format!("http://{DEFAULT_BIND_ADDRESS}/mcp"),
        )
    }

    /// `ConfigFileOutcome::Written` fixture.
    fn fixture_written() -> ConfigFileOutcome {
        ConfigFileOutcome::Written {
            path: fixture_config_path(),
        }
    }

    /// `ConfigFileOutcome::AlreadyExists` fixture carrying a sample
    /// divergence warning.
    fn fixture_already_exists_with_warning() -> ConfigFileOutcome {
        ConfigFileOutcome::AlreadyExists {
            path: fixture_config_path(),
            warnings: vec![SAMPLE_WARNING.to_owned()],
        }
    }

    fn render_stderr(case: &Case<'_>) -> String {
        let bearer = fixture_bearer_token();
        let project = fixture_project();
        let auth = Auth::Bearer {
            token: bearer.clone(),
        };
        let mcp_entry = fixture_mcp_entry(case.transport, Some(&auth));
        let handoff = Handoff {
            bearer_token: &bearer,
            principal_key: "principal:local",
            principal_id: fixture_principal_id(),
            project_id: project.id(),
            project_name: project.name(),
            git_remote: project.git_remote(),
            transport: case.transport,
            mcp_entry: &mcp_entry,
            config_file: &case.config_file,
            persistence: case.persistence,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_human(&mut buf, &handoff).expect("write_human succeeds");
        String::from_utf8(buf).expect("utf8")
    }

    fn render_json(transport: TransportKind) -> serde_json::Value {
        let bearer = fixture_bearer_token();
        let project = fixture_project();
        let auth = Auth::Bearer {
            token: bearer.clone(),
        };
        let mcp_entry = fixture_mcp_entry(transport, Some(&auth));
        let config_file = fixture_written();
        let handoff = Handoff {
            bearer_token: &bearer,
            principal_key: "principal:local",
            principal_id: fixture_principal_id(),
            project_id: project.id(),
            project_name: project.name(),
            git_remote: project.git_remote(),
            transport,
            mcp_entry: &mcp_entry,
            config_file: &config_file,
            persistence: ConfigPersistence::Minimal,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_json(&mut buf, &handoff).expect("write_json succeeds");
        let captured = String::from_utf8(buf).expect("utf8");
        serde_json::from_str(&captured).expect("output is valid JSON")
    }

    // -- Stderr × stdio -------------------------------------------------------

    #[test]
    fn test_stderr_stdio_first_run_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_stdio_file_exists_minimal_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_already_exists_with_warning(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-file-exists-minimal.txt"
        );
    }

    #[test]
    fn test_stderr_stdio_file_exists_persisted_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_already_exists_with_warning(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-file-exists-persisted.txt"
        );
    }

    // -- Stderr × http --------------------------------------------------------

    #[test]
    fn test_stderr_http_first_run_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_http_file_exists_minimal_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_already_exists_with_warning(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-file-exists-minimal.txt"
        );
    }

    #[test]
    fn test_stderr_http_file_exists_persisted_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_already_exists_with_warning(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-file-exists-persisted.txt"
        );
    }

    // -- JSON -----------------------------------------------------------------

    #[test]
    fn test_json_stdio_matches_snapshot() {
        let payload = render_json(TransportKind::Stdio);
        assert_json_snapshot!(&payload, "src/commands/bootstrap/snapshots/json-stdio.json");
    }

    #[test]
    fn test_json_http_matches_snapshot() {
        let payload = render_json(TransportKind::Http);
        assert_json_snapshot!(&payload, "src/commands/bootstrap/snapshots/json-http.json");
    }
}
