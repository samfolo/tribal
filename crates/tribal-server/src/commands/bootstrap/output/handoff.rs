//! Top-level bootstrap hand-off: data shape, view component, and the
//! two write-out entry points (`--json` to stdout, polished form to
//! stderr).

use std::io::{self, Write};

use serde_json::Value;
use tribal_config::{ConfigPersistence, TransportKind};
use tribal_domain::{BearerToken, GitRemote, PrincipalId, ProjectId};
use tribal_ui::{Component, Header, RenderCtx, Theme};

use super::{
    action_list::{ActionInputs, ActionList, http_sse_steps, stdio_steps},
    status_block::StatusBlock,
    token_block::{HttpSseTokenBlock, StdioTokenBlock},
};
use crate::commands::setup::ConfigFileOutcome;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Title of the polished hand-off block.
const TITLE: &str = "Bootstrap complete.";

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of everything the bootstrap renderers need.
pub(in crate::commands::bootstrap) struct Handoff<'a> {
    pub bearer_token: &'a BearerToken,
    pub principal_key: &'a str,
    pub principal_id: PrincipalId,
    pub project_id: ProjectId,
    pub project_name: &'a str,
    pub git_remote: &'a GitRemote,
    pub transport: TransportKind,
    pub mcp_entry: &'a Value,
    pub config_file: &'a ConfigFileOutcome,
    pub persistence: ConfigPersistence<'a>,
    pub advertised_url: &'a str,
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

/// Component view over a [`Handoff`]: title, status block, then the
/// per-transport body (action list and bearer-token block).
struct HandoffView<'a> {
    handoff: &'a Handoff<'a>,
}

impl Component for HandoffView<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let h = self.handoff;

        Header::new(TITLE).render(ctx)?;
        writeln!(ctx)?;
        writeln!(ctx)?;

        StatusBlock {
            project_id: h.project_id,
            project_name: h.project_name,
            git_remote: h.git_remote,
            transport: h.transport,
            advertised_url: h.advertised_url,
        }
        .render(ctx)?;
        writeln!(ctx)?;

        let inputs = ActionInputs {
            bearer_token: h.bearer_token,
            transport: h.transport,
            project_id: h.project_id,
            config_path: h.config_file.path().display().to_string(),
            config_file: h.config_file,
            persistence: h.persistence,
        };

        match h.transport {
            TransportKind::Stdio => {
                let steps = stdio_steps(&inputs);
                ActionList { steps: &steps }.render(ctx)?;
                writeln!(ctx)?;
                writeln!(ctx)?;
                StdioTokenBlock {
                    token: h.bearer_token,
                }
                .render(ctx)?;
            }
            TransportKind::Http | TransportKind::Sse => {
                HttpSseTokenBlock {
                    token: h.bearer_token,
                }
                .render(ctx)?;
                writeln!(ctx)?;
                writeln!(ctx)?;
                let steps = http_sse_steps(&inputs);
                ActionList { steps: &steps }.render(ctx)?;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// Writes the bootstrap result as a single JSON object on stdout.
pub(in crate::commands::bootstrap) fn write_json(
    out: &mut dyn Write,
    handoff: &Handoff<'_>,
) -> io::Result<()> {
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
    writeln!(out, "{rendered}")?;
    out.flush()
}

// ---------------------------------------------------------------------------
// Human output
// ---------------------------------------------------------------------------

/// Writes the bootstrap result as a polished hand-off on stderr.
///
/// SGR escapes from themed components pass straight through to `out`.
/// Callers that need plain text (snapshot tests, redirected output)
/// wrap `out` with `AutoStream::never` at their boundary.
pub(in crate::commands::bootstrap) fn write_human(
    out: &mut dyn Write,
    theme: &Theme,
    handoff: &Handoff<'_>,
) -> io::Result<()> {
    let view = HandoffView { handoff };
    let mut ctx = RenderCtx::new(out, theme);
    view.render(&mut ctx)?;
    ctx.flush()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anstream::{AutoStream, ColorChoice};
    use tribal_config::{
        Auth, CliOverrides, DEFAULT_BIND_ADDRESS, EmbeddingCliOverrides, InferenceCliOverrides,
        InferenceStageCliOverrides, ProviderKind, TelemetryCliOverrides,
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

    fn fixture_advertised_url() -> String {
        format!("http://{DEFAULT_BIND_ADDRESS}/mcp")
    }

    /// Persistable-flag fixture exercising every family: embedding,
    /// inference (one stage), and telemetry. Keeps the persistence
    /// snapshots representative without ballooning to all nine flags.
    fn fixture_overrides() -> CliOverrides {
        CliOverrides {
            embedding: Some(EmbeddingCliOverrides {
                provider: Some(ProviderKind::OpenAi),
                model: Some("text-embedding-3-small".into()),
            }),
            inference: Some(InferenceCliOverrides {
                extraction: Some(InferenceStageCliOverrides {
                    provider: Some(ProviderKind::OpenAi),
                    model: Some("gpt-4o-mini".into()),
                }),
                triage: None,
                relation: None,
            }),
            telemetry: Some(TelemetryCliOverrides {
                otlp_endpoint: Some("http://localhost:4317".into()),
            }),
            ..CliOverrides::default()
        }
    }

    /// Sample divergence warning used in `AlreadyExists` fixtures so
    /// the warning-rendering path is exercised.
    const SAMPLE_WARNING: &str = "database.url differs from the resolved value";

    fn fixture_project() -> Project {
        a_project()
            .id(ProjectId::from(Uuid::from_u128(2)))
            .git_remote(GitRemote::from_parts("github.com", "acme/widgets", None))
            .name("widgets".to_owned())
            .build()
    }

    fn fixture_mcp_entry(transport: TransportKind, auth: Option<&Auth>) -> Value {
        let project = fixture_project();
        let path = fixture_config_path();
        build_snippet_entry(
            project.id(),
            transport,
            auth,
            &path,
            &fixture_advertised_url(),
        )
    }

    fn fixture_written() -> ConfigFileOutcome {
        ConfigFileOutcome::Written {
            path: fixture_config_path(),
        }
    }

    fn fixture_already_exists() -> ConfigFileOutcome {
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
        let advertised_url = fixture_advertised_url();
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
            advertised_url: &advertised_url,
        };
        let theme = Theme::default_dark();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = AutoStream::new(&mut buf, ColorChoice::Never);
            write_human(&mut writer, &theme, &handoff).expect("write_human succeeds");
        }
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
        let advertised_url = fixture_advertised_url();
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
            advertised_url: &advertised_url,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_json(&mut buf, &handoff).expect("write_json succeeds");
        let captured = String::from_utf8(buf).expect("utf8");
        serde_json::from_str(&captured).expect("output is valid JSON")
    }

    // -- Stderr × stdio (4 cases) ---------------------------------------------

    #[test]
    fn test_stderr_stdio_no_flags_first_run_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-no-flags-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_stdio_no_flags_file_exists_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_already_exists(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-no-flags-file-exists.txt"
        );
    }

    #[test]
    fn test_stderr_stdio_flags_first_run_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-flags-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_stdio_flags_file_exists_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Stdio,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_already_exists(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-stdio-flags-file-exists.txt"
        );
    }

    // -- Stderr × http (4 cases) ----------------------------------------------

    #[test]
    fn test_stderr_http_no_flags_first_run_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-no-flags-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_http_no_flags_file_exists_matches_snapshot() {
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Minimal,
            config_file: fixture_already_exists(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-no-flags-file-exists.txt"
        );
    }

    #[test]
    fn test_stderr_http_flags_first_run_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_written(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-flags-first-run.txt"
        );
    }

    #[test]
    fn test_stderr_http_flags_file_exists_matches_snapshot() {
        let overrides = fixture_overrides();
        let captured = render_stderr(&Case {
            transport: TransportKind::Http,
            persistence: ConfigPersistence::Persisted(&overrides),
            config_file: fixture_already_exists(),
        });
        assert_text_snapshot!(
            &captured,
            "src/commands/bootstrap/snapshots/stderr-http-flags-file-exists.txt"
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
