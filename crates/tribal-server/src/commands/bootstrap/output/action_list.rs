//! Numbered action list rendered after the status block.

use std::io::{self, Write};

use tribal_config::{ConfigPersistence, ENV_AUTH_TOKEN, TransportKind, env_var_for_path};
use tribal_domain::{BearerToken, ProjectId};
use tribal_ui::{Component, OrderedList, RenderCtx, Text};

use crate::{cli::PersistableFlag, commands::setup::ConfigFileOutcome};

// ---------------------------------------------------------------------------
// List wrapper
// ---------------------------------------------------------------------------

/// Numbered "Next steps:" block: heading line, blank line, then the
/// numbered list of [`ActionStep`]s. Renders nothing when `steps` is
/// empty so the caller does not need to guard the call site.
pub(super) struct ActionList<'a> {
    pub steps: &'a [ActionStep],
}

impl Component for ActionList<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        if self.steps.is_empty() {
            return Ok(());
        }
        let style = ctx.theme().typography.emphasis;
        Text::new("Next steps:").with_style(style).renderln(ctx)?;
        writeln!(ctx)?;
        OrderedList::new(self.steps).render(ctx)
    }
}

// ---------------------------------------------------------------------------
// Action step
// ---------------------------------------------------------------------------

/// One numbered step in the bootstrap hand-off action list.
///
/// Renders the heading on the first line and each body line below.
/// The enclosing [`OrderedList`] provides the marker prefix and the
/// continuation indent that lines body content up under the heading
/// column; per-line `extra_indent` shifts an individual line deeper
/// when the spec calls for a stand-out command or path.
pub(super) struct ActionStep {
    heading: String,
    body: Vec<BodyLine>,
}

/// One line of an action step's body.
pub(super) struct BodyLine {
    content: String,
    extra_indent: usize,
}

impl BodyLine {
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            extra_indent: 0,
        }
    }

    /// Shift this line `n` columns deeper than the body indent.
    #[must_use]
    pub fn indented_by(mut self, n: usize) -> Self {
        self.extra_indent = n;
        self
    }
}

impl ActionStep {
    pub fn new(heading: impl Into<String>, body: Vec<BodyLine>) -> Self {
        Self {
            heading: heading.into(),
            body,
        }
    }
}

impl Component for ActionStep {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        let theme = *ctx.theme();
        Text::new(&self.heading)
            .with_style(theme.typography.body)
            .render(ctx)?;
        for line in &self.body {
            writeln!(ctx)?;
            Text::new(line.content.as_str())
                .with_style(theme.typography.body)
                .with_pad_left(line.extra_indent)
                .render(ctx)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Step composition
// ---------------------------------------------------------------------------

/// Inputs the per-transport composers read when building their step
/// list. Bundles the strings/ids each step needs so the helpers stay
/// signature-light.
pub(super) struct ActionInputs<'a> {
    pub bearer_token: &'a BearerToken,
    pub transport: TransportKind,
    pub project_id: ProjectId,
    pub config_path: String,
    pub config_file: &'a ConfigFileOutcome,
    pub persistence: ConfigPersistence<'a>,
}

/// Builds the stdio variant's action list.
pub(super) fn stdio_steps(inputs: &ActionInputs<'_>) -> Vec<ActionStep> {
    let mut steps = Vec::new();
    if let Some(step) = persistence_step(inputs) {
        steps.push(step);
    }
    push_verify_steps(&mut steps);
    push_wire_up_step(&mut steps);
    push_skills_step(&mut steps);
    steps
}

/// Builds the http/sse variant's action list.
pub(super) fn http_sse_steps(inputs: &ActionInputs<'_>) -> Vec<ActionStep> {
    let mut steps = Vec::new();
    steps.push(export_token_step(inputs));
    steps.push(durable_transport_step(inputs));
    if let Some(step) = persistence_step(inputs) {
        steps.push(step);
    }
    steps.push(start_server_step(inputs));
    push_verify_steps(&mut steps);
    push_wire_up_step(&mut steps);
    push_skills_step(&mut steps);
    steps
}

// ---------------------------------------------------------------------------
// Step constructors
// ---------------------------------------------------------------------------

/// Extra indent applied to body lines the spec sets two columns deeper
/// than the body column — exports and file paths in the durable-
/// transport and persistence steps.
const DEEPER: usize = 2;

fn persistence_step(inputs: &ActionInputs<'_>) -> Option<ActionStep> {
    let ConfigPersistence::Persisted(overrides) = inputs.persistence else {
        return None;
    };
    let flags = PersistableFlag::from_cli_overrides(overrides);
    if flags.is_empty() {
        return None;
    }
    Some(match inputs.config_file {
        ConfigFileOutcome::Written { .. } => persistence_step_written(&inputs.config_path, &flags),
        ConfigFileOutcome::AlreadyExists { .. } => {
            persistence_step_already_exists(&inputs.config_path, &flags)
        }
    })
}

fn persistence_step_written(path: &str, flags: &[PersistableFlag]) -> ActionStep {
    let flag_list = flags
        .iter()
        .map(|f| format!("--{}", f.flag_name()))
        .collect::<Vec<_>>()
        .join(", ");
    ActionStep::new(
        format!("Persisted to {path}: {flag_list}."),
        vec![BodyLine::new(
            "Future invocations will read these from the config file.",
        )],
    )
}

fn persistence_step_already_exists(path: &str, flags: &[PersistableFlag]) -> ActionStep {
    let mut body = vec![BodyLine::new(format!(
        "Edit {path} or export the corresponding TRIBAL_* env vars:",
    ))];
    for flag in flags {
        body.push(BodyLine::new(format!("export {}=…", flag.env_var())).indented_by(DEEPER));
    }
    ActionStep::new(
        "These flag values were NOT persisted because the config file already exists.",
        body,
    )
}

fn export_token_step(inputs: &ActionInputs<'_>) -> ActionStep {
    ActionStep::new(
        "Export the token for your shell session:",
        vec![BodyLine::new(format!(
            "export {ENV_AUTH_TOKEN}=\"{}\"",
            inputs.bearer_token.as_str(),
        ))],
    )
}

fn durable_transport_step(inputs: &ActionInputs<'_>) -> ActionStep {
    // `--transport` is intentionally not in the persistable family
    // (see `cli::flags`), so derive the env var directly from the
    // config path through the same loader convention.
    let server_transport_env = env_var_for_path("server.transport");
    ActionStep::new(
        "Make this transport durable so `tribal check` and `tribal serve`",
        vec![
            BodyLine::new("pick it up without re-passing flags. Either set:"),
            BodyLine::new(format!(
                "export {server_transport_env}=\"{}\"",
                inputs.transport,
            ))
            .indented_by(DEEPER),
            BodyLine::new("for the current shell, or edit your config file:"),
            BodyLine::new(inputs.config_path.clone()).indented_by(DEEPER),
            BodyLine::new(format!(
                "to set `server.transport: {}` (and if you want a non-default",
                inputs.transport,
            )),
            BodyLine::new("bind, `server.bind_address`)."),
        ],
    )
}

fn start_server_step(inputs: &ActionInputs<'_>) -> ActionStep {
    ActionStep::new(
        "Start the server in another terminal (it is NOT running yet —",
        vec![
            BodyLine::new("bootstrap registers + mints a token but does not start serve):"),
            BodyLine::new(format!(
                "tribal --config {} serve --transport {} --project {}",
                inputs.config_path, inputs.transport, inputs.project_id,
            )),
            BodyLine::new("Leave it running. The MCP URL above is what your agent connects to."),
        ],
    )
}

fn push_verify_steps(steps: &mut Vec<ActionStep>) {
    steps.push(ActionStep::new(
        "Verify your installation is ready for use:",
        vec![BodyLine::new("tribal check")],
    ));
    steps.push(ActionStep::new(
        "(When ready for real ingest) verify providers:",
        vec![BodyLine::new("tribal check --providers")],
    ));
}

fn push_wire_up_step(steps: &mut Vec<ActionStep>) {
    steps.push(ActionStep::new(
        "Wire Tribal into your agent harness. For Claude Code:",
        vec![
            BodyLine::new("claude mcp add-json tribal \"$(tribal mcp-config)\"")
                .indented_by(DEEPER),
            BodyLine::new("For Codex / Gemini CLI / OpenCode, see the installation skill."),
        ],
    ));
}

fn push_skills_step(steps: &mut Vec<ActionStep>) {
    steps.push(ActionStep::new(
        "(Optional) Install Tribal skills into your agent:",
        vec![BodyLine::new("npx skills add samfolo/tribal-skills")],
    ));
}
