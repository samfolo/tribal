//! Entry point and step-pipeline driver for `tribal check`.

use std::{
    io::{self, IsTerminal, Write},
    path::Path,
};

use anstream::AutoStream;
use strum::IntoEnumIterator;
use tribal_ui::{Mode, StreamThemeContext, Theme, resolve_mode};

use super::{
    checks::{CheckOutcome, CheckOutcomes, CheckState, CheckStep, Preflight, SkipMask},
    output::{CheckOutput, write_human, write_json},
};
use crate::{cli::CheckArgs, commands::common::resolve_absolute_config_path, error::AppError};

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of inputs threaded into [`run_async`].  Re-exported at the
/// crate root under the `test-helpers` feature for integration-test
/// consumers.
pub struct CheckOptions<'a> {
    /// Absolute path to the resolved config file.
    pub config_path: &'a Path,
    /// Whether to emit the wire format on stdout.
    pub json: bool,
    /// Whether to run fatal provider probes.
    pub providers: bool,
    /// Project ID override.
    pub project: Option<&'a str>,
    /// Bearer token override.
    pub token: Option<&'a str>,
    /// Theme used for the human-readable writer.
    pub theme: &'a Theme,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal check` diagnostic flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config-path resolution or the underlying
/// async runtime fails to spin up.
pub(crate) fn run(config_path: &str, args: CheckArgs) -> Result<(), AppError> {
    let CheckArgs {
        providers,
        project,
        token,
        json,
    } = args;

    let absolute_config_path = resolve_absolute_config_path(config_path)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let stderr_lock = io::stderr().lock();
    let is_tty = stderr_lock.is_terminal();
    let stream_ctx = StreamThemeContext::probe_stderr(is_tty, resolve_mode(Mode::Auto));
    let mut wrapped_stderr = AutoStream::new(stderr_lock, stream_ctx.color_choice);
    let mut stdout = io::stdout().lock();

    rt.block_on(run_async(
        CheckOptions {
            config_path: &absolute_config_path,
            json,
            providers,
            project: project.as_deref(),
            token: token.as_deref(),
            theme: &stream_ctx.theme,
        },
        &mut stdout,
        &mut wrapped_stderr,
    ))
}

/// Async core for [`run`].
///
/// Iterates [`CheckStep`] in declared order.  Each step's preflight
/// classifies its applicability against shared [`CheckState`]; the
/// orchestrator then either runs the action, emits a `Skip` row, or
/// omits the row entirely.  `out_stdout` carries the `--json` payload;
/// `out_stderr` carries the themed human-readable output.
///
/// # Errors
///
/// Returns an [`AppError`] if building the shared HTTP client or
/// writing output fails.
///
/// # Panics
///
/// Panics if JSON serialisation of [`CheckOutput`] fails.  All fields
/// derive `Serialize` from primitive types, so this is unreachable in
/// practice.
pub async fn run_async(
    opts: CheckOptions<'_>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<(), AppError> {
    let mut state = build_state(&opts)?;
    let mut outcomes = CheckOutcomes::new();

    for step in CheckStep::iter() {
        match step.preflight(&state) {
            Preflight::Run => outcomes.push(step.act(&mut state).await),
            Preflight::Skip(reason) => {
                outcomes.push(CheckOutcome::dependency_skipped(step.name(), reason));
            }
            Preflight::Omit => {}
        }
    }

    let output = CheckOutput::from(&outcomes);
    if opts.json {
        write_json(out_stdout, &output).map_err(|source| AppError::Io {
            context: "writing tribal check output to stdout".to_owned(),
            source,
        })?;
    } else {
        write_human(out_stderr, opts.theme, &output).map_err(|source| AppError::Io {
            context: "writing tribal check output to stderr".to_owned(),
            source,
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// State construction
// ---------------------------------------------------------------------------

fn build_state(opts: &CheckOptions<'_>) -> Result<CheckState, AppError> {
    let http_client =
        reqwest::Client::builder()
            .build()
            .map_err(|source| AppError::HttpClient {
                context: "tribal check probe client".into(),
                source,
            })?;
    Ok(CheckState {
        config_path: opts.config_path.to_path_buf(),
        providers: opts.providers,
        project_override: opts.project.map(str::to_owned),
        token_override: opts.token.map(str::to_owned),
        path_var: std::env::var("PATH").unwrap_or_default(),
        http_client,
        config: None,
        skip_mask: SkipMask::default(),
        pool: None,
    })
}
