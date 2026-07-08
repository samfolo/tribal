//! Entry point and step-pipeline driver for `tribal check`.

use std::{
    io::{self, IsTerminal, Write},
    path::Path,
    time::Duration,
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

impl<'a> CheckOptions<'a> {
    fn report_options(&self) -> CheckReportOptions<'a> {
        CheckReportOptions {
            config_path: self.config_path,
            providers: self.providers,
            project: self.project,
            token: self.token,
        }
    }
}

/// Inputs for a check report that is returned as data instead of written to a
/// CLI stream.
pub(crate) struct CheckReportOptions<'a> {
    /// Absolute path to the resolved config file.
    pub config_path: &'a Path,
    /// Whether to run fatal provider probes.
    pub providers: bool,
    /// Project ID override.
    pub project: Option<&'a str>,
    /// Bearer token override.
    pub token: Option<&'a str>,
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

    let output = rt.block_on(run_async(
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
    ))?;
    if output.ok {
        Ok(())
    } else {
        Err(AppError::CheckFailed)
    }
}

/// Async core for [`run`].
///
/// Iterates [`CheckStep`] in declared order.  Each step's preflight
/// classifies its applicability against shared [`CheckState`]; the
/// orchestrator then either runs the action, emits a `Skip` row, or
/// omits the row entirely.  `out_stdout` carries the `--json` payload;
/// `out_stderr` carries the themed human-readable output.
///
/// Returns the assembled [`CheckOutput`] so callers can inspect the
/// overall `ok` flag and per-row statuses without re-parsing the wire
/// payload.  [`run`] uses this to translate `!output.ok` into the
/// silent [`AppError::CheckFailed`] exit signal.
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
) -> Result<CheckOutput, AppError> {
    let output = run_report_async(opts.report_options()).await?;
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
    Ok(output)
}

/// Async core that returns the same report `tribal check --json` writes.
///
/// # Errors
///
/// Returns an [`AppError`] if the shared HTTP client cannot be built.
pub(crate) async fn run_report_async(
    opts: CheckReportOptions<'_>,
) -> Result<CheckOutput, AppError> {
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

    Ok(CheckOutput::from(&outcomes))
}

// ---------------------------------------------------------------------------
// State construction
// ---------------------------------------------------------------------------

/// 10s bounds blackholed providers without choking slow cloud probes;
/// per-request timeouts (e.g. advertised-url's 2s) override.
const PROBE_CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

fn build_state(opts: &CheckReportOptions<'_>) -> Result<CheckState, AppError> {
    // advertised_url's "something is bound" semantics treat any HTTP
    // response — including 3xx — as proof.  Following redirects would
    // turn a redirect to a broken target into a false unreachable.
    let http_client = reqwest::Client::builder()
        .timeout(PROBE_CLIENT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|source| AppError::HttpClient {
            context: "tribal check probe client".into(),
            source,
        })?;
    Ok(CheckState {
        config_path: opts.config_path.to_path_buf(),
        providers: opts.providers,
        project_override: opts.project.map(str::to_owned),
        token_override: opts.token.and_then(canonical_token),
        path_var: std::env::var("PATH").unwrap_or_default(),
        http_client,
        config: None,
        skip_mask: SkipMask::default(),
        pool: None,
        gateway: None,
    })
}

/// Trims and discards whitespace-only inputs so downstream callers see
/// the canonical token form.
fn canonical_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}
