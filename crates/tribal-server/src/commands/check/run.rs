//! Entry point and async core for `tribal check`.

use std::{
    io::{self, Write},
    path::Path,
};

use super::{
    checks::{CheckDetail, CheckName, CheckOutcome, CheckStatus},
    output::{CheckOutput, CheckResult},
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

    rt.block_on(run_async(CheckOptions {
        config_path: &absolute_config_path,
        json,
        providers,
        project: project.as_deref(),
        token: token.as_deref(),
    }))
}

/// Async core for [`run`].
///
/// Renders the diagnostic output.  When `--json` is set, the wire
/// format is written to stdout.
///
/// # Errors
///
/// Returns an [`AppError`] if writing the output fails.
///
/// # Panics
///
/// Panics if JSON serialisation of [`CheckOutput`] fails.  All fields
/// derive `Serialize` from primitive types, so this is unreachable in
/// practice.
pub async fn run_async(opts: CheckOptions<'_>) -> Result<(), AppError> {
    let outcome = CheckOutcome {
        name: CheckName::ConfigParse,
        status: CheckStatus::Pass,
        detail: CheckDetail::ConfigLoaded {
            path: opts.config_path.to_path_buf(),
        },
    };

    let output = CheckOutput {
        ok: true,
        checks: vec![CheckResult::from(&outcome)],
    };

    if opts.json {
        let rendered =
            serde_json::to_string_pretty(&output).expect("CheckOutput is always serialisable");
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{rendered}").map_err(|source| AppError::Io {
            context: "writing tribal check output to stdout".to_owned(),
            source,
        })?;
        stdout.flush().map_err(|source| AppError::Io {
            context: "flushing tribal check output to stdout".to_owned(),
            source,
        })?;
    }

    let _ = opts.providers;
    let _ = opts.project;
    let _ = opts.token;

    Ok(())
}
