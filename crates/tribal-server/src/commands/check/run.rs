//! Entry point and async core for `tribal check`.

use std::{
    io::{self, Write},
    path::Path,
};

use tribal_config::{ConfigError, load_config, validate};

use super::{
    checks::{CheckContext, CheckOutcome, CheckOutcomes, database_reachable, migrations_current},
    output::CheckOutput,
};
use crate::{
    cli::CheckArgs,
    commands::common::{DATABASE_COMMAND_DEFAULTS, resolve_absolute_config_path},
    error::AppError,
};

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
    let config_path_str = opts
        .config_path
        .to_str()
        .expect("CheckOptions::config_path is resolved from a &str input path");

    let parse_result = load_config(config_path_str, None, Some(&DATABASE_COMMAND_DEFAULTS));
    let mut outcomes = CheckOutcomes::new();

    outcomes.push(if let Err(error) = &parse_result {
        CheckOutcome::config_parse_failed(error, opts.config_path)
    } else {
        CheckOutcome::config_parse_loaded(opts.config_path.to_path_buf())
    });

    if let Ok(config) = parse_result {
        outcomes.push(match validate(&config) {
            Ok(()) => CheckOutcome::config_validate_satisfied(),
            Err(ConfigError::ValidationFailed { errors }) => {
                CheckOutcome::config_validate_failed(errors)
            }
            Err(other) => CheckOutcome::config_validate_failed(vec![other.to_string()]),
        });

        let (db_outcome, pool) = database_reachable(&config.database).await;
        outcomes.push(db_outcome);
        if let Some(pool) = pool {
            let ctx = CheckContext { pool };
            outcomes.push(migrations_current(&ctx).await);
        }
    }

    let output = CheckOutput::from(&outcomes);

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
