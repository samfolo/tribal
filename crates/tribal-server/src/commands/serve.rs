//! Implementation of the `tribal serve` subcommand.
//!
//! Loads configuration, initialises telemetry, delegates to
//! [`start_server`](crate::orchestration::start_server) for the full
//! bootstrap and worker startup, then blocks on OS signal handling until
//! shutdown.

#[cfg(not(unix))]
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal as unix_signal};
use tokio_util::sync::CancellationToken;
use tribal_config::{load_config, validate};

use crate::{cli::ServeArgs, error::AppError, orchestration};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal serve` startup sequence and blocks until shutdown.
///
/// Loads configuration, initialises telemetry, delegates to
/// [`start_server`](crate::orchestration::start_server), then blocks on
/// OS signal handling.
///
/// # Errors
///
/// Returns an [`AppError`] if any startup phase fails or if the worker
/// dies unexpectedly during operation.
pub(crate) fn run(config_path: &str, args: ServeArgs) -> Result<(), AppError> {
    let (cli_overrides, cli_project) = args.into_cli_overrides();

    let config = load_config(config_path, Some(cli_overrides), None)?;
    validate(&config)?;

    // Telemetry must be initialised before the async runtime so the guard
    // outlives `block_on` and flushes pending writes on shutdown.
    let _telemetry_guard = tribal_telemetry::init_subscriber(&config.logging)?;

    let cancellation_token = CancellationToken::new();

    let handle = orchestration::start_server(&config, cli_project, cancellation_token.clone())?;

    tracing::info!("startup sequence complete");

    // -- Signal handling -----------------------------------------------------
    // Races OS signals against the cancellation token (which fires on
    // programmatic cancellation, e.g. WorkerDeathGuard).
    handle.main_runtime().block_on(async {
        match await_shutdown_trigger(&cancellation_token).await {
            Ok(Some(name)) => {
                tracing::info!(trigger = name, "received OS signal, initiating shutdown");
                cancellation_token.cancel();
            }
            Ok(None) => {
                tracing::info!(
                    trigger = "programmatic",
                    "shutdown triggered programmatically",
                );
            }
            Err(AppError::SignalHandler { source }) => {
                tracing::warn!(
                    error = %source,
                    "signal handler registration failed; \
                     falling back to programmatic cancellation",
                );
                cancellation_token.cancelled().await;
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "shutdown trigger failed; \
                     falling back to programmatic cancellation",
                );
                cancellation_token.cancelled().await;
            }
        }
    });

    handle.shutdown()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Waits for either an OS signal or the cancellation token to fire.
///
/// Returns `Ok(Some(signal_name))` if an OS signal triggered shutdown, or
/// `Ok(None)` if the cancellation token was fired programmatically (e.g. by
/// the worker death guard).
///
/// # Errors
///
/// Returns [`AppError::SignalHandler`] if the SIGINT handler cannot be
/// registered (unix only — the non-unix branch falls back to programmatic
/// cancellation if `ctrl_c()` registration fails).  SIGTERM registration
/// is best-effort: failure is logged but does not prevent shutdown.
async fn await_shutdown_trigger(
    cancellation_token: &CancellationToken,
) -> Result<Option<&'static str>, AppError> {
    if cancellation_token.is_cancelled() {
        return Ok(None);
    }

    #[cfg(unix)]
    {
        let mut sigint = unix_signal(SignalKind::interrupt())
            .map_err(|source| AppError::SignalHandler { source })?;

        let mut sigterm = match unix_signal(SignalKind::terminate()) {
            Ok(stream) => Some(stream),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "SIGTERM handler registration failed; \
                     SIGINT and programmatic cancellation remain active",
                );
                None
            }
        };

        Ok(tokio::select! {
            Some(()) = sigint.recv() => Some("SIGINT"),
            Some(()) = async {
                match sigterm.as_mut() {
                    Some(s) => s.recv().await,
                    None => std::future::pending().await,
                }
            } => Some("SIGTERM"),
            () = cancellation_token.cancelled() => None,
        })
    }

    #[cfg(not(unix))]
    {
        // `Ok(())` pattern: if `ctrl_c()` returns `Err`, the arm is skipped
        // and the system falls back to programmatic cancellation only.  This
        // branch is not exercised on any supported platform (macOS and Linux
        // are both unix) and exists only for cross-platform compilation.
        Ok(tokio::select! {
            Ok(()) = signal::ctrl_c() => Some("SIGINT"),
            () = cancellation_token.cancelled() => None,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_await_shutdown_trigger_returns_none_on_pre_cancelled_token() {
        let token = CancellationToken::new();
        token.cancel();

        let result = await_shutdown_trigger(&token).await;
        assert!(matches!(result, Ok(None)));
    }
}
