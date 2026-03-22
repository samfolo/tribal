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
use tribal_config::{TransportKind, load_config, validate};
use tribal_mcp::HandlerConfig;

use crate::{cli::ServeArgs, error::AppError, orchestration, startup::POOL_NAME_MCP, transport};

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

    let transport = config.server.transport;

    if transport == TransportKind::Sse {
        return Err(AppError::TransportUnsupported { transport });
    }

    let handle = orchestration::start_server(&config, cli_project, cancellation_token.clone())?;

    let handler_config = HandlerConfig::from(&config).with_pool_name(POOL_NAME_MCP);

    tracing::info!(%transport, "startup sequence complete");

    // -- Transport + signal handling -----------------------------------------
    // Races the transport future against OS signals. Whichever completes
    // first triggers the cancellation token, causing the other to shut
    // down.
    handle.main_runtime().block_on(async {
        tokio::select! {
            result = run_transport(
                transport,
                &handle,
                handler_config,
                cancellation_token.clone(),
                &config.server,
            ) => {
                if let Err(ref error) = result {
                    tracing::error!(%error, "transport failed");
                }
                cancellation_token.cancel();
            }
            trigger = await_shutdown_trigger(&cancellation_token) => {
                handle_shutdown_trigger(trigger, &cancellation_token);
            }
        }
    });

    handle.shutdown()
}

/// Dispatches to the appropriate transport runner.
async fn run_transport(
    transport: TransportKind,
    handle: &orchestration::ServerHandle,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    server_config: &tribal_config::ServerConfig,
) -> Result<(), AppError> {
    match transport {
        TransportKind::Http => {
            transport::run_http_transport(
                handle.state(),
                server_config,
                handler_config,
                cancellation_token,
                None,
            )
            .await
        }
        TransportKind::Stdio => {
            transport::run_stdio_transport(handle.state(), handler_config, cancellation_token).await
        }
        TransportKind::Sse => Err(AppError::TransportUnsupported { transport }),
    }
}

/// Processes the result of `await_shutdown_trigger`.
fn handle_shutdown_trigger(
    trigger: Result<Option<&'static str>, AppError>,
    cancellation_token: &CancellationToken,
) {
    match trigger {
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
        Err(error) => {
            tracing::warn!(
                %error,
                "shutdown trigger failed; cancelling",
            );
            cancellation_token.cancel();
        }
    }
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
