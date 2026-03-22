//! Implementation of the `tribal serve` subcommand.
//!
//! Loads configuration, initialises telemetry, delegates to
//! [`start_server`](crate::orchestration::start_server) for the full
//! bootstrap and worker startup, then blocks on OS signal handling until
//! shutdown.

use std::sync::Arc;

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
    // The transport runs in a spawned task so that OS signal handling
    // can cancel the token without dropping the transport future.  This
    // lets axum's graceful shutdown drain active connections before the
    // server exits.
    let transport_error: Option<AppError> = handle.main_runtime().block_on(async {
        let mut transport_handle = tokio::spawn(run_transport(
            transport,
            Arc::clone(handle.state()),
            handler_config,
            cancellation_token.clone(),
            config.server.clone(),
        ));

        // Wait for the transport to exit or an OS signal to arrive.
        // When the signal branch wins, the transport task continues
        // running — it observes the cancellation token and shuts down
        // gracefully rather than being dropped mid-flight.
        let signal_fired = tokio::select! {
            result = &mut transport_handle => {
                cancellation_token.cancel();
                return match result {
                    Ok(Err(error)) => {
                        tracing::error!(%error, "transport failed");
                        Some(error)
                    }
                    _ => None,
                };
            }
            trigger = await_shutdown_trigger(&cancellation_token) => trigger,
        };

        handle_shutdown_trigger(signal_fired, &cancellation_token);

        // Let the transport drain active connections.
        match transport_handle.await {
            Ok(Err(error)) => {
                tracing::error!(%error, "transport failed during shutdown");
                Some(error)
            }
            _ => None,
        }
    });

    // Prefer the transport error over the shutdown result — a bind
    // failure is more informative than a clean worker shutdown.
    let shutdown_result = handle.shutdown();
    match transport_error {
        Some(err) => Err(err),
        None => shutdown_result,
    }
}

/// Dispatches to the appropriate transport runner.
///
/// Takes owned values because the future is spawned as a task.
async fn run_transport(
    transport: TransportKind,
    state: Arc<tribal_mcp::AppState>,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    server_config: tribal_config::ServerConfig,
) -> Result<(), AppError> {
    match transport {
        TransportKind::Http => {
            transport::run_http_transport(
                &state,
                &server_config,
                handler_config,
                cancellation_token,
                None,
            )
            .await
        }
        TransportKind::Stdio => {
            transport::run_stdio_transport(&state, handler_config, cancellation_token).await
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
