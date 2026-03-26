//! Implementation of the `tribal serve` subcommand.
//!
//! Loads configuration, initialises telemetry, delegates to
//! [`start_server`](crate::orchestration::start_server) for the full
//! bootstrap and worker startup, then blocks on OS signal handling until
//! shutdown.

use std::{io, sync::Arc};

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

    // The OTLP gRPC exporter needs a reactor for init and for
    // background batch export.  This runtime lives for the duration
    // of the serve command so export tasks have a live executor.
    let telemetry_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let (telemetry_guard, metrics) = telemetry_rt.block_on(async {
        tribal_telemetry::init_subscriber(&config.logging, &config.telemetry)
    })?;

    let cancellation_token = CancellationToken::new();

    let transport = config.server.transport;

    let handle = orchestration::start_server(
        &config,
        cli_project,
        cancellation_token.clone(),
        Some(telemetry_guard),
        metrics,
    )?;

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
                return resolve_transport_result(result, transport, "transport failed");
            }
            trigger = await_shutdown_trigger(&cancellation_token) => trigger,
        };

        handle_shutdown_trigger(signal_fired, &cancellation_token);

        // Let the transport drain active connections.
        resolve_transport_result(
            transport_handle.await,
            transport,
            "transport failed during shutdown",
        )
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
        TransportKind::Sse => {
            transport::run_sse_transport(
                &state,
                &server_config,
                handler_config,
                cancellation_token,
                None,
            )
            .await
        }
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

/// Converts a transport task's `JoinHandle` result into an optional
/// `AppError`.
///
/// If the task panicked, the panic is propagated via
/// [`std::panic::resume_unwind`] — a transport panic is fatal and must
/// not be silently swallowed.
///
/// # Panics
///
/// Re-panics if the transport task panicked.
fn resolve_transport_result(
    result: Result<Result<(), AppError>, tokio::task::JoinError>,
    transport: TransportKind,
    context: &str,
) -> Option<AppError> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => {
            tracing::error!(%error, "{context}");
            Some(error)
        }
        Err(join_error) => {
            if join_error.is_panic() {
                std::panic::resume_unwind(join_error.into_panic());
            }
            tracing::error!(%join_error, "{context}: task aborted");
            Some(AppError::TransportServe {
                transport,
                source: io::Error::other(join_error.to_string()),
            })
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

    // -- resolve_transport_result -------------------------------------------

    #[test]
    fn test_resolve_transport_result_ok_ok_returns_none() {
        assert!(resolve_transport_result(Ok(Ok(())), TransportKind::Http, "test").is_none());
    }

    #[test]
    fn test_resolve_transport_result_ok_err_returns_error() {
        let app_error = AppError::TransportServe {
            transport: TransportKind::Http,
            source: io::Error::other("test"),
        };
        let result = resolve_transport_result(Ok(Err(app_error)), TransportKind::Http, "test");
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_resolve_transport_result_aborted_task_returns_error() {
        let handle = tokio::spawn(async { std::future::pending::<Result<(), AppError>>().await });
        handle.abort();
        let join_result = handle.await;

        let result = resolve_transport_result(join_result, TransportKind::Http, "test");
        assert!(result.is_some());
    }

    #[tokio::test]
    #[should_panic(expected = "task panicked")]
    async fn test_resolve_transport_result_panicked_task_repanics() {
        let handle: tokio::task::JoinHandle<Result<(), AppError>> = tokio::spawn(async {
            panic!("task panicked");
        });
        let join_result = handle.await;

        resolve_transport_result(join_result, TransportKind::Http, "test");
    }
}
