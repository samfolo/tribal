//! Implementation of the `tribal serve` subcommand.
//!
//! Loads configuration, initialises telemetry, delegates to
//! [`start_server`](crate::orchestration::start_server) for the full
//! bootstrap and worker startup, then blocks on OS signal handling until
//! shutdown.

use std::{
    io,
    os::fd::{FromRawFd as _, OwnedFd, RawFd},
    path::Path,
    sync::Arc,
};

#[cfg(not(unix))]
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal as unix_signal};
use tokio_util::sync::CancellationToken;
use tribal_auth::oauth::OAuthRuntimeConfig;
use tribal_config::{TribalConfig, config_warnings, load_config, validate};
use tribal_domain::{ProjectId, TransportKind};
use tribal_mcp::HandlerConfig;

use crate::{
    cli::ServeArgs,
    error::AppError,
    management::{
        authority::{
            AuthorityAcquire, AuthorityDescriptor, AuthorityError, AuthorityLease,
            AuthorityOwnerKind,
        },
        custody::{MANAGED_RUNTIME_INSTANCE_ID, RuntimeCustodyGuard},
        runtime_control::{self, RuntimeControlService},
    },
    orchestration,
    startup::{
        POOL_NAME_MCP, bound_socket_address, init_config_watcher, resolve_oauth_runtime,
        runtime_data_plane,
    },
    transport,
};

/// Inherited locked authority description for a manager-spawned runtime.
pub(crate) const MANAGED_AUTHORITY_FD: &str = "TRIBAL_MANAGED_AUTHORITY_FD";

/// Initial project context selected after command-line parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServeProjectMode {
    Auto,
    Unscoped,
    Project(ProjectId),
}

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
pub(crate) async fn run(config_path: &str, args: ServeArgs) -> Result<(), AppError> {
    let authority_lease = acquire_runtime_authority(Path::new(config_path))?;
    let cancellation_token = CancellationToken::new();
    let runtime_custody = RuntimeCustodyGuard::bootstrap_from_environment(
        &authority_lease,
        cancellation_token.clone(),
    )
    .map_err(|source| AppError::Management {
        source: Box::new(source),
    })?;
    let mut managed_control = managed_runtime_control(&authority_lease, runtime_custody.as_ref())?;
    let mode = serve_project_mode(args.project.clone(), args.unscoped)?;
    let (cli_overrides, _) = args.into_cli_overrides();
    let config = load_config(config_path, Some(cli_overrides), None)?;
    validate(&config)?;
    if managed_control.is_some() {
        reject_managed_ephemeral_port(&config)?;
    }

    let (log_events, _) = tokio::sync::broadcast::channel(512);

    let (telemetry_guard, metrics, log_ring, log_filter) =
        tribal_telemetry::init_subscriber_with_log_bridge(
            &config.logging,
            &config.telemetry,
            log_events.clone(),
        )?;

    // Surfaced now that the subscriber is live: inert or surprising config
    // that validation admits but the operator may not have intended.
    for warning in config_warnings(&config) {
        tracing::warn!("{warning}");
    }

    let transport = config.server.transport;

    let handle = tokio::task::block_in_place(|| {
        orchestration::start_server_with_mode(
            &config,
            mode,
            cancellation_token.clone(),
            Some(telemetry_guard),
            metrics,
        )
    })?;

    let handler_config = HandlerConfig::from(&config).with_pool_name(POOL_NAME_MCP);

    let oauth_runtime = Arc::new(resolve_oauth_runtime(&config)?);

    tracing::info!(%transport, "startup sequence complete");

    // -- Transport + signal handling -----------------------------------------
    // The transport runs in a spawned task so that OS signal handling
    // can cancel the token without dropping the transport future.  This
    // lets axum's graceful shutdown drain active connections before the
    // server exits.
    let transport_error: Option<AppError> = async {
        let expanded_config_path =
            std::path::PathBuf::from(shellexpand::tilde(config_path).into_owned());
        let runtime_config = tokio::sync::watch::Sender::new(Arc::new(config.clone()));
        let runtime_control_task = match managed_control.take() {
            Some((runtime, proof)) => match runtime_control::spawn_server(
                authority_lease.paths().runtime_control_socket_path.clone(),
                proof,
                RuntimeControlService {
                    runtime,
                    data_plane: runtime_data_plane(&config, &oauth_runtime),
                    config_path: expanded_config_path.clone(),
                    config: runtime_config,
                    log_filter,
                    log_ring,
                    events: log_events.clone(),
                    shutdown: cancellation_token.clone(),
                },
            )
            .await
            {
                Ok(task) => Some(task),
                Err(source) => {
                    return Some(AppError::Management {
                        source: Box::new(source),
                    });
                }
            },
            None => None,
        };
        let config_watcher_task =
            match init_config_watcher(&expanded_config_path, cancellation_token.clone()) {
                Ok(watcher) => Some(tokio::spawn(watcher)),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "config-file watcher init failed; external edits will not notify",
                    );
                    None
                }
            };

        let mut transport_handle = tokio::spawn(run_transport(
            transport,
            Arc::clone(handle.state()),
            Arc::clone(&oauth_runtime),
            handler_config,
            cancellation_token.clone(),
            config.server.clone(),
        ));

        // Wait for the transport to exit or an OS signal to arrive.
        // When the signal branch wins, the transport task continues
        // running — it observes the cancellation token and shuts down
        // gracefully rather than being dropped mid-flight.
        let transport_result = tokio::select! {
            result = &mut transport_handle => {
                cancellation_token.cancel();
                resolve_transport_result(result, transport, "transport failed")
            }
            trigger = await_shutdown_trigger(&cancellation_token) => {
                handle_shutdown_trigger(trigger, &cancellation_token);
                resolve_transport_result(
                    transport_handle.await,
                    transport,
                    "transport failed during shutdown",
                )
            },
        };
        join_companion_tasks(runtime_control_task, config_watcher_task).await;
        transport_result
    }
    .await;

    // Prefer the transport error over the shutdown result — a bind
    // failure is more informative than a clean worker shutdown.
    let shutdown_result = tokio::task::block_in_place(|| handle.shutdown());
    match transport_error {
        Some(err) => Err(err),
        None => shutdown_result,
    }
}

fn serve_project_mode(
    project: Option<String>,
    unscoped: bool,
) -> Result<ServeProjectMode, AppError> {
    if unscoped {
        return Ok(ServeProjectMode::Unscoped);
    }
    project.map_or(Ok(ServeProjectMode::Auto), |raw| {
        raw.parse()
            .map(ServeProjectMode::Project)
            .map_err(|_| AppError::ProjectResolution {
                context: format!("invalid project ID format: {raw}"),
            })
    })
}

async fn join_companion_tasks(
    runtime_control: Option<tokio::task::JoinHandle<()>>,
    config_watcher: Option<tokio::task::JoinHandle<()>>,
) {
    for (name, task) in [
        ("runtime-control", runtime_control),
        ("config-file watcher", config_watcher),
    ] {
        if let Some(task) = task
            && let Err(error) = task.await
        {
            tracing::error!(%error, task = name, "runtime companion task failed during shutdown");
        }
    }
}

fn managed_runtime_control(
    lease: &AuthorityLease,
    custody: Option<&RuntimeCustodyGuard>,
) -> Result<
    Option<(
        tribal_wire::management::RuntimeIdentity,
        crate::management::custody::RuntimeProofRegistry,
    )>,
    AppError,
> {
    let Some(custody) = custody else {
        return Ok(None);
    };
    let Ok(runtime_instance_id) = std::env::var(MANAGED_RUNTIME_INSTANCE_ID) else {
        return Err(AppError::Management {
            source: Box::new(crate::management::custody::CustodyError::EnvironmentIncomplete),
        });
    };
    Ok(Some((
        tribal_wire::management::RuntimeIdentity {
            instance_id: runtime_instance_id,
            pid: std::process::id(),
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
            config_path: tribal_wire::management::ConfigFilePath {
                path: lease
                    .paths()
                    .canonical_config_path
                    .to_string_lossy()
                    .into_owned(),
            },
        },
        custody.control_proof(),
    )))
}

/// Refuses an ephemeral network port under managed custody: the manager
/// projects the bound address as the data plane, and port `0` names an
/// endpoint no restart can keep. Unmanaged servers keep ephemeral ports.
fn reject_managed_ephemeral_port(config: &TribalConfig) -> Result<(), AppError> {
    if config.server.transport != TransportKind::Stdio && bound_socket_address(config).port() == 0 {
        return Err(AppError::ConfigInvariant {
            reason:
                "a managed runtime requires a fixed server.bind_address port; port 0 is ephemeral"
                    .to_owned(),
        });
    }
    Ok(())
}

fn acquire_runtime_authority(config_path: &Path) -> Result<AuthorityLease, AppError> {
    if let Some(raw) = std::env::var_os(MANAGED_AUTHORITY_FD) {
        let fd = raw
            .to_string_lossy()
            .parse::<RawFd>()
            .map_err(|_| AppError::Management {
                source: Box::new(AuthorityError::Filesystem {
                    path: config_path.to_owned(),
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "managed authority descriptor is invalid",
                    ),
                }),
            })?;
        // SAFETY: the manager passes one owned inherited descriptor number;
        // this process adopts it exactly once at serve entry.
        let descriptor = unsafe { OwnedFd::from_raw_fd(fd) };
        return AuthorityLease::from_inherited(config_path, descriptor).map_err(management_error);
    }

    match AuthorityLease::acquire(config_path).map_err(management_error)? {
        AuthorityAcquire::Acquired(mut lease) => {
            let descriptor = AuthorityDescriptor {
                kind: AuthorityOwnerKind::StandaloneRuntime,
                instance_id: uuid::Uuid::new_v4().to_string(),
                pid: std::process::id(),
                binary_version: env!("CARGO_PKG_VERSION").to_owned(),
                canonical_config_path: lease.paths().canonical_config_path.clone(),
                socket_path: None,
                protocol_version: None,
            };
            lease.publish(&descriptor).map_err(management_error)?;
            Ok(lease)
        }
        AuthorityAcquire::Occupied(_) => Err(management_error(AuthorityError::Occupied {
            path: config_path.to_owned(),
        })),
    }
}

fn management_error(source: AuthorityError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}

/// Dispatches to the appropriate transport runner.
///
/// Takes owned values because the future is spawned as a task.
async fn run_transport(
    transport: TransportKind,
    state: Arc<tribal_mcp::AppState>,
    oauth_runtime: Arc<OAuthRuntimeConfig>,
    handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    server_config: tribal_config::ServerConfig,
) -> Result<(), AppError> {
    match transport {
        TransportKind::Http => {
            transport::run_http_transport(
                &state,
                &server_config,
                oauth_runtime,
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
                oauth_runtime,
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

    #[test]
    fn test_managed_custody_refuses_an_ephemeral_network_port() {
        let mut config = TribalConfig::default();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:0".to_owned());

        let refusal = reject_managed_ephemeral_port(&config);

        assert!(matches!(refusal, Err(AppError::ConfigInvariant { .. })));
    }

    #[test]
    fn test_managed_custody_accepts_a_fixed_network_port() {
        let mut config = TribalConfig::default();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".to_owned());

        assert!(reject_managed_ephemeral_port(&config).is_ok());
    }

    #[test]
    fn test_managed_custody_ignores_the_port_for_stdio() {
        let mut config = TribalConfig::default();
        config.server.transport = TransportKind::Stdio;
        config.server.bind_address = Some("127.0.0.1:0".to_owned());

        assert!(reject_managed_ephemeral_port(&config).is_ok());
    }

    #[test]
    fn test_serve_project_mode_preserves_all_three_modes() {
        let project = ProjectId::new();
        assert_eq!(
            serve_project_mode(None, false).expect("auto mode resolves"),
            ServeProjectMode::Auto
        );
        assert_eq!(
            serve_project_mode(None, true).expect("unscoped mode resolves"),
            ServeProjectMode::Unscoped
        );
        assert_eq!(
            serve_project_mode(Some(project.to_string()), false).expect("project mode resolves"),
            ServeProjectMode::Project(project)
        );
        assert!(serve_project_mode(Some("not-a-project".to_owned()), false).is_err());
    }

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
