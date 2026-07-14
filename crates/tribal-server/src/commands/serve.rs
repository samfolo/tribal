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
use tribal_config::{CliShadow, TransportKind, config_warnings, load_config, validate};
use tribal_domain::ProjectId;
use tribal_mcp::HandlerConfig;

use crate::{
    cli::ServeArgs,
    control,
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
    startup::{POOL_NAME_MCP, SelfWriteSentinel, init_config_watcher},
    transport,
};

/// The environment marker a supervisor (the desktop app, launchd) exports when
/// it spawns the binary, declaring that it owns the process's lifecycle. It
/// governs `server.restart`: mediated when set, refused otherwise.
const SUPERVISED_MARKER: &str = "TRIBAL_SUPERVISED";

/// Inherited locked authority description for a manager-spawned runtime.
pub(crate) const MANAGED_AUTHORITY_FD: &str = "TRIBAL_MANAGED_AUTHORITY_FD";

/// Initial project context selected after command-line parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServeProjectMode {
    Auto,
    Unscoped,
    Project(ProjectId),
}

/// Whether a supervisor owns this process, read from [`SUPERVISED_MARKER`].
fn is_supervised() -> bool {
    supervised_from(std::env::var(SUPERVISED_MARKER).ok().as_deref())
}

/// Interprets the supervision marker's value. Only an explicit truthy value
/// counts, so a stray empty or `0` export never claims supervision the operator
/// did not intend.
fn supervised_from(value: Option<&str>) -> bool {
    value.is_some_and(|raw| {
        let value = raw.trim();
        value == "1" || value.eq_ignore_ascii_case("true")
    })
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
    let cli_shadow = CliShadow::from_overrides(&cli_overrides);

    let config = load_config(config_path, Some(cli_overrides), None)?;
    validate(&config)?;

    // The control-plane event bus, created before telemetry init so the
    // log-capture layer can publish onto it: the prompt watcher, the config-file
    // watcher, the log-capture layer, and the control socket all publish to and
    // subscribe from this one channel.
    let (control_events, _) = tokio::sync::broadcast::channel(control::EVENT_BUS_CAPACITY);

    let (telemetry_guard, metrics, log_ring, log_filter) =
        tribal_telemetry::init_subscriber_with_log_bridge(
            &config.logging,
            &config.telemetry,
            control_events.clone(),
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
            Some(control_events.clone()),
        )
    })?;

    let handler_config = HandlerConfig::from(&config).with_pool_name(POOL_NAME_MCP);

    let oauth_runtime = Arc::new(crate::startup::resolve_oauth_runtime(&config)?);

    tracing::info!(%transport, "startup sequence complete");

    // -- Transport + signal handling -----------------------------------------
    // The transport runs in a spawned task so that OS signal handling
    // can cancel the token without dropping the transport future.  This
    // lets axum's graceful shutdown drain active connections before the
    // server exits.
    let transport_error: Option<AppError> = async {
        // The local control plane serves alongside the MCP transport for the
        // whole run; it binds best-effort and never blocks MCP from serving.
        let expanded_config_path =
            std::path::PathBuf::from(shellexpand::tilde(config_path).into_owned());
        let self_write = SelfWriteSentinel::default();
        let embedding_profile =
            match crate::startup::read_active_profile(handle.state().mcp_pool()).await {
                Ok(profile) => control::EmbeddingProfileSnapshot::active(&profile),
                Err(error) => control::EmbeddingProfileSnapshot::Unknown {
                    detail: error.to_string(),
                },
            };
        let control_context = control::ControlContext {
            config: tokio::sync::watch::Sender::new(Arc::new(config.clone())),
            config_path: expanded_config_path.clone(),
            cli_shadow: cli_shadow.clone(),
            self_write: self_write.clone(),
            config_write_lock: tokio::sync::Mutex::new(()),
            pool: handle.state().mcp_pool().clone(),
            embedding_profile: tokio::sync::watch::Sender::new(embedding_profile),
            events: control_events.clone(),
            log_ring,
            log_filter,
            project: handle.state().resolved_project().map(|project| {
                tribal_wire::control::ProjectSummary {
                    id: project.id().to_string(),
                    name: project.name().to_owned(),
                }
            }),
            cancellation_token: cancellation_token.clone(),
            started_at: std::time::Instant::now(),
            binary_version: Arc::clone(handle.state().build_version()),
            instance_id: Arc::clone(handle.state().instance_id()),
            supervised: is_supervised(),
        };
        let runtime_control_task = match managed_control.take() {
            Some((runtime, proof)) => match runtime_control::spawn_server(
                authority_lease.paths().runtime_control_socket_path.clone(),
                proof,
                RuntimeControlService {
                    runtime,
                    config_path: expanded_config_path.clone(),
                    config: control_context.config.clone(),
                    log_filter: control_context.log_filter.clone(),
                    log_ring: control_context.log_ring.clone(),
                    events: control_events.clone(),
                    pool: control_context.pool.clone(),
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
        let control_task = control::spawn_control_plane(control_context).await;

        // The config-file watcher announces an external edit to the file as
        // `config.changed`; best-effort like the control plane, a failed init
        // never blocks MCP from serving.
        let config_watcher_task = match init_config_watcher(
            &expanded_config_path,
            control_events.clone(),
            self_write,
            cancellation_token.clone(),
        ) {
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
        join_companion_tasks(runtime_control_task, control_task, config_watcher_task).await;
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
    control: Option<tokio::task::JoinHandle<()>>,
    config_watcher: Option<tokio::task::JoinHandle<()>>,
) {
    for (name, task) in [
        ("runtime-control", runtime_control),
        ("control", control),
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
    fn test_supervised_marker_counts_only_explicit_truthy_values() {
        for truthy in ["1", "true", "TRUE", "True", "  true  "] {
            assert!(supervised_from(Some(truthy)), "{truthy:?} means supervised");
        }
        for falsy in [None, Some(""), Some("0"), Some("false"), Some("yes")] {
            assert!(
                !supervised_from(falsy),
                "{falsy:?} does not claim supervision"
            );
        }
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
