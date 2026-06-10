//! Programmatic server lifecycle — bootstrap, worker startup, and shutdown.
//!
//! [`start_server`] is the entry point for running the full Tribal server
//! outside of the CLI.  It accepts a pre-loaded [`TribalConfig`] and a
//! [`CancellationToken`], performs the complete bootstrap and worker startup
//! sequence, and returns a [`ServerHandle`] for transport connection and
//! graceful shutdown.

use std::{path::PathBuf, sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio::{
    runtime::{Builder, Runtime},
    sync::{RwLock, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tribal_common::JobStateTxs;
use tribal_config::{PromptSource, TribalConfig};
use tribal_inference::{InferenceFacade, ProviderIdentity};
use tribal_mcp::{AppState, build_inference_parameters};
use tribal_telemetry::{MetricsRecorder, TelemetryGuard};
use tribal_worker::{PgLedgerSink, Worker, WorkerError};

use crate::{
    error::AppError,
    startup::{
        CatalogueCredentialResolver, POOL_NAME_MCP, POOL_NAME_WORKER, build_provider_registry,
        check_first_run, completion_stage_specs, create_pool_with_retry, ensure_prompt_files,
        generate_instance_id, init_prompt_watcher, load_prompts, load_prompts_embedded,
        probe_startup_providers, provision_genesis, read_active_profile, resolve_project,
        run_migrations, validate_embedding_identity,
    },
};

// ---------------------------------------------------------------------------
// ServerHandle
// ---------------------------------------------------------------------------

/// Handle to a running Tribal server instance.
///
/// Returned by [`start_server`].  Provides access to the application state
/// for transport connection, and a method to trigger graceful shutdown.
/// The worker and sweep task are already running when this handle is returned.
#[must_use]
pub struct ServerHandle {
    /// Shared application state.
    state: Arc<AppState>,
    /// Main tokio runtime hosting the sweep task.
    main_rt: Runtime,
    /// Worker tokio runtime hosting the poll-claim-dispatch loop.
    worker_rt: Runtime,
    /// Worker join handle for shutdown coordination.
    worker_handle: JoinHandle<()>,
    /// Receives `()` if the worker dies unexpectedly.
    death_rx: oneshot::Receiver<()>,
    /// Cancellation token — shared with worker and sweep.
    cancellation_token: CancellationToken,
    /// Shutdown deadline from configuration.
    shutdown_deadline: Duration,
    /// Telemetry guard — holds OTLP provider shutdown handles and the
    /// log writer flush guard.  Dropped during [`shutdown`](Self::shutdown),
    /// after the worker has stopped.  `None` when telemetry is
    /// initialised externally.
    telemetry_guard: Option<TelemetryGuard>,
}

impl ServerHandle {
    /// Returns the shared application state.
    #[must_use]
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// Returns a reference to the main runtime for blocking on transport
    /// or signal-handling futures.
    #[must_use]
    pub fn main_runtime(&self) -> &Runtime {
        &self.main_rt
    }

    /// Initiates graceful shutdown and blocks until complete.
    ///
    /// Cancels the token (no-op if already cancelled), waits for the worker
    /// within the configured deadline, and reports whether the worker died
    /// unexpectedly.
    ///
    /// The main runtime drops implicitly when `self` is consumed at the end
    /// of this function.  The sweep task terminates via the already-cancelled
    /// [`CancellationToken`].
    ///
    /// # Panics
    ///
    /// Must be called from outside a tokio runtime.  This method uses
    /// [`Runtime::block_on`](tokio::runtime::Runtime::block_on) internally,
    /// which panics if invoked from within an existing runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::WorkerDeath`] if the worker exited unexpectedly
    /// before shutdown was initiated, or
    /// [`AppError::ShutdownDeadlineExceeded`] if the worker did not finish
    /// within the configured deadline.
    pub fn shutdown(mut self) -> Result<(), AppError> {
        self.cancellation_token.cancel();

        let deadline = self.shutdown_deadline;
        let worker_handle = self.worker_handle;
        let deadline_exceeded = self
            .worker_rt
            .block_on(async { tokio::time::timeout(deadline, worker_handle).await })
            .is_err();

        if deadline_exceeded {
            tracing::warn!(
                deadline_ms = self.shutdown_deadline.as_millis(),
                "shutdown deadline expired; dropping worker runtime",
            );
        }

        // When the deadline expires, the JoinHandle is dropped, aborting the
        // worker task.  This triggers WorkerDeathGuard, making death_rx a
        // false positive.  Check deadline_exceeded first to avoid masking it
        // with WorkerDeath.
        let worker_died = matches!(self.death_rx.try_recv(), Ok(()));

        drop(self.worker_rt);

        // Flush OTLP providers on the main runtime — the tonic gRPC
        // channel needs the reactor to send final batches.
        self.main_rt.block_on(async {
            drop(self.telemetry_guard);
        });

        // Explicitly shut down main_rt with a bounded deadline rather
        // than relying on the implicit `Drop`.  `Runtime::drop` blocks
        // until all spawned blocking threads finish, which hangs
        // indefinitely when a transport uses a blocking stdin reader
        // (the `read()` syscall never returns once the client
        // disconnects).  For HTTP/SSE transports this completes
        // instantly since no blocking threads outlive the transport.
        self.main_rt.shutdown_timeout(self.shutdown_deadline);

        tracing::info!(worker_died, deadline_exceeded, "shutdown complete");

        if deadline_exceeded {
            return Err(AppError::ShutdownDeadlineExceeded {
                deadline_ms: self.shutdown_deadline.as_millis(),
            });
        }

        if worker_died {
            return Err(AppError::WorkerDeath);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// start_server
// ---------------------------------------------------------------------------

/// Starts the Tribal server with full bootstrap, worker startup, and sweep.
///
/// Accepts a pre-loaded and validated [`TribalConfig`], a
/// [`CancellationToken`] for external shutdown control, and a
/// pre-initialised telemetry guard and metrics recorder.
/// [`ServerHandle`] holds the guard so it outlives the runtimes
/// and flushes OTLP data on shutdown.
///
/// Returns a [`ServerHandle`] providing access to the running server's state
/// and shutdown mechanism.
///
/// # Panics
///
/// Must be called from outside a tokio runtime.  This function creates its
/// own runtimes and calls [`Runtime::block_on`](tokio::runtime::Runtime::block_on),
/// which panics if invoked from within an existing runtime context.
///
/// # Errors
///
/// Returns [`AppError`] if runtime creation, telemetry initialisation,
/// database connection, migration, provider setup, prompt loading,
/// project resolution, or worker startup fails.
pub fn start_server(
    config: &TribalConfig,
    cli_project: Option<String>,
    cancellation_token: CancellationToken,
    telemetry_guard: Option<TelemetryGuard>,
    metrics: Arc<dyn MetricsRecorder>,
) -> Result<ServerHandle, AppError> {
    let job_state_txs: JobStateTxs = Arc::new(DashMap::new());

    // -- Main runtime --------------------------------------------------------

    let main_rt = Builder::new_multi_thread()
        .thread_name("tribal-main")
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let (state, worker) = match main_rt.block_on(bootstrap(
        config,
        cli_project,
        cancellation_token.clone(),
        Arc::clone(&job_state_txs),
        metrics.clone(),
    )) {
        Ok(result) => result,
        Err(e) => {
            // Flush OTLP providers on the runtime before returning —
            // dropping outside the runtime silently loses pending spans.
            main_rt.block_on(async { drop(telemetry_guard) });
            return Err(e);
        }
    };

    // -- Worker runtime ------------------------------------------------------

    let worker_rt = match Builder::new_multi_thread()
        .thread_name("tribal-worker")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(source) => {
            main_rt.block_on(async { drop(telemetry_guard) });
            return Err(AppError::WorkerRuntime { source });
        }
    };

    if let Err(source) = worker_rt.block_on(worker.startup()) {
        main_rt.block_on(async { drop(telemetry_guard) });
        return Err(AppError::WorkerStartup { source });
    }

    let (death_tx, death_rx) = oneshot::channel::<()>();

    let spawn_token = cancellation_token.clone();
    let worker_handle = worker_rt.spawn(async move {
        let mut guard = WorkerDeathGuard {
            cancellation_token: spawn_token,
            death_tx: Some(death_tx),
        };

        let result = worker.run().await;

        match result {
            Err(WorkerError::Cancelled) => {
                tracing::info!("worker stopped: cancellation requested");
                guard.disarm();
            }
            Err(ref error) => {
                tracing::error!(%error, "worker died unexpectedly");
            }
            Ok(()) => {
                tracing::error!("worker exited without error or cancellation");
            }
        }
    });

    // -- Job-state sweep -----------------------------------------------------
    // JoinHandle discarded — the sweep is a performance optimisation, not
    // a correctness mechanism.  If it panics, watch entries accumulate
    // until the process restarts; the DB remains authoritative.

    let terminal_ttl = Duration::from_secs(config.server.job_state_ttl_seconds);
    let hard_ttl = Duration::from_secs(config.server.job_state_hard_ttl_seconds);
    drop(main_rt.spawn(tribal_mcp::sweep::run_job_state_sweep(
        Arc::clone(&job_state_txs),
        terminal_ttl,
        hard_ttl,
        cancellation_token.clone(),
    )));

    // -- Queue health gauges -------------------------------------------------
    // JoinHandle discarded — the gauge task is a monitoring optimisation,
    // not a correctness mechanism.  If it panics, gauges stop updating
    // but the worker continues normally; the DB remains authoritative.
    drop(worker_rt.spawn(tribal_worker::run_queue_health_gauges(
        state.worker_pool().clone(),
        metrics,
        cancellation_token.clone(),
    )));

    // -- Prompt hot-reload watcher -------------------------------------------
    // JoinHandle discarded — the watcher is a convenience feature, not
    // a correctness mechanism.  If it panics, prompts remain at the last
    // loaded version until the process restarts.
    if let PromptSource::Disk {
        directory,
        hot_reload: true,
    } = &config.prompts.source
    {
        let prompts_dir = expand_prompts_dir(directory);
        let watcher_future = init_prompt_watcher(
            prompts_dir,
            state.mcp_pool().clone(),
            state.active_prompt_versions().clone(),
            cancellation_token.clone(),
        )?;
        drop(main_rt.spawn(watcher_future));
    }

    Ok(ServerHandle {
        state,
        main_rt,
        worker_rt,
        worker_handle,
        death_rx,
        cancellation_token,
        shutdown_deadline: Duration::from_millis(config.server.shutdown_deadline_ms),
        telemetry_guard,
    })
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Asynchronous startup sequence: pools, migrations, providers, prompts,
/// project resolution, `AppState` assembly, and `Worker` construction.
async fn bootstrap(
    config: &TribalConfig,
    cli_project: Option<String>,
    cancellation_token: CancellationToken,
    job_state_txs: JobStateTxs,
    metrics: Arc<dyn MetricsRecorder>,
) -> Result<(Arc<AppState>, Arc<Worker>), AppError> {
    // -- Database pools ------------------------------------------------------

    let pool_mcp = create_pool_with_retry(
        &config.database,
        POOL_NAME_MCP,
        config.database.pool_mcp_max_connections,
        config.database.statement_timeout_mcp_ms,
        config.database.max_connect_attempts,
    )
    .await?;

    let pool_worker = create_pool_with_retry(
        &config.database,
        POOL_NAME_WORKER,
        config.database.pool_worker_max_connections,
        config.database.statement_timeout_worker_ms,
        config.database.max_connect_attempts,
    )
    .await?;

    // -- Migrations ----------------------------------------------------------

    check_first_run(&pool_mcp).await?;
    run_migrations(&pool_mcp).await?;

    // -- First-boot provisioning ---------------------------------------------

    provision_genesis(&pool_mcp, config).await?;

    // -- Instance identity ---------------------------------------------------

    let instance_id = generate_instance_id();

    // -- Prompts -------------------------------------------------------------

    let active_prompt_versions = match &config.prompts.source {
        PromptSource::Embedded {} => load_prompts_embedded(&pool_mcp).await?,
        PromptSource::Disk { directory, .. } => {
            let prompts_dir = expand_prompts_dir(directory);
            ensure_prompt_files(&prompts_dir).await?;
            load_prompts(&pool_mcp, &prompts_dir).await?
        }
    };

    // -- Providers -----------------------------------------------------------

    // The active profile (seeded by provisioning) is the live embedding
    // identity; the registry and provider are built from it, not from config.
    let active_profile = read_active_profile(&pool_mcp).await?;
    let registry = build_provider_registry(config, &active_profile)?;

    // The façade owns provider construction, credentials, permits, and
    // accounting; the ledger sink writes through the worker pool.
    let sink = Arc::new(PgLedgerSink::new(pool_worker.clone(), metrics.clone()));
    let facade = Arc::new(
        InferenceFacade::new(
            registry,
            &completion_stage_specs(config),
            Arc::new(CatalogueCredentialResolver::new(config.credentials.clone())),
            sink,
        )
        .map_err(|e| AppError::ProviderSetup {
            context: e.to_string(),
        })?,
    );
    let embedding_identity = ProviderIdentity {
        name: active_profile.provider_kind().to_string(),
        model: active_profile.model().to_owned(),
    };

    // Boot fails closed when the active embedding identity is unusable (a
    // missing cloud credential, a provider kind with no embedding API):
    // booting past it would dead-letter every ingest and fail every
    // discover with only a warn line to explain why.
    validate_embedding_identity(&facade, config, &active_profile)?;

    probe_startup_providers(&facade, &active_profile).await;

    // -- Project resolution --------------------------------------------------

    let resolved_project = resolve_project(&pool_mcp, cli_project).await?;

    // -- Worker construction -------------------------------------------------
    // Worker is built before AppState so shared values can be cloned for the
    // worker and the originals moved into AppState.

    let worker = Arc::new(Worker::new(
        pool_worker.clone(),
        Arc::clone(&facade),
        cancellation_token.clone(),
        config.worker.clone(),
        config.logging.include_llm_content,
        instance_id.to_string(),
        Arc::clone(&job_state_txs),
        metrics.clone(),
    ));

    // -- AppState assembly ---------------------------------------------------

    let inference_parameters = build_inference_parameters(config, active_profile.dimensions());

    let base = AppState::builder()
        .pool_mcp(pool_mcp)
        .pool_worker(pool_worker)
        .instance_id(instance_id)
        .build_version(Arc::from(env!("TRIBAL_GIT_DESCRIBE")))
        .inference_parameters(inference_parameters)
        .active_prompt_versions(Arc::new(RwLock::new(active_prompt_versions)))
        .facade(facade)
        .embedding_identity(embedding_identity)
        .worker_config(config.worker.clone())
        .server_config(Arc::new(config.server.clone()))
        .cancellation_token(cancellation_token)
        .job_state_txs(job_state_txs)
        .metrics(metrics);

    let state = Arc::new(match resolved_project {
        Some(project) => base.resolved_project(project).build(),
        None => base.build(),
    });

    Ok((state, worker))
}

// ---------------------------------------------------------------------------
// WorkerDeathGuard
// ---------------------------------------------------------------------------

/// Drop guard ensuring the cancellation token fires on all worker exit paths.
///
/// On drop, sends the death signal via `death_tx` (if not disarmed) and
/// cancels the token.  This covers unexpected returns, errors, and panics.
/// Call [`disarm`](Self::disarm) on the clean cancellation path to suppress
/// the death signal.
struct WorkerDeathGuard {
    cancellation_token: CancellationToken,
    death_tx: Option<oneshot::Sender<()>>,
}

impl WorkerDeathGuard {
    /// Disarms the guard on clean shutdown (cancellation path).
    fn disarm(&mut self) {
        self.death_tx.take();
    }
}

impl Drop for WorkerDeathGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.death_tx.take() {
            let _ = tx.send(());
        }
        self.cancellation_token.cancel();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Expands tilde (`~`) in the prompts directory path.
///
/// Defensive: covers programmatic callers that assemble a config
/// without routing it through the loader's path-expansion contract.
fn expand_prompts_dir(raw: &str) -> PathBuf {
    PathBuf::from(shellexpand::tilde(raw).as_ref())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_prompts_dir_no_tilde() {
        let result = expand_prompts_dir("/absolute/path/prompts");
        assert_eq!(result, PathBuf::from("/absolute/path/prompts"));
    }

    #[test]
    fn test_expand_prompts_dir_with_tilde() {
        let result = expand_prompts_dir("~/prompts");
        assert!(!result.to_str().unwrap().starts_with('~'));
        assert!(result.to_str().unwrap().ends_with("/prompts"));
    }

    #[test]
    fn test_death_guard_armed_drop_sends_signal_and_cancels() {
        let token = CancellationToken::new();
        let (death_tx, mut death_rx) = oneshot::channel::<()>();

        let guard = WorkerDeathGuard {
            cancellation_token: token.clone(),
            death_tx: Some(death_tx),
        };

        drop(guard);

        assert!(token.is_cancelled());
        assert!(matches!(death_rx.try_recv(), Ok(())));
    }

    #[test]
    fn test_death_guard_disarmed_drop_cancels_without_signal() {
        let token = CancellationToken::new();
        let (death_tx, mut death_rx) = oneshot::channel::<()>();

        let mut guard = WorkerDeathGuard {
            cancellation_token: token.clone(),
            death_tx: Some(death_tx),
        };

        guard.disarm();
        drop(guard);

        assert!(token.is_cancelled());
        assert!(matches!(
            death_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }
}
