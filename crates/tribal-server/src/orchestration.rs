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
use tribal_config::TribalConfig;
use tribal_mcp::AppState;
use tribal_worker::{Worker, WorkerError};

use crate::{
    error::AppError,
    startup::{
        POOL_NAME_MCP, POOL_NAME_WORKER, build_embedding_provider, build_inference_provider,
        build_provider_registry, check_first_run, create_pool_with_retry, ensure_prompt_files,
        generate_instance_id, load_prompts, resolve_project, run_migrations,
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

        let deadline_exceeded = self
            .worker_rt
            .block_on(tokio::time::timeout(
                self.shutdown_deadline,
                self.worker_handle,
            ))
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
/// Accepts a pre-loaded and validated [`TribalConfig`] and a
/// [`CancellationToken`] for external shutdown control.  Does not initialise
/// telemetry — callers are responsible for tracing subscriber setup.
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
/// Returns [`AppError`] if runtime creation, database connection, migration,
/// provider setup, prompt loading, project resolution, or worker startup
/// fails.
pub fn start_server(
    config: &TribalConfig,
    cli_project: Option<String>,
    cancellation_token: CancellationToken,
) -> Result<ServerHandle, AppError> {
    let job_state_txs: JobStateTxs = Arc::new(DashMap::new());

    // -- Main runtime --------------------------------------------------------

    let main_rt = Builder::new_multi_thread()
        .thread_name("tribal-main")
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let (state, worker) = main_rt.block_on(bootstrap(
        config,
        cli_project,
        cancellation_token.clone(),
        Arc::clone(&job_state_txs),
    ))?;

    // -- Worker runtime ------------------------------------------------------

    let worker_rt = Builder::new_multi_thread()
        .thread_name("tribal-worker")
        .enable_all()
        .build()
        .map_err(|source| AppError::WorkerRuntime { source })?;

    worker_rt
        .block_on(worker.startup())
        .map_err(|source| AppError::WorkerStartup { source })?;

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

    Ok(ServerHandle {
        state,
        main_rt,
        worker_rt,
        worker_handle,
        death_rx,
        cancellation_token,
        shutdown_deadline: Duration::from_millis(config.server.shutdown_deadline_ms),
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

    // -- Instance identity ---------------------------------------------------

    let instance_id = generate_instance_id();

    // -- Prompts -------------------------------------------------------------

    let prompts_dir = expand_prompts_dir(&config.prompts.directory);
    ensure_prompt_files(&prompts_dir).await?;
    let active_prompt_versions = load_prompts(&pool_mcp, &prompts_dir).await?;

    // -- Providers -----------------------------------------------------------

    let registry = build_provider_registry(config)?;

    let (embedding_provider, embedding_key) =
        build_embedding_provider(&registry, &config.embedding).await?;

    let (extraction_provider, extraction_key) =
        build_inference_provider(&registry, &config.inference.extraction).await?;

    let (triage_provider, triage_key) =
        build_inference_provider(&registry, &config.inference.triage).await?;

    let (relation_provider, relation_key) =
        build_inference_provider(&registry, &config.inference.relation).await?;

    // -- Project resolution --------------------------------------------------

    let resolved_project = resolve_project(&pool_mcp, cli_project).await?;

    // -- Worker construction -------------------------------------------------
    // Worker is built before AppState so shared values can be cloned for the
    // worker and the originals moved into AppState.

    let registry = Arc::new(registry);

    let worker = Arc::new(Worker::new(
        pool_worker.clone(),
        Arc::clone(&registry),
        Arc::clone(&extraction_provider),
        Arc::clone(&triage_provider),
        Arc::clone(&relation_provider),
        Arc::clone(&embedding_provider),
        extraction_key.clone(),
        triage_key.clone(),
        embedding_key.clone(),
        relation_key.clone(),
        cancellation_token.clone(),
        config.worker.clone(),
        config.logging.include_llm_content,
        instance_id.to_string(),
        Arc::clone(&job_state_txs),
    ));

    // -- AppState assembly ---------------------------------------------------

    let base = AppState::builder()
        .pool_mcp(pool_mcp)
        .pool_worker(pool_worker)
        .instance_id(instance_id)
        .active_prompt_versions(Arc::new(RwLock::new(active_prompt_versions)))
        .provider_registry(registry)
        .embedding_provider(embedding_provider)
        .extraction_provider(extraction_provider)
        .triage_provider(triage_provider)
        .relation_provider(relation_provider)
        .embedding_key(embedding_key)
        .extraction_key(extraction_key)
        .triage_key(triage_key)
        .relation_key(relation_key)
        .worker_config(config.worker.clone())
        .server_config(Arc::new(config.server.clone()))
        .cancellation_token(cancellation_token)
        .job_state_txs(job_state_txs);

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
