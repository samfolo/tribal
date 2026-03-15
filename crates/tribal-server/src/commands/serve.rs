//! Implementation of the `tribal serve` subcommand.
//!
//! Runs the full infrastructure bootstrap (database, migrations, providers,
//! prompts, project resolution) and assembles [`AppState`].  Launches dual
//! tokio runtimes — one for MCP transport, one for the worker poll loop —
//! coordinated by a shared [`CancellationToken`].

use std::{path::PathBuf, sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio::{
    runtime::Builder,
    sync::{RwLock, oneshot, watch},
};
use tokio_util::sync::CancellationToken;
use tribal_config::{TribalConfig, load_config, validate};
use tribal_domain::JobId;
use tribal_mcp::{AppState, HandlerConfig};
use tribal_worker::{Worker, WorkerError};

use crate::{
    cli::ServeArgs,
    error::AppError,
    startup::{
        POOL_NAME_MCP, POOL_NAME_WORKER, build_embedding_provider, build_inference_provider,
        build_provider_registry, check_first_run, create_pool_with_retry, ensure_prompt_files,
        generate_instance_id, load_prompts, resolve_project, run_migrations,
    },
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal serve` startup sequence and blocks until shutdown.
///
/// Creates two tokio runtimes (`tribal-main` for MCP handlers,
/// `tribal-worker` for the poll-claim-dispatch loop), runs worker
/// startup, then blocks until the cancellation token fires.
///
/// # Errors
///
/// Returns an [`AppError`] if any startup phase fails or if the worker
/// dies unexpectedly during operation.
pub(crate) fn run(config_path: &str, args: ServeArgs) -> Result<(), AppError> {
    let (cli_overrides, cli_project) = args.into_cli_overrides();

    let config = load_config(config_path, Some(cli_overrides))?;
    validate(&config)?;

    let handler_config = HandlerConfig::from(&config).with_pool_name(POOL_NAME_MCP);

    // Telemetry must be initialised before the async runtime so the guard
    // outlives `block_on` and flushes pending writes on shutdown.
    let _telemetry_guard = tribal_telemetry::init_subscriber(&config.logging)?;

    let cancellation_token = CancellationToken::new();
    let job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>> = Arc::new(DashMap::new());

    // -- Main runtime --------------------------------------------------------

    let main_rt = Builder::new_multi_thread()
        .thread_name("tribal-main")
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let (_state, worker) = main_rt.block_on(bootstrap(
        &config,
        cli_project,
        handler_config,
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

    let (death_tx, mut death_rx) = oneshot::channel::<()>();

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

    tracing::info!("startup sequence complete");

    // -- MCP transport placeholder -------------------------------------------
    // Blocks until the cancellation token fires.  Replaced by transport
    // launch in a subsequent ticket.
    main_rt.block_on(cancellation_token.cancelled());

    // -- Graceful shutdown ---------------------------------------------------
    // Await worker completion within the shutdown deadline.  This gives
    // Worker::run()'s internal drain time to finish in-flight tasks
    // before the runtime is force-dropped.

    let deadline = Duration::from_millis(config.server.shutdown_deadline_ms);
    if worker_rt
        .block_on(tokio::time::timeout(deadline, worker_handle))
        .is_err()
    {
        tracing::warn!(
            deadline_ms = config.server.shutdown_deadline_ms,
            "shutdown deadline expired; dropping worker runtime",
        );
    }

    let worker_died = matches!(death_rx.try_recv(), Ok(()));

    drop(worker_rt);

    tracing::info!("shutdown complete");

    if worker_died {
        return Err(AppError::WorkerDeath);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Asynchronous startup sequence: pools, migrations, providers, prompts,
/// project resolution, `AppState` assembly, and `Worker` construction.
async fn bootstrap(
    config: &TribalConfig,
    cli_project: Option<String>,
    _handler_config: HandlerConfig,
    cancellation_token: CancellationToken,
    job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>>,
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
        build_inference_provider(&registry, &config.inference.extraction)?;

    let (triage_provider, triage_key) =
        build_inference_provider(&registry, &config.inference.triage)?;

    let (relation_provider, relation_key) =
        build_inference_provider(&registry, &config.inference.relation)?;

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
