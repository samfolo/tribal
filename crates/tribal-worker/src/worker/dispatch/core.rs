//! Worker struct, construction, and the poll-claim-dispatch loop.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tribal_common::{JobStateTxs, POOL_NAME_WORKER, clamp_to_i32, clamp_to_u32};
use tribal_config::{CredentialCatalogue, WorkerConfig};
use tribal_db::{
    JobRepository, JobStatusTransition, NewTask, NewTokenUsage, PgJobRepository,
    PgPrincipalRepository, PgTaskRepository, PgTokenUsageRepository, PrincipalRepository,
    TaskRepository, TokenUsageRepository,
};
use tribal_domain::{
    Job, JobId, JobState, JobStatus, PromptVersionId, Task, TaskType, TokenUsageStage, span_attrs,
};
use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderRegistry, Usage,
};
use tribal_telemetry::MetricsRecorder;

use crate::{
    error::{SEMAPHORE_CLOSED, STAGE_PRE_DISPATCH, StageError, WorkerError},
    stages::StageOutput,
    worker::{
        backfill::BackfillProcessor,
        backoff::BACKOFF_CAP_SECS,
        heartbeat::{run_reclaim_sweep, run_startup_reclaim, spawn_heartbeat},
        reindex::{EmbeddingProviderCache, drive_reindex_cycle, reconcile_orphan_building_profile},
    },
};

/// Cadence at which the reindex loop polls for a live run to drive. A reindex is
/// a rare, operator-initiated event, so this is a fixed liveness-detection
/// interval rather than a tuned throughput knob; once a run is live, the driver
/// runs it through to completion without waiting on this poll.
const REINDEX_POLL_INTERVAL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// Core worker driving the poll-claim-dispatch loop.
///
/// Constructed via [`Worker::new`] with explicit dependency injection.
/// Call [`Worker::run`] to start the loop; it runs until the
/// cancellation token is triggered.
pub struct Worker {
    pool: PgPool,
    provider_registry: Arc<ProviderRegistry>,
    pub(crate) extraction_provider: Arc<dyn InferenceProvider>,
    pub(crate) triage_provider: Arc<dyn InferenceProvider>,
    pub(crate) relation_provider: Arc<dyn InferenceProvider>,
    pub(crate) embedding_provider: Arc<dyn EmbeddingProvider>,
    /// Embedding providers built for reindex building profiles, keyed by profile
    /// id. The reindex driver populates it; the commit path reads it.
    embedding_providers: EmbeddingProviderCache,
    /// The embedding-credential catalogue, used to resolve a reindex target
    /// provider's credential fail-closed.
    credentials: CredentialCatalogue,
    pub(crate) extraction_key: ProviderKey,
    pub(crate) triage_inference_key: ProviderKey,
    pub(crate) triage_embedding_key: ProviderKey,
    pub(crate) relation_key: ProviderKey,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    include_llm_content: bool,
    instance_id: String,
    job_state_txs: JobStateTxs,
    metrics: Arc<dyn MetricsRecorder>,
    /// Current number of in-flight tasks.
    active_tasks: Arc<AtomicUsize>,
    /// High-water mark of simultaneously in-flight tasks.
    peak_concurrent: Arc<AtomicUsize>,
}

impl Worker {
    /// Creates a new worker with all dependencies injected.
    ///
    /// The `job_state_txs` map is shared with MCP handlers so they
    /// can subscribe to job status changes via `watch` channels.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: PgPool,
        provider_registry: Arc<ProviderRegistry>,
        extraction_provider: Arc<dyn InferenceProvider>,
        triage_provider: Arc<dyn InferenceProvider>,
        relation_provider: Arc<dyn InferenceProvider>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
        embedding_providers: EmbeddingProviderCache,
        credentials: CredentialCatalogue,
        extraction_key: ProviderKey,
        triage_inference_key: ProviderKey,
        triage_embedding_key: ProviderKey,
        relation_key: ProviderKey,
        cancellation_token: CancellationToken,
        config: WorkerConfig,
        include_llm_content: bool,
        instance_id: String,
        job_state_txs: JobStateTxs,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            pool,
            provider_registry,
            extraction_provider,
            triage_provider,
            relation_provider,
            embedding_provider,
            embedding_providers,
            credentials,
            extraction_key,
            triage_inference_key,
            triage_embedding_key,
            relation_key,
            cancellation_token,
            config,
            include_llm_content,
            instance_id,
            job_state_txs,
            metrics,
            active_tasks: Arc::new(AtomicUsize::new(0)),
            peak_concurrent: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns a reference to the worker configuration.
    #[must_use]
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Returns whether raw LLM content should be included in log output.
    pub(crate) fn include_llm_content(&self) -> bool {
        self.include_llm_content
    }

    /// Returns a reference to the database pool.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a reference to the provider registry.
    pub(crate) fn provider_registry(&self) -> &Arc<ProviderRegistry> {
        &self.provider_registry
    }

    /// Returns the cache of embedding providers built for reindex profiles.
    pub(crate) fn embedding_providers(&self) -> &EmbeddingProviderCache {
        &self.embedding_providers
    }

    /// Returns the embedding-credential catalogue.
    pub(crate) fn credentials(&self) -> &CredentialCatalogue {
        &self.credentials
    }

    /// Returns a reference to the telemetry metric instruments.
    pub(crate) fn metrics(&self) -> &dyn MetricsRecorder {
        &self.metrics
    }

    /// Returns the high-water mark of simultaneously in-flight tasks
    /// observed since the worker was created.
    #[must_use]
    pub fn peak_concurrent(&self) -> usize {
        self.peak_concurrent.load(Ordering::SeqCst)
    }

    /// Runs all startup operations: reclaims orphaned tasks, heals
    /// dead-lettered jobs, and backfills missing data.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on database failures during reclaim.
    pub async fn startup(&self) -> Result<(), WorkerError> {
        tracing::info!(instance_id = %self.instance_id, "worker startup began");
        self.startup_reclaim().await?;
        self.run_startup_backfills().await;
        tracing::info!(instance_id = %self.instance_id, "worker startup complete");
        Ok(())
    }

    /// Reclaims stale tasks that were left claimed by a previous worker
    /// instance and heals any dead-lettered jobs.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on database failures.
    pub async fn startup_reclaim(&self) -> Result<u32, WorkerError> {
        let stats = run_startup_reclaim(
            &self.pool,
            self.config.task_timeout(),
            self.config.task_max_retries,
        )
        .await?;

        if stats.total() > 0 {
            tracing::info!(
                requeued = stats.requeued,
                dead_lettered = stats.dead_lettered,
                "startup reclaim recovered orphaned tasks",
            );
        }

        self.heal_dead_lettered_jobs().await;
        self.heal_stuck_triaging_jobs().await;

        Ok(stats.total())
    }

    /// Runs all startup backfill operations.
    async fn run_startup_backfills(&self) {
        let semaphore = self
            .provider_registry()
            .semaphore(&self.triage_embedding_key)
            .cloned();

        let Some(semaphore) = semaphore else {
            tracing::warn!("triage embedding key not registered, skipping tag backfill");
            return;
        };

        let processor = BackfillProcessor::new(
            self.pool.clone(),
            Arc::clone(&self.embedding_provider),
            semaphore,
            self.cancellation_token.clone(),
        );

        let outcome = processor.tag_embeddings().await;

        if outcome.cancelled {
            tracing::info!("tag embedding backfill interrupted by shutdown");
        } else if !outcome.is_empty() {
            tracing::info!(
                processed = outcome.processed,
                skipped = outcome.skipped,
                total = outcome.total,
                "tag embedding backfill complete",
            );
        }
    }

    /// Runs the main poll-claim-dispatch loop until cancellation.
    ///
    /// Each cycle sleeps for the configured poll interval, then claims up
    /// to `semaphore.available_permits()` queued tasks and spawns each
    /// onto a Tokio task guarded by a semaphore permit.  If a claim cycle
    /// fails, the poll interval doubles (capped at 60 s) as a transient-
    /// failure backoff; it resets on the next successful claim.
    ///
    /// On cancellation, the loop stops claiming new tasks and drains all
    /// in-flight tasks before returning.
    ///
    /// # Panics
    ///
    /// Panics if the internal semaphore is closed, which indicates a
    /// programming error in the worker lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Cancelled`] when the cancellation token
    /// is triggered.  Transient errors (pool exhaustion, claim failures)
    /// are handled internally via backoff — they do not terminate the
    /// loop.
    pub async fn run(self: &Arc<Self>) -> Result<(), WorkerError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut poll_interval = self.config.poll_interval();
        let mut in_flight = tokio::task::JoinSet::new();

        let reclaim_worker = Arc::clone(self);
        let reclaim_handle = tokio::spawn(async move {
            reclaim_worker.run_reclaim_loop().await;
        });

        let reindex_worker = Arc::clone(self);
        let reindex_handle = tokio::spawn(async move {
            reindex_worker.run_reindex_loop().await;
        });

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    reclaim_handle.abort();
                    reindex_handle.abort();
                    tracing::info!(instance_id = %self.instance_id, "worker cancelled, draining in-flight tasks");
                    while in_flight.join_next().await.is_some() {}
                    return Err(WorkerError::Cancelled);
                }
                () = tokio::time::sleep(poll_interval) => {}
            }

            // Claim up to the number of available semaphore permits.
            // When all permits are held, skip the claim cycle entirely
            // to avoid unnecessary database round-trips.
            let available = semaphore.available_permits();
            if available == 0 {
                continue;
            }

            let limit = clamp_to_u32(available);

            let acquire_start = Instant::now();
            let mut conn = match self.pool.acquire().await {
                Ok(c) => {
                    self.metrics
                        .record_pool_acquire(POOL_NAME_WORKER, acquire_start.elapsed());
                    c
                }
                Err(e) => {
                    tracing::warn!(error = %e, "pool acquire failed, backing off");
                    poll_interval = next_claim_backoff(poll_interval);
                    continue;
                }
            };

            let claim_result = PgTaskRepository
                .claim(&mut conn, limit, &self.instance_id)
                .await;

            match claim_result {
                Ok(tasks) => {
                    poll_interval = self.config.poll_interval();
                    for task in tasks {
                        tracing::info!(
                            task_id = %task.id(),
                            task_type = %task.task_type(),
                            job_id = %task.job_id(),
                            retry_count = task.retry_count(),
                            "task.claimed",
                        );
                        let permit = semaphore
                            .clone()
                            .acquire_owned()
                            .await
                            .expect(SEMAPHORE_CLOSED);
                        let worker = Arc::clone(self);
                        in_flight.spawn(async move {
                            worker.run_task(task).await;
                            drop(permit);
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "claim cycle failed, backing off");
                    poll_interval = next_claim_backoff(poll_interval);
                }
            }
        }
    }

    /// Dispatches a single claimed task through its pipeline stage.
    ///
    /// 1. Loads the parent job.
    /// 2. Transitions the job status to the in-progress state matching
    ///    the task type (e.g. Extraction → Extracting).  This is a
    ///    best-effort, non-transactional update — if it fails the task
    ///    still proceeds.
    /// 3. Notifies any job-state watch subscribers so they can observe
    ///    the status change.
    /// 4. Races the stage execution against the task timeout and the
    ///    cancellation token.
    /// 5. On success, records token usage and commits domain effects.
    ///    On failure, delegates to [`handle_stage_failure`](Self::handle_stage_failure).
    async fn run_task(&self, task: Task) {
        let active = self.active_tasks.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_concurrent.fetch_max(active, Ordering::SeqCst);

        // Cooperative yield so other spawned tasks can increment their
        // counters before this task proceeds.  Ensures peak_concurrent
        // captures true concurrency regardless of internal yield points.
        tokio::task::yield_now().await;

        self.run_task_inner(task).await;

        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
    }

    /// Inner task dispatch, separated from [`run_task`](Self::run_task)
    /// so the active-task counter is always decremented on all exit
    /// paths.
    async fn run_task_inner(&self, task: Task) {
        let job_id = task.job_id();

        let (job, principal_key): (Job, String) = {
            let acquire_start = Instant::now();
            let mut conn = match self.pool.acquire().await {
                Ok(c) => {
                    self.metrics
                        .record_pool_acquire(POOL_NAME_WORKER, acquire_start.elapsed());
                    c
                }
                Err(e) => {
                    let stage_err = StageError::Database {
                        stage: STAGE_PRE_DISPATCH.into(),
                        context: format!("acquiring connection for job {job_id}"),
                        source: tribal_db::DbError::QueryFailed {
                            context: "pool acquire".into(),
                            source: e,
                        },
                    };
                    self.handle_stage_failure(&task, None, &stage_err).await;
                    return;
                }
            };
            let job = match PgJobRepository.find_by_id(&mut conn, job_id).await {
                Ok(j) => j,
                Err(e) => {
                    let stage_err = StageError::Database {
                        stage: STAGE_PRE_DISPATCH.into(),
                        context: format!("loading job {job_id}"),
                        source: e,
                    };
                    self.handle_stage_failure(&task, None, &stage_err).await;
                    return;
                }
            };
            let principal_key = match PgPrincipalRepository
                .find_by_id(&mut conn, job.principal_id())
                .await
            {
                Ok(p) => p.principal_key().to_owned(),
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        principal_id = %job.principal_id(),
                        error = %e,
                        "failed to resolve principal for span; falling back to principal_id",
                    );
                    job.principal_id().to_string()
                }
            };
            (job, principal_key)
        };

        // -- Trace propagation: tribal.job span --------------------------------

        let job_span = tracing::info_span!(
            parent: None,
            "tribal.job",
            { span_attrs::JOB_ID } = %job.id(),
            { span_attrs::PROJECT_ID } = %job.project_id(),
            { span_attrs::PRINCIPAL_KEY } = principal_key.as_str(),
            { span_attrs::EPISODE_ID } = tracing::field::Empty,
            { span_attrs::TRACE_CONTEXT_INVALID } = tracing::field::Empty,
        );

        if let Some(episode_id) = job.correlation_id() {
            job_span.record(span_attrs::EPISODE_ID, tracing::field::display(episode_id));
        }

        if tribal_telemetry::parent_span_from_traceparent(&job_span, job.trace_context())
            .is_invalid()
        {
            job_span.record(span_attrs::TRACE_CONTEXT_INVALID, true);
        }

        async {
            // Best-effort job status transition: moves the job to the
            // in-progress state corresponding to this task type (e.g.
            // Extraction → Extracting).  Non-transactional and fire-and-
            // forget — a failure here does not block task execution.
            let target_status = JobStatus::from(task.task_type());
            if job.status() != target_status
                && let Ok(mut conn) = self.pool.acquire().await
            {
                let transition = tribal_db::JobStatusTransition::builder()
                    .status(target_status)
                    .build();
                let _ = PgJobRepository
                    .update_status(&mut conn, job_id, &transition)
                    .await;
            }

            // Notify job-state watch subscribers so they can observe the
            // claim-time status change.
            self.notify_job_state(job_id, JobState::from(target_status));

            let Some(claim_token) = task.claim_token() else {
                tracing::error!(task_id = %task.id(), "task has no claim token after claiming");
                return;
            };

            let mut heartbeat = spawn_heartbeat(
                self.pool.clone(),
                task.id(),
                claim_token,
                self.config.heartbeat_interval(),
                self.cancellation_token.clone(),
            );

            let deadline = tokio::time::Instant::now() + self.config.task_timeout();

            let stage_result = tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    heartbeat.abort();
                    tracing::info!(task_id = %task.id(), "task cancelled mid-execution");
                    return;
                }
                () = tokio::time::sleep(self.config.task_timeout()) => {
                    Err(StageError::Timeout {
                        timeout_millis: self.config.task_timeout_ms,
                    })
                }
                Ok(()) = &mut heartbeat.ownership_lost_rx => {
                    Err(StageError::OwnershipLost)
                }
                result = self.dispatch_stage(&job, &task, deadline) => {
                    result
                }
            };

            match stage_result {
                Ok(output) => {
                    for usage in &output.usages {
                        self.record_token_usage(&job, &task, usage).await;
                    }

                    if self.cancellation_token.is_cancelled() {
                        heartbeat.abort();
                        tracing::info!(
                            task_id = %task.id(),
                            "cancellation detected after stage; skipping commit",
                        );
                        return;
                    }

                    if let Err(e) = self.commit_domain_effects(&task, &job, output.commit).await {
                        self.handle_stage_failure(&task, Some(&job), &e).await;
                    }
                }
                Err(e) => {
                    self.handle_stage_failure(&task, Some(&job), &e).await;
                }
            }

            heartbeat.abort();
        }
        .instrument(job_span)
        .await;
    }

    /// Routes to the correct stage based on task type.
    async fn dispatch_stage(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        match task.task_type() {
            TaskType::Extraction => self.run_extraction(job, task, deadline).await,
            TaskType::Triage => self.run_triage(job, task, deadline).await,
            TaskType::Relation => self.run_relation(job, task, deadline).await,
        }
    }

    /// Records a single token usage record.
    ///
    /// Best-effort: logs a warning on failure without failing the task.
    /// Uses a freshly acquired connection from the pool (not the domain
    /// commit transaction) so recording is independent of task outcome.
    pub(super) async fn record_token_usage(&self, job: &Job, task: &Task, usage: &Usage) {
        let mut conn = match self.pool().acquire().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    task_id = %task.id(),
                    "failed to acquire connection for token usage recording",
                );
                return;
            }
        };

        let attempt = clamp_to_i32(task.retry_count());
        let trace_id = job
            .trace_context()
            .and_then(tribal_telemetry::trace_id_from_traceparent)
            .or_else(tribal_telemetry::current_trace_id);

        let new = match usage {
            Usage::Completion { usage: cu } => {
                let (system_pv_id, user_pv_id) = prompt_version_ids_for_task(job, task);
                NewTokenUsage::builder()
                    .job_id(Some(job.id()))
                    .task_id(Some(task.id()))
                    .attempt(attempt)
                    .stage(task.task_type().into())
                    .provider(cu.provider.clone())
                    .model(cu.model.clone())
                    .tokens_input(clamp_to_i32(cu.input_tokens))
                    .tokens_output(clamp_to_i32(cu.output_tokens))
                    .tokens_cache_read(clamp_to_i32(cu.cache_read_tokens))
                    .tokens_cache_write(clamp_to_i32(cu.cache_write_tokens))
                    .latency_ms(clamp_to_i32(cu.latency.as_millis()))
                    .system_prompt_version_id(Some(system_pv_id))
                    .user_prompt_version_id(Some(user_pv_id))
                    .trace_id(trace_id)
                    .build()
            }
            Usage::Embedding { usage: eu, purpose } => NewTokenUsage::builder()
                .job_id(Some(job.id()))
                .task_id(Some(task.id()))
                .attempt(attempt)
                .stage(TokenUsageStage::Embedding { purpose: *purpose })
                .provider(eu.provider.clone())
                .model(eu.model.clone())
                .tokens_input(clamp_to_i32(eu.total_tokens))
                .tokens_output(0)
                .latency_ms(clamp_to_i32(eu.latency.as_millis()))
                .trace_id(trace_id)
                .build(),
        };

        let stage = new.stage.pipeline_stage();
        match PgTokenUsageRepository.insert(&mut conn, &new).await {
            Ok(recorded) => {
                tracing::debug!(
                    task_id = %task.id(),
                    stage = %stage,
                    tokens_total = recorded.tokens_total(),
                    latency_ms = recorded.latency_ms(),
                    "token usage recorded",
                );
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task.id(),
                    stage = %stage,
                    error = %e,
                    "failed to record token usage",
                );
            }
        }
    }

    /// Sends a typed [`JobState`] to any watch subscribers for the given job.
    ///
    /// When `state` is terminal, stamps `terminal_at` on the entry so the
    /// background sweep can evict it after the configured TTL.
    pub(super) fn notify_job_state(&self, job_id: JobId, state: JobState) {
        if let Some(mut entry) = self.job_state_txs.get_mut(&job_id) {
            let _ = entry.sender.send(state);
            if state.is_terminal() {
                entry.stamp_terminal();
            }
        }
    }

    /// Transitions jobs with dead-lettered extraction or relation tasks
    /// to `Failed` and notifies watch subscribers.  Best-effort — failures
    /// are logged but not propagated.
    async fn heal_dead_lettered_jobs(&self) {
        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "pool acquire failed for job healing");
                return;
            }
        };

        match PgJobRepository
            .fail_stale_dead_lettered_jobs(&mut conn)
            .await
        {
            Ok(job_ids) => {
                for job_id in &job_ids {
                    self.notify_job_state(*job_id, JobState::Failed);
                }
                if !job_ids.is_empty() {
                    tracing::warn!(count = job_ids.len(), "transitioned stuck jobs to failed");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to transition dead-lettered jobs");
            }
        }
    }

    /// Creates relation tasks for jobs stuck in `Triaging` where all
    /// triage tasks are terminal but no relation task exists.
    ///
    /// Each stuck job is healed in its own transaction so that a
    /// failure for one job does not block others.  Best-effort —
    /// failures are logged but not propagated.
    async fn heal_stuck_triaging_jobs(&self) {
        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "pool acquire failed for triaging job healing");
                return;
            }
        };

        let stuck_job_ids = match PgJobRepository.find_stuck_triaging_jobs(&mut conn).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "failed to detect stuck triaging jobs");
                return;
            }
        };

        for job_id in &stuck_job_ids {
            let Ok(mut txn) = sqlx::Connection::begin(&mut *conn).await else {
                tracing::warn!(job_id = %job_id, "failed to begin healing transaction");
                continue;
            };

            let new_task = NewTask::builder()
                .job_id(*job_id)
                .task_type(TaskType::Relation)
                .build();

            if let Err(e) = PgTaskRepository.upsert(&mut txn, &new_task).await {
                tracing::warn!(job_id = %job_id, error = %e, "failed to create relation task for stuck job");
                let _ = txn.rollback().await;
                continue;
            }

            let transition = JobStatusTransition::builder()
                .status(JobStatus::Relating)
                .build();

            if let Err(e) = PgJobRepository
                .update_status(&mut txn, *job_id, &transition)
                .await
            {
                tracing::warn!(job_id = %job_id, error = %e, "failed to transition stuck job to relating");
                let _ = txn.rollback().await;
                continue;
            }

            let Ok(()) = txn.commit().await else {
                tracing::warn!(job_id = %job_id, "failed to commit healing transaction");
                continue;
            };

            self.notify_job_state(*job_id, JobState::Relating);
        }

        if !stuck_job_ids.is_empty() {
            tracing::warn!(count = stuck_job_ids.len(), "healed stuck triaging jobs");
        }
    }

    /// Runs the periodic reclaim sweep until cancellation.
    ///
    /// Sweeps for stale heartbeats every `reclaim_interval`, requeuing
    /// or dead-lettering abandoned tasks.  When dead-lettered tasks are
    /// found, transitions their parent jobs to `Failed` if appropriate.
    async fn run_reclaim_loop(&self) {
        let mut ticker = tokio::time::interval(self.config.reclaim_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip first immediate tick

        let limit = clamp_to_u32(self.config.max_concurrent_tasks.saturating_mul(2));

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    return;
                }
                _ = ticker.tick() => {}
            }

            match run_reclaim_sweep(
                &self.pool,
                self.config.task_timeout(),
                self.config.task_max_retries,
                limit,
            )
            .await
            {
                Ok(stats) => {
                    if stats.dead_lettered > 0 {
                        tracing::warn!(
                            requeued = stats.requeued,
                            dead_lettered = stats.dead_lettered,
                            "reclaim sweep dead-lettered tasks",
                        );
                    } else if stats.requeued > 0 {
                        tracing::info!(
                            requeued = stats.requeued,
                            "reclaim sweep requeued stale tasks",
                        );
                    }

                    if stats.dead_lettered > 0 {
                        self.heal_dead_lettered_jobs().await;
                    }

                    self.heal_stuck_triaging_jobs().await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reclaim sweep failed");
                }
            }
        }
    }

    /// Drives the single live reindex run: reconciles an orphan building profile
    /// on boot, then polls for a live run to promote and enrol. Sibling to the
    /// reclaim loop, it returns on cancellation.
    async fn run_reindex_loop(&self) {
        self.reindex_boot_reconcile().await;

        let mut ticker = tokio::time::interval(REINDEX_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    return;
                }
                _ = ticker.tick() => {}
            }

            let mut conn = match self.pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "reindex loop pool acquire failed");
                    continue;
                }
            };

            if let Err(e) = drive_reindex_cycle(
                &mut conn,
                &self.provider_registry,
                &self.embedding_providers,
                &self.credentials,
                &self.instance_id,
            )
            .await
            {
                tracing::warn!(error = %e, "reindex drive cycle failed");
            }
        }
    }

    /// Fails a building profile orphaned by a crashed run, once at boot.
    ///
    /// Runs in a transaction so reconcile holds the transaction-scoped
    /// single-flight lock across its read-then-fail.
    async fn reindex_boot_reconcile(&self) {
        let mut txn = match self.pool.begin().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "reindex boot reconcile begin failed");
                return;
            }
        };
        match reconcile_orphan_building_profile(&mut txn).await {
            Ok(reconciled) => {
                if let Err(e) = txn.commit().await {
                    tracing::warn!(error = %e, "reindex boot reconcile commit failed");
                    return;
                }
                if reconciled {
                    tracing::warn!("failed an orphan building profile left by a crashed reindex");
                }
            }
            Err(e) => tracing::warn!(error = %e, "reindex boot reconcile failed"),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the `(system, user)` prompt version pair for the task's stage.
fn prompt_version_ids_for_task(job: &Job, task: &Task) -> (PromptVersionId, PromptVersionId) {
    match task.task_type() {
        TaskType::Extraction => (
            job.extraction_system_prompt_version_id(),
            job.extraction_user_prompt_version_id(),
        ),
        TaskType::Triage => (
            job.triage_system_prompt_version_id(),
            job.triage_user_prompt_version_id(),
        ),
        TaskType::Relation => (
            job.relation_system_prompt_version_id(),
            job.relation_user_prompt_version_id(),
        ),
    }
}

/// Computes the next claim-cycle backoff by doubling the current poll
/// interval, capping at [`BACKOFF_CAP_SECS`], and flooring at 1 s.
fn next_claim_backoff(current: std::time::Duration) -> std::time::Duration {
    let doubled = current.as_secs().saturating_mul(2);
    let capped = doubled.min(BACKOFF_CAP_SECS);
    std::time::Duration::from_secs(capped.max(1))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claim_backoff_doubles_and_caps() {
        let one_sec = std::time::Duration::from_secs(1);
        let two_sec = std::time::Duration::from_secs(2);

        // Doubles from initial poll interval.
        assert_eq!(next_claim_backoff(one_sec), two_sec);
        assert_eq!(
            next_claim_backoff(two_sec),
            std::time::Duration::from_secs(4)
        );

        // Caps at BACKOFF_CAP_SECS.
        let near_cap = std::time::Duration::from_secs(BACKOFF_CAP_SECS - 1);
        assert_eq!(
            next_claim_backoff(near_cap),
            std::time::Duration::from_secs(BACKOFF_CAP_SECS),
        );

        // Already at cap stays at cap.
        let at_cap = std::time::Duration::from_secs(BACKOFF_CAP_SECS);
        assert_eq!(
            next_claim_backoff(at_cap),
            std::time::Duration::from_secs(BACKOFF_CAP_SECS),
        );

        // Zero-second interval floors at 1 s (not 0).
        let zero = std::time::Duration::from_secs(0);
        assert_eq!(next_claim_backoff(zero), one_sec);
    }
}
