//! Worker struct, construction, and the poll-claim-dispatch loop.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::Utc;
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tribal_db::{
    ExtractionResultRepository, JobRepository, JobStatusTransition, NewExtractionResult, NewTask,
    PgExtractionResultRepository, PgJobRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{Job, JobId, JobOutcome, JobStatus, Task, TaskType};
use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderRegistry, Usage,
};

use super::{
    backoff::{BACKOFF_CAP_SECS, backoff_duration},
    heartbeat::{HeartbeatHandle, run_reclaim_sweep, run_startup_reclaim, spawn_heartbeat},
};
use crate::{
    config::WorkerConfig,
    error::{StageError, WorkerError},
    stages::{StageCommit, StageOutput},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SEMAPHORE_CLOSED: &str = "semaphore closed unexpectedly";

const STAGE_PRE_DISPATCH: &str = "pre-dispatch";
const STAGE_EXTRACTION: &str = "extraction";

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
    #[allow(dead_code)]
    provider_registry: Arc<ProviderRegistry>,
    extraction_provider: Arc<dyn InferenceProvider>,
    #[allow(dead_code)]
    triage_provider: Arc<dyn InferenceProvider>,
    #[allow(dead_code)]
    relation_provider: Arc<dyn InferenceProvider>,
    #[allow(dead_code)]
    embedding_provider: Arc<dyn EmbeddingProvider>,
    #[allow(dead_code)]
    extraction_key: ProviderKey,
    #[allow(dead_code)]
    triage_inference_key: ProviderKey,
    #[allow(dead_code)]
    triage_embedding_key: ProviderKey,
    #[allow(dead_code)]
    relation_key: ProviderKey,
    cancellation_token: CancellationToken,
    config: WorkerConfig,
    instance_id: String,
    job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>>,
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
        extraction_key: ProviderKey,
        triage_inference_key: ProviderKey,
        triage_embedding_key: ProviderKey,
        relation_key: ProviderKey,
        cancellation_token: CancellationToken,
        config: WorkerConfig,
        instance_id: String,
        job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>>,
    ) -> Self {
        Self {
            pool,
            provider_registry,
            extraction_provider,
            triage_provider,
            relation_provider,
            embedding_provider,
            extraction_key,
            triage_inference_key,
            triage_embedding_key,
            relation_key,
            cancellation_token,
            config,
            instance_id,
            job_state_txs,
            active_tasks: Arc::new(AtomicUsize::new(0)),
            peak_concurrent: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns a reference to the worker configuration.
    #[must_use]
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Returns a reference to the extraction inference provider.
    pub(crate) fn extraction_provider(&self) -> &Arc<dyn InferenceProvider> {
        &self.extraction_provider
    }

    /// Returns the high-water mark of simultaneously in-flight tasks
    /// observed since the worker was created.
    #[must_use]
    pub fn peak_concurrent(&self) -> usize {
        self.peak_concurrent.load(Ordering::SeqCst)
    }

    /// Reclaims stale tasks that were left claimed by a previous worker
    /// instance.
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

        // Heal any jobs left stuck from a previous instance that
        // crashed between reclaim-sweep dead-lettering and the job
        // failure transition.
        match self.pool.acquire().await {
            Ok(mut conn) => {
                match PgJobRepository
                    .fail_stale_dead_lettered_jobs(&mut conn)
                    .await
                {
                    Ok(job_ids) => {
                        for job_id in &job_ids {
                            self.notify_job_state(*job_id);
                            self.job_state_txs.remove(job_id);
                        }
                        if !job_ids.is_empty() {
                            tracing::warn!(
                                count = job_ids.len(),
                                "transitioned stuck jobs to failed after startup reclaim",
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to transition dead-lettered jobs during startup");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "pool acquire failed for startup job healing");
            }
        }

        Ok(stats.total())
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

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    reclaim_handle.abort();
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

            let mut conn = match self.pool.acquire().await {
                Ok(c) => c,
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

        let job: Job = {
            let mut conn = match self.pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    let stage_err = StageError::Database {
                        stage: STAGE_PRE_DISPATCH.into(),
                        context: format!("acquiring connection for job {job_id}"),
                        source: tribal_db::DbError::QueryFailed {
                            context: "pool acquire".into(),
                            source: e,
                        },
                    };
                    self.handle_stage_failure(&task, &stage_err).await;
                    return;
                }
            };
            match PgJobRepository.find_by_id(&mut conn, job_id).await {
                Ok(j) => j,
                Err(e) => {
                    let stage_err = StageError::Database {
                        stage: STAGE_PRE_DISPATCH.into(),
                        context: format!("loading job {job_id}"),
                        source: e,
                    };
                    self.handle_stage_failure(&task, &stage_err).await;
                    return;
                }
            }
        };

        // Best-effort job status transition: moves the job to the
        // in-progress state corresponding to this task type (e.g.
        // Extraction → Extracting).  Non-transactional and fire-and-
        // forget — a failure here does not block task execution.
        let target_status = job_status_for_task_type(task.task_type());
        if job.status() != target_status
            && let Ok(mut conn) = self.pool.acquire().await
        {
            let transition = JobStatusTransition::builder().status(target_status).build();
            let _ = PgJobRepository
                .update_status(&mut conn, job_id, &transition)
                .await;
        }

        // Notify job-state watch subscribers so they can observe the
        // claim-time status change.
        self.notify_job_state(job_id);

        let Some(claim_token) = task.claim_token() else {
            tracing::error!(task_id = %task.id(), "task has no claim token after claiming");
            return;
        };

        let HeartbeatHandle {
            mut ownership_lost_rx,
            abort_handle,
        } = spawn_heartbeat(
            self.pool.clone(),
            task.id(),
            claim_token,
            self.config.heartbeat_interval(),
            self.cancellation_token.clone(),
        );

        let stage_result = tokio::select! {
            () = self.cancellation_token.cancelled() => {
                abort_handle.abort();
                tracing::info!(task_id = %task.id(), "task cancelled mid-execution");
                return;
            }
            () = tokio::time::sleep(self.config.task_timeout()) => {
                Err(StageError::Timeout {
                    timeout_seconds: self.config.task_timeout_seconds,
                })
            }
            Ok(()) = &mut ownership_lost_rx => {
                Err(StageError::OwnershipLost)
            }
            result = self.dispatch_stage(&job, &task) => {
                result
            }
        };

        match stage_result {
            Ok(output) => {
                self.record_token_usage(&job, &task, &output.usages).await;

                if self.cancellation_token.is_cancelled() {
                    abort_handle.abort();
                    tracing::info!(
                        task_id = %task.id(),
                        "cancellation detected after stage; skipping commit",
                    );
                    return;
                }

                if let Err(e) = self.commit_domain_effects(&task, output.commit).await {
                    self.handle_stage_failure(&task, &e).await;
                }
            }
            Err(e) => {
                self.handle_stage_failure(&task, &e).await;
            }
        }

        abort_handle.abort();
    }

    /// Routes to the correct stage based on task type.
    async fn dispatch_stage(&self, job: &Job, task: &Task) -> Result<StageOutput, StageError> {
        match task.task_type() {
            TaskType::Extraction => self.run_extraction(job, task).await,
            TaskType::Triage => self.run_triage(job, task).await,
            TaskType::Relation => self.run_relation(job, task).await,
        }
    }

    /// Handles a stage failure: computes backoff, fails the task via
    /// [`TaskRepository::fail`], and optionally transitions the parent
    /// job to `Failed` when the task is dead-lettered.
    ///
    /// All mutations (task fail + optional job transition) are composed
    /// in a single transaction so they commit or roll back atomically.
    async fn handle_stage_failure(&self, task: &Task, error: &StageError) {
        tracing::warn!(
            task_id = %task.id(),
            error = %error,
            "stage failed",
        );

        let error_kind = error.to_error_kind();
        let error_message = error.to_string();
        let post_increment_retry = task.retry_count() + 1;
        #[allow(clippy::cast_possible_truncation)]
        let task_seed = task.id().inner().as_u128() as u64;
        let available_at = Utc::now() + backoff_duration(post_increment_retry, task_seed);
        let is_dead_lettered = post_increment_retry > self.config.task_max_retries;

        // Determine upfront whether the job should fail — needed after
        // commit to decide whether to notify and clean up the watch map.
        let job_failed = is_dead_lettered
            && matches!(task.task_type(), TaskType::Extraction | TaskType::Relation);

        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    task_id = %task.id(),
                    "failed to acquire connection for failure handling",
                );
                return;
            }
        };

        let Some(claim_token) = task.claim_token() else {
            tracing::error!(task_id = %task.id(), "task has no claim token");
            return;
        };

        let mut txn = match sqlx::Connection::begin(&mut *conn).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "failed to begin failure transaction");
                return;
            }
        };

        let rows_affected = match PgTaskRepository
            .fail(
                &mut txn,
                task.id(),
                claim_token,
                self.config.task_max_retries,
                available_at,
                error_kind,
                &error_message,
            )
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(error = %e, task_id = %task.id(), "failed to fail task");
                return;
            }
        };

        if rows_affected == 0 {
            tracing::warn!(task_id = %task.id(), "ownership lost during failure handling");
            return;
        }

        // When a task exhausts its retry budget, dead-lettering is
        // terminal for Extraction and Relation tasks — both imply the
        // job cannot progress, so the job transitions to Failed.
        // Triage failures are non-fatal: remaining triage tasks can
        // still succeed, and the relation stage runs on whatever
        // triage results are available.
        if job_failed {
            let transition = JobStatusTransition::builder()
                .status(JobStatus::Failed)
                .outcome(Some(JobOutcome::Failure))
                .error_message(Some(error_message))
                .completed_at(Some(Utc::now()))
                .build();
            if let Err(e) = PgJobRepository
                .update_status(&mut txn, task.job_id(), &transition)
                .await
            {
                tracing::error!(
                    error = %e,
                    job_id = %task.job_id(),
                    "failed to transition job to Failed on dead-letter",
                );
                return;
            }
        }

        match txn.commit().await {
            Ok(()) => {
                if job_failed {
                    self.notify_job_state(task.job_id());
                    self.job_state_txs.remove(&task.job_id());
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to commit failure transaction");
            }
        }
    }

    /// Commits domain effects produced by a successful stage.
    async fn commit_domain_effects(
        &self,
        task: &Task,
        commit: StageCommit,
    ) -> Result<(), StageError> {
        match commit {
            StageCommit::Extraction {
                extraction_result,
                triage_tasks,
                batch_size,
                original_count,
            } => {
                self.commit_extraction(
                    task,
                    extraction_result,
                    triage_tasks,
                    batch_size,
                    original_count,
                )
                .await
            }
        }
    }

    /// Commits extraction stage effects within a single transaction:
    ///
    /// 1. Inserts the extraction result.
    /// 2. Creates triage tasks (skipped when `batch_size == 0`).
    /// 3. Updates the job's batch size and original count.
    /// 4. Transitions the job status to `Triaging` (or `Completed` /
    ///    `Empty` when zero candidates were extracted).
    /// 5. Marks the task as completed, guarded by claim token.
    async fn commit_extraction(
        &self,
        task: &Task,
        extraction_result: NewExtractionResult,
        triage_tasks: Vec<NewTask>,
        batch_size: u32,
        original_count: u32,
    ) -> Result<(), StageError> {
        let Some(claim_token) = task.claim_token() else {
            return Err(StageError::OwnershipLost);
        };

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "acquiring connection".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;

        let mut txn =
            sqlx::Connection::begin(&mut *conn)
                .await
                .map_err(|e| StageError::Database {
                    stage: STAGE_EXTRACTION.into(),
                    context: "beginning transaction".into(),
                    source: tribal_db::DbError::QueryFailed {
                        context: "begin".into(),
                        source: e,
                    },
                })?;

        PgExtractionResultRepository
            .insert(&mut txn, &extraction_result)
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "inserting extraction result".into(),
                source: e,
            })?;

        let is_empty = batch_size == 0;

        if !is_empty {
            for new_task in &triage_tasks {
                PgTaskRepository
                    .insert(&mut txn, new_task)
                    .await
                    .map_err(|e| StageError::Database {
                        stage: STAGE_EXTRACTION.into(),
                        context: "creating triage task".into(),
                        source: e,
                    })?;
            }
        }

        PgJobRepository
            .update_batch_size(&mut txn, task.job_id(), batch_size, original_count)
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "updating batch size".into(),
                source: e,
            })?;

        // Zero-candidate path: when extraction produces no candidates,
        // the job completes immediately with an Empty outcome — no
        // triage or relation stages are needed.
        let job_transition = if is_empty {
            JobStatusTransition::builder()
                .status(JobStatus::Completed)
                .outcome(Some(JobOutcome::Empty))
                .completed_at(Some(Utc::now()))
                .build()
        } else {
            JobStatusTransition::builder()
                .status(JobStatus::Triaging)
                .build()
        };

        PgJobRepository
            .update_status(&mut txn, task.job_id(), &job_transition)
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "transitioning job status".into(),
                source: e,
            })?;

        let rows = PgTaskRepository
            .complete(&mut txn, task.id(), claim_token)
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_EXTRACTION.into(),
                context: "completing task".into(),
                source: e,
            })?;

        if rows == 0 {
            return Err(StageError::OwnershipLost);
        }

        txn.commit().await.map_err(|e| StageError::Database {
            stage: STAGE_EXTRACTION.into(),
            context: "committing transaction".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "commit".into(),
                source: e,
            },
        })?;

        // Notify watch subscribers of the job state change.
        self.notify_job_state(task.job_id());

        // Clean up watch channel entry for terminal job transitions.
        if is_empty {
            self.job_state_txs.remove(&task.job_id());
        }

        Ok(())
    }

    /// Records token usage for a completed stage.
    #[allow(clippy::unused_self, clippy::unused_async)]
    async fn record_token_usage(&self, _job: &Job, _task: &Task, _usages: &[Usage]) {
        // Implemented by ticket 4.6
    }

    /// Sends a wake-up signal to any watch subscribers for the given job.
    fn notify_job_state(&self, job_id: JobId) {
        if let Some(tx) = self.job_state_txs.get(&job_id) {
            let _ = tx.send(());
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
                        match self.pool.acquire().await {
                            Ok(mut conn) => {
                                match PgJobRepository
                                    .fail_stale_dead_lettered_jobs(&mut conn)
                                    .await
                                {
                                    Ok(job_ids) => {
                                        for job_id in &job_ids {
                                            self.notify_job_state(*job_id);
                                            self.job_state_txs.remove(job_id);
                                        }
                                        if !job_ids.is_empty() {
                                            tracing::warn!(
                                                count = job_ids.len(),
                                                "transitioned stuck jobs to failed after reclaim",
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "failed to transition dead-lettered jobs",
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "pool acquire failed for reclaim job healing",
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reclaim sweep failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Clamps a `usize` to `u32`, saturating at [`u32::MAX`].
fn clamp_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Computes the next claim-cycle backoff by doubling the current poll
/// interval, capping at [`BACKOFF_CAP_SECS`], and flooring at 1 s.
fn next_claim_backoff(current: std::time::Duration) -> std::time::Duration {
    let doubled = current.as_secs().saturating_mul(2);
    let capped = doubled.min(BACKOFF_CAP_SECS);
    std::time::Duration::from_secs(capped.max(1))
}

/// Maps a task type to the corresponding in-progress job status.
fn job_status_for_task_type(task_type: TaskType) -> JobStatus {
    match task_type {
        TaskType::Extraction => JobStatus::Extracting,
        TaskType::Triage => JobStatus::Triaging,
        TaskType::Relation => JobStatus::Relating,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clamp_to_u32_within_range() {
        assert_eq!(clamp_to_u32(42), 42);
        assert_eq!(clamp_to_u32(0), 0);
    }

    #[test]
    fn test_clamp_to_u32_saturates() {
        assert_eq!(clamp_to_u32(usize::MAX), u32::MAX);
    }

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

    #[test]
    fn test_job_status_for_task_type() {
        assert_eq!(
            job_status_for_task_type(TaskType::Extraction),
            JobStatus::Extracting,
        );
        assert_eq!(
            job_status_for_task_type(TaskType::Triage),
            JobStatus::Triaging,
        );
        assert_eq!(
            job_status_for_task_type(TaskType::Relation),
            JobStatus::Relating,
        );
    }
}
