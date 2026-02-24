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

use super::backoff::backoff_duration;
use crate::{
    config::WorkerConfig,
    error::{StageError, WorkerError},
    stages::{StageCommit, StageOutput},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SEMAPHORE_CLOSED: &str = "semaphore closed unexpectedly";

const STAGE_EXTRACTION: &str = "extraction";

/// Maximum backoff duration in seconds when claim cycles fail
/// repeatedly.  The poll interval doubles on each failure until it
/// reaches this cap, then resets on the next successful claim.
const MAX_CLAIM_BACKOFF_SECS: u64 = 60;

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
    #[allow(dead_code)]
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
    pub(crate) config: WorkerConfig,
    instance_id: String,
    #[allow(dead_code)]
    job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>>,
    /// Current number of in-flight tasks.
    active_tasks: Arc<AtomicUsize>,
    /// High-water mark of simultaneously in-flight tasks.
    peak_concurrent: Arc<AtomicUsize>,
}

impl Worker {
    /// Creates a new worker with all dependencies injected.
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
            job_state_txs: Arc::new(DashMap::new()),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            peak_concurrent: Arc::new(AtomicUsize::new(0)),
        }
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
    #[allow(clippy::unused_async)]
    pub async fn startup_reclaim(&self) -> Result<u64, WorkerError> {
        // Implemented by ticket 4.2
        Ok(0)
    }

    /// Runs the main poll-claim-dispatch loop until cancellation.
    ///
    /// Each cycle sleeps for the configured poll interval, then claims up
    /// to `semaphore.available_permits()` queued tasks and spawns each
    /// onto a Tokio task guarded by a semaphore permit.  If a claim cycle
    /// fails, the poll interval doubles (capped at 60 s) as a transient-
    /// failure backoff; it resets on the next successful claim.
    ///
    /// # Panics
    ///
    /// Panics if the internal semaphore is closed, which indicates a
    /// programming error in the worker lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Cancelled`] when the cancellation token
    /// is triggered.  Returns [`WorkerError::PoolExhausted`] if the
    /// connection pool cannot provide a connection.
    pub async fn run(self: &Arc<Self>) -> Result<(), WorkerError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut poll_interval = self.config.poll_interval();

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    tracing::info!(instance_id = %self.instance_id, "worker cancelled");
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

            let claim_result = {
                let mut conn = self
                    .pool
                    .acquire()
                    .await
                    .map_err(|e| WorkerError::PoolExhausted { source: e })?;
                PgTaskRepository
                    .claim(&mut conn, limit, &self.instance_id)
                    .await
            };

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
                        tokio::spawn(async move {
                            worker.run_task(task).await;
                            drop(permit);
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "claim cycle failed, backing off");
                    let doubled = poll_interval.as_secs().saturating_mul(2);
                    let capped = doubled.min(MAX_CLAIM_BACKOFF_SECS);
                    poll_interval = std::time::Duration::from_secs(capped.max(1));
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
                    tracing::error!(error = %e, "failed to acquire connection for task");
                    return;
                }
            };
            match PgJobRepository.find_by_id(&mut conn, job_id).await {
                Ok(j) => j,
                Err(e) => {
                    let stage_err = StageError::Database {
                        stage: "pre-dispatch".into(),
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

        // Notify job-state watch subscribers.  Each job has an optional
        // `watch::Sender` in `job_state_txs`; sending `()` wakes any
        // receivers waiting for status changes (used by integration
        // tests and future SSE endpoints).
        if let Some(tx) = self.job_state_txs.get(&job_id) {
            let _ = tx.send(());
        }

        let stage_result = tokio::select! {
            () = self.cancellation_token.cancelled() => {
                tracing::info!(task_id = %task.id(), "task cancelled mid-execution");
                return;
            }
            () = tokio::time::sleep(self.config.task_timeout()) => {
                Err(StageError::Timeout {
                    timeout_seconds: self.config.task_timeout_seconds,
                })
            }
            result = self.dispatch_stage(&job, &task) => {
                result
            }
        };

        match stage_result {
            Ok(output) => {
                self.record_token_usage(&output.usages);

                if self.cancellation_token.is_cancelled() {
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
        let available_at = Utc::now() + backoff_duration(post_increment_retry);
        let is_dead_lettered = post_increment_retry > self.config.task_max_retries;

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

        let fail_result = PgTaskRepository
            .fail(
                &mut txn,
                task.id(),
                claim_token,
                self.config.task_max_retries,
                available_at,
                error_kind,
                &error_message,
            )
            .await;

        if let Err(e) = fail_result {
            tracing::error!(error = %e, task_id = %task.id(), "failed to fail task");
            return;
        }

        // When a task exhausts its retry budget, dead-lettering is
        // terminal for Extraction and Relation tasks — both imply the
        // job cannot progress, so the job transitions to Failed.
        // Triage failures are non-fatal: remaining triage tasks can
        // still succeed, and the relation stage runs on whatever
        // triage results are available.
        if is_dead_lettered {
            let should_fail_job =
                matches!(task.task_type(), TaskType::Extraction | TaskType::Relation);
            if should_fail_job {
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
        }

        if let Err(e) = txn.commit().await {
            tracing::error!(error = %e, "failed to commit failure transaction");
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

        let Some(claim_token) = task.claim_token() else {
            return Err(StageError::OwnershipLost);
        };

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

        Ok(())
    }

    /// Records token usage for a completed stage.
    #[allow(clippy::unused_self)]
    fn record_token_usage(&self, _usages: &[Usage]) {
        // Implemented by ticket 4.6
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Clamps a `usize` to `u32`, saturating at [`u32::MAX`].
fn clamp_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
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
