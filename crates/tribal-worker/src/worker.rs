//! Worker struct and poll-claim-dispatch loop.
//!
//! The [`Worker`] owns the main event loop: poll for claimable tasks,
//! claim them atomically, dispatch each to the appropriate pipeline
//! stage, and commit or fail the result.  Concurrency is bounded by a
//! [`Semaphore`] and graceful shutdown is signalled via a
//! [`CancellationToken`].

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tribal_db::repositories::{
    ExtractionResultRepository, JobRepository, JobStatusTransition, PgExtractionResultRepository,
    PgJobRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{JobId, JobOutcome, JobStatus, TaskType};
use tribal_inference::{
    EmbeddingProvider, InferenceProvider, ProviderKey, ProviderRegistry, Usage,
};

use crate::config::WorkerConfig;
use crate::error::{StageError, WorkerError};
use crate::stages::{StageCommit, StageOutput};

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
    config: WorkerConfig,
    instance_id: String,
    #[allow(dead_code)]
    job_state_txs: Arc<DashMap<JobId, watch::Sender<()>>>,
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
        }
    }

    /// Reclaims stale tasks that were left claimed by a previous worker
    /// instance.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on database failures.
    pub async fn startup_reclaim(&self) -> Result<u64, WorkerError> {
        // Implemented by ticket 4.2
        Ok(0)
    }

    /// Runs the main poll-claim-dispatch loop until cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Cancelled`] when the cancellation token
    /// is triggered.  Returns [`WorkerError::PoolExhausted`] or
    /// [`WorkerError::ClaimFailed`] on transient database failures (after
    /// backoff retries).
    pub async fn run(self: &Arc<Self>) -> Result<(), WorkerError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut poll_interval = self.config.poll_interval();

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    tracing::info!(instance_id = %self.instance_id, "worker cancelled");
                    return Err(WorkerError::Cancelled);
                }
                () = tokio::time::sleep(poll_interval) => {
                    // Continue to claim logic below.
                }
            }

            let available = semaphore.available_permits();
            if available == 0 {
                continue;
            }

            #[allow(clippy::cast_possible_truncation)]
            let limit = available as u32;

            let claim_result = {
                let mut conn = self.pool.acquire().await.map_err(|e| {
                    WorkerError::PoolExhausted { source: e }
                })?;
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
                            .expect("semaphore closed unexpectedly");
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
                    let capped = doubled.min(60);
                    poll_interval = std::time::Duration::from_secs(capped.max(1));
                }
            }
        }
    }

    /// Dispatches a single task to the appropriate pipeline stage.
    async fn run_task(&self, task: tribal_domain::Task) {
        let job_id = task.job_id();

        let job = {
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

        // Best-effort claim-time job status transition.
        let target_status = job_status_for_task_type(task.task_type());
        if job.status() != target_status {
            if let Ok(mut conn) = self.pool.acquire().await {
                let transition = JobStatusTransition::builder()
                    .status(target_status)
                    .build();
                let _ = PgJobRepository
                    .update_status(&mut conn, job_id, &transition)
                    .await;
            }
        }

        // Notify job state watchers.
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
    async fn dispatch_stage(
        &self,
        job: &tribal_domain::Job,
        task: &tribal_domain::Task,
    ) -> Result<StageOutput, StageError> {
        match task.task_type() {
            TaskType::Extraction => self.run_extraction(job, task).await,
            TaskType::Triage => self.run_triage(job, task).await,
            TaskType::Relation => self.run_relation(job, task).await,
        }
    }

    /// Handles a stage failure: computes backoff, fails the task, and
    /// optionally transitions the job to `Failed` on dead-letter.
    async fn handle_stage_failure(&self, task: &tribal_domain::Task, error: &StageError) {
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
                &mut *txn,
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

        if is_dead_lettered {
            let should_fail_job = matches!(
                task.task_type(),
                TaskType::Extraction | TaskType::Relation
            );
            if should_fail_job {
                let transition = JobStatusTransition::builder()
                    .status(JobStatus::Failed)
                    .outcome(Some(JobOutcome::Failure))
                    .error_message(Some(error_message))
                    .completed_at(Some(Utc::now()))
                    .build();
                if let Err(e) = PgJobRepository
                    .update_status(&mut *txn, task.job_id(), &transition)
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
        task: &tribal_domain::Task,
        commit: StageCommit,
    ) -> Result<(), StageError> {
        match commit {
            StageCommit::Extraction {
                extraction_result,
                triage_tasks,
                batch_size,
                original_count,
            } => {
                self.commit_extraction(task, extraction_result, triage_tasks, batch_size, original_count)
                    .await
            }
        }
    }

    /// Commits extraction stage effects within a single transaction.
    async fn commit_extraction(
        &self,
        task: &tribal_domain::Task,
        extraction_result: tribal_db::repositories::NewExtractionResult,
        triage_tasks: Vec<tribal_db::repositories::NewTask>,
        batch_size: u32,
        original_count: u32,
    ) -> Result<(), StageError> {
        let mut conn = self.pool.acquire().await.map_err(|e| StageError::Database {
            stage: "extraction".into(),
            context: "acquiring connection".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "pool acquire".into(),
                source: e,
            },
        })?;

        let mut txn = sqlx::Connection::begin(&mut *conn).await.map_err(|e| {
            StageError::Database {
                stage: "extraction".into(),
                context: "beginning transaction".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "begin".into(),
                    source: e,
                },
            }
        })?;

        PgExtractionResultRepository
            .insert(&mut *txn, &extraction_result)
            .await
            .map_err(|e| StageError::Database {
                stage: "extraction".into(),
                context: "inserting extraction result".into(),
                source: e,
            })?;

        let is_empty = batch_size == 0;

        if !is_empty {
            for new_task in &triage_tasks {
                PgTaskRepository
                    .insert(&mut *txn, new_task)
                    .await
                    .map_err(|e| StageError::Database {
                        stage: "extraction".into(),
                        context: "creating triage task".into(),
                        source: e,
                    })?;
            }
        }

        PgJobRepository
            .update_batch_size(&mut *txn, task.job_id(), batch_size, original_count)
            .await
            .map_err(|e| StageError::Database {
                stage: "extraction".into(),
                context: "updating batch size".into(),
                source: e,
            })?;

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
            .update_status(&mut *txn, task.job_id(), &job_transition)
            .await
            .map_err(|e| StageError::Database {
                stage: "extraction".into(),
                context: "transitioning job status".into(),
                source: e,
            })?;

        let Some(claim_token) = task.claim_token() else {
            return Err(StageError::OwnershipLost);
        };

        let rows = PgTaskRepository
            .complete(&mut *txn, task.id(), claim_token)
            .await
            .map_err(|e| StageError::Database {
                stage: "extraction".into(),
                context: "completing task".into(),
                source: e,
            })?;

        if rows == 0 {
            return Err(StageError::OwnershipLost);
        }

        txn.commit().await.map_err(|e| StageError::Database {
            stage: "extraction".into(),
            context: "committing transaction".into(),
            source: tribal_db::DbError::QueryFailed {
                context: "commit".into(),
                source: e,
            },
        })?;

        Ok(())
    }

    /// Records token usage for a completed stage.
    fn record_token_usage(&self, _usages: &[Usage]) {
        // Implemented by ticket 4.6
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Computes backoff duration: `min(2^retry_count, 60)` seconds with
/// ±20% jitter.
///
/// `retry_count` is the post-increment value (i.e. the retry that just
/// happened).
fn backoff_duration(retry_count: u32) -> chrono::Duration {
    let base_secs = 2u64.saturating_pow(retry_count).min(60);
    let jitter_range = base_secs / 5; // 20%
    let jitter = if jitter_range > 0 {
        let offset = simple_hash_jitter(retry_count, jitter_range);
        #[allow(clippy::cast_possible_wrap)]
        let signed_offset = (offset as i64) - (jitter_range as i64);
        signed_offset
    } else {
        0
    };
    #[allow(clippy::cast_possible_wrap)]
    let total_secs = (base_secs as i64) + jitter;
    chrono::Duration::seconds(total_secs.max(1))
}

/// Deterministic jitter based on retry count (avoids RNG dependency).
fn simple_hash_jitter(retry_count: u32, range: u64) -> u64 {
    let hash = u64::from(retry_count)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    hash % (range * 2 + 1)
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
    fn test_backoff_duration_values() {
        // Base values (ignoring jitter): 2^1=2, 2^2=4, 2^3=8, 2^4=16, 2^5=32, 2^6=60(capped)
        for retry in 1..=10 {
            let duration = backoff_duration(retry);
            let base = 2u64.saturating_pow(retry).min(60);
            let lower = (base as f64 * 0.8).floor() as i64;
            let upper = (base as f64 * 1.2).ceil() as i64;
            let secs = duration.num_seconds();
            assert!(
                secs >= lower.max(1) && secs <= upper,
                "retry {retry}: expected [{lower}, {upper}], got {secs}",
            );
        }
    }

    #[test]
    fn test_backoff_capped_at_60s() {
        for retry in 7..=15 {
            let duration = backoff_duration(retry);
            let secs = duration.num_seconds();
            // 60 + 20% = 72
            assert!(secs <= 72, "retry {retry}: got {secs}, expected <= 72");
        }
    }

    #[test]
    fn test_backoff_jitter_within_bounds() {
        for retry in 1..=100 {
            let duration = backoff_duration(retry);
            let base = 2u64.saturating_pow(retry).min(60);
            let lower = (base as f64 * 0.8).floor() as i64;
            let upper = (base as f64 * 1.2).ceil() as i64;
            let secs = duration.num_seconds();
            assert!(
                secs >= lower.max(1) && secs <= upper,
                "retry {retry}: expected [{}, {upper}], got {secs}",
                lower.max(1),
            );
        }
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
