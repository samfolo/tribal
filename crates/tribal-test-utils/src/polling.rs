//! Poll-until helpers for integration tests.
//!
//! These helpers replace wall-clock sleeps with condition-based polling,
//! eliminating timing assumptions and making tests both faster and more
//! deterministic.

use std::{future::Future, time::Duration};

use sqlx::PgPool;
use tribal_db::{JobRepository, PgJobRepository, PgTaskRepository, TaskRepository};
use tribal_domain::{Job, JobId, JobStatus, Task, TaskId, TaskStatus};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENTITY_POLL_INTERVAL: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Generic poll
// ---------------------------------------------------------------------------

/// Polls `f` every `interval` until it returns `Some(T)`, panicking if
/// `timeout` elapses first.
///
/// # Panics
///
/// Panics with `description` if the timeout is reached before `f`
/// returns `Some`.
pub async fn poll_until<T, F, Fut>(
    description: &str,
    interval: Duration,
    timeout: Duration,
    f: F,
) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "poll_until timed out: {description}",
        );
        tokio::time::sleep(interval).await;
    }
}

// ---------------------------------------------------------------------------
// Task polling
// ---------------------------------------------------------------------------

/// Polls until the task reaches the expected status, returning the task.
///
/// Uses a 50 ms internal poll interval.
///
/// # Panics
///
/// Panics if the timeout is reached before the task reaches `target`.
pub async fn poll_task_status(
    pool: &PgPool,
    task_id: TaskId,
    target: TaskStatus,
    timeout: Duration,
) -> Task {
    let description = format!("task {task_id} to reach {target:?}");
    poll_until(&description, ENTITY_POLL_INTERVAL, timeout, || {
        let pool = pool.clone();
        async move {
            let mut conn = pool.acquire().await.ok()?;
            let task = PgTaskRepository.find_by_id(&mut conn, task_id).await.ok()?;
            if task.status() == target {
                Some(task)
            } else {
                None
            }
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Job polling
// ---------------------------------------------------------------------------

/// Polls until the job reaches the expected status, returning the job.
///
/// Uses a 50 ms internal poll interval.
///
/// # Panics
///
/// Panics if the timeout is reached before the job reaches `target`.
pub async fn poll_job_status(
    pool: &PgPool,
    job_id: JobId,
    target: JobStatus,
    timeout: Duration,
) -> Job {
    let description = format!("job {job_id} to reach {target:?}");
    poll_until(&description, ENTITY_POLL_INTERVAL, timeout, || {
        let pool = pool.clone();
        async move {
            let mut conn = pool.acquire().await.ok()?;
            let job = PgJobRepository.find_by_id(&mut conn, job_id).await.ok()?;
            if job.status() == target {
                Some(job)
            } else {
                None
            }
        }
    })
    .await
}
