//! Periodic queue health gauge task.
//!
//! Queries task counts grouped by `(task_type, status)` every
//! [`GAUGE_POLL_INTERVAL`] and sets the `tasks_queued` and
//! `tasks_claimed` gauges on the provided [`MetricsRecorder`].

use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use strum::IntoEnumIterator;
use tokio_util::sync::CancellationToken;
use tribal_db::{PgTaskRepository, TaskRepository};
use tribal_domain::{TaskStatus, TaskType};
use tribal_telemetry::MetricsRecorder;

/// Interval between queue health gauge updates.
const GAUGE_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Periodically queries task status counts and updates queue health gauges.
///
/// Runs until the cancellation token is triggered.  Errors are logged
/// and retried on the next tick — a single failed query does not
/// terminate the task.
pub async fn run_queue_health_gauges(
    pool: PgPool,
    metrics: Arc<dyn MetricsRecorder>,
    cancellation_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(GAUGE_POLL_INTERVAL);
    // Skip the first immediate tick — let the worker settle.
    interval.tick().await;

    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => {
                tracing::info!("queue health gauge task cancelled");
                return;
            }
            _ = interval.tick() => {
                update_queue_gauges(&pool, &metrics).await;
            }
        }
    }
}

/// Single iteration of the gauge update loop.
///
/// Exported for testing: callers can invoke this directly without
/// spawning the full periodic task.
pub(crate) async fn update_queue_gauges(pool: &PgPool, metrics: &dyn MetricsRecorder) {
    let Ok(mut conn) = pool.acquire().await.inspect_err(|e| {
        tracing::warn!(error = %e, "gauge task: pool acquire failed");
    }) else {
        return;
    };

    let Ok(counts) = PgTaskRepository
        .count_by_status(&mut conn)
        .await
        .inspect_err(|e| {
            tracing::warn!(error = %e, "gauge task: count_by_status query failed");
        })
    else {
        return;
    };

    // Zero all gauges first to handle task types that have drained
    // to zero — the SQL only returns rows with non-zero counts.
    for task_type in TaskType::iter() {
        metrics.set_queue_gauge(task_type.as_str(), "queued", 0);
        metrics.set_queue_gauge(task_type.as_str(), "claimed", 0);
    }

    for row in &counts {
        metrics.set_queue_gauge(row.task_type.as_str(), row.status.as_str(), row.count);
    }
}
