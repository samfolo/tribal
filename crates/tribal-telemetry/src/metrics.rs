//! OpenTelemetry metric instruments for Tribal.
//!
//! [`Metrics`] bundles all 11 instruments defined in Server §6.7.
//! Consumers clone the struct (cheap — instruments are `Arc`-based)
//! and call recording methods at the appropriate sites.

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;

// ---------------------------------------------------------------------------
// Metric name constants
// ---------------------------------------------------------------------------

/// Queue gauge: number of queued tasks by type.
pub const TASKS_QUEUED: &str = "tribal.tasks.queued";
/// Queue gauge: number of claimed tasks by type.
pub const TASKS_CLAIMED: &str = "tribal.tasks.claimed";
/// Counter: successfully completed tasks.
pub const TASKS_COMPLETED: &str = "tribal.tasks.completed";
/// Counter: tasks that failed but were retried.
pub const TASKS_RETRIED: &str = "tribal.tasks.retried";
/// Counter: tasks that exhausted retries and were dead-lettered.
pub const TASKS_DEAD_LETTER: &str = "tribal.tasks.dead_letter";
/// Counter: jobs that reached a terminal state.
pub const JOBS_COMPLETED: &str = "tribal.jobs.completed";
/// Histogram: database pool acquire wait in milliseconds.
pub const POOL_ACQUIRE_WAIT_MS: &str = "tribal.pool.acquire_wait_ms";
/// Histogram: semaphore acquire wait in milliseconds.
pub const SEMAPHORE_ACQUIRE_WAIT_MS: &str = "tribal.semaphore.acquire_wait_ms";
/// Histogram: total job duration in milliseconds.
pub const JOB_DURATION_MS: &str = "tribal.job.duration_ms";
/// Histogram: individual task duration in milliseconds.
pub const TASK_DURATION_MS: &str = "tribal.task.duration_ms";
/// Histogram: provider call latency in milliseconds.
pub const PROVIDER_CALL_MS: &str = "tribal.provider.call_ms";

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Bundles all OpenTelemetry metric instruments for Tribal.
///
/// Cloning is cheap — instrument handles are internally `Arc`-based.
/// When constructed via [`Metrics::noop`], all recordings are silently
/// discarded.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Number of queued tasks, labelled by `task_type`.
    pub tasks_queued: Gauge<i64>,
    /// Number of claimed tasks, labelled by `task_type`.
    pub tasks_claimed: Gauge<i64>,
    /// Successfully completed tasks, labelled by `task_type`.
    pub tasks_completed: Counter<u64>,
    /// Tasks retried (not dead-lettered), labelled by `task_type`.
    pub tasks_retried: Counter<u64>,
    /// Tasks dead-lettered after exhausting retries, labelled by `task_type`.
    pub tasks_dead_letter: Counter<u64>,
    /// Jobs reaching a terminal state, labelled by `outcome`.
    pub jobs_completed: Counter<u64>,
    /// Database pool acquire wait time, labelled by `pool`.
    pub pool_acquire_wait_ms: Histogram<f64>,
    /// Semaphore acquire wait time, labelled by `provider_key`.
    pub semaphore_acquire_wait_ms: Histogram<f64>,
    /// Total job duration from creation to completion, labelled by `outcome`.
    pub job_duration_ms: Histogram<f64>,
    /// Individual task duration from claim to commit, labelled by `task_type`.
    pub task_duration_ms: Histogram<f64>,
    /// Provider call latency, labelled by `provider`, `model`, `stage`.
    pub provider_call_ms: Histogram<f64>,
}

impl Metrics {
    /// Creates instruments from the given meter.
    pub fn new(meter: &Meter) -> Self {
        Self {
            tasks_queued: meter.i64_gauge(TASKS_QUEUED).build(),
            tasks_claimed: meter.i64_gauge(TASKS_CLAIMED).build(),
            tasks_completed: meter.u64_counter(TASKS_COMPLETED).build(),
            tasks_retried: meter.u64_counter(TASKS_RETRIED).build(),
            tasks_dead_letter: meter.u64_counter(TASKS_DEAD_LETTER).build(),
            jobs_completed: meter.u64_counter(JOBS_COMPLETED).build(),
            pool_acquire_wait_ms: meter.f64_histogram(POOL_ACQUIRE_WAIT_MS).build(),
            semaphore_acquire_wait_ms: meter.f64_histogram(SEMAPHORE_ACQUIRE_WAIT_MS).build(),
            job_duration_ms: meter.f64_histogram(JOB_DURATION_MS).build(),
            task_duration_ms: meter.f64_histogram(TASK_DURATION_MS).build(),
            provider_call_ms: meter.f64_histogram(PROVIDER_CALL_MS).build(),
        }
    }

    /// Creates no-op instruments that silently discard all recordings.
    ///
    /// Uses a default [`SdkMeterProvider`] with no readers, so all
    /// recordings are silently discarded.  Used when telemetry is
    /// disabled or no OTLP endpoint is configured.
    pub fn noop() -> Self {
        let provider = SdkMeterProvider::default();
        let meter = provider.meter("noop");
        Self::new(&meter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_metrics_can_be_created_and_cloned() {
        let metrics = Metrics::noop();
        let cloned = metrics.clone();
        // Verify instruments accept recordings without panic.
        cloned.tasks_completed.add(1, &[]);
        cloned.pool_acquire_wait_ms.record(42.0, &[]);
        cloned.tasks_queued.record(5, &[]);
    }
}
