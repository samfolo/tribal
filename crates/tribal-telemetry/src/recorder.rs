//! Metrics recording abstraction.
//!
//! [`MetricsRecorder`] decouples consumers from `OpenTelemetry` types.
//! The [`OtelMetricsRecorder`] implementation handles all attribute
//! construction and duration conversion internally;
//! [`NoopMetricsRecorder`] silently discards everything, suitable for
//! tests.

use std::{sync::Arc, time::Duration};

use opentelemetry::KeyValue;
use tribal_domain::{gen_ai, span_attrs};

use crate::metrics::{
    LABEL_MODEL, LABEL_OUTCOME, LABEL_POOL, LABEL_PROVIDER, LABEL_PROVIDER_KEY, LABEL_STAGE,
    LABEL_TASK_TYPE, Metrics,
};

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One GenAI client operation, as the duration and token-usage histograms
/// record it.
///
/// `operation` and the attribute keys follow the GenAI client metric
/// conventions; `stage` and `purpose` are Tribal's own pipeline
/// attribution, carried as additional attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InferenceOperationRecord<'a> {
    /// The GenAI operation name value (`chat` or `embeddings`).
    pub operation: &'a str,
    /// The provider name.
    pub provider: &'a str,
    /// The model named in the request.
    pub model: &'a str,
    /// The pipeline stage the call is attributed to.
    pub stage: &'a str,
    /// The embedding purpose, for embedding operations.
    pub purpose: Option<&'a str>,
    /// Wall-clock duration of the call.
    pub duration: Duration,
    /// Input (prompt) token count.
    pub input_tokens: u64,
    /// Output (completion) token count.
    pub output_tokens: u64,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Abstraction for recording operational metrics.
///
/// Consumers depend on this trait, never on OpenTelemetry types
/// directly.  The trait uses only primitives and [`Duration`] so the
/// telemetry backend can be swapped or stubbed in tests.
pub trait MetricsRecorder: Send + Sync {
    /// Records pool connection acquisition latency.
    fn record_pool_acquire(&self, pool: &str, elapsed: Duration);

    /// Records semaphore acquisition latency.
    fn record_semaphore_acquire(&self, provider_key: &str, elapsed: Duration);

    /// Records provider (LLM/embedding) call latency.
    fn record_provider_call(&self, provider: &str, model: &str, stage: &str, elapsed: Duration);

    /// Records one GenAI client operation: its duration in seconds and
    /// its token counts classed by type.
    fn record_inference_operation(&self, record: &InferenceOperationRecord<'_>);

    /// Records a successfully committed task.
    fn record_task_completed(&self, task_type: &str, duration_ms: f64);

    /// Records a retried (non-dead-lettered) task failure.
    fn record_task_retried(&self, task_type: &str);

    /// Records a dead-lettered task.
    fn record_task_dead_lettered(&self, task_type: &str);

    /// Records a job reaching a terminal state.
    ///
    /// `duration_ms` is `None` when the job was not loaded (pre-dispatch
    /// failure) — the counter fires but the histogram is skipped.
    fn record_job_completed(&self, outcome: &str, duration_ms: Option<f64>);

    /// Sets the queue gauge for a specific task type and status.
    ///
    /// Only `"queued"` and `"claimed"` statuses are recorded; other
    /// values are silently ignored.
    fn set_queue_gauge(&self, task_type: &str, status: &str, count: i64);
}

/// Delegates through `Arc` so that `Arc<dyn MetricsRecorder>` satisfies
/// the trait without manual dereferencing at call sites.
impl<T: MetricsRecorder + ?Sized> MetricsRecorder for Arc<T> {
    fn record_pool_acquire(&self, pool: &str, elapsed: Duration) {
        (**self).record_pool_acquire(pool, elapsed);
    }

    fn record_semaphore_acquire(&self, provider_key: &str, elapsed: Duration) {
        (**self).record_semaphore_acquire(provider_key, elapsed);
    }

    fn record_provider_call(&self, provider: &str, model: &str, stage: &str, elapsed: Duration) {
        (**self).record_provider_call(provider, model, stage, elapsed);
    }

    fn record_inference_operation(&self, record: &InferenceOperationRecord<'_>) {
        (**self).record_inference_operation(record);
    }

    fn record_task_completed(&self, task_type: &str, duration_ms: f64) {
        (**self).record_task_completed(task_type, duration_ms);
    }

    fn record_task_retried(&self, task_type: &str) {
        (**self).record_task_retried(task_type);
    }

    fn record_task_dead_lettered(&self, task_type: &str) {
        (**self).record_task_dead_lettered(task_type);
    }

    fn record_job_completed(&self, outcome: &str, duration_ms: Option<f64>) {
        (**self).record_job_completed(outcome, duration_ms);
    }

    fn set_queue_gauge(&self, task_type: &str, status: &str, count: i64) {
        (**self).set_queue_gauge(task_type, status, count);
    }
}

// ---------------------------------------------------------------------------
// OTel implementation
// ---------------------------------------------------------------------------

/// OpenTelemetry-backed [`MetricsRecorder`].
///
/// Constructs `KeyValue` attributes and delegates to the underlying
/// [`Metrics`] instruments.  Duration conversion happens once per
/// method, not at every call site.
pub struct OtelMetricsRecorder {
    metrics: Metrics,
}

impl OtelMetricsRecorder {
    /// Wraps the given instruments in a recorder.
    #[must_use]
    pub fn new(metrics: Metrics) -> Self {
        Self { metrics }
    }
}

/// Converts a [`Duration`] to milliseconds as `f64`.
fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

impl MetricsRecorder for OtelMetricsRecorder {
    fn record_pool_acquire(&self, pool: &str, elapsed: Duration) {
        self.metrics.pool_acquire_wait_ms.record(
            duration_ms(elapsed),
            &[KeyValue::new(LABEL_POOL, pool.to_owned())],
        );
    }

    fn record_semaphore_acquire(&self, provider_key: &str, elapsed: Duration) {
        self.metrics.semaphore_acquire_wait_ms.record(
            duration_ms(elapsed),
            &[KeyValue::new(LABEL_PROVIDER_KEY, provider_key.to_owned())],
        );
    }

    fn record_provider_call(&self, provider: &str, model: &str, stage: &str, elapsed: Duration) {
        self.metrics.provider_call_ms.record(
            duration_ms(elapsed),
            &[
                KeyValue::new(LABEL_PROVIDER, provider.to_owned()),
                KeyValue::new(LABEL_MODEL, model.to_owned()),
                KeyValue::new(LABEL_STAGE, stage.to_owned()),
            ],
        );
    }

    fn record_inference_operation(&self, record: &InferenceOperationRecord<'_>) {
        let mut attributes = vec![
            KeyValue::new(gen_ai::OPERATION_NAME, record.operation.to_owned()),
            KeyValue::new(gen_ai::PROVIDER_NAME, record.provider.to_owned()),
            KeyValue::new(gen_ai::REQUEST_MODEL, record.model.to_owned()),
            KeyValue::new(span_attrs::STAGE, record.stage.to_owned()),
        ];
        if let Some(purpose) = record.purpose {
            attributes.push(KeyValue::new(
                span_attrs::EMBEDDING_PURPOSE,
                purpose.to_owned(),
            ));
        }

        self.metrics
            .gen_ai_operation_duration
            .record(record.duration.as_secs_f64(), &attributes);

        attributes.push(KeyValue::new(
            gen_ai::TOKEN_TYPE,
            gen_ai::TOKEN_TYPE_INPUT.to_owned(),
        ));
        self.metrics
            .gen_ai_token_usage
            .record(record.input_tokens, &attributes);

        let last = attributes
            .last_mut()
            .expect("the token-type attribute was just pushed");
        *last = KeyValue::new(gen_ai::TOKEN_TYPE, gen_ai::TOKEN_TYPE_OUTPUT.to_owned());
        self.metrics
            .gen_ai_token_usage
            .record(record.output_tokens, &attributes);
    }

    fn record_task_completed(&self, task_type: &str, task_duration_ms: f64) {
        let attr = KeyValue::new(LABEL_TASK_TYPE, task_type.to_owned());
        self.metrics
            .tasks_completed
            .add(1, std::slice::from_ref(&attr));
        self.metrics
            .task_duration_ms
            .record(task_duration_ms, &[attr]);
    }

    fn record_task_retried(&self, task_type: &str) {
        self.metrics
            .tasks_retried
            .add(1, &[KeyValue::new(LABEL_TASK_TYPE, task_type.to_owned())]);
    }

    fn record_task_dead_lettered(&self, task_type: &str) {
        self.metrics
            .tasks_dead_letter
            .add(1, &[KeyValue::new(LABEL_TASK_TYPE, task_type.to_owned())]);
    }

    fn record_job_completed(&self, outcome: &str, job_duration_ms: Option<f64>) {
        let attr = KeyValue::new(LABEL_OUTCOME, outcome.to_owned());
        self.metrics
            .jobs_completed
            .add(1, std::slice::from_ref(&attr));
        if let Some(ms) = job_duration_ms {
            self.metrics.job_duration_ms.record(ms, &[attr]);
        }
    }

    fn set_queue_gauge(&self, task_type: &str, status: &str, count: i64) {
        let attrs = &[KeyValue::new(LABEL_TASK_TYPE, task_type.to_owned())];
        match status {
            "queued" => self.metrics.tasks_queued.record(count, attrs),
            "claimed" => self.metrics.tasks_claimed.record(count, attrs),
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// No-op implementation
// ---------------------------------------------------------------------------

/// No-op [`MetricsRecorder`] that silently discards all recordings.
///
/// Used in tests where telemetry infrastructure is not needed.
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn record_pool_acquire(&self, _pool: &str, _elapsed: Duration) {}
    fn record_semaphore_acquire(&self, _provider_key: &str, _elapsed: Duration) {}
    fn record_provider_call(
        &self,
        _provider: &str,
        _model: &str,
        _stage: &str,
        _elapsed: Duration,
    ) {
    }
    fn record_inference_operation(&self, _record: &InferenceOperationRecord<'_>) {}
    fn record_task_completed(&self, _task_type: &str, _duration_ms: f64) {}
    fn record_task_retried(&self, _task_type: &str) {}
    fn record_task_dead_lettered(&self, _task_type: &str) {}
    fn record_job_completed(&self, _outcome: &str, _duration_ms: Option<f64>) {}
    fn set_queue_gauge(&self, _task_type: &str, _status: &str, _count: i64) {}
}

/// Creates a no-op recorder for use in tests.
#[must_use]
pub fn noop_recorder() -> Arc<dyn MetricsRecorder> {
    Arc::new(NoopMetricsRecorder)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_recorder_accepts_all_recordings() {
        let recorder = noop_recorder();
        recorder.record_pool_acquire("worker", Duration::from_millis(5));
        recorder.record_semaphore_acquire("extraction", Duration::from_millis(2));
        recorder.record_provider_call("ollama", "llama3", "extraction", Duration::from_secs(1));
        recorder.record_task_completed("extraction", 150.0);
        recorder.record_task_retried("triage");
        recorder.record_task_dead_lettered("relation");
        recorder.record_job_completed("success", Some(1200.0));
        recorder.record_job_completed("failure", None);
        recorder.set_queue_gauge("extraction", "queued", 5);
    }

    #[test]
    fn test_inference_operation_record_accepts_both_shapes() {
        let recorder = OtelMetricsRecorder::new(Metrics::noop());
        recorder.record_inference_operation(&InferenceOperationRecord {
            operation: "chat",
            provider: "anthropic",
            model: "claude",
            stage: "triage",
            purpose: None,
            duration: Duration::from_millis(120),
            input_tokens: 10,
            output_tokens: 5,
        });
        recorder.record_inference_operation(&InferenceOperationRecord {
            operation: "embeddings",
            provider: "ollama",
            model: "nomic-embed-text:v1.5",
            stage: "embedding",
            purpose: Some("query"),
            duration: Duration::from_millis(8),
            input_tokens: 3,
            output_tokens: 0,
        });
    }

    #[test]
    fn test_otel_recorder_accepts_all_recordings() {
        let metrics = Metrics::noop();
        let recorder = OtelMetricsRecorder::new(metrics);
        recorder.record_pool_acquire("mcp", Duration::from_millis(3));
        recorder.record_semaphore_acquire("triage_inference", Duration::from_millis(1));
        recorder.record_provider_call(
            "anthropic",
            "claude",
            "triage_inference",
            Duration::from_secs(2),
        );
        recorder.record_task_completed("triage", 200.0);
        recorder.record_task_retried("extraction");
        recorder.record_task_dead_lettered("extraction");
        recorder.record_job_completed("partial", Some(5000.0));
        recorder.record_job_completed("failure", None);
        recorder.set_queue_gauge("triage", "queued", 3);
        recorder.set_queue_gauge("extraction", "claimed", 1);
        recorder.set_queue_gauge("relation", "completed", 10);
    }
}
