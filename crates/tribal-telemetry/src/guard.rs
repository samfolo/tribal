//! Telemetry guard that flushes pending writes and export pipelines on drop.
//!
//! [`TelemetryGuard`] wraps the worker guard from `tracing-appender`'s
//! non-blocking writer, plus optional trace and metrics provider handles.
//! Callers must hold this value for the program lifetime; dropping it
//! flushes all buffered output and shuts down export pipelines.

use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider};

/// Opaque guard returned by [`init_subscriber`](crate::init_subscriber).
///
/// Holds the worker thread guard for the non-blocking log writer and
/// optional trace and metrics provider shutdown handles.  When this
/// value is dropped, export pipelines are flushed first, then the
/// log writer is drained.
///
/// # Usage
///
/// Bind the guard in `main` and keep it alive until shutdown:
///
/// ```ignore
/// let (_guard, _metrics) = tribal_telemetry::init_subscriber(logging, telemetry)?;
/// // … run program …
/// // guard dropped here, flushing exports and logs
/// ```
#[derive(Debug)]
pub struct TelemetryGuard {
    /// Tracer provider shutdown handle.  Flushed before the log writer.
    tracer_provider: Option<SdkTracerProvider>,
    /// Meter provider shutdown handle.  Flushed before the log writer.
    meter_provider: Option<SdkMeterProvider>,
    /// The worker guard from `tracing_appender::non_blocking`.
    ///
    /// Both stderr and file writers use non-blocking output.  The
    /// field is held purely for its `Drop` implementation, which
    /// must run last to capture any log output from provider shutdown.
    _worker_guard: tracing_appender::non_blocking::WorkerGuard,
}

impl TelemetryGuard {
    /// Creates a new guard wrapping the given components.
    pub(crate) fn new(
        worker_guard: tracing_appender::non_blocking::WorkerGuard,
        tracer_provider: Option<SdkTracerProvider>,
        meter_provider: Option<SdkMeterProvider>,
    ) -> Self {
        Self {
            tracer_provider,
            meter_provider,
            _worker_guard: worker_guard,
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Flush export pipelines before the log writer drains.
        // Use eprintln for errors — the tracing subscriber may be
        // shutting down and unable to process log events.
        if let Some(tracer) = self.tracer_provider.take()
            && let Err(e) = tracer.shutdown()
        {
            eprintln!("tracer provider shutdown error: {e}");
        }
        if let Some(meter) = self.meter_provider.take()
            && let Err(e) = meter.shutdown()
        {
            eprintln!("meter provider shutdown error: {e}");
        }
        // _worker_guard drops automatically after this, flushing logs.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_can_be_created_and_dropped() {
        let (_writer, guard) = tracing_appender::non_blocking(std::io::sink());
        let telemetry_guard = TelemetryGuard::new(guard, None, None);
        drop(telemetry_guard);
    }
}
