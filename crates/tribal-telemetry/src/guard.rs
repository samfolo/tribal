//! Telemetry guard that flushes pending writes on drop.
//!
//! [`TelemetryGuard`] wraps the worker guard from `tracing-appender`'s
//! non-blocking writer.  Callers must hold this value for the program
//! lifetime; dropping it flushes any buffered log output.

/// Opaque guard returned by [`init_subscriber`](crate::init_subscriber).
///
/// Holds the worker thread guard for the non-blocking log writer.
/// When this value is dropped, the worker thread is joined and all
/// pending writes are flushed.
///
/// # Usage
///
/// Bind the guard in `main` and keep it alive until shutdown:
///
/// ```ignore
/// let _guard = tribal_telemetry::init_subscriber(config)?;
/// // … run program …
/// // guard dropped here, flushing logs
/// ```
#[derive(Debug)]
pub struct TelemetryGuard {
    /// The worker guard from `tracing_appender::non_blocking`.
    ///
    /// Both stderr and file writers use non-blocking output.  The
    /// field is held purely for its `Drop` implementation.
    _worker_guard: tracing_appender::non_blocking::WorkerGuard,
}

impl TelemetryGuard {
    /// Creates a new guard wrapping the given worker guard.
    pub(crate) fn new(guard: tracing_appender::non_blocking::WorkerGuard) -> Self {
        Self {
            _worker_guard: guard,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_can_be_created_and_dropped() {
        let (_writer, guard) = tracing_appender::non_blocking(std::io::sink());
        let telemetry_guard = TelemetryGuard::new(guard);
        drop(telemetry_guard);
    }
}
