//! Tracing subscriber initialisation.
//!
//! [`init_subscriber`] builds a layered subscriber from a
//! [`LoggingConfig`](tribal_config::LoggingConfig).  When
//! [`TelemetryConfig::enabled`](tribal_config::TelemetryConfig) is `true`,
//! an OpenTelemetry tracing layer is installed so spans carry valid trace
//! context (trace IDs, span IDs).  Three independently gated export paths
//! are available:
//!
//! - **OTLP** — when an endpoint is configured, spans are exported to a
//!   collector.
//! - **Console** — when `console_export` is true, individual spans are
//!   written to stderr as newline-delimited JSON (proto `Span` objects).
//! - **File** — when `file_export` is true, individual spans are written
//!   as newline-delimited JSON to rotating files in `file_directory`.
//!
//! `enabled` acts as a master switch: when false, all export paths are
//! silently disabled.  It should be called exactly once, early in program
//! startup.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use opentelemetry::{metrics::MeterProvider, trace::TracerProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::sync::broadcast;
use tracing::subscriber::set_global_default;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use tribal_config::{FileRotation, LogFormat, LogOutput, LoggingConfig, TelemetryConfig};
use tribal_wire::control::ControlEvent;

use crate::{
    error::TelemetryError,
    exporter::WriterSpanExporter,
    guard::TelemetryGuard,
    log_ring::{LogRing, LogRingLayer},
    metrics::Metrics,
    otlp,
    recorder::{MetricsRecorder, OtelMetricsRecorder, noop_recorder},
};

fn rotation_from(file_rotation: FileRotation) -> Rotation {
    match file_rotation {
        FileRotation::Daily => Rotation::DAILY,
        FileRotation::Hourly => Rotation::HOURLY,
        FileRotation::Never => Rotation::NEVER,
    }
}

/// Whether [`init_subscriber`] has already been called.
static INITIALISED: AtomicBool = AtomicBool::new(false);

/// Initialises the global tracing subscriber and optional export
/// pipelines.
///
/// Builds a layered subscriber stack based on the given configuration:
///
/// 1. **Filter layer** — `EnvFilter` parsed from `logging.level`.
/// 2. **Format layer** — JSON or pretty, depending on `logging.format`.
/// 3. **Output layer** — stderr or rolling file, depending on `logging.output`.
/// 4. **OpenTelemetry layer** — when `telemetry.enabled` is true, spans
///    carry valid trace context (trace IDs, span IDs).  Three
///    independently gated export paths may be active simultaneously:
///    - OTLP export when `otlp_endpoint` is configured.
///    - Console export (stderr) when `console_export` is true.
///    - File export (rotating JSONL) when `file_export` is true.
///
/// Both stderr and file output use non-blocking writers for consistent
/// behaviour.  The returned [`TelemetryGuard`] must be held for the
/// program lifetime to ensure pending writes and export data are flushed
/// on shutdown.
///
/// The returned [`MetricsRecorder`] provides methods for recording
/// all 11 operational metrics.  When telemetry is disabled or no OTLP
/// endpoint is configured, recordings are silently discarded.
///
/// # Errors
///
/// - [`TelemetryError::SubscriberAlreadyInitialised`] if this function
///   has already been called successfully.
/// - [`TelemetryError::InvalidFilterDirective`] if the filter string
///   cannot be parsed.
/// - [`TelemetryError::FileAppenderInit`] if the rolling file appender
///   cannot be initialised (e.g. the directory cannot be created).
/// - [`TelemetryError::SetGlobalDefault`] if another library already
///   registered a global subscriber.
/// - [`TelemetryError::UnrecognisedOtlpProtocol`] if `otlp_protocol`
///   is not `"grpc"` or `"http"`.
/// - [`TelemetryError::OtlpTracePipelineInit`] or
///   [`TelemetryError::MetricsPipelineInit`] if the OTLP pipeline
///   fails to initialise.
///
/// # Panics
///
/// Does not panic.  All failure modes return `Err`.
pub fn init_subscriber(
    logging: &LoggingConfig,
    telemetry: &TelemetryConfig,
) -> Result<(TelemetryGuard, Arc<dyn MetricsRecorder>), TelemetryError> {
    // No control plane: capture into a ring nobody reads and publish to a
    // detached bus. The log-capture layer is cheap and uniform across callers.
    let (events, _) = broadcast::channel(1);
    let (guard, recorder, _ring) = init_subscriber_with_log_bridge(logging, telemetry, events)?;
    Ok((guard, recorder))
}

/// Initialises the global tracing subscriber, additionally bridging captured
/// log lines to the control plane: each admitted line lands in the returned
/// [`LogRing`] (answering `logs.tail`) and publishes as a `logs.line` event on
/// `control_events` (feeding a subscribed client live).
///
/// This is [`init_subscriber`] plus the bridge; see it for the layer stack and
/// the full error contract.
///
/// # Errors
///
/// Returns the same [`TelemetryError`] variants as [`init_subscriber`].
pub fn init_subscriber_with_log_bridge(
    logging: &LoggingConfig,
    telemetry: &TelemetryConfig,
    control_events: broadcast::Sender<ControlEvent>,
) -> Result<(TelemetryGuard, Arc<dyn MetricsRecorder>, LogRing), TelemetryError> {
    if INITIALISED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(TelemetryError::SubscriberAlreadyInitialised);
    }

    match try_init_subscriber(logging, telemetry, control_events) {
        Ok(result) => Ok(result),
        Err(err) => {
            INITIALISED.store(false, Ordering::SeqCst);
            Err(err)
        }
    }
}

/// Inner implementation that builds and installs the subscriber.
///
/// Separated from [`init_subscriber`] so that the `INITIALISED` flag can
/// be reset cleanly on failure without duplicating the guard logic.
fn try_init_subscriber(
    logging: &LoggingConfig,
    telemetry: &TelemetryConfig,
    control_events: broadcast::Sender<ControlEvent>,
) -> Result<(TelemetryGuard, Arc<dyn MetricsRecorder>, LogRing), TelemetryError> {
    let env_filter = EnvFilter::try_new(&logging.level).map_err(|source| {
        TelemetryError::InvalidFilterDirective {
            directive: logging.level.clone(),
            source,
        }
    })?;

    // Build writer and guard based on output destination.
    let (writer, guard) = match logging.output {
        LogOutput::Stderr => {
            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
            (non_blocking, guard)
        }
        LogOutput::File => {
            let rotation = rotation_from(logging.file_rotation);

            let suffix = match logging.format {
                LogFormat::Json => "jsonl",
                LogFormat::Pretty => "log",
            };

            let appender = RollingFileAppender::builder()
                .rotation(rotation)
                .filename_prefix("tribal")
                .filename_suffix(suffix)
                .build(&logging.file_directory)
                .map_err(|source| TelemetryError::FileAppenderInit {
                    path: logging.file_directory.clone(),
                    source,
                })?;

            let (non_blocking, guard) = tracing_appender::non_blocking(appender);
            (non_blocking, guard)
        }
    };

    // -- Trace pipeline ---------------------------------------------------
    //
    // When telemetry is enabled, build a `SdkTracerProvider` with whichever
    // exporters are configured (OTLP, console, file).  `enabled` acts as a
    // master switch — when false, sub-flags are silently ignored.

    let otlp_enabled = telemetry.enabled && telemetry.otlp_endpoint.is_some();

    let (tracer_provider, otel_layer) = if telemetry.enabled {
        let mut builder =
            SdkTracerProvider::builder().with_resource(otlp::build_resource(telemetry));

        // -- OTLP exporter (optional)
        if telemetry.otlp_endpoint.is_some() {
            let exporter = otlp::build_otlp_span_exporter(telemetry)?;
            builder = builder.with_batch_exporter(exporter);
        }

        // -- Console exporter (optional)
        if telemetry.console_export {
            let exporter = WriterSpanExporter::new(std::io::stderr());
            builder = builder.with_batch_exporter(exporter);
        }

        // -- File exporter (optional)
        if telemetry.file_export {
            let rotation = rotation_from(telemetry.file_rotation);

            let appender = RollingFileAppender::builder()
                .rotation(rotation)
                .filename_prefix("tribal-traces")
                .filename_suffix("jsonl")
                .build(&telemetry.file_directory)
                .map_err(|source| TelemetryError::FileAppenderInit {
                    path: telemetry.file_directory.clone(),
                    source,
                })?;

            let exporter = WriterSpanExporter::new(appender);
            builder = builder.with_batch_exporter(exporter);
        }

        let provider = builder.build();
        let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("tribal"));
        (Some(provider), Some(layer))
    } else {
        (None, None)
    };

    let (meter_provider, recorder): (Option<_>, Arc<dyn MetricsRecorder>) = if otlp_enabled {
        let provider = otlp::build_meter_provider(telemetry)?;
        let meter = provider.meter("tribal");
        let metrics = Metrics::new(&meter);
        (Some(provider), Arc::new(OtelMetricsRecorder::new(metrics)))
    } else {
        (None, noop_recorder())
    };

    // -- Log-capture layer -----------------------------------------------
    //
    // Captures every event the `env_filter` admits into the ring and mirrors
    // it live on the control bus. The ring returns to the caller for
    // `logs.tail`; the layer joins the stack below.
    let (log_layer, log_ring) = LogRingLayer::new(control_events);

    // -- Subscriber assembly ---------------------------------------------
    //
    // JSON and Pretty produce different concrete types, so the
    // `set_global_default` call is duplicated in each branch.
    match logging.format {
        LogFormat::Json => {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(otel_layer)
                .with(log_layer)
                .with(
                    fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_target(true)
                        .with_current_span(true)
                        .with_span_list(true),
                );
            set_global_default(subscriber)
                .map_err(|source| TelemetryError::SetGlobalDefault { source })?;
        }
        LogFormat::Pretty => {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(otel_layer)
                .with(log_layer)
                .with(fmt::layer().pretty().with_writer(writer).with_target(true));
            set_global_default(subscriber)
                .map_err(|source| TelemetryError::SetGlobalDefault { source })?;
        }
    }

    if logging.output == LogOutput::File && logging.used_temp_dir_fallback {
        tracing::warn!(
            directory = %logging.file_directory,
            "no standard state/data directory found; using temporary directory for log files",
        );
    }

    if telemetry.file_export && telemetry.used_temp_dir_fallback {
        tracing::warn!(
            directory = %telemetry.file_directory,
            "no standard data directory found; using temporary directory for trace files",
        );
    }

    Ok((
        TelemetryGuard::new(guard, tracer_provider, meter_provider),
        recorder,
        log_ring,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Serialises tests that manipulate the process-global `INITIALISED`
    /// flag.  Without this, parallel test threads race on the `AtomicBool`.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    // Note: each test calls `INITIALISED.store(false, ...)` at the start
    // to reset state from whichever test ran previously.  Error-path tests
    // do not need end-of-test cleanup because `init_subscriber` resets the
    // flag automatically when it fails.  Tests where the second call may
    // *succeed* still need explicit cleanup at the end.

    #[test]
    fn test_invalid_filter_directive_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            level: "not valid [[".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&config, &TelemetryConfig::default());

        assert!(
            matches!(result, Err(TelemetryError::InvalidFilterDirective { .. })),
            "expected InvalidFilterDirective, got {:?}",
            result.err(),
        );
        assert!(
            !INITIALISED.load(Ordering::SeqCst),
            "flag should be reset after failure"
        );
    }

    #[test]
    fn test_file_output_with_invalid_directory_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        // Use a regular file as the "directory" to guarantee the appender
        // cannot create it, regardless of process permissions.
        let tmp = tempfile::NamedTempFile::new().expect("should create temp file");

        let config = LoggingConfig {
            output: LogOutput::File,
            file_directory: tmp.path().display().to_string(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&config, &TelemetryConfig::default());

        assert!(
            matches!(result, Err(TelemetryError::FileAppenderInit { .. })),
            "expected FileAppenderInit, got {:?}",
            result.err(),
        );
        assert!(
            !INITIALISED.load(Ordering::SeqCst),
            "flag should be reset after failure"
        );
    }

    #[test]
    fn test_failed_init_allows_retry() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        // First call with an invalid directive fails.
        let bad_config = LoggingConfig {
            level: "not valid [[".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&bad_config, &TelemetryConfig::default());
        assert!(result.is_err());

        // Flag was reset — a subsequent call with valid config is not
        // rejected as `SubscriberAlreadyInitialised`.
        let good_config = LoggingConfig::default();
        let result = init_subscriber(&good_config, &TelemetryConfig::default());

        // We expect `SetGlobalDefault` (the unit test process may already
        // have a subscriber) rather than `SubscriberAlreadyInitialised`.
        // The key assertion is that the flag did not block retry.
        assert!(
            !matches!(result, Err(TelemetryError::SubscriberAlreadyInitialised)),
            "flag should have been reset after failed init, got {:?}",
            result.err(),
        );

        // Clean up: the second call may have succeeded, leaving the flag true.
        INITIALISED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_trace_file_export_with_invalid_directory_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        // Use a regular file as the "directory" to guarantee the appender
        // cannot create it, regardless of process permissions.
        let tmp = tempfile::NamedTempFile::new().expect("should create temp file");

        let logging = LoggingConfig::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            console_export: false,
            file_export: true,
            file_directory: tmp.path().display().to_string(),
            ..TelemetryConfig::default()
        };
        let result = init_subscriber(&logging, &telemetry);

        assert!(
            matches!(result, Err(TelemetryError::FileAppenderInit { .. })),
            "expected FileAppenderInit, got {:?}",
            result.err(),
        );
        assert!(
            !INITIALISED.load(Ordering::SeqCst),
            "flag should be reset after failure"
        );
    }
}
