//! Crate-level error type for `tribal-telemetry`.
//!
//! [`TelemetryError`] is the single error enum for the telemetry layer.
//! All variants use named fields where applicable; wrapped errors carry
//! `#[source]` for error chain propagation.

use thiserror::Error;

/// Errors produced by the telemetry initialisation layer.
///
/// All variants use named fields where applicable.  `#[source]` preserves
/// the error chain for debugging.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// The global tracing subscriber has already been initialised.
    ///
    /// [`init_subscriber`](crate::init_subscriber) can only be called
    /// once per program lifetime.  Subsequent calls return this error
    /// instead of panicking.
    #[error("subscriber already initialised")]
    SubscriberAlreadyInitialised,

    /// The filter directive string is invalid.
    ///
    /// The directive comes from the `level` field of
    /// [`LoggingConfig`](tribal_config::LoggingConfig).  Directive strings support
    /// per-target granularity, e.g. `"info,tribal_db=debug"`.
    #[error("invalid filter directive: {directive}")]
    InvalidFilterDirective {
        /// The directive string that could not be parsed.
        directive: String,
        /// The underlying parse error from `tracing-subscriber`.
        #[source]
        source: tracing_subscriber::filter::ParseError,
    },

    /// The level filter could not be swapped because the subscriber that
    /// owns it is gone.
    #[error("failed to reload the level filter")]
    FilterReload {
        /// The underlying reload error.
        #[source]
        source: tracing_subscriber::reload::Error,
    },

    /// Failed to initialise the rolling file appender.
    ///
    /// Covers directory creation failures, permission errors, and other
    /// I/O problems during appender setup.
    #[error("failed to initialise file appender at {path}")]
    FileAppenderInit {
        /// The directory path passed to the appender builder.
        path: String,
        /// The underlying initialisation error.
        #[source]
        source: tracing_appender::rolling::InitError,
    },

    /// Failed to set the global default subscriber.
    ///
    /// This typically means another library already called
    /// `tracing::subscriber::set_global_default`.
    #[error("failed to set global default subscriber")]
    SetGlobalDefault {
        /// The underlying error from `tracing`.
        #[source]
        source: tracing::subscriber::SetGlobalDefaultError,
    },

    /// The OTLP endpoint is not configured.
    ///
    /// Returned when the OTLP pipeline builders are called without
    /// an endpoint, indicating a logic error in the caller.
    #[error("OTLP endpoint not configured")]
    OtlpEndpointMissing,

    /// The `otlp_protocol` configuration value is not recognised.
    ///
    /// Only `"grpc"` and `"http"` are supported.
    #[error("unrecognised OTLP protocol: {protocol}")]
    UnrecognisedOtlpProtocol {
        /// The unrecognised protocol string.
        protocol: String,
    },

    /// Failed to initialise the OTLP trace export pipeline.
    #[error("failed to initialise OTLP trace pipeline")]
    OtlpTracePipelineInit {
        /// The underlying exporter build error.
        #[source]
        source: opentelemetry_otlp::ExporterBuildError,
    },

    /// Failed to initialise the OpenTelemetry metrics pipeline.
    #[error("failed to initialise metrics pipeline")]
    MetricsPipelineInit {
        /// The underlying exporter build error.
        #[source]
        source: opentelemetry_otlp::ExporterBuildError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_otlp_endpoint_missing() {
        let err = TelemetryError::OtlpEndpointMissing;
        assert_eq!(err.to_string(), "OTLP endpoint not configured");
    }

    #[test]
    fn test_display_subscriber_already_initialised() {
        let err = TelemetryError::SubscriberAlreadyInitialised;
        assert_eq!(err.to_string(), "subscriber already initialised");
    }

    #[test]
    fn test_display_invalid_filter_directive() {
        let source = tracing_subscriber::EnvFilter::try_new("not valid [[").unwrap_err();
        let err = TelemetryError::InvalidFilterDirective {
            directive: "not valid [[".to_owned(),
            source,
        };
        assert!(
            err.to_string().starts_with("invalid filter directive: "),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_unrecognised_otlp_protocol() {
        let err = TelemetryError::UnrecognisedOtlpProtocol {
            protocol: "quic".to_owned(),
        };
        assert_eq!(err.to_string(), "unrecognised OTLP protocol: quic");
    }

    #[test]
    fn test_display_otlp_trace_pipeline_init() {
        let err = TelemetryError::OtlpTracePipelineInit {
            source: opentelemetry_otlp::ExporterBuildError::NoHttpClient,
        };
        assert_eq!(err.to_string(), "failed to initialise OTLP trace pipeline");
    }

    #[test]
    fn test_display_metrics_pipeline_init() {
        let err = TelemetryError::MetricsPipelineInit {
            source: opentelemetry_otlp::ExporterBuildError::NoHttpClient,
        };
        assert_eq!(err.to_string(), "failed to initialise metrics pipeline");
    }

    #[test]
    fn test_display_file_appender_init() {
        // Use a regular file as the "directory" to guarantee the appender
        // cannot create it, regardless of process permissions.
        let tmp = tempfile::NamedTempFile::new().expect("should create temp file");
        let file_path = tmp.path().display().to_string();

        let result = tracing_appender::rolling::RollingFileAppender::builder()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("tribal")
            .filename_suffix("jsonl")
            .build(&file_path);

        let init_error = result.expect_err("should fail when path is a regular file");
        let err = TelemetryError::FileAppenderInit {
            path: file_path.clone(),
            source: init_error,
        };
        assert_eq!(
            err.to_string(),
            format!("failed to initialise file appender at {file_path}"),
        );
    }
}
