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

    /// Failed to create the log output directory.
    #[error("failed to create log directory at {path}")]
    DirectoryCreation {
        /// The directory path that could not be created.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_display_directory_creation() {
        let err = TelemetryError::DirectoryCreation {
            path: "/nonexistent/dir".to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            ),
        };
        assert_eq!(
            err.to_string(),
            "failed to create log directory at /nonexistent/dir",
        );
    }
}
