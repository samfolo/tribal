//! Crate-level error type for `tribal-server`.
//!
//! [`AppError`] is the single error enum for the server binary.
//! All variants use named fields where applicable; wrapped errors carry
//! `#[source]` for error chain propagation.

use std::io;

use thiserror::Error;
use tribal_config::ConfigError;

// ---------------------------------------------------------------------------
// AppError
// ---------------------------------------------------------------------------

/// Errors that can occur during application startup or subcommand execution.
///
/// All variants use named fields where applicable. `#[source]` preserves
/// the error chain for debugging.
#[derive(Debug, Error)]
pub enum AppError {
    /// A CLI argument combination was invalid.
    #[error("{source}")]
    Cli {
        /// The underlying clap validation error.
        #[source]
        source: clap::Error,
    },

    /// Configuration loading or validation failed.
    #[error("{source}")]
    Config {
        /// The underlying configuration error.
        #[source]
        source: ConfigError,
    },

    /// Failed to write help text to stdout.
    #[error("failed to write help output")]
    HelpOutput {
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
}

impl From<clap::Error> for AppError {
    fn from(source: clap::Error) -> Self {
        Self::Cli { source }
    }
}

impl From<ConfigError> for AppError {
    fn from(source: ConfigError) -> Self {
        Self::Config { source }
    }
}

impl From<io::Error> for AppError {
    fn from(source: io::Error) -> Self {
        Self::HelpOutput { source }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn test_display_cli_error() {
        let source = clap::Error::raw(
            ErrorKind::ArgumentConflict,
            "--bind cannot be used with --transport stdio",
        );
        let err = AppError::Cli { source };
        assert!(
            err.to_string()
                .contains("--bind cannot be used with --transport stdio"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_config_error() {
        let err = AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec!["database.url must not be empty".into()],
            },
        };
        assert!(
            err.to_string().contains("database.url"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_help_output() {
        let err = AppError::HelpOutput {
            source: io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"),
        };
        assert_eq!(err.to_string(), "failed to write help output");
    }
}
