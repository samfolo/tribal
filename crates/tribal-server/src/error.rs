//! Crate-level error type for `tribal-server`.
//!
//! [`AppError`] is the single error enum for the server binary.
//! All variants use named fields where applicable; wrapped errors carry
//! `#[source]` for error chain propagation.

use thiserror::Error;

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

    /// Failed to write help text to stdout.
    #[error("failed to write help output")]
    HelpOutput {
        /// The underlying formatting error.
        #[source]
        source: std::fmt::Error,
    },
}

impl From<clap::Error> for AppError {
    fn from(source: clap::Error) -> Self {
        Self::Cli { source }
    }
}

impl From<std::fmt::Error> for AppError {
    fn from(source: std::fmt::Error) -> Self {
        Self::HelpOutput { source }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_cli_error() {
        let source = clap::Error::raw(
            clap::error::ErrorKind::ArgumentConflict,
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
    fn test_display_help_output() {
        let err = AppError::HelpOutput {
            source: std::fmt::Error,
        };
        assert_eq!(err.to_string(), "failed to write help output");
    }
}
