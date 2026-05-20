//! Configuration error types.

use thiserror::Error;

use crate::validation::{Diagnostics, ValidationError};

// ---------------------------------------------------------------------------
// ConfigError
// ---------------------------------------------------------------------------

/// Errors produced during configuration loading or validation.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to load or deserialise the configuration.
    #[error("failed to load configuration: {source}")]
    Load {
        /// The underlying figment error.
        #[source]
        source: Box<figment::Error>,
    },

    /// The merged configuration failed validation.
    ///
    /// Contains every invariant violation, not just the first.
    #[error("configuration validation failed:\n{}", format_diagnostics(diagnostics.as_slice()))]
    ValidationFailed {
        /// Typed diagnostics collected during validation.
        diagnostics: Diagnostics,
    },

    /// Configuration serialisation failed.
    #[error("failed to render configuration: {source}")]
    Render {
        /// The underlying serialisation error.
        #[source]
        source: Box<serde_yaml::Error>,
    },
}

/// Renders `items` as one indented bullet per line.
///
/// Lives here rather than on [`Diagnostics`] because the bullet/indent
/// shape is a property of the envelope error message, not of the
/// collection itself.
fn format_diagnostics(items: &[ValidationError]) -> String {
    items
        .iter()
        .map(|d| format!("  - {d}"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::ConfigPath;

    #[test]
    fn test_display_validation_failed() {
        let err = ConfigError::ValidationFailed {
            diagnostics: Diagnostics::from(vec![
                ValidationError::Empty {
                    field: ConfigPath::from_static("database.url"),
                },
                ValidationError::BelowMin {
                    field: ConfigPath::from_static("worker.max_concurrent_tasks"),
                    value: 0,
                    min: 1,
                },
            ]),
        };
        let display = err.to_string();
        assert!(display.contains("database.url must not be empty"));
        assert!(display.contains("worker.max_concurrent_tasks must be greater than zero"));
        assert!(
            display.starts_with("configuration validation failed:\n  - "),
            "expected envelope + bullet indent, got: {display}",
        );
    }
}
