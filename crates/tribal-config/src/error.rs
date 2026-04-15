//! Configuration error types.

use thiserror::Error;

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
    /// Contains every invalid field, not just the first.
    #[error("configuration validation failed:\n{}", format_errors(errors))]
    ValidationFailed {
        /// All validation errors collected during the check.
        errors: Vec<String>,
    },

    /// Configuration serialisation failed.
    #[error("failed to render configuration: {source}")]
    Render {
        /// The underlying serialisation error.
        #[source]
        source: Box<serde_yaml::Error>,
    },
}

fn format_errors(errors: &[String]) -> String {
    errors
        .iter()
        .map(|e| format!("  - {e}"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_validation_failed() {
        let err = ConfigError::ValidationFailed {
            errors: vec![
                "database.url must not be empty".into(),
                "worker.max_concurrent_tasks must be greater than zero".into(),
            ],
        };
        let display = err.to_string();
        assert!(display.contains("database.url must not be empty"));
        assert!(display.contains("worker.max_concurrent_tasks"));
    }
}
