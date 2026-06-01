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

    /// A removed top-level `embedding` config shape was detected.
    ///
    /// The `embedding.*` fields and `TRIBAL_EMBEDDING__*` env vars from before
    /// the credential-catalogue reshape no longer exist; this is the legible
    /// migration message that replaces the bare figment "unknown field" parse
    /// error, naming the two sections that supersede it.
    #[error(
        "the `embedding` config section has been removed: the genesis seed now \
         lives under `init.embedding` (provider, model, dimensions, base_url) and \
         the provider credential lives in the `credentials` catalogue (e.g. \
         `credentials.openai_default` with an `api_key`, or \
         `TRIBAL_CREDENTIALS__OPENAI_DEFAULT__API_KEY`); move the removed \
         {detected} accordingly"
    )]
    RemovedEmbeddingShape {
        /// The removed input that was detected (a YAML key or an env var).
        detected: RemovedEmbeddingSource,
    },
}

/// The input that tripped [`ConfigError::RemovedEmbeddingShape`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovedEmbeddingSource {
    /// A top-level `embedding:` mapping in the YAML config file.
    YamlSection,
    /// One or more `TRIBAL_EMBEDDING__*` environment variables.
    EnvVar { name: String },
}

impl std::fmt::Display for RemovedEmbeddingSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YamlSection => f.write_str("top-level `embedding:` config section"),
            Self::EnvVar { name } => write!(f, "`{name}` environment variable"),
        }
    }
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
