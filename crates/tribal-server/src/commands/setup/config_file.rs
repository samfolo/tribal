//! Config file writing and divergence detection.
//!
//! Handles writing the minimal `tribal.yaml` on first run and detecting
//! when the resolved configuration diverges from an existing file.
//! YAML rendering and divergence comparison are delegated to
//! `tribal-config`.

use std::path::Path;

use tribal_config::{TribalConfig, check_config_divergence, render_minimal_config};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Warning emitted when the existing config file cannot be read.
pub(super) const WARNING_CONFIG_UNREADABLE: &str =
    "could not read existing config file for comparison";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Outcome of the config file write attempt.
pub(super) enum ConfigFileOutcome {
    /// The file was written successfully.
    Written {
        /// Path where the file was written.
        path: String,
    },
    /// The file already existed and was not modified.
    AlreadyExists {
        /// Warnings about configuration divergence (may be empty).
        warnings: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Writes the minimal config file if it does not already exist.
///
/// When the file exists, checks for configuration divergence and returns
/// diagnostic messages for the caller to present. When the file does not
/// exist, writes a minimal YAML containing only `database.url`.
///
/// Returns a [`ConfigFileOutcome`] describing what happened.
pub(super) async fn write_if_absent(
    config_path: &Path,
    config: &TribalConfig,
) -> Result<ConfigFileOutcome, AppError> {
    if tokio::fs::try_exists(config_path).await.unwrap_or(false) {
        let warnings = read_and_check_divergence(config_path, config).await;
        return Ok(ConfigFileOutcome::AlreadyExists { warnings });
    }

    let content = render_minimal_config(&config.database.url)
        .map_err(|source| AppError::Config { source })?;

    tokio::fs::write(config_path, content)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("write config file {}", config_path.display()),
            source,
        })?;

    Ok(ConfigFileOutcome::Written {
        path: config_path.display().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Reads the existing config file and delegates divergence detection to
/// `tribal-config`.
async fn read_and_check_divergence(config_path: &Path, config: &TribalConfig) -> Vec<String> {
    let content = match tokio::fs::read_to_string(config_path).await {
        Ok(c) => c,
        Err(err) => {
            return vec![format!("{WARNING_CONFIG_UNREADABLE}: {err}")];
        }
    };

    check_config_divergence(&content, config)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Write behaviour ----------------------------------------------------

    #[tokio::test]
    async fn test_write_if_absent_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");

        let config = TribalConfig::default();
        let outcome = write_if_absent(&path, &config).await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::Written { .. }));
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("database"));
    }

    #[tokio::test]
    async fn test_write_if_absent_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");

        let original = "# my custom config\n";
        tokio::fs::write(&path, original).await.unwrap();

        let config = TribalConfig::default();
        let outcome = write_if_absent(&path, &config).await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::AlreadyExists { .. }));
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, original, "existing file should not be overwritten");
    }

    // -- Divergence via file read -------------------------------------------

    #[tokio::test]
    async fn test_read_and_check_divergence_warns_on_url_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");

        tokio::fs::write(&path, "database:\n  url: \"postgres://file-url/db\"\n")
            .await
            .unwrap();

        let config = TribalConfig::minimum_valid("postgres://resolved-url/db");

        let warnings = read_and_check_divergence(&path, &config).await;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], tribal_config::WARNING_DATABASE_URL_DIVERGENCE);
    }

    #[tokio::test]
    async fn test_read_and_check_divergence_handles_read_failure() {
        let config = TribalConfig::default();
        let warnings =
            read_and_check_divergence(Path::new("/nonexistent/path.yaml"), &config).await;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].starts_with(WARNING_CONFIG_UNREADABLE));
    }
}
