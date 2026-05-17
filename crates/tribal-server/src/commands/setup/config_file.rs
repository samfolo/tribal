//! Config file writing and divergence detection.
//!
//! Handles writing `tribal.yaml` on first run and detecting when the
//! resolved configuration diverges from an existing file. YAML content
//! is rendered by the caller; this module concerns itself only with the
//! file-on-disk side of the contract.

use std::path::{Path, PathBuf};

use tribal_config::{TribalConfig, check_config_divergence};

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Warning emitted when the existing config file cannot be read.
pub(crate) const WARNING_CONFIG_UNREADABLE: &str =
    "could not read existing config file for comparison";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Outcome of the config file write attempt.
#[derive(Debug)]
pub enum ConfigFileOutcome {
    /// The file was written successfully.
    Written {
        /// Path where the file was written.
        path: PathBuf,
    },
    /// The file already existed and was not modified.
    AlreadyExists {
        /// Path of the existing file.
        path: PathBuf,
        /// Warnings about configuration divergence (may be empty).
        warnings: Vec<String>,
    },
}

impl ConfigFileOutcome {
    /// The path the write targeted, regardless of variant.
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Written { path } | Self::AlreadyExists { path, .. } => path,
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Writes `content` to `config_path` if the file does not already exist.
///
/// When the file exists, the on-disk content is compared against the
/// resolved `config` and any divergence is returned as warnings rather
/// than overwriting. When the file does not exist, `content` is written
/// verbatim — choice of YAML shape is the caller's.
pub(crate) async fn write_if_absent(
    config_path: &Path,
    config: &TribalConfig,
    content: &str,
) -> Result<ConfigFileOutcome, AppError> {
    if tokio::fs::try_exists(config_path).await.unwrap_or(false) {
        let warnings = read_and_check_divergence(config_path, config).await;
        return Ok(ConfigFileOutcome::AlreadyExists {
            path: config_path.to_path_buf(),
            warnings,
        });
    }

    tokio::fs::write(config_path, content)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("write config file {}", config_path.display()),
            source,
        })?;

    Ok(ConfigFileOutcome::Written {
        path: config_path.to_path_buf(),
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
        let content = "database:\n  url: \"postgres://test/db\"\n";
        let outcome = write_if_absent(&path, &config, content).await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::Written { .. }));
        let written = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(written, content);
    }

    #[tokio::test]
    async fn test_write_if_absent_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");

        let original = "# my custom config\n";
        tokio::fs::write(&path, original).await.unwrap();

        let config = TribalConfig::default();
        let outcome = write_if_absent(&path, &config, "ignored\n").await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::AlreadyExists { .. }));
        let written = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(written, original, "existing file should not be overwritten");
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
