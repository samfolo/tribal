//! Config file rendering, writing, and divergence detection.
//!
//! Handles writing the minimal `tribal.yaml` on first run and detecting
//! when the resolved configuration diverges from an existing file.

use std::{io, path::Path};

use serde::Serialize;
use tribal_config::TribalConfig;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Header comment written at the top of the generated config file.
const CONFIG_HEADER: &str = "# Tribal configuration\n# See documentation for full options.\n";

/// Warning emitted when the existing config file cannot be read.
pub(super) const WARNING_CONFIG_UNREADABLE: &str =
    "could not read existing config file for comparison";

/// Warning emitted when the existing config file contains invalid YAML.
pub(super) const WARNING_CONFIG_UNPARSEABLE: &str =
    "existing config file could not be parsed, skipping divergence check";

/// Warning emitted when the resolved database URL differs from the value
/// in the existing config file.
pub(super) const WARNING_DATABASE_URL_DIVERGENCE: &str = "resolved database URL differs from the config file's database.url; \
     the config file was not updated";

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

/// Minimal config file structure containing only the database URL.
///
/// Serialised via `serde_yaml` to guarantee valid YAML output.
#[derive(Serialize)]
struct MinimalConfig<'a> {
    database: MinimalDatabaseConfig<'a>,
}

/// Database section of the minimal config file.
#[derive(Serialize)]
struct MinimalDatabaseConfig<'a> {
    url: &'a str,
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
    config_path: &str,
    config: &TribalConfig,
) -> Result<ConfigFileOutcome, AppError> {
    let path = Path::new(config_path);

    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        let warnings = check_divergence(config_path, config).await;
        return Ok(ConfigFileOutcome::AlreadyExists { warnings });
    }

    let content = render_minimal(&config.database.url)?;

    tokio::fs::write(path, content)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("write config file {}", path.display()),
            source,
        })?;

    Ok(ConfigFileOutcome::Written {
        path: config_path.to_owned(),
    })
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Renders the minimal config file content.
///
/// # Errors
///
/// Returns [`AppError::SetupIo`] if YAML serialisation fails.
fn render_minimal(database_url: &str) -> Result<String, AppError> {
    let minimal = MinimalConfig {
        database: MinimalDatabaseConfig { url: database_url },
    };

    let body = serde_yaml::to_string(&minimal).map_err(|err| AppError::SetupIo {
        context: "serialise minimal config".to_owned(),
        source: io::Error::other(err),
    })?;

    Ok(format!("{CONFIG_HEADER}\n{body}"))
}

// ---------------------------------------------------------------------------
// Divergence detection
// ---------------------------------------------------------------------------

/// Compares the resolved configuration against the existing config file
/// and returns warnings for any divergent values.
///
/// Never includes credential-bearing values in warning messages.
async fn check_divergence(config_path: &str, config: &TribalConfig) -> Vec<String> {
    let mut warnings = Vec::new();

    let content = match tokio::fs::read_to_string(config_path).await {
        Ok(c) => c,
        Err(err) => {
            warnings.push(format!("{WARNING_CONFIG_UNREADABLE}: {err}"));
            return warnings;
        }
    };

    let Ok(existing) = serde_yaml::from_str::<serde_yaml::Value>(&content) else {
        warnings.push(WARNING_CONFIG_UNPARSEABLE.to_owned());
        return warnings;
    };

    check_database_url_divergence(&existing, config, &mut warnings);

    warnings
}

/// Checks whether the resolved database URL differs from the config file's
/// `database.url` value.
fn check_database_url_divergence(
    file_value: &serde_yaml::Value,
    config: &TribalConfig,
    warnings: &mut Vec<String>,
) {
    let Some(file_url) = file_value
        .get("database")
        .and_then(|d| d.get("url"))
        .and_then(|u| u.as_str())
    else {
        return;
    };

    if file_url != config.database.url {
        warnings.push(WARNING_DATABASE_URL_DIVERGENCE.to_owned());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Rendering ----------------------------------------------------------

    #[test]
    fn test_render_minimal_is_valid_yaml() {
        let content = render_minimal("postgres://h/db").unwrap();
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&content).expect("rendered config should be valid YAML");
        let url = parsed
            .get("database")
            .and_then(|d| d.get("url"))
            .and_then(|u| u.as_str());
        assert_eq!(url, Some("postgres://h/db"));
    }

    #[test]
    fn test_render_minimal_contains_header() {
        let content = render_minimal("postgres://h/db").unwrap();
        assert!(content.starts_with(CONFIG_HEADER));
    }

    // -- Write behaviour ----------------------------------------------------

    #[tokio::test]
    async fn test_write_if_absent_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        let config = TribalConfig::default();
        let outcome = write_if_absent(path_str, &config).await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::Written { .. }));
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&content).expect("written file should be valid YAML");
        assert!(parsed.get("database").is_some());
    }

    #[tokio::test]
    async fn test_write_if_absent_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        let original = "# my custom config\n";
        tokio::fs::write(&path, original).await.unwrap();

        let config = TribalConfig::default();
        let outcome = write_if_absent(path_str, &config).await.unwrap();

        assert!(matches!(outcome, ConfigFileOutcome::AlreadyExists { .. }));
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, original, "existing file should not be overwritten");
    }

    // -- Divergence detection -----------------------------------------------

    #[tokio::test]
    async fn test_check_divergence_warns_on_url_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        tokio::fs::write(&path, "database:\n  url: \"postgres://file-url/db\"\n")
            .await
            .unwrap();

        let mut config = TribalConfig::default();
        config.database.url = "postgres://resolved-url/db".to_owned();

        let warnings = check_divergence(path_str, &config).await;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], WARNING_DATABASE_URL_DIVERGENCE);
    }

    #[tokio::test]
    async fn test_check_divergence_no_warning_when_urls_match() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        let mut config = TribalConfig::default();
        config.database.url = "postgres://same/db".to_owned();

        tokio::fs::write(&path, "database:\n  url: \"postgres://same/db\"\n")
            .await
            .unwrap();

        let warnings = check_divergence(path_str, &config).await;
        assert!(warnings.is_empty());
    }

    #[tokio::test]
    async fn test_check_divergence_handles_malformed_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        tokio::fs::write(&path, "{{{{not valid yaml").await.unwrap();

        let config = TribalConfig::default();
        let warnings = check_divergence(path_str, &config).await;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], WARNING_CONFIG_UNPARSEABLE);
    }

    #[tokio::test]
    async fn test_check_divergence_skips_when_no_database_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        tokio::fs::write(&path, "server:\n  transport: http\n")
            .await
            .unwrap();

        let config = TribalConfig::default();
        let warnings = check_divergence(path_str, &config).await;
        assert!(warnings.is_empty());
    }
}
