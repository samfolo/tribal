//! Config file divergence detection.
//!
//! Compares the raw YAML content of an existing config file against the
//! resolved [`TribalConfig`] and produces warnings for any values that
//! differ. This is a pure synchronous comparison — file I/O is the
//! caller's responsibility.

use crate::TribalConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Warning emitted when the config file contains invalid YAML.
pub const WARNING_CONFIG_UNPARSEABLE: &str =
    "existing config file could not be parsed, skipping divergence check";

/// Warning emitted when the resolved database URL differs from the value
/// in the existing config file.
pub const WARNING_DATABASE_URL_DIVERGENCE: &str = "resolved database URL differs from the config file's database.url; \
     the config file was not updated";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compares the raw YAML content of a config file against the resolved
/// configuration and returns warnings for any divergent values.
///
/// Never includes credential-bearing values in warning messages.
///
/// Returns an empty `Vec` when no divergence is detected or the file
/// lacks the compared fields.
#[must_use]
pub fn check_config_divergence(file_content: &str, config: &TribalConfig) -> Vec<String> {
    let mut warnings = Vec::new();

    let Ok(existing) = serde_yaml::from_str::<serde_yaml::Value>(file_content) else {
        warnings.push(WARNING_CONFIG_UNPARSEABLE.to_owned());
        return warnings;
    };

    check_database_url(&existing, config, &mut warnings);

    warnings
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Checks whether the resolved database URL differs from the config file's
/// `database.url` value.
fn check_database_url(
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

    // -- Divergence detection -----------------------------------------------

    #[test]
    fn test_warns_on_url_mismatch() {
        let yaml = "database:\n  url: \"postgres://file-url/db\"\n";
        let mut config = TribalConfig::default();
        config.database.url = "postgres://resolved-url/db".to_owned();

        let warnings = check_config_divergence(yaml, &config);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], WARNING_DATABASE_URL_DIVERGENCE);
    }

    #[test]
    fn test_no_warning_when_urls_match() {
        let mut config = TribalConfig::default();
        config.database.url = "postgres://same/db".to_owned();

        let yaml = "database:\n  url: \"postgres://same/db\"\n";
        let warnings = check_config_divergence(yaml, &config);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_handles_malformed_yaml() {
        let config = TribalConfig::default();
        let warnings = check_config_divergence("{{{{not valid yaml", &config);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], WARNING_CONFIG_UNPARSEABLE);
    }

    #[test]
    fn test_skips_when_no_database_key() {
        let config = TribalConfig::default();
        let yaml = "server:\n  transport: http\n";
        let warnings = check_config_divergence(yaml, &config);
        assert!(warnings.is_empty());
    }
}
