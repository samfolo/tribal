//! Outcome constructors for the `config_parse` check.

use std::path::{Path, PathBuf};

use tribal_config::ConfigError;

use super::types::{CheckDetail, CheckOutcome, CheckRemediation};

impl CheckOutcome {
    /// Constructs the outcome for a successful config load from `path`.
    pub(in crate::commands::check) fn config_parse_loaded(path: PathBuf) -> Self {
        Self::Pass {
            detail: CheckDetail::ConfigLoaded { path },
        }
    }

    /// Constructs the outcome for a failed config load against `path`.
    pub(in crate::commands::check) fn config_parse_failed(
        error: &ConfigError,
        path: &Path,
    ) -> Self {
        Self::Fail {
            detail: CheckDetail::ConfigParseFailed {
                error: error.to_string(),
                path: path.to_path_buf(),
            },
            remediation: CheckRemediation::InspectConfigFile {
                path: path.to_path_buf(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parse_loaded_is_pass() {
        let path = PathBuf::from("/etc/tribal/config.yaml");
        let outcome = CheckOutcome::config_parse_loaded(path.clone());
        assert!(matches!(
            &outcome,
            CheckOutcome::Pass {
                detail: CheckDetail::ConfigLoaded { path: p },
            } if p == &path,
        ));
    }

    #[test]
    fn test_config_parse_failed_carries_error_and_remediation() {
        let path = PathBuf::from("/etc/tribal/config.yaml");
        let error = ConfigError::ValidationFailed {
            errors: vec!["database.url must not be empty".into()],
        };
        let outcome = CheckOutcome::config_parse_failed(&error, &path);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ConfigParseFailed {
                    error: msg,
                    path: detail_path,
                },
                remediation: CheckRemediation::InspectConfigFile { path: rec_path },
            } if msg.contains("database.url must not be empty")
                && detail_path == &path
                && rec_path == &path,
        ));
    }
}
