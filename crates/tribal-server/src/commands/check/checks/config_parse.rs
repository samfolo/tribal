//! Outcome constructors and action for the `config_parse` check.

use std::path::{Path, PathBuf};

use tribal_config::{ConfigError, load_config};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};
use crate::commands::common::DATABASE_COMMAND_DEFAULTS;

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

/// Loads the config file referenced by `state` and, on success, stores
/// the parsed config back on state so downstream steps can consume it.
// `load_config` is sync, but the step dispatcher requires every action
// to share the `async fn act` signature.
#[allow(clippy::unused_async)]
pub(in crate::commands::check) async fn act(state: &mut CheckState) -> CheckOutcome {
    // `canonicalize` can resolve symlinks into non-UTF-8 byte sequences;
    // when that happens the parse never gets a chance to run.
    let Some(path_str) = state.config_path.to_str() else {
        return CheckOutcome::Fail {
            detail: CheckDetail::ConfigParseFailed {
                error: "config path is not valid UTF-8".to_owned(),
                path: state.config_path.clone(),
            },
            remediation: CheckRemediation::InspectConfigFile {
                path: state.config_path.clone(),
            },
        };
    };
    match load_config(path_str, None, Some(&DATABASE_COMMAND_DEFAULTS)) {
        Ok(config) => {
            let path = state.config_path.clone();
            state.config = Some(config);
            CheckOutcome::config_parse_loaded(path)
        }
        Err(error) => CheckOutcome::config_parse_failed(&error, &state.config_path),
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
