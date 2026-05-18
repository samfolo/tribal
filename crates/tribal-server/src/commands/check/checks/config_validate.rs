//! Outcome constructors for the `config_validate` check.
//!
//! Targeted hints map known validation-error prefixes (the API-key
//! invariants exposed by `tribal_config::validation`) to a concrete
//! action.  Errors without a known hint render with no remediation.

use tribal_config::{
    EMBEDDING_API_KEY_REQUIRED_PREFIX, EXTRACTION_API_KEY_REQUIRED_PREFIX,
    RELATION_API_KEY_REQUIRED_PREFIX, TRIAGE_API_KEY_REQUIRED_PREFIX, env_var_for_path,
};

use super::types::{CheckDetail, CheckOutcome, CheckRemediation};

/// Configuration paths whose API-key prefix triggers a targeted hint.
///
/// Each row is `(prefix, config_path)`.  The config path doubles as the
/// YAML field a hint mentions and the input to [`env_var_for_path`].
const API_KEY_HINT_PATHS: &[(&str, &str)] = &[
    (EMBEDDING_API_KEY_REQUIRED_PREFIX, "embedding.api_key"),
    (
        EXTRACTION_API_KEY_REQUIRED_PREFIX,
        "inference.extraction.api_key",
    ),
    (TRIAGE_API_KEY_REQUIRED_PREFIX, "inference.triage.api_key"),
    (
        RELATION_API_KEY_REQUIRED_PREFIX,
        "inference.relation.api_key",
    ),
];

impl CheckOutcome {
    /// Constructs the outcome for a configuration that passes every
    /// invariant in [`tribal_config::validate`].
    pub(in crate::commands::check) fn config_validate_satisfied() -> Self {
        Self::Pass {
            detail: CheckDetail::AllInvariantsSatisfied,
        }
    }

    /// Constructs the outcome for one or more configuration invariant
    /// violations.  Each error string is matched against known
    /// prefixes; matches contribute a targeted hint to the remediation.
    pub(in crate::commands::check) fn config_validate_failed(errors: Vec<String>) -> Self {
        let hints: Vec<String> = errors.iter().filter_map(|e| hint_for_error(e)).collect();
        let remediation = if hints.is_empty() {
            None
        } else {
            Some(CheckRemediation::FixConfigInvariant { hints })
        };
        Self::Fail {
            detail: CheckDetail::ValidationFailed { errors },
            remediation,
        }
    }
}

/// Returns a targeted hint for `error` if it matches a known prefix.
fn hint_for_error(error: &str) -> Option<String> {
    API_KEY_HINT_PATHS
        .iter()
        .find(|(prefix, _)| error.starts_with(prefix))
        .map(|(_, path)| format!("set `{path}` or export `{}`", env_var_for_path(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate_satisfied_is_pass() {
        assert!(matches!(
            &CheckOutcome::config_validate_satisfied(),
            CheckOutcome::Pass {
                detail: CheckDetail::AllInvariantsSatisfied,
            },
        ));
    }

    #[test]
    fn test_config_validate_failed_with_api_key_error_has_hint() {
        let errors = vec![format!("{EMBEDDING_API_KEY_REQUIRED_PREFIX} openai")];
        let outcome = CheckOutcome::config_validate_failed(errors.clone());

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { errors: stored },
                remediation: Some(CheckRemediation::FixConfigInvariant { hints }),
            } if stored == &errors
                && hints.len() == 1
                && hints[0].contains("embedding.api_key")
                && hints[0].contains("TRIBAL_EMBEDDING__API_KEY"),
        ));
    }

    #[test]
    fn test_config_validate_failed_with_unknown_error_has_no_remediation() {
        let errors = vec!["database.url must not be empty".into()];
        let outcome = CheckOutcome::config_validate_failed(errors.clone());

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { errors: stored },
                remediation: None,
            } if stored == &errors,
        ));
    }

    #[test]
    fn test_config_validate_failed_mixes_known_and_unknown_errors() {
        let errors = vec![
            "database.url must not be empty".into(),
            format!("{TRIAGE_API_KEY_REQUIRED_PREFIX} openai"),
            "auth.token_ttl_hours must be greater than zero".into(),
        ];
        let outcome = CheckOutcome::config_validate_failed(errors);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                remediation: Some(CheckRemediation::FixConfigInvariant { hints }),
                ..
            } if hints.len() == 1 && hints[0].contains("inference.triage.api_key"),
        ));
    }
}
