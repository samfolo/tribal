//! Outcome constructors for the `config_validate` check.
//!
//! Targeted hints map known validation-error prefixes (the API-key
//! invariants exposed by `tribal_config::validation`) to a concrete
//! action.  Errors without a known hint render with no remediation.

use tribal_config::{
    ConfigError, EMBEDDING_API_KEY_REQUIRED_PREFIX, EXTRACTION_API_KEY_REQUIRED_PREFIX,
    RELATION_API_KEY_REQUIRED_PREFIX, TRIAGE_API_KEY_REQUIRED_PREFIX, env_var_for_path, validate,
};

use super::{
    skip_rules::SkipMask,
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};

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
    /// violations.  Each error string is matched against known prefixes
    /// for a targeted hint; unmatched errors are echoed verbatim so the
    /// remediation always carries one hint per error.
    pub(in crate::commands::check) fn config_validate_failed(errors: Vec<String>) -> Self {
        let hints: Vec<String> = errors
            .iter()
            .map(|e| hint_for_error(e).unwrap_or_else(|| e.clone()))
            .collect();
        Self::Fail {
            detail: CheckDetail::ValidationFailed { errors },
            remediation: CheckRemediation::FixConfigInvariant { hints },
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

/// Validates the parsed config currently on `state` and, on failure,
/// classifies the errors into a [`SkipMask`] stored back on state.
// `validate` is sync, but the step dispatcher requires every action
// to share the `async fn act` signature.
#[allow(clippy::unused_async)]
pub(in crate::commands::check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");
    match validate(config) {
        Ok(()) => CheckOutcome::config_validate_satisfied(),
        Err(ConfigError::ValidationFailed { errors }) => {
            state.skip_mask = SkipMask::from_validation_errors(&errors);
            CheckOutcome::config_validate_failed(errors)
        }
        Err(other) => CheckOutcome::config_validate_failed(vec![other.to_string()]),
    }
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
    fn test_config_validate_failed_with_api_key_error_has_targeted_hint() {
        let errors = vec![format!("{EMBEDDING_API_KEY_REQUIRED_PREFIX} openai")];
        let outcome = CheckOutcome::config_validate_failed(errors.clone());

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { errors: stored },
                remediation: CheckRemediation::FixConfigInvariant { hints },
            } if stored == &errors
                && hints.len() == 1
                && hints[0].contains("embedding.api_key")
                && hints[0].contains("TRIBAL_EMBEDDING__API_KEY"),
        ));
    }

    #[test]
    fn test_config_validate_failed_with_unknown_error_falls_back_to_verbatim_hint() {
        let errors = vec!["database.url must not be empty".into()];
        let outcome = CheckOutcome::config_validate_failed(errors.clone());

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { errors: stored },
                remediation: CheckRemediation::FixConfigInvariant { hints },
            } if stored == &errors
                && hints.len() == 1
                && hints[0] == "database.url must not be empty",
        ));
    }

    #[test]
    fn test_config_validate_failed_emits_one_hint_per_error() {
        let errors = vec![
            "database.url must not be empty".into(),
            format!("{TRIAGE_API_KEY_REQUIRED_PREFIX} openai"),
            "auth.token_ttl_hours must be greater than zero".into(),
        ];
        let outcome = CheckOutcome::config_validate_failed(errors);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                remediation: CheckRemediation::FixConfigInvariant { hints },
                ..
            } if hints.len() == 3
                && hints[0] == "database.url must not be empty"
                && hints[1].contains("inference.triage.api_key")
                && hints[2] == "auth.token_ttl_hours must be greater than zero",
        ));
    }
}
