//! Outcome constructors for the `config_validate` check.
//!
//! Each [`ValidationError`] variant maps to either a targeted hint or
//! its own [`Display`](std::fmt::Display) text (the catch-all echo).

use tribal_config::{
    ConfigError, Diagnostics, ProviderStage, ValidationError, standard_env_var_name, validate,
};
use tribal_domain::ProviderKind;

use super::{
    skip_rules::SkipMask,
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};

const STDIO_CONFLICT_HINT: &str = "remove `server.bind_address` for stdio transport";
const MALFORMED_ADDRESS_HINT: &str = "set `server.bind_address` to a valid `<host>:<port>`";

impl CheckOutcome {
    /// Constructs the outcome for a configuration that passes every
    /// invariant in [`tribal_config::validate`].
    pub(in crate::commands::check) fn config_validate_satisfied() -> Self {
        Self::Pass {
            detail: CheckDetail::AllInvariantsSatisfied,
        }
    }

    /// Constructs the outcome for one or more configuration invariant
    /// violations.  Each diagnostic produces one hint — either a
    /// targeted hint from [`hint_for_error`] or the diagnostic's own
    /// rendered text — so the remediation always carries one entry
    /// per diagnostic.
    pub(in crate::commands::check) fn config_validate_failed(diagnostics: Diagnostics) -> Self {
        let hints: Vec<String> = diagnostics
            .iter()
            .map(|d| hint_for_error(d).unwrap_or_else(|| d.to_string()))
            .collect();
        Self::Fail {
            detail: CheckDetail::ValidationFailed { diagnostics },
            remediation: CheckRemediation::FixConfigInvariant { hints },
        }
    }
}

/// Returns a targeted hint for `error` if its variant has one.  Other
/// variants render via [`Display`](std::fmt::Display) at the caller.
fn hint_for_error(error: &ValidationError) -> Option<String> {
    match error {
        ValidationError::BindAddressStdioConflict => Some(STDIO_CONFLICT_HINT.into()),
        ValidationError::BindAddressMalformed { .. } => Some(MALFORMED_ADDRESS_HINT.into()),
        ValidationError::MissingApiKey { stage, provider } => Some(api_key_hint(*stage, *provider)),
        ValidationError::Empty { .. }
        | ValidationError::ContainsWhitespace { .. }
        | ValidationError::BelowMin { .. }
        | ValidationError::AboveMax { .. }
        | ValidationError::OutOfRange { .. }
        | ValidationError::FieldOrdering { .. }
        | ValidationError::DerivedFloor { .. }
        | ValidationError::EmbeddingProviderUnsupported { .. }
        | ValidationError::UrlMalformed { .. }
        | ValidationError::UrlUnsupportedForm { .. }
        | ValidationError::NonLoopbackDcrConflict
        | ValidationError::TelemetryFileExportRequiresEnabled => None,
    }
}

/// Renders the hint for a [`ValidationError::MissingApiKey`], naming
/// the field path and every env var that satisfies it.
fn api_key_hint(stage: ProviderStage, provider: ProviderKind) -> String {
    let path = stage.api_key_path();
    let figment = path.env_var();
    match standard_env_var_name(provider) {
        Some(standard) => format!("set `{path}` or export `{figment}` / `{standard}`"),
        None => format!("set `{path}` or export `{figment}`"),
    }
}

/// Validates the parsed config currently on `state` and, on failure,
/// classifies the diagnostics into a [`SkipMask`] stored back on state.
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
        Err(ConfigError::ValidationFailed { diagnostics }) => {
            state.skip_mask = SkipMask::from_validation_errors(diagnostics.as_slice());
            CheckOutcome::config_validate_failed(diagnostics)
        }
        Err(err @ (ConfigError::Load { .. } | ConfigError::Render { .. })) => {
            // validate() only emits ValidationFailed; Load and Render
            // originate from load_config (already run in config_parse).
            // This branch reaches only via implementation bug — log
            // loudly so the operator can correlate the empty-hint Fail
            // with the underlying cause.
            tracing::error!(
                error = %err,
                "config_validate: unexpected ConfigError variant from validate()",
            );
            CheckOutcome::config_validate_failed(Diagnostics::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use tribal_config::{ConfigPath, Diagnostics, ProviderStage, ValidationError};

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
        let diagnostics = Diagnostics::from(vec![ValidationError::MissingApiKey {
            stage: ProviderStage::Embedding,
            provider: ProviderKind::OpenAi,
        }]);
        let outcome = CheckOutcome::config_validate_failed(diagnostics);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { diagnostics: stored },
                remediation: CheckRemediation::FixConfigInvariant { hints },
            } if stored.len() == 1
                && hints.len() == 1
                && hints[0].contains("embedding.api_key")
                && hints[0].contains("TRIBAL_EMBEDDING__API_KEY")
                && hints[0].contains("OPENAI_API_KEY"),
        ));
    }

    #[test]
    fn test_config_validate_failed_stdio_conflict_yields_remove_bind_address_hint() {
        let outcome = CheckOutcome::config_validate_failed(Diagnostics::from(vec![
            ValidationError::BindAddressStdioConflict,
        ]));

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                remediation: CheckRemediation::FixConfigInvariant { hints },
                ..
            } if hints.as_slice() == [STDIO_CONFLICT_HINT.to_owned()],
        ));
    }

    #[test]
    fn test_config_validate_failed_malformed_address_yields_set_valid_address_hint() {
        let outcome = CheckOutcome::config_validate_failed(Diagnostics::from(vec![
            ValidationError::BindAddressMalformed {
                value: "not-an-address".into(),
            },
        ]));

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                remediation: CheckRemediation::FixConfigInvariant { hints },
                ..
            } if hints.as_slice() == [MALFORMED_ADDRESS_HINT.to_owned()],
        ));
    }

    #[test]
    fn test_config_validate_failed_with_unknown_error_falls_back_to_verbatim_hint() {
        let diagnostics = Diagnostics::from(vec![ValidationError::Empty {
            field: ConfigPath::from_static("database.url"),
        }]);
        let outcome = CheckOutcome::config_validate_failed(diagnostics);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { diagnostics: stored },
                remediation: CheckRemediation::FixConfigInvariant { hints },
            } if stored.len() == 1
                && hints.len() == 1
                && hints[0] == "database.url must not be empty",
        ));
    }

    #[test]
    fn test_config_validate_failed_emits_one_hint_per_diagnostic() {
        let diagnostics = Diagnostics::from(vec![
            ValidationError::Empty {
                field: ConfigPath::from_static("database.url"),
            },
            ValidationError::MissingApiKey {
                stage: ProviderStage::Triage,
                provider: ProviderKind::OpenAi,
            },
            ValidationError::BelowMin {
                field: ConfigPath::from_static("auth.token_ttl_hours"),
                value: 0,
                min: 1,
            },
        ]);
        let outcome = CheckOutcome::config_validate_failed(diagnostics);

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
