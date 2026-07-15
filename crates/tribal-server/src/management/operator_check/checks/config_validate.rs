//! Outcome constructors for the `config_validate` check.
//!
//! Each [`ValidationError`] variant maps to either a targeted hint or
//! its own [`Display`](std::fmt::Display) text (the catch-all echo).

use tribal_config::{ConfigError, Diagnostics, ValidationError, standard_env_var_name, validate};

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
    pub(in crate::management::operator_check) fn config_validate_satisfied() -> Self {
        Self::Pass {
            detail: CheckDetail::AllInvariantsSatisfied,
        }
    }

    /// Constructs the outcome for one or more configuration invariant
    /// violations.  Each diagnostic produces one hint — either a
    /// targeted hint from [`hint_for_error`] or the diagnostic's own
    /// rendered text — so the remediation always carries one entry
    /// per diagnostic.
    pub(in crate::management::operator_check) fn config_validate_failed(
        diagnostics: Diagnostics,
    ) -> Self {
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
        ValidationError::ProviderConnectionMissing { field, connection } => Some(format!(
            "define `provider_connections.{connection}` or point `{field}` at an existing connection"
        )),
        ValidationError::ProviderConnectionCredentialMissing {
            connection,
            provider,
        } => Some(provider_key_hint(connection.as_str(), *provider)),
        ValidationError::Empty { .. }
        | ValidationError::ContainsWhitespace { .. }
        | ValidationError::BelowMin { .. }
        | ValidationError::AboveMax { .. }
        | ValidationError::OutOfRange { .. }
        | ValidationError::FieldOrdering { .. }
        | ValidationError::DerivedFloor { .. }
        | ValidationError::ProviderConnectionUnsupported { .. }
        | ValidationError::PlatformProviderNotLocal { .. }
        | ValidationError::UrlMalformed { .. }
        | ValidationError::UrlUnsupportedForm { .. }
        | ValidationError::DuplicateProviderConnectionEndpoint { .. }
        | ValidationError::TelemetryFileExportRequiresEnabled
        | ValidationError::LogFilterMalformed { .. } => None,
    }
}

/// Names the connection field and every conventional environment override.
fn provider_key_hint(connection: &str, provider: tribal_domain::ProviderKind) -> String {
    let path = format!("provider_connections.{connection}.api_key");
    let figment = format!(
        "TRIBAL_PROVIDER_CONNECTIONS__{}__API_KEY",
        connection.to_uppercase()
    );
    match standard_env_var_name(provider) {
        Some(standard) => format!("set `{path}` or export `{figment}` / `{standard}`"),
        None => format!("set `{path}` or export `{figment}`"),
    }
}

/// Validates the parsed config currently on `state` and, on failure,
/// classifies the diagnostics into a [`SkipMask`] stored back on state.
#[expect(
    clippy::unused_async,
    reason = "the step dispatcher gives every check one async action signature"
)]
pub(in crate::management::operator_check) async fn act(state: &mut CheckState) -> CheckOutcome {
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
        Err(err) => {
            // validate() only emits ValidationFailed; the load-time variants
            // (parse, render, removed-shape) originate from load_config,
            // already run in config_parse. This branch reaches only via an
            // implementation bug, so log loudly so the operator can correlate
            // the empty-hint Fail with the underlying cause.
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
    use tribal_config::{ConfigPath, Diagnostics, ValidationError};
    use tribal_domain::{ProviderConnectionName, ProviderKind};

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
        let diagnostics =
            Diagnostics::from(vec![ValidationError::ProviderConnectionCredentialMissing {
                connection: ProviderConnectionName::parse("openai_primary").unwrap(),
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
                && hints[0].contains("provider_connections.openai_primary.api_key")
                && hints[0].contains("TRIBAL_PROVIDER_CONNECTIONS__OPENAI_PRIMARY__API_KEY")
                && hints[0].contains("OPENAI_API_KEY"),
        ));
    }

    #[test]
    fn test_config_validate_failed_with_missing_reference_has_targeted_hint() {
        let diagnostics = Diagnostics::from(vec![ValidationError::ProviderConnectionMissing {
            field: ConfigPath::from_static("inference.extraction.connection"),
            connection: ProviderConnectionName::parse("managed").unwrap(),
        }]);
        let outcome = CheckOutcome::config_validate_failed(diagnostics);

        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::ValidationFailed { diagnostics: stored },
                remediation: CheckRemediation::FixConfigInvariant { hints },
            } if stored.len() == 1
                && hints.len() == 1
                && hints[0].contains("inference.extraction.connection")
                && hints[0].contains("provider_connections.managed"),
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
            ValidationError::ProviderConnectionCredentialMissing {
                connection: ProviderConnectionName::parse("openai_primary").unwrap(),
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
                && hints[1].contains("provider_connections.openai_primary.api_key")
                && hints[2] == "auth.token_ttl_hours must be greater than zero",
        ));
    }
}
