//! Wire types for `tribal check` and the conversion from the internal
//! [`CheckOutcome`] data layer.
//!
//! [`CheckOutput`] is the frozen JSON shape consumed by downstream
//! tooling: every field is additive-only and discriminants stay
//! lowercase.  Internal callers (the orchestrator in `super::run`)
//! produce [`CheckOutcome`] values, then map them through the [`From`]
//! impl on this layer before serialisation.

use serde::{Deserialize, Serialize};

use super::checks::{CheckDetail, CheckName, CheckOutcome, CheckRemediation, CheckStatus};

// ---------------------------------------------------------------------------
// CheckResult
// ---------------------------------------------------------------------------

/// One row of the `checks` array in the wire format.
///
/// `detail` and `remediation` are rendered from the matching typed
/// variants on [`CheckOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    pub name: CheckName,
    pub status: CheckStatus,
    pub detail: String,
    pub remediation: Option<String>,
}

// ---------------------------------------------------------------------------
// CheckOutput
// ---------------------------------------------------------------------------

/// The wire format `tribal check --json` emits.
///
/// `ok` is `true` iff every entry in `checks` is `pass | warn | skip`.
/// Frozen schema: additive-only field changes after this commit lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckOutput {
    pub ok: bool,
    pub checks: Vec<CheckResult>,
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

impl From<&CheckOutcome> for CheckResult {
    fn from(outcome: &CheckOutcome) -> Self {
        Self {
            name: outcome.name,
            status: outcome.status,
            detail: outcome.detail.render(),
            remediation: outcome.remediation.as_ref().map(CheckRemediation::render),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_check_result_from_outcome_renders_pass_with_no_remediation() {
        let outcome = CheckOutcome {
            name: CheckName::ConfigValidate,
            status: CheckStatus::Pass,
            detail: CheckDetail::AllInvariantsSatisfied,
            remediation: None,
        };

        let result = CheckResult::from(&outcome);
        assert_eq!(result.name, CheckName::ConfigValidate);
        assert_eq!(result.status, CheckStatus::Pass);
        assert_eq!(result.detail, "all config invariants satisfied");
        assert_eq!(result.remediation, None);
    }

    #[test]
    fn test_check_result_from_outcome_renders_remediation_when_present() {
        let outcome = CheckOutcome {
            name: CheckName::ConfigParse,
            status: CheckStatus::Pass,
            detail: CheckDetail::ConfigLoaded {
                path: PathBuf::from("/etc/tribal/config.yaml"),
            },
            remediation: Some(CheckRemediation::InspectConfigFile {
                path: PathBuf::from("/etc/tribal/config.yaml"),
            }),
        };

        let result = CheckResult::from(&outcome);
        assert_eq!(result.detail, "config loaded from /etc/tribal/config.yaml");
        assert_eq!(
            result.remediation.as_deref(),
            Some("inspect the config file at /etc/tribal/config.yaml"),
        );
    }

    #[test]
    fn test_check_output_round_trips_through_json() {
        let output = CheckOutput {
            ok: false,
            checks: vec![
                CheckResult {
                    name: CheckName::ConfigParse,
                    status: CheckStatus::Pass,
                    detail: "config loaded from /a".into(),
                    remediation: None,
                },
                CheckResult {
                    name: CheckName::DatabaseReachable,
                    status: CheckStatus::Fail,
                    detail: "cannot connect".into(),
                    remediation: Some("run `pg_isready`".into()),
                },
            ],
        };

        let json = serde_json::to_string(&output).expect("serialise");
        let back: CheckOutput = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(output, back);
    }

    #[test]
    fn test_check_output_wire_shape_is_stable() {
        let output = CheckOutput {
            ok: true,
            checks: vec![CheckResult {
                name: CheckName::ConfigValidate,
                status: CheckStatus::Warn,
                detail: "no project resolved".into(),
                remediation: Some("set TRIBAL_PROJECT_ID".into()),
            }],
        };

        let json = serde_json::to_value(&output).expect("serialise");
        assert_eq!(json["ok"], serde_json::Value::Bool(true));
        let checks = json["checks"].as_array().expect("checks is array");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0]["name"], "config_validate");
        assert_eq!(checks[0]["status"], "warn");
        assert_eq!(checks[0]["detail"], "no project resolved");
        assert_eq!(checks[0]["remediation"], "set TRIBAL_PROJECT_ID");
    }

    #[test]
    fn test_check_name_serialises_as_snake_case() {
        for (name, wire) in [
            (CheckName::ConfigParse, "config_parse"),
            (CheckName::ConfigValidate, "config_validate"),
            (CheckName::DatabaseReachable, "database_reachable"),
            (CheckName::MigrationsCurrent, "migrations_current"),
            (CheckName::ProjectResolution, "project_resolution"),
            (CheckName::ValidTokenExists, "valid_token_exists"),
            (
                CheckName::AdvertisedUrlReachable,
                "advertised_url_reachable",
            ),
            (CheckName::BinaryUniqueness, "binary_uniqueness"),
            (CheckName::ProviderEmbedding, "provider_embedding"),
            (CheckName::ProviderExtraction, "provider_extraction"),
            (CheckName::ProviderTriage, "provider_triage"),
            (CheckName::ProviderRelation, "provider_relation"),
        ] {
            let json = serde_json::to_string(&name).expect("serialise");
            assert_eq!(json, format!("\"{wire}\""));
            let back: CheckName = serde_json::from_str(&json).expect("deserialise");
            assert_eq!(name, back);
        }
    }
}
