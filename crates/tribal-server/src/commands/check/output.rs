//! Wire types for `tribal check` and the conversion from the internal
//! [`CheckOutcome`] data layer.
//!
//! [`CheckOutput`] is the JSON shape consumed by downstream tooling.
//! The status is the variant tag: `Pass` and `Skip` cannot carry a
//! `remediation` field, mirroring [`CheckOutcome`].

use serde::{Deserialize, Serialize};

use super::checks::{CheckName, CheckOutcome, CheckOutcomes, CheckRemediation};

// ---------------------------------------------------------------------------
// CheckResult
// ---------------------------------------------------------------------------

/// One row of the `checks` array in the wire format.
///
/// Serialised with `status` as an internal tag so each row deserialises
/// to `{"status": "<lowercase variant>", "name": ..., "detail": ...,
/// "remediation"?: ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum CheckResult {
    Pass {
        name: CheckName,
        detail: String,
    },
    Warn {
        name: CheckName,
        detail: String,
        remediation: Option<String>,
    },
    Fail {
        name: CheckName,
        detail: String,
        remediation: Option<String>,
    },
    Skip {
        name: CheckName,
        detail: String,
    },
}

// ---------------------------------------------------------------------------
// CheckOutput
// ---------------------------------------------------------------------------

/// The wire format `tribal check --json` emits.
///
/// `ok` is `true` iff no entry in `checks` is `fail`.
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
        match outcome {
            CheckOutcome::Pass { name, detail } => Self::Pass {
                name: *name,
                detail: detail.render(),
            },
            CheckOutcome::Warn {
                name,
                detail,
                remediation,
            } => Self::Warn {
                name: *name,
                detail: detail.render(),
                remediation: remediation.as_ref().map(CheckRemediation::render),
            },
            CheckOutcome::Fail {
                name,
                detail,
                remediation,
            } => Self::Fail {
                name: *name,
                detail: detail.render(),
                remediation: remediation.as_ref().map(CheckRemediation::render),
            },
            CheckOutcome::Skip { name, detail } => Self::Skip {
                name: *name,
                detail: detail.render(),
            },
        }
    }
}

impl From<&CheckOutcomes> for CheckOutput {
    fn from(outcomes: &CheckOutcomes) -> Self {
        Self {
            ok: outcomes
                .iter()
                .all(|o| !matches!(o, CheckOutcome::Fail { .. })),
            checks: outcomes.iter().map(CheckResult::from).collect(),
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
    fn test_check_result_from_pass_outcome_omits_remediation() {
        let outcome = CheckOutcome::config_parse_loaded(PathBuf::from("/etc/tribal/config.yaml"));
        let result = CheckResult::from(&outcome);
        assert!(matches!(
            &result,
            CheckResult::Pass {
                name: CheckName::ConfigParse,
                detail,
            } if detail == "config loaded from /etc/tribal/config.yaml",
        ));
    }

    #[test]
    fn test_check_output_from_outcomes_ok_when_no_fail() {
        let mut outcomes = CheckOutcomes::new();
        outcomes.push(CheckOutcome::config_parse_loaded(PathBuf::from(
            "/cfg.yaml",
        )));
        outcomes.push(CheckOutcome::config_validate_satisfied());

        let output = CheckOutput::from(&outcomes);
        assert!(output.ok);
        assert_eq!(output.checks.len(), 2);
    }

    #[test]
    fn test_check_output_from_outcomes_not_ok_when_any_fail() {
        let mut outcomes = CheckOutcomes::new();
        outcomes.push(CheckOutcome::config_parse_loaded(PathBuf::from(
            "/cfg.yaml",
        )));
        outcomes.push(CheckOutcome::config_validate_failed(vec![
            "database.url must not be empty".into(),
        ]));

        let output = CheckOutput::from(&outcomes);
        assert!(!output.ok);
        assert_eq!(output.checks.len(), 2);
    }

    #[test]
    fn test_check_output_round_trips_through_json() {
        let output = CheckOutput {
            ok: false,
            checks: vec![
                CheckResult::Pass {
                    name: CheckName::ConfigParse,
                    detail: "config loaded from /a".into(),
                },
                CheckResult::Fail {
                    name: CheckName::DatabaseReachable,
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
    fn test_pass_wire_shape_omits_remediation_field() {
        let output = CheckOutput {
            ok: true,
            checks: vec![CheckResult::Pass {
                name: CheckName::ConfigParse,
                detail: "config loaded from /etc/tribal/config.yaml".into(),
            }],
        };
        let json = serde_json::to_value(&output).expect("serialise");
        let pass = &json["checks"][0];
        assert_eq!(pass["status"], "pass");
        assert_eq!(pass["name"], "config_parse");
        assert_eq!(pass["detail"], "config loaded from /etc/tribal/config.yaml");
        assert!(pass.get("remediation").is_none());
    }

    #[test]
    fn test_skip_wire_shape_omits_remediation_field() {
        let output = CheckOutput {
            ok: true,
            checks: vec![CheckResult::Skip {
                name: CheckName::ValidTokenExists,
                detail: "stdio transport: no `--token` supplied; verification skipped".into(),
            }],
        };
        let json = serde_json::to_value(&output).expect("serialise");
        let skip = &json["checks"][0];
        assert_eq!(skip["status"], "skip");
        assert_eq!(skip["name"], "valid_token_exists");
        assert!(skip.get("remediation").is_none());
    }

    #[test]
    fn test_warn_wire_shape_carries_status_warn_and_keeps_ok_true() {
        let output = CheckOutput {
            ok: true,
            checks: vec![CheckResult::Warn {
                name: CheckName::ProjectResolution,
                detail: "no project resolved".into(),
                remediation: Some("set TRIBAL_PROJECT_ID".into()),
            }],
        };
        let json = serde_json::to_value(&output).expect("serialise");
        assert_eq!(json["ok"], serde_json::Value::Bool(true));
        let warn = &json["checks"][0];
        assert_eq!(warn["status"], "warn");
        assert_eq!(warn["name"], "project_resolution");
        assert_eq!(warn["detail"], "no project resolved");
        assert_eq!(warn["remediation"], "set TRIBAL_PROJECT_ID");
    }

    #[test]
    fn test_fail_wire_shape_includes_remediation_field() {
        let output = CheckOutput {
            ok: false,
            checks: vec![CheckResult::Fail {
                name: CheckName::ConfigParse,
                detail: "config at /etc/tribal/config.yaml failed to load: ...".into(),
                remediation: Some("inspect /etc/tribal/config.yaml for syntax errors".into()),
            }],
        };
        let json = serde_json::to_value(&output).expect("serialise");
        let fail = &json["checks"][0];
        assert_eq!(fail["status"], "fail");
        assert_eq!(fail["name"], "config_parse");
        assert!(fail["detail"].as_str().unwrap().starts_with("config at"));
        assert_eq!(
            fail["remediation"],
            "inspect /etc/tribal/config.yaml for syntax errors"
        );
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
