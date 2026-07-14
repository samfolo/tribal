//! Projection and writers for `tribal check`.

pub use tribal_wire::control::{CheckReport as CheckOutput, CheckResult};

use super::checks::{CheckOutcome, CheckOutcomes};

#[cfg(any(test, feature = "test-helpers"))]
mod human;
#[cfg(any(test, feature = "test-helpers"))]
mod json;

#[cfg(feature = "test-helpers")]
pub(in crate::commands::check) use human::write_human;
#[cfg(feature = "test-helpers")]
pub(in crate::commands::check) use json::write_json;

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

pub(in crate::commands::check) fn from_outcomes(outcomes: &CheckOutcomes) -> CheckOutput {
    CheckOutput {
        ok: outcomes
            .iter()
            .all(|o| !matches!(o, CheckOutcome::Fail { .. })),
        checks: outcomes.iter().map(result_from_outcome).collect(),
    }
}

fn result_from_outcome(outcome: &CheckOutcome) -> CheckResult {
    match outcome {
        CheckOutcome::Pass { detail } => CheckResult::Pass {
            name: detail.name(),
            detail: detail.render(),
        },
        CheckOutcome::Warn {
            detail,
            remediation,
        } => CheckResult::Warn {
            name: detail.name(),
            detail: detail.render(),
            remediation: remediation.render(),
        },
        CheckOutcome::Fail {
            detail,
            remediation,
        } => CheckResult::Fail {
            name: detail.name(),
            detail: detail.render(),
            remediation: remediation.render(),
        },
        CheckOutcome::Skip { detail } => CheckResult::Skip {
            name: detail.name(),
            detail: detail.render(),
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tribal_config::{ConfigPath, Diagnostics, ValidationError};
    use tribal_wire::control::CheckName;

    use super::*;

    #[test]
    fn test_check_result_from_pass_outcome_omits_remediation() {
        let outcome = CheckOutcome::config_parse_loaded(PathBuf::from("/etc/tribal/config.yaml"));
        let result = result_from_outcome(&outcome);
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

        let output = from_outcomes(&outcomes);
        assert!(output.ok);
        assert_eq!(output.checks.len(), 2);
    }

    #[test]
    fn test_check_output_from_outcomes_not_ok_when_any_fail() {
        let mut outcomes = CheckOutcomes::new();
        outcomes.push(CheckOutcome::config_parse_loaded(PathBuf::from(
            "/cfg.yaml",
        )));
        outcomes.push(CheckOutcome::config_validate_failed(Diagnostics::from(
            vec![ValidationError::Empty {
                field: ConfigPath::from_static("database.url"),
            }],
        )));

        let output = from_outcomes(&outcomes);
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
                    remediation: "run `pg_isready`".into(),
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
                remediation: "set TRIBAL_PROJECT_ID".into(),
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
                remediation: "inspect /etc/tribal/config.yaml for syntax errors".into(),
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
