//! Pretty-printed JSON writer for `tribal check --json`.

use std::io::{self, Write};

use super::CheckOutput;

/// Writes `output` to `out` as a pretty-printed JSON document with a
/// trailing newline.  Calls `out.flush()` so the bytes survive
/// `BufWriter`-wrapped non-TTY redirection.
pub(in crate::management::operator_check) fn write_json(
    out: &mut dyn Write,
    output: &CheckOutput,
) -> io::Result<()> {
    let rendered =
        serde_json::to_string_pretty(output).expect("CheckOutput is always serialisable");
    writeln!(out, "{rendered}")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{assert_json_snapshot, render_to_string};

    use super::*;
    use crate::management::operator_check::{checks::CheckName, output::CheckResult};

    fn render(output: &CheckOutput) -> serde_json::Value {
        let captured = render_to_string(|w| write_json(w, output));
        serde_json::from_str(&captured).expect("output is valid JSON")
    }

    fn fixture_all_pass() -> CheckOutput {
        CheckOutput {
            ok: true,
            checks: vec![
                CheckResult::Pass {
                    name: CheckName::ConfigParse,
                    detail: "config loaded from /etc/tribal/config.yaml".into(),
                },
                CheckResult::Pass {
                    name: CheckName::ConfigValidate,
                    detail: "all configuration invariants satisfied".into(),
                },
                CheckResult::Pass {
                    name: CheckName::BinaryUniqueness,
                    detail: "`tribal` resolves to /usr/local/bin/tribal".into(),
                },
            ],
        }
    }

    fn fixture_mixed() -> CheckOutput {
        CheckOutput {
            ok: false,
            checks: vec![
                CheckResult::Pass {
                    name: CheckName::ConfigParse,
                    detail: "config loaded from /etc/tribal/config.yaml".into(),
                },
                CheckResult::Warn {
                    name: CheckName::ProjectResolution,
                    detail:
                        "no project resolved from --project, TRIBAL_PROJECT_ID, or the git remote"
                            .into(),
                    remediation: "register a project with `tribal project register` or set \
                                  `TRIBAL_PROJECT_ID`"
                        .into(),
                },
                CheckResult::Fail {
                    name: CheckName::DatabaseReachable,
                    detail: "database unreachable: connection refused".into(),
                    remediation: "run `pg_isready` against the configured database URL and \
                                  verify the host, port, and credentials"
                        .into(),
                },
                CheckResult::Skip {
                    name: CheckName::MigrationsCurrent,
                    detail: "skipped because the database is unreachable".into(),
                },
            ],
        }
    }

    #[test]
    fn test_write_json_all_pass_matches_snapshot() {
        let captured = render(&fixture_all_pass());
        assert_json_snapshot!(
            &captured,
            "src/management/operator_check/snapshots/json-all-pass.json"
        );
    }

    #[test]
    fn test_write_json_mixed_matches_snapshot() {
        let captured = render(&fixture_mixed());
        assert_json_snapshot!(
            &captured,
            "src/management/operator_check/snapshots/json-mixed.json"
        );
    }
}
