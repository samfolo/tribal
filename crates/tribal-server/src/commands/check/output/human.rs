//! Themed human-readable writer for `tribal check`.
//!
//! Each check row is a [`StatusLine`] (glyph + name + detail).  Rows
//! that carry remediation render the remediation on a follow-up indented
//! line so the user reads cause and fix together.

use std::io::{self, Write};

use tribal_ui::{Component, RenderCtx, Status, StatusLine, Text, Theme};

use super::{CheckOutput, CheckResult};
use crate::commands::check::checks::CheckName;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Prefix that introduces the remediation line under a Warn or Fail row.
const REMEDIATION_PREFIX: &str = "fix: ";

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Writes `output` to `out` as a themed human-readable block on stderr.
///
/// SGR escapes pass straight through; callers that need plain text wrap
/// `out` with `AutoStream::never` at their boundary.
pub(in crate::commands::check) fn write_human(
    out: &mut dyn Write,
    theme: &Theme,
    output: &CheckOutput,
) -> io::Result<()> {
    let view = CheckOutputView { output };
    let mut ctx = RenderCtx::new(out, theme);
    view.render(&mut ctx)?;
    ctx.flush()
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

struct CheckOutputView<'a> {
    output: &'a CheckOutput,
}

impl Component for CheckOutputView<'_> {
    fn render(&self, ctx: &mut RenderCtx) -> io::Result<()> {
        for (idx, result) in self.output.checks.iter().enumerate() {
            if idx > 0 {
                writeln!(ctx)?;
            }
            render_row(ctx, result)?;
        }
        Ok(())
    }
}

fn render_row(ctx: &mut RenderCtx, result: &CheckResult) -> io::Result<()> {
    let view = RowView::from(result);
    StatusLine::new(view.status, view.name)
        .with_detail(view.detail)
        .render(ctx)?;
    if let Some(remediation) = view.remediation {
        writeln!(ctx)?;
        let indent_prefix = ctx
            .theme()
            .indentation
            .prefix(ctx.indent().saturating_next());
        let style = ctx.theme().typography.dim_label;
        write!(ctx, "{indent_prefix}")?;
        Text::new(format!("{REMEDIATION_PREFIX}{remediation}"))
            .with_style(style)
            .render(ctx)?;
    }
    Ok(())
}

/// Flattened view over [`CheckResult`] so [`StatusLine`] sees the same
/// shape regardless of which wire variant produced the row.
struct RowView<'a> {
    status: Status,
    name: &'a str,
    detail: &'a str,
    remediation: Option<&'a str>,
}

impl<'a> From<&'a CheckResult> for RowView<'a> {
    fn from(result: &'a CheckResult) -> Self {
        match result {
            CheckResult::Pass { name, detail } => Self {
                status: Status::Pass,
                name: check_name_str(*name),
                detail,
                remediation: None,
            },
            CheckResult::Warn {
                name,
                detail,
                remediation,
            } => Self {
                status: Status::Warning,
                name: check_name_str(*name),
                detail,
                remediation: Some(remediation),
            },
            CheckResult::Fail {
                name,
                detail,
                remediation,
            } => Self {
                status: Status::Fail,
                name: check_name_str(*name),
                detail,
                remediation: Some(remediation),
            },
            CheckResult::Skip { name, detail } => Self {
                status: Status::Skipped,
                name: check_name_str(*name),
                detail,
                remediation: None,
            },
        }
    }
}

fn check_name_str(name: CheckName) -> &'static str {
    name.as_str()
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{assert_text_snapshot, render_to_string};
    use tribal_ui::Theme;

    use super::*;
    use crate::commands::check::output::CheckResult;

    fn render(output: &CheckOutput) -> String {
        let theme = Theme::default_dark();
        render_to_string(|w| write_human(w, &theme, output))
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
                    detail: "no project resolved from CLI flag, environment, or git remote".into(),
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
    fn test_write_human_all_pass_matches_snapshot() {
        let captured = render(&fixture_all_pass());
        assert_text_snapshot!(
            &captured,
            "src/commands/check/snapshots/stderr-all-pass.txt"
        );
    }

    #[test]
    fn test_write_human_mixed_matches_snapshot() {
        let captured = render(&fixture_mixed());
        assert_text_snapshot!(&captured, "src/commands/check/snapshots/stderr-mixed.txt");
    }
}
