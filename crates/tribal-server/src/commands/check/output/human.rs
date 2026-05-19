//! Themed human-readable writer for `tribal check`.
//!
//! Each row is `StatusLine` (glyph + name) followed by an indented
//! [`Paragraph`] for the detail.  Rows that carry a remediation render
//! a second indented [`Paragraph`] prefixed with `fix: `.  Both
//! paragraphs wrap at the same width so long error strings stay
//! readable.  A summary footer closes the block: `Badge` glyph + label
//! mirroring the wire `ok` flag, then a tally of non-zero categories
//! in priority order (failed, warned, passed, skipped).

use std::io::{self, Write};

use tribal_ui::{
    Badge, Component, HStack, InlineComponent, Paragraph, RenderCtx, Status, StatusLine, Text,
    Theme,
};

use super::{CheckOutput, CheckResult};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Prefix that introduces the remediation paragraph under a Warn or
/// Fail row.
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
        if !self.output.checks.is_empty() {
            writeln!(ctx)?;
            writeln!(ctx)?;
            render_footer(ctx, self.output)?;
        }
        Ok(())
    }
}

fn render_footer(ctx: &mut RenderCtx, output: &CheckOutput) -> io::Result<()> {
    let counts = StatusCounts::from(&output.checks[..]);
    let badge_status = if output.ok {
        Status::Pass
    } else {
        Status::Fail
    };
    let items: Vec<Box<dyn InlineComponent>> = vec![
        Box::new(Badge::new(badge_status)),
        Box::new(Text::new(counts.tally())),
    ];
    HStack::new(items).with_separator('—').render(ctx)
}

#[derive(Debug, Default, Clone, Copy)]
struct StatusCounts {
    passed: usize,
    warned: usize,
    failed: usize,
    skipped: usize,
}

impl StatusCounts {
    fn from(rows: &[CheckResult]) -> Self {
        let mut counts = Self::default();
        for row in rows {
            match row {
                CheckResult::Pass { .. } => counts.passed += 1,
                CheckResult::Warn { .. } => counts.warned += 1,
                CheckResult::Fail { .. } => counts.failed += 1,
                CheckResult::Skip { .. } => counts.skipped += 1,
            }
        }
        counts
    }

    /// Comma-separated non-zero categories in priority order: failed,
    /// warned, passed, skipped.
    fn tally(self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.failed > 0 {
            parts.push(format!("{} failed", self.failed));
        }
        if self.warned > 0 {
            parts.push(format!("{} warned", self.warned));
        }
        if self.passed > 0 {
            parts.push(format!("{} passed", self.passed));
        }
        if self.skipped > 0 {
            parts.push(format!("{} skipped", self.skipped));
        }
        parts.join(", ")
    }
}

fn render_row(ctx: &mut RenderCtx, result: &CheckResult) -> io::Result<()> {
    let view = RowView::from(result);
    StatusLine::new(view.status, view.name).render(ctx)?;

    let dim = ctx.theme().typography.dim_label;
    ctx.indented(|ctx| -> io::Result<()> {
        writeln!(ctx)?;
        Paragraph::new(view.detail).with_style(dim).render(ctx)?;
        if let Some(remediation) = view.remediation {
            writeln!(ctx)?;
            let block = format!("{REMEDIATION_PREFIX}{remediation}");
            Paragraph::new(&block).with_style(dim).render(ctx)?;
        }
        Ok(())
    })
}

/// Flattened view over [`CheckResult`] so the renderer sees the same
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
                name: name.as_str(),
                detail,
                remediation: None,
            },
            CheckResult::Warn {
                name,
                detail,
                remediation,
            } => Self {
                status: Status::Warning,
                name: name.as_str(),
                detail,
                remediation: Some(remediation),
            },
            CheckResult::Fail {
                name,
                detail,
                remediation,
            } => Self {
                status: Status::Fail,
                name: name.as_str(),
                detail,
                remediation: Some(remediation),
            },
            CheckResult::Skip { name, detail } => Self {
                status: Status::Skipped,
                name: name.as_str(),
                detail,
                remediation: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{assert_text_snapshot, render_to_string};
    use tribal_ui::Theme;

    use super::*;
    use crate::commands::check::{checks::CheckName, output::CheckResult};

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
                    detail: "no project resolved from --project, TRIBAL_PROJECT_ID, or the git remote".into(),
                    remediation: "register a project with `tribal project register` or set \
                                  `TRIBAL_PROJECT_ID`"
                        .into(),
                },
                CheckResult::Fail {
                    name: CheckName::DatabaseReachable,
                    detail: "database unreachable: query failed (connecting to pool 'check'): \
                             error communicating with database: failed to lookup address \
                             information: nodename nor servname provided, or not known"
                        .into(),
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

    fn fixture_validate_multi_hint() -> CheckOutput {
        CheckOutput {
            ok: false,
            checks: vec![CheckResult::Fail {
                name: CheckName::ConfigValidate,
                detail: "server.bind_address cannot be set when server.transport is stdio\n\
                         embedding.api_key is required when embedding.provider is openai\n\
                         inference.triage.api_key is required when inference.triage.provider is \
                         openai"
                    .into(),
                remediation: "remove `server.bind_address` for stdio transport\n\
                              set `embedding.api_key` or export `TRIBAL_EMBEDDING__API_KEY`\n\
                              set `inference.triage.api_key` or export \
                              `TRIBAL_INFERENCE__TRIAGE__API_KEY`"
                    .into(),
            }],
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

    #[test]
    fn test_write_human_validate_multi_hint_matches_snapshot() {
        let captured = render(&fixture_validate_multi_hint());
        assert_text_snapshot!(
            &captured,
            "src/commands/check/snapshots/stderr-validate-multi-hint.txt"
        );
    }
}
