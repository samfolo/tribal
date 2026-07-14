//! Entry point and step-pipeline driver for `tribal check`.

use std::{path::Path, time::Duration};

#[cfg(feature = "test-helpers")]
use std::io::Write;
use strum::IntoEnumIterator;
#[cfg(feature = "test-helpers")]
use tribal_ui::Theme;

#[cfg(feature = "test-helpers")]
use super::output::{write_human, write_json};
use super::{
    checks::{CheckOutcome, CheckOutcomes, CheckState, CheckStep, Preflight, SkipMask},
    output::{self, CheckOutput},
};
use crate::error::AppError;

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of inputs threaded into [`run_async`].  Re-exported at the
/// crate root under the `test-helpers` feature for integration-test
/// consumers.
#[cfg(feature = "test-helpers")]
pub struct CheckOptions<'a> {
    /// Absolute path to the resolved config file.
    pub config_path: &'a Path,
    /// Whether to emit the wire format on stdout.
    pub json: bool,
    /// Whether to run fatal provider probes.
    pub providers: bool,
    /// Project ID override.
    pub project: Option<&'a str>,
    /// Bearer token override.
    pub token: Option<&'a str>,
    /// Theme used for the human-readable writer.
    pub theme: &'a Theme,
}

#[cfg(feature = "test-helpers")]
impl<'a> CheckOptions<'a> {
    fn report_options(&self) -> CheckReportOptions<'a> {
        CheckReportOptions {
            config_path: self.config_path,
            source: CheckConfigSource::Path,
            providers: self.providers,
            project: self.project,
            token: self.token,
        }
    }
}

/// Inputs for a check report that is returned as data instead of written to a
/// CLI stream.
pub(crate) struct CheckReportOptions<'a> {
    /// Absolute path to the resolved config file.
    pub config_path: &'a Path,
    /// Configuration source the check pipeline must consume.
    pub source: CheckConfigSource,
    /// Whether to run fatal provider probes.
    pub providers: bool,
    /// Project ID override.
    pub project: Option<&'a str>,
    /// Bearer token override.
    pub token: Option<&'a str>,
}

/// Mutually exclusive configuration input for one check run.
pub(crate) enum CheckConfigSource {
    /// Load the named filesystem path.
    #[cfg(feature = "test-helpers")]
    Path,
    /// Consume an already-parsed in-process snapshot.
    Parsed(Box<tribal_config::TribalConfig>),
    /// Parse bytes whose filesystem identity and revision were proven.
    ProvenBytes(zeroize::Zeroizing<Vec<u8>>),
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Async core for [`run`].
///
/// Iterates [`CheckStep`] in declared order.  Each step's preflight
/// classifies its applicability against shared [`CheckState`]; the
/// orchestrator then either runs the action, emits a `Skip` row, or
/// omits the row entirely.  `out_stdout` carries the `--json` payload;
/// `out_stderr` carries the themed human-readable output.
///
/// Returns the assembled [`CheckOutput`] so callers can inspect the
/// overall `ok` flag and per-row statuses without re-parsing the wire
/// payload.  [`run`] uses this to translate `!output.ok` into the
/// silent [`AppError::CheckFailed`] exit signal.
///
/// # Errors
///
/// Returns an [`AppError`] if building the shared HTTP client or
/// writing output fails.
///
/// # Panics
///
/// Panics if JSON serialisation of [`CheckOutput`] fails.  All fields
/// derive `Serialize` from primitive types, so this is unreachable in
/// practice.
#[cfg(feature = "test-helpers")]
pub async fn run_async(
    opts: CheckOptions<'_>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<CheckOutput, AppError> {
    let output = run_report_async(opts.report_options()).await?;
    if opts.json {
        write_json(out_stdout, &output).map_err(|source| AppError::Io {
            context: "writing tribal check output to stdout".to_owned(),
            source,
        })?;
    } else {
        write_human(out_stderr, opts.theme, &output).map_err(|source| AppError::Io {
            context: "writing tribal check output to stderr".to_owned(),
            source,
        })?;
    }
    Ok(output)
}

/// Async core that returns the same report `tribal check --json` writes.
///
/// # Errors
///
/// Returns an [`AppError`] if the shared HTTP client cannot be built.
pub(crate) async fn run_report_async(
    opts: CheckReportOptions<'_>,
) -> Result<CheckOutput, AppError> {
    let mut state = build_state(opts)?;
    let mut outcomes = CheckOutcomes::new();

    for step in CheckStep::iter() {
        match step.preflight(&state) {
            Preflight::Run => outcomes.push(step.act(&mut state).await),
            Preflight::Skip(reason) => {
                outcomes.push(CheckOutcome::dependency_skipped(step.name(), reason));
            }
            Preflight::Omit => {}
        }
    }

    Ok(output::from_outcomes(&outcomes))
}

// ---------------------------------------------------------------------------
// State construction
// ---------------------------------------------------------------------------

/// 10s bounds blackholed providers without choking slow cloud probes;
/// per-request timeouts (e.g. advertised-url's 2s) override.
const PROBE_CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

fn build_state(opts: CheckReportOptions<'_>) -> Result<CheckState, AppError> {
    // advertised_url's "something is bound" semantics treat any HTTP
    // response — including 3xx — as proof.  Following redirects would
    // turn a redirect to a broken target into a false unreachable.
    let http_client = reqwest::Client::builder()
        .timeout(PROBE_CLIENT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|source| AppError::HttpClient {
            context: "tribal check probe client".into(),
            source,
        })?;
    let (config, config_bytes) = match opts.source {
        #[cfg(feature = "test-helpers")]
        CheckConfigSource::Path => (None, None),
        CheckConfigSource::Parsed(config) => (Some(*config), None),
        CheckConfigSource::ProvenBytes(bytes) => (None, Some(bytes)),
    };
    Ok(CheckState {
        config_path: opts.config_path.to_path_buf(),
        providers: opts.providers,
        project_override: opts.project.map(str::to_owned),
        token_override: opts.token.and_then(canonical_token),
        path_var: std::env::var("PATH").unwrap_or_default(),
        http_client,
        config,
        config_bytes,
        skip_mask: SkipMask::default(),
        pool: None,
        gateway: None,
    })
}

/// Trims and discards whitespace-only inputs so downstream callers see
/// the canonical token form.
fn canonical_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use tribal_wire::control::{CheckName, CheckResult};

    use super::*;

    #[tokio::test]
    async fn test_proven_bytes_win_over_the_live_path() {
        let temp = tempfile::tempdir().expect("temporary check root");
        let path = temp.path().join("tribal.yaml");
        let config = tribal_config::TribalConfig::minimum_valid(
            "postgres://user:pass@localhost:5432/tribal",
        );
        std::fs::write(
            &path,
            serde_yaml::to_string(&config).expect("config serialises"),
        )
        .expect("config writes");

        let report = run_report_async(CheckReportOptions {
            config_path: &path,
            source: CheckConfigSource::ProvenBytes(zeroize::Zeroizing::new(
                b"database: [".to_vec(),
            )),
            providers: false,
            project: None,
            token: None,
        })
        .await
        .expect("check report is produced");

        assert!(matches!(
            report.checks.first(),
            Some(CheckResult::Fail {
                name: CheckName::ConfigParse,
                ..
            })
        ));
    }
}
