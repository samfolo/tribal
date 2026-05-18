//! Shared state threaded through phase-3 check functions.

use sqlx::PgPool;
use tribal_config::TribalConfig;

/// Resources built once for the duration of a `tribal check` run.
///
/// Constructed after the database connection is verified; dropped when
/// the run exits.
pub(in crate::commands::check) struct CheckContext {
    pub pool: PgPool,
    /// Merged configuration — checks read transport, bind address,
    /// timeouts, and similar fields directly from here.
    pub config: TribalConfig,
    /// HTTP client shared across network probes.
    pub http_client: reqwest::Client,
    /// Project ID supplied via `--project`; takes precedence over the
    /// `TRIBAL_PROJECT_ID` env var and git-remote heuristic.
    pub project_override: Option<String>,
    /// Bearer token supplied via `--token`; takes precedence over the
    /// `TRIBAL_AUTH_TOKEN` env var and `credentials.json`.
    pub token_override: Option<String>,
}
