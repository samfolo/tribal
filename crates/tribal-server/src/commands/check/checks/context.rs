//! Shared state threaded through phase-3 check functions.

use sqlx::PgPool;
use tribal_config::TransportKind;

/// Resources built once for the duration of a `tribal check` run.
///
/// Constructed after the database connection is verified; dropped when
/// the run exits.
pub(in crate::commands::check) struct CheckContext {
    pub pool: PgPool,
    /// Resolved server transport — picks the auth-resolution path.
    pub transport: TransportKind,
    /// Project ID supplied via `--project`; takes precedence over the
    /// `TRIBAL_PROJECT_ID` env var and git-remote heuristic.
    pub project_override: Option<String>,
    /// Bearer token supplied via `--token`; takes precedence over the
    /// `TRIBAL_AUTH_TOKEN` env var and `credentials.json`.
    pub token_override: Option<String>,
}
