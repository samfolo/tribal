//! Shared state threaded through every check step.
//!
//! The orchestrator constructs one [`CheckState`] per run and mutates
//! it in place as steps complete: a successful `config_parse` populates
//! [`CheckState::config`]; a successful `database_reachable` populates
//! [`CheckState::pool`]; `config_validate` may populate
//! [`CheckState::skip_mask`].  Subsequent steps read this state in
//! their preflights and actions.

use std::path::PathBuf;

use sqlx::PgPool;
use tribal_config::TribalConfig;

use super::skip_rules::SkipMask;

/// State container threaded through the check pipeline.
pub(in crate::commands::check) struct CheckState {
    /// Absolute path of the configuration file under test.
    pub config_path: PathBuf,
    /// Whether the `--providers` gate is on; controls whether the
    /// four provider steps are considered at all.
    pub providers: bool,
    /// Project ID supplied via `--project` (takes precedence over the
    /// `TRIBAL_PROJECT_ID` env var and git-remote heuristic).
    pub project_override: Option<String>,
    /// Bearer token supplied via `--token` (takes precedence over the
    /// `TRIBAL_AUTH_TOKEN` env var and `credentials.json`).
    pub token_override: Option<String>,
    /// PATH variable read once at construction time, consulted by
    /// `binary_uniqueness`.
    pub path_var: String,
    /// HTTP client built once per run and shared by every network
    /// probe.
    pub http_client: reqwest::Client,

    /// Populated by `config_parse` on success.
    pub config: Option<TribalConfig>,
    /// Populated by `config_validate` when validation fails; the
    /// default empty mask is correct for both "validation passed" and
    /// "validation never ran".
    pub skip_mask: SkipMask,
    /// Populated by `database_reachable` on success.
    pub pool: Option<PgPool>,
    /// Built lazily by the first provider probe from the parsed config
    /// and whatever pool the database step produced.
    pub gateway: Option<std::sync::Arc<tribal_inference::InferenceGateway>>,
}
