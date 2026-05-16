//! Outcome of a successful setup run.

use tribal_domain::{BearerToken, PrincipalId};

use super::config_file::ConfigFileOutcome;

/// Result of [`super::run::run`] when setup completes successfully.
///
/// Holds the freshly-minted bearer token, the principal it was issued
/// against, and the outcome of the config-file write. Callers dispatch
/// on `config_file` to decide whether user-supplied flags reached disk
/// or whether an existing file silently blocked them.
#[derive(Debug)]
pub(crate) struct SetupOutcome {
    /// The bearer token in plain text.
    pub bearer_token: BearerToken,
    /// Key of the principal the token was issued against.
    pub principal_key: String,
    /// Database id of the principal the token was issued against.
    pub principal_id: PrincipalId,
    /// What happened to the config file during this run.
    pub config_file: ConfigFileOutcome,
}
