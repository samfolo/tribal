//! Outcome of a successful setup run.

use tribal_domain::{BearerToken, PrincipalId};

use super::config_file::ConfigFileOutcome;
use crate::commands::common::CredentialsPersistOutcome;

/// Result of [`super::run::run`] when setup completes successfully.
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
    /// What happened to the credentials.json write that followed the
    /// token-row insert.
    pub credentials: CredentialsPersistOutcome,
}
