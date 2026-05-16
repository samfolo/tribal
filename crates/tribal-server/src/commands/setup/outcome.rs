//! Outcome of a successful setup run.

use tribal_domain::{BearerToken, PrincipalId};

/// Result of [`super::run::run`] when setup completes successfully.
///
/// Holds the freshly-minted bearer token and the principal it was
/// issued against. The standalone `setup` wrapper discards this value
/// after printing instructions; `bootstrap` consumes it to thread the
/// credentials onward into project registration.
#[derive(Debug)]
pub(crate) struct SetupOutcome {
    /// The bearer token in plain text.
    pub bearer_token: BearerToken,
    /// Key of the principal the token was issued against.
    pub principal_key: String,
    /// Database id of the principal the token was issued against.
    pub principal_id: PrincipalId,
}
