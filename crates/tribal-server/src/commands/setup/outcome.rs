//! Outcome of a successful setup run.

use std::path::PathBuf;

use tribal_domain::{BearerToken, PrincipalId};

/// Result of [`super::run::run`] when setup completes successfully.
///
/// Holds the freshly-minted bearer token, the principal it was issued
/// against, and the resolved absolute config path. The standalone
/// `setup` wrapper discards this value after printing instructions;
/// `bootstrap` consumes it to thread the credentials onward into
/// project registration.
// Fields are read by `bootstrap::run_async` in Phase 5 of issue #152.
// Remove this allow when bootstrap lands.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SetupOutcome {
    /// The bearer token in plain text.
    pub bearer_token: BearerToken,
    /// Key of the principal the token was issued against.
    pub principal_key: String,
    /// Database id of the principal the token was issued against.
    pub principal_id: PrincipalId,
    /// Absolute, normalised path of the resolved config file.
    pub config_path: PathBuf,
}
