//! Authentication configuration.

use serde::{Deserialize, Serialize};

use crate::config_section;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default token lifetime in hours (~1 year).
pub const DEFAULT_TOKEN_TTL_HOURS: u64 = 8760;

/// Maximum `auth.token_ttl_hours` accepted by `validate()`.
///
/// Set to the boundary where `chrono::TimeDelta::try_hours` starts
/// returning `None` (`i64::MAX / 3_600_000`, where 3.6e6 is
/// milliseconds-per-hour).  Values above this cap cannot represent a
/// valid `TimeDelta` regardless of subsequent arithmetic.
#[allow(clippy::cast_sign_loss)]
pub const MAX_TTL_HOURS: u64 = (i64::MAX / 3_600_000) as u64;

const fn default_token_ttl_hours() -> u64 {
    DEFAULT_TOKEN_TTL_HOURS
}

// ---------------------------------------------------------------------------
// AuthConfig
// ---------------------------------------------------------------------------

config_section! {
    /// Authentication settings.
    ///
    /// Controls token lifetime defaults.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct AuthConfig {
        /// Default token lifetime in hours.
        ///
        /// Defaults to 8760 (~1 year).
        #[serde(default = "default_token_ttl_hours")]
        pub token_ttl_hours: u64,
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            token_ttl_hours: default_token_ttl_hours(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = AuthConfig::default();
        assert_eq!(config.token_ttl_hours, DEFAULT_TOKEN_TTL_HOURS);
    }
}
