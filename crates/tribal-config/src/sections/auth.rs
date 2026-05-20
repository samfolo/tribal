//! Authentication configuration.

use serde::{Deserialize, Serialize};

use crate::validation::{ConfigPath, EnumerateFields};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default token lifetime in hours (~1 year).
pub const DEFAULT_TOKEN_TTL_HOURS: u64 = 8760;

const fn default_token_ttl_hours() -> u64 {
    DEFAULT_TOKEN_TTL_HOURS
}

// ---------------------------------------------------------------------------
// AuthConfig
// ---------------------------------------------------------------------------

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

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            token_ttl_hours: default_token_ttl_hours(),
        }
    }
}

// ---------------------------------------------------------------------------
// EnumerateFields
// ---------------------------------------------------------------------------

impl EnumerateFields for AuthConfig {
    fn enumerate(prefix: &str, out: &mut Vec<ConfigPath>) {
        out.push(ConfigPath::child(prefix, "token_ttl_hours"));
    }
}

#[cfg(test)]
#[allow(dead_code, clippy::let_underscore_untyped)]
fn _check_auth_config_fields(c: &AuthConfig) {
    let _ = &c.token_ttl_hours;
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
