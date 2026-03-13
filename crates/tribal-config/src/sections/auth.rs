//! Authentication configuration.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const fn default_token_ttl_hours() -> u64 {
    8760
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = AuthConfig::default();
        assert_eq!(config.token_ttl_hours, 8760);
    }
}
