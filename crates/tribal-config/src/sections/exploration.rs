//! Exploration (graph traversal) configuration.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Hard cap on traversal depth.
pub const DEFAULT_MAX_DEPTH: u32 = 3;

/// Default traversal depth when not specified by the caller.
pub const DEFAULT_DEPTH: u32 = 1;

/// Default number of results per traversal level.
pub const DEFAULT_LIMIT: u32 = 20;

/// Maximum number of results a caller may request.
pub const DEFAULT_MAX_LIMIT: u32 = 100;

// ---------------------------------------------------------------------------
// ExplorationConfig
// ---------------------------------------------------------------------------

/// Configuration for the exploration (graph traversal) handler.
///
/// Controls depth and result limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExplorationConfig {
    /// Maximum traversal depth (hard cap).
    #[serde(default = "default_max_depth")]
    pub max_depth: u32,

    /// Default traversal depth when not specified by the caller.
    #[serde(default = "default_depth")]
    pub default_depth: u32,

    /// Default number of results when the caller does not specify a limit.
    #[serde(default = "default_limit")]
    pub default_limit: u32,

    /// Maximum number of results a caller may request.
    #[serde(default = "default_max_limit")]
    pub max_limit: u32,
}

impl Default for ExplorationConfig {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            default_depth: DEFAULT_DEPTH,
            default_limit: DEFAULT_LIMIT,
            max_limit: DEFAULT_MAX_LIMIT,
        }
    }
}

const fn default_max_depth() -> u32 {
    DEFAULT_MAX_DEPTH
}

const fn default_depth() -> u32 {
    DEFAULT_DEPTH
}

const fn default_limit() -> u32 {
    DEFAULT_LIMIT
}

const fn default_max_limit() -> u32 {
    DEFAULT_MAX_LIMIT
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = ExplorationConfig::default();
        assert_eq!(config.max_depth, DEFAULT_MAX_DEPTH);
        assert_eq!(config.default_depth, DEFAULT_DEPTH);
        assert_eq!(config.default_limit, DEFAULT_LIMIT);
        assert_eq!(config.max_limit, DEFAULT_MAX_LIMIT);
    }
}
