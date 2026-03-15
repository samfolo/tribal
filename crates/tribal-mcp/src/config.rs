//! Handler-specific configuration for the MCP layer.
//!
//! Each nested struct carries only the fields the MCP handlers need.
//! The [`From<&TribalConfig>`] impl projects the full configuration
//! into this handler-specific subset.

use tribal_config::TribalConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_POOL_NAME: &str = "<anonymous>";
const DEFAULT_DISCOVERY_LIMIT: u32 = 10;
const MAX_DISCOVERY_LIMIT: u32 = 50;
const DEFAULT_EXPLORATION_DEPTH: u32 = 1;
const MAX_EXPLORATION_DEPTH: u32 = 3;
const DEFAULT_EXPLORATION_LIMIT: u32 = 20;
const MAX_EXPLORATION_LIMIT: u32 = 100;

// ---------------------------------------------------------------------------
// HandlerConfig
// ---------------------------------------------------------------------------

/// Configuration values threaded into [`TribalServerHandler`](crate::TribalServerHandler).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerConfig {
    /// Pool name reported in pool-exhaustion errors, set by the server on
    /// startup.
    pub pool_name: &'static str,

    /// Discovery (semantic search) limits.
    pub discovery: HandlerDiscoveryConfig,

    /// Exploration (graph traversal) limits.
    pub exploration: HandlerExplorationConfig,
}

impl HandlerConfig {
    /// Sets the pool name on an existing configuration.
    #[must_use]
    pub fn with_pool_name(mut self, pool_name: &'static str) -> Self {
        self.pool_name = pool_name;
        self
    }
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            pool_name: DEFAULT_POOL_NAME,
            discovery: HandlerDiscoveryConfig::default(),
            exploration: HandlerExplorationConfig::default(),
        }
    }
}

impl From<&TribalConfig> for HandlerConfig {
    fn from(config: &TribalConfig) -> Self {
        Self {
            pool_name: DEFAULT_POOL_NAME,
            discovery: HandlerDiscoveryConfig {
                default_limit: config.discovery.default_limit,
                max_limit: config.discovery.max_limit,
            },
            exploration: HandlerExplorationConfig {
                default_depth: config.exploration.default_depth,
                max_depth: config.exploration.max_depth,
                default_limit: config.exploration.default_limit,
                max_limit: config.exploration.max_limit,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerDiscoveryConfig
// ---------------------------------------------------------------------------

/// Discovery configuration consumed by the `tribal_discover` handler.
///
/// `overfetch_multiplier` and `similarity_threshold` from
/// `DiscoveryConfig` are intentionally omitted — the handler does not
/// yet consume them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerDiscoveryConfig {
    /// Default number of results when the caller does not specify a limit.
    pub default_limit: u32,

    /// Maximum number of results a caller may request.
    pub max_limit: u32,
}

impl Default for HandlerDiscoveryConfig {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_DISCOVERY_LIMIT,
            max_limit: MAX_DISCOVERY_LIMIT,
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerExplorationConfig
// ---------------------------------------------------------------------------

/// Exploration configuration consumed by the `tribal_explore` handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerExplorationConfig {
    /// Default traversal depth when not specified by the caller.
    pub default_depth: u32,

    /// Maximum traversal depth (hard cap).
    pub max_depth: u32,

    /// Default number of results when the caller does not specify a limit.
    pub default_limit: u32,

    /// Maximum number of results a caller may request.
    pub max_limit: u32,
}

impl Default for HandlerExplorationConfig {
    fn default() -> Self {
        Self {
            default_depth: DEFAULT_EXPLORATION_DEPTH,
            max_depth: MAX_EXPLORATION_DEPTH,
            default_limit: DEFAULT_EXPLORATION_LIMIT,
            max_limit: MAX_EXPLORATION_LIMIT,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_discovery_config_defaults() {
        let config = HandlerDiscoveryConfig::default();
        assert_eq!(config.default_limit, DEFAULT_DISCOVERY_LIMIT);
        assert_eq!(config.max_limit, MAX_DISCOVERY_LIMIT);
    }

    #[test]
    fn test_handler_exploration_config_defaults() {
        let config = HandlerExplorationConfig::default();
        assert_eq!(config.default_depth, DEFAULT_EXPLORATION_DEPTH);
        assert_eq!(config.max_depth, MAX_EXPLORATION_DEPTH);
        assert_eq!(config.default_limit, DEFAULT_EXPLORATION_LIMIT);
        assert_eq!(config.max_limit, MAX_EXPLORATION_LIMIT);
    }

    #[test]
    fn test_handler_config_default_delegates_to_nested() {
        let config = HandlerConfig::default();
        assert_eq!(config.discovery, HandlerDiscoveryConfig::default());
        assert_eq!(config.exploration, HandlerExplorationConfig::default());
    }

    #[test]
    fn test_from_tribal_config() {
        let tribal = TribalConfig::default();
        let handler = HandlerConfig::from(&tribal);
        assert_eq!(
            handler.discovery.default_limit,
            tribal.discovery.default_limit
        );
        assert_eq!(handler.discovery.max_limit, tribal.discovery.max_limit);
        assert_eq!(
            handler.exploration.default_depth,
            tribal.exploration.default_depth
        );
        assert_eq!(handler.exploration.max_depth, tribal.exploration.max_depth);
        assert_eq!(
            handler.exploration.default_limit,
            tribal.exploration.default_limit
        );
        assert_eq!(handler.exploration.max_limit, tribal.exploration.max_limit);
    }

    /// Catches divergence if someone changes a default in one crate
    /// without updating the other.
    #[test]
    fn test_handler_defaults_match_tribal_config_defaults() {
        let handler = HandlerConfig::default();
        let from_config = HandlerConfig::from(&TribalConfig::default());
        assert_eq!(handler, from_config);
    }
}
