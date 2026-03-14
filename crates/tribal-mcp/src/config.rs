//! Handler-specific configuration extracted from [`tribal_config`].
//!
//! Each nested struct carries only the fields the MCP handlers need,
//! keeping the handler layer decoupled from the full configuration shape.

use tribal_config::{
    DiscoveryConfig as FullDiscoveryConfig, ExplorationConfig as FullExplorationConfig,
    TribalConfig,
};

// ---------------------------------------------------------------------------
// HandlerConfig
// ---------------------------------------------------------------------------

/// Configuration values threaded into [`TribalServerHandler`](crate::TribalServerHandler).
#[derive(Default)]
pub struct HandlerConfig {
    /// Discovery (semantic search) limits.
    pub(crate) discovery: HandlerDiscoveryConfig,

    /// Exploration (graph traversal) limits.
    pub(crate) exploration: HandlerExplorationConfig,
}

impl From<&TribalConfig> for HandlerConfig {
    fn from(config: &TribalConfig) -> Self {
        Self {
            discovery: HandlerDiscoveryConfig::from(&config.discovery),
            exploration: HandlerExplorationConfig::from(&config.exploration),
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerDiscoveryConfig
// ---------------------------------------------------------------------------

/// Discovery configuration consumed by the `tribal_discover` handler.
pub(crate) struct HandlerDiscoveryConfig {
    /// Default number of results when the caller does not specify a limit.
    pub(crate) default_limit: u32,

    /// Maximum number of results a caller may request.
    pub(crate) max_limit: u32,
}

impl From<&FullDiscoveryConfig> for HandlerDiscoveryConfig {
    fn from(config: &FullDiscoveryConfig) -> Self {
        Self {
            default_limit: config.default_limit,
            max_limit: config.max_limit,
        }
    }
}

impl Default for HandlerDiscoveryConfig {
    fn default() -> Self {
        Self::from(&FullDiscoveryConfig::default())
    }
}

// ---------------------------------------------------------------------------
// HandlerExplorationConfig
// ---------------------------------------------------------------------------

/// Exploration configuration consumed by the `tribal_explore` handler.
pub(crate) struct HandlerExplorationConfig {
    /// Default traversal depth when not specified by the caller.
    pub(crate) default_depth: u32,

    /// Maximum traversal depth (hard cap).
    pub(crate) max_depth: u32,

    /// Default number of results when the caller does not specify a limit.
    pub(crate) default_limit: u32,

    /// Maximum number of results a caller may request.
    pub(crate) max_limit: u32,
}

impl From<&FullExplorationConfig> for HandlerExplorationConfig {
    fn from(config: &FullExplorationConfig) -> Self {
        Self {
            default_depth: config.default_depth,
            max_depth: config.max_depth,
            default_limit: config.default_limit,
            max_limit: config.max_limit,
        }
    }
}

impl Default for HandlerExplorationConfig {
    fn default() -> Self {
        Self::from(&FullExplorationConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_config::{DiscoveryConfig, ExplorationConfig};

    use super::*;

    #[test]
    fn test_handler_discovery_config_from_full() {
        let full = DiscoveryConfig {
            default_limit: 5,
            max_limit: 25,
            ..DiscoveryConfig::default()
        };
        let handler = HandlerDiscoveryConfig::from(&full);
        assert_eq!(handler.default_limit, 5);
        assert_eq!(handler.max_limit, 25);
    }

    #[test]
    fn test_handler_exploration_config_from_full() {
        let full = ExplorationConfig {
            default_depth: 2,
            max_depth: 5,
            default_limit: 10,
            max_limit: 50,
        };
        let handler = HandlerExplorationConfig::from(&full);
        assert_eq!(handler.default_depth, 2);
        assert_eq!(handler.max_depth, 5);
        assert_eq!(handler.default_limit, 10);
        assert_eq!(handler.max_limit, 50);
    }

    #[test]
    fn test_handler_config_from_tribal_config() {
        let config = TribalConfig::default();
        let handler = HandlerConfig::from(&config);
        assert_eq!(
            handler.discovery.default_limit,
            config.discovery.default_limit
        );
        assert_eq!(handler.discovery.max_limit, config.discovery.max_limit);
        assert_eq!(
            handler.exploration.default_depth,
            config.exploration.default_depth
        );
        assert_eq!(handler.exploration.max_depth, config.exploration.max_depth);
        assert_eq!(
            handler.exploration.default_limit,
            config.exploration.default_limit
        );
        assert_eq!(handler.exploration.max_limit, config.exploration.max_limit);
    }
}
