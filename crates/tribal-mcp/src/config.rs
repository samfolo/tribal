//! Handler-specific configuration for the MCP layer.
//!
//! Each nested struct carries only the fields the MCP handlers need.
//! Conversions from the full configuration shape live in the consumer
//! crate (`tribal-server`), keeping this crate free of transitive
//! dependencies on `tribal-config`.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HandlerConfig {
    /// Discovery (semantic search) limits.
    pub discovery: HandlerDiscoveryConfig,

    /// Exploration (graph traversal) limits.
    pub exploration: HandlerExplorationConfig,
}

// ---------------------------------------------------------------------------
// HandlerDiscoveryConfig
// ---------------------------------------------------------------------------

/// Discovery configuration consumed by the `tribal_discover` handler.
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
}
