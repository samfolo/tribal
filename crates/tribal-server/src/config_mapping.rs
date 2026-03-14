//! Conversions from full configuration types to consumer-specific projections.
//!
//! These live here (not in the consumer crates) because each consumer
//! crate must remain independent of `tribal-config` to avoid transitive
//! coupling through `tribal-config`'s interim dependencies.

use tribal_config::TribalConfig;
use tribal_mcp::{HandlerConfig, HandlerDiscoveryConfig, HandlerExplorationConfig};

/// Builds a [`HandlerConfig`] from the full configuration.
///
/// Extracts only the fields relevant to the MCP handler layer.
#[must_use]
pub fn handler_config_from(config: &TribalConfig) -> HandlerConfig {
    HandlerConfig {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_config_from_tribal_config() {
        let config = TribalConfig::default();
        let handler = handler_config_from(&config);
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

    /// Catches divergence if someone changes a default in one crate
    /// without updating the other.
    #[test]
    fn test_handler_defaults_match_tribal_config_defaults() {
        let handler = HandlerConfig::default();
        let config = TribalConfig::default();

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
