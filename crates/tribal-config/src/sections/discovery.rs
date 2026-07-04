//! Discovery (semantic search) configuration.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of results returned by discovery queries.
pub const DEFAULT_LIMIT: u32 = 10;

/// Maximum number of results allowed in a discovery query.
pub const DEFAULT_MAX_LIMIT: u32 = 50;

/// Default overfetch multiplier for the initial semantic search.
pub const DEFAULT_OVERFETCH_MULTIPLIER: u32 = 3;

/// Maximum permitted overfetch multiplier.
pub const MAX_OVERFETCH_MULTIPLIER: u32 = 10;

/// Default minimum cosine similarity to include in results.
pub const DEFAULT_SIMILARITY_THRESHOLD: f64 = 0.3;

// ---------------------------------------------------------------------------
// DiscoveryConfig
// ---------------------------------------------------------------------------

/// Configuration for the discovery (semantic search) handler.
///
/// Controls result limits, overfetch behaviour, and similarity
/// thresholds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    /// Default number of results when the caller does not specify a limit.
    #[serde(default = "default_limit")]
    pub default_limit: u32,

    /// Maximum number of results a caller may request.
    #[serde(default = "default_max_limit")]
    pub max_limit: u32,

    /// Multiplier applied to the limit for the initial semantic search.
    ///
    /// `K = limit * overfetch_multiplier` rows are fetched, then filtered
    /// by similarity threshold.
    #[serde(default = "default_overfetch_multiplier")]
    pub overfetch_multiplier: u32,

    /// Minimum cosine similarity to include a result.
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_LIMIT,
            max_limit: DEFAULT_MAX_LIMIT,
            overfetch_multiplier: DEFAULT_OVERFETCH_MULTIPLIER,
            similarity_threshold: DEFAULT_SIMILARITY_THRESHOLD,
        }
    }
}

const fn default_limit() -> u32 {
    DEFAULT_LIMIT
}

const fn default_max_limit() -> u32 {
    DEFAULT_MAX_LIMIT
}

const fn default_overfetch_multiplier() -> u32 {
    DEFAULT_OVERFETCH_MULTIPLIER
}

const fn default_similarity_threshold() -> f64 {
    DEFAULT_SIMILARITY_THRESHOLD
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = DiscoveryConfig::default();
        assert_eq!(config.default_limit, DEFAULT_LIMIT);
        assert_eq!(config.max_limit, DEFAULT_MAX_LIMIT);
        assert_eq!(config.overfetch_multiplier, DEFAULT_OVERFETCH_MULTIPLIER);
        assert!((config.similarity_threshold - DEFAULT_SIMILARITY_THRESHOLD).abs() < f64::EPSILON);
    }
}
