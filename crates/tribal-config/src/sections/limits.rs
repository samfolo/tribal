//! Per-provider concurrency and timeout limits.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::provider_kind::ProviderKind;
use crate::config_section;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LOCAL_MAX_IN_FLIGHT: u32 = 2;
const LOCAL_REQUEST_TIMEOUT_MS: u64 = 180_000;

const CLOUD_MAX_IN_FLIGHT: u32 = 8;
const CLOUD_REQUEST_TIMEOUT_MS: u64 = 120_000;

// ---------------------------------------------------------------------------
// ProviderLimitsConfig
// ---------------------------------------------------------------------------

/// Concurrency and timeout limits for a single provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLimitsConfig {
    /// Maximum concurrent in-flight requests.
    pub max_in_flight: u32,

    /// Per-request timeout in milliseconds.
    pub request_timeout_ms: u64,
}

// ---------------------------------------------------------------------------
// LimitsConfig
// ---------------------------------------------------------------------------

config_section! {
    /// Per-provider concurrency limits.
    ///
    /// Bounds in-flight requests to any single external service, preventing
    /// worker tasks from starving MCP read-path queries that share the same
    /// provider.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct LimitsConfig {
        /// Per-provider limits, keyed by provider kind.
        ///
        /// Pre-populated with defaults for all three providers.  Typed keys
        /// mean a YAML typo fails at parse time rather than silently creating
        /// an unreferenced entry.  Runtime-keyed: the validator constructs
        /// paths like `limits.providers.<provider>.<field>` at the call site.
        #[serde(default = "default_providers")]
        @skip pub providers: HashMap<ProviderKind, ProviderLimitsConfig>,
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            providers: default_providers(),
        }
    }
}

fn default_providers() -> HashMap<ProviderKind, ProviderLimitsConfig> {
    let mut map = HashMap::with_capacity(3);
    map.insert(
        ProviderKind::Ollama,
        ProviderLimitsConfig {
            max_in_flight: LOCAL_MAX_IN_FLIGHT,
            request_timeout_ms: LOCAL_REQUEST_TIMEOUT_MS,
        },
    );
    map.insert(
        ProviderKind::Anthropic,
        ProviderLimitsConfig {
            max_in_flight: CLOUD_MAX_IN_FLIGHT,
            request_timeout_ms: CLOUD_REQUEST_TIMEOUT_MS,
        },
    );
    map.insert(
        ProviderKind::OpenAi,
        ProviderLimitsConfig {
            max_in_flight: CLOUD_MAX_IN_FLIGHT,
            request_timeout_ms: CLOUD_REQUEST_TIMEOUT_MS,
        },
    );
    map
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_has_all_providers() {
        let config = LimitsConfig::default();
        assert_eq!(config.providers.len(), 3);
        assert!(config.providers.contains_key(&ProviderKind::Ollama));
        assert!(config.providers.contains_key(&ProviderKind::Anthropic));
        assert!(config.providers.contains_key(&ProviderKind::OpenAi));
    }

    #[test]
    fn test_local_defaults() {
        let config = LimitsConfig::default();
        let ollama = &config.providers[&ProviderKind::Ollama];
        assert_eq!(ollama.max_in_flight, LOCAL_MAX_IN_FLIGHT);
        assert_eq!(ollama.request_timeout_ms, LOCAL_REQUEST_TIMEOUT_MS);
    }

    #[test]
    fn test_cloud_defaults() {
        let config = LimitsConfig::default();

        let anthropic = &config.providers[&ProviderKind::Anthropic];
        assert_eq!(anthropic.max_in_flight, CLOUD_MAX_IN_FLIGHT);
        assert_eq!(anthropic.request_timeout_ms, CLOUD_REQUEST_TIMEOUT_MS);

        let openai = &config.providers[&ProviderKind::OpenAi];
        assert_eq!(openai.max_in_flight, CLOUD_MAX_IN_FLIGHT);
        assert_eq!(openai.request_timeout_ms, CLOUD_REQUEST_TIMEOUT_MS);
    }
}
