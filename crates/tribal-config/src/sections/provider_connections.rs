//! Reusable provider connections and their connection-backed consumers.

use std::collections::{BTreeMap, btree_map::Entry};

use serde::{Deserialize, Serialize};
use tribal_domain::{ApiKey, ProviderConnectionName, ProviderKind, normalise_endpoint_url};

/// One reusable provider endpoint and credential.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "provider", rename_all = "lowercase", deny_unknown_fields)]
pub enum ProviderConnectionConfig {
    /// A local or remotely hosted Ollama endpoint.
    Ollama {
        /// Provider base URL.
        base_url: String,
    },
    /// An Anthropic endpoint.
    Anthropic {
        /// Provider base URL.
        base_url: String,
        /// Provider credential.
        #[serde(default)]
        api_key: Option<ApiKey>,
    },
    /// An OpenAI-compatible endpoint.
    OpenAi {
        /// Provider base URL.
        base_url: String,
        /// Provider credential.
        #[serde(default)]
        api_key: Option<ApiKey>,
    },
    /// Tribal's managed provider connection.
    Platform {},
}

impl ProviderConnectionConfig {
    /// Returns the provider kind represented by this connection.
    #[must_use]
    pub const fn provider(&self) -> ProviderKind {
        match self {
            Self::Ollama { .. } => ProviderKind::Ollama,
            Self::Anthropic { .. } => ProviderKind::Anthropic,
            Self::OpenAi { .. } => ProviderKind::OpenAi,
            Self::Platform {} => ProviderKind::Platform,
        }
    }

    /// Returns the configured endpoint, if the provider uses one.
    #[must_use]
    pub fn base_url(&self) -> Option<&str> {
        match self {
            Self::Ollama { base_url }
            | Self::Anthropic { base_url, .. }
            | Self::OpenAi { base_url, .. } => Some(base_url),
            Self::Platform {} => None,
        }
    }

    /// Returns the configured API key, if the provider accepts one.
    #[must_use]
    pub fn api_key(&self) -> Option<&ApiKey> {
        match self {
            Self::Anthropic { api_key, .. } | Self::OpenAi { api_key, .. } => api_key.as_ref(),
            Self::Ollama { .. } | Self::Platform {} => None,
        }
    }
}

/// Provider connections keyed by their stable names.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct ProviderConnections(BTreeMap<ProviderConnectionName, ProviderConnectionConfig>);

impl ProviderConnections {
    /// Returns a named connection.
    #[must_use]
    pub fn get(&self, name: &ProviderConnectionName) -> Option<&ProviderConnectionConfig> {
        self.0.get(name)
    }

    /// Inserts or replaces a named connection.
    pub fn insert(
        &mut self,
        name: ProviderConnectionName,
        connection: ProviderConnectionConfig,
    ) -> Option<ProviderConnectionConfig> {
        self.0.insert(name, connection)
    }

    /// Iterates connections in name order.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&ProviderConnectionName, &ProviderConnectionConfig)> {
        self.0.iter()
    }

    /// Returns whether the catalogue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of connections.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Validates endpoints, credentials, and every supplied reference.
    #[must_use]
    pub fn violations(
        &self,
        stages: &[ConnectionStageInferenceConfig],
        genesis: &ConnectionInitEmbeddingConfig,
    ) -> Vec<ProviderConnectionViolation> {
        let mut violations = self.connection_violations();

        for stage in stages {
            if self.get(&stage.connection).is_none() {
                violations.push(ProviderConnectionViolation::MissingReference {
                    connection: stage.connection.clone(),
                    usage: ProviderConnectionUsage::Inference,
                });
            }
        }

        match self.get(&genesis.connection) {
            None => violations.push(ProviderConnectionViolation::MissingReference {
                connection: genesis.connection.clone(),
                usage: ProviderConnectionUsage::GenesisEmbedding,
            }),
            Some(connection) if !connection.provider().supports_embedding() => {
                violations.push(ProviderConnectionViolation::UnsupportedCapability {
                    connection: genesis.connection.clone(),
                    provider: connection.provider(),
                    usage: ProviderConnectionUsage::GenesisEmbedding,
                });
            }
            Some(_) => {}
        }

        violations
    }

    fn connection_violations(&self) -> Vec<ProviderConnectionViolation> {
        let mut violations = Vec::new();
        let mut endpoints = BTreeMap::new();

        for (name, connection) in &self.0 {
            if connection.provider().requires_api_key() && connection.api_key().is_none() {
                violations.push(ProviderConnectionViolation::MissingCredential {
                    connection: name.clone(),
                    provider: connection.provider(),
                });
            }

            let Some(base_url) = connection.base_url() else {
                continue;
            };
            let Ok(normalised) = normalise_endpoint_url(base_url) else {
                violations.push(ProviderConnectionViolation::InvalidEndpoint {
                    connection: name.clone(),
                    value: base_url.to_owned(),
                });
                continue;
            };

            let endpoint = (connection.provider().as_str(), normalised.clone());
            match endpoints.entry(endpoint) {
                Entry::Vacant(slot) => {
                    slot.insert(name.clone());
                }
                Entry::Occupied(first) => {
                    violations.push(ProviderConnectionViolation::DuplicateEndpoint {
                        provider: connection.provider(),
                        normalised_base_url: normalised,
                        first: first.get().clone(),
                        second: name.clone(),
                    });
                }
            }
        }

        violations
    }
}

impl FromIterator<(ProviderConnectionName, ProviderConnectionConfig)> for ProviderConnections {
    fn from_iter<T: IntoIterator<Item = (ProviderConnectionName, ProviderConnectionConfig)>>(
        iter: T,
    ) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Inference settings that refer to one reusable connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ConnectionStageInferenceConfig {
    /// Provider connection name.
    pub connection: ProviderConnectionName,
    /// Model identifier.
    pub model: String,
    /// Sampling temperature.
    #[serde(default)]
    pub temperature: Option<f64>,
    /// Per-call output-token ceiling.
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

/// Genesis embedding settings that refer to one reusable connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ConnectionInitEmbeddingConfig {
    /// Provider connection name.
    pub connection: ProviderConnectionName,
    /// Embedding model identifier.
    pub model: String,
    /// Requested dimensions, or the model's native dimensions when absent.
    #[serde(default)]
    pub dimensions: Option<u32>,
}

/// The role for which a provider connection is referenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConnectionUsage {
    /// Completion inference.
    Inference,
    /// Genesis embedding.
    GenesisEmbedding,
}

/// A violation of the reusable-connection contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderConnectionViolation {
    /// A referenced connection does not exist.
    MissingReference {
        /// Missing connection name.
        connection: ProviderConnectionName,
        /// Intended use.
        usage: ProviderConnectionUsage,
    },
    /// A connection cannot serve its intended use.
    UnsupportedCapability {
        /// Connection name.
        connection: ProviderConnectionName,
        /// Connection provider.
        provider: ProviderKind,
        /// Intended use.
        usage: ProviderConnectionUsage,
    },
    /// A provider that requires a credential has none.
    MissingCredential {
        /// Connection name.
        connection: ProviderConnectionName,
        /// Connection provider.
        provider: ProviderKind,
    },
    /// A connection endpoint is invalid.
    InvalidEndpoint {
        /// Connection name.
        connection: ProviderConnectionName,
        /// Invalid endpoint value.
        value: String,
    },
    /// Two connections identify the same provider endpoint.
    DuplicateEndpoint {
        /// Endpoint provider.
        provider: ProviderKind,
        /// Canonical endpoint identity.
        normalised_base_url: String,
        /// First connection in canonical name order.
        first: ProviderConnectionName,
        /// Conflicting connection.
        second: ProviderConnectionName,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(raw: &str) -> ProviderConnectionName {
        ProviderConnectionName::parse(raw).expect("fixture name is valid")
    }

    fn ollama(base_url: &str) -> ProviderConnectionConfig {
        ProviderConnectionConfig::Ollama {
            base_url: base_url.to_owned(),
        }
    }

    fn genesis(connection: &str) -> ConnectionInitEmbeddingConfig {
        ConnectionInitEmbeddingConfig {
            connection: name(connection),
            model: "nomic-embed-text:v1.5".to_owned(),
            dimensions: None,
        }
    }

    #[test]
    fn test_debug_redacts_connection_credentials() {
        let key = "sentinel-secret"
            .parse::<ApiKey>()
            .expect("fixture key is valid");
        let connection = ProviderConnectionConfig::OpenAi {
            base_url: "https://api.openai.com".to_owned(),
            api_key: Some(key),
        };

        let debug = format!("{connection:?}");
        assert!(!debug.contains("sentinel-secret"));
        assert!(debug.contains("ApiKey("));
    }

    #[test]
    fn test_duplicate_normalised_endpoints_are_rejected() {
        let connections = [
            (name("ollama_a"), ollama("http://localhost:11434/")),
            (name("ollama_b"), ollama("HTTP://LOCALHOST:11434")),
        ]
        .into_iter()
        .collect::<ProviderConnections>();

        assert_eq!(
            connections.violations(&[], &genesis("ollama_a")),
            vec![ProviderConnectionViolation::DuplicateEndpoint {
                provider: ProviderKind::Ollama,
                normalised_base_url: "http://localhost:11434".to_owned(),
                first: name("ollama_a"),
                second: name("ollama_b"),
            }]
        );
    }

    #[test]
    fn test_references_and_embedding_capability_are_validated_together() {
        let key = "anthropic-key"
            .parse::<ApiKey>()
            .expect("fixture key is valid");
        let connections = [(
            name("anthropic_default"),
            ProviderConnectionConfig::Anthropic {
                base_url: "https://api.anthropic.com".to_owned(),
                api_key: Some(key),
            },
        )]
        .into_iter()
        .collect::<ProviderConnections>();
        let stages = [ConnectionStageInferenceConfig {
            connection: name("missing"),
            model: "model".to_owned(),
            temperature: None,
            max_tokens: None,
        }];

        assert_eq!(
            connections.violations(&stages, &genesis("anthropic_default")),
            vec![
                ProviderConnectionViolation::MissingReference {
                    connection: name("missing"),
                    usage: ProviderConnectionUsage::Inference,
                },
                ProviderConnectionViolation::UnsupportedCapability {
                    connection: name("anthropic_default"),
                    provider: ProviderKind::Anthropic,
                    usage: ProviderConnectionUsage::GenesisEmbedding,
                },
            ]
        );
    }

    #[test]
    fn test_missing_cloud_credential_is_rejected() {
        let connections = [(
            name("openai_default"),
            ProviderConnectionConfig::OpenAi {
                base_url: "https://api.openai.com".to_owned(),
                api_key: None,
            },
        )]
        .into_iter()
        .collect::<ProviderConnections>();

        assert_eq!(
            connections.violations(&[], &genesis("openai_default")),
            vec![ProviderConnectionViolation::MissingCredential {
                connection: name("openai_default"),
                provider: ProviderKind::OpenAi,
            }]
        );
    }
}
