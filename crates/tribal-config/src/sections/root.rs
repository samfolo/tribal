//! Top-level configuration struct composing all sections.

use serde::{Deserialize, Serialize};

use super::{
    auth::AuthConfig, database::DatabaseConfig, discovery::DiscoveryConfig,
    embedding::EmbeddingConfig, exploration::ExplorationConfig, inference::InferenceConfig,
    limits::LimitsConfig, logging::LoggingConfig, prompts::PromptsConfig, server::ServerConfig,
    telemetry::TelemetryConfig, worker::WorkerConfig,
};
use crate::validation::{ConfigPath, EnumerateFields};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Current crate version, stamped into every loaded configuration.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

fn default_version() -> String {
    String::from(VERSION)
}

// ---------------------------------------------------------------------------
// TribalConfig
// ---------------------------------------------------------------------------

/// Top-level configuration for the Tribal server.
///
/// All fields default to sensible values for local development.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TribalConfig {
    /// Schema version, set automatically to the crate version.
    ///
    /// Used to detect stale configuration files that may need migration.
    #[serde(default = "default_version")]
    pub version: String,

    /// Server transport and connection settings.
    #[serde(default)]
    pub server: ServerConfig,

    /// Database connection pool settings.
    #[serde(default)]
    pub database: DatabaseConfig,

    /// Authentication settings.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Worker loop settings.
    #[serde(default)]
    pub worker: WorkerConfig,

    /// Embedding provider settings.
    #[serde(default)]
    pub embedding: EmbeddingConfig,

    /// Per-stage inference settings.
    #[serde(default)]
    pub inference: InferenceConfig,

    /// Per-provider concurrency limits.
    #[serde(default)]
    pub limits: LimitsConfig,

    /// Prompt file settings.
    #[serde(default)]
    pub prompts: PromptsConfig,

    /// Discovery (semantic search) settings.
    #[serde(default)]
    pub discovery: DiscoveryConfig,

    /// Exploration (graph traversal) settings.
    #[serde(default)]
    pub exploration: ExplorationConfig,

    /// Logging settings.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// OpenTelemetry and trace export settings.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

impl TribalConfig {
    /// Serialises the configuration to YAML.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] if serialisation fails.
    pub fn to_yaml(&self) -> Result<String, crate::ConfigError> {
        serde_yaml::to_string(self).map_err(|e| crate::ConfigError::Render {
            source: Box::new(e),
        })
    }

    /// Builds the smallest configuration that passes [`crate::validate`].
    ///
    /// Defaults for every field except those that have no sensible
    /// default and must be supplied by the caller. The signature is
    /// the agreed point of update: when a new always-required field
    /// joins the schema, it joins this parameter list, forcing every
    /// caller to provide a value rather than hand-rolling a sentinel
    /// at each call site.
    #[must_use]
    pub fn minimum_valid(database_url: impl Into<String>) -> Self {
        let mut config = Self::default();
        config.database.url = database_url.into();
        config
    }
}

impl Default for TribalConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            server: ServerConfig::default(),
            database: DatabaseConfig::default(),
            auth: AuthConfig::default(),
            worker: WorkerConfig::default(),
            embedding: EmbeddingConfig::default(),
            inference: InferenceConfig::default(),
            limits: LimitsConfig::default(),
            prompts: PromptsConfig::default(),
            discovery: DiscoveryConfig::default(),
            exploration: ExplorationConfig::default(),
            logging: LoggingConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// EnumerateFields
// ---------------------------------------------------------------------------

/// Top-level depth-first walker.  `prefix` is ignored — `TribalConfig`
/// is the root, so each section's prefix is its own absolute name.
impl EnumerateFields for TribalConfig {
    fn enumerate(_prefix: &str, out: &mut Vec<ConfigPath>) {
        out.push(ConfigPath::from_static("version"));
        ServerConfig::enumerate("server", out);
        DatabaseConfig::enumerate("database", out);
        AuthConfig::enumerate("auth", out);
        WorkerConfig::enumerate("worker", out);
        EmbeddingConfig::enumerate("embedding", out);
        InferenceConfig::enumerate("inference", out);
        LimitsConfig::enumerate("limits", out);
        PromptsConfig::enumerate("prompts", out);
        DiscoveryConfig::enumerate("discovery", out);
        ExplorationConfig::enumerate("exploration", out);
        LoggingConfig::enumerate("logging", out);
        TelemetryConfig::enumerate("telemetry", out);
    }
}

#[cfg(test)]
#[allow(dead_code, clippy::let_underscore_untyped)]
fn _check_tribal_config_fields(c: &TribalConfig) {
    let _ = &c.version;
    let _ = &c.server;
    let _ = &c.database;
    let _ = &c.auth;
    let _ = &c.worker;
    let _ = &c.embedding;
    let _ = &c.inference;
    let _ = &c.limits;
    let _ = &c.prompts;
    let _ = &c.discovery;
    let _ = &c.exploration;
    let _ = &c.logging;
    let _ = &c.telemetry;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_deserialises_from_empty_object() {
        let config: TribalConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, TribalConfig::default());
    }

    #[test]
    fn test_default_version_matches_crate_version() {
        let config = TribalConfig::default();
        assert_eq!(config.version, VERSION);
    }
}
