//! Exhaustive validation of the merged configuration.
//!
//! All invalid fields are collected before returning, so the operator
//! sees every problem at once rather than fixing them one at a time.

use std::net::SocketAddr;

use crate::{
    error::ConfigError,
    sections::{TransportKind, TribalConfig},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Additional connections the worker pool requires beyond the concurrent task
/// count (heartbeat, reclaim, poll, and one spare).
const POOL_CONNECTION_OVERHEAD: usize = 4;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validates the merged configuration.
///
/// Collects all errors and returns them together.
///
/// # Errors
///
/// Returns [`ConfigError::ValidationFailed`] if any field violates its
/// constraint.
pub fn validate(config: &TribalConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();

    validate_database(config, &mut errors);
    validate_server(config, &mut errors);
    validate_worker(config, &mut errors);
    validate_pool_sizing(config, &mut errors);
    validate_provider_timeouts(config, &mut errors);
    validate_api_key_presence(config, &mut errors);
    validate_discovery(config, &mut errors);
    validate_exploration(config, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationFailed { errors })
    }
}

// ---------------------------------------------------------------------------
// Section validators
// ---------------------------------------------------------------------------

fn validate_database(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.database.url.is_empty() {
        errors.push("database.url must not be empty".into());
    }

    if config.database.pool_mcp_max_connections == 0 {
        errors.push("database.pool_mcp_max_connections must be greater than zero".into());
    }

    if config.database.pool_worker_max_connections == 0 {
        errors.push("database.pool_worker_max_connections must be greater than zero".into());
    }
}

fn validate_server(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.server.transport == TransportKind::Stdio && config.server.bind_address.is_some() {
        errors.push("server.bind_address cannot be set when server.transport is stdio".into());
    }

    if let Some(ref addr) = config.server.bind_address
        && addr.parse::<SocketAddr>().is_err()
    {
        errors.push(format!(
            "server.bind_address is not a valid socket address: {addr}"
        ));
    }
}

fn validate_worker(config: &TribalConfig, errors: &mut Vec<String>) {
    if let Err(e) = config.worker.validate() {
        errors.push(format!("worker: {e}"));
    }
}

fn validate_pool_sizing(config: &TribalConfig, errors: &mut Vec<String>) {
    let available = config.database.pool_worker_max_connections;
    let required = config.worker.max_concurrent_tasks + POOL_CONNECTION_OVERHEAD;

    if (available as usize) < required {
        errors.push(format!(
            "database.pool_worker_max_connections ({available}) must be at least \
             worker.max_concurrent_tasks + {POOL_CONNECTION_OVERHEAD} ({required})"
        ));
    }
}

fn validate_provider_timeouts(config: &TribalConfig, errors: &mut Vec<String>) {
    let task_timeout = config.worker.task_timeout_millis;

    for (provider, limits) in &config.limits.providers {
        let request_timeout = limits.request_timeout_ms;

        if request_timeout == 0 {
            errors.push(format!(
                "limits.providers.{provider:?}.request_timeout_ms must be greater than zero"
            ));
        } else if request_timeout >= task_timeout {
            errors.push(format!(
                "limits.providers.{provider:?}.request_timeout_ms ({request_timeout}) \
                 must be less than worker.task_timeout_millis ({task_timeout})"
            ));
        }
    }
}

fn validate_api_key_presence(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.embedding.provider.requires_api_key() && config.embedding.api_key.is_none() {
        errors.push(format!(
            "embedding.api_key is required when embedding.provider is {:?}",
            config.embedding.provider
        ));
    }

    let stages = [
        ("inference.extraction", &config.inference.extraction),
        ("inference.triage", &config.inference.triage),
        ("inference.relation", &config.inference.relation),
    ];
    for (path, stage) in stages {
        if stage.provider.requires_api_key() && stage.api_key.is_none() {
            errors.push(format!(
                "{path}.api_key is required when {path}.provider is {:?}",
                stage.provider
            ));
        }
    }
}

fn validate_discovery(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.discovery.default_limit == 0 {
        errors.push("discovery.default_limit must be greater than zero".into());
    }

    if config.discovery.max_limit == 0 {
        errors.push("discovery.max_limit must be greater than zero".into());
    }

    if config.discovery.default_limit > config.discovery.max_limit {
        errors.push("discovery.default_limit must be at most discovery.max_limit".into());
    }

    if config.discovery.overfetch_multiplier == 0 {
        errors.push("discovery.overfetch_multiplier must be greater than zero".into());
    }
}

fn validate_exploration(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.exploration.default_depth == 0 {
        errors.push("exploration.default_depth must be greater than zero".into());
    }

    if config.exploration.max_depth == 0 {
        errors.push("exploration.max_depth must be greater than zero".into());
    }

    if config.exploration.default_depth > config.exploration.max_depth {
        errors.push("exploration.default_depth must be at most exploration.max_depth".into());
    }

    if config.exploration.default_limit == 0 {
        errors.push("exploration.default_limit must be greater than zero".into());
    }

    if config.exploration.max_limit == 0 {
        errors.push("exploration.max_limit must be greater than zero".into());
    }

    if config.exploration.default_limit > config.exploration.max_limit {
        errors.push("exploration.default_limit must be at most exploration.max_limit".into());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderKind;

    fn valid_config() -> TribalConfig {
        let mut config = TribalConfig::default();
        config.database.url = "postgres://localhost/tribal".into();
        config
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        assert!(validate(&valid_config()).is_ok());
    }

    #[test]
    fn test_validate_rejects_empty_database_url() {
        let config = TribalConfig::default();
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("database.url must not be empty"));
    }

    #[test]
    fn test_validate_rejects_bind_with_stdio() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Stdio;
        config.server.bind_address = Some("127.0.0.1:7077".into());
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bind_address cannot be set when server.transport is stdio"));
    }

    #[test]
    fn test_validate_rejects_invalid_bind_address() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("not-an-address".into());
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a valid socket address"));
    }

    #[test]
    fn test_validate_rejects_zero_pool_connections() {
        let mut config = valid_config();
        config.database.pool_mcp_max_connections = 0;
        config.database.pool_worker_max_connections = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("pool_mcp_max_connections must be greater than zero"));
        assert!(msg.contains("pool_worker_max_connections must be greater than zero"));
    }

    #[test]
    fn test_validate_pool_sizing_floor() {
        let mut config = valid_config();
        config.worker.max_concurrent_tasks = 10;
        config.database.pool_worker_max_connections = 10;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        let required = 10 + POOL_CONNECTION_OVERHEAD;
        assert!(msg.contains(&format!(
            "pool_worker_max_connections (10) must be at least \
             worker.max_concurrent_tasks + {POOL_CONNECTION_OVERHEAD} ({required})"
        )));
    }

    #[test]
    fn test_validate_rejects_request_timeout_exceeding_task_timeout() {
        let mut config = valid_config();
        config.worker.task_timeout_millis = 100_000;
        config
            .limits
            .providers
            .get_mut(&ProviderKind::Ollama)
            .unwrap()
            .request_timeout_ms = 100_000;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("request_timeout_ms"));
        assert!(msg.contains("must be less than"));
    }

    #[test]
    fn test_validate_rejects_missing_api_key_for_cloud_provider() {
        let mut config = valid_config();
        config.embedding.provider = ProviderKind::Anthropic;
        config.embedding.api_key = None;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("embedding.api_key is required"));
    }

    #[test]
    fn test_validate_rejects_zero_overfetch_multiplier() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("overfetch_multiplier must be greater than zero"));
    }

    #[test]
    fn test_validate_collects_multiple_errors() {
        let mut config = TribalConfig::default();
        config.database.pool_mcp_max_connections = 0;
        config.discovery.overfetch_multiplier = 0;
        let err = validate(&config).unwrap_err();
        if let ConfigError::ValidationFailed { errors } = err {
            assert!(
                errors.len() >= 3,
                "expected at least 3 errors, got {}: {errors:?}",
                errors.len()
            );
        } else {
            panic!("expected ValidationFailed");
        }
    }
}
