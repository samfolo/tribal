//! Exhaustive validation of the merged configuration.
//!
//! All invalid fields are collected before returning, so the operator
//! sees every problem at once rather than fixing them one at a time.

use std::net::SocketAddr;

use crate::{
    MAX_LIFECYCLE_DURATION_MS, MAX_OVERFETCH_MULTIPLIER,
    error::ConfigError,
    sections::{ProviderKind, TransportKind, TribalConfig},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Error message for a zero `auth.token_ttl_hours` value.
pub const ERR_TTL_ZERO: &str = "auth.token_ttl_hours must be greater than zero";

/// Error message when `file_export` is set without `enabled`.
pub const FILE_EXPORT_REQUIRES_ENABLED: &str =
    "telemetry.file_export requires telemetry.enabled to be true";

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
    validate_auth(config, &mut errors);
    validate_worker(config, &mut errors);
    validate_pool_sizing(config, &mut errors);
    validate_embedding(config, &mut errors);
    validate_provider_limits(config, &mut errors);
    validate_api_key_presence(config, &mut errors);
    validate_discovery(config, &mut errors);
    validate_exploration(config, &mut errors);
    validate_telemetry(config, &mut errors);

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

    if config.server.shutdown_deadline_ms == 0 {
        errors.push("server.shutdown_deadline_ms must be greater than zero".into());
    }

    if config.server.job_state_ttl_seconds == 0 {
        errors.push("server.job_state_ttl_seconds must be greater than zero".into());
    }

    if config.server.job_state_hard_ttl_seconds == 0 {
        errors.push("server.job_state_hard_ttl_seconds must be greater than zero".into());
    } else if config.server.job_state_ttl_seconds > 0
        && config.server.job_state_hard_ttl_seconds < config.server.job_state_ttl_seconds
    {
        errors.push(format!(
            "server.job_state_hard_ttl_seconds ({}) must be greater than or equal to \
             server.job_state_ttl_seconds ({})",
            config.server.job_state_hard_ttl_seconds, config.server.job_state_ttl_seconds
        ));
    }

    let sse = &config.server.sse;

    if sse.max_connection_age_ms == 0 {
        errors.push("server.sse.max_connection_age_ms must be greater than zero".into());
    } else if sse.max_connection_age_ms > MAX_LIFECYCLE_DURATION_MS {
        errors.push(format!(
            "server.sse.max_connection_age_ms ({}) must not exceed {MAX_LIFECYCLE_DURATION_MS}",
            sse.max_connection_age_ms
        ));
    }

    if sse.idle_timeout_ms == 0 {
        errors.push("server.sse.idle_timeout_ms must be greater than zero".into());
    } else if sse.idle_timeout_ms > MAX_LIFECYCLE_DURATION_MS {
        errors.push(format!(
            "server.sse.idle_timeout_ms ({}) must not exceed {MAX_LIFECYCLE_DURATION_MS}",
            sse.idle_timeout_ms
        ));
    }

    if sse.keepalive_interval_ms == 0 {
        errors.push("server.sse.keepalive_interval_ms must be greater than zero".into());
    } else if sse.keepalive_interval_ms > MAX_LIFECYCLE_DURATION_MS {
        errors.push(format!(
            "server.sse.keepalive_interval_ms ({}) must not exceed {MAX_LIFECYCLE_DURATION_MS}",
            sse.keepalive_interval_ms
        ));
    } else if sse.idle_timeout_ms > 0 && sse.keepalive_interval_ms >= sse.idle_timeout_ms {
        errors.push(format!(
            "server.sse.keepalive_interval_ms ({}) must be less than \
             server.sse.idle_timeout_ms ({})",
            sse.keepalive_interval_ms, sse.idle_timeout_ms
        ));
    }
}

fn validate_auth(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.auth.token_ttl_hours == 0 {
        errors.push(ERR_TTL_ZERO.into());
    }
}

fn validate_embedding(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.embedding.dimensions == 0 {
        errors.push("embedding.dimensions must be greater than zero".into());
    }

    if config.embedding.provider == ProviderKind::Anthropic {
        errors.push(
            "embedding.provider cannot be anthropic: \
             Anthropic does not provide an embedding API"
                .into(),
        );
    }
}

fn validate_worker(config: &TribalConfig, errors: &mut Vec<String>) {
    config.worker.validate(errors);
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

fn validate_provider_limits(config: &TribalConfig, errors: &mut Vec<String>) {
    let task_timeout = config.worker.task_timeout_ms;

    for (provider, limits) in &config.limits.providers {
        if limits.max_in_flight == 0 {
            errors.push(format!(
                "limits.providers.{provider}.max_in_flight must be greater than zero"
            ));
        }

        let request_timeout = limits.request_timeout_ms;

        if request_timeout == 0 {
            errors.push(format!(
                "limits.providers.{provider}.request_timeout_ms must be greater than zero"
            ));
        } else if request_timeout >= task_timeout {
            errors.push(format!(
                "limits.providers.{provider}.request_timeout_ms ({request_timeout}) \
                 must be less than worker.task_timeout_ms ({task_timeout})"
            ));
        }
    }
}

fn validate_api_key_presence(config: &TribalConfig, errors: &mut Vec<String>) {
    if config.embedding.provider.requires_api_key() && config.embedding.api_key.is_none() {
        errors.push(format!(
            "embedding.api_key is required when embedding.provider is {}",
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
                "{path}.api_key is required when {path}.provider is {}",
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
    } else if config.discovery.overfetch_multiplier > MAX_OVERFETCH_MULTIPLIER {
        errors.push(format!(
            "discovery.overfetch_multiplier ({}) must not exceed {MAX_OVERFETCH_MULTIPLIER}",
            config.discovery.overfetch_multiplier,
        ));
    }

    let threshold = config.discovery.similarity_threshold;
    if !(threshold > 0.0 && threshold <= 1.0) {
        errors.push(format!(
            "discovery.similarity_threshold ({threshold}) must be in (0.0, 1.0]"
        ));
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

fn validate_telemetry(config: &TribalConfig, errors: &mut Vec<String>) {
    // `file_export` defaults to false, so an operator must explicitly set
    // it.  Requiring `enabled` for file export catches misconfiguration.
    //
    // `console_export` is intentionally not validated here: it defaults to
    // true via serde, so any config setting only `enabled = false` would
    // fail validation without the operator ever mentioning console_export.
    if config.telemetry.file_export && !config.telemetry.enabled {
        errors.push(FILE_EXPORT_REQUIRES_ENABLED.into());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_BIND_ADDRESS, ProviderKind};

    fn valid_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://localhost/tribal")
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
        config.server.bind_address = Some(DEFAULT_BIND_ADDRESS.into());
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
    fn test_validate_rejects_zero_max_in_flight() {
        let mut config = valid_config();
        config
            .limits
            .providers
            .get_mut(&ProviderKind::Ollama)
            .unwrap()
            .max_in_flight = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("max_in_flight must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_request_timeout_exceeding_task_timeout() {
        let mut config = valid_config();
        config.worker.task_timeout_ms = 100_000;
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
    fn test_validate_rejects_overfetch_multiplier_above_max() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = MAX_OVERFETCH_MULTIPLIER + 1;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("overfetch_multiplier"));
        assert!(msg.contains("must not exceed"));
    }

    #[test]
    fn test_validate_accepts_overfetch_multiplier_at_max() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = MAX_OVERFETCH_MULTIPLIER;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_similarity_threshold() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 0.0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("similarity_threshold"));
        assert!(msg.contains("must be in (0.0, 1.0]"));
    }

    #[test]
    fn test_validate_rejects_similarity_threshold_above_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.5;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("similarity_threshold"));
    }

    #[test]
    fn test_validate_accepts_similarity_threshold_at_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.0;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_token_ttl_hours() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(ERR_TTL_ZERO));
    }

    #[test]
    fn test_validate_rejects_zero_shutdown_deadline() {
        let mut config = valid_config();
        config.server.shutdown_deadline_ms = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.shutdown_deadline_ms must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.job_state_ttl_seconds must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_hard_ttl() {
        let mut config = valid_config();
        config.server.job_state_hard_ttl_seconds = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.job_state_hard_ttl_seconds must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_hard_ttl_less_than_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 600;
        config.server.job_state_hard_ttl_seconds = 300;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(
                "server.job_state_hard_ttl_seconds (300) must be greater than or equal to"
            )
        );
    }

    #[test]
    fn test_validate_accepts_hard_ttl_equal_to_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 300;
        config.server.job_state_hard_ttl_seconds = 300;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_anthropic_embedding_provider() {
        let mut config = valid_config();
        config.embedding.provider = ProviderKind::Anthropic;
        config.embedding.api_key = Some("sk-test".into());
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Anthropic does not provide an embedding API"));
    }

    #[test]
    fn test_validate_rejects_zero_embedding_dimensions() {
        let mut config = valid_config();
        config.embedding.dimensions = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("embedding.dimensions must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_zero_sse_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.sse.keepalive_interval_ms must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_zero_sse_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.sse.idle_timeout_ms must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_zero_sse_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = 0;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.sse.max_connection_age_ms must be greater than zero"));
    }

    #[test]
    fn test_validate_rejects_keepalive_gte_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 300_000;
        config.server.sse.idle_timeout_ms = 300_000;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("keepalive_interval_ms"));
        assert!(msg.contains("must be less than"));
    }

    #[test]
    fn test_validate_rejects_excessive_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("max_connection_age_ms"));
        assert!(msg.contains("must not exceed"));
    }

    #[test]
    fn test_validate_rejects_excessive_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("idle_timeout_ms"));
        assert!(msg.contains("must not exceed"));
    }

    #[test]
    fn test_validate_rejects_excessive_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("keepalive_interval_ms"));
        assert!(msg.contains("must not exceed"));
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

    #[test]
    fn test_validate_rejects_file_export_without_enabled() {
        let mut config = valid_config();
        config.telemetry.file_export = true;
        config.telemetry.enabled = false;
        let err = validate(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(FILE_EXPORT_REQUIRES_ENABLED));
    }

    #[test]
    fn test_validate_accepts_file_export_with_enabled() {
        let mut config = valid_config();
        config.telemetry.file_export = true;
        config.telemetry.enabled = true;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_console_export_without_enabled() {
        let mut config = valid_config();
        config.telemetry.console_export = true;
        config.telemetry.enabled = false;
        assert!(
            validate(&config).is_ok(),
            "console_export defaults to true — should not reject when enabled is false",
        );
    }
}
