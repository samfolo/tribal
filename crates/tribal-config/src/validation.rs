//! Exhaustive validation of the merged configuration.
//!
//! All invariant violations are collected into a [`Diagnostics`] before
//! returning, so the operator sees every problem at once rather than
//! fixing them one at a time.

use std::net::SocketAddr;

use url::{Host, Url};

use crate::{
    MAX_LIFECYCLE_DURATION_MS, MAX_OVERFETCH_MULTIPLIER, MAX_TTL_HOURS,
    error::ConfigError,
    sections::{
        MAX_AUTHORIZATION_CODE_TTL_SECONDS, MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        MIN_AUTHORIZATION_CODE_TTL_SECONDS, TransportKind, TribalConfig,
    },
};

mod diagnostics;
mod error;

pub use diagnostics::Diagnostics;
pub use error::{
    ComputedFloor, ConfigPath, Endpoint, FieldValue, Inclusion, NumericRange, OrderRelation,
    ProviderStage, ValidationError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Additional connections the worker pool requires beyond the concurrent task
/// count (heartbeat, reclaim, poll, and one spare).
const POOL_CONNECTION_OVERHEAD: u64 = 4;

/// Permitted range for cosine-similarity thresholds.  Shared by every
/// similarity-bearing config field; defining it once keeps the wording
/// uniform when [`ValidationError::OutOfRange`] renders.
pub(crate) const SIMILARITY_RANGE: NumericRange = NumericRange {
    low: Endpoint::open(0.0),
    high: Endpoint::closed(1.0),
};

/// Permitted range for per-stage sampling temperature. A gross-error guard
/// covering the widest provider maximum; per-model field admissibility is the
/// capability layer's concern, and a provider rejects a value above its own
/// tighter limit at request time.
pub(crate) const TEMPERATURE_RANGE: NumericRange = NumericRange {
    low: Endpoint::closed(0.0),
    high: Endpoint::closed(2.0),
};

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
    let mut diags = Diagnostics::default();

    validate_database(config, &mut diags);
    validate_server(config, &mut diags);
    validate_auth(config, &mut diags);
    validate_oauth(config, &mut diags);
    validate_worker(config, &mut diags);
    validate_pool_sizing(config, &mut diags);
    validate_embedding(config, &mut diags);
    validate_inference(config, &mut diags);
    validate_provider_limits(config, &mut diags);
    validate_api_key_presence(config, &mut diags);
    validate_discovery(config, &mut diags);
    validate_exploration(config, &mut diags);
    validate_telemetry(config, &mut diags);

    if diags.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationFailed { diagnostics: diags })
    }
}

// ---------------------------------------------------------------------------
// Section validators
// ---------------------------------------------------------------------------

fn validate_database(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.database.url.is_empty() {
        diags.push(ValidationError::Empty {
            field: ConfigPath::from_static("database.url"),
        });
    }

    if config.database.pool_mcp_max_connections == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "database.pool_mcp_max_connections",
        )));
    }

    if config.database.pool_worker_max_connections == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "database.pool_worker_max_connections",
        )));
    }
}

fn validate_server(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.server.transport == TransportKind::Stdio && config.server.bind_address.is_some() {
        diags.push(ValidationError::BindAddressStdioConflict);
    }

    if let Some(ref addr) = config.server.bind_address
        && addr.parse::<SocketAddr>().is_err()
    {
        diags.push(ValidationError::BindAddressMalformed {
            value: addr.clone(),
        });
    }

    if config.server.shutdown_deadline_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.shutdown_deadline_ms",
        )));
    }

    if config.server.job_state_ttl_seconds == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.job_state_ttl_seconds",
        )));
    }

    if config.server.job_state_hard_ttl_seconds == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.job_state_hard_ttl_seconds",
        )));
    } else if config.server.job_state_ttl_seconds > 0
        && config.server.job_state_hard_ttl_seconds < config.server.job_state_ttl_seconds
    {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.job_state_hard_ttl_seconds"),
                value: config.server.job_state_hard_ttl_seconds,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.job_state_ttl_seconds"),
                value: config.server.job_state_ttl_seconds,
            },
            relation: OrderRelation::AtLeast,
        });
    }

    let sse = &config.server.sse;

    if sse.max_connection_age_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.max_connection_age_ms",
        )));
    } else if sse.max_connection_age_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.max_connection_age_ms"),
            value: sse.max_connection_age_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    }

    if sse.idle_timeout_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.idle_timeout_ms",
        )));
    } else if sse.idle_timeout_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
            value: sse.idle_timeout_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    }

    if sse.keepalive_interval_ms == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "server.sse.keepalive_interval_ms",
        )));
    } else if sse.keepalive_interval_ms > MAX_LIFECYCLE_DURATION_MS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("server.sse.keepalive_interval_ms"),
            value: sse.keepalive_interval_ms,
            limit: MAX_LIFECYCLE_DURATION_MS,
        });
    } else if sse.idle_timeout_ms > 0 && sse.keepalive_interval_ms >= sse.idle_timeout_ms {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("server.sse.keepalive_interval_ms"),
                value: sse.keepalive_interval_ms,
            },
            bound: FieldValue {
                field: ConfigPath::from_static("server.sse.idle_timeout_ms"),
                value: sse.idle_timeout_ms,
            },
            relation: OrderRelation::LessThan,
        });
    }
}

fn validate_auth(config: &TribalConfig, diags: &mut Diagnostics) {
    let ttl = config.auth.token_ttl_hours;
    if ttl == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "auth.token_ttl_hours",
        )));
    } else if ttl > MAX_TTL_HOURS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("auth.token_ttl_hours"),
            value: ttl,
            limit: MAX_TTL_HOURS,
        });
    }
}

fn validate_oauth(config: &TribalConfig, diags: &mut Diagnostics) {
    let access_ttl = config.oauth.access_token_ttl_hours;
    if access_ttl == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "oauth.access_token_ttl_hours",
        )));
    } else if access_ttl > MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("oauth.access_token_ttl_hours"),
            value: access_ttl,
            limit: MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        });
    }

    let code_ttl = config.oauth.authorization_code_ttl_seconds;
    if code_ttl < MIN_AUTHORIZATION_CODE_TTL_SECONDS {
        diags.push(ValidationError::BelowMin {
            field: ConfigPath::from_static("oauth.authorization_code_ttl_seconds"),
            value: code_ttl,
            min: MIN_AUTHORIZATION_CODE_TTL_SECONDS,
        });
    } else if code_ttl > MAX_AUTHORIZATION_CODE_TTL_SECONDS {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("oauth.authorization_code_ttl_seconds"),
            value: code_ttl,
            limit: MAX_AUTHORIZATION_CODE_TTL_SECONDS,
        });
    }

    // Fail fast at load time on a malformed or unsupported issuer/resource
    // URL, rather than only when the runtime config is built at serve.
    validate_issuer_url(config.oauth.issuer_url.as_deref(), diags);
    validate_resource_url(config.oauth.resource_url.as_deref(), diags);

    // /register is unauthenticated, so DCR is refused when the OAuth
    // surface is advertised on a routable host. An explicitly non-loopback
    // issuer or resource URL is the reliable "remote clients intended"
    // signal: an unset URL derives a loopback address (wildcard binds are
    // rewritten to 127.0.0.1 at runtime), so a container bound to 0.0.0.0
    // behind a loopback port mapping is correctly allowed.
    if matches!(
        config.server.transport,
        TransportKind::Http | TransportKind::Sse
    ) && config.oauth.dcr_enabled
        && (url_is_explicit_non_loopback(config.oauth.issuer_url.as_deref())
            || url_is_explicit_non_loopback(config.oauth.resource_url.as_deref()))
    {
        diags.push(ValidationError::NonLoopbackDcrConflict);
    }
}

/// Returns `true` when `value` is set and parses to a URL whose host is
/// not a loopback address — the signal that the OAuth surface is
/// advertised to remote clients.
fn url_is_explicit_non_loopback(value: Option<&str>) -> bool {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(url) = Url::parse(raw) else {
        return false;
    };
    match url.host() {
        Some(Host::Ipv4(ip)) => !ip.is_loopback(),
        Some(Host::Ipv6(ip)) => !ip.is_loopback(),
        Some(Host::Domain(domain)) => domain != "localhost",
        None => false,
    }
}

/// Required form of `oauth.issuer_url`.
const ISSUER_ORIGIN_REQUIREMENT: &str = "must be an origin URL with no path, query, or fragment";

/// Required form of `oauth.resource_url` (RFC 8707).
const RESOURCE_FRAGMENT_REQUIREMENT: &str = "must not contain a fragment";

/// Validates `oauth.issuer_url`: when set, it must parse and be an origin
/// (no path, query, or fragment).
///
/// The authorisation-server metadata endpoints are appended to the issuer
/// and served at absolute root paths, so a sub-path issuer would advertise
/// endpoints the router does not serve. An unset field is admissible (the
/// consumer derives it from the bind address).
fn validate_issuer_url(value: Option<&str>, diags: &mut Diagnostics) {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return;
    };
    let field = ConfigPath::from_static("oauth.issuer_url");
    let Ok(url) = Url::parse(raw) else {
        diags.push(ValidationError::UrlMalformed {
            field,
            value: raw.to_owned(),
        });
        return;
    };
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        diags.push(ValidationError::UrlUnsupportedForm {
            field,
            value: raw.to_owned(),
            requirement: ISSUER_ORIGIN_REQUIREMENT,
        });
    }
}

/// Validates `oauth.resource_url`: when set, it must parse and carry no
/// fragment (RFC 8707 forbids a fragment on a resource indicator). An
/// unset field is admissible (the consumer derives it from the bind
/// address).
fn validate_resource_url(value: Option<&str>, diags: &mut Diagnostics) {
    let Some(raw) = value.filter(|raw| !raw.is_empty()) else {
        return;
    };
    let field = ConfigPath::from_static("oauth.resource_url");
    let Ok(url) = Url::parse(raw) else {
        diags.push(ValidationError::UrlMalformed {
            field,
            value: raw.to_owned(),
        });
        return;
    };
    if url.fragment().is_some() {
        diags.push(ValidationError::UrlUnsupportedForm {
            field,
            value: raw.to_owned(),
            requirement: RESOURCE_FRAGMENT_REQUIREMENT,
        });
    }
}

/// Validates a model-ID field: it must be a non-empty token with no whitespace.
fn validate_model_id(field: ConfigPath, model: &str, diags: &mut Diagnostics) {
    if model.is_empty() {
        diags.push(ValidationError::Empty { field });
    } else if model.chars().any(char::is_whitespace) {
        diags.push(ValidationError::ContainsWhitespace { field });
    }
}

fn validate_embedding(config: &TribalConfig, diags: &mut Diagnostics) {
    validate_model_id(
        ConfigPath::from_static("embedding.model"),
        &config.embedding.model,
        diags,
    );

    if config.embedding.dimensions == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "embedding.dimensions",
        )));
    }

    if !config.embedding.provider.supports_embedding() {
        diags.push(ValidationError::EmbeddingProviderUnsupported {
            provider: config.embedding.provider,
        });
    }
}

fn validate_worker(config: &TribalConfig, diags: &mut Diagnostics) {
    config.worker.validate(diags);
}

fn validate_pool_sizing(config: &TribalConfig, diags: &mut Diagnostics) {
    let available = u64::from(config.database.pool_worker_max_connections);
    let max_concurrent_tasks =
        u64::try_from(config.worker.max_concurrent_tasks).unwrap_or(u64::MAX);
    let required = max_concurrent_tasks.saturating_add(POOL_CONNECTION_OVERHEAD);

    if available < required {
        diags.push(ValidationError::DerivedFloor {
            field: ConfigPath::from_static("database.pool_worker_max_connections"),
            value: available,
            floor: ComputedFloor {
                value: required,
                addend: ConfigPath::from_static("worker.max_concurrent_tasks"),
                overhead: POOL_CONNECTION_OVERHEAD,
            },
        });
    }
}

fn validate_inference(config: &TribalConfig, diags: &mut Diagnostics) {
    // Range checks apply only to set values; `None` means provider default
    // and is always admissible.
    let stages = [
        ("inference.extraction", &config.inference.extraction),
        ("inference.triage", &config.inference.triage),
        ("inference.relation", &config.inference.relation),
    ];
    for (prefix, cfg) in stages {
        validate_model_id(ConfigPath::child(prefix, "model"), &cfg.model, diags);

        if let Some(temperature) = cfg.temperature
            && !TEMPERATURE_RANGE.contains(temperature)
        {
            diags.push(ValidationError::OutOfRange {
                field: ConfigPath::child(prefix, "temperature"),
                value: temperature,
                range: TEMPERATURE_RANGE,
            });
        }

        if cfg.max_tokens == Some(0) {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                prefix,
                "max_tokens",
            )));
        }
    }
}

fn validate_provider_limits(config: &TribalConfig, diags: &mut Diagnostics) {
    let task_timeout = config.worker.task_timeout_ms;

    for (provider, limits) in &config.limits.providers {
        let provider_prefix = format!("limits.providers.{provider}");

        if limits.max_in_flight == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                &provider_prefix,
                "max_in_flight",
            )));
        }

        let request_timeout = limits.request_timeout_ms;

        if request_timeout == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::child(
                &provider_prefix,
                "request_timeout_ms",
            )));
        } else if request_timeout >= task_timeout {
            diags.push(ValidationError::FieldOrdering {
                subject: FieldValue {
                    field: ConfigPath::child(&provider_prefix, "request_timeout_ms"),
                    value: request_timeout,
                },
                bound: FieldValue {
                    field: ConfigPath::from_static("worker.task_timeout_ms"),
                    value: task_timeout,
                },
                relation: OrderRelation::LessThan,
            });
        }
    }
}

fn validate_api_key_presence(config: &TribalConfig, diags: &mut Diagnostics) {
    let embedding_provider = config.embedding.provider;
    // Skip the api-key check for embedding providers that are
    // unsupported anyway — `validate_embedding` already emits
    // `EmbeddingProviderUnsupported`, so a parallel "set the api key"
    // remediation would only mislead the operator.
    if embedding_provider.supports_embedding()
        && embedding_provider.requires_api_key()
        && config.embedding.api_key.is_none()
    {
        diags.push(ValidationError::MissingApiKey {
            stage: ProviderStage::Embedding,
            provider: embedding_provider,
        });
    }

    let stages = [
        (ProviderStage::Extraction, &config.inference.extraction),
        (ProviderStage::Triage, &config.inference.triage),
        (ProviderStage::Relation, &config.inference.relation),
    ];
    for (stage, cfg) in stages {
        if cfg.provider.requires_api_key() && cfg.api_key.is_none() {
            diags.push(ValidationError::MissingApiKey {
                stage,
                provider: cfg.provider,
            });
        }
    }
}

fn validate_discovery(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.discovery.default_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.default_limit",
        )));
    }

    if config.discovery.max_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.max_limit",
        )));
    }

    if config.discovery.default_limit > config.discovery.max_limit {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("discovery.default_limit"),
                value: u64::from(config.discovery.default_limit),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("discovery.max_limit"),
                value: u64::from(config.discovery.max_limit),
            },
            relation: OrderRelation::AtMost,
        });
    }

    if config.discovery.overfetch_multiplier == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "discovery.overfetch_multiplier",
        )));
    } else if config.discovery.overfetch_multiplier > MAX_OVERFETCH_MULTIPLIER {
        diags.push(ValidationError::AboveMax {
            field: ConfigPath::from_static("discovery.overfetch_multiplier"),
            value: u64::from(config.discovery.overfetch_multiplier),
            limit: u64::from(MAX_OVERFETCH_MULTIPLIER),
        });
    }

    let threshold = config.discovery.similarity_threshold;
    if !SIMILARITY_RANGE.contains(threshold) {
        diags.push(ValidationError::OutOfRange {
            field: ConfigPath::from_static("discovery.similarity_threshold"),
            value: threshold,
            range: SIMILARITY_RANGE,
        });
    }
}

fn validate_exploration(config: &TribalConfig, diags: &mut Diagnostics) {
    if config.exploration.default_depth == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.default_depth",
        )));
    }

    if config.exploration.max_depth == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.max_depth",
        )));
    }

    if config.exploration.default_depth > config.exploration.max_depth {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("exploration.default_depth"),
                value: u64::from(config.exploration.default_depth),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("exploration.max_depth"),
                value: u64::from(config.exploration.max_depth),
            },
            relation: OrderRelation::AtMost,
        });
    }

    if config.exploration.default_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.default_limit",
        )));
    }

    if config.exploration.max_limit == 0 {
        diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
            "exploration.max_limit",
        )));
    }

    if config.exploration.default_limit > config.exploration.max_limit {
        diags.push(ValidationError::FieldOrdering {
            subject: FieldValue {
                field: ConfigPath::from_static("exploration.default_limit"),
                value: u64::from(config.exploration.default_limit),
            },
            bound: FieldValue {
                field: ConfigPath::from_static("exploration.max_limit"),
                value: u64::from(config.exploration.max_limit),
            },
            relation: OrderRelation::AtMost,
        });
    }
}

fn validate_telemetry(config: &TribalConfig, diags: &mut Diagnostics) {
    // `file_export` defaults to false, so an operator must explicitly set
    // it.  Requiring `enabled` for file export catches misconfiguration.
    //
    // `console_export` is intentionally not validated here: it defaults to
    // true via serde, so any config setting only `enabled = false` would
    // fail validation without the operator ever mentioning console_export.
    if config.telemetry.file_export && !config.telemetry.enabled {
        diags.push(ValidationError::TelemetryFileExportRequiresEnabled);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::ProviderKind;

    use super::*;
    use crate::DEFAULT_BIND_ADDRESS;

    fn valid_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://localhost/tribal")
    }

    /// Returns the diagnostics from a failed [`validate`] run, panicking
    /// if the run succeeded or produced a non-validation error.
    fn diagnostics_for(config: &TribalConfig) -> Diagnostics {
        match validate(config) {
            Err(ConfigError::ValidationFailed { diagnostics }) => diagnostics,
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
    }

    /// Returns true if `diags` contains a [`ValidationError`] matching
    /// `pred`.
    fn any<P: Fn(&ValidationError) -> bool>(diags: &Diagnostics, pred: P) -> bool {
        diags.iter().any(pred)
    }

    // -- valid -------------------------------------------------------------

    #[test]
    fn test_validate_accepts_valid_config() {
        assert!(validate(&valid_config()).is_ok());
    }

    // -- database ----------------------------------------------------------

    #[test]
    fn test_validate_rejects_empty_database_url() {
        let config = TribalConfig::default();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "database.url",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_pool_connections() {
        let mut config = valid_config();
        config.database.pool_mcp_max_connections = 0;
        config.database.pool_worker_max_connections = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "database.pool_mcp_max_connections",
        )));
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "database.pool_worker_max_connections",
        )));
    }

    // -- server ------------------------------------------------------------

    #[test]
    fn test_validate_rejects_bind_with_stdio() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Stdio;
        config.server.bind_address = Some(DEFAULT_BIND_ADDRESS.into());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BindAddressStdioConflict,
        )));
    }

    #[test]
    fn test_validate_rejects_invalid_bind_address() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("not-an-address".into());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BindAddressMalformed { value }
                if value == "not-an-address",
        )));
    }

    #[test]
    fn test_validate_rejects_dcr_with_routable_resource_url() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.oauth.resource_url = Some("https://tribal.example.com/mcp".into());
        config.oauth.dcr_enabled = true;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::NonLoopbackDcrConflict,
        )));
    }

    #[test]
    fn test_validate_rejects_dcr_with_routable_issuer_url() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.oauth.issuer_url = Some("https://auth.example.com".into());
        config.oauth.dcr_enabled = true;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::NonLoopbackDcrConflict,
        )));
    }

    #[test]
    fn test_validate_accepts_dcr_with_loopback_resource_url() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.oauth.resource_url = Some("http://127.0.0.1:8725/mcp".into());
        config.oauth.dcr_enabled = true;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_dcr_on_wildcard_bind_with_unset_urls() {
        // A wildcard bind with unset issuer/resource URLs derives a
        // loopback audience, so DCR stays allowed regardless of the bind
        // — the shape of a container bound to 0.0.0.0 behind a loopback
        // port mapping. A routable advertised URL is refused above.
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("0.0.0.0:8725".into());
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        config.oauth.dcr_enabled = true;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_routable_resource_without_dcr() {
        let mut config = valid_config();
        config.server.transport = TransportKind::Http;
        config.oauth.resource_url = Some("https://tribal.example.com/mcp".into());
        config.oauth.dcr_enabled = false;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_shutdown_deadline() {
        let mut config = valid_config();
        config.server.shutdown_deadline_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.shutdown_deadline_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.job_state_ttl_seconds",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_job_state_hard_ttl() {
        let mut config = valid_config();
        config.server.job_state_hard_ttl_seconds = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.job_state_hard_ttl_seconds",
        )));
    }

    #[test]
    fn test_validate_rejects_hard_ttl_less_than_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 600;
        config.server.job_state_hard_ttl_seconds = 300;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::AtLeast,
            } if subject.field.as_str() == "server.job_state_hard_ttl_seconds"
                && subject.value == 300
                && bound.field.as_str() == "server.job_state_ttl_seconds"
                && bound.value == 600,
        )));
    }

    #[test]
    fn test_validate_accepts_hard_ttl_equal_to_terminal_ttl() {
        let mut config = valid_config();
        config.server.job_state_ttl_seconds = 300;
        config.server.job_state_hard_ttl_seconds = 300;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_sse_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.keepalive_interval_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_sse_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.idle_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_sse_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "server.sse.max_connection_age_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_keepalive_gte_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = 300_000;
        config.server.sse.idle_timeout_ms = 300_000;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::LessThan,
            } if subject.field.as_str() == "server.sse.keepalive_interval_ms"
                && bound.field.as_str() == "server.sse.idle_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_max_connection_age() {
        let mut config = valid_config();
        config.server.sse.max_connection_age_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.max_connection_age_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_idle_timeout() {
        let mut config = valid_config();
        config.server.sse.idle_timeout_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.idle_timeout_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_keepalive_interval() {
        let mut config = valid_config();
        config.server.sse.keepalive_interval_ms = MAX_LIFECYCLE_DURATION_MS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "server.sse.keepalive_interval_ms"
                    && *limit == MAX_LIFECYCLE_DURATION_MS,
        )));
    }

    // -- auth --------------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_token_ttl_hours() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "auth.token_ttl_hours",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_token_ttl_hours() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = u64::MAX;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "auth.token_ttl_hours"
                    && *limit == MAX_TTL_HOURS,
        )));
    }

    #[test]
    fn test_validate_accepts_token_ttl_at_max() {
        let mut config = valid_config();
        config.auth.token_ttl_hours = MAX_TTL_HOURS;
        assert!(validate(&config).is_ok());
    }

    // -- oauth -------------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_oauth_access_ttl() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "oauth.access_token_ttl_hours",
        )));
    }

    #[test]
    fn test_validate_rejects_excessive_oauth_access_ttl() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "oauth.access_token_ttl_hours"
                    && *limit == MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS,
        )));
    }

    #[test]
    fn test_validate_accepts_oauth_access_ttl_at_max() {
        let mut config = valid_config();
        config.oauth.access_token_ttl_hours = MAX_OAUTH_ACCESS_TOKEN_TTL_HOURS;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_oauth_code_ttl_below_min() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MIN_AUTHORIZATION_CODE_TTL_SECONDS - 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min, .. }
                if field.as_str() == "oauth.authorization_code_ttl_seconds"
                    && *min == MIN_AUTHORIZATION_CODE_TTL_SECONDS,
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_code_ttl_above_max() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MAX_AUTHORIZATION_CODE_TTL_SECONDS + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "oauth.authorization_code_ttl_seconds"
                    && *limit == MAX_AUTHORIZATION_CODE_TTL_SECONDS,
        )));
    }

    #[test]
    fn test_validate_accepts_oauth_code_ttl_at_bounds() {
        let mut config = valid_config();
        config.oauth.authorization_code_ttl_seconds = MIN_AUTHORIZATION_CODE_TTL_SECONDS;
        assert!(validate(&config).is_ok());
        config.oauth.authorization_code_ttl_seconds = MAX_AUTHORIZATION_CODE_TTL_SECONDS;
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_malformed_oauth_issuer_url() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("not a url".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "oauth.issuer_url",
        )));
    }

    #[test]
    fn test_validate_rejects_malformed_oauth_resource_url() {
        let mut config = valid_config();
        config.oauth.resource_url = Some(":::not-a-url".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlMalformed { field, .. }
                if field.as_str() == "oauth.resource_url",
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_issuer_with_path() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("https://auth.example.com/tribal".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "oauth.issuer_url",
        )));
    }

    #[test]
    fn test_validate_rejects_oauth_resource_with_fragment() {
        let mut config = valid_config();
        config.oauth.resource_url = Some("https://auth.example.com/mcp#frag".to_owned());
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::UrlUnsupportedForm { field, .. }
                if field.as_str() == "oauth.resource_url",
        )));
    }

    #[test]
    fn test_validate_accepts_valid_oauth_urls() {
        let mut config = valid_config();
        config.oauth.issuer_url = Some("https://auth.example.com".to_owned());
        config.oauth.resource_url = Some("https://auth.example.com/mcp".to_owned());
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_accepts_unset_oauth_urls() {
        // Unset URLs are admissible: the consumer derives them from the
        // bind address at startup.
        let mut config = valid_config();
        config.oauth.issuer_url = None;
        config.oauth.resource_url = None;
        assert!(validate(&config).is_ok());
    }

    // -- embedding ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_embedding_dimensions() {
        let mut config = valid_config();
        config.embedding.dimensions = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "embedding.dimensions",
        )));
    }

    #[test]
    fn test_validate_rejects_anthropic_embedding_provider() {
        let mut config = valid_config();
        config.embedding.provider = ProviderKind::Anthropic;
        config.embedding.api_key = None;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::EmbeddingProviderUnsupported {
                provider: ProviderKind::Anthropic
            },
        )));
        // `Anthropic` is unsupported for embedding, so a parallel
        // "set the api key" remediation would mislead the operator.
        // `validate_api_key_presence` must skip the embedding stage
        // when the chosen provider is unsupported.
        assert!(!any(&diags, |d| matches!(
            d,
            ValidationError::MissingApiKey {
                stage: ProviderStage::Embedding,
                ..
            },
        )));
    }

    // -- pool sizing -------------------------------------------------------

    #[test]
    fn test_validate_pool_sizing_floor() {
        let mut config = valid_config();
        config.worker.max_concurrent_tasks = 10;
        config.database.pool_worker_max_connections = 10;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::DerivedFloor { field, value, floor }
                if field.as_str() == "database.pool_worker_max_connections"
                    && *value == 10
                    && floor.value == 14
                    && floor.addend.as_str() == "worker.max_concurrent_tasks"
                    && floor.overhead == POOL_CONNECTION_OVERHEAD,
        )));
    }

    // -- provider limits ---------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_max_in_flight() {
        let mut config = valid_config();
        config
            .limits
            .providers
            .get_mut(&ProviderKind::Ollama)
            .unwrap()
            .max_in_flight = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "limits.providers.ollama.max_in_flight",
        )));
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
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::LessThan,
            } if subject.field.as_str() == "limits.providers.ollama.request_timeout_ms"
                && bound.field.as_str() == "worker.task_timeout_ms",
        )));
    }

    // -- api key presence --------------------------------------------------

    #[test]
    fn test_validate_rejects_missing_api_key_for_cloud_provider() {
        let mut config = valid_config();
        config.embedding.provider = ProviderKind::OpenAi;
        config.embedding.api_key = None;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::MissingApiKey {
                stage: ProviderStage::Embedding,
                provider: ProviderKind::OpenAi,
            },
        )));
    }

    #[test]
    fn test_validate_emits_one_missing_api_key_per_stage() {
        let mut config = valid_config();
        config.embedding.provider = ProviderKind::OpenAi;
        config.embedding.api_key = None;
        config.inference.extraction.provider = ProviderKind::OpenAi;
        config.inference.extraction.api_key = None;
        config.inference.triage.provider = ProviderKind::OpenAi;
        config.inference.triage.api_key = None;
        config.inference.relation.provider = ProviderKind::OpenAi;
        config.inference.relation.api_key = None;

        let diags = diagnostics_for(&config);
        for stage in [
            ProviderStage::Embedding,
            ProviderStage::Extraction,
            ProviderStage::Triage,
            ProviderStage::Relation,
        ] {
            assert!(
                any(&diags, |d| matches!(
                    d,
                    ValidationError::MissingApiKey { stage: s, provider: ProviderKind::OpenAi }
                        if *s == stage,
                )),
                "no MissingApiKey for {stage:?}; diagnostics: {diags:?}",
            );
        }
    }

    // -- discovery ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_zero_overfetch_multiplier() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = 0;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "discovery.overfetch_multiplier",
        )));
    }

    #[test]
    fn test_validate_rejects_overfetch_multiplier_above_max() {
        let mut config = valid_config();
        config.discovery.overfetch_multiplier = MAX_OVERFETCH_MULTIPLIER + 1;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::AboveMax { field, limit, .. }
                if field.as_str() == "discovery.overfetch_multiplier"
                    && *limit == u64::from(MAX_OVERFETCH_MULTIPLIER),
        )));
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
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "discovery.similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_rejects_similarity_threshold_above_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.5;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "discovery.similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_accepts_similarity_threshold_at_one() {
        let mut config = valid_config();
        config.discovery.similarity_threshold = 1.0;
        assert!(validate(&config).is_ok());
    }

    // -- inference ---------------------------------------------------------

    #[test]
    fn test_validate_accepts_unset_inference_sampling() {
        // The default config leaves temperature and max_tokens unset.
        assert!(validate(&valid_config()).is_ok());
    }

    #[test]
    fn test_validate_rejects_temperature_above_range() {
        let mut config = valid_config();
        config.inference.extraction.temperature = Some(2.5);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "inference.extraction.temperature",
        )));
    }

    #[test]
    fn test_validate_rejects_negative_temperature() {
        let mut config = valid_config();
        config.inference.triage.temperature = Some(-0.1);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "inference.triage.temperature",
        )));
    }

    #[test]
    fn test_validate_accepts_temperature_at_bounds() {
        let mut config = valid_config();
        config.inference.extraction.temperature = Some(0.0);
        config.inference.triage.temperature = Some(2.0);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_max_tokens() {
        let mut config = valid_config();
        config.inference.relation.max_tokens = Some(0);
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "inference.relation.max_tokens",
        )));
    }

    #[test]
    fn test_validate_rejects_empty_model() {
        let mut config = valid_config();
        config.inference.extraction.model = String::new();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "inference.extraction.model",
        )));
    }

    #[test]
    fn test_validate_rejects_whitespace_model() {
        let mut config = valid_config();
        config.inference.triage.model = "gpt 4o".to_owned();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::ContainsWhitespace { field }
                if field.as_str() == "inference.triage.model",
        )));
    }

    #[test]
    fn test_validate_rejects_empty_embedding_model() {
        let mut config = valid_config();
        config.embedding.model = String::new();
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::Empty { field } if field.as_str() == "embedding.model",
        )));
    }

    // -- telemetry ---------------------------------------------------------

    #[test]
    fn test_validate_rejects_file_export_without_enabled() {
        let mut config = valid_config();
        config.telemetry.file_export = true;
        config.telemetry.enabled = false;
        let diags = diagnostics_for(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::TelemetryFileExportRequiresEnabled,
        )));
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

    // -- aggregation -------------------------------------------------------

    #[test]
    fn test_validate_collects_multiple_errors() {
        let mut config = TribalConfig::default();
        config.database.pool_mcp_max_connections = 0;
        config.discovery.overfetch_multiplier = 0;
        let diags = diagnostics_for(&config);
        assert!(
            diags.len() >= 3,
            "expected at least 3 diagnostics, got {}: {diags:?}",
            diags.len(),
        );
    }
}
